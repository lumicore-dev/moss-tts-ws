"""
MOSS-TTS WebSocket Server V2 — 真正的低延迟流式合成
====================================================
核心改进：不再等完整 waveform 生成完，而是在 ONNX 逐帧解码
回调中实时通过 WebSocket 推送 audio chunk。

流式流程：
  1. 准备文本 & prompt audio codes (一次)
  2. 按句分割文本（多段）
  3. 对每段执行 generate_audio_frames(..., on_frame=on_frame)
  4. on_frame 回调中：累积帧 → 触发 codec 解码 → 解码后的 PCM chunk → 直接 ws.send()
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable

import numpy as np
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

_PROJECT_ROOT = Path(__file__).resolve().parent
_MOSS_TTS_DIR = _PROJECT_ROOT.parent / "MOSS-TTS-Nano"
if _MOSS_TTS_DIR.exists():
    sys.path.insert(0, str(_MOSS_TTS_DIR))

from onnx_tts_runtime import OnnxTtsRuntime
from onnx_tts_runtime import (
    _concat_waveforms,
    _merge_audio_channels,
    _resolve_stream_decode_frame_budget,
)

logger = logging.getLogger("moss-tts-ws-v2")

# ── 常量 ──────────────────────────────────────────────────────
TARGET_CHUNK_SEC = 0.30       # 每个 audio chunk 目标时长（秒）

# ── 全局运行时 ─────────────────────────────────────────────────
_runtime: OnnxTtsRuntime | None = None
_runtime_lock = asyncio.Lock()

async def get_runtime(model_dir: str | Path, execution_provider: str = "cpu") -> OnnxTtsRuntime:
    global _runtime
    if _runtime is None:
        async with _runtime_lock:
            if _runtime is None:
                logger.info("加载 ONNX TTS Runtime...")
                _runtime = OnnxTtsRuntime(
                    model_dir=str(model_dir),
                    execution_provider=execution_provider,
                )
                logger.info("加载完成")
    return _runtime

# ── FastAPI ───────────────────────────────────────────────────
app = FastAPI(title="MOSS-TTS WebSocket Server V2", version="0.2.0")

@app.get("/")
async def root():
    return {"service": "MOSS-TTS WS V2", "status": "ok", "mode": "real-time streaming"}


def _build_audio_chunk_msg(
    waveform: np.ndarray,
    sr: int,
    is_final: bool,
) -> str:
    """构建 audio_chunk JSON 消息"""
    pcm = waveform.astype(np.float32).tobytes()
    b64 = base64.b64encode(pcm).decode("ascii")
    channels = waveform.shape[1] if waveform.ndim > 1 else 1
    return json.dumps({
        "type": "audio_chunk",
        "data": b64,
        "sample_rate": sr,
        "channels": channels,
        "is_final": is_final,
    })


# ── 流式合成核心 ──────────────────────────────────────────────
def _run_streaming_synthesis(
    runtime: OnnxTtsRuntime,
    text: str,
    voice: str | None,
    sample_mode: str | None,
    do_sample: bool,
    seed: int | None,
    max_new_frames: int | None,
    voice_clone_max_text_tokens: int,
    cb: Callable[[np.ndarray], None],
) -> dict:
    """
    在同步线程中执行，通过 cb 回调实时吐出 audio chunk。
    cb(waveform_chunk) 每次推一小段 PCM。
    """
    # 1. 参数解析
    if max_new_frames is not None:
        runtime.manifest["generation_defaults"]["max_new_frames"] = int(max_new_frames)
    if sample_mode:
        runtime.manifest["generation_defaults"]["sample_mode"] = sample_mode
    runtime.manifest["generation_defaults"]["do_sample"] = do_sample
    if seed is not None:
        runtime.rng = np.random.default_rng(int(seed))

    # 2. 准备文本
    prepared_texts = runtime.prepare_synthesis_text(
        text=text,
        voice=str(voice or ""),
        enable_wetext=True,
        enable_normalize_tts_text=True,
    )
    prepared_text = str(prepared_texts["text"])

    # 3. 获取 prompt audio codes
    prompt_audio_codes = runtime.resolve_prompt_audio_codes(
        voice=voice,
        prompt_audio_path=None,
    )

    # 4. 按句分割
    text_chunks = runtime.split_voice_clone_text(
        prepared_text,
        max_tokens=int(voice_clone_max_text_tokens),
    )

    logger.debug("文本已分割为 %d 段", len(text_chunks))

    total_generated_frames: list[list[int]] = []
    total_audio_samples = 0
    sr = int(runtime.codec_meta["codec_config"]["sample_rate"])
    channels = int(runtime.codec_meta["codec_config"]["channels"])

    for chunk_idx, chunk_text in enumerate(text_chunks):
        # 5. 编码 & 构建 request_rows
        text_token_ids = runtime.encode_text(chunk_text)
        request_rows = runtime.build_voice_clone_request_rows(
            prompt_audio_codes, text_token_ids
        )

        # 6. 开始生成 — 使用 on_frame 回调实现实时解码推送
        pending_decode_frames: list[list[int]] = []
        emitted_chunks_buffer: list[np.ndarray] = []
        emitted_samples_total = 0
        first_audio_emitted_at: float | None = None

        runtime.codec_streaming_session.reset()

        def decode_pending(force: bool):
            nonlocal emitted_samples_total, first_audio_emitted_at
            pending = len(pending_decode_frames)
            if pending <= 0:
                return

            budget = 0
            if not force:
                budget = _resolve_stream_decode_frame_budget(
                    emitted_samples_total, sr, first_audio_emitted_at
                )
            if not force and pending < max(1, budget):
                return

            frame_budget = pending if force else min(pending, max(1, budget))
            frame_chunk = pending_decode_frames[:frame_budget]
            del pending_decode_frames[:frame_budget]

            decoded = runtime.codec_streaming_session.run_frames(frame_chunk)
            if decoded is None:
                return

            audio, audio_length = decoded
            if audio_length <= 0:
                return

            if first_audio_emitted_at is None:
                first_audio_emitted_at = time.perf_counter()

            emitted_samples_total += audio_length
            merged = _merge_audio_channels([
                audio[0, ch, :audio_length] for ch in range(audio.shape[1])
            ])
            emitted_chunks_buffer.append(merged)

            # 🚀 实时推送：累积够目标时长就发
            total_emitted = sum(c.shape[0] for c in emitted_chunks_buffer)
            if total_emitted >= sr * TARGET_CHUNK_SEC or (force and emitted_chunks_buffer):
                combined = np.concatenate(emitted_chunks_buffer, axis=0)
                emitted_chunks_buffer.clear()
                cb(combined)

        def on_frame(_generated, _step_idx, frame):
            pending_decode_frames.append(list(frame))
            decode_pending(force=False)

        try:
            generated = runtime.generate_audio_frames(request_rows, on_frame=on_frame)
            total_generated_frames.extend(generated)

            # 解码剩余帧
            decode_pending(force=True)

            total_audio_samples += emitted_samples_total

            # 段间静音
            if chunk_idx < len(text_chunks) - 1:
                pause_sec = runtime.estimate_voice_clone_inter_chunk_pause_seconds(chunk_text)
                pause_samples = max(0, int(round(sr * pause_sec)))
                if pause_samples > 0:
                    silence = np.zeros((pause_samples, channels), dtype=np.float32)
                    cb(silence)
                    total_audio_samples += pause_samples

        finally:
            runtime.codec_streaming_session.reset()

    return {
        "total_frames": len(total_generated_frames),
        "total_audio_samples": total_audio_samples,
        "sample_rate": sr,
        "channels": channels,
    }


# ── WebSocket ─────────────────────────────────────────────────
@app.websocket("/tts")
async def websocket_tts(ws: WebSocket):
    await ws.accept()
    runtime = await get_runtime(
        _PROJECT_ROOT / "onnx_models",
        execution_provider="cpu",
    )
    logger.info("✅ 连接: %s", ws.client)

    loop = asyncio.get_event_loop()
    send_queue: asyncio.Queue[dict | None] = asyncio.Queue()

    async def _sender():
        """从 queue 取 audio chunk 并通过 ws 发送"""
        while True:
            item = await send_queue.get()
            if item is None:
                break
            await ws.send_text(item["json"])

    sender_task = asyncio.create_task(_sender())

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            text = data.get("text", "").strip()
            if not text:
                await ws.send_text(json.dumps({"type": "error", "message": "text 不能为空"}))
                continue

            voice = data.get("voice")
            sample_mode = data.get("sample_mode")
            do_sample = data.get("do_sample", True)
            max_new_frames = data.get("max_new_frames")
            voice_clone_max_text_tokens = data.get("voice_clone_max_text_tokens", 75)
            seed = data.get("seed")

            logger.info("合成: text_len=%d voice=%s", len(text), voice or "default")

            t0 = time.perf_counter()

            # 回调：在同步 on_frame 线程中被调用，塞入 async queue
            def make_cb():
                def _cb(wav: np.ndarray):
                    msg = _build_audio_chunk_msg(wav, 48000, False)
                    # 非阻塞放入 queue
                    send_queue.put_nowait({"json": msg})
                return _cb

            # 在后台线程执行同步推理
            result = await asyncio.to_thread(
                _run_streaming_synthesis,
                runtime,
                text,
                voice,
                sample_mode,
                do_sample,
                seed,
                max_new_frames,
                voice_clone_max_text_tokens,
                make_cb(),
            )

            elapsed = time.perf_counter() - t0
            audio_sec = result["total_audio_samples"] / result["sample_rate"]

            # 发送 final chunk（空标识）
            await ws.send_text(json.dumps({
                "type": "audio_chunk",
                "data": "",
                "sample_rate": result["sample_rate"],
                "channels": result["channels"],
                "is_final": True,
            }))

            await ws.send_text(json.dumps({
                "type": "done",
                "total_frames": result["total_frames"],
                "audio_duration_sec": round(audio_sec, 3),
                "inference_elapsed_sec": round(elapsed, 3),
            }))

            logger.info("✅ 完成: %.2fs 音频, 推理 %.2fs (%.2fx)", audio_sec, elapsed, audio_sec/elapsed if elapsed else 0)

    except WebSocketDisconnect:
        logger.info("❌ 断开: %s", ws.client)
    except Exception as e:
        logger.exception("出错")
        try:
            await ws.send_text(json.dumps({"type": "error", "message": str(e)}))
        except Exception:
            pass
    finally:
        await send_queue.put(None)
        await sender_task


# ── 启动 ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument("--model-dir", default=str(_PROJECT_ROOT / "onnx_models"))
    parser.add_argument("--execution-provider", default="cpu", choices=["cpu", "cuda"])
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    global _runtime
    _runtime = OnnxTtsRuntime(model_dir=args.model_dir, execution_provider=args.execution_provider)
    logger.info("模型已加载，内置 %d 种音色", len(_runtime.list_builtin_voices()))

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level.lower())


if __name__ == "__main__":
    main()
