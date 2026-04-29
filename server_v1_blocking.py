"""
MOSS-TTS WebSocket Server
=========================
流式语音合成服务，支持逐段发送文本、逐段推送音频 chunk。
每段文本可独立指定音色、采样模式等参数。

协议（JSON over WebSocket）：
  客户端 -> 服务端：
    {
      "text": "要合成的文本",
      "voice": "Junhao",           // 可选，音色名
      "sample_mode": "fixed",      // 可选：fixed / greedy / full
      "do_sample": true,           // 可选
      "streaming": true,           // 是否流式返回音频 chunk
      "voice_clone_max_text_tokens": 75
    }

  服务端 -> 客户端（流式）：
    {"type": "audio_chunk", "data": "<base64_pcm_data>", "sample_rate": 48000, "channels": 2, "is_final": false}
    {"type": "audio_chunk", ..., "is_final": true}
    {"type": "done", "total_frames": 87, "audio_duration_sec": 3.5}
  或错误：
    {"type": "error", "message": "..."}
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
from typing import Any

import numpy as np
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# 将 MOSS-TTS-Nano 项目加入路径
_PROJECT_ROOT = Path(__file__).resolve().parent
_MOSS_TTS_DIR = _PROJECT_ROOT.parent / "MOSS-TTS-Nano"
if _MOSS_TTS_DIR.exists():
    sys.path.insert(0, str(_MOSS_TTS_DIR))
else:
    pass

from onnx_tts_runtime import OnnxTtsRuntime

logger = logging.getLogger("moss-tts-ws")

# ── 配置 ──────────────────────────────────────────────────────
# 每帧音频帧对应的样本数（从 codec 配置读取，fallback=320）
FRAME_SAMPLES = 320
# WebSocket 单条消息的最大 payload（字节），超过则自动分片
MAX_PCM_BYTES_PER_MSG = 196_608  # ~0.75MB base64 → ~0.56MB PCM ≈ 1.7 秒音频

# ── 全局运行时（只加载一次模型） ─────────────────────────────────
_runtime: OnnxTtsRuntime | None = None
_runtime_lock = asyncio.Lock()


async def get_runtime(model_dir: str | Path, execution_provider: str = "cpu") -> OnnxTtsRuntime:
    global _runtime
    if _runtime is None:
        async with _runtime_lock:
            if _runtime is None:
                logger.info("正在加载 ONNX TTS Runtime（模型目录：%s）...", model_dir)
                t0 = time.perf_counter()
                _runtime = await asyncio.to_thread(
                    OnnxTtsRuntime,
                    model_dir=str(model_dir),
                    execution_provider=execution_provider,
                )
                logger.info("模型加载完成，耗时 %.1f 秒", time.perf_counter() - t0)
    return _runtime


# ── FastAPI 应用 ────────────────────────────────────────────────
app = FastAPI(title="MOSS-TTS WebSocket Server", version="0.1.0")


@app.get("/")
async def root():
    if _runtime is None:
        return {"service": "MOSS-TTS WebSocket Server", "status": "loading"}
    voices = _runtime.list_builtin_voices()
    return {
        "service": "MOSS-TTS WebSocket Server",
        "status": "ok",
        "builtin_voices": [
            {"voice": v["voice"], "display_name": v["display_name"], "group": v["group"]}
            for v in voices
        ],
    }


@app.get("/voices")
async def list_voices():
    if _runtime is None:
        return {"error": "runtime not loaded"}
    voices = _runtime.list_builtin_voices()
    return {"voices": [
        {"voice": v["voice"], "display_name": v["display_name"], "group": v["group"]}
        for v in voices
    ]}


def _split_waveform_to_chunks(
    waveform: np.ndarray,
    max_pcm_bytes: int,
) -> list[np.ndarray]:
    """将波形按最大字节数切分成多个 chunk"""
    n_samples = waveform.shape[0]
    n_channels = waveform.shape[1] if waveform.ndim > 1 else 1
    bytes_per_sample = 4  # float32
    max_samples = max_pcm_bytes // (bytes_per_sample * n_channels)
    # 对齐到帧边界（每个音频帧 FRAME_SAMPLES 个样本）
    max_samples = max(FRAME_SAMPLES, (max_samples // FRAME_SAMPLES) * FRAME_SAMPLES)

    chunks: list[np.ndarray] = []
    start = 0
    while start < n_samples:
        end = min(start + max_samples, n_samples)
        chunks.append(waveform[start:end])
        start = end
    return chunks


# ── WebSocket 端点 ─────────────────────────────────────────────
@app.websocket("/tts")
async def websocket_tts(ws: WebSocket):
    await ws.accept()
    runtime = await get_runtime(
        model_dir=_PROJECT_ROOT / "onnx_models",
        execution_provider="cpu",
    )
    logger.info("WebSocket 已连接: %s", ws.client)

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
            streaming = data.get("streaming", True)
            max_new_frames = data.get("max_new_frames")
            voice_clone_max_text_tokens = data.get("voice_clone_max_text_tokens", 75)
            seed = data.get("seed")

            logger.info(
                "合成请求: text_len=%d voice=%s streaming=%s",
                len(text), voice or "default", streaming,
            )

            # 执行合成（在异步线程池中运行）
            t0 = time.perf_counter()
            result: dict[str, Any] = await asyncio.to_thread(
                runtime.synthesize,
                text=text,
                voice=voice,
                sample_mode=sample_mode,
                do_sample=do_sample,
                streaming=streaming,
                max_new_frames=max_new_frames,
                voice_clone_max_text_tokens=voice_clone_max_text_tokens,
                seed=seed,
            )
            elapsed = time.perf_counter() - t0

            waveform: np.ndarray = result["waveform"]
            sr: int = result["sample_rate"]
            total_frames: int = len(result["audio_token_ids"])
            audio_duration = waveform.shape[0] / sr

            logger.info("合成耗时 %.2f 秒，音频 %.2f 秒（%.2fx 实时率）", elapsed, audio_duration, audio_duration / elapsed if elapsed else 0)

            # 将波形切片并逐块发送
            chunks_to_send = _split_waveform_to_chunks(waveform, MAX_PCM_BYTES_PER_MSG)
            n_channels = waveform.shape[1] if waveform.ndim > 1 else 1

            for chunk_idx, chunk_wav in enumerate(chunks_to_send):
                pcm_bytes = chunk_wav.astype(np.float32).tobytes()
                b64data = base64.b64encode(pcm_bytes).decode("ascii")
                is_last = (chunk_idx == len(chunks_to_send) - 1)

                await ws.send_text(json.dumps({
                    "type": "audio_chunk",
                    "data": b64data,
                    "sample_rate": sr,
                    "channels": n_channels,
                    "is_final": is_last,
                    "chunk_index": chunk_idx,
                }))

            # 发送完成消息
            await ws.send_text(json.dumps({
                "type": "done",
                "total_frames": int(total_frames),
                "audio_duration_sec": round(float(audio_duration), 3),
                "inference_elapsed_sec": round(float(elapsed), 3),
            }))

    except WebSocketDisconnect:
        logger.info("WebSocket 已断开: %s", ws.client)
    except json.JSONDecodeError:
        try:
            await ws.send_text(json.dumps({"type": "error", "message": "无效的 JSON 格式"}))
        except Exception:
            pass
    except Exception as e:
        logger.exception("处理请求时出错")
        try:
            await ws.send_text(json.dumps({"type": "error", "message": str(e)}))
        except Exception:
            pass


# ── 启动入口 ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="MOSS-TTS WebSocket Server")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser.add_argument("--port", type=int, default=8767, help="监听端口")
    parser.add_argument("--model-dir", default=str(_PROJECT_ROOT / "onnx_models"), help="ONNX 模型目录")
    parser.add_argument("--execution-provider", default="cpu", choices=["cpu", "cuda"], help="推理后端")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 预加载模型
    logger.info("预加载模型中...")
    global _runtime
    _runtime = OnnxTtsRuntime(
        model_dir=args.model_dir,
        execution_provider=args.execution_provider,
    )
    voices = _runtime.list_builtin_voices()
    logger.info("模型已加载，内置 %d 种音色", len(voices))

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()
