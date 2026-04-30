"""
Unified TTS WebSocket Server — Standard Implementation (MOSS-TTS Engine)
=========================================================================
Reference implementation of api.md v0.1.0-draft.

REST Endpoints:
  GET /api/v1/info     — Engine capabilities discovery
  GET /api/v1/voices   — List available voices

WebSocket Endpoint:
  ws://host:port/api/v1/synthesize

Features:
  ✅ Streaming synthesis via real-time frame decoding
  ✅ Audio format negotiation (pcm_f32le, pcm_s16le)
  ✅ Request ID correlation for multi-request connections
  ✅ Cancel/interrupt support
  ✅ Engine-specific options via options bag
  ✅ SSML detection (graceful SSML_NOT_SUPPORTED error)
  ✅ Standard error codes
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import signal
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ── MOSS-TTS 路径注入 ─────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent
_MOSS_TTS_DIR = _PROJECT_ROOT.parent / "MOSS-TTS-Nano"
if _MOSS_TTS_DIR.exists():
    sys.path.insert(0, str(_MOSS_TTS_DIR))

from onnx_tts_runtime import OnnxTtsRuntime
from onnx_tts_runtime import _merge_audio_channels, _resolve_stream_decode_frame_budget

# ── 全局配置 ──────────────────────────────────────────────────────
API_PREFIX = "/api/v1"
TARGET_CHUNK_SEC = 0.30       # 每个 audio chunk 的目标时长（秒）
ENGINE_NAME = "moss-tts"
ENGINE_VERSION = "0.3.0"

logger = logging.getLogger("tts-standard")

# ── 全局状态 ──────────────────────────────────────────────────────
_runtime: OnnxTtsRuntime | None = None
_runtime_lock = asyncio.Lock()

# 活跃请求追踪：request_id → threading.Event（用于取消）
_active_requests: dict[str, threading.Event] = {}
_active_requests_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════
#  音频格式工具
# ═══════════════════════════════════════════════════════════════════

def _reformat_audio(
    waveform: np.ndarray,
    target_encoding: str,
    target_sample_rate: int,
    target_channels: int,
    native_sr: int,
    native_channels: int,
) -> tuple[bytes, int, int]:
    """
    将波形按目标格式重编码。
    返回 (pcm_bytes, actual_sample_rate, actual_channels)。
    """
    # 声道转换
    if target_channels == 1 and native_channels == 2:
        waveform = np.mean(waveform, axis=1, keepdims=False)
        actual_channels = 1
    elif target_channels == 2 and native_channels == 1:
        waveform = np.column_stack([waveform, waveform])
        actual_channels = 2
    else:
        actual_channels = native_channels

    # 采样率转换（线性插值）
    actual_sr = native_sr
    if target_sample_rate != native_sr:
        ratio = target_sample_rate / native_sr
        old_len = waveform.shape[0]
        new_len = int(round(old_len * ratio))
        old_indices = np.arange(new_len) / ratio
        floor = np.floor(old_indices).astype(np.int64)
        ceil = np.minimum(floor + 1, old_len - 1)
        frac = old_indices - floor
        if waveform.ndim == 1:
            waveform = waveform[floor] * (1 - frac) + waveform[ceil] * frac
        else:
            waveform = (waveform[floor] * (1 - frac[:, np.newaxis]) +
                         waveform[ceil] * frac[:, np.newaxis])
        actual_sr = target_sample_rate

    # 编码转换
    if target_encoding == "pcm_s16le":
        pcm = (np.clip(waveform, -1.0, 1.0) * 32767).astype(np.int16).tobytes()
    else:
        pcm = waveform.astype(np.float32).tobytes()

    return pcm, actual_sr, actual_channels


# ═══════════════════════════════════════════════════════════════════
#  引擎生命周期
# ═══════════════════════════════════════════════════════════════════

async def get_runtime(
    model_dir: str | Path,
    execution_provider: str = "cpu",
) -> OnnxTtsRuntime:
    """懒加载全局运行时"""
    global _runtime
    if _runtime is None:
        async with _runtime_lock:
            if _runtime is None:
                logger.info("Loading ONNX TTS Runtime from %s ...", model_dir)
                _runtime = await asyncio.to_thread(
                    OnnxTtsRuntime,
                    model_dir=str(model_dir),
                    execution_provider=execution_provider,
                )
                logger.info("Runtime loaded. %d voices available.",
                            len(_runtime.list_builtin_voices()))
    return _runtime


def _get_voices_list() -> list[dict]:
    """安全获取音色列表"""
    if _runtime is None:
        return []
    return _runtime.list_builtin_voices()


# ═══════════════════════════════════════════════════════════════════
#  FastAPI 应用
# ═══════════════════════════════════════════════════════════════════

app = FastAPI(title=f"Unified TTS Server ({ENGINE_NAME})", version=ENGINE_VERSION)


# ── REST 端点 ─────────────────────────────────────────────────────

@app.get(f"{API_PREFIX}/info")
async def api_info():
    """引擎能力发现"""
    voices = _get_voices_list()
    default_voice = voices[0]["voice"] if voices else "Junhao"
    return {
        "engine": {
            "name": ENGINE_NAME,
            "version": ENGINE_VERSION,
            "description": "MOSS-TTS-Nano real-time streaming TTS engine",
        },
        "endpoints": {
            "websocket": f"/{API_PREFIX}/synthesize",
            "voices": f"/{API_PREFIX}/voices",
        },
        "capabilities": {
            "streaming": True,
            "interruptible": True,
            "ssml": False,
            "speed_control": False,
            "pitch_control": False,
            "emotion_control": False,
            "multi_request_per_connection": True,
            "max_text_length": 5000,
        },
        "voices": {
            "type": "predefined",
            "dynamic": False,
            "default": default_voice,
        },
        "formats": {
            "sample_rates": [48000],
            "encodings": ["pcm_f32le", "pcm_s16le"],
            "channels": [1, 2],
            "container": "raw",
        },
        "parameters": {
            "seed": {
                "type": "integer",
                "description": "Random seed for reproducible generation",
                "optional": True,
                "default": None,
            },
        },
        "options": {
            "sample_mode": {
                "type": "string",
                "description": "Sampling mode for audio token generation",
                "values": ["fixed", "greedy", "full"],
                "optional": True,
                "default": "fixed",
            },
            "do_sample": {
                "type": "boolean",
                "description": "Whether to use stochastic sampling",
                "optional": True,
                "default": True,
            },
            "max_new_frames": {
                "type": "integer",
                "description": "Maximum number of new audio frames to generate",
                "optional": True,
                "default": None,
            },
            "voice_clone_max_text_tokens": {
                "type": "integer",
                "description": "Max text tokens per chunk for voice cloning",
                "optional": True,
                "default": 75,
                "min": 10,
                "max": 200,
            },
        },
    }


@app.get(f"{API_PREFIX}/voices")
async def api_voices():
    """可用音色列表"""
    voices = _get_voices_list()
    return {
        "voices": [
            {
                "id": v["voice"],
                "display_name": v["display_name"],
                "gender": v.get("gender", "unknown"),
                "language": ["zh", "en"],
                "group": v.get("group", "builtin"),
                "description": f"{v['display_name']} ({v.get('group', 'builtin')})",
                "ssml": False,
            }
            for v in voices
        ],
    }


# ── WebSocket: 消息构建 ──────────────────────────────────────────

def _build_audio_msg(
    request_id: str,
    seq: int,
    pcm_bytes: bytes,
    sample_rate: int,
    encoding: str,
    channels: int,
    is_final: bool,
) -> str:
    b64 = base64.b64encode(pcm_bytes).decode("ascii")
    return json.dumps({
        "type": "audio",
        "request_id": request_id,
        "seq": seq,
        "data": b64,
        "sample_rate": sample_rate,
        "encoding": encoding,
        "channels": channels,
        "is_final": is_final,
    })


def _build_done_msg(
    request_id: str,
    total_frames: int,
    audio_duration_sec: float,
    inference_elapsed_sec: float,
    text_was_ssml: bool = False,
) -> str:
    return json.dumps({
        "type": "done",
        "request_id": request_id,
        "engine": ENGINE_NAME,
        "total_audio_frames": total_frames,
        "audio_duration_sec": round(audio_duration_sec, 3),
        "inference_elapsed_sec": round(inference_elapsed_sec, 3),
        "text_was_ssml": text_was_ssml,
    })


def _build_error_msg(
    request_id: str,
    code: str,
    message: str,
    fatal: bool = False,
) -> str:
    return json.dumps({
        "type": "error",
        "request_id": request_id,
        "code": code,
        "message": message,
        "fatal": fatal,
    })


# ── 流式合成核心 ──────────────────────────────────────────────────

def _run_streaming_synthesis(
    runtime: OnnxTtsRuntime,
    text: str,
    voice: str | None,
    sample_mode: str | None,
    do_sample: bool,
    seed: int | None,
    max_new_frames: int | None,
    voice_clone_max_text_tokens: int,
    target_encoding: str,
    target_sample_rate: int,
    target_channels: int,
    cancel_event: threading.Event,
    cb_send_audio: callable,
) -> dict:
    """
    在同步线程中执行流式合成。
    通过 cb_send_audio(pcm_bytes, sample_rate, channels, seq) 回调实时推送音频。
    通过 cancel_event 支持中断。
    """
    # 参数注入
    if max_new_frames is not None:
        runtime.manifest["generation_defaults"]["max_new_frames"] = int(max_new_frames)
    if sample_mode:
        runtime.manifest["generation_defaults"]["sample_mode"] = sample_mode
    runtime.manifest["generation_defaults"]["do_sample"] = do_sample
    if seed is not None:
        runtime.rng = np.random.default_rng(int(seed))

    if cancel_event.is_set():
        return {"cancelled": True, "total_frames": 0, "total_audio_samples": 0}

    # 准备文本
    try:
        prepared_texts = runtime.prepare_synthesis_text(
            text=text,
            voice=str(voice or ""),
            enable_wetext=True,
            enable_normalize_tts_text=True,
        )
        prepared_text = str(prepared_texts["text"])
    except Exception as e:
        raise RuntimeError(f"Text preparation failed: {e}") from e

    if cancel_event.is_set():
        return {"cancelled": True, "total_frames": 0, "total_audio_samples": 0}

    # 获取 prompt audio codes
    prompt_audio_codes = runtime.resolve_prompt_audio_codes(
        voice=voice, prompt_audio_path=None,
    )

    # 按句分割
    text_chunks = runtime.split_voice_clone_text(
        prepared_text, max_tokens=int(voice_clone_max_text_tokens),
    )
    logger.debug("Text split into %d chunks", len(text_chunks))

    total_generated_frames: list[list[int]] = []
    total_audio_samples = 0
    native_sr = int(runtime.codec_meta["codec_config"]["sample_rate"])
    native_channels = int(runtime.codec_meta["codec_config"]["channels"])
    seq = 0

    for chunk_idx, chunk_text in enumerate(text_chunks):
        if cancel_event.is_set():
            logger.debug("Cancelled during chunk %d", chunk_idx)
            break

        text_token_ids = runtime.encode_text(chunk_text)
        request_rows = runtime.build_voice_clone_request_rows(
            prompt_audio_codes, text_token_ids,
        )

        pending_decode_frames: list[list[int]] = []
        emitted_chunks_buffer: list[np.ndarray] = []
        emitted_samples_total = 0
        first_audio_emitted_at: float | None = None
        runtime.codec_streaming_session.reset()

        def decode_pending(force: bool):
            nonlocal emitted_samples_total, first_audio_emitted_at, seq
            pending = len(pending_decode_frames)
            if pending <= 0:
                return
            budget = 0
            if not force:
                budget = _resolve_stream_decode_frame_budget(
                    emitted_samples_total, native_sr, first_audio_emitted_at,
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

            total_emitted = sum(c.shape[0] for c in emitted_chunks_buffer)
            if total_emitted >= native_sr * TARGET_CHUNK_SEC or (force and emitted_chunks_buffer):
                combined = np.concatenate(emitted_chunks_buffer, axis=0)
                emitted_chunks_buffer.clear()
                pcm, actual_sr, actual_ch = _reformat_audio(
                    combined, target_encoding, target_sample_rate,
                    target_channels, native_sr, native_channels,
                )
                cb_send_audio(pcm, actual_sr, actual_ch, seq)
                seq += 1

        def on_frame(_generated, _step_idx, frame):
            pending_decode_frames.append(list(frame))
            decode_pending(force=False)

        try:
            generated = runtime.generate_audio_frames(request_rows, on_frame=on_frame)
            if cancel_event.is_set():
                break
            total_generated_frames.extend(generated)
            decode_pending(force=True)
            total_audio_samples += emitted_samples_total

            # 段间静音
            if chunk_idx < len(text_chunks) - 1 and not cancel_event.is_set():
                pause_sec = runtime.estimate_voice_clone_inter_chunk_pause_seconds(chunk_text)
                pause_samples = max(0, int(round(native_sr * pause_sec)))
                if pause_samples > 0:
                    silence = np.zeros((pause_samples, native_channels), dtype=np.float32)
                    pcm, actual_sr, actual_ch = _reformat_audio(
                        silence, target_encoding, target_sample_rate,
                        target_channels, native_sr, native_channels,
                    )
                    cb_send_audio(pcm, actual_sr, actual_ch, seq)
                    seq += 1
                    total_audio_samples += pause_samples
        finally:
            runtime.codec_streaming_session.reset()

    return {
        "cancelled": cancel_event.is_set(),
        "total_frames": len(total_generated_frames),
        "total_audio_samples": total_audio_samples,
        "sample_rate": native_sr,
        "channels": native_channels,
    }


# ── WebSocket 端点 ───────────────────────────────────────────────

@app.websocket(f"{API_PREFIX}/synthesize")
async def websocket_synthesize(ws: WebSocket):
    await ws.accept()
    runtime = await get_runtime(
        _PROJECT_ROOT / "onnx_models",
        execution_provider="cpu",
    )
    logger.info("WebSocket connected: %s", ws.client)

    send_queue: asyncio.Queue[str | None] = asyncio.Queue()

    async def _sender():
        while True:
            msg = await send_queue.get()
            if msg is None:
                break
            await ws.send_text(msg)

    sender_task = asyncio.create_task(_sender())

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type", "")

            # ── cancel ─────────────────────────────────────────
            if msg_type == "cancel":
                req_id = data.get("request_id", "")
                if not req_id:
                    await send_queue.put(
                        _build_error_msg("", "INVALID_REQUEST",
                                         "cancel requires request_id"))
                    continue
                with _active_requests_lock:
                    cancel_ev = _active_requests.get(req_id)
                    if cancel_ev:
                        cancel_ev.set()
                        logger.info("Cancelled request: %s", req_id)
                await send_queue.put(
                    _build_error_msg(req_id, "CANCELLED",
                                     "Request cancelled by client"))
                continue

            # ── ping/pong ──────────────────────────────────────
            if msg_type == "ping":
                await send_queue.put(json.dumps({
                    "type": "pong",
                    "request_id": data.get("request_id", ""),
                }))
                continue

            # ── synthesize ─────────────────────────────────────
            if msg_type != "synthesize":
                await send_queue.put(
                    _build_error_msg(data.get("request_id", ""),
                                     "INVALID_REQUEST",
                                     f"Unknown message type: {msg_type}"))
                continue

            # ── 字段校验 ──────────────────────────────────────
            request_id = data.get("request_id", str(uuid.uuid4()))
            text = data.get("text", "").strip()
            if not text:
                await send_queue.put(
                    _build_error_msg(request_id, "INVALID_TEXT",
                                     "text cannot be empty"))
                continue

            # ── SSML 检查 ─────────────────────────────────────
            if data.get("ssml", False):
                await send_queue.put(
                    _build_error_msg(request_id, "SSML_NOT_SUPPORTED",
                                     "This engine does not support SSML. "
                                     "Set ssml=false or use a different engine. "
                                     "See /api/v1/info for capabilities."))
                continue

            # ── 格式协商 ──────────────────────────────────────
            fmt = data.get("format", {})
            target_encoding = fmt.get("encoding", "pcm_f32le")
            target_sample_rate = fmt.get("sample_rate", 48000)
            target_channels = fmt.get("channels", 2)
            if target_encoding not in ("pcm_f32le", "pcm_s16le"):
                await send_queue.put(
                    _build_error_msg(request_id, "INVALID_FORMAT",
                                     f"Unsupported encoding: {target_encoding}. "
                                     f"Supported: pcm_f32le, pcm_s16le"))
                continue

            # ── 参数解析 ──────────────────────────────────────
            voice = data.get("voice")
            opts = data.get("options", {})
            sample_mode = opts.get("sample_mode")
            do_sample = opts.get("do_sample", True)
            seed = opts.get("seed")
            max_new_frames = opts.get("max_new_frames")
            voice_clone_max_text_tokens = opts.get("voice_clone_max_text_tokens", 75)

            # ── 注册取消事件 ─────────────────────────────────
            cancel_event = threading.Event()
            with _active_requests_lock:
                _active_requests[request_id] = cancel_event

            logger.info(
                "Synthesize: req=%s text_len=%d voice=%s fmt=%s/%dch/%dHz",
                request_id, len(text), voice or "default",
                target_encoding, target_channels, target_sample_rate,
            )

            t0 = time.perf_counter()

            def _on_audio(pcm: bytes, sr: int, ch: int, seq: int):
                msg = _build_audio_msg(
                    request_id, seq, pcm, sr, target_encoding, ch, False,
                )
                send_queue.put_nowait(msg)

            try:
                result = await asyncio.to_thread(
                    _run_streaming_synthesis,
                    runtime, text, voice,
                    sample_mode, do_sample, seed,
                    max_new_frames, voice_clone_max_text_tokens,
                    target_encoding, target_sample_rate, target_channels,
                    cancel_event, _on_audio,
                )

                elapsed = time.perf_counter() - t0

                if not result.get("cancelled", False):
                    # final chunk
                    await send_queue.put(
                        _build_audio_msg(request_id, 0, b"", target_sample_rate,
                                          target_encoding, target_channels, True))
                    # done
                    sr = result["sample_rate"]
                    audio_sec = result["total_audio_samples"] / sr if sr else 0
                    await send_queue.put(
                        _build_done_msg(request_id, result["total_frames"],
                                        audio_sec, elapsed))
                    logger.info(
                        "Done: req=%s %.2fs audio, %.2fs inference (%.2fx)",
                        request_id, audio_sec, elapsed,
                        audio_sec / elapsed if elapsed else 0,
                    )
            except RuntimeError as e:
                await send_queue.put(
                    _build_error_msg(request_id, "ENGINE_ERROR", str(e)))
                logger.exception("Engine error for req=%s", request_id)
            except Exception as e:
                await send_queue.put(
                    _build_error_msg(request_id, "ENGINE_ERROR", str(e)))
                logger.exception("Unexpected error for req=%s", request_id)
            finally:
                with _active_requests_lock:
                    _active_requests.pop(request_id, None)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected: %s", ws.client)
        with _active_requests_lock:
            for ev in _active_requests.values():
                ev.set()
            _active_requests.clear()
    except json.JSONDecodeError:
        try:
            await ws.send_text(
                _build_error_msg("", "INVALID_REQUEST", "Invalid JSON"))
        except Exception:
            pass
    except Exception as e:
        logger.exception("WebSocket error")
        try:
            await ws.send_text(
                _build_error_msg("", "ENGINE_ERROR", str(e), fatal=True))
        except Exception:
            pass
    finally:
        await send_queue.put(None)
        await sender_task


# ═══════════════════════════════════════════════════════════════════
#  启动入口
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description=f"Unified TTS Server ({ENGINE_NAME})")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--model-dir", default=str(_PROJECT_ROOT / "onnx_models"))
    parser.add_argument("--execution-provider", default="cpu", choices=["cpu", "cuda"])
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Pre-loading model from %s ...", args.model_dir)
    global _runtime
    _runtime = OnnxTtsRuntime(
        model_dir=args.model_dir,
        execution_provider=args.execution_provider,
    )
    voices = _runtime.list_builtin_voices()
    logger.info("Model loaded. %d voices available.", len(voices))
    for v in voices:
        logger.info("  Voice: %s | %s | %s", v["voice"], v["display_name"], v["group"])

    logger.info("Starting server on %s:%d", args.host, args.port)
    logger.info("REST:  http://%s:%d%s/info", args.host, args.port, API_PREFIX)
    logger.info("WS:    ws://%s:%d%s/synthesize", args.host, args.port, API_PREFIX)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()
