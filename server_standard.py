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
DEFAULT_VOICE = "Yuewen"

# ── 自定义参考音频 ───────────────────────────────────────────────
# 使用 "Dabao" voice 时，用这个文件做实时编码克隆
DABAO_REFERENCE_AUDIO = str(_PROJECT_ROOT / "voice_samples" / "oknv_ref.wav")

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
    """懒加载全局运行时（仅 REST 端点使用，或退化保底）"""
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


# ── Runtime Pool for Parallel WebSocket Synthesis ─────────────
_RUNTIME_POOL: list[OnnxTtsRuntime] = []
_RUNTIME_POOL_SEM: asyncio.Semaphore | None = None
_RUNTIME_POOL_SIZE = 4  # 4 concurrent synthesis = 002 can finish before 001


async def init_runtime_pool():
    """Eagerly initialize the runtime pool at server startup."""
    global _RUNTIME_POOL, _RUNTIME_POOL_SEM
    if _RUNTIME_POOL:
        return
    _RUNTIME_POOL_SEM = asyncio.Semaphore(_RUNTIME_POOL_SIZE)
    model_dir = _PROJECT_ROOT / "onnx_models"
    logger.info(
        "Initializing runtime pool (%d instances)...", _RUNTIME_POOL_SIZE)
    for i in range(_RUNTIME_POOL_SIZE):
        rt = await asyncio.to_thread(
            OnnxTtsRuntime,
            model_dir=str(model_dir),
            execution_provider="cpu",
        )
        _RUNTIME_POOL.append(rt)
    logger.info(
        "Runtime pool ready. %d instances, %d voices available.",
        len(_RUNTIME_POOL),
        len(_RUNTIME_POOL[0].list_builtin_voices()),
    )

    # Also set _runtime for REST endpoints
    global _runtime
    _runtime = _RUNTIME_POOL[0]


async def acquire_runtime() -> OnnxTtsRuntime:
    """Acquire a runtime from the pool (async wait if all busy)."""
    await _RUNTIME_POOL_SEM.acquire()
    return _RUNTIME_POOL.pop()


def release_runtime(rt: OnnxTtsRuntime):
    """Return a runtime to the pool."""
    _RUNTIME_POOL.append(rt)
    _RUNTIME_POOL_SEM.release()


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
            "default": DEFAULT_VOICE,
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
    audio_temperature: float | None = None,
    audio_top_k: int | None = None,
    audio_top_p: float | None = None,
    audio_repetition_penalty: float | None = None,
    prompt_audio_path: str | None = None,
) -> dict:
    """
    在同步线程中执行流式合成。
    通过 cb_send_audio(pcm_bytes, sample_rate, channels, seq) 回调实时推送音频。
    通过 cancel_event 支持中断。
    prompt_audio_path: 如果传了，就实时编码这个文件作为参考音色，
                       否则用内置 voice 的预编码数据。
    """
    # 参数注入
    if max_new_frames is not None:
        runtime.manifest["generation_defaults"]["max_new_frames"] = int(max_new_frames)
    if sample_mode:
        runtime.manifest["generation_defaults"]["sample_mode"] = sample_mode
    runtime.manifest["generation_defaults"]["do_sample"] = do_sample
    if seed is not None:
        runtime.rng = np.random.default_rng(int(seed))
    if audio_temperature is not None:
        runtime.manifest["generation_defaults"]["audio_temperature"] = float(audio_temperature)
    if audio_top_k is not None:
        runtime.manifest["generation_defaults"]["audio_top_k"] = int(audio_top_k)
    if audio_top_p is not None:
        runtime.manifest["generation_defaults"]["audio_top_p"] = float(audio_top_p)
    if audio_repetition_penalty is not None:
        runtime.manifest["generation_defaults"]["audio_repetition_penalty"] = float(audio_repetition_penalty)

    if cancel_event.is_set():
        return {"cancelled": True, "total_frames": 0, "total_audio_samples": 0}

    # 准备文本
    try:
        prepared_texts = runtime.prepare_synthesis_text(
            text=text,
            voice=str(voice or ""),
            enable_wetext=False,
            enable_normalize_tts_text=False,
        )
        prepared_text = str(prepared_texts["text"])
    except Exception as e:
        raise RuntimeError(f"Text preparation failed: {e}") from e

    if cancel_event.is_set():
        return {"cancelled": True, "total_frames": 0, "total_audio_samples": 0}

    # 获取 prompt audio codes
    # 如果有 prompt_audio_path，实时编码参考音频；否则用内置 voice 的预编码数据
    prompt_audio_codes = runtime.resolve_prompt_audio_codes(
        voice=voice,
        prompt_audio_path=prompt_audio_path,
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
    """
    并行合成 WebSocket 端点。

    ── 核心设计 ──
    1. 主循环仅负责接收 WebSocket 消息，收到 synthesize 请求后立即创建
       asyncio.Task 并发执行，不 await 等待合成完成。
    2. 每个合成任务从全局 Runtime Pool 中获取一个独立的 OnnxTtsRuntime 实例，
       因此多个句子可以真正并行推理（短句先完成、先发 done）。
    3. 所有任务通过共享的 send_queue 发送音频/done/error 消息，
       sender 协程负责将消息写回 WebSocket。
    4. 客户端通过 RequestID 区分属于不同句子的音频包，
       Go 网关层的 Map 缓冲区自动完成乱序拼装。

    ── 关键好处 ──
    - 客户端可以 1 秒内推送整个批次的文本，服务端立即全部 RECV_TEXT
    - 短句（002）即使比长句（001）后收到，也可能先合成完成、先下发 done
    - 取消或断开时，所有正在合成的任务被优雅终止
    """
    await ws.accept()
    logger.info("WebSocket connected: %s", ws.client)

    send_queue: asyncio.Queue[str | None] = asyncio.Queue()
    running_tasks: list[asyncio.Task] = []

    # ── sender: 将队列消息写回 WebSocket ──────────────────────
    async def _sender():
        while True:
            msg = await send_queue.get()
            if msg is None:
                break
            try:
                await ws.send_text(msg)
            except Exception:
                break

    sender_task = asyncio.create_task(_sender())

    # ── 单个合成任务（并发执行） ──────────────────────────────
    async def _run_synthesis(data: dict):
        """Receive a request, acquire runtime, run synthesis, send results."""
        runtime: OnnxTtsRuntime | None = None
        request_id = ""
        try:
            # 1. Acquire a runtime from the pool (may wait if all busy)
            try:
                runtime = await acquire_runtime()
            except asyncio.CancelledError:
                return  # Task cancelled before getting runtime, nothing to clean

            request_id = data.get("request_id", str(uuid.uuid4()))
            text = data.get("text", "").strip()

            # ── 立即打印 RECV_TEXT 日志 ──────────────────────
            logger.info("RECV_TEXT: req=%s client=%s text=%r text_len=%d",
                        request_id, ws.client, text, len(text))

            # ── 校验 ──────────────────────────────────────────
            if not text:
                await send_queue.put(
                    _build_error_msg(request_id, "INVALID_TEXT",
                                     "text cannot be empty"))
                return

            if data.get("ssml", False):
                await send_queue.put(
                    _build_error_msg(request_id, "SSML_NOT_SUPPORTED",
                                     "This engine does not support SSML."))
                return

            fmt = data.get("format", {})
            target_encoding = fmt.get("encoding", "pcm_f32le")
            target_sample_rate = fmt.get("sample_rate", 48000)
            target_channels = fmt.get("channels", 2)
            if target_encoding not in ("pcm_f32le", "pcm_s16le"):
                await send_queue.put(
                    _build_error_msg(request_id, "INVALID_FORMAT",
                                     f"Unsupported encoding: {target_encoding}"))
                return

            # ── 参数解析 ──────────────────────────────────────
            voice = data.get("voice", DEFAULT_VOICE)
            opts = data.get("options", {})
            sample_mode = opts.get("sample_mode")
            do_sample = opts.get("do_sample", True)
            seed = opts.get("seed")
            max_new_frames = opts.get("max_new_frames")
            voice_clone_max_text_tokens = opts.get(
                "voice_clone_max_text_tokens", 75)
            audio_temperature = opts.get("audio_temperature", 0.78)
            audio_top_k = opts.get("audio_top_k", 22)
            audio_top_p = opts.get("audio_top_p", 0.92)
            audio_repetition_penalty = opts.get("audio_repetition_penalty", 1.2)

            prompt_audio_path = None
            if voice == "Dabao":
                prompt_audio_path = DABAO_REFERENCE_AUDIO
                logger.info(
                    "Using custom reference audio for Dabao: %s",
                    prompt_audio_path)

            # ── 取消事件 ──────────────────────────────────────
            cancel_event = threading.Event()
            with _active_requests_lock:
                _active_requests[request_id] = cancel_event

            logger.info(
                "Synthesize: req=%s client=%s text=%r text_len=%d voice=%s params=%s",
                request_id, ws.client, text, len(text), voice or "default",
                json.dumps(data.get("options", {}), ensure_ascii=False),
            )

            t0 = time.perf_counter()

            # ── 音频回调（每个任务独立的闭包，携带自己的 request_id） ──
            def _on_audio(pcm: bytes, sr: int, ch: int, seq: int):
                msg = _build_audio_msg(
                    request_id, seq, pcm, sr, target_encoding, ch, False,
                )
                send_queue.put_nowait(msg)

            # ── 在独立线程中执行流式合成 ──────────────────────
            try:
                result = await asyncio.to_thread(
                    _run_streaming_synthesis,
                    runtime, text, voice,
                    sample_mode, do_sample, seed,
                    max_new_frames, voice_clone_max_text_tokens,
                    target_encoding, target_sample_rate, target_channels,
                    cancel_event, _on_audio,
                    audio_temperature, audio_top_k, audio_top_p,
                    audio_repetition_penalty,
                    prompt_audio_path,
                )
            except asyncio.CancelledError:
                cancel_event.set()
                raise

            elapsed = time.perf_counter() - t0

            if not result.get("cancelled", False):
                # final chunk (zero-length audio with is_final=True)
                await send_queue.put(
                    _build_audio_msg(request_id, 0, b"", target_sample_rate,
                                     target_encoding, target_channels, True))
                # done signal
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

        except asyncio.CancelledError:
            pass  # Task was cancelled gracefully
        except RuntimeError as e:
            if request_id:
                await send_queue.put(
                    _build_error_msg(request_id, "ENGINE_ERROR", str(e)))
                logger.exception("Engine error for req=%s", request_id)
        except Exception as e:
            if request_id:
                await send_queue.put(
                    _build_error_msg(request_id, "ENGINE_ERROR", str(e)))
                logger.exception("Unexpected error for req=%s", request_id)
        finally:
            if request_id:
                with _active_requests_lock:
                    _active_requests.pop(request_id, None)
            if runtime:
                release_runtime(runtime)

    # ── 主循环：仅负责接收 WebSocket 消息，不做合成 ──────────
    try:
        while True:
            try:
                raw = await ws.receive_text()
            except Exception:
                break

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

            # ── synthesize: 创建并发任务，不 await！───────────
            if msg_type != "synthesize":
                await send_queue.put(
                    _build_error_msg(data.get("request_id", ""),
                                     "INVALID_REQUEST",
                                     f"Unknown message type: {msg_type}"))
                continue

            # 🚀 每来一个合成请求，立即创建异步任务并发执行
            task = asyncio.create_task(_run_synthesis(data))
            running_tasks.append(task)
            # 清理已完成的任务，防止内存泄漏
            running_tasks[:] = [t for t in running_tasks if not t.done()]

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
        # 取消所有正在运行的任务
        with _active_requests_lock:
            for ev in _active_requests.values():
                ev.set()
            _active_requests.clear()
        for task in running_tasks:
            task.cancel()
        await asyncio.gather(*running_tasks, return_exceptions=True)
        await send_queue.put(None)
        await sender_task

@app.on_event("startup")
async def _startup_init_pool():
    """Initialize the runtime pool on FastAPI server start."""
    await init_runtime_pool()


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

    logger.info("Pre-loading runtime pool from %s ...", args.model_dir)
    # _PROJECT_ROOT is module-level, no global needed
    global _RUNTIME_POOL_SIZE
    # Override pool size from cmdline if needed (default 2)
    asyncio.run(init_runtime_pool())
    voices = _RUNTIME_POOL[0].list_builtin_voices()
    logger.info("Runtime pool ready. %d instances, %d voices available.",
                len(_RUNTIME_POOL), len(voices))
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
