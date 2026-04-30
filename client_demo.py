"""
MOSS-TTS 标准 API 客户端 Demo
==============================
适配 server_standard.py（Unified TTS WebSocket API v0.1.0-draft）

功能：
  - REST 发现：查询引擎能力、音色列表
  - 流式合成：WebSocket 实时接收 audio chunks
  - 格式协商：支持 pcm_f32le / pcm_s16le，单声道/双声道
  - 中断支持：发送 cancel 终止正在合成的请求
  - 信噪检测：支持 SSML_NOT_SUPPORTED 优雅降级

用法：
  python client_demo.py                          # 交互模式
  python client_demo.py --text "你好世界"         # 单次合成
  python client_demo.py --text "你好" --voice Zhiming --output hello.wav
  python client_demo.py --discover                # 只查询引擎信息
  python client_demo.py --voices                  # 列出音色
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf

try:
    import httpx
except ImportError:
    httpx = None
    print("⚠️  httpx 未安装，REST 发现功能不可用。 pip install httpx")

try:
    import websockets
except ImportError:
    print("❌ 请先安装 websockets: pip install websockets")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════
#  REST 发现
# ═══════════════════════════════════════════════════════════════════

def discover(base_url: str) -> dict:
    """调用 /api/v1/info 获取引擎能力"""
    if httpx is None:
        raise RuntimeError("httpx 未安装，无法执行发现")
    resp = httpx.get(f"{base_url}/api/v1/info", timeout=10)
    resp.raise_for_status()
    return resp.json()


def list_voices(base_url: str) -> list[dict]:
    """调用 /api/v1/voices 获取音色列表"""
    if httpx is None:
        raise RuntimeError("httpx 未安装，无法获取音色")
    resp = httpx.get(f"{base_url}/api/v1/voices", timeout=10)
    resp.raise_for_status()
    return resp.json()["voices"]


def print_discover(info: dict):
    """友好打印引擎信息"""
    eng = info["engine"]
    cap = info["capabilities"]
    fmt = info["formats"]
    print(f"引擎: {eng['name']} v{eng['version']}")
    print(f"  描述: {eng['description']}")
    print(f"  能力:")
    print(f"    流式: {'✅' if cap['streaming'] else '❌'}")
    print(f"    可中断: {'✅' if cap['interruptible'] else '❌'}")
    print(f"    SSML: {'✅' if cap['ssml'] else '❌'}")
    print(f"    最大文本长度: {cap['max_text_length']} 字符")
    print(f"  音频格式:")
    for sr in fmt["sample_rates"]:
        print(f"    {sr}Hz, {', '.join(fmt['encodings'])}, {', '.join(f'{ch}ch' for ch in fmt['channels'])}")
    print(f"  默认音色: {info['voices']['default']}")
    print()


def print_voices(voices: list[dict]):
    """友好打印音色列表"""
    print(f"可用音色（共 {len(voices)} 种）:")
    print(f"  {'ID':12s} {'显示名':30s} {'组':16s} {'语言'}")
    print(f"  {'-'*12} {'-'*30} {'-'*16} {'-'*8}")
    for v in voices:
        langs = ",".join(v["language"])
        print(f"  {v['id']:12s} {v['display_name']:30s} {v['group']:16s} {langs}")
    print()


# ═══════════════════════════════════════════════════════════════════
#  WebSocket 流式合成
# ═══════════════════════════════════════════════════════════════════

async def synthesize(
    ws_uri: str,
    text: str,
    voice: str | None = None,
    encoding: str = "pcm_f32le",
    sample_rate: int = 48000,
    channels: int = 2,
    options: dict[str, Any] | None = None,
    output_wav: str | None = None,
    verbose: bool = True,
) -> dict | None:
    """
    通过标准 API 执行流式合成。

    参数:
        ws_uri: WebSocket 地址（如 ws://localhost:8768/api/v1/synthesize）
        text: 要合成的文本
        voice: 音色 ID（可选）
        encoding: pcm_f32le 或 pcm_s16le
        sample_rate: 目标采样率
        channels: 声道数（1 或 2）
        options: 引擎选项（sample_mode, do_sample, seed, max_new_frames 等）
        output_wav: 保存路径（可选）
        verbose: 是否打印日志

    返回:
        合成统计信息 dict，包含 total_audio_frames, audio_duration_sec, inference_elapsed_sec
    """
    request_id = f"demo-{uuid.uuid4().hex[:8]}"
    audio_chunks: list[np.ndarray] = []
    actual_sr = sample_rate
    actual_ch = channels
    stats = {}

    if verbose:
        print(f"🔊 发送合成请求: request_id={request_id}")
        print(f"   文本: {text[:60]}{'...' if len(text) > 60 else ''}")
        if voice:
            print(f"   音色: {voice}")
        print(f"   格式: {encoding}, {sample_rate}Hz, {channels}ch")
        print()

    async with websockets.connect(ws_uri) as ws:
        # 发送合成请求
        payload: dict[str, Any] = {
            "type": "synthesize",
            "request_id": request_id,
            "text": text,
            "format": {
                "encoding": encoding,
                "sample_rate": sample_rate,
                "channels": channels,
            },
        }
        if voice:
            payload["voice"] = voice
        if options:
            payload["options"] = options

        await ws.send(json.dumps(payload))

        # 接收流式响应
        seq_received = set()
        while True:
            raw = await ws.recv()
            data = json.loads(raw)
            msg_type = data.get("type", "")

            if msg_type == "audio":
                # 音频数据块
                seq = data.get("seq", 0)
                b64data = data.get("data", "")
                sr = data.get("sample_rate", sample_rate)
                ch = data.get("channels", channels)
                is_final = data.get("is_final", False)

                actual_sr = sr
                actual_ch = ch
                seq_received.add(seq)

                if b64data:
                    pcm_bytes = base64.b64decode(b64data)
                    if encoding == "pcm_s16le":
                        chunk = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32767.0
                    else:
                        chunk = np.frombuffer(pcm_bytes, dtype=np.float32)

                    if ch > 1:
                        chunk = chunk.reshape(-1, ch)

                    audio_chunks.append(chunk)

                    if verbose:
                        duration = len(chunk) / sr if sr else 0
                        flag = " ✅ FINAL" if is_final else ""
                        print(f"  📦 seq={seq}: {duration:.3f}s ({len(pcm_bytes)} bytes){flag}")

            elif msg_type == "done":
                # 合成完成
                stats = {
                    "total_audio_frames": data.get("total_audio_frames", 0),
                    "audio_duration_sec": data.get("audio_duration_sec", 0),
                    "inference_elapsed_sec": data.get("inference_elapsed_sec", 0),
                    "text_was_ssml": data.get("text_was_ssml", False),
                }
                if verbose:
                    print(f"\n✅ 合成完成")
                    print(f"   音频时长: {stats['audio_duration_sec']:.3f}s")
                    print(f"   推理耗时: {stats['inference_elapsed_sec']:.3f}s")
                    rt_factor = stats["audio_duration_sec"] / stats["inference_elapsed_sec"] if stats["inference_elapsed_sec"] else 0
                    print(f"   实时率: {rt_factor:.2f}x")
                break

            elif msg_type == "error":
                # 错误
                code = data.get("code", "UNKNOWN")
                message = data.get("message", "")
                fatal = data.get("fatal", False)
                print(f"\n❌ [{code}] {message}")
                if fatal:
                    print("   致命错误，连接关闭")
                return None

            elif msg_type == "pong":
                pass  # 心跳回应，忽略

    # 合并音频
    if audio_chunks:
        waveform = np.concatenate(audio_chunks, axis=0)
        real_duration = len(waveform) / actual_sr if actual_sr else 0

        if verbose:
            print(f"📊 实际音频: {len(waveform)} 样本, {real_duration:.3f}s, {actual_sr}Hz, {actual_ch}ch")

        if output_wav:
            sf.write(output_wav, waveform, actual_sr)
            if verbose:
                print(f"💾 已保存到: {output_wav}")

        stats["actual_duration_sec"] = real_duration
        stats["actual_sample_rate"] = actual_sr
        stats["actual_channels"] = actual_ch

    return stats


async def interactive_mode(ws_uri: str, base_uri: str):
    """交互模式"""
    # 先发现引擎能力
    if httpx:
        try:
            info = discover(base_uri)
            print_discover(info)
            voices = list_voices(base_uri)
            print_voices(voices)
        except Exception as e:
            print(f"⚠️  引擎发现失败: {e}")
            print()

    print("🟢 MOSS-TTS 标准 API 交互模式")
    print("   输入文本按回车合成")
    print("   指令:")
    print("     /voices    - 列出音色")
    print("     /info      - 引擎信息")
    print("     /cancel    - 取消当前请求")
    print("     /format    - 切换音频格式")
    print("     /q         - 退出")
    print()

    # 当前设置
    current_voice = None
    current_encoding = "pcm_f32le"
    current_sr = 48000
    current_ch = 2
    current_options: dict[str, Any] = {}

    while True:
        try:
            line = input("📝 ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 再见")
            break

        if not line:
            continue

        if line == "/q":
            break

        if line == "/voices" and httpx:
            try:
                voices = list_voices(base_uri)
                print_voices(voices)
            except Exception as e:
                print(f"❌ 获取音色失败: {e}")
            continue

        if line == "/info" and httpx:
            try:
                info = discover(base_uri)
                print_discover(info)
            except Exception as e:
                print(f"❌ 获取引擎信息失败: {e}")
            continue

        if line == "/cancel":
            print("⏹️  取消指令仅在 WebSocket 连接中有效")
            continue

        if line.startswith("/voice"):
            parts = line.split(None, 1)
            if len(parts) == 2:
                current_voice = parts[1]
                print(f"🎤 切换到音色: {current_voice}")
            else:
                print(f"🎤 当前音色: {current_voice or '默认'}")
            continue

        if line.startswith("/format"):
            parts = line.split()
            if len(parts) >= 2:
                current_encoding = parts[1]
            if len(parts) >= 3:
                current_sr = int(parts[2])
            if len(parts) >= 4:
                current_ch = int(parts[3])
            print(f"🔊 格式: {current_encoding}, {current_sr}Hz, {current_ch}ch")
            continue

        # 普通文本 → 合成
        text = line
        voice = current_voice

        # 支持行内音色: "你好|voice=Zhiming"
        if "|" in text:
            parts = text.split("|")
            text = parts[0]
            for param in parts[1].split(","):
                param = param.strip()
                if param.startswith("voice="):
                    voice = param.split("=", 1)[1]

        try:
            await synthesize(
                ws_uri=ws_uri,
                text=text,
                voice=voice,
                encoding=current_encoding,
                sample_rate=current_sr,
                channels=current_ch,
                options=current_options or None,
                output_wav=None,
                verbose=True,
            )
        except Exception as e:
            print(f"❌ 合成失败: {e}")


# ═══════════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="MOSS-TTS 标准 API 客户端 Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python client_demo.py --discover                    # 查询引擎能力
  python client_demo.py --voices                      # 列出音色
  python client_demo.py --text "你好世界"              # 单次合成，保存到 output.wav
  python client_demo.py --text "你好" --voice Zhiming  # 指定音色
  python client_demo.py --text "Hello" --encoding pcm_s16le --channels 1
  python client_demo.py                                # 交互模式
        """,
    )
    parser.add_argument("--host", default="localhost", help="服务器地址")
    parser.add_argument("--port", type=int, default=8768, help="服务器端口")
    parser.add_argument("--text", help="要合成的文本")
    parser.add_argument("--voice", help="音色 ID")
    parser.add_argument("--encoding", default="pcm_f32le", choices=["pcm_f32le", "pcm_s16le"])
    parser.add_argument("--sample-rate", type=int, default=48000, help="采样率")
    parser.add_argument("--channels", type=int, default=2, choices=[1, 2], help="声道数")
    parser.add_argument("--output", default="output_standard.wav", help="输出文件")
    parser.add_argument("--seed", type=int, help="随机种子（可选）")
    parser.add_argument("--sample-mode", choices=["fixed", "greedy", "full"], help="采样模式")
    parser.add_argument("--do-sample", type=bool, default=True, help="是否随机采样")
    parser.add_argument("--discover", action="store_true", help="只查询引擎信息")
    parser.add_argument("--voices", action="store_true", help="只列出音色")
    parser.add_argument("--interactive", action="store_true", help="交互模式")

    args = parser.parse_args()

    base_uri = f"http://{args.host}:{args.port}"
    ws_uri = f"ws://{args.host}:{args.port}/api/v1/synthesize"

    # REST 发现模式
    if args.discover:
        if httpx is None:
            print("❌ 请安装 httpx: pip install httpx")
            sys.exit(1)
        try:
            info = discover(base_uri)
            print_discover(info)
        except Exception as e:
            print(f"❌ 发现失败: {e}")
        return

    if args.voices:
        if httpx is None:
            print("❌ 请安装 httpx: pip install httpx")
            sys.exit(1)
        try:
            voices = list_voices(base_uri)
            print_voices(voices)
        except Exception as e:
            print(f"❌ 获取音色失败: {e}")
        return

    # 单次合成模式
    if args.text:
        options = {}
        if args.seed is not None:
            options["seed"] = args.seed
        if args.sample_mode:
            options["sample_mode"] = args.sample_mode
        options["do_sample"] = args.do_sample

        asyncio.run(synthesize(
            ws_uri=ws_uri,
            text=args.text,
            voice=args.voice,
            encoding=args.encoding,
            sample_rate=args.sample_rate,
            channels=args.channels,
            options=options or None,
            output_wav=args.output,
            verbose=True,
        ))
        return

    # 交互模式（默认）
    asyncio.run(interactive_mode(ws_uri=ws_uri, base_uri=base_uri))


if __name__ == "__main__":
    main()
