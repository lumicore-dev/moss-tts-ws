"""
MOSS-TTS WebSocket 测试客户端
用法：
    python test_client.py                          # 交互模式
    python test_client.py --text "你好世界"        # 单次合成
    python test_client.py --text "故事" --voice Zhiming
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time

import numpy as np
import soundfile as sf

try:
    import websockets
except ImportError:
    print("请先安装 websockets: pip install websockets")
    sys.exit(1)


async def send_and_receive(
    uri: str,
    text: str,
    voice: str | None = None,
    streaming: bool = True,
    output_wav: str | None = None,
):
    """连接 WebSocket 并发送合成请求"""
    payload = {"text": text, "streaming": streaming}
    if voice:
        payload["voice"] = voice

    print(f"🔊 发送: {json.dumps(payload, ensure_ascii=False)}")
    print()

    audio_chunks: list[np.ndarray] = []
    sample_rate = 48000
    channels = 2

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps(payload))

        while True:
            raw = await ws.recv()
            data = json.loads(raw)
            msg_type = data.get("type", "")

            if msg_type == "audio_chunk":
                b64data = data["data"]
                sr = data.get("sample_rate", 48000)
                ch = data.get("channels", 2)
                sample_rate = sr
                channels = ch

                pcm_bytes = base64.b64decode(b64data)
                chunk = np.frombuffer(pcm_bytes, dtype=np.float32).reshape(-1, ch)
                audio_chunks.append(chunk)

                duration = len(chunk) / sr
                print(f"  📦 chunk #{data.get('chunk_index', '?')}: {duration:.2f}s  {'✅ FINAL' if data.get('is_final') else ''}")

            elif msg_type == "done":
                total_duration = data.get("audio_duration_sec", 0)
                total_frames = data.get("total_frames", 0)
                print(f"\n✅ 合成完成: {total_frames} 帧, {total_duration:.2f}s")
                break

            elif msg_type == "error":
                print(f"\n❌ 错误: {data.get('message')}")
                return None

    if audio_chunks:
        waveform = np.concatenate(audio_chunks, axis=0)
        real_duration = len(waveform) / sample_rate
        print(f"📊 实际音频: {len(waveform)} 样本, {real_duration:.2f}s, {sample_rate}Hz, {channels}声道")

        if output_wav:
            sf.write(output_wav, waveform, sample_rate)
            print(f"💾 已保存到: {output_wav}")

        return waveform, sample_rate
    return None


async def interactive_mode(uri: str):
    """交互模式：一条一条发文本"""
    print("🟢 MOSS-TTS WebSocket 交互模式")
    print("   输入文本按回车发送，输入 /voices 查看音色，输入 /q 退出")
    print()

    voices = ["Junhao"]  # 默认音色

    async with websockets.connect(uri) as ws:
        print("已连接 WebSocket 服务器")

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
            if line == "/voices":
                # 通过 HTTP API 获取音色列表
                import httpx
                resp = httpx.get(uri.replace("ws://", "http://").rstrip("/") + "/voices")
                print("📢 可用音色:")
                for v in resp.json().get("voices", []):
                    print(f"   {v['voice']:12s} | {v['display_name']:30s} | {v['group']}")
                continue

            # 解析音色: "文本内容|voice=Zhiming"
            text = line
            voice = None
            if "|" in line:
                parts = line.split("|", 1)
                text = parts[0]
                for param in parts[1].split(","):
                    if param.startswith("voice="):
                        voice = param.split("=", 1)[1]

            payload = {"text": text, "streaming": True}
            if voice:
                payload["voice"] = voice

            await ws.send(json.dumps(payload))
            audio_chunks = []

            while True:
                raw = await ws.recv()
                data = json.loads(raw)
                mt = data.get("type", "")

                if mt == "audio_chunk":
                    pcm = base64.b64decode(data["data"])
                    chunk = np.frombuffer(pcm, dtype=np.float32).reshape(-1, data.get("channels", 2))
                    audio_chunks.append(chunk)
                    dur = len(chunk) / data.get("sample_rate", 48000)
                    print(f"  chunk #{data.get('chunk_index', '?')}: {dur:.2f}s")
                elif mt == "done":
                    total = sum(len(c) for c in audio_chunks) / 48000
                    print(f"  ✅ {total:.2f}s\n")
                    break
                elif mt == "error":
                    print(f"  ❌ {data.get('message')}\n")
                    break


def main():
    parser = argparse.ArgumentParser(description="MOSS-TTS WebSocket Test Client")
    parser.add_argument("--uri", default="ws://localhost:8765/tts", help="WebSocket 地址")
    parser.add_argument("--text", help="要合成的文本（单次模式）")
    parser.add_argument("--voice", help="音色名")
    parser.add_argument("--output", default="output_ws.wav", help="输出音频文件")
    parser.add_argument("--interactive", action="store_true", help="交互模式")
    args = parser.parse_args()

    if args.interactive or not args.text:
        asyncio.run(interactive_mode(args.uri))
    else:
        asyncio.run(send_and_receive(
            uri=args.uri,
            text=args.text,
            voice=args.voice,
            streaming=True,
            output_wav=args.output,
        ))


if __name__ == "__main__":
    main()
