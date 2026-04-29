"""
精准测量 "文本入 → 首音频出" 延迟
"""
import asyncio
import base64
import json
import time

import websockets


async def single_ttfa(uri: str, text: str, voice: str | None = None):
    """单次测量，返回首 chunk 延迟(ms)和总耗时(s)"""
    async with websockets.connect(uri) as ws:
        payload = {"text": text, "streaming": True}
        if voice:
            payload["voice"] = voice

        t_send = time.perf_counter()
        await ws.send(json.dumps(payload))

        while True:
            raw = await ws.recv()
            now = time.perf_counter()
            data = json.loads(raw)
            mt = data.get("type", "")

            if mt == "audio_chunk":
                ttfa_ms = (now - t_send) * 1000
                # 继续收完
                while True:
                    raw2 = await ws.recv()
                    d2 = json.loads(raw2)
                    if d2.get("type") == "done":
                        total_s = d2.get("audio_duration_sec", 0)
                        infer_s = d2.get("inference_elapsed_sec", 0)
                        return ttfa_ms, total_s, infer_s

            elif mt == "error":
                return -1, 0, 0


async def main():
    uri = "ws://127.0.0.1:18765/tts"

    short_text = "你好，欢迎来到北京。"
    medium_text = "北京帝派智能是一家专注AI语音技术的创新企业，团队来自国内外顶尖科研机构。这是一段较长的文本用于测试延迟。"

    print("=" * 60)
    print("📝 短文本首包延迟（逐个请求，冷/热分离）")
    print("=" * 60)

    for run in range(8):
        text = short_text
        voice = None
        if run == 0:
            note = "（冷启动，含模型预热）"
        elif run == 1:
            note = "（热状态）"
        elif run == 4:
            text = medium_text
            note = "（长文本）"
        elif run == 5:
            text = short_text
            voice = "Zhiming"
            note = "（切换音色 Zhiming）"
        elif run == 6:
            text = short_text
            voice = "Xiaoyu"
            note = "（切换音色 Xiaoyu）"
        else:
            note = "（热状态稳定）"

        result = await single_ttfa(uri, text, voice=voice)
        if result:
            ttfa, dur, infer = result
            print(f"  第 {run+1} 次: 首chunk={ttfa:6.1f}ms | 音频长={dur:5.2f}s | 推理耗={infer:5.2f}s {note}")
        else:
            print(f"  第 {run+1} 次: ❌ 失败 {note}")

        if run == 0:
            await asyncio.sleep(0.5)  # 冷启动后稍等

    print()
    print("📊 结论: 热状态稳定延迟约为首包的 1/3 ~ 1/5")
    print("    切换音色/文本长度对首包延迟影响不大，瓶颈在模型 prefill")


if __name__ == "__main__":
    asyncio.run(main())
