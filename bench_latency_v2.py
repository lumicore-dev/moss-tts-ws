"""
精准测量首包延迟（V1 server）
"""
import asyncio, json, time
import websockets


async def single(uri, text, voice=None):
    async with websockets.connect(uri) as ws:
        t_send = time.perf_counter()
        await ws.send(json.dumps({"text": text, "streaming": True, **({"voice": voice} if voice else {})}))

        ttfa = audio_sec = infer_sec = chunks = -1
        while True:
            raw = await ws.recv()
            now = time.perf_counter()
            data = json.loads(raw)

            if data["type"] == "audio_chunk" and data.get("data"):
                chunks += 1
                if ttfa < 0:
                    ttfa = (now - t_send) * 1000
            elif data["type"] == "done":
                audio_sec = data["audio_duration_sec"]
                infer_sec = data["inference_elapsed_sec"]
                return ttfa, audio_sec, infer_sec, chunks + 1
            elif data["type"] == "error":
                return -1, 0, 0, 0


async def main():
    uri = "ws://127.0.0.1:18765/tts"

    tests = [
        ("极短 2字", "你好", None),
        ("短文本 9字", "你好，欢迎来到北京。", None),
        ("短文本+音色", "你好，欢迎来到北京。", "Zhiming"),
        ("中文本 30字", "北京帝派智能是一家专注AI语音技术的创新企业。", None),
        ("长文本 60字", "北京帝派智能科技有限公司是一家专注于人工智能语音技术的创新企业，核心团队来自国内外顶尖科研机构。", None),
    ]

    print(f"{'测试':<20s} {'首chunk':>10s} {'chunks':>6s} {'音频长':>8s} {'推理':>8s} {'实时率':>7s}")
    print("-" * 65)

    for label, text, voice in tests:
        # 预热
        await single(uri, text, voice)
        await asyncio.sleep(0.2)

        # 采集3次
        results = [await single(uri, text, voice) for _ in range(3)]
        await asyncio.sleep(0.2)

        ok = [r for r in results if r[0] > 0]
        if ok:
            avg_ttfa = sum(r[0] for r in ok) / len(ok)
            avg_chunks = sum(r[3] for r in ok) / len(ok)
            avg_audio = sum(r[1] for r in ok) / len(ok)
            avg_infer = sum(r[2] for r in ok) / len(ok)
            rt = avg_audio / avg_infer if avg_infer else 0
            print(f"{label:<20s} {avg_ttfa:>8.1f}ms {avg_chunks:>5.0f}  {avg_audio:>6.2f}s {avg_infer:>6.2f}s {rt:>5.2f}x")
        else:
            print(f"{label:<20s}  ❌")

    print("-" * 65)
    print("首chunk = 从ws.send()到收到第一条非空audio_chunk的时间")


if __name__ == "__main__":
    asyncio.run(main())
