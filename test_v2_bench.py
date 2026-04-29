"""
对比 V1 vs V2 的首包延迟
V1 server: 端口 18765（完整生成后切分）
V2 server: 端口 18766（实时 on_frame 推送）
"""
import asyncio, json, time
import websockets

async def single(uri, text, voice=None):
    async with websockets.connect(uri) as ws:
        t_send = time.perf_counter()
        await ws.send(json.dumps({"text": text, "streaming": True, **({"voice": voice} if voice else {})}))
        ttfa = -1
        while True:
            raw = await ws.recv()
            now = time.perf_counter()
            data = json.loads(raw)
            if data["type"] == "audio_chunk" and data.get("data"):
                if ttfa < 0:
                    ttfa = (now - t_send) * 1000
            elif data["type"] == "done":
                return ttfa, data["audio_duration_sec"], data["inference_elapsed_sec"]
            elif data["type"] == "error":
                return -1, 0, 0

async def main():
    tests = [
        ("极短 '你好'", "你好"),
        ("短 '测试首包'", "测试首包延迟"),
        ("中 '北京帝派智能是一家创新企业'", "北京帝派智能是一家专注AI语音技术的创新企业。"),
        ("长 '北京帝派智能科技有限...'", "北京帝派智能科技有限公司是一家专注于人工智能语音技术的创新企业，核心团队来自国内外顶尖科研机构。"),
    ]
    
    for label, text in tests:
        # V1
        try:
            v1 = await single("ws://127.0.0.1:18765/tts", text)
            await asyncio.sleep(0.5)
        except:
            v1 = None
        
        # V2
        try:
            v2 = await single("ws://127.0.0.1:18766/tts", text)
            await asyncio.sleep(0.5)
        except:
            v2 = None
        
        print(f"\n{'─'*50}")
        print(f"📝 {label}")
        if v1:
            ttfa, dur, infer = v1
            print(f"  V1 (等全生成): 首chunk={ttfa:7.1f}ms | 推理={infer:.2f}s | 音频={dur:.2f}s")
        if v2:
            ttfa2, dur2, infer2 = v2
            saved = (v1[0] - ttfa2) if v1 else 0
            print(f"  V2 (逐帧推送): 首chunk={ttfa2:7.1f}ms | 推理={infer2:.2f}s | 音频={dur2:.2f}s")
            if v1:
                print(f"  🎯 V2 首包比 V1 早 {saved:.0f}ms ({saved/v1[0]*100:.0f}%)")

if __name__ == "__main__":
    asyncio.run(main())
