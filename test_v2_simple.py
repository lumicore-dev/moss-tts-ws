import asyncio, json, time, websockets

async def main():
    uri = "ws://127.0.0.1:18766/tts"
    t0 = time.perf_counter()
    
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"text": "测试首包延迟", "streaming": True}))
        
        chunk_count = 0
        while True:
            raw = await ws.recv()
            now = time.perf_counter()
            data = json.loads(raw)
            
            if data["type"] == "audio_chunk":
                d = data.get("data", "")
                if d:
                    chunk_count += 1
                    ms = (now - t0) * 1000
                    if chunk_count == 1:
                        print(f"首chunk到达: {ms:.0f}ms")
                    elif chunk_count <= 5:
                        print(f"  chunk #{chunk_count}: {ms:.0f}ms, size={len(d)}B")

            elif data["type"] == "done":
                print(f"done: 推理{data['inference_elapsed_sec']:.2f}s, 音频{data['audio_duration_sec']:.2f}s")
                break
            elif data["type"] == "error":
                print(f"错误: {data['message']}")
                break

asyncio.run(main())
