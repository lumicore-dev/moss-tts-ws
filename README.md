# MOSS-TTS WebSocket Server

基于 MOSS-TTS-Nano ONNX CPU Runtime 的流式语音合成 WebSocket 服务。

## 特点

- **纯 CPU 实时合成**：无需 GPU，4 核 CPU 即可达到 >1x 实时率
- **WebSocket 流式输出**：文本分段输入，音频 chunk 逐段推送
- **每段可独立设音色**：支持 18 种内置音色（中文男/女声、英文、日文）
- **支持声音克隆**：提供参考音频即可模仿任意音色

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务（默认监听 0.0.0.0:8765）
python server.py

# 指定端口和日志级别
python server.py --port 8888 --log-level DEBUG
```

## WebSocket 协议

### 客户端 -> 服务端（JSON）

```json
{
  "text": "要合成的文本",
  "voice": "Zhiming",           // 可选，音色名，默认 Junhao
  "sample_mode": "fixed",       // 可选：fixed / greedy / full
  "do_sample": true,            // 可选
  "streaming": true,            // 是否流式返回（建议 true）
  "max_new_frames": null,       // 可选，最大帧数限制
  "voice_clone_max_text_tokens": 75,
  "seed": null                  // 可选，随机种子
}
```

### 服务端 -> 客户端（JSON）

```json
// 音频 chunk（可能多条）
{"type": "audio_chunk", "data": "<base64_pcm_float32>", "sample_rate": 48000, "channels": 2, "is_final": false, "chunk_index": 0}
{"type": "audio_chunk", "data": "...", "is_final": true, "chunk_index": 1}
// 完成消息
{"type": "done", "total_frames": 87, "audio_duration_sec": 3.5}
// 错误消息
{"type": "error", "message": "错误描述"}
```

## 可用音色

| 音色名 | 显示名 | 分组 |
|--------|--------|------|
| Junhao | CN 欢迎关注模思智能 | Chinese Male |
| Zhiming | CN 京味胡同闲聊 | Chinese Male |
| Weiguo | CN 说书 | Chinese Male |
| Xiaoyu | CN 明星 | Chinese Female |
| Yuewen | CN 机车 | Chinese Female |
| Lingyu | CN 深夜电台 | Chinese Female |
| Trump | EN Trump | English Male |
| Ava | EN The Bitter Lesson | English Female |
| Bella | EN A Gentle Reminder | English Female |
| Adam | EN English News | English Male |
| Nathan | EN The Quiet Motion of the World | English Male |
| Soyo / Saki / Mortis / Umiri / Mei / Anon / Arisa | JP voices | Japanese Female |

## 测试

```bash
# 单次合成
python test_client.py --text "你好世界" --voice Junhao --output hello.wav

# 交互模式（逐条输入）
python test_client.py --interactive
```

## 声音克隆

提供参考音频路径（需服务端能访问到该文件），即可克隆指定音色：

```json
{
  "text": "克隆的声音说这段话",
  "prompt_audio_path": "/path/to/reference.wav"
}
```
