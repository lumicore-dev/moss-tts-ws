# MOSS-TTS WebSocket Server

基于 MOSS-TTS-Nano ONNX CPU Runtime 的**流式语音合成 WebSocket 服务**。

## 核心特性

- **真正的低延迟流式输出**：ONNX 逐帧解码回调 → 实时 WebSocket 推送，首包延迟 ~700ms（CPU）
- **首包延迟与文本长度无关**：无论 2 字还是 50 字，首 chunk 都在 ~700ms 内到达
- **文本逐段输入，音频 chunk 逐段输出**
- **每段可独立设音色**：支持 18 种内置音色 + 声音克隆
- **纯 CPU 实时合成**：4 核 CPU 达到 ~0.5x 实时率（CUDA 可达 3-5x）

## 延迟指标（CPU）

| 文本长度 | 首包延迟 | 后续 chunk 间隔 |
|---------|:-------:|:--------------:|
| 2 字     | ~700ms  | ~400ms         |
| 9 字     | ~700ms  | ~400ms         |
| 22 字    | ~720ms  | ~400ms         |
| 50 字    | ~730ms  | ~400ms         |

首包延迟瓶颈在 ONNX prefill（~600ms），使用 `--execution-provider cuda` 可压至 100ms 内。

## 快速开始

```bash
pip install -r requirements.txt
python server.py
```

默认监听 `0.0.0.0:8765`，WebSocket 端点 `/tts`。

## 参数

| 参数 | 说明 |
|------|------|
| `--host` | 监听地址，默认 `0.0.0.0` |
| `--port` | 端口，默认 `8765` |
| `--model-dir` | ONNX 模型目录 |
| `--execution-provider` | `cpu` 或 `cuda` |
| `--log-level` | `DEBUG / INFO / WARNING / ERROR` |

## WebSocket 协议

### 客户端 → 服务端

```json
{
  "text": "要合成的文本",
  "voice": "Zhiming",                    // 可选，音色名
  "sample_mode": "fixed",                // 可选：fixed / greedy / full
  "do_sample": true,
  "streaming": true,
  "max_new_frames": null,
  "voice_clone_max_text_tokens": 75,
  "seed": null
}
```

### 服务端 → 客户端（流式）

```json
{"type": "audio_chunk", "data": "<base64_pcm_float32>", "sample_rate": 48000, "channels": 2, "is_final": false}
{"type": "audio_chunk", "data": "", "sample_rate": 48000, "channels": 2, "is_final": true}
{"type": "done", "total_frames": 87, "audio_duration_sec": 3.5, "inference_elapsed_sec": 4.3}
```

音频数据为 **float32 PCM**，48kHz 双声道，base64 编码。每 chunk ~0.3 秒。

## 内置音色（18种）

| 音色 | 风格 | 语言 |
|------|------|------|
| Junhao | 欢迎关注模思智能 | Chinese Male |
| Zhiming | 京味胡同闲聊 | Chinese Male |
| Weiguo | 说书 | Chinese Male |
| Xiaoyu | 明星 | Chinese Female |
| Yuewen | 机车 | Chinese Female |
| Lingyu | 深夜电台 | Chinese Female |
| Trump | Trump | English Male |
| Ava | The Bitter Lesson | English Female |
| Bella | A Gentle Reminder | English Female |
| Adam | English News | English Male |
| Nathan | The Quiet Motion of the World | English Male |
| Soyo / Saki / Mortis / Umiri / Mei / Anon / Arisa | JP voices | Japanese Female |

## 架构说明

V2 版采用**真正的实时流式架构**：

```
文本 → 分段 → generate_audio_frames(on_frame=callback)
                                         ↓
                                on_frame: 帧→codec解码→chunk
                                         ↓
                                asyncio.Queue → WebSocket.send
```

相比 V1（等全 waveform 生成再切分），V2 在 ONNX 逐帧生成时就通过 `on_frame` 回调解码并推送，首包延迟从「总推理耗时」降为「prefill + 首帧解码」。
