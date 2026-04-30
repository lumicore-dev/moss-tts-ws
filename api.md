# Unified TTS WebSocket API Specification

> **Version:** 0.1.0-draft  
> **Status:** Draft  
> **Last Updated:** 2026-04-30

## 1. Philosophy

Different TTS engines have inherently different capabilities — some support
streaming, some don't; some offer fine-grained prosody control, others only
voice selection. This specification does **not** attempt to flatten all engines
into a single rigid schema. Instead, it defines:

- A **discovery mechanism** for engines to advertise their capabilities.
- A **standard message envelope** for request/response correlation.
- A set of **well-known fields** that most engines share.
- An **extensible options bag** for engine-specific features.

## 2. Message Envelope

Every JSON message on the WebSocket, in both directions, MUST contain a `type`
field as the message discriminator. Messages SHOULD include a `request_id` for
correlation when applicable.

## 3. REST Endpoints

### 3.1 `GET /api/v1/info`

Returns the engine's capabilities and metadata. This is the primary discovery
endpoint — clients MUST call this first to learn how to interact with the
engine.

**Response:**

```json
{
  "engine": {
    "name": "moss-tts",
    "version": "0.2.0",
    "description": "MOSS-TTS-Nano real-time streaming TTS engine"
  },
  "endpoints": {
    "websocket": "wss://host/api/v1/synthesize",
    "voices": "/api/v1/voices"
  },
  "capabilities": {
    "streaming": true,
    "interruptible": true,
    "ssml": false,
    "speed_control": false,
    "pitch_control": false,
    "emotion_control": false,
    "multi_request_per_connection": true,
    "max_text_length": 5000
  },
  "voices": {
    "type": "predefined",
    "dynamic": false,
    "default": "Junhao"
  },
  "formats": {
    "sample_rates": [24000, 48000],
    "encodings": ["pcm_f32le", "pcm_s16le"],
    "channels": [1, 2],
    "container": "raw"
  },
  "parameters": {
    "seed": {
      "type": "integer",
      "description": "Random seed for reproducible generation",
      "optional": true,
      "default": null
    }
  },
  "options": {
    "sample_mode": {
      "type": "string",
      "description": "Sampling mode for audio token generation",
      "values": ["fixed", "greedy", "full"],
      "optional": true,
      "default": "fixed"
    },
    "do_sample": {
      "type": "boolean",
      "description": "Whether to use stochastic sampling",
      "optional": true,
      "default": true
    },
    "max_new_frames": {
      "type": "integer",
      "description": "Maximum number of new audio frames to generate",
      "optional": true,
      "default": null
    },
    "voice_clone_max_text_tokens": {
      "type": "integer",
      "description": "Maximum text tokens per chunk for voice cloning",
      "optional": true,
      "default": 75,
      "min": 10,
      "max": 200
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `engine` | Engine identity metadata |
| `endpoints` | Where to connect for WebSocket and voice listing |
| `capabilities` | Boolean/descriptive flags of engine features |
| `voices` | Voice management model — `predefined`, `dynamic`, or `external` |
| `formats` | Supported audio output configurations |
| `parameters` | Standard (well-known) parameters the engine accepts |
| `options` | Engine-specific parameters with full schema documentation |

#### Standard Parameter Registry

Well-known parameters that engines SHOULD report in `parameters` if supported:

| Parameter | Type | Description |
|-----------|------|-------------|
| `seed` | int | Random seed for deterministic output |
| `speed` | float | Speaking speed multiplier (e.g. 1.2 = 120%) |
| `pitch` | float | Pitch shift in semitones |

### 3.2 `GET /api/v1/voices`

Returns the list of available voices.

**Response:**

```json
{
  "voices": [
    {
      "id": "Junhao",
      "display_name": "Junhao (Male)",
      "gender": "male",
      "language": ["zh", "en"],
      "group": "builtin",
      "description": "Default male voice"
    },
    {
      "id": "Zhiming",
      "display_name": "Zhiming (Male)",
      "gender": "male",
      "language": ["zh", "en"],
      "group": "builtin",
      "description": "Alternate male voice"
    }
  ]
}
```

The voices listed here serve as the authoritative set of allowed `voice` values
in synthesis requests.

---

## 4. WebSocket Endpoint

### 4.1 Connection

```
ws://host:port/api/v1/synthesize
```

A client MAY send multiple synthesis requests over a single connection. Each
request is identified by a unique `request_id`.

### 4.2 Client-to-Server Messages

#### `synthesize` — Submit a synthesis request

```json
{
  "type": "synthesize",
  "request_id": "req-001",
  "text": "你好，世界！Hello, world!",
  "voice": "Junhao",
  "format": {
    "sample_rate": 24000,
    "encoding": "pcm_f32le",
    "channels": 1
  },
  "options": {
    "sample_mode": "fixed",
    "do_sample": true,
    "seed": 42
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | ✅ | Must be `"synthesize"` |
| `request_id` | ✅ | Unique identifier for this request; used to correlate `audio`, `done`, and `error` responses |
| `text` | ✅ | The text to synthesize. Length limits advertised in `/api/v1/info`. |
| `voice` | ❌ | Voice ID from `/api/v1/voices`. Engine uses default if omitted. |
| `format` | ❌ | Desired audio format. Engine responds with the closest supported format or rejects with an error. |
| `format.sample_rate` | ❌ | Target sample rate. Default: engine's native rate. |
| `format.encoding` | ❌ | PCM encoding. Supported values: `pcm_f32le`, `pcm_s16le`. Default: engine default. |
| `format.channels` | ❌ | Number of audio channels. Default: engine default. |
| `options` | ❌ | Engine-specific parameters, each validated against the schema in `/api/v1/info`. |

**Audio encoding format specification:**

| Encoding | Description |
|----------|-------------|
| `pcm_f32le` | IEEE float 32-bit little-endian samples |
| `pcm_s16le` | Signed 16-bit integer little-endian samples |
| `mu_law` | 8-bit μ-law encoded samples |
| `opus` | Opus codec in Ogg container (container must be `ogg`) |

#### `cancel` — Cancel an ongoing synthesis

```json
{
  "type": "cancel",
  "request_id": "req-001"
}
```

The server SHALL stop processing the specified request immediately. Any
partially-sent audio chunks for that request MAY be silently discarded. No
further `audio` or `done` messages for the cancelled `request_id` will be
sent.

This is necessary for interactive applications where the user says something
new while the previous utterance is still being synthesized.

#### `ping` — Keepalive

```json
{
  "type": "ping",
  "request_id": "ping-001"
}
```

The server MUST reply with a `pong` message containing the same `request_id`.

### 4.3 Server-to-Client Messages

#### `audio` — Audio data chunk

```json
{
  "type": "audio",
  "request_id": "req-001",
  "seq": 1,
  "data": "<base64-encoded PCM bytes>",
  "sample_rate": 24000,
  "encoding": "pcm_f32le",
  "channels": 1,
  "is_final": false
}
```

| Field | Description |
|-------|-------------|
| `type` | `"audio"` |
| `request_id` | Correlates to the `synthesize` request |
| `seq` | Monotonically increasing sequence number for ordering |
| `data` | Base64-encoded audio bytes in the requested format |
| `sample_rate` | Actual sample rate of this chunk |
| `encoding` | Actual encoding of this chunk |
| `channels` | Actual channel count of this chunk |
| `is_final` | `true` for the last chunk of the request |

The actual parameters (`sample_rate`, `encoding`, `channels`) MAY differ from
the requested `format` if the engine cannot exactly match the request. The
client MUST honour the received parameters.

#### `done` — Synthesis complete

```json
{
  "type": "done",
  "request_id": "req-001",
  "engine": "moss-tts",
  "total_audio_frames": 87,
  "audio_duration_sec": 3.52,
  "inference_elapsed_sec": 0.84
}
```

Sent after the final `audio` chunk. No further messages for this
`request_id` will follow.

#### `word_timestamps` — Timestamp metadata (optional)

If the engine supports word-level timestamps, this message MAY be sent before
or after the `done` message for the same `request_id`.

```json
{
  "type": "word_timestamps",
  "request_id": "req-001",
  "words": [
    {"word": "你好", "start_sec": 0.1, "end_sec": 0.35},
    {"word": "世界", "start_sec": 0.4, "end_sec": 0.75}
  ]
}
```

Engines that do not support timestamping will never emit this message.

#### `error` — Error notification

```json
{
  "type": "error",
  "request_id": "req-001",
  "code": "INVALID_VOICE",
  "message": "Voice 'Robot' not found. Available voices: Junhao, Zhiming",
  "fatal": false
}
```

| Field | Description |
|-------|-------------|
| `type` | `"error"` |
| `request_id` | Correlates to the failing request |
| `code` | Machine-readable error code (see error codes below) |
| `message` | Human-readable error description |
| `fatal` | If `true`, the WebSocket connection will close after this error |

**Standard Error Codes:**

| Code | Description |
|------|-------------|
| `INVALID_REQUEST` | Malformed JSON or missing required fields |
| `INVALID_TEXT` | Text is empty, too long, or contains unsupported characters |
| `INVALID_VOICE` | Specified voice does not exist |
| `INVALID_FORMAT` | Requested audio format is not supported |
| `INVALID_OPTION` | An engine-specific option is invalid |
| `RATE_LIMITED` | Too many requests in a given time window |
| `ENGINE_ERROR` | Internal engine failure (fatal) |
| `CANCELLED` | Request was cancelled by the client |

#### `pong` — Keepalive response

```json
{
  "type": "pong",
  "request_id": "ping-001"
}
```

---

## 5. Example Client Flow

```
1. Client → GET /api/v1/info
   ← Engine advertises capabilities, voices, formats, parameters.

2. Client → GET /api/v1/voices
   ← Client caches available voice list.

3. Client                  → Server (WebSocket /api/v1/synthesize)
   Connect WebSocket

4. Client → Server:
   {"type": "synthesize", "request_id": "req-1", "text": "你好",
    "format": {"sample_rate": 24000, "encoding": "pcm_s16le", "channels": 1}}

5. Server → Client:
   {"type": "audio", "request_id": "req-1", "seq": 0, "data": "...", "is_final": false}
   {"type": "audio", "request_id": "req-1", "seq": 1, "data": "...", "is_final": false}
   {"type": "audio", "request_id": "req-1", "seq": 2, "data": "...", "is_final": false}
   {"type": "audio", "request_id": "req-1", "seq": 3, "data": "...", "is_final": true}
   {"type": "done", "request_id": "req-1", "audio_duration_sec": 1.2, ...}

6. (Optional) Client interrupts:
   Client → Server:
   {"type": "cancel", "request_id": "req-1"}

7. Close WebSocket (or reuse for more requests)
```

---

## 6. Multi-Engine Proxy (Future)

An optional proxy layer MAY route requests to different engines based on:

- `voice` prefixes (e.g. `moss_Junhao` → moss-tts, `kokoro_Default` → Kokoro)
- Explicit `engine` field in the `synthesize` message

This is deliberately left out of the core spec to keep the protocol simple.

---

## 7. Versioning

The protocol version is specified in the server's `/api/v1/info` response.
Breaking changes to the message format will increment the major version and
be exposed at a new path (e.g. `/api/v2/...`).

---

## 8. Conventions

- All JSON messages use UTF-8 encoding.
- Timestamps are in seconds as float64.
- Base64 encoding uses URL-safe alphabet without padding (RFC 4648 §5).
- Audio sample data MUST be base64-encoded **before** encoding the JSON message.
- Error messages SHOULD include actionable information (e.g. available voices).
- Unknown fields in any message SHOULD be ignored by both sides.
