# The Oracle — Bridge Server

Voice AI backend for [Metaphysics and Computing](https://www.metaphysicsandcomputing.com).

## Architecture

```
Browser → Node.js Bridge → DeepInfra API (LLM + TTS)
                        → UAIMC (localhost:8765, RAG context)
```

**Stack:** Node.js + Express + WebSocket + OpenAI SDK (pointed at DeepInfra)

**Pipeline per request:**
1. Query UAIMC for RAG context (BM25 + GPU-AM retrieval)
2. Inject context into Oracle system prompt
3. Stream LLM response via DeepInfra (Qwen3.5-A17B or 397B)
4. Detect sentence boundaries in the stream
5. TTS each sentence via DeepInfra (`openai/tts-1`)
6. Stream NDJSON to browser: text first, audio follows per-sentence
7. Ingest full conversation back into UAIMC (background)

## Setup

```bash
cd oracle-backend
npm install
cp .env.example .env
# Edit .env — add your DEEPINFRA_API_KEY
node server.js
```

## API

### POST /api/chat

```json
{ "message": "What is QEGG?", "sessionId": "optional-uuid", "page": "homepage" }
```

Response: NDJSON stream (`Content-Type: application/x-ndjson`)

```jsonl
{"type":"text","content":"QEGG stands for Quantum..."}
{"type":"audio","data":"<base64 mp3>"}
{"type":"text","content":"It represents..."}
{"type":"audio","data":"<base64 mp3>"}
{"type":"done"}
```

### GET /api/health

```json
{ "status": "ok", "uaimc": true, "deepinfra": true, "model": "Qwen/Qwen3.5-A17B" }
```

### WS /ws/oracle

Same pipeline over WebSocket for lower-latency streaming.
Send JSON: `{ "message": "...", "sessionId": "..." }`
Receive same NDJSON event objects.

## Deployment

### Local Development

```bash
npm run dev   # node --watch (auto-restarts on file changes)
```

### Production (Railway / Render / VPS)

1. Set environment variables in your platform dashboard
2. Set `UAIMC_URL` to your UAIMC instance URL (or keep localhost if co-located)
3. Set `CORS_ORIGIN` to `https://www.metaphysicsandcomputing.com`
4. `npm start`

### Cloudflare Workers (alternative)

The server uses Node.js built-ins (`http`, `Buffer`) which require Node.js compatibility mode in Workers. Standard deployment on Railway/Render is recommended.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEEPINFRA_API_KEY` | — | **Required.** Get from deepinfra.com/dashboard |
| `UAIMC_URL` | `http://localhost:8765` | UAIMC knowledge base endpoint |
| `PORT` | `3001` | Server port |
| `CORS_ORIGIN` | `https://www.metaphysicsandcomputing.com` | Allowed frontend origin |
| `RATE_LIMIT_MAX` | `30` | Max requests per IP per window |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Rate limit window (1 minute) |
| `LLM_MODEL` | `Qwen/Qwen3.5-A17B` | DeepInfra LLM model ID |
| `TTS_VOICE` | `aura-asteria-en` | DeepInfra TTS voice |
