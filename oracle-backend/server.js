'use strict';

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const OpenAI = require('openai');

const { buildSystemPrompt } = require('./oracle-prompt');
const { buildRagContext, ingestConversation, checkUAIMCHealth, checkCANSHealth } = require('./uaimc-client');
const { sentenceBufferedStream, writeNdjsonEvent } = require('./streaming');
const { applySecurityMiddleware, apiRateLimiter, validateChatInput } = require('./security');

// ─── Configuration ────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3001', 10);
const CORS_ORIGINS = (process.env.CORS_ORIGIN || 'https://www.metaphysicsandcomputing.com')
  .split(',').map(s => s.trim()).filter(Boolean);
// Also allow the non-www variant for redirect-edge-case requests
if (CORS_ORIGINS.some(o => o.includes('www.metaphysicsandcomputing.com'))) {
  if (!CORS_ORIGINS.includes('https://metaphysicsandcomputing.com')) {
    CORS_ORIGINS.push('https://metaphysicsandcomputing.com');
  }
}
const DEEPINFRA_API_KEY = process.env.DEEPINFRA_API_KEY;
const LLM_MODEL = process.env.LLM_MODEL || 'Qwen/Qwen3.5-397B-A17B';
const TTS_BASE_URL = process.env.TTS_BASE_URL || null;   // e.g. https://kokoro-xxx.up.railway.app/v1
const TTS_MODEL    = process.env.TTS_MODEL    || 'kokoro';
const TTS_VOICE    = process.env.TTS_VOICE    || 'af_bella';
const TTS_API_KEY  = process.env.TTS_API_KEY  || 'not-needed';

if (!DEEPINFRA_API_KEY) {
  console.error('FATAL: DEEPINFRA_API_KEY is not set. Copy .env.example to .env and add your key.');
  process.exit(1);
}

// ─── TTS Client (Kokoro-FastAPI or any OpenAI-compatible TTS endpoint) ─────────
// Falls back to null (TTS disabled) if TTS_BASE_URL is not configured.
const ttsClient = TTS_BASE_URL ? new OpenAI({
  apiKey: TTS_API_KEY,
  baseURL: TTS_BASE_URL
}) : null;

// ─── DeepInfra Streaming Helper ──────────────────────────────────────────────
// Uses raw fetch instead of the OpenAI SDK to pass chat_template_kwargs
// (the SDK's extra_body doesn't reliably pass DeepInfra-specific params)
async function* createDeepInfraStream(messages, opts = {}) {
  const resp = await fetch('https://api.deepinfra.com/v1/openai/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${DEEPINFRA_API_KEY}`
    },
    body: JSON.stringify({
      model: opts.model || LLM_MODEL,
      messages,
      stream: true,
      max_tokens: opts.max_tokens || 1024,
      temperature: opts.temperature || 0.7,
      chat_template_kwargs: { enable_thinking: false }
    })
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`DeepInfra API error ${resp.status}: ${errText}`);
  }

  // Parse SSE stream into OpenAI-compatible chunk objects
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let partial = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    partial += decoder.decode(value, { stream: true });

    const lines = partial.split('\n');
    partial = lines.pop(); // Keep incomplete line for next iteration

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || !trimmed.startsWith('data: ')) continue;
      const data = trimmed.slice(6);
      if (data === '[DONE]') return;
      try {
        yield JSON.parse(data);
      } catch {
        // Skip malformed JSON chunks
      }
    }
  }
}

// ─── Express App ──────────────────────────────────────────────────────────────
const app = express();
app.set('trust proxy', 1); // Railway runs behind a reverse proxy

applySecurityMiddleware(app);

app.use(cors({
  origin: CORS_ORIGINS,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '16kb' }));

// ─── Routes ───────────────────────────────────────────────────────────────────

/**
 * GET /api/health
 * Returns server + dependency health status.
 */
app.get('/api/health', async (req, res) => {
  const [uaimcHealthy, cansAvailable] = await Promise.all([
    checkUAIMCHealth(),
    checkCANSHealth()
  ]);
  const deepinfraConfigured = Boolean(DEEPINFRA_API_KEY);

  res.json({
    status: 'ok',
    uaimc: uaimcHealthy,
    cans: cansAvailable,
    deepinfra: deepinfraConfigured,
    model: LLM_MODEL,
    tts: ttsClient ? TTS_MODEL : 'disabled',
    tts_voice: TTS_VOICE,
    timestamp: new Date().toISOString()
  });
});

/**
 * POST /api/chat
 * Main Oracle chat endpoint. Streams NDJSON response.
 *
 * Request body: { message: string, sessionId?: string, page?: string }
 *
 * Response: NDJSON stream, one JSON object per line:
 *   { "type": "text",  "content": "sentence text" }
 *   { "type": "audio", "data": "<base64 mp3>" }
 *   { "type": "done" }
 *   { "type": "error", "message": "..." }
 */
app.post('/api/chat', apiRateLimiter, async (req, res) => {
  const validation = validateChatInput(req.body);
  if (!validation.valid) {
    return res.status(400).json({ error: validation.error });
  }

  const { message, sessionId, page } = validation.data;

  // Set up NDJSON streaming response
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Transfer-Encoding', 'chunked');
  res.setHeader('Cache-Control', 'no-cache');
  res.flushHeaders();

  const conversationMessages = [{ role: 'user', content: message }];

  try {
    // 1. Retrieve RAG context from UAIMC
    const ragContext = await buildRagContext(message);

    // 2. Build system prompt with injected context
    const systemPrompt = buildSystemPrompt(ragContext);

    // 3. Start streaming LLM response from DeepInfra (thinking suppressed via raw fetch)
    const llmStream = createDeepInfraStream([
      { role: 'system', content: systemPrompt },
      { role: 'user', content: message }
    ]);

    // 4. Sentence-buffered streaming: LLM → sentence detection → TTS → NDJSON
    let fullResponse = '';
    for await (const event of sentenceBufferedStream(llmStream, ttsClient, TTS_VOICE, TTS_MODEL)) {
      if (event.type === 'text') {
        fullResponse += event.content + ' ';
      }
      writeNdjsonEvent(res, event);
    }

    // 5. Ingest the full conversation into UAIMC asynchronously (non-blocking)
    conversationMessages.push({ role: 'assistant', content: fullResponse.trim() });
    ingestConversation(conversationMessages, sessionId).catch(err => {
      console.warn('Background UAIMC ingest failed:', err.message);
    });

  } catch (err) {
    console.error('Oracle chat error:', err.message);
    // If headers already sent, write error as NDJSON event
    if (res.headersSent) {
      writeNdjsonEvent(res, { type: 'error', message: 'The Oracle encountered an unexpected disturbance. Please try again.' });
    } else {
      res.status(500).json({ error: 'Internal server error.' });
    }
  } finally {
    res.end();
  }
});

// ─── 3D-Map Proxy Routes (UAIMC → Railway) ──────────────────────────────────
const UAIMC_URL = process.env.UAIMC_URL || 'http://localhost:8765';

app.get('/api/v1/3d-map/overview', async (req, res) => {
  try {
    const resp = await fetch(`${UAIMC_URL}/api/v1/3d-map/overview`, {
      signal: AbortSignal.timeout(5000)
    });
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    console.error('3d-map overview proxy error:', err.message);
    res.status(502).json({ error: 'UAIMC unreachable' });
  }
});

app.get('/api/v1/3d-map/search', async (req, res) => {
  try {
    const q = req.query.q || '';
    const limit = parseInt(req.query.limit) || 50;
    const resp = await fetch(`${UAIMC_URL}/api/v1/3d-map/search?q=${encodeURIComponent(q)}&limit=${limit}`, {
      signal: AbortSignal.timeout(5000)
    });
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    console.error('3d-map search proxy error:', err.message);
    res.status(502).json({ error: 'UAIMC unreachable' });
  }
});

app.get('/api/v1/3d-map/expand/*', async (req, res) => {
  try {
    const nodeId = req.params[0];
    const resp = await fetch(`${UAIMC_URL}/api/v1/3d-map/expand/${encodeURIComponent(nodeId)}`, {
      signal: AbortSignal.timeout(5000)
    });
    const data = await resp.json();
    res.json(data);
  } catch (err) {
    console.error('3d-map expand proxy error:', err.message);
    res.status(502).json({ error: 'UAIMC unreachable' });
  }
});

// ─── WebSocket Server (optional lower-latency path) ──────────────────────────
const server = http.createServer(app);

// Per-IP message rate tracking for WebSocket connections
const wsRateBuckets = new Map();
const WS_RATE_LIMIT = 30;  // messages per minute (matches REST rate limiter)
const WS_RATE_WINDOW = 60000;

function wsRateCheck(ip) {
  const now = Date.now();
  let bucket = wsRateBuckets.get(ip);
  if (!bucket || now - bucket.start > WS_RATE_WINDOW) {
    bucket = { start: now, count: 0 };
    wsRateBuckets.set(ip, bucket);
  }
  bucket.count++;
  return bucket.count <= WS_RATE_LIMIT;
}

// Clean stale rate buckets every 2 minutes
setInterval(() => {
  const cutoff = Date.now() - WS_RATE_WINDOW;
  for (const [ip, bucket] of wsRateBuckets) {
    if (bucket.start < cutoff) wsRateBuckets.delete(ip);
  }
}, 120000).unref();

const wss = new WebSocketServer({
  server,
  path: '/ws/oracle',
  verifyClient: ({ req }, done) => {
    const origin = req.headers.origin || '';
    if (CORS_ORIGINS.some(o => origin === o)) {
      done(true);
    } else {
      console.warn(`WS connection rejected: origin "${origin}" not in allowed list`);
      done(false, 403, 'Forbidden');
    }
  }
});

wss.on('connection', (ws, req) => {
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress;
  console.log(`WS connection from ${clientIp}`);

  ws.on('message', async (raw) => {
    // Rate limit check
    if (!wsRateCheck(clientIp)) {
      ws.send(JSON.stringify({ type: 'error', message: 'Too many requests. Please wait a moment.' }));
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(raw.toString());
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON.' }));
      return;
    }

    const validation = validateChatInput(parsed);
    if (!validation.valid) {
      ws.send(JSON.stringify({ type: 'error', message: validation.error }));
      return;
    }

    const { message, sessionId } = validation.data;
    const conversationMessages = [{ role: 'user', content: message }];

    try {
      const ragContext = await buildRagContext(message);
      const systemPrompt = buildSystemPrompt(ragContext);

      const llmStream = createDeepInfraStream([
        { role: 'system', content: systemPrompt },
        { role: 'user', content: message }
      ]);

      let fullResponse = '';
      for await (const event of sentenceBufferedStream(llmStream, ttsClient, TTS_VOICE, TTS_MODEL)) {
        if (ws.readyState !== ws.OPEN) break;
        if (event.type === 'text') fullResponse += event.content + ' ';
        ws.send(JSON.stringify(event));
      }

      conversationMessages.push({ role: 'assistant', content: fullResponse.trim() });
      ingestConversation(conversationMessages, sessionId).catch(err => {
        console.warn('Background UAIMC ingest (WS) failed:', err.message);
      });

    } catch (err) {
      console.error('WS Oracle error:', err.message);
      if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: 'The Oracle encountered an unexpected disturbance.' }));
      }
    }
  });

  ws.on('error', err => console.warn('WS error:', err.message));
});

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`The Oracle bridge server listening on port ${PORT}`);
  console.log(`  CORS origins: ${CORS_ORIGINS.join(', ')}`);
  console.log(`  LLM model:   ${LLM_MODEL} (thinking suppressed)`);
  console.log(`  TTS:         ${TTS_BASE_URL ? `${TTS_MODEL} @ ${TTS_BASE_URL}` : 'disabled'}`);
  console.log(`  TTS voice:   ${TTS_VOICE}`);
  console.log(`  UAIMC:       ${process.env.UAIMC_URL || 'http://localhost:8765'}`);
  console.log(`  REST:  POST http://localhost:${PORT}/api/chat`);
  console.log(`  Health: GET http://localhost:${PORT}/api/health`);
  console.log(`  WS:    ws://localhost:${PORT}/ws/oracle`);
});

// ─── Graceful Shutdown ────────────────────────────────────────────────────────
function gracefulShutdown(signal) {
  console.log(`${signal} received — shutting down gracefully`);
  // Stop accepting new connections
  wss.clients.forEach(ws => {
    ws.send(JSON.stringify({ type: 'error', message: 'Server restarting, please reconnect.' }));
    ws.close(1001, 'Server shutting down');
  });
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
  // Force exit after 5 seconds if graceful shutdown stalls
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

module.exports = { app, server };
