'use strict';

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const OpenAI = require('openai');

const { buildSystemPrompt } = require('./oracle-prompt');
const { buildRagContext, ingestConversation, checkUAIMCHealth } = require('./uaimc-client');
const { sentenceBufferedStream, writeNdjsonEvent } = require('./streaming');
const { applySecurityMiddleware, apiRateLimiter, validateChatInput } = require('./security');

// ─── Configuration ────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3001', 10);
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'https://www.metaphysicsandcomputing.com';
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

// ─── LLM Client (DeepInfra) ───────────────────────────────────────────────────
const deepinfraLLM = new OpenAI({
  apiKey: DEEPINFRA_API_KEY,
  baseURL: 'https://api.deepinfra.com/v1/openai'
});

// ─── TTS Client (Kokoro-FastAPI or any OpenAI-compatible TTS endpoint) ─────────
// Falls back to null (TTS disabled) if TTS_BASE_URL is not configured.
const ttsClient = TTS_BASE_URL ? new OpenAI({
  apiKey: TTS_API_KEY,
  baseURL: TTS_BASE_URL
}) : null;

// ─── Express App ──────────────────────────────────────────────────────────────
const app = express();

applySecurityMiddleware(app);

app.use(cors({
  origin: CORS_ORIGIN,
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
  const uaimcHealthy = await checkUAIMCHealth();
  // Quick DeepInfra check: we assume healthy if API key is set (avoid billing a test call)
  const deepinfraConfigured = Boolean(DEEPINFRA_API_KEY);

  res.json({
    status: 'ok',
    uaimc: uaimcHealthy,
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

    // 3. Start streaming LLM response from DeepInfra (thinking suppressed)
    const llmStream = await deepinfraLLM.chat.completions.create({
      model: LLM_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: message }
      ],
      stream: true,
      max_tokens: 512,
      temperature: 0.7,
      extra_body: { chat_template_kwargs: { enable_thinking: false } }
    });

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

// ─── WebSocket Server (optional lower-latency path) ──────────────────────────
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws/oracle' });

wss.on('connection', (ws, req) => {
  console.log(`WS connection from ${req.socket.remoteAddress}`);

  ws.on('message', async (raw) => {
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

      const llmStream = await deepinfraLLM.chat.completions.create({
        model: LLM_MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: message }
        ],
        stream: true,
        max_tokens: 512,
        temperature: 0.7,
        extra_body: { chat_template_kwargs: { enable_thinking: false } }
      });

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
  console.log(`  CORS origin: ${CORS_ORIGIN}`);
  console.log(`  LLM model:   ${LLM_MODEL} (thinking suppressed)`);
  console.log(`  TTS:         ${TTS_BASE_URL ? `${TTS_MODEL} @ ${TTS_BASE_URL}` : 'disabled'}`);
  console.log(`  TTS voice:   ${TTS_VOICE}`);
  console.log(`  UAIMC:       ${process.env.UAIMC_URL || 'http://localhost:8765'}`);
  console.log(`  REST:  POST http://localhost:${PORT}/api/chat`);
  console.log(`  Health: GET http://localhost:${PORT}/api/health`);
  console.log(`  WS:    ws://localhost:${PORT}/ws/oracle`);
});

module.exports = { app, server };
