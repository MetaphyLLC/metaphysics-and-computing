'use strict';

// Matches sentence boundaries: end of text followed by .!? and optional whitespace
// Handles abbreviations poorly intentionally — simplicity > perfection for TTS chunking
const SENTENCE_BOUNDARY = /^(.*?[.!?])\s+(.*)/s;
// Minimum sentence length to avoid TTS calls on tiny fragments
const MIN_SENTENCE_CHARS = 4;
const FLUSH_TIMEOUT_MS = 2000;
const FLUSH_WORD_COUNT = 30;

/**
 * Normalize text before sending to TTS.
 * Expands brand acronyms so the TTS engine pronounces them correctly
 * and strips markdown/special characters that would be read aloud.
 * @param {string} text
 * @returns {string}
 */
function normalizeTtsText(text) {
  return text
    // Brand pronunciation for natural TTS speech
    .replace(/\bMetaphy\b/gi, 'Meta-fye')     // "MET-uh-fye" (rhymes with Wi-Fi)
    .replace(/\bQEGG\b/g, 'Kegg')             // Pronounced "Kegg" (keg + egg, hard K)
    .replace(/\bDRGFC\b/g, 'D.R.G.F.C.')
    .replace(/\bHMSS\b/g, 'H.M.S.S.')
    .replace(/\bBCPS\b/g, 'B.C.P.S.')
    .replace(/\bUAIMC\b/g, 'you-ay-eye-em-see')
    // Strip markdown that TTS would read aloud
    .replace(/\*\*([^*]+)\*\*/g, '$1')   // **bold**
    .replace(/\*([^*]+)\*/g, '$1')       // *italic*
    .replace(/`([^`]+)`/g, '$1')         // `code`
    .replace(/—/g, ', ')                 // em-dash → pause
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Strip Qwen3.5 chain-of-thought thinking blocks from a text buffer.
 * Exported for unit testing; the main streaming loop uses stateful detection instead.
 * @param {string} buffer - Current accumulation buffer
 * @returns {{ clean: string, inThinking: boolean }}
 */
function stripThinkingFromBuffer(buffer) {
  // Remove complete <think>...</think> blocks
  const cleaned = buffer.replace(/<think>[\s\S]*?<\/think>/g, '');
  // Detect if we're inside an open thinking block (no closing tag yet)
  const openIdx = cleaned.lastIndexOf('<think>');
  if (openIdx !== -1) {
    return { clean: cleaned.slice(0, openIdx).trim(), inThinking: true };
  }
  return { clean: cleaned, inThinking: false };
}

/**
 * Attempt TTS with a single retry on failure.
 * @param {Object} client - OpenAI-compatible TTS client
 * @param {string} model - TTS model name
 * @param {string} voice - TTS voice name
 * @param {string} text - Text to synthesize
 * @returns {Promise<{success: boolean, data?: string, message?: string}>}
 */
async function attemptTts(client, model, voice, text) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const audio = await client.audio.speech.create({
        model,
        voice,
        input: normalizeTtsText(text),
        response_format: 'mp3'
      });
      const audioBuffer = Buffer.from(await audio.arrayBuffer());
      return { success: true, data: audioBuffer.toString('base64') };
    } catch (err) {
      if (attempt === 0) {
        console.warn(`TTS attempt 1 failed, retrying: ${err.message}`);
        continue;
      }
      return { success: false, message: err.message };
    }
  }
}

/**
 * Sentence-buffered streaming pipeline.
 * Consumes an LLM stream, detects sentence boundaries,
 * calls TTS on each sentence, and yields NDJSON events.
 *
 * Uses stateful thinking-block detection: content inside <think>...</think>
 * is discarded in real time as chunks arrive, so the sentence buffer only
 * ever sees clean response text.
 *
 * Includes timeout flush (2s) and word-count flush (30 words) to ensure
 * the user sees text quickly even when sentences are long.
 *
 * Yields objects:
 *   { type: 'text',  content: string }
 *   { type: 'audio', data: string }   ← base64 MP3
 *   { type: 'done' }
 *
 * @param {AsyncIterable} llmStream - OpenAI streaming completion
 * @param {Object|null} ttsClient - OpenAI-compatible client for TTS, or null to skip audio
 * @param {string} voice - TTS voice name (default: 'af_bella')
 * @param {string} ttsModel - TTS model name (default: 'kokoro')
 */
async function* sentenceBufferedStream(llmStream, ttsClient, voice = 'af_bella', ttsModel = 'kokoro') {
  let outputBuffer = '';  // Accumulated clean text, ready for sentence detection
  let inThinking = false; // Whether we're currently inside a <think> block
  let lastYieldTime = Date.now();

  for await (const chunk of llmStream) {
    const rawContent = chunk.choices[0]?.delta?.content || '';
    if (!rawContent) continue;

    // Route content: thinking goes to /dev/null, everything else to outputBuffer.
    // We process the chunk string-segment by segment so <think> blocks that span
    // multiple chunks are handled correctly regardless of where the tags land.
    let remaining = rawContent;
    while (remaining.length > 0) {
      if (inThinking) {
        const closeIdx = remaining.indexOf('</think>');
        if (closeIdx === -1) {
          // Entire remaining is inside the thinking block — discard it
          remaining = '';
        } else {
          // Thinking block closes in this chunk; skip past the tag
          remaining = remaining.slice(closeIdx + '</think>'.length);
          inThinking = false;
        }
      } else {
        const openIdx = remaining.indexOf('<think>');
        if (openIdx === -1) {
          // No thinking tag — all clean output
          outputBuffer += remaining;
          remaining = '';
        } else {
          // Thinking block opens in this chunk; take everything before the tag
          outputBuffer += remaining.slice(0, openIdx);
          remaining = remaining.slice(openIdx + '<think>'.length);
          inThinking = true;
        }
      }
    }

    if (inThinking) continue; // Don't attempt sentence detection mid-think

    // Attempt to extract complete sentences from the output buffer
    let match;
    while ((match = SENTENCE_BOUNDARY.exec(outputBuffer)) !== null) {
      const sentence = match[1].trim();
      outputBuffer = match[2];

      if (sentence.length < MIN_SENTENCE_CHARS) {
        // Too short for TTS — prepend back to buffer for next sentence
        outputBuffer = sentence + ' ' + outputBuffer;
        break;
      }

      // Yield text immediately — client renders text before audio arrives
      yield { type: 'text', content: sentence };
      lastYieldTime = Date.now();

      // Generate TTS with single retry on failure
      if (ttsClient) {
        const result = await attemptTts(ttsClient, ttsModel, voice, sentence);
        if (result.success) {
          yield { type: 'audio', data: result.data };
        } else {
          yield { type: 'tts_error', message: result.message };
        }
      }
    }

    // Timeout flush: if buffer has content and 2s elapsed without yielding
    const bufferWordCount = outputBuffer.trim().split(/\s+/).filter(Boolean).length;
    if (outputBuffer.length > 20 && Date.now() - lastYieldTime > FLUSH_TIMEOUT_MS) {
      const flushed = outputBuffer.trim();
      outputBuffer = '';
      yield { type: 'text', content: flushed };
      lastYieldTime = Date.now();

      if (ttsClient) {
        const result = await attemptTts(ttsClient, ttsModel, voice, flushed);
        if (result.success) {
          yield { type: 'audio', data: result.data };
        } else {
          yield { type: 'tts_error', message: result.message };
        }
      }
    } else if (bufferWordCount >= FLUSH_WORD_COUNT) {
      // Word-count flush: too many words without a sentence boundary
      const flushed = outputBuffer.trim();
      outputBuffer = '';
      yield { type: 'text', content: flushed };
      lastYieldTime = Date.now();

      if (ttsClient) {
        const result = await attemptTts(ttsClient, ttsModel, voice, flushed);
        if (result.success) {
          yield { type: 'audio', data: result.data };
        } else {
          yield { type: 'tts_error', message: result.message };
        }
      }
    }
  }

  // Flush remaining output buffer after LLM stream ends
  const remaining = outputBuffer.trim();
  if (remaining.length >= MIN_SENTENCE_CHARS) {
    yield { type: 'text', content: remaining };
    if (ttsClient) {
      const result = await attemptTts(ttsClient, ttsModel, voice, remaining);
      if (result.success) {
        yield { type: 'audio', data: result.data };
      } else {
        yield { type: 'tts_error', message: result.message };
      }
    }
  } else if (remaining.length > 0) {
    // Tiny trailing fragment — just yield as text, skip TTS
    yield { type: 'text', content: remaining };
  }

  yield { type: 'done' };
}

/**
 * Write a single NDJSON event to an Express response stream.
 * @param {Object} res - Express response object
 * @param {Object} event - Event object to serialize
 */
function writeNdjsonEvent(res, event) {
  res.write(JSON.stringify(event) + '\n');
}

module.exports = { sentenceBufferedStream, writeNdjsonEvent, normalizeTtsText, stripThinkingFromBuffer };
