'use strict';

// Matches sentence boundaries: end of text followed by .!? and optional whitespace
// Handles abbreviations poorly intentionally — simplicity > perfection for TTS chunking
const SENTENCE_BOUNDARY = /^(.*?[.!?])\s+(.*)/s;
// Minimum sentence length to avoid TTS calls on tiny fragments
const MIN_SENTENCE_CHARS = 8;

/**
 * Normalize text before sending to TTS.
 * Expands brand acronyms so the TTS engine pronounces them correctly
 * and strips markdown/special characters that would be read aloud.
 * @param {string} text
 * @returns {string}
 */
function normalizeTtsText(text) {
  return text
    // Brand acronym expansion for natural TTS pronunciation
    .replace(/\bQEGG\b/g, 'Q. E. G. G.')
    .replace(/\bDRGFC\b/g, 'D. R. G. F. C.')
    .replace(/\bHMSS\b/g, 'H. M. S. S.')
    .replace(/\bBCPS\b/g, 'B. C. P. S.')
    .replace(/\bQUAD\b/g, 'Q. U. A. D.')
    .replace(/\bLWIS\b/g, 'L. W. I. S.')
    .replace(/\bSPTS\b/g, 'S. P. T. S.')
    .replace(/\bUAIMC\b/g, 'U. A. I. M. C.')
    // Strip markdown that TTS would read aloud
    .replace(/\*\*([^*]+)\*\*/g, '$1')   // **bold**
    .replace(/\*([^*]+)\*/g, '$1')       // *italic*
    .replace(/`([^`]+)`/g, '$1')         // `code`
    .replace(/—/g, ', ')                 // em-dash → pause
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Sentence-buffered streaming pipeline.
 * Consumes an LLM stream, detects sentence boundaries,
 * calls TTS on each sentence, and yields NDJSON events.
 *
 * Yields objects:
 *   { type: 'text',  content: string }
 *   { type: 'audio', data: string }   ← base64 MP3
 *   { type: 'done' }
 *
 * @param {AsyncIterable} llmStream - OpenAI streaming completion
 * @param {Object} ttsClient - OpenAI client (pointed at DeepInfra) for TTS
 * @param {string} voice - TTS voice name (default: 'aura-asteria-en')
 */
async function* sentenceBufferedStream(llmStream, ttsClient, voice = 'aura-asteria-en') {
  let buffer = '';

  for await (const chunk of llmStream) {
    const content = chunk.choices[0]?.delta?.content || '';
    if (!content) continue;
    buffer += content;

    // Attempt to extract complete sentences from the buffer
    let match;
    while ((match = SENTENCE_BOUNDARY.exec(buffer)) !== null) {
      const sentence = match[1].trim();
      buffer = match[2];

      if (sentence.length < MIN_SENTENCE_CHARS) {
        // Too short for TTS — prepend back to buffer for next sentence
        buffer = sentence + ' ' + buffer;
        break;
      }

      // Yield text immediately — client renders text before audio arrives
      yield { type: 'text', content: sentence };

      // Generate TTS for this sentence (normalize before sending)
      try {
        const audio = await ttsClient.audio.speech.create({
          model: 'openai/tts-1',
          voice,
          input: normalizeTtsText(sentence),
          response_format: 'mp3'
        });
        const audioBuffer = Buffer.from(await audio.arrayBuffer());
        yield { type: 'audio', data: audioBuffer.toString('base64') };
      } catch (err) {
        console.warn(`TTS failed for sentence: ${err.message}`);
        // Yield error event but keep streaming text
        yield { type: 'tts_error', message: err.message };
      }
    }
  }

  // Flush remaining buffer after LLM stream ends
  const remaining = buffer.trim();
  if (remaining.length >= MIN_SENTENCE_CHARS) {
    yield { type: 'text', content: remaining };
    try {
      const audio = await ttsClient.audio.speech.create({
        model: 'openai/tts-1',
        voice,
        input: normalizeTtsText(remaining),
        response_format: 'mp3'
      });
      const audioBuffer = Buffer.from(await audio.arrayBuffer());
      yield { type: 'audio', data: audioBuffer.toString('base64') };
    } catch (err) {
      console.warn(`TTS flush failed: ${err.message}`);
      yield { type: 'tts_error', message: err.message };
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

module.exports = { sentenceBufferedStream, writeNdjsonEvent, normalizeTtsText };
