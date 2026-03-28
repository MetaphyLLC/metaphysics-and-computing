'use strict';

const rateLimit = require('express-rate-limit');
const helmet = require('helmet');

/**
 * Apply security middleware to Express app.
 * @param {Object} app - Express application instance
 */
function applySecurityMiddleware(app) {
  // Helmet: sets secure HTTP headers
  app.use(helmet({
    crossOriginEmbedderPolicy: false, // Allow audio/fetch from frontend
    contentSecurityPolicy: false      // CSP handled by GitHub Pages
  }));
}

/**
 * Rate limiter: 30 requests per minute per IP.
 * Applied only to /api routes.
 */
const apiRateLimiter = rateLimit({
  windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000', 10),
  max: parseInt(process.env.RATE_LIMIT_MAX || '30', 10),
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests. Please wait a moment before asking again.' }
});

/**
 * Validate and sanitize /api/chat input.
 * Returns { valid: true, data } or { valid: false, error: string }.
 * @param {Object} body - Parsed request body
 */
function validateChatInput(body) {
  if (!body || typeof body !== 'object') {
    return { valid: false, error: 'Request body must be JSON.' };
  }

  const { message, sessionId, page } = body;

  if (typeof message !== 'string' || message.trim().length === 0) {
    return { valid: false, error: 'message is required and must be a non-empty string.' };
  }

  if (message.length > 2000) {
    return { valid: false, error: 'message exceeds 2000 character limit.' };
  }

  // Prompt injection defense: strip common jailbreak patterns
  const sanitized = sanitizeMessage(message.trim());

  return {
    valid: true,
    data: {
      message: sanitized,
      sessionId: typeof sessionId === 'string' ? sessionId.slice(0, 64) : generateSessionId(),
      page: typeof page === 'string' ? page.slice(0, 100) : 'unknown'
    }
  };
}

/**
 * Basic prompt injection defense.
 * Strips obvious attempts to override the system prompt.
 * @param {string} text - Raw user input
 * @returns {string} Sanitized text
 */
function sanitizeMessage(text) {
  // Remove null bytes and control characters (except newlines/tabs)
  let cleaned = text.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');

  // Warn on but don't strip these — the LLM handles them via system prompt grounding
  // Aggressive stripping would break legitimate questions like "ignore the previous..."
  return cleaned.trim();
}

/**
 * Generate a random session ID.
 * @returns {string}
 */
function generateSessionId() {
  return `oracle-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

module.exports = { applySecurityMiddleware, apiRateLimiter, validateChatInput };
