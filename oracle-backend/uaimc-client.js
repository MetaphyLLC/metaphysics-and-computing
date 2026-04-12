'use strict';

const UAIMC_URL = process.env.UAIMC_URL || 'http://localhost:8765';

/**
 * Query UAIMC for relevant knowledge snippets.
 * @param {string} query - The user's query
 * @param {number} limit - Max results to return
 * @returns {Promise<Object>} UAIMC query response
 */
async function queryUAIMC(query, limit = 5) {
  const url = `${UAIMC_URL}/query?q=${encodeURIComponent(query)}&limit=${limit}`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(8000), keepalive: true });
  if (!resp.ok) throw new Error(`UAIMC query failed: ${resp.status}`);
  return resp.json();
}

/**
 * Get assembled context for Oracle agent, optionally scoped to a query.
 * @param {string} query - Optional query to scope context retrieval
 * @returns {Promise<Object>} UAIMC context response
 */
async function getOracleContext(query) {
  const q = query ? `&q=${encodeURIComponent(query)}` : '';
  const url = `${UAIMC_URL}/context?agent=ORACLE${q}`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(8000), keepalive: true });
  if (!resp.ok) throw new Error(`UAIMC context failed: ${resp.status}`);
  return resp.json();
}

/**
 * Ingest an Oracle conversation into UAIMC.
 * @param {Array<{role: string, content: string}>} messages - Conversation turns
 * @param {string} sessionId - Session identifier
 */
async function ingestConversation(messages, sessionId) {
  const content = messages
    .map(m => `${m.role}: ${m.content}`)
    .join('\n');

  const payload = {
    content,
    source: 'oracle_conversation',
    author: 'ORACLE',
    channel: 'website',
    metadata: { sessionId, timestamp: new Date().toISOString() }
  };

  const resp = await fetch(`${UAIMC_URL}/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(10000),
    keepalive: true
  });

  if (!resp.ok && resp.status !== 405 && resp.status !== 501) {
    // Suppress warnings for 405/501 — UAIMC-lite in query_only mode rejects writes by design
    console.warn(`UAIMC ingest failed: ${resp.status}`);
  }
}

/**
 * Query CANS (Continuous Associative Neural Substrate) 3D map search.
 * @param {string} query - The user's query
 * @param {number} limit - Max results to return
 * @returns {Promise<Object>} CANS search response
 */
async function queryCANS(query, limit = 5) {
  const url = `${UAIMC_URL}/api/v1/3d-map/search?q=${encodeURIComponent(query)}&limit=${limit}`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(8000), keepalive: true });
  if (!resp.ok) throw new Error(`CANS query failed: ${resp.status}`);
  return resp.json();
}

/**
 * Get CANS-mode context for Oracle agent.
 * @param {string} query - Query to scope context retrieval
 * @returns {Promise<Object>} CANS context response
 */
async function getCANSContext(query) {
  const url = `${UAIMC_URL}/context?agent=ORACLE&topic=${encodeURIComponent(query)}&mode=cans`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(8000), keepalive: true });
  if (!resp.ok) throw new Error(`CANS context failed: ${resp.status}`);
  return resp.json();
}

/**
 * Check CANS endpoint health.
 * @returns {Promise<boolean>} true if CANS is available
 */
async function checkCANSHealth() {
  try {
    const resp = await fetch(
      `${UAIMC_URL}/api/v1/3d-map/search?q=health&limit=1`,
      { signal: AbortSignal.timeout(3000), keepalive: true }
    );
    return resp.ok;
  } catch {
    return false;
  }
}

/**
 * Check UAIMC health.
 * @returns {Promise<boolean>} true if healthy
 */
async function checkUAIMCHealth() {
  try {
    const resp = await fetch(`${UAIMC_URL}/health`, { signal: AbortSignal.timeout(3000), keepalive: true });
    const data = await resp.json();
    return data.status === 'healthy';
  } catch {
    return false;
  }
}

/**
 * Build a combined RAG context string.
 * Tries CANS context first for richer relational knowledge,
 * falls back to legacy query + context endpoints.
 * @param {string} userMessage - The user's message
 * @returns {Promise<string>} Formatted context string for system prompt injection
 */
async function buildRagContext(userMessage) {
  try {
    // Try CANS-enriched context first
    const cansResult = await getCANSContext(userMessage).catch(() => null);
    if (cansResult?.context) {
      // CANS mode returns a structured context string with === FACTS ===, === REFLECTIONS ===, === EPISODES === sections
      // and a sections object with counts: { facts: N, reflections: N, episodes: N }
      const totalSections = (cansResult.sections?.facts || 0) + (cansResult.sections?.reflections || 0) + (cansResult.sections?.episodes || 0);
      if (totalSections > 0) {
        return cansResult.context.substring(0, 3000);
      }
      // Even without structured sections, the context string has useful content
      return cansResult.context.substring(0, 2000);
    }

    // Fall back to legacy UAIMC endpoints
    const [queryResult, contextResult] = await Promise.allSettled([
      queryUAIMC(userMessage, 5),
      getOracleContext(userMessage)
    ]);

    const parts = [];

    if (queryResult.status === 'fulfilled' && queryResult.value.results?.length) {
      const snippets = queryResult.value.results
        .filter(r => r.relevance_score > 0.5)
        .slice(0, 4)
        .map(r => `[${r.source}] ${r.summary}`.substring(0, 400));
      if (snippets.length) {
        parts.push('RELEVANT KNOWLEDGE:\n' + snippets.join('\n---\n'));
      }
    }

    if (contextResult.status === 'fulfilled' && contextResult.value.context) {
      parts.push(contextResult.value.context.substring(0, 2000));
    }

    return parts.length ? parts.join('\n\n') : 'No specific context available.';
  } catch (err) {
    console.warn('RAG context build failed:', err.message);
    return 'Knowledge base temporarily unavailable.';
  }
}

/**
 * Query UAIMC's intelligent oracle-search for 3D map nodes.
 * Uses the full query pipeline (BM25, SimHash, graph ranking) instead of basic FTS5.
 * @param {string} query - Natural language query
 * @param {number} limit - Max nodes to return
 * @returns {Promise<Object>} 3D-map-compatible node results
 */
async function queryOracleSearch(query, limit = 30) {
  const url = `${UAIMC_URL}/api/v1/3d-map/story-search?q=${encodeURIComponent(query)}&limit=${limit}`;
  const resp = await fetch(url, { signal: AbortSignal.timeout(10000), keepalive: true });
  if (!resp.ok) throw new Error(`Oracle search failed: ${resp.status}`);
  return resp.json();
}

module.exports = {
  queryUAIMC, getOracleContext, ingestConversation,
  checkUAIMCHealth, buildRagContext,
  queryCANS, getCANSContext, checkCANSHealth,
  queryOracleSearch
};
