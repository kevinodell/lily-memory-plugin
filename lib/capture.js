// ============================================================================
// Auto-capture: extract facts from messages (with entity validation + limits)
// ============================================================================

import { sqliteQuery, sqliteExec, escapeSqlValue } from "./sqlite.js";
import { extractFacts, MAX_VALUE_LENGTH } from "./extraction.js";
import { randomUUID } from "node:crypto";

/** Maximum active entries in the database. Evict lowest-importance when exceeded. */
export const MAX_ACTIVE_ENTRIES = 50;

/** Maximum stable entries in the database. */
export const MAX_STABLE_ENTRIES = 30;

/** Status keywords that should auto-downgrade to session TTL. */
const STATUS_KEYWORDS = /(?:status|complete|deployed|spawned|launched|ready|sprint|checklist|final_state|session_|restart|attempt|debug|fix_|milestone|infrastructure|live_|validation_)/i;

/**
 * Capture facts from conversation messages and store them in the database.
 * Enforces value length limits and global entry count caps.
 *
 * @param {string} dbPath - Path to SQLite database
 * @param {Array} messages - Array of {role, content} message objects
 * @param {number} maxCapture - Maximum number of facts to capture this call
 * @param {Set<string>} runtimeEntities - Set of lowercase known entity names
 * @param {Function|null} logger - logger(msg) for status; falls back to console.log
 * @returns {{ stored: number, newDecisionIds: Array<{id: string, text: string}> }}
 */
export function captureFromMessages(dbPath, messages, maxCapture, runtimeEntities, logger) {
  const log = logger ?? console.log;
  const nowMs = Date.now();
  let stored = 0;
  const newDecisionIds = [];

  // Flatten messages to { role, text } pairs
  const texts = [];
  for (const msg of messages) {
    if (!msg || typeof msg !== "object") continue;
    const role = msg.role;
    if (role !== "user" && role !== "assistant") continue;

    const content = msg.content;
    if (typeof content === "string") {
      texts.push({ role, text: content });
      continue;
    }
    if (Array.isArray(content)) {
      for (const block of content) {
        if (block && typeof block === "object" && block.type === "text" && typeof block.text === "string") {
          texts.push({ role, text: block.text });
        }
      }
    }
  }

  for (const { role, text } of texts) {
    if (stored >= maxCapture) break;

    // Skip injected memory context
    if (text.includes("<lily-memory>") || text.includes("<relevant-memories>")) continue;
    // Skip very short or very long
    if (text.length < 30 || text.length > 5000) continue;

    const facts = extractFacts(text, runtimeEntities);
    for (const fact of facts) {
      if (stored >= maxCapture) break;

      // Enforce value length cap (defense-in-depth with extraction.js)
      if (fact.value.length > MAX_VALUE_LENGTH) continue;

      const entity = escapeSqlValue(fact.entity);
      const key = escapeSqlValue(fact.key);
      const value = escapeSqlValue(fact.value);

      // Check if this fact already exists
      const existing = sqliteQuery(dbPath, `SELECT id FROM decisions WHERE entity = '${entity}' AND fact_key = '${key}' AND (expires_at IS NULL OR expires_at > ${nowMs}) LIMIT 1`);

      if (existing.length > 0) {
        const existingId = escapeSqlValue(existing[0].id);
        sqliteExec(dbPath, `UPDATE decisions SET fact_value = '${value}', description = '${escapeSqlValue(`${fact.entity}.${fact.key} = ${fact.value}`)}', timestamp = ${nowMs}, last_accessed_at = ${nowMs} WHERE id = '${existingId}'`);
        newDecisionIds.push({ id: existing[0].id, text: `${fact.entity}.${fact.key} = ${fact.value}` });
        log(`lily-memory: updated fact ${fact.entity}.${fact.key}`);
        continue;
      }

      // Heuristic TTL assignment — status facts get session TTL (24h)
      let ttlClass = role === "user" ? "stable" : "active";
      if (STATUS_KEYWORDS.test(fact.key)) {
        ttlClass = "session";
      }

      const ttlMs = { permanent: null, stable: 90 * 86400000, active: 14 * 86400000, session: 86400000 };
      const expiresAt = ttlMs[ttlClass] === null ? "NULL" : nowMs + ttlMs[ttlClass];

      // Heuristic importance
      const importance = role === "user" ? 0.75 : 0.6;

      // Enforce global active entry cap — evict lowest-importance if full
      if (ttlClass === "active") {
        const countResult = sqliteQuery(dbPath, `SELECT COUNT(*) as cnt FROM decisions WHERE ttl_class = 'active' AND (expires_at IS NULL OR expires_at > ${nowMs})`);
        if (countResult[0]?.cnt >= MAX_ACTIVE_ENTRIES) {
          const evicted = sqliteQuery(dbPath, `SELECT id FROM decisions WHERE ttl_class = 'active' AND (expires_at IS NULL OR expires_at > ${nowMs}) ORDER BY importance ASC, timestamp ASC LIMIT 1`);
          if (evicted.length > 0) {
            const evictId = escapeSqlValue(evicted[0].id);
            sqliteExec(dbPath, `DELETE FROM decisions WHERE id = '${evictId}'`);
            sqliteExec(dbPath, `DELETE FROM vectors WHERE decision_id = '${evictId}'`);
            log(`lily-memory: evicted low-importance active entry to make room`);
          }
        }
      }

      // Enforce global stable entry cap
      if (ttlClass === "stable") {
        const countResult = sqliteQuery(dbPath, `SELECT COUNT(*) as cnt FROM decisions WHERE ttl_class = 'stable' AND (expires_at IS NULL OR expires_at > ${nowMs})`);
        if (countResult[0]?.cnt >= MAX_STABLE_ENTRIES) {
          const evicted = sqliteQuery(dbPath, `SELECT id FROM decisions WHERE ttl_class = 'stable' AND (expires_at IS NULL OR expires_at > ${nowMs}) ORDER BY importance ASC, timestamp ASC LIMIT 1`);
          if (evicted.length > 0) {
            const evictId = escapeSqlValue(evicted[0].id);
            sqliteExec(dbPath, `DELETE FROM decisions WHERE id = '${evictId}'`);
            sqliteExec(dbPath, `DELETE FROM vectors WHERE decision_id = '${evictId}'`);
            log(`lily-memory: evicted low-importance stable entry to make room`);
          }
        }
      }

      const id = randomUUID();
      const description = escapeSqlValue(`${fact.entity}.${fact.key} = ${fact.value}`);
      const ok = sqliteExec(dbPath, `INSERT INTO decisions (id, session_id, timestamp, category, description, rationale, classification, importance, ttl_class, expires_at, last_accessed_at, entity, fact_key, fact_value, tags) VALUES ('${escapeSqlValue(id)}', 'plugin-auto', ${nowMs}, 'auto-capture', '${description}', 'Auto-captured by lily-memory plugin v5', 'ARCHIVE', ${importance}, '${ttlClass}', ${expiresAt}, ${nowMs}, '${entity}', '${key}', '${value}', '["auto-capture","v5"]')`);

      if (ok) {
        stored++;
        newDecisionIds.push({ id, text: `${fact.entity}.${fact.key} = ${fact.value}` });
        log(`lily-memory: captured ${fact.entity}.${fact.key} = ${fact.value}`);
      }
    }
  }

  return { stored, newDecisionIds };
}
