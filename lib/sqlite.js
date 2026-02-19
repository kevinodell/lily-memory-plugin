// ============================================================================
// SQLite utilities — secure version using better-sqlite3
// ============================================================================

import { createRequire } from "node:module";
import path from "node:path";
import os from "node:os";

const require = createRequire(import.meta.url);
const Database = require("better-sqlite3");

/** Singleton connection pool keyed by resolved path. */
const dbConnections = new Map();

/**
 * Get or create a database connection.
 * @param {string} dbPath - Absolute path to database file
 * @returns {import('better-sqlite3').Database}
 */
function getDb(dbPath) {
  if (!dbConnections.has(dbPath)) {
    const db = new Database(dbPath);
    db.pragma("journal_mode = WAL");
    db.pragma("busy_timeout = 5000");
    db.pragma("foreign_keys = ON");
    dbConnections.set(dbPath, db);
  }
  return dbConnections.get(dbPath);
}

/**
 * Resolve database path, expanding ~ to home directory.
 * Validates resolved path is within ~/.openclaw/memory/.
 * @param {string} configured - Configured path or null/undefined for default
 * @returns {string} Absolute path to database
 */
export function resolveDbPath(configured) {
  const raw = configured || "~/.openclaw/memory/decisions.db";
  const expanded = raw.startsWith("~") ? path.join(os.homedir(), raw.slice(1)) : raw;
  const absolute = path.resolve(expanded);
  const baseDir = path.resolve(path.join(os.homedir(), ".openclaw", "memory"));

  if (!absolute.startsWith(baseDir + path.sep) && absolute !== path.join(baseDir, path.basename(absolute))) {
    throw new Error(`Path traversal detected. Database path must be within ${baseDir}, got: ${absolute}`);
  }

  return absolute;
}

/**
 * Sanitize a value before database storage.
 * Strips null bytes and enforces length cap. Not SQL escaping —
 * parameterized queries handle injection prevention.
 * @param {*} value - Value to sanitize
 * @returns {string} Sanitized string
 */
export function sanitizeValue(value) {
  if (value === null || value === undefined) return "";
  let str = String(value);
  str = str.replace(/\0/g, "");
  if (str.length > 10000) str = str.substring(0, 10000);
  return str;
}

/**
 * Run a SQLite query with parameterized statements.
 * @param {string} dbPath - Path to database file
 * @param {string} sql - SQL query with ? placeholders
 * @param {Array} [params] - Query parameters (bound safely)
 * @returns {Array} Query results as array of objects
 */
export function sqliteQuery(dbPath, sql, params = []) {
  try {
    const db = getDb(dbPath);
    const stmt = db.prepare(sql);
    return stmt.all(...params);
  } catch (err) {
    console.error(`[lily-memory] Query failed: ${err.message}`);
    return [];
  }
}

/**
 * Run a SQLite statement (INSERT/UPDATE/DELETE) with parameterized execution.
 * @param {string} dbPath - Path to database file
 * @param {string} sql - SQL statement with ? placeholders
 * @param {Array} [params] - Statement parameters (bound safely)
 * @returns {boolean} true on success, false on error
 */
export function sqliteExec(dbPath, sql, params = []) {
  try {
    const db = getDb(dbPath);
    if (params.length > 0) {
      const stmt = db.prepare(sql);
      stmt.run(...params);
    } else {
      db.exec(sql);
    }
    return true;
  } catch (err) {
    console.error(`[lily-memory] Exec failed: ${err.message}`);
    return false;
  }
}

/**
 * Ensure all required tables exist in the database.
 * @param {string} dbPath - Path to database file
 * @returns {boolean} true if successful, false on error
 */
export function ensureTables(dbPath) {
  const schema = `
CREATE TABLE IF NOT EXISTS decisions (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    rationale TEXT NOT NULL,
    classification TEXT NOT NULL DEFAULT 'ARCHIVE',
    importance REAL NOT NULL,
    constraints TEXT,
    affected_files TEXT,
    tags TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    ttl_class TEXT DEFAULT 'active',
    expires_at INTEGER,
    last_accessed_at INTEGER,
    entity TEXT,
    fact_key TEXT,
    fact_value TEXT
);

CREATE INDEX IF NOT EXISTS idx_decisions_ttl ON decisions(ttl_class);
CREATE INDEX IF NOT EXISTS idx_decisions_expires ON decisions(expires_at);
CREATE INDEX IF NOT EXISTS idx_decisions_entity ON decisions(entity);
CREATE INDEX IF NOT EXISTS idx_decisions_fact ON decisions(entity, fact_key);
CREATE INDEX IF NOT EXISTS idx_decisions_importance ON decisions(importance DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp);

CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
    description, rationale, entity, fact_key, fact_value, tags,
    content='decisions', content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
    INSERT INTO decisions_fts(rowid, description, rationale, entity, fact_key, fact_value, tags)
    VALUES (new.rowid, new.description, new.rationale, new.entity, new.fact_key, new.fact_value, new.tags);
END;

CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
    INSERT INTO decisions_fts(decisions_fts, rowid, description, rationale, entity, fact_key, fact_value, tags)
    VALUES ('delete', old.rowid, old.description, old.rationale, old.entity, old.fact_key, old.fact_value, old.tags);
END;

CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
    INSERT INTO decisions_fts(decisions_fts, rowid, description, rationale, entity, fact_key, fact_value, tags)
    VALUES ('delete', old.rowid, old.description, old.rationale, old.entity, old.fact_key, old.fact_value, old.tags);
    INSERT INTO decisions_fts(rowid, description, rationale, entity, fact_key, fact_value, tags)
    VALUES (new.rowid, new.description, new.rationale, new.entity, new.fact_key, new.fact_value, new.tags);
END;

CREATE TABLE IF NOT EXISTS vectors (
    id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    text_content TEXT NOT NULL,
    embedding TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS entities (
    name TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    added_by TEXT NOT NULL DEFAULT 'runtime',
    added_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);

CREATE TABLE IF NOT EXISTS security_events (
    id TEXT PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    source_role TEXT,
    entity TEXT,
    fact_key TEXT,
    fact_value TEXT,
    matched_pattern TEXT,
    source_snippet TEXT
);

CREATE INDEX IF NOT EXISTS idx_security_events_timestamp ON security_events(timestamp);
  `.trim();

  try {
    const db = getDb(dbPath);
    db.exec(schema);
    return true;
  } catch (err) {
    console.error(`[lily-memory] Failed to create tables: ${err.message}`);
    return false;
  }
}

// ============================================================================
// Schema migrations
// ============================================================================

/**
 * Get the current schema version.
 * @param {string} dbPath
 * @returns {number}
 */
export function getSchemaVersion(dbPath) {
  try {
    const db = getDb(dbPath);
    // Check if schema_version table exists
    const tableCheck = db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'`
    ).all();
    if (tableCheck.length === 0) return 0;
    const row = db.prepare(`SELECT MAX(version) as v FROM schema_version`).get();
    return row?.v || 0;
  } catch {
    return 0;
  }
}

/** All migrations in order. Each has a version and an up() function. */
const MIGRATIONS = [
  {
    version: 1,
    description: "Pipeline engine tables",
    up(db) {
      db.exec(`
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    created_by TEXT NOT NULL DEFAULT 'user',
    trigger_message TEXT NOT NULL DEFAULT '',
    template_id TEXT,
    config TEXT,
    error TEXT,
    summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_created ON pipelines(created_at);

CREATE TABLE IF NOT EXISTS pipeline_steps (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
    name TEXT NOT NULL,
    step_type TEXT NOT NULL DEFAULT 'task',
    status TEXT NOT NULL DEFAULT 'pending',
    tier TEXT NOT NULL DEFAULT 'gemini-flash',
    executor TEXT NOT NULL DEFAULT 'lily',
    prompt_template TEXT NOT NULL DEFAULT '',
    depends_on_all INTEGER DEFAULT 1,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    input_artifact TEXT,
    output_artifact TEXT,
    result_summary TEXT,
    error TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_pipeline_steps_pipeline ON pipeline_steps(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_steps_status ON pipeline_steps(status);

CREATE TABLE IF NOT EXISTS pipeline_edges (
    parent_step_id TEXT NOT NULL REFERENCES pipeline_steps(id),
    child_step_id TEXT NOT NULL REFERENCES pipeline_steps(id),
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
    condition TEXT,
    PRIMARY KEY (parent_step_id, child_step_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_edges_pipeline ON pipeline_edges(pipeline_id);

CREATE TABLE IF NOT EXISTS pipeline_triggers (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
    schedule TEXT NOT NULL,
    timezone TEXT DEFAULT 'America/New_York',
    enabled INTEGER DEFAULT 1,
    last_fired INTEGER,
    next_fire INTEGER
);

CREATE INDEX IF NOT EXISTS idx_pipeline_triggers_enabled ON pipeline_triggers(enabled);
      `.trim());
    },
  },
];

/**
 * Run all pending migrations.
 * @param {string} dbPath
 * @returns {{ applied: number, current: number }}
 */
export function runMigrations(dbPath) {
  const db = getDb(dbPath);

  // Ensure schema_version table exists
  db.exec(`CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    description TEXT,
    applied_at INTEGER NOT NULL
  )`);

  const currentVersion = getSchemaVersion(dbPath);
  let applied = 0;

  for (const migration of MIGRATIONS) {
    if (migration.version <= currentVersion) continue;

    try {
      const txn = db.transaction(() => {
        migration.up(db);
        db.prepare(
          `INSERT INTO schema_version (version, description, applied_at) VALUES (?, ?, ?)`
        ).run(migration.version, migration.description, Date.now());
      });
      txn();
      applied++;
      console.log(`[lily-memory] Migration ${migration.version}: ${migration.description}`);
    } catch (err) {
      console.error(`[lily-memory] Migration ${migration.version} failed: ${err.message}`);
      throw err;
    }
  }

  return { applied, current: currentVersion + applied };
}

/**
 * Close all database connections (cleanup on shutdown).
 */
export function closeAllConnections() {
  for (const db of dbConnections.values()) {
    try { db.close(); } catch {}
  }
  dbConnections.clear();
}
