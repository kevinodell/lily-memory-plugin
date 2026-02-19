#!/usr/bin/env node
// ============================================================================
// Pipeline Scheduler — standalone tick loop for autonomous pipeline execution
// Run via launchd/cron every 60 seconds. Reads state from SQLite, dispatches
// ready steps to executors (Ollama, Gemini API), writes results back.
// ============================================================================

import { resolveDbPath, sqliteQuery, sqliteExec, ensureTables } from "./sqlite.js";
import { runMigrations } from "./sqlite.js";
import { loadDAG, readySteps, skippableSteps, checkPipelineComplete } from "./graph.js";
import { advanceStep, startPipeline } from "./pipeline.js";

const LOG_PREFIX = "[scheduler]";

function log(msg) {
  const ts = new Date().toISOString();
  console.log(`${ts} ${LOG_PREFIX} ${msg}`);
}

function logError(msg) {
  const ts = new Date().toISOString();
  console.error(`${ts} ${LOG_PREFIX} ERROR: ${msg}`);
}

// ============================================================================
// Cron expression evaluation
// ============================================================================

/**
 * Check if a cron expression matches the current time.
 * Supports: *, specific numbers, ranges (1-5), lists (1,3,5), intervals (asterisk/15).
 * @param {string} expr - 5-field cron expression
 * @param {Date} date
 * @returns {boolean}
 */
function cronMatches(expr, date) {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) return false;

  const fields = [
    date.getMinutes(),
    date.getHours(),
    date.getDate(),
    date.getMonth() + 1,
    date.getDay(),
  ];

  for (let i = 0; i < 5; i++) {
    if (!fieldMatches(parts[i], fields[i])) return false;
  }
  return true;
}

function fieldMatches(pattern, value) {
  if (pattern === "*") return true;

  // Handle */N (every N)
  if (pattern.startsWith("*/")) {
    const interval = parseInt(pattern.slice(2), 10);
    return !isNaN(interval) && interval > 0 && value % interval === 0;
  }

  // Handle comma-separated values
  const segments = pattern.split(",");
  for (const seg of segments) {
    // Range: 1-5
    if (seg.includes("-")) {
      const [lo, hi] = seg.split("-").map(Number);
      if (!isNaN(lo) && !isNaN(hi) && value >= lo && value <= hi) return true;
    } else {
      if (parseInt(seg, 10) === value) return true;
    }
  }
  return false;
}

/**
 * Check if a trigger should fire now.
 * Prevents double-firing within the same minute.
 * @param {object} trigger
 * @param {Date} now
 * @returns {boolean}
 */
function shouldFire(trigger, now) {
  if (!trigger.enabled) return false;

  // Check if cron matches current time
  if (!cronMatches(trigger.schedule, now)) return false;

  // Prevent double-fire: skip if last_fired was within the same minute
  if (trigger.last_fired) {
    const lastDate = new Date(trigger.last_fired);
    if (lastDate.getFullYear() === now.getFullYear() &&
        lastDate.getMonth() === now.getMonth() &&
        lastDate.getDate() === now.getDate() &&
        lastDate.getHours() === now.getHours() &&
        lastDate.getMinutes() === now.getMinutes()) {
      return false;
    }
  }

  return true;
}

// ============================================================================
// Step dispatch
// ============================================================================

/**
 * Dispatch a step to its executor.
 * @param {object} step - Step row from pipeline_steps
 * @param {object} graph - Loaded DAG
 * @param {object} config - Scheduler config (API keys, etc.)
 * @returns {Promise<{ success: boolean, output?: string, error?: string }>}
 */
async function dispatchStep(step, graph, config) {
  // Build input from parent outputs
  const parentIds = graph.parents[step.id] || [];
  const parentOutputs = parentIds
    .map(pid => graph.steps[pid])
    .filter(p => p && p.status === "complete" && p.output_artifact)
    .map(p => `[${p.name}]: ${p.output_artifact}`)
    .join("\n\n");

  // Replace {{prev_result}} placeholder in prompt
  let prompt = step.prompt_template || "";
  prompt = prompt.replace(/\{\{prev_result\}\}/g, parentOutputs);
  prompt = prompt.replace(/\{\{parent_outputs\}\}/g, parentOutputs);

  // Add parent context if not in template
  if (!prompt.includes(parentOutputs) && parentOutputs) {
    prompt = `Previous step outputs:\n${parentOutputs}\n\n---\n\n${prompt}`;
  }

  const executor = step.executor || "lily";
  const tier = step.tier || "gemini-flash";

  try {
    if (executor === "local" || tier.startsWith("deepseek") || tier.startsWith("qwen")) {
      return await dispatchOllama(prompt, tier, config);
    }

    if (tier === "gemini-flash" || tier.startsWith("gemini")) {
      return await dispatchGemini(prompt, tier, config);
    }

    // Fallback: try Ollama with a default model
    log(`No specific dispatch for executor=${executor} tier=${tier}, trying Ollama`);
    return await dispatchOllama(prompt, "qwen3:30b", config);
  } catch (err) {
    return { success: false, error: err.message };
  }
}

async function dispatchOllama(prompt, model, config) {
  const ollamaUrl = config.ollamaUrl || "http://localhost:11434";
  const modelName = model.includes(":") ? model : `${model}:latest`;

  const response = await fetch(`${ollamaUrl}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: modelName,
      prompt,
      stream: false,
      options: { num_predict: 4096 },
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    return { success: false, error: `Ollama ${response.status}: ${text.substring(0, 200)}` };
  }

  const data = await response.json();
  return { success: true, output: data.response || "" };
}

async function dispatchGemini(prompt, model, config) {
  const apiKey = config.geminiApiKey || process.env.GEMINI_API_KEY;
  if (!apiKey) {
    return { success: false, error: "GEMINI_API_KEY not configured" };
  }

  const modelId = model === "gemini-flash" ? "gemini-2.5-flash-preview-05-20" : model;
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelId}:generateContent?key=${apiKey}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { maxOutputTokens: 4096 },
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    return { success: false, error: `Gemini ${response.status}: ${text.substring(0, 200)}` };
  }

  const data = await response.json();
  const output = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
  return { success: true, output };
}

// ============================================================================
// Main tick
// ============================================================================

async function tick(dbPath, config) {
  const now = new Date();
  log(`tick at ${now.toISOString()}`);

  // 1. Check triggers
  const triggers = sqliteQuery(dbPath,
    `SELECT t.*, p.name as pipeline_name, p.id as source_pipeline_id
     FROM pipeline_triggers t
     JOIN pipelines p ON t.pipeline_id = p.id
     WHERE t.enabled = 1`
  );

  for (const trigger of triggers) {
    if (!shouldFire(trigger, now)) continue;

    log(`trigger fired: "${trigger.pipeline_name}" (${trigger.schedule})`);

    // Check for already-running instance from same source
    const running = sqliteQuery(dbPath,
      `SELECT COUNT(*) as cnt FROM pipelines
       WHERE name = ? AND status IN ('pending', 'running', 'paused')`,
      [trigger.pipeline_name]
    );

    if (running[0]?.cnt > 0) {
      log(`skipping trigger: "${trigger.pipeline_name}" already has an active instance`);
      sqliteExec(dbPath,
        `UPDATE pipeline_triggers SET last_fired = ? WHERE id = ?`,
        [now.getTime(), trigger.id]
      );
      continue;
    }

    // Instantiate a new pipeline run from the source pipeline's steps
    const sourceSteps = sqliteQuery(dbPath,
      `SELECT * FROM pipeline_steps WHERE pipeline_id = ?`,
      [trigger.source_pipeline_id]
    );
    const sourceEdges = sqliteQuery(dbPath,
      `SELECT * FROM pipeline_edges WHERE pipeline_id = ?`,
      [trigger.source_pipeline_id]
    );

    if (sourceSteps.length > 0) {
      // Create a new instance by cloning the source pipeline's structure
      const { randomUUID } = await import("node:crypto");
      const newPipelineId = randomUUID();
      const stepIdMap = {};

      sqliteExec(dbPath,
        `INSERT INTO pipelines (id, name, status, created_at, updated_at, created_by, trigger_message, config)
         VALUES (?, ?, 'running', ?, ?, 'scheduler', ?, NULL)`,
        [newPipelineId, trigger.pipeline_name, now.getTime(), now.getTime(), `Triggered by schedule: ${trigger.schedule}`]
      );

      for (const s of sourceSteps) {
        const newStepId = randomUUID();
        stepIdMap[s.id] = newStepId;
        sqliteExec(dbPath,
          `INSERT INTO pipeline_steps (id, pipeline_id, name, step_type, status, tier, executor, prompt_template, depends_on_all, created_at, max_retries)
           VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)`,
          [newStepId, newPipelineId, s.name, s.step_type, s.tier, s.executor, s.prompt_template, s.depends_on_all, now.getTime(), s.max_retries]
        );
      }

      for (const e of sourceEdges) {
        const newParent = stepIdMap[e.parent_step_id];
        const newChild = stepIdMap[e.child_step_id];
        if (newParent && newChild) {
          sqliteExec(dbPath,
            `INSERT INTO pipeline_edges (parent_step_id, child_step_id, pipeline_id, condition)
             VALUES (?, ?, ?, ?)`,
            [newParent, newChild, newPipelineId, e.condition]
          );
        }
      }

      log(`instantiated new run for "${trigger.pipeline_name}" (${newPipelineId})`);
    }

    sqliteExec(dbPath,
      `UPDATE pipeline_triggers SET last_fired = ?, next_fire = ? WHERE id = ?`,
      [now.getTime(), now.getTime() + 60000, trigger.id]
    );
  }

  // 2. Process running pipelines
  const pipelines = sqliteQuery(dbPath,
    `SELECT * FROM pipelines WHERE status = 'running'`
  );

  for (const pipeline of pipelines) {
    const graph = loadDAG(dbPath, pipeline.id);

    // Mark skippable steps
    const toSkip = skippableSteps(graph);
    for (const skipId of toSkip) {
      sqliteExec(dbPath,
        `UPDATE pipeline_steps SET status = 'skipped', completed_at = ? WHERE id = ?`,
        [now.getTime(), skipId]
      );
      log(`skipped step "${graph.steps[skipId]?.name}" in "${pipeline.name}"`);
    }

    // Reload after skips
    const updatedGraph = toSkip.length > 0 ? loadDAG(dbPath, pipeline.id) : graph;

    // Check completion
    const completion = checkPipelineComplete(updatedGraph);
    if (completion.complete) {
      sqliteExec(dbPath,
        `UPDATE pipelines SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?`,
        [completion.status, now.getTime(), now.getTime(), pipeline.id]
      );
      log(`pipeline "${pipeline.name}" ${completion.status}`);
      continue;
    }

    // Find and dispatch ready steps
    const ready = readySteps(updatedGraph);
    for (const stepId of ready) {
      const step = updatedGraph.steps[stepId];

      // Mark as running
      sqliteExec(dbPath,
        `UPDATE pipeline_steps SET status = 'running', started_at = ? WHERE id = ?`,
        [now.getTime(), stepId]
      );

      log(`dispatching step "${step.name}" (${step.tier}/${step.executor}) in "${pipeline.name}"`);

      try {
        const result = await dispatchStep(step, updatedGraph, config);

        if (result.success) {
          log(`step "${step.name}" completed successfully`);
        } else {
          log(`step "${step.name}" failed: ${result.error}`);
        }

        // Advance the step
        advanceStep(dbPath, stepId, {
          output: result.output || "",
          success: result.success,
          error: result.error,
          summary: (result.output || "").substring(0, 500),
        });
      } catch (err) {
        logError(`step "${step.name}" dispatch error: ${err.message}`);
        advanceStep(dbPath, stepId, {
          output: "",
          success: false,
          error: err.message,
        });
      }
    }
  }

  // 3. Detect stuck steps (running > 30 min with no progress)
  const stuckThreshold = now.getTime() - 30 * 60 * 1000;
  const stuck = sqliteQuery(dbPath,
    `SELECT s.id, s.name, s.pipeline_id, p.name as pipeline_name
     FROM pipeline_steps s
     JOIN pipelines p ON s.pipeline_id = p.id
     WHERE s.status = 'running' AND s.started_at < ? AND s.started_at IS NOT NULL`,
    [stuckThreshold]
  );

  for (const s of stuck) {
    log(`stuck step detected: "${s.name}" in "${s.pipeline_name}" (running > 30 min)`);
    advanceStep(dbPath, s.id, {
      output: "",
      success: false,
      error: "Step timed out (running > 30 minutes)",
    });
  }

  log(`tick complete: ${pipelines.length} pipelines, ${triggers.length} triggers checked`);
}

// ============================================================================
// Entry point
// ============================================================================

async function main() {
  const dbPath = resolveDbPath(process.env.LILY_DB_PATH || null);

  const config = {
    ollamaUrl: process.env.OLLAMA_URL || "http://localhost:11434",
    geminiApiKey: process.env.GEMINI_API_KEY || "",
  };

  // Ensure tables exist (including pipeline tables via migrations)
  ensureTables(dbPath);
  runMigrations(dbPath);

  await tick(dbPath, config);
}

main().catch(err => {
  logError(`Fatal: ${err.message}`);
  process.exit(1);
});

export { tick, cronMatches, shouldFire, dispatchStep };
