// ============================================================================
// Pipeline Engine — DAG-based workflow execution for Lily
// ============================================================================

import { randomUUID } from "node:crypto";
import { sqliteQuery, sqliteExec, sanitizeValue } from "./sqlite.js";
import { buildDAG, validateDAG, loadDAG, readySteps, skippableSteps, checkPipelineComplete } from "./graph.js";

/** Max chars for artifact storage inline in SQLite. */
const ARTIFACT_MAX_CHARS = 65536;

// ============================================================================
// Core pipeline operations
// ============================================================================

/**
 * Create a pipeline from structured input.
 * @param {string} dbPath
 * @param {object} input - { name, trigger_message, steps: [...], config }
 * @returns {{ success: boolean, pipelineId?: string, error?: string }}
 */
export function createPipeline(dbPath, input) {
  const { name, trigger_message, steps: stepDefs, config } = input;

  if (!name || !stepDefs || !Array.isArray(stepDefs) || stepDefs.length === 0) {
    return { success: false, error: "Pipeline requires a name and at least one step" };
  }

  const pipelineId = randomUUID();
  const now = Date.now();

  // Build step objects with generated IDs
  const stepsWithIds = stepDefs.map(s => ({
    id: randomUUID(),
    name: s.name,
    step_type: s.step_type || "task",
    tier: s.tier || "gemini-flash",
    executor: s.executor || "lily",
    prompt_template: s.prompt || s.prompt_template || "",
    depends_on_all: s.depends_on_all !== undefined ? (s.depends_on_all ? 1 : 0) : 1,
    max_retries: s.max_retries ?? 1,
    status: "pending",
  }));

  // Build name-to-ID map
  const nameToId = {};
  for (const s of stepsWithIds) {
    if (nameToId[s.name]) {
      return { success: false, error: `Duplicate step name: "${s.name}"` };
    }
    nameToId[s.name] = s.id;
  }

  // Build edges from depends_on
  const edges = [];
  for (let i = 0; i < stepDefs.length; i++) {
    const deps = stepDefs[i].depends_on;
    if (!deps || !Array.isArray(deps)) continue;

    const childId = stepsWithIds[i].id;

    for (const dep of deps) {
      if (typeof dep === "string") {
        // Simple dependency (unconditional)
        const parentId = nameToId[dep];
        if (!parentId) {
          return { success: false, error: `Step "${stepsWithIds[i].name}" depends on unknown step "${dep}"` };
        }
        edges.push({ parent_step_id: parentId, child_step_id: childId, condition: null });
      } else if (dep && typeof dep === "object" && dep.step) {
        // Conditional dependency
        const parentId = nameToId[dep.step];
        if (!parentId) {
          return { success: false, error: `Step "${stepsWithIds[i].name}" depends on unknown step "${dep.step}"` };
        }
        edges.push({ parent_step_id: parentId, child_step_id: childId, condition: dep.when || null });
      }
    }
  }

  // Validate the DAG before inserting
  const graph = buildDAG(stepsWithIds, edges);
  const validation = validateDAG(graph);
  if (!validation.valid) {
    return { success: false, error: `Invalid DAG: ${validation.errors.join("; ")}` };
  }

  // Insert pipeline
  const ok = sqliteExec(dbPath,
    `INSERT INTO pipelines (id, name, status, created_at, updated_at, created_by, trigger_message, config)
     VALUES (?, ?, 'pending', ?, ?, 'user', ?, ?)`,
    [pipelineId, sanitizeValue(name), now, now, sanitizeValue(trigger_message || ""), config ? JSON.stringify(config) : null]
  );
  if (!ok) return { success: false, error: "Failed to insert pipeline" };

  // Insert steps
  for (const s of stepsWithIds) {
    sqliteExec(dbPath,
      `INSERT INTO pipeline_steps (id, pipeline_id, name, step_type, status, tier, executor, prompt_template, depends_on_all, created_at, max_retries)
       VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)`,
      [s.id, pipelineId, s.name, s.step_type, s.tier, s.executor, s.prompt_template, s.depends_on_all, now, s.max_retries]
    );
  }

  // Insert edges
  for (const e of edges) {
    sqliteExec(dbPath,
      `INSERT INTO pipeline_edges (parent_step_id, child_step_id, pipeline_id, condition)
       VALUES (?, ?, ?, ?)`,
      [e.parent_step_id, e.child_step_id, pipelineId, e.condition ? JSON.stringify(e.condition) : null]
    );
  }

  return { success: true, pipelineId };
}

/**
 * Start a pipeline (mark as running, root steps become ready).
 * @param {string} dbPath
 * @param {string} pipelineId
 * @returns {{ success: boolean, error?: string }}
 */
export function startPipeline(dbPath, pipelineId) {
  const pipeline = sqliteQuery(dbPath, `SELECT status FROM pipelines WHERE id = ?`, [pipelineId]);
  if (!pipeline.length) return { success: false, error: "Pipeline not found" };
  if (pipeline[0].status !== "pending") return { success: false, error: `Pipeline is ${pipeline[0].status}, not pending` };

  sqliteExec(dbPath,
    `UPDATE pipelines SET status = 'running', started_at = ?, updated_at = ? WHERE id = ?`,
    [Date.now(), Date.now(), pipelineId]
  );

  return { success: true };
}

/**
 * Get pipeline status with DAG visualization.
 * @param {string} dbPath
 * @param {string} [pipelineId] - specific pipeline, or null for all active
 * @returns {object}
 */
export function getPipelineStatus(dbPath, pipelineId) {
  if (pipelineId) {
    const pipeline = sqliteQuery(dbPath, `SELECT * FROM pipelines WHERE id = ?`, [pipelineId]);
    if (!pipeline.length) return { found: false };

    const graph = loadDAG(dbPath, pipelineId);
    const completion = checkPipelineComplete(graph);
    const ready = readySteps(graph);

    return {
      found: true,
      pipeline: pipeline[0],
      steps: Object.values(graph.steps),
      ready,
      completion,
    };
  }

  // All active pipelines
  const pipelines = sqliteQuery(dbPath,
    `SELECT * FROM pipelines WHERE status IN ('pending', 'running', 'paused') ORDER BY created_at DESC`
  );

  const results = pipelines.map(p => {
    const graph = loadDAG(dbPath, p.id);
    const completion = checkPipelineComplete(graph);
    return { pipeline: p, stepCount: Object.keys(graph.steps).length, completion };
  });

  return { found: true, pipelines: results };
}

/**
 * Advance a pipeline after a step completes.
 * Stores output, evaluates conditions, activates children, checks completion.
 * @param {string} dbPath
 * @param {string} stepId
 * @param {{ output: string, success: boolean, error?: string }} result
 * @returns {{ advanced: boolean, pipelineComplete: boolean, pipelineStatus?: string, readySteps?: string[], skipped?: string[], error?: string }}
 */
export function advanceStep(dbPath, stepId, result) {
  const step = sqliteQuery(dbPath, `SELECT * FROM pipeline_steps WHERE id = ?`, [stepId]);
  if (!step.length) return { advanced: false, pipelineComplete: false, error: "Step not found" };

  const pipelineId = step[0].pipeline_id;
  const now = Date.now();

  // Truncate output if too large
  let output = result.output || "";
  if (output.length > ARTIFACT_MAX_CHARS) {
    output = output.substring(0, ARTIFACT_MAX_CHARS - 50) + "\n\n...(truncated to 64KB)";
  }

  if (result.success) {
    sqliteExec(dbPath,
      `UPDATE pipeline_steps SET status = 'complete', output_artifact = ?, result_summary = ?, completed_at = ? WHERE id = ?`,
      [output, (result.summary || output).substring(0, 500), now, stepId]
    );
  } else {
    // Check retry count
    const retryCount = (step[0].retry_count || 0) + 1;
    if (retryCount <= (step[0].max_retries ?? 1)) {
      // Retry: reset to pending
      sqliteExec(dbPath,
        `UPDATE pipeline_steps SET status = 'pending', retry_count = ?, error = ? WHERE id = ?`,
        [retryCount, sanitizeValue(result.error || "Unknown error"), stepId]
      );
      return { advanced: false, pipelineComplete: false, retrying: true, retryCount };
    }

    // Max retries exceeded: mark as failed
    sqliteExec(dbPath,
      `UPDATE pipeline_steps SET status = 'failed', error = ?, completed_at = ? WHERE id = ?`,
      [sanitizeValue(result.error || "Unknown error"), now, stepId]
    );
  }

  // Reload the graph to find newly ready/skippable steps
  const graph = loadDAG(dbPath, pipelineId);
  const newReady = readySteps(graph);
  const toSkip = skippableSteps(graph);

  // Mark skippable steps
  for (const skipId of toSkip) {
    sqliteExec(dbPath,
      `UPDATE pipeline_steps SET status = 'skipped', completed_at = ? WHERE id = ?`,
      [now, skipId]
    );
  }

  // Check pipeline completion
  // Reload graph after skip updates
  const updatedGraph = loadDAG(dbPath, pipelineId);
  const completion = checkPipelineComplete(updatedGraph);

  if (completion.complete) {
    sqliteExec(dbPath,
      `UPDATE pipelines SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?`,
      [completion.status, now, now, pipelineId]
    );
  } else {
    sqliteExec(dbPath, `UPDATE pipelines SET updated_at = ? WHERE id = ?`, [now, pipelineId]);
  }

  return {
    advanced: true,
    pipelineComplete: completion.complete,
    pipelineStatus: completion.status,
    readySteps: newReady.map(id => updatedGraph.steps[id]?.name || id),
    skipped: toSkip.map(id => graph.steps[id]?.name || id),
  };
}

/**
 * Cancel a pipeline and all its pending/running steps.
 * @param {string} dbPath
 * @param {string} pipelineId
 * @returns {{ success: boolean, error?: string }}
 */
export function cancelPipeline(dbPath, pipelineId) {
  const pipeline = sqliteQuery(dbPath, `SELECT status FROM pipelines WHERE id = ?`, [pipelineId]);
  if (!pipeline.length) return { success: false, error: "Pipeline not found" };
  if (pipeline[0].status === "complete" || pipeline[0].status === "cancelled") {
    return { success: false, error: `Pipeline is already ${pipeline[0].status}` };
  }

  const now = Date.now();
  sqliteExec(dbPath,
    `UPDATE pipeline_steps SET status = 'cancelled', completed_at = ? WHERE pipeline_id = ? AND status IN ('pending', 'running', 'ready')`,
    [now, pipelineId]
  );
  sqliteExec(dbPath,
    `UPDATE pipelines SET status = 'cancelled', completed_at = ?, updated_at = ? WHERE id = ?`,
    [now, now, pipelineId]
  );

  // Disable associated triggers
  sqliteExec(dbPath,
    `UPDATE pipeline_triggers SET enabled = 0 WHERE pipeline_id = ?`,
    [pipelineId]
  );

  return { success: true };
}

/**
 * Create or update a recurring trigger for a pipeline.
 * @param {string} dbPath
 * @param {object} input - { pipeline_id, schedule, timezone, enabled }
 * @returns {{ success: boolean, triggerId?: string, error?: string }}
 */
export function schedulePipeline(dbPath, input) {
  const { pipeline_id, schedule, timezone, enabled } = input;

  const pipeline = sqliteQuery(dbPath, `SELECT id FROM pipelines WHERE id = ?`, [pipeline_id]);
  if (!pipeline.length) return { success: false, error: "Pipeline not found" };

  // Validate cron expression (basic check)
  if (!schedule || typeof schedule !== "string") {
    return { success: false, error: "Schedule must be a cron expression string" };
  }
  const parts = schedule.trim().split(/\s+/);
  if (parts.length !== 5) {
    return { success: false, error: "Cron expression must have 5 fields: minute hour day month weekday" };
  }

  const triggerId = randomUUID();
  const now = Date.now();

  sqliteExec(dbPath,
    `INSERT INTO pipeline_triggers (id, pipeline_id, schedule, timezone, enabled, last_fired, next_fire)
     VALUES (?, ?, ?, ?, ?, NULL, ?)`,
    [triggerId, pipeline_id, schedule, timezone || "America/New_York", enabled !== false ? 1 : 0, now]
  );

  return { success: true, triggerId };
}

/**
 * Pipeline tick — find all ready work across running pipelines.
 * Called by Lily when woken by cron to check for pipeline work.
 * @param {string} dbPath
 * @returns {{ work: object[], paused: object[], pipelineCount: number }}
 */
export function pipelineTick(dbPath) {
  const pipelines = sqliteQuery(dbPath,
    `SELECT * FROM pipelines WHERE status = 'running' ORDER BY created_at`
  );

  const work = [];
  for (const p of pipelines) {
    const graph = loadDAG(dbPath, p.id);
    const ready = readySteps(graph);
    if (ready.length === 0) continue;

    for (const stepId of ready) {
      const step = graph.steps[stepId];

      // Build context from parent outputs
      const parentIds = graph.parents[stepId] || [];
      const parentContext = parentIds
        .map(pid => graph.steps[pid])
        .filter(ps => ps && ps.status === "complete" && ps.output_artifact)
        .map(ps => `[${ps.name}]: ${(ps.result_summary || ps.output_artifact || "").substring(0, 500)}`)
        .join("\n");

      work.push({
        pipelineId: p.id,
        pipelineName: p.name,
        stepId: step.id,
        stepName: step.name,
        stepType: step.step_type,
        prompt: step.prompt_template,
        tier: step.tier,
        executor: step.executor,
        parentContext,
      });
    }
  }

  // Check for paused pipelines awaiting input
  const paused = sqliteQuery(dbPath,
    `SELECT p.id as pipeline_id, p.name as pipeline_name,
            s.id as step_id, s.name as step_name, s.output_artifact
     FROM pipelines p
     JOIN pipeline_steps s ON s.pipeline_id = p.id
     WHERE p.status = 'paused' AND s.status = 'paused'`
  );

  return { work, paused, pipelineCount: pipelines.length };
}

/**
 * Get "lily today" summary.
 * @param {string} dbPath
 * @returns {string} Formatted summary text
 */
export function lilyToday(dbPath) {
  const now = Date.now();
  const dayAgo = now - 86400000;

  // Active pipelines
  const active = sqliteQuery(dbPath,
    `SELECT p.id, p.name, p.status, p.started_at FROM pipelines p
     WHERE p.status IN ('running', 'paused') ORDER BY p.started_at DESC`
  );

  // Completed today
  const completed = sqliteQuery(dbPath,
    `SELECT p.id, p.name, p.status, p.completed_at FROM pipelines p
     WHERE p.status IN ('complete', 'failed') AND p.completed_at > ?
     ORDER BY p.completed_at DESC`,
    [dayAgo]
  );

  // Scheduled triggers
  const triggers = sqliteQuery(dbPath,
    `SELECT t.schedule, t.timezone, p.name FROM pipeline_triggers t
     JOIN pipelines p ON t.pipeline_id = p.id
     WHERE t.enabled = 1
     ORDER BY t.schedule`
  );

  const lines = [];
  lines.push("Pipeline Status");
  lines.push("===============\n");

  if (active.length > 0) {
    lines.push("Active Pipelines:");
    for (const p of active) {
      const steps = sqliteQuery(dbPath,
        `SELECT status FROM pipeline_steps WHERE pipeline_id = ?`, [p.id]
      );
      const total = steps.length;
      const done = steps.filter(s => s.status === "complete" || s.status === "skipped").length;
      const running = steps.filter(s => s.status === "running").length;
      const runningNames = sqliteQuery(dbPath,
        `SELECT name FROM pipeline_steps WHERE pipeline_id = ? AND status = 'running'`, [p.id]
      );
      const runLabel = runningNames.map(r => r.name).join(", ");
      lines.push(`  ${p.name}  [${done}/${total} steps]${runLabel ? `  running: ${runLabel}` : ""}`);
    }
    lines.push("");
  } else {
    lines.push("No active pipelines.\n");
  }

  if (triggers.length > 0) {
    lines.push("Scheduled:");
    for (const t of triggers) {
      lines.push(`  ${t.schedule}  ${t.name}  (${t.timezone})`);
    }
    lines.push("");
  }

  if (completed.length > 0) {
    lines.push("Completed Today:");
    for (const p of completed) {
      const time = new Date(p.completed_at).toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
      const icon = p.status === "complete" ? "done" : "FAILED";
      lines.push(`  ${time}  ${p.name}  ${icon}`);
    }
    lines.push("");
  }

  // Memory stats
  const memCount = sqliteQuery(dbPath,
    `SELECT COUNT(*) as cnt FROM decisions WHERE expires_at IS NULL OR expires_at > ?`, [now]
  );

  lines.push(`Memory: ${memCount[0]?.cnt || 0} active facts`);

  return lines.join("\n");
}

// ============================================================================
// Tool registration
// ============================================================================

/**
 * Register all pipeline tools with the OpenClaw API.
 * @param {object} api - OpenClaw plugin API
 * @param {string} dbPath - Database path
 * @param {object} log - Logger
 */
export function registerPipelineTools(api, dbPath, log) {

  // --- Tool: pipeline_create ---
  api.registerTool({
    name: "pipeline_create",
    label: "Create Pipeline",
    description: `Create a multi-step workflow pipeline. Define steps with dependencies to form a DAG. Steps execute in order based on their dependencies, with cost-aware model routing. Supports conditional branching.`,
    parameters: { type: "object", properties: {
      name: { type: "string", description: "Pipeline name" },
      trigger_message: { type: "string", description: "Original user instruction that created this pipeline" },
      steps: {
        type: "array",
        description: "Pipeline steps. Each step has: name, prompt (template with {{prev_result}}), tier (gemini-flash|local|claude), executor (lily|claude|local), step_type (task|decision|notify), depends_on (array of step names or {step, when} objects)",
        items: { type: "object" },
      },
      config: { type: "object", description: "Pipeline config: { notify: boolean, max_retries: number }" },
      auto_start: { type: "boolean", description: "Start pipeline immediately after creation (default: true)" },
    }, required: ["name", "steps"] },
    async execute(_id, params) {
      const result = createPipeline(dbPath, params);
      if (!result.success) {
        return { content: [{ type: "text", text: `Failed to create pipeline: ${result.error}` }] };
      }

      // Auto-start unless explicitly disabled
      if (params.auto_start !== false) {
        startPipeline(dbPath, result.pipelineId);
      }

      const graph = loadDAG(dbPath, result.pipelineId);
      const ready = readySteps(graph);
      const readyNames = ready.map(id => graph.steps[id]?.name || id);

      let text = `Pipeline "${params.name}" created (${Object.keys(graph.steps).length} steps).`;
      text += `\nID: ${result.pipelineId}`;
      if (params.auto_start !== false) {
        text += `\nStatus: running`;
        text += `\nReady steps: ${readyNames.join(", ") || "none"}`;
      } else {
        text += `\nStatus: pending (call pipeline_advance or start manually)`;
      }

      log.info?.(`pipeline: created "${params.name}" (${result.pipelineId})`);
      return { content: [{ type: "text", text }], details: { pipelineId: result.pipelineId, readySteps: readyNames } };
    },
  }, { name: "pipeline_create" });

  // --- Tool: pipeline_status ---
  api.registerTool({
    name: "pipeline_status",
    label: "Pipeline Status",
    description: "View pipeline status. Shows active, scheduled, and recently completed pipelines. Provide a pipeline_id for detailed DAG view.",
    parameters: { type: "object", properties: {
      pipeline_id: { type: "string", description: "Specific pipeline ID (optional, shows all active if omitted)" },
      format: { type: "string", description: "Output format: 'summary' (default) or 'today' for lily_today view" },
    } },
    async execute(_id, params = {}) {
      if (params.format === "today") {
        const summary = lilyToday(dbPath);
        return { content: [{ type: "text", text: summary }] };
      }

      const status = getPipelineStatus(dbPath, params.pipeline_id);

      if (params.pipeline_id) {
        if (!status.found) {
          return { content: [{ type: "text", text: `Pipeline not found: ${params.pipeline_id}` }] };
        }

        const p = status.pipeline;
        const lines = [`Pipeline: ${p.name}`, `Status: ${p.status}`, `ID: ${p.id}`, ""];

        // DAG visualization
        lines.push("Steps:");
        for (const step of status.steps) {
          const icon = { pending: "[ ]", running: "[>]", complete: "[x]", failed: "[!]", skipped: "[-]", cancelled: "[/]" }[step.status] || "[?]";
          const isReady = status.ready.includes(step.id);
          lines.push(`  ${icon} ${step.name} (${step.tier}, ${step.executor})${isReady ? " <- READY" : ""}${step.error ? ` ERROR: ${step.error}` : ""}`);
          if (step.result_summary) {
            lines.push(`      Result: ${step.result_summary.substring(0, 100)}`);
          }
        }

        const text = lines.join("\n");
        return { content: [{ type: "text", text }], details: status };
      }

      // All active
      if (!status.pipelines || status.pipelines.length === 0) {
        return { content: [{ type: "text", text: "No active pipelines." }] };
      }

      const lines = ["Active Pipelines:", ""];
      for (const { pipeline: p, stepCount, completion } of status.pipelines) {
        lines.push(`  ${p.name} (${p.status}) - ${completion.summary.complete}/${stepCount} steps complete`);
      }

      return { content: [{ type: "text", text: lines.join("\n") }], details: status };
    },
  }, { name: "pipeline_status" });

  // --- Tool: pipeline_advance ---
  api.registerTool({
    name: "pipeline_advance",
    label: "Advance Pipeline Step",
    description: "Report a step's completion and advance the pipeline. The engine evaluates edge conditions, activates ready children, skips unreachable steps, and checks for pipeline completion.",
    parameters: { type: "object", properties: {
      step_id: { type: "string", description: "The step ID that completed" },
      output: { type: "string", description: "Step output/result" },
      success: { type: "boolean", description: "Whether the step succeeded (default: true)" },
      error: { type: "string", description: "Error message if the step failed" },
    }, required: ["step_id"] },
    async execute(_id, params) {
      const result = advanceStep(dbPath, params.step_id, {
        output: params.output || "",
        success: params.success !== false,
        error: params.error,
        summary: params.output ? params.output.substring(0, 500) : "",
      });

      if (!result.advanced && result.retrying) {
        return { content: [{ type: "text", text: `Step failed, retrying (attempt ${result.retryCount}).` }], details: result };
      }

      if (!result.advanced) {
        return { content: [{ type: "text", text: `Failed to advance: ${result.error}` }] };
      }

      let text = "";
      if (result.pipelineComplete) {
        text = `Pipeline ${result.pipelineStatus}. All steps finished.`;
      } else {
        text = `Step advanced. Ready: ${result.readySteps?.join(", ") || "none"}.`;
      }
      if (result.skipped?.length > 0) {
        text += ` Skipped: ${result.skipped.join(", ")}.`;
      }

      log.info?.(`pipeline: step advanced, ready=[${result.readySteps?.join(",")}]`);
      return { content: [{ type: "text", text }], details: result };
    },
  }, { name: "pipeline_advance" });

  // --- Tool: pipeline_cancel ---
  api.registerTool({
    name: "pipeline_cancel",
    label: "Cancel Pipeline",
    description: "Cancel a running pipeline. Marks all pending steps as cancelled and disables any associated triggers.",
    parameters: { type: "object", properties: {
      pipeline_id: { type: "string", description: "Pipeline ID to cancel" },
    }, required: ["pipeline_id"] },
    async execute(_id, { pipeline_id }) {
      const result = cancelPipeline(dbPath, pipeline_id);
      if (!result.success) {
        return { content: [{ type: "text", text: `Cancel failed: ${result.error}` }] };
      }
      log.info?.(`pipeline: cancelled ${pipeline_id}`);
      return { content: [{ type: "text", text: "Pipeline cancelled." }] };
    },
  }, { name: "pipeline_cancel" });

  // --- Tool: pipeline_schedule ---
  api.registerTool({
    name: "pipeline_schedule",
    label: "Schedule Pipeline",
    description: "Create a recurring trigger for a pipeline using a cron expression. Examples: '0 5 * * *' (daily 5AM), '0 9 * * 1-5' (weekdays 9AM), '0 */2 * * *' (every 2h).",
    parameters: { type: "object", properties: {
      pipeline_id: { type: "string", description: "Pipeline to schedule" },
      schedule: { type: "string", description: "Cron expression (5 fields: min hour day month weekday)" },
      timezone: { type: "string", description: "IANA timezone (default: America/New_York)" },
      enabled: { type: "boolean", description: "Enable trigger (default: true)" },
    }, required: ["pipeline_id", "schedule"] },
    async execute(_id, params) {
      const result = schedulePipeline(dbPath, params);
      if (!result.success) {
        return { content: [{ type: "text", text: `Schedule failed: ${result.error}` }] };
      }
      log.info?.(`pipeline: scheduled ${params.pipeline_id} at "${params.schedule}"`);
      return { content: [{ type: "text", text: `Trigger created: ${params.schedule} (${params.timezone || "America/New_York"})` }], details: result };
    },
  }, { name: "pipeline_schedule" });

  // --- Tool: pipeline_request ---
  api.registerTool({
    name: "pipeline_request",
    label: "Pipeline Request",
    description: "Pause a pipeline step and request input/resources. Used when a step needs something before continuing (e.g., Claude needs data from Lily, or a step needs human approval).",
    parameters: { type: "object", properties: {
      step_id: { type: "string", description: "Step ID requesting input" },
      request_type: { type: "string", description: "Type: 'input' (need data), 'approval' (need human OK), 'resource' (need external resource)" },
      message: { type: "string", description: "What is needed" },
    }, required: ["step_id", "message"] },
    async execute(_id, params) {
      const step = sqliteQuery(dbPath, `SELECT * FROM pipeline_steps WHERE id = ?`, [params.step_id]);
      if (!step.length) {
        return { content: [{ type: "text", text: "Step not found." }] };
      }

      sqliteExec(dbPath,
        `UPDATE pipeline_steps SET status = 'paused', output_artifact = ? WHERE id = ?`,
        [JSON.stringify({ request_type: params.request_type || "input", message: params.message }), params.step_id]
      );

      sqliteExec(dbPath,
        `UPDATE pipelines SET status = 'paused', updated_at = ? WHERE id = ?`,
        [Date.now(), step[0].pipeline_id]
      );

      log.info?.(`pipeline: step ${params.step_id} paused, requesting: ${params.message}`);
      return { content: [{ type: "text", text: `Step paused. Request: ${params.message}` }] };
    },
  }, { name: "pipeline_request" });

  // --- Tool: pipeline_tick ---
  api.registerTool({
    name: "pipeline_tick",
    label: "Pipeline Tick",
    description: `Check all running pipelines for ready work. Returns steps you need to execute, with their prompts and parent context. Workflow: call pipeline_tick → execute each step using your tools → call pipeline_advance for each → repeat until no work remains. Also shows paused steps awaiting input.`,
    parameters: { type: "object", properties: {} },
    async execute() {
      const tick = pipelineTick(dbPath);

      if (tick.work.length === 0 && tick.paused.length === 0) {
        return { content: [{ type: "text", text: `No pipeline work pending. (${tick.pipelineCount} running pipelines checked)` }] };
      }

      const lines = [];

      if (tick.work.length > 0) {
        lines.push(`Ready steps (${tick.work.length}):\n`);
        for (const w of tick.work) {
          lines.push(`Pipeline: ${w.pipelineName}`);
          lines.push(`  Step: ${w.stepName} (${w.stepType})`);
          lines.push(`  Step ID: ${w.stepId}`);
          lines.push(`  Tier: ${w.tier} | Executor: ${w.executor}`);
          lines.push(`  Prompt: ${w.prompt}`);
          if (w.parentContext) {
            lines.push(`  Context from previous steps:\n    ${w.parentContext.replace(/\n/g, "\n    ")}`);
          }
          lines.push("");
        }
      }

      if (tick.paused.length > 0) {
        lines.push(`Paused steps awaiting input (${tick.paused.length}):\n`);
        for (const p of tick.paused) {
          let request = p.output_artifact || "";
          try { request = JSON.parse(request)?.message || request; } catch {}
          lines.push(`  ${p.pipeline_name} → ${p.step_name}: ${request}`);
        }
      }

      const text = lines.join("\n");
      log.info?.(`pipeline: tick found ${tick.work.length} ready, ${tick.paused.length} paused`);
      return { content: [{ type: "text", text }], details: tick };
    },
  }, { name: "pipeline_tick" });

  log.info?.("pipeline: 7 tools registered");
}
