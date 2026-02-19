// ============================================================================
// DAG Graph Engine — in-memory traversal for pipeline execution
// SQLite stores structure; this module does all graph operations in JS.
// ============================================================================

import { sqliteQuery } from "./sqlite.js";

/**
 * Load a pipeline's DAG from SQLite into an in-memory adjacency map.
 * @param {string} dbPath
 * @param {string} pipelineId
 * @returns {{ pipelineId: string, children: Object, parents: Object, conditions: Object, steps: Object, nameToId: Object, rootSteps: string[] }}
 */
export function loadDAG(dbPath, pipelineId) {
  const edgeRows = sqliteQuery(dbPath,
    `SELECT parent_step_id, child_step_id, condition FROM pipeline_edges WHERE pipeline_id = ?`,
    [pipelineId]
  );

  const stepRows = sqliteQuery(dbPath,
    `SELECT id, name, status, step_type, depends_on_all, output_artifact, tier, executor, prompt_template, retry_count, max_retries
     FROM pipeline_steps WHERE pipeline_id = ?`,
    [pipelineId]
  );

  const graph = {
    pipelineId,
    children: {},
    parents: {},
    conditions: {},
    steps: {},
    nameToId: {},
    rootSteps: [],
  };

  for (const step of stepRows) {
    graph.steps[step.id] = step;
    graph.nameToId[step.name] = step.id;
    graph.children[step.id] = [];
    graph.parents[step.id] = [];
  }

  for (const e of edgeRows) {
    graph.children[e.parent_step_id].push(e.child_step_id);
    graph.parents[e.child_step_id].push(e.parent_step_id);
    if (e.condition) {
      const cond = typeof e.condition === "string" ? JSON.parse(e.condition) : e.condition;
      graph.conditions[`${e.parent_step_id}->${e.child_step_id}`] = cond;
    }
  }

  // Identify root steps (no parents)
  for (const stepId of Object.keys(graph.steps)) {
    if (graph.parents[stepId].length === 0) {
      graph.rootSteps.push(stepId);
    }
  }

  return graph;
}

/**
 * Build a DAG from in-memory step and edge definitions (for validation before insert).
 * @param {Array<{ id: string, name: string, depends_on_all?: number }>} steps
 * @param {Array<{ parent_step_id: string, child_step_id: string, condition?: object }>} edges
 * @returns {object} Graph structure (same shape as loadDAG output)
 */
export function buildDAG(steps, edges) {
  const graph = {
    pipelineId: null,
    children: {},
    parents: {},
    conditions: {},
    steps: {},
    nameToId: {},
    rootSteps: [],
  };

  for (const step of steps) {
    graph.steps[step.id] = step;
    graph.nameToId[step.name] = step.id;
    graph.children[step.id] = [];
    graph.parents[step.id] = [];
  }

  for (const e of edges) {
    graph.children[e.parent_step_id].push(e.child_step_id);
    graph.parents[e.child_step_id].push(e.parent_step_id);
    if (e.condition) {
      graph.conditions[`${e.parent_step_id}->${e.child_step_id}`] = e.condition;
    }
  }

  for (const stepId of Object.keys(graph.steps)) {
    if (graph.parents[stepId].length === 0) {
      graph.rootSteps.push(stepId);
    }
  }

  return graph;
}

/**
 * Find steps that are ready to execute.
 * A step is ready when it's 'pending' and all dependency conditions are satisfied.
 * @param {object} graph - DAG from loadDAG/buildDAG
 * @returns {string[]} Array of ready step IDs
 */
export function readySteps(graph) {
  const ready = [];

  for (const [stepId, step] of Object.entries(graph.steps)) {
    if (step.status !== "pending") continue;

    const parentIds = graph.parents[stepId];

    // Root steps are always ready
    if (parentIds.length === 0) {
      ready.push(stepId);
      continue;
    }

    const dependsOnAll = step.depends_on_all !== 0;

    if (dependsOnAll) {
      // ALL mode: every parent must be complete with its edge condition passing
      const allSatisfied = parentIds.every(pid => {
        const parent = graph.steps[pid];
        if (!parent || parent.status !== "complete") return false;
        const condKey = `${pid}->${stepId}`;
        const cond = graph.conditions[condKey];
        return evaluateCondition(cond, parent.output_artifact || "");
      });
      if (allSatisfied) ready.push(stepId);
    } else {
      // ANY mode: at least one parent complete with condition passing
      const anySatisfied = parentIds.some(pid => {
        const parent = graph.steps[pid];
        if (!parent || parent.status !== "complete") return false;
        const condKey = `${pid}->${stepId}`;
        const cond = graph.conditions[condKey];
        return evaluateCondition(cond, parent.output_artifact || "");
      });
      if (anySatisfied) ready.push(stepId);
    }
  }

  return ready;
}

/**
 * Find steps that should be skipped because their dependencies can never be satisfied.
 * @param {object} graph
 * @returns {string[]} Array of step IDs to skip
 */
export function skippableSteps(graph) {
  const skippable = [];

  for (const [stepId, step] of Object.entries(graph.steps)) {
    if (step.status !== "pending") continue;

    const parentIds = graph.parents[stepId];
    if (parentIds.length === 0) continue;

    const dependsOnAll = step.depends_on_all !== 0;

    // Check if all parents are in terminal states
    const allTerminal = parentIds.every(pid => {
      const p = graph.steps[pid];
      return p && (p.status === "complete" || p.status === "failed" || p.status === "skipped" || p.status === "cancelled");
    });

    if (!allTerminal) continue;

    if (dependsOnAll) {
      // If any parent failed/skipped/cancelled, this step can never run
      const anyFailed = parentIds.some(pid => {
        const p = graph.steps[pid];
        return p && (p.status === "failed" || p.status === "skipped" || p.status === "cancelled");
      });
      if (anyFailed) {
        skippable.push(stepId);
        continue;
      }
      // All parents complete but check conditions
      const anyConditionFailed = parentIds.some(pid => {
        const condKey = `${pid}->${stepId}`;
        const cond = graph.conditions[condKey];
        const parent = graph.steps[pid];
        return cond && !evaluateCondition(cond, parent?.output_artifact || "");
      });
      if (anyConditionFailed) skippable.push(stepId);
    } else {
      // ANY mode: skip only if NO parent completed with condition passing
      const anyPassing = parentIds.some(pid => {
        const parent = graph.steps[pid];
        if (!parent || parent.status !== "complete") return false;
        const condKey = `${pid}->${stepId}`;
        const cond = graph.conditions[condKey];
        return evaluateCondition(cond, parent.output_artifact || "");
      });
      if (!anyPassing) skippable.push(stepId);
    }
  }

  return skippable;
}

/**
 * Evaluate an edge condition against step output.
 * @param {object|null} condition - null = unconditional (always true)
 * @param {string} output - the parent step's output artifact
 * @returns {boolean}
 */
export function evaluateCondition(condition, output) {
  if (!condition) return true;

  if (condition.output_contains) {
    return (output || "").toLowerCase().includes(condition.output_contains.toLowerCase());
  }
  if (condition.output_match) {
    try {
      return new RegExp(condition.output_match, "i").test(output || "");
    } catch {
      return false;
    }
  }

  // Unknown condition type: treat as unconditional
  return true;
}

/**
 * Detect cycles using DFS with coloring.
 * @param {object} graph
 * @returns {{ hasCycle: boolean, cycle: string[] | null }}
 */
export function detectCycles(graph) {
  const WHITE = 0, GRAY = 1, BLACK = 2;
  const color = {};
  const parent = {};

  for (const id of Object.keys(graph.steps)) {
    color[id] = WHITE;
    parent[id] = null;
  }

  for (const startId of Object.keys(graph.steps)) {
    if (color[startId] !== WHITE) continue;

    const stack = [{ id: startId, childIdx: 0 }];
    color[startId] = GRAY;

    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      const children = graph.children[frame.id] || [];

      if (frame.childIdx >= children.length) {
        color[frame.id] = BLACK;
        stack.pop();
        continue;
      }

      const childId = children[frame.childIdx];
      frame.childIdx++;

      if (color[childId] === GRAY) {
        // Found a cycle: reconstruct it
        const cycle = [childId];
        for (let i = stack.length - 1; i >= 0; i--) {
          cycle.push(stack[i].id);
          if (stack[i].id === childId) break;
        }
        return { hasCycle: true, cycle: cycle.reverse() };
      }

      if (color[childId] === WHITE) {
        color[childId] = GRAY;
        parent[childId] = frame.id;
        stack.push({ id: childId, childIdx: 0 });
      }
    }
  }

  return { hasCycle: false, cycle: null };
}

/**
 * Topological sort using Kahn's algorithm.
 * Returns null if the graph has a cycle.
 * @param {object} graph
 * @returns {string[] | null}
 */
export function topoSort(graph) {
  const inDegree = {};
  for (const id of Object.keys(graph.steps)) {
    inDegree[id] = (graph.parents[id] || []).length;
  }

  const queue = [];
  for (const [id, deg] of Object.entries(inDegree)) {
    if (deg === 0) queue.push(id);
  }

  const sorted = [];
  while (queue.length > 0) {
    const node = queue.shift();
    sorted.push(node);
    for (const child of (graph.children[node] || [])) {
      inDegree[child]--;
      if (inDegree[child] === 0) queue.push(child);
    }
  }

  return sorted.length === Object.keys(graph.steps).length ? sorted : null;
}

/**
 * Validate a DAG before insertion.
 * Checks: cycles, orphans, reachability, default edges on decision nodes, step count.
 * @param {object} graph
 * @param {{ maxSteps?: number }} opts
 * @returns {{ valid: boolean, errors: string[] }}
 */
export function validateDAG(graph, opts = {}) {
  const errors = [];
  const maxSteps = opts.maxSteps || 50;
  const stepCount = Object.keys(graph.steps).length;

  // 1. Step count
  if (stepCount === 0) {
    errors.push("Pipeline has no steps");
    return { valid: false, errors };
  }
  if (stepCount > maxSteps) {
    errors.push(`Pipeline has ${stepCount} steps (max: ${maxSteps})`);
  }

  // 2. Must have at least one root step
  if (graph.rootSteps.length === 0) {
    errors.push("Pipeline has no root steps (every step has a dependency, which means there is a cycle)");
  }

  // 3. Cycle detection
  const { hasCycle, cycle } = detectCycles(graph);
  if (hasCycle) {
    const names = cycle.map(id => graph.steps[id]?.name || id);
    errors.push(`Cycle detected: ${names.join(" -> ")}`);
  }

  // 4. Reachability: every step must be reachable from a root
  if (!hasCycle && graph.rootSteps.length > 0) {
    const reachable = new Set();
    const queue = [...graph.rootSteps];
    while (queue.length > 0) {
      const id = queue.shift();
      if (reachable.has(id)) continue;
      reachable.add(id);
      for (const child of (graph.children[id] || [])) {
        queue.push(child);
      }
    }
    for (const id of Object.keys(graph.steps)) {
      if (!reachable.has(id)) {
        errors.push(`Step "${graph.steps[id]?.name || id}" is not reachable from any root step`);
      }
    }
  }

  // 5. Terminal reachability: at least one leaf node (step with no children)
  const leafSteps = Object.keys(graph.steps).filter(id => (graph.children[id] || []).length === 0);
  if (leafSteps.length === 0 && stepCount > 0) {
    errors.push("Pipeline has no leaf steps (every step has children, which suggests a cycle)");
  }

  // 6. Decision nodes should have a default (unconditional) edge
  for (const [stepId, step] of Object.entries(graph.steps)) {
    if (step.step_type !== "decision") continue;
    const childIds = graph.children[stepId] || [];
    if (childIds.length === 0) continue;

    const hasDefault = childIds.some(childId => {
      const condKey = `${stepId}->${childId}`;
      return !graph.conditions[condKey];
    });

    if (!hasDefault) {
      errors.push(`Decision step "${step.name}" has no default (unconditional) edge. Add a fallback path.`);
    }
  }

  // 7. Verify all edge references are valid step IDs
  for (const stepId of Object.keys(graph.children)) {
    for (const childId of graph.children[stepId]) {
      if (!graph.steps[childId]) {
        errors.push(`Edge references nonexistent step ID: ${childId}`);
      }
    }
  }

  return { valid: errors.length === 0, errors };
}

/**
 * Check if a pipeline is complete (all steps in terminal states).
 * @param {object} graph
 * @returns {{ complete: boolean, status: 'complete'|'failed'|'running', summary: object }}
 */
export function checkPipelineComplete(graph) {
  const counts = { pending: 0, ready: 0, running: 0, complete: 0, failed: 0, skipped: 0, cancelled: 0 };
  for (const step of Object.values(graph.steps)) {
    counts[step.status] = (counts[step.status] || 0) + 1;
  }

  const total = Object.keys(graph.steps).length;
  const terminal = counts.complete + counts.failed + counts.skipped + counts.cancelled;

  if (terminal < total) {
    return { complete: false, status: "running", summary: counts };
  }

  // All steps are terminal
  if (counts.failed > 0) {
    return { complete: true, status: "failed", summary: counts };
  }
  return { complete: true, status: "complete", summary: counts };
}
