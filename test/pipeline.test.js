import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { ensureTables, closeAllConnections, sqliteQuery, sqliteExec, runMigrations } from "../lib/sqlite.js";
import { createPipeline, startPipeline, getPipelineStatus, advanceStep, cancelPipeline, schedulePipeline, lilyToday, pipelineTick } from "../lib/pipeline.js";
import { loadDAG, readySteps } from "../lib/graph.js";

describe("Pipeline Engine", () => {
  let tempDir;
  let dbPath;

  before(() => {
    tempDir = mkdtempSync(path.join(tmpdir(), "lily-pipeline-test-"));
    dbPath = path.join(tempDir, "test.db");
    ensureTables(dbPath);
    runMigrations(dbPath);
  });

  after(() => {
    closeAllConnections();
    rmSync(tempDir, { recursive: true, force: true });
  });

  describe("createPipeline", () => {
    it("creates a simple linear pipeline", () => {
      const result = createPipeline(dbPath, {
        name: "test-linear",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "Do A" },
          { name: "step-b", prompt: "Do B", depends_on: ["step-a"] },
          { name: "step-c", prompt: "Do C", depends_on: ["step-b"] },
        ],
      });

      assert.equal(result.success, true);
      assert.ok(result.pipelineId);

      const graph = loadDAG(dbPath, result.pipelineId);
      assert.equal(Object.keys(graph.steps).length, 3);
      assert.equal(graph.rootSteps.length, 1);
    });

    it("creates a branching pipeline", () => {
      const result = createPipeline(dbPath, {
        name: "test-branch",
        trigger_message: "test branching",
        steps: [
          { name: "research", prompt: "Research topic" },
          { name: "build", prompt: "Build collector", depends_on: [{ step: "research", when: { output_contains: "build_needed" } }] },
          { name: "report", prompt: "Write report", depends_on: ["research"] },
        ],
      });

      assert.equal(result.success, true);
      const graph = loadDAG(dbPath, result.pipelineId);
      assert.ok(graph.conditions);
      const condKeys = Object.keys(graph.conditions);
      assert.equal(condKeys.length, 1);
    });

    it("rejects duplicate step names", () => {
      const result = createPipeline(dbPath, {
        name: "test-dup",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A" },
          { name: "step-a", prompt: "A2" },
        ],
      });

      assert.equal(result.success, false);
      assert.ok(result.error.includes("Duplicate"));
    });

    it("rejects unknown dependency", () => {
      const result = createPipeline(dbPath, {
        name: "test-unknown-dep",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A", depends_on: ["nonexistent"] },
        ],
      });

      assert.equal(result.success, false);
      assert.ok(result.error.includes("unknown step"));
    });

    it("rejects empty steps array", () => {
      const result = createPipeline(dbPath, { name: "empty", trigger_message: "test", steps: [] });
      assert.equal(result.success, false);
    });

    it("rejects cyclic dependencies", () => {
      // Create steps that would form a cycle via edges
      // Since edges are derived from depends_on and must reference earlier-defined steps
      // by name, we can't easily create a cycle through the API. But validateDAG catches it.
      // Let's test validateDAG directly in graph.test.js instead.
      // Here, test that a self-dependency is caught:
      const result = createPipeline(dbPath, {
        name: "test-self-dep",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A", depends_on: ["step-a"] },
        ],
      });

      assert.equal(result.success, false);
      assert.ok(result.error.includes("Cycle"));
    });

    it("assigns correct default tiers and executors", () => {
      const result = createPipeline(dbPath, {
        name: "test-defaults",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A" },
          { name: "step-b", prompt: "B", tier: "claude", executor: "claude", depends_on: ["step-a"] },
        ],
      });

      assert.equal(result.success, true);
      const graph = loadDAG(dbPath, result.pipelineId);
      const stepA = Object.values(graph.steps).find(s => s.name === "step-a");
      const stepB = Object.values(graph.steps).find(s => s.name === "step-b");
      assert.equal(stepA.tier, "gemini-flash");
      assert.equal(stepA.executor, "lily");
      assert.equal(stepB.tier, "claude");
      assert.equal(stepB.executor, "claude");
    });
  });

  describe("startPipeline", () => {
    it("starts a pending pipeline", () => {
      const created = createPipeline(dbPath, {
        name: "test-start",
        trigger_message: "test",
        steps: [{ name: "step-a", prompt: "A" }],
        auto_start: false,
      });
      // createPipeline with auto_start: false doesn't call startPipeline
      // But our createPipeline always starts unless auto_start is false... let's check
      const pipeline = sqliteQuery(dbPath, `SELECT status FROM pipelines WHERE id = ?`, [created.pipelineId]);
      assert.equal(pipeline[0].status, "pending");

      const result = startPipeline(dbPath, created.pipelineId);
      assert.equal(result.success, true);

      const updated = sqliteQuery(dbPath, `SELECT status FROM pipelines WHERE id = ?`, [created.pipelineId]);
      assert.equal(updated[0].status, "running");
    });
  });

  describe("advanceStep", () => {
    it("advances a step on success", () => {
      const created = createPipeline(dbPath, {
        name: "test-advance",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A" },
          { name: "step-b", prompt: "B", depends_on: ["step-a"] },
        ],
      });
      startPipeline(dbPath, created.pipelineId);

      const graph = loadDAG(dbPath, created.pipelineId);
      const stepAId = graph.nameToId["step-a"];

      // Mark step-a as running first
      sqliteExec(dbPath, `UPDATE pipeline_steps SET status = 'running' WHERE id = ?`, [stepAId]);

      const result = advanceStep(dbPath, stepAId, {
        output: "Step A completed successfully",
        success: true,
      });

      assert.equal(result.advanced, true);
      assert.equal(result.pipelineComplete, false);
      assert.ok(result.readySteps.includes("step-b"));
    });

    it("marks pipeline complete when all steps done", () => {
      const created = createPipeline(dbPath, {
        name: "test-complete",
        trigger_message: "test",
        steps: [{ name: "only-step", prompt: "Do it" }],
      });
      startPipeline(dbPath, created.pipelineId);

      const graph = loadDAG(dbPath, created.pipelineId);
      const stepId = graph.nameToId["only-step"];

      sqliteExec(dbPath, `UPDATE pipeline_steps SET status = 'running' WHERE id = ?`, [stepId]);

      const result = advanceStep(dbPath, stepId, { output: "done", success: true });

      assert.equal(result.advanced, true);
      assert.equal(result.pipelineComplete, true);
      assert.equal(result.pipelineStatus, "complete");
    });

    it("retries on failure when retries remain", () => {
      const created = createPipeline(dbPath, {
        name: "test-retry",
        trigger_message: "test",
        steps: [{ name: "retry-step", prompt: "Try", max_retries: 2 }],
      });
      startPipeline(dbPath, created.pipelineId);

      const graph = loadDAG(dbPath, created.pipelineId);
      const stepId = graph.nameToId["retry-step"];

      const result = advanceStep(dbPath, stepId, { output: "", success: false, error: "timeout" });

      assert.equal(result.advanced, false);
      assert.equal(result.retrying, true);
      assert.equal(result.retryCount, 1);

      // Step should be back to pending
      const step = sqliteQuery(dbPath, `SELECT status, retry_count FROM pipeline_steps WHERE id = ?`, [stepId]);
      assert.equal(step[0].status, "pending");
      assert.equal(step[0].retry_count, 1);
    });

    it("fails step when retries exhausted", () => {
      const created = createPipeline(dbPath, {
        name: "test-fail",
        trigger_message: "test",
        steps: [{ name: "fail-step", prompt: "Fail", max_retries: 0 }],
      });
      startPipeline(dbPath, created.pipelineId);

      const graph = loadDAG(dbPath, created.pipelineId);
      const stepId = graph.nameToId["fail-step"];

      const result = advanceStep(dbPath, stepId, { output: "", success: false, error: "permanent failure" });

      assert.equal(result.advanced, true);
      assert.equal(result.pipelineComplete, true);
      assert.equal(result.pipelineStatus, "failed");
    });

    it("skips downstream steps when condition fails", () => {
      const created = createPipeline(dbPath, {
        name: "test-skip-condition",
        trigger_message: "test",
        steps: [
          { name: "decide", prompt: "Decide", step_type: "decision" },
          { name: "build", prompt: "Build", depends_on: [{ step: "decide", when: { output_contains: "build_needed" } }] },
          { name: "report", prompt: "Report", depends_on: ["decide"] },
        ],
      });
      startPipeline(dbPath, created.pipelineId);

      const graph = loadDAG(dbPath, created.pipelineId);
      const decideId = graph.nameToId["decide"];

      sqliteExec(dbPath, `UPDATE pipeline_steps SET status = 'running' WHERE id = ?`, [decideId]);

      const result = advanceStep(dbPath, decideId, {
        output: "Everything looks good, no action needed",
        success: true,
      });

      assert.equal(result.advanced, true);
      // report should be ready (unconditional), build should be skipped (condition failed)
      assert.ok(result.readySteps.includes("report"));
      assert.ok(result.skipped.includes("build"));
    });
  });

  describe("cancelPipeline", () => {
    it("cancels a running pipeline", () => {
      const created = createPipeline(dbPath, {
        name: "test-cancel",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A" },
          { name: "step-b", prompt: "B", depends_on: ["step-a"] },
        ],
      });
      startPipeline(dbPath, created.pipelineId);

      const result = cancelPipeline(dbPath, created.pipelineId);
      assert.equal(result.success, true);

      const pipeline = sqliteQuery(dbPath, `SELECT status FROM pipelines WHERE id = ?`, [created.pipelineId]);
      assert.equal(pipeline[0].status, "cancelled");

      const steps = sqliteQuery(dbPath, `SELECT status FROM pipeline_steps WHERE pipeline_id = ?`, [created.pipelineId]);
      assert.ok(steps.every(s => s.status === "cancelled"));
    });

    it("rejects cancelling completed pipeline", () => {
      const created = createPipeline(dbPath, {
        name: "test-cancel-done",
        trigger_message: "test",
        steps: [{ name: "step-a", prompt: "A" }],
      });
      startPipeline(dbPath, created.pipelineId);

      // Complete it
      const graph = loadDAG(dbPath, created.pipelineId);
      const stepId = graph.nameToId["step-a"];
      sqliteExec(dbPath, `UPDATE pipeline_steps SET status = 'running' WHERE id = ?`, [stepId]);
      advanceStep(dbPath, stepId, { output: "done", success: true });

      const result = cancelPipeline(dbPath, created.pipelineId);
      assert.equal(result.success, false);
    });
  });

  describe("schedulePipeline", () => {
    it("creates a trigger", () => {
      const created = createPipeline(dbPath, {
        name: "test-schedule",
        trigger_message: "test",
        steps: [{ name: "step-a", prompt: "A" }],
      });

      const result = schedulePipeline(dbPath, {
        pipeline_id: created.pipelineId,
        schedule: "0 5 * * *",
        timezone: "America/New_York",
      });

      assert.equal(result.success, true);
      assert.ok(result.triggerId);

      const trigger = sqliteQuery(dbPath, `SELECT * FROM pipeline_triggers WHERE id = ?`, [result.triggerId]);
      assert.equal(trigger.length, 1);
      assert.equal(trigger[0].schedule, "0 5 * * *");
    });

    it("rejects invalid cron expression", () => {
      const created = createPipeline(dbPath, {
        name: "test-bad-cron",
        trigger_message: "test",
        steps: [{ name: "step-a", prompt: "A" }],
      });

      const result = schedulePipeline(dbPath, {
        pipeline_id: created.pipelineId,
        schedule: "not a cron",
      });

      assert.equal(result.success, false);
      assert.ok(result.error.includes("5 fields"));
    });
  });

  describe("getPipelineStatus", () => {
    it("returns status for specific pipeline", () => {
      const created = createPipeline(dbPath, {
        name: "test-status",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "A" },
          { name: "step-b", prompt: "B", depends_on: ["step-a"] },
        ],
      });

      const status = getPipelineStatus(dbPath, created.pipelineId);
      assert.equal(status.found, true);
      assert.equal(status.steps.length, 2);
      assert.ok(status.ready.length > 0);
    });

    it("returns all active pipelines when no ID given", () => {
      const status = getPipelineStatus(dbPath);
      assert.equal(status.found, true);
      assert.ok(Array.isArray(status.pipelines));
    });

    it("returns not found for nonexistent ID", () => {
      const status = getPipelineStatus(dbPath, "nonexistent-id");
      assert.equal(status.found, false);
    });
  });

  describe("lilyToday", () => {
    it("returns a formatted summary", () => {
      const summary = lilyToday(dbPath);
      assert.ok(typeof summary === "string");
      assert.ok(summary.includes("Pipeline Status"));
      assert.ok(summary.includes("Memory:"));
    });
  });

  describe("pipelineTick", () => {
    it("returns ready steps from running pipelines", () => {
      const created = createPipeline(dbPath, {
        name: "test-tick",
        trigger_message: "test",
        steps: [
          { name: "step-a", prompt: "Do step A" },
          { name: "step-b", prompt: "Do step B", depends_on: ["step-a"] },
        ],
      });
      startPipeline(dbPath, created.pipelineId);

      const tick = pipelineTick(dbPath);
      assert.ok(tick.work.length > 0);
      const stepA = tick.work.find(w => w.pipelineName === "test-tick" && w.stepName === "step-a");
      assert.ok(stepA);
      assert.equal(stepA.prompt, "Do step A");
    });

    it("returns empty when no work pending", () => {
      // Cancel the pipeline so it's not running
      const pipelines = sqliteQuery(dbPath,
        `SELECT id FROM pipelines WHERE name = 'test-tick'`
      );
      for (const p of pipelines) cancelPipeline(dbPath, p.id);

      const tick = pipelineTick(dbPath);
      // Filter to just our test pipeline's work (other tests may leave running pipelines)
      const tickWork = tick.work.filter(w => w.pipelineName === "test-tick");
      assert.equal(tickWork.length, 0);
    });

    it("includes parent context in ready steps", () => {
      const created = createPipeline(dbPath, {
        name: "test-tick-context",
        trigger_message: "test",
        steps: [
          { name: "first", prompt: "Do first" },
          { name: "second", prompt: "Do second with {{prev_result}}", depends_on: ["first"] },
        ],
      });
      startPipeline(dbPath, created.pipelineId);

      // Complete first step
      const graph = loadDAG(dbPath, created.pipelineId);
      const firstId = graph.nameToId["first"];
      sqliteExec(dbPath, `UPDATE pipeline_steps SET status = 'running' WHERE id = ?`, [firstId]);
      advanceStep(dbPath, firstId, { output: "First step output data", success: true });

      const tick = pipelineTick(dbPath);
      const secondStep = tick.work.find(w => w.stepName === "second");
      assert.ok(secondStep);
      assert.ok(secondStep.parentContext.includes("First step output data"));
    });
  });

  describe("schema migrations", () => {
    it("creates pipeline tables via migration", () => {
      // Tables should already exist from before()
      const tables = sqliteQuery(dbPath,
        `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'pipeline%' ORDER BY name`
      );
      const names = tables.map(t => t.name);
      assert.ok(names.includes("pipeline_edges"));
      assert.ok(names.includes("pipeline_steps"));
      assert.ok(names.includes("pipeline_triggers"));
      assert.ok(names.includes("pipelines"));
    });

    it("records migration version", () => {
      const version = sqliteQuery(dbPath, `SELECT MAX(version) as v FROM schema_version`);
      assert.ok(version[0].v >= 1);
    });

    it("is idempotent on second run", () => {
      const result = runMigrations(dbPath);
      assert.equal(result.applied, 0); // Already applied
    });
  });
});
