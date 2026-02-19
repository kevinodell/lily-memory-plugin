import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  buildDAG,
  validateDAG,
  readySteps,
  skippableSteps,
  evaluateCondition,
  detectCycles,
  topoSort,
  checkPipelineComplete,
} from "../lib/graph.js";

// Helper: create a step object
function step(id, name, opts = {}) {
  return {
    id,
    name,
    status: opts.status || "pending",
    step_type: opts.step_type || "task",
    depends_on_all: opts.depends_on_all !== undefined ? opts.depends_on_all : 1,
    output_artifact: opts.output || null,
    tier: opts.tier || "gemini-flash",
    executor: opts.executor || "lily",
  };
}

// Helper: create an edge
function edge(parent, child, condition = null) {
  return { parent_step_id: parent, child_step_id: child, condition };
}

describe("Graph Engine", () => {

  describe("buildDAG", () => {
    it("builds a simple linear DAG", () => {
      const steps = [step("a", "step-a"), step("b", "step-b")];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(graph.children["a"], ["b"]);
      assert.deepEqual(graph.parents["b"], ["a"]);
      assert.deepEqual(graph.rootSteps, ["a"]);
      assert.equal(graph.nameToId["step-a"], "a");
    });

    it("identifies multiple root steps", () => {
      const steps = [step("a", "s-a"), step("b", "s-b"), step("c", "s-c")];
      const edges = [edge("a", "c"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      assert.equal(graph.rootSteps.length, 2);
      assert.ok(graph.rootSteps.includes("a"));
      assert.ok(graph.rootSteps.includes("b"));
    });

    it("stores edge conditions", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const edges = [edge("a", "b", { output_contains: "build_needed" })];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(graph.conditions["a->b"], { output_contains: "build_needed" });
    });
  });

  describe("evaluateCondition", () => {
    it("returns true for null condition (unconditional)", () => {
      assert.equal(evaluateCondition(null, "anything"), true);
      assert.equal(evaluateCondition(undefined, "anything"), true);
    });

    it("matches output_contains (case-insensitive)", () => {
      assert.equal(evaluateCondition({ output_contains: "build" }, "We need to BUILD a collector"), true);
      assert.equal(evaluateCondition({ output_contains: "deploy" }, "We need to build"), false);
    });

    it("matches output_match regex", () => {
      assert.equal(evaluateCondition({ output_match: "need.*collector" }, "We need a new collector"), true);
      assert.equal(evaluateCondition({ output_match: "^ERROR" }, "ERROR: something broke"), true);
      assert.equal(evaluateCondition({ output_match: "^ERROR" }, "No errors here"), false);
    });

    it("returns false for invalid regex", () => {
      assert.equal(evaluateCondition({ output_match: "[invalid" }, "test"), false);
    });

    it("returns true for unknown condition types", () => {
      assert.equal(evaluateCondition({ unknown_field: "foo" }, "test"), true);
    });

    it("handles empty output", () => {
      assert.equal(evaluateCondition({ output_contains: "x" }, ""), false);
      assert.equal(evaluateCondition({ output_contains: "x" }, null), false);
    });
  });

  describe("readySteps", () => {
    it("returns root steps as ready", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      const ready = readySteps(graph);
      assert.deepEqual(ready, ["a"]);
    });

    it("returns child when parent is complete", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      const ready = readySteps(graph);
      assert.deepEqual(ready, ["b"]);
    });

    it("does not return child when parent is still running", () => {
      const steps = [
        step("a", "s-a", { status: "running" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(readySteps(graph), []);
    });

    it("handles fan-in ALL mode: waits for all parents", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "pending" }),
        step("c", "s-c", { depends_on_all: 1 }),
      ];
      const edges = [edge("a", "c"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(readySteps(graph), ["b"]); // b is root, c waits for both
    });

    it("handles fan-in ANY mode: fires when any parent completes", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "pending" }),
        step("c", "s-c", { depends_on_all: 0 }),
      ];
      const edges = [edge("a", "c"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      const ready = readySteps(graph);
      assert.ok(ready.includes("b")); // root
      assert.ok(ready.includes("c")); // any-mode, a is complete
    });

    it("respects conditional edges", () => {
      const steps = [
        step("a", "s-a", { status: "complete", output: "all good, no build needed" }),
        step("b", "s-b"), // conditional child
      ];
      const edges = [edge("a", "b", { output_contains: "build_needed" })];
      const graph = buildDAG(steps, edges);

      // Condition doesn't match, so b is not ready
      assert.deepEqual(readySteps(graph), []);
    });

    it("activates child when condition matches", () => {
      const steps = [
        step("a", "s-a", { status: "complete", output: "analysis says build_needed for collectors" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b", { output_contains: "build_needed" })];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(readySteps(graph), ["b"]);
    });

    it("skips already completed/failed/skipped steps", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "complete" }),
        step("c", "s-c", { status: "failed" }),
      ];
      const edges = [edge("a", "b"), edge("a", "c")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(readySteps(graph), []);
    });
  });

  describe("skippableSteps", () => {
    it("skips child of failed parent in ALL mode", () => {
      const steps = [
        step("a", "s-a", { status: "failed" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(skippableSteps(graph), ["b"]);
    });

    it("skips child when all conditional edges fail", () => {
      const steps = [
        step("a", "s-a", { status: "complete", output: "nothing special" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b", { output_contains: "build_needed" })];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(skippableSteps(graph), ["b"]);
    });

    it("does not skip when parent is still pending", () => {
      const steps = [
        step("a", "s-a", { status: "pending" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(skippableSteps(graph), []);
    });

    it("does not skip in ANY mode when one parent completed", () => {
      const steps = [
        step("a", "s-a", { status: "complete", output: "done" }),
        step("b", "s-b", { status: "failed" }),
        step("c", "s-c", { depends_on_all: 0 }),
      ];
      const edges = [edge("a", "c"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      assert.deepEqual(skippableSteps(graph), []);
    });
  });

  describe("detectCycles", () => {
    it("returns false for acyclic graph", () => {
      const steps = [step("a", "s-a"), step("b", "s-b"), step("c", "s-c")];
      const edges = [edge("a", "b"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      const result = detectCycles(graph);
      assert.equal(result.hasCycle, false);
      assert.equal(result.cycle, null);
    });

    it("detects simple cycle", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const graph = buildDAG(steps, []);
      // Manually add cycle
      graph.children["a"] = ["b"];
      graph.children["b"] = ["a"];
      graph.parents["a"] = ["b"];
      graph.parents["b"] = ["a"];
      graph.rootSteps = [];

      const result = detectCycles(graph);
      assert.equal(result.hasCycle, true);
      assert.ok(result.cycle.length >= 2);
    });

    it("detects 3-node cycle", () => {
      const steps = [step("a", "s-a"), step("b", "s-b"), step("c", "s-c")];
      const graph = buildDAG(steps, []);
      graph.children["a"] = ["b"];
      graph.children["b"] = ["c"];
      graph.children["c"] = ["a"];
      graph.parents["b"] = ["a"];
      graph.parents["c"] = ["b"];
      graph.parents["a"] = ["c"];
      graph.rootSteps = [];

      const result = detectCycles(graph);
      assert.equal(result.hasCycle, true);
    });

    it("handles single-node graph", () => {
      const steps = [step("a", "s-a")];
      const graph = buildDAG(steps, []);

      const result = detectCycles(graph);
      assert.equal(result.hasCycle, false);
    });
  });

  describe("topoSort", () => {
    it("sorts a linear chain", () => {
      const steps = [step("a", "s-a"), step("b", "s-b"), step("c", "s-c")];
      const edges = [edge("a", "b"), edge("b", "c")];
      const graph = buildDAG(steps, edges);

      const sorted = topoSort(graph);
      assert.ok(sorted);
      assert.equal(sorted.indexOf("a"), 0);
      assert.ok(sorted.indexOf("b") > sorted.indexOf("a"));
      assert.ok(sorted.indexOf("c") > sorted.indexOf("b"));
    });

    it("sorts a diamond DAG", () => {
      const steps = [
        step("a", "s-a"), step("b", "s-b"),
        step("c", "s-c"), step("d", "s-d"),
      ];
      const edges = [edge("a", "b"), edge("a", "c"), edge("b", "d"), edge("c", "d")];
      const graph = buildDAG(steps, edges);

      const sorted = topoSort(graph);
      assert.ok(sorted);
      assert.equal(sorted.length, 4);
      assert.ok(sorted.indexOf("a") < sorted.indexOf("b"));
      assert.ok(sorted.indexOf("a") < sorted.indexOf("c"));
      assert.ok(sorted.indexOf("b") < sorted.indexOf("d"));
      assert.ok(sorted.indexOf("c") < sorted.indexOf("d"));
    });

    it("returns null for cyclic graph", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const graph = buildDAG(steps, []);
      graph.children["a"] = ["b"];
      graph.children["b"] = ["a"];
      graph.parents["a"] = ["b"];
      graph.parents["b"] = ["a"];
      graph.rootSteps = [];

      const sorted = topoSort(graph);
      assert.equal(sorted, null);
    });
  });

  describe("validateDAG", () => {
    it("validates a correct DAG", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const edges = [edge("a", "b")];
      const graph = buildDAG(steps, edges);

      const result = validateDAG(graph);
      assert.equal(result.valid, true);
      assert.equal(result.errors.length, 0);
    });

    it("rejects empty DAG", () => {
      const graph = buildDAG([], []);
      const result = validateDAG(graph);
      assert.equal(result.valid, false);
      assert.ok(result.errors.some(e => e.includes("no steps")));
    });

    it("rejects DAG with too many steps", () => {
      const steps = Array.from({ length: 5 }, (_, i) => step(`s${i}`, `step-${i}`));
      const graph = buildDAG(steps, []);
      const result = validateDAG(graph, { maxSteps: 3 });
      assert.equal(result.valid, false);
      assert.ok(result.errors.some(e => e.includes("5 steps")));
    });

    it("rejects cyclic DAG", () => {
      const steps = [step("a", "s-a"), step("b", "s-b")];
      const graph = buildDAG(steps, []);
      graph.children["a"] = ["b"];
      graph.children["b"] = ["a"];
      graph.parents["a"] = ["b"];
      graph.parents["b"] = ["a"];
      graph.rootSteps = [];

      const result = validateDAG(graph);
      assert.equal(result.valid, false);
      assert.ok(result.errors.some(e => e.includes("Cycle")));
    });

    it("warns about decision nodes without default edge", () => {
      const steps = [
        step("a", "s-a", { step_type: "decision" }),
        step("b", "s-b"),
      ];
      const edges = [edge("a", "b", { output_contains: "foo" })];
      const graph = buildDAG(steps, edges);

      const result = validateDAG(graph);
      assert.ok(result.errors.some(e => e.includes("default")));
    });

    it("passes decision node with default edge", () => {
      const steps = [
        step("a", "s-a", { step_type: "decision" }),
        step("b", "s-b"),
        step("c", "s-c"),
      ];
      const edges = [
        edge("a", "b", { output_contains: "foo" }),
        edge("a", "c"), // default (no condition)
      ];
      const graph = buildDAG(steps, edges);

      const result = validateDAG(graph);
      const decisionErrors = result.errors.filter(e => e.includes("default"));
      assert.equal(decisionErrors.length, 0);
    });
  });

  describe("checkPipelineComplete", () => {
    it("returns running when steps are pending", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "pending" }),
      ];
      const graph = buildDAG(steps, [edge("a", "b")]);

      const result = checkPipelineComplete(graph);
      assert.equal(result.complete, false);
      assert.equal(result.status, "running");
    });

    it("returns complete when all done", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "complete" }),
      ];
      const graph = buildDAG(steps, [edge("a", "b")]);

      const result = checkPipelineComplete(graph);
      assert.equal(result.complete, true);
      assert.equal(result.status, "complete");
    });

    it("returns failed when any step failed", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "failed" }),
      ];
      const graph = buildDAG(steps, [edge("a", "b")]);

      const result = checkPipelineComplete(graph);
      assert.equal(result.complete, true);
      assert.equal(result.status, "failed");
    });

    it("counts skipped as terminal", () => {
      const steps = [
        step("a", "s-a", { status: "complete" }),
        step("b", "s-b", { status: "skipped" }),
      ];
      const graph = buildDAG(steps, [edge("a", "b")]);

      const result = checkPipelineComplete(graph);
      assert.equal(result.complete, true);
      assert.equal(result.status, "complete");
    });
  });
});
