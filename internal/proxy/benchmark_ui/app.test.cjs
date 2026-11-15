"use strict";

const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function createUI() {
  class Element {
    constructor(tag) {
      this.tag = tag;
      this.children = [];
      this.textContent = "";
    }
    appendChild(child) { this.children.push(child); return child; }
    append(...children) { this.children.push(...children); }
    replaceChildren() { this.children = []; }
    addEventListener() {}
  }
  const elements = new Map();
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, new Element("div"));
      return elements.get(id);
    },
    createElement(tag) { return new Element(tag); },
  };
  const context = vm.createContext({ document, window: { isSecureContext: true } });
  vm.runInContext(readFileSync(join(__dirname, "app.js"), "utf8"), context);
  return { context, content: document.getElementById("evidence-content") };
}

function text(node) {
  return [node.textContent, ...node.children.flatMap(text)];
}

test("saved run displays measured and unknown sample evidence distinctly", () => {
  const { context, content } = createUI();
  context.record = {
    run_id: "fixture", status: "partial",
    requested: { backend: "local", model: "fake", repetitions: 2 },
    workloads: [{ name: "workload", samples: [
      { iteration: 1, status: "completed", correctness: "passed", duration_ms: 0,
        requests: [{ diagnostic_state: "request_id_unavailable" }] },
      { iteration: 2, status: "canceled", correctness: "not_evaluated", requests: [] },
    ] }],
    limitations: [],
  };
  vm.runInContext("displayRun(record)", context);
  const output = text(content);
  assert.ok(output.includes("0.00"));
  assert.ok(output.includes("request_id_unavailable"));
  assert.ok(output.filter((value) => value === "unknown").length >= 3);
  assert.ok(output.includes("not recorded"));
});

test("saved canceled run can omit workload evidence", () => {
  const { context, content } = createUI();
  context.record = {
    run_id: "fixture", status: "canceled",
    requested: { backend: "local", model: "fake", repetitions: 1 },
    workloads: null, limitations: [],
  };
  vm.runInContext("displayRun(record)", context);
  assert.ok(text(content).includes("No workload evidence was recorded."));
});

test("comparison distinguishes missing measurements and confounders", () => {
  const { context, content } = createUI();
  const coverage = { recorded: false, samples_recorded: 0, samples_expected: 1 };
  const correctness = { passed: 0, failed: 0, not_evaluated: 0 };
  const failures = {
    failed_samples: 0, canceled_samples: 0, incomplete_transport: 0, unknown_transport: 0,
    incomplete_generation: 0, unknown_generation: 0, truncated_generation: 0,
  };
  context.report = {
    baseline_run_id: "baseline", candidate_run_id: "candidate",
    interpretation: "descriptive only", confounders: ["cache state unverified"],
    context: { per_round_acceptance_state: "unsupported" },
    workloads: [{
      name: "workload",
      coverage: { baseline: coverage, candidate: coverage },
      correctness: { baseline: correctness, candidate: correctness },
      failures: { baseline: failures, candidate: failures },
      metrics: [{
        name: "end_to_end_ms", baseline: { mean: 0, sample_count: 1 },
        candidate: { sample_count: 0 }, attribution: "benchmark client",
      }],
    }],
  };
  vm.runInContext("displayComparison(report)", context);
  const output = text(content);
  assert.ok(output.some((value) => value.includes("workload missing")));
  assert.ok(output.includes("0.00 (1)"));
  assert.ok(output.includes("unknown (0)"));
  assert.ok(output.includes("cache state unverified"));
  assert.ok(output.includes("unsupported"));
});
