"use strict";

const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function createUI(publicReadOnly = false) {
  const requests = [];
  class Element {
    constructor(tag) {
      this.tag = tag;
      this.children = [];
      this.textContent = "";
      this.hidden = false;
      this.classes = new Set();
      this.classList = {
        add: (value) => this.classes.add(value),
        remove: (value) => this.classes.delete(value),
        contains: (value) => this.classes.has(value),
      };
    }
    appendChild(child) { this.children.push(child); return child; }
    append(...children) { this.children.push(...children); }
    replaceChildren() { this.children = []; }
    addEventListener() {}
  }
  const elements = new Map();
  const document = {
    documentElement: { dataset: { publicReadOnly: publicReadOnly ? "true" : "false" } },
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, new Element("div"));
      return elements.get(id);
    },
    createElement(tag) { return new Element(tag); },
  };
  const context = vm.createContext({
    document,
    window: { isSecureContext: true },
    fetch: async (url, options) => {
      requests.push({ url, options });
      return { ok: true, json: async () => url.endsWith("/runs") ? { runs: [], truncated: false } : ({}) };
    },
  });
  const source = readFileSync(join(__dirname, "app.js"), "utf8");
  const ready = vm.runInContext(`(async function () {
    globalThis.api = api;
    globalThis.displayRun = displayRun;
    globalThis.displayComparison = displayComparison;
    ${source}
  })()`, context);
  return { context, content: document.getElementById("evidence-content"), requests, ready };
}

function text(node) {
  return [node.textContent, ...node.children.flatMap(text)];
}

test("public read-only mode lists saved summaries without a benchmark token or write controls", async () => {
  const { context, requests, ready } = createUI(true);
  await ready;
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/benchmark/v1/runs");
  assert.equal(requests[0].options.method || "GET", "GET");
  assert.equal(requests[0].options.headers.Authorization, undefined);
  assert.equal(requests[0].options.credentials, "same-origin");
  assert.equal(context.document.getElementById("workspace").hidden, false);
  assert.equal(context.document.getElementById("workspace").classList.contains("read-only"), true);
  assert.equal(context.document.getElementById("run-panel").hidden, true);
  assert.equal(context.document.getElementById("job-panel").hidden, true);
  assert.equal(context.document.getElementById("compare").hidden, true);
  assert.equal(context.document.getElementById("read-only-note").hidden, false);
});

test("API requests include same-origin authentication cookies", async () => {
  const { context, requests } = createUI();
  context.token = "fixture-token";
  await vm.runInContext("api('/runs')", context);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/benchmark/v1/runs");
  assert.equal(requests[0].options.credentials, "same-origin");
  assert.equal(requests[0].options.redirect, "error");
});

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
