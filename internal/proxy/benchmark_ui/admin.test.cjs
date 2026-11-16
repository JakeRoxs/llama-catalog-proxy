const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

class Element {
  constructor(tag = "div") {
    this.tag = tag;
    this.children = [];
    this.hidden = false;
    this.value = "";
    this.textContent = "";
    this.listeners = {};
  }
  addEventListener(name, callback) { this.listeners[name] = callback; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  querySelector() { return new Element("input"); }
}

function createDashboard(fetchImpl) {
  const elements = new Map();
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, new Element());
      return elements.get(id);
    },
    createElement(tag) { return new Element(tag); },
    createTextNode(text) { const node = new Element("text"); node.textContent = text; return node; },
  };
  const context = { document, fetch: fetchImpl, Date, encodeURIComponent, console };
  const source = fs.readFileSync(path.join(__dirname, "admin.js"), "utf8");
  vm.runInNewContext(source, context, { filename: "admin.js" });
  return { context, elements };
}

function textOf(node) {
  return [node.textContent, ...node.children.flatMap(textOf)].filter(Boolean).join(" ");
}

test("admin dashboard uses OIDC session and renders only the safe overview fields", async () => {
  const calls = [];
  const responses = {
    "/admin/api/v1/session": { subject: "local-user", roles: ["view"], csrf_token: "csrf" },
    "/admin/api/v1/overview": {
      node_id: "udms", updated_at: "2026-11-16T00:00:00Z",
      models: [{ id: "bigboi/comfyui_auto", owned_by: "bigboi" }],
      peers: [{ node_id: "bigboi", status: "fresh" }],
      request_summary_state: "available",
      request_summary: { count: 4, outcomes: { "200": 3, "503": 1 }, duration_ms: { p50: 20, sample_count: 4 } },
    },
    "/admin/api/v1/activity": { state: "available", events: [
      { time: "2026-11-16T00:00:00Z", backend_id: "local", status: "http_success", duration_ms: 12 },
    ] },
  };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => responses[url] };
  };
  const { elements } = createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(calls.map((call) => call.url), ["/admin/api/v1/session", "/admin/api/v1/overview", "/admin/api/v1/activity"]);
  assert.ok(calls.every((call) => call.options.credentials === "same-origin"));
  assert.ok(calls.every((call) => call.options.headers === undefined || call.options.headers.Authorization === undefined));
  assert.equal(elements.get("dashboard").hidden, false);
  assert.equal(elements.get("signed-in-as").textContent, "local-user");
  assert.equal(elements.get("node-id").textContent, "udms");
  assert.equal(elements.get("model-count").textContent, "1");
  assert.equal(elements.get("request-count").textContent, "4");
  assert.match(textOf(elements.get("models")), /bigboi\/comfyui_auto/);
  assert.match(textOf(elements.get("peers")), /bigboi.*fresh/);
  assert.match(textOf(elements.get("request-outcomes")), /200.*3.*503.*1/);
  assert.match(textOf(elements.get("activity")), /local.*http_success.*12 ms/);
});

test("admin dashboard does not query sensitive endpoints", async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => url.endsWith("/session")
      ? { subject: "viewer", roles: ["view"], csrf_token: "csrf" }
      : url.endsWith("/overview")
        ? { node_id: "udms", updated_at: "2026-11-16T00:00:00Z", models: [], peers: [], request_summary_state: "not_configured" }
        : { state: "not_configured", events: [] } };
  };
  createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(calls.map((call) => call.url), ["/admin/api/v1/session", "/admin/api/v1/overview", "/admin/api/v1/activity"]);
  assert.ok(calls.every((call) => !/config|logs|benchmark|management|federation\/grants/i.test(call.url)));
});
