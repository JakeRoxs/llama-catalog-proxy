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
    this.classList = { toggle() {} };
  }
  addEventListener(name, callback) { this.listeners[name] = callback; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  querySelector() { return new Element("input"); }
  querySelectorAll() { return []; }
}

async function createDashboard(fetchImpl) {
  const elements = new Map();
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, new Element());
      return elements.get(id);
    },
    createElement(tag) { return new Element(tag); },
    createTextNode(text) { const node = new Element("text"); node.textContent = text; return node; },
  };
  const context = {
    document, fetch: fetchImpl, Date, encodeURIComponent, console, URLSearchParams,
    crypto: { getRandomValues(bytes) { bytes.fill(0x2a); return bytes; } },
  };
  const source = fs.readFileSync(path.join(__dirname, "admin.js"), "utf8");
  await vm.runInNewContext(`(async () => {\n${source}\n})()`, context, { filename: "admin.js" });
  return { context, elements };
}

function textOf(node) {
  return [node.textContent, ...node.children.flatMap(textOf)].filter(Boolean).join(" ");
}

// Shared fetch stub for the catalog-read and inference grant UI tests. It records every
// request in `calls` in order, serves the shared session/overview/invitations/grants/
// catalog/inference/audit fixtures, uses the supplied members payload, and answers POST
// mutation requests with the same grant response. Unmatched URLs fall back to the prior
// default grant response so existing dashboard rendering behavior is preserved.
function adminGrantFetch(calls, membersResponse) {
  const responses = {
    "/admin/api/v1/session": { subject: "operator", roles: ["view", "administer_federation"], csrf_token: "csrf" },
    "/admin/api/v1/overview": { node_id: "node-a", updated_at: "2026-11-16T00:00:00Z", models: [], peers: [], request_summary_state: "not_configured" },
    "/admin/api/v1/federation/invitations": { state: "available", invitations: [] },
    "/admin/api/v1/federation/catalog-grants": { state: "available", grants: [] },
    "/admin/api/v1/federation/inference-grants": { state: "available", grants: [] },
    "/admin/api/v1/federation/grants": { state: "not_configured", grants: [] },
    "/admin/api/v1/audit/events?limit=50": { state: "available", events: [] },
  };
  return async (url, options = {}) => {
    calls.push({ url, options });
    if (url === "/admin/api/v1/federation/members") {
      return { ok: true, status: 200, json: async () => membersResponse };
    }
    if (options && options.method === "POST") {
      return { ok: true, status: 200, json: async () => ({ grant: { node_id: "node-b" } }) };
    }
    return { ok: true, status: 200, json: async () => responses[url] ?? { grant: { node_id: "node-b" } } };
  };
}

test("admin dashboard uses OIDC session and renders only the safe overview fields", async () => {
  const calls = [];
  const responses = {
    "/admin/api/v1/session": { subject: "local-user", roles: ["view", "administer_federation"], csrf_token: "csrf" },
    "/admin/api/v1/overview": {
      node_id: "udms", updated_at: "2026-11-16T00:00:00Z",
      models: [{ id: "bigboi/comfyui_auto", owned_by: "bigboi" }],
      peers: [{ node_id: "bigboi", status: "fresh", identity_state: "paired", authorization_state: "scopes_configured", configured_scopes: ["catalog_read", "inference"], model_readiness_state: "unknown" }],
      federation_policy: {
        state: "fresh",
        client_crl: { state: "fresh", updated_at: "2026-11-15T23:00:00Z", expires_at: "2026-11-16T23:00:00Z", max_age_seconds: 86400 },
        peer_crls: [{ node_id: "bigboi", state: "fresh", updated_at: "2026-11-15T23:00:00Z", expires_at: "2026-11-16T23:00:00Z", max_age_seconds: 86400 }],
      },
      request_summary_state: "available",
      request_summary: { count: 4, outcomes: { "200": 3, "503": 1 }, duration_ms: { p50: 20, sample_count: 4 } },
    },
    "/admin/api/v1/activity": { state: "available", events: [
      { time: "2026-11-16T00:00:00Z", backend_id: "local", status: "http_success", duration_ms: 12 },
    ] },
    "/admin/api/v1/federation/members": { state: "available", members: [{ node_id: "peer-b", certificate_sha256: "a".repeat(64), paired_at: "2026-11-16T00:00:00Z" }] },
    "/admin/api/v1/federation/invitations": { state: "available", invitations: [{ node_id: "peer-c", expires_at: "2026-11-16T00:10:00Z" }] },
    "/admin/api/v1/federation/grants": { state: "available", grants: [{ id: "0123456789abcdef0123456789abcdef", requester_node_id: "peer-b", backend_id: "backend-a", target_id: "target-a", model_ids: ["model-a"], expires_at: "2026-11-17T00:00:00Z" }] },
    "/admin/api/v1/federation/catalog-grants": { state: "available", grants: [{ node_id: "peer-b", certificate_sha256: "a".repeat(64), current_fingerprint: true }] },
    "/admin/api/v1/federation/inference-grants": { state: "available", grants: [{ node_id: "peer-b", certificate_sha256: "a".repeat(64), allow_incoming_inference: true, route_outgoing_inference: false, current_fingerprint: true }] },
    "/admin/api/v1/audit/events?limit=50": { state: "available", retention_days: 30, events: [] },
  };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => responses[url] };
  };
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(calls.map((call) => call.url), ["/admin/api/v1/session", "/admin/api/v1/overview", "/admin/api/v1/activity", "/admin/api/v1/federation/members", "/admin/api/v1/federation/invitations", "/admin/api/v1/federation/grants", "/admin/api/v1/federation/catalog-grants", "/admin/api/v1/federation/inference-grants", "/admin/api/v1/audit/events?limit=50"]);
  assert.ok(calls.every((call) => call.options.credentials === "same-origin"));
  assert.ok(calls.every((call) => call.options.headers === undefined || call.options.headers.Authorization === undefined));
  assert.equal(elements.get("dashboard").hidden, false);
  assert.equal(elements.get("signed-in-as").textContent, "local-user");
  assert.equal(elements.get("node-id").textContent, "udms");
  assert.equal(elements.get("model-count").textContent, "1");
  assert.equal(elements.get("request-count").textContent, "4");
  assert.match(textOf(elements.get("models")), /bigboi\/comfyui_auto/);
  assert.match(textOf(elements.get("peers")), /bigboi.*fresh/);
  assert.match(textOf(elements.get("peers")), /Identity paired; scopes scopes_configured \(catalog_read, inference\); model readiness unknown/);
  assert.match(elements.get("federation-policy-state").textContent, /within their freshness limits/);
  assert.match(textOf(elements.get("federation-policy")), /Inbound clients.*fresh.*Outbound: bigboi/);
  assert.match(textOf(elements.get("request-outcomes")), /200.*3.*503.*1/);
  assert.match(textOf(elements.get("activity")), /local.*http_success.*12 ms/);
  assert.match(elements.get("activity-state").textContent, /1 recent request records/);
  assert.equal(elements.get("pairing-panel").hidden, false);
  assert.match(textOf(elements.get("paired-members")), /peer-b/);
  assert.match(textOf(elements.get("pending-invitations")), /peer-c/);
  assert.match(textOf(elements.get("owner-grants")), /peer-b.*backend-a.*model-a/);
  assert.match(textOf(elements.get("catalog-read-grants")), /peer-b.*Fingerprint matches current paired identity/);
  assert.match(textOf(elements.get("inference-grants")), /peer-b.*Allowed.*Off/);
  await elements.get("refresh").listeners.click();
  assert.equal(calls.length, 18, "manual refresh reloads overview, activity, pairing, catalog and inference grants, owner grants and audit events");
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
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(calls.map((call) => call.url), ["/admin/api/v1/session", "/admin/api/v1/overview", "/admin/api/v1/activity"]);
  assert.ok(calls.every((call) => !/config|logs|benchmark|management|federation\/grants/i.test(call.url)));
  assert.equal(elements.get("pairing-panel").hidden, true);
  assert.ok(calls.every((call) => !call.url.includes("/federation/")), "view-only users do not query pairing APIs");
  assert.equal(elements.get("request-summary-state").textContent, "Diagnostics API not configured");
  assert.match(textOf(elements.get("request-outcomes")), /Unknown until diagnostics API and bounded request history are enabled/);
});

test("catalog-read grant UI requires explicit confirmation and uses CSRF-protected API", async () => {
  const calls = [];
  const fetchImpl = adminGrantFetch(calls, { state: "available", members: [{ node_id: "node-b", certificate_sha256: "a".repeat(64), paired_at: "2026-11-16T00:00:00Z" }] });
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  elements.set("catalog-read-node", new Element("input"));
  elements.set("catalog-read-confirm", new Element("input"));
  elements.get("catalog-read-node").value = "node-b";
  elements.get("catalog-read-confirm").checked = true;
  await elements.get("catalog-read-form").listeners.submit({ preventDefault() {} });
  const mutation = calls.find((call) => call.url === "/admin/api/v1/federation/catalog-grants" && call.options.method === "POST");
  assert.ok(mutation);
  assert.equal(mutation.options.headers["X-Llama-Catalog-CSRF"], "csrf");
  assert.deepEqual(JSON.parse(mutation.options.body), { node_id: "node-b" });
  assert.equal(mutation.options.headers.Authorization, undefined);
  assert.match(elements.get("pairing-notice").textContent, /approved for the current paired certificate/);
});

test("inference grant UI submits independent directions with CSRF protection", async () => {
  const calls = [];
  const fetchImpl = adminGrantFetch(calls, { state: "available", members: [{ node_id: "node-b" }] });
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  elements.set("inference-grant-node", new Element("input"));
  elements.set("inference-grant-incoming", new Element("input"));
  elements.set("inference-grant-outgoing", new Element("input"));
  elements.set("inference-grant-confirm", new Element("input"));
  elements.get("inference-grant-node").value = "node-b";
  elements.get("inference-grant-incoming").checked = true;
  elements.get("inference-grant-outgoing").checked = false;
  elements.get("inference-grant-confirm").checked = true;
  await elements.get("inference-grant-form").listeners.submit({ preventDefault() {} });
  const mutation = calls.find((call) => call.url === "/admin/api/v1/federation/inference-grants" && call.options.method === "POST");
  assert.ok(mutation);
  assert.equal(mutation.options.headers["X-Llama-Catalog-CSRF"], "csrf");
  assert.deepEqual(JSON.parse(mutation.options.body), {
    node_id: "node-b", allow_incoming_inference: true, route_outgoing_inference: false,
  });
});

test("federation administrator creates a one-time invitation through the protected API", async () => {
  const calls = [];
  const responses = {
    "/admin/api/v1/session": { subject: "operator", roles: ["view", "administer_federation"], csrf_token: "csrf-token" },
    "/admin/api/v1/overview": { node_id: "node-a", updated_at: "2026-11-16T00:00:00Z", models: [], peers: [], request_summary_state: "not_configured" },
    "/admin/api/v1/activity": { state: "not_configured", events: [] },
    "/admin/api/v1/federation/members": { state: "available", members: [] },
    "/admin/api/v1/federation/invitations": { state: "available", invitations: [] },
    "/admin/api/v1/federation/grants": { state: "available", grants: [] },
  };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (url === "/admin/api/v1/federation/invitations" && options.method === "POST") {
      return { ok: true, status: 201, json: async () => ({ code: "secret-one-time-code", node_id: "node-b", expires_at: "2026-11-16T00:10:00Z" }) };
    }
    if (url === "/admin/api/v1/federation/join" && options.method === "POST") {
      return { ok: true, status: 200, json: async () => ({ state: "paired", member: { node_id: "node-b" } }) };
    }
    if (url === "/admin/api/v1/federation/grants" && options.method === "POST") {
      return { ok: true, status: 201, json: async () => ({ id: "0123456789abcdef0123456789abcdef" }) };
    }
    return { ok: true, status: 200, json: async () => responses[url] };
  };
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setImmediate(resolve));
  elements.set("invite-node-id", new Element("input"));
  elements.get("invite-node-id").value = "node-b";
  await elements.get("invite-form").listeners.submit({ preventDefault() {} });
  const createCall = calls.find((call) => call.url === "/admin/api/v1/federation/invitations" && call.options.method === "POST");
  assert.ok(createCall);
  assert.equal(createCall.options.headers["X-Llama-Catalog-CSRF"], "csrf-token");
  assert.equal(JSON.parse(createCall.options.body).node_id, "node-b");
  assert.equal(createCall.options.headers.Authorization, undefined);
  assert.equal(elements.get("invitation-code").textContent, "secret-one-time-code");
  assert.match(elements.get("pairing-notice").textContent, /no operation scopes/i);
  elements.set("join-url", new Element("input"));
  elements.set("join-code", new Element("input"));
  elements.get("join-url").value = "https://peer.example";
  elements.get("join-code").value = "remote-invitation-code";
  await elements.get("join-form").listeners.submit({ preventDefault() {} });
  const joinCall = calls.find((call) => call.url === "/admin/api/v1/federation/join");
  assert.ok(joinCall);
  assert.equal(joinCall.options.headers["X-Llama-Catalog-CSRF"], "csrf-token");
  assert.deepEqual(JSON.parse(joinCall.options.body), { url: "https://peer.example", invitation_code: "remote-invitation-code" });
  assert.equal(elements.get("join-code").value, "");
  assert.match(elements.get("pairing-notice").textContent, /No operation scopes/);
  for (const id of ["grant-requester", "grant-backend", "grant-models", "grant-expiry", "grant-confirm"]) {
    elements.set(id, new Element("input"));
  }
  elements.get("grant-requester").value = "node-b";
  elements.get("grant-backend").value = "backend-a";
  elements.get("grant-models").value = "repo/model-a, repo/model-b";
  elements.get("grant-expiry").value = "24h";
  elements.get("grant-confirm").checked = true;
  await elements.get("owner-grant-form").listeners.submit({ preventDefault() {} });
  const grantCall = calls.find((call) => call.url === "/admin/api/v1/federation/grants" && call.options.method === "POST");
  assert.ok(grantCall);
  assert.equal(grantCall.options.headers["X-Llama-Catalog-CSRF"], "csrf-token");
  assert.deepEqual(JSON.parse(grantCall.options.body), {
    requester_node_id: "node-b", backend_id: "backend-a", model_ids: ["repo/model-a", "repo/model-b"],
    expires_in: "24h", confirm_grant: true,
  });
});

test("configure_models user reviews typed remote settings and inspects an operation without retry", async () => {
  const calls = [];
  const owner = { node_id: "owner-b", proxy_instance_id: "b".repeat(32), config_generation: 2 };
  const grant = { id: "c".repeat(32), requester_node_id: "node-a", action: "manage_settings", backend_id: "backend-a", target_id: "target-a", model_ids: ["repo/model-a"], expires_at: "2026-11-17T00:00:00Z" };
  const preview = { target_id: "target-a", model_id: "repo/model-a", revision: "r".repeat(64), validation: "yaml_syntax_and_supported_command_only", changes: [{ setting: "context", before: "2048", after: "4096" }] };
  const proposal = { owner, requester_node_id: "node-a", backend_id: "backend-a", expires_at: "2026-11-16T12:15:00Z", review_revision: "v".repeat(64), proposal: { id: "d".repeat(32), target_id: "target-a", model_id: "repo/model-a", revision: preview.revision, settings: { context_size: 4096 }, changes: preview.changes } };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    let body = {};
    let status = 200;
    if (url === "/admin/api/v1/session") body = { subject: "operator", roles: ["view", "configure_models"], csrf_token: "csrf-models" };
    else if (url === "/admin/api/v1/overview") body = { node_id: "node-a", updated_at: "2026-11-16T00:00:00Z", models: [], peers: [], request_summary_state: "not_configured" };
    else if (url === "/admin/api/v1/activity") body = { state: "not_configured", events: [] };
    else if (url.startsWith("/admin/api/v1/management/grants?")) body = { owner, grants: [grant] };
    else if (url === "/admin/api/v1/management/settings/snapshot") body = { owner, snapshot: { target_id: "target-a", model_id: "repo/model-a", revision: preview.revision, validation: preview.validation, settings: { context_size: 2048 } } };
    else if (url === "/admin/api/v1/management/settings/preview") body = { owner, preview };
    else if (url === "/admin/api/v1/management/proposals") body = proposal;
    else if (url === "/admin/api/v1/management/proposals/apply") body = { operation_id: JSON.parse(options.body).operation_id, proposal_id: proposal.proposal.id, outcome: "written" };
    else if (url.startsWith("/admin/api/v1/management/operations?")) body = { operation_id: "2a".repeat(16), proposal_id: proposal.proposal.id, outcome: "written" };
    else throw new Error(`unexpected request: ${url}`);
    return { ok: status < 400, status, json: async () => body };
  };
  const { elements } = await createDashboard(fetchImpl);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(elements.get("remote-settings-panel").hidden, false);
  elements.set("remote-owner-id", new Element("input"));
  elements.get("remote-owner-id").value = "owner-b";
  await elements.get("remote-grant-load").listeners.submit({ preventDefault() {} });
  assert.match(textOf(elements.get("remote-grant-select")), /target-a.*repo\/model-a/);
  await elements.get("remote-load-snapshot").listeners.click();
  elements.set("setting-context", new Element("input"));
  elements.get("setting-context").value = "4096";
  await elements.get("remote-settings-form").listeners.submit({ preventDefault() {} });
  assert.match(textOf(elements.get("remote-preview-changes")), /context.*2048.*4096/);
  await elements.get("remote-create-proposal").listeners.click();
  assert.equal(elements.get("remote-settings-review").hidden, false);
  elements.get("ack-review").checked = true;
  elements.get("ack-disruption").checked = true;
  elements.get("ack-exclusive").checked = true;
  await elements.get("remote-apply").listeners.click();
  assert.match(elements.get("remote-operation-state").textContent, /not proof of runtime effectiveness/);
  assert.equal(calls.filter((call) => call.url === "/admin/api/v1/management/proposals/apply").length, 1);
  await elements.get("remote-check-operation").listeners.click();
  assert.match(elements.get("remote-operation-state").textContent, /No retry was sent/);
  const proposalCall = calls.find((call) => call.url === "/admin/api/v1/management/proposals");
  assert.equal(proposalCall.options.headers["X-Llama-Catalog-CSRF"], "csrf-models");
  const proposalRequest = JSON.parse(proposalCall.options.body);
  assert.deepEqual(proposalRequest.settings, { context_size: 4096 });
  assert.equal(proposalRequest.target_id, "target-a");
});
