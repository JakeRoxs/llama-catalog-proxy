const byId = (id) => document.getElementById(id);

async function adminApi(path, options = {}) {
  const response = await fetch(path, { credentials: "same-origin", cache: "no-store", ...options });
  if (!response.ok) {
    const error = new Error(response.status === 401 ? "Sign-in required" : response.status === 403 ? "View permission required" : `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function peerBadge(status) {
  const span = document.createElement("span");
  span.className = `peer-state ${status === "fresh" ? "fresh" : status === "stale" ? "stale" : ""}`;
  const dot = document.createElement("span");
  dot.className = "dot";
  span.append(dot, document.createTextNode(status));
  return span;
}

function renderOverview(data) {
  byId("node-id").textContent = data.node_id || "standalone";
  byId("model-count").textContent = String(data.models.length);
  byId("peer-count").textContent = String(data.peers.length);
  byId("snapshot-at").textContent = new Date(data.updated_at).toLocaleTimeString();
	const requestSummary = data.request_summary;
	const summaryAvailable = data.request_summary_state === "available" && requestSummary;
	byId("request-count").textContent = summaryAvailable ? String(requestSummary.count) : "Unknown";
	byId("request-summary-state").textContent = summaryAvailable ? "Aggregated history" : (data.request_summary_state || "Unavailable");
  const duration = summaryAvailable ? requestSummary.duration_ms : null;
  byId("request-p50").textContent = duration && typeof duration.p50 === "number" ? `${Math.round(duration.p50)} ms` : "Unknown";
  byId("duration-samples").textContent = duration && typeof duration.sample_count === "number" ? `${duration.sample_count} samples` : "Sample count unknown";
	const outcomes = byId("request-outcomes");
	outcomes.replaceChildren();
	if (!summaryAvailable) {
		const unknown = document.createElement("p");
		unknown.className = "empty";
		unknown.textContent = data.request_summary_state === "not_configured" ? "Unknown until request history is enabled." : "Request history is unavailable.";
		outcomes.append(unknown);
	} else {
		const entries = Object.entries(requestSummary.outcomes || {}).sort(([a],[b]) => a.localeCompare(b));
		if (entries.length === 0) {
			const empty = document.createElement("p");
			empty.className = "empty";
			empty.textContent = "No requests recorded in this window.";
			outcomes.append(empty);
		} else {
			for (const [status, count] of entries) {
				const row = document.createElement("div");
				row.className = "outcome";
				const label = document.createElement("span");
				label.textContent = status;
				const value = document.createElement("strong");
				value.textContent = String(count);
				row.append(label, value);
				outcomes.append(row);
			}
		}
	}

  const models = byId("models");
  models.replaceChildren();
  byId("models-empty").hidden = data.models.length !== 0;
  for (const model of data.models) {
    const row = document.createElement("tr");
    const id = document.createElement("td");
    const owner = document.createElement("td");
    id.textContent = model.id;
    owner.textContent = model.owned_by || "unknown";
    row.append(id, owner);
    models.append(row);
  }

  const peers = byId("peers");
  peers.replaceChildren();
  byId("peers-empty").hidden = data.peers.length !== 0;
  for (const peer of data.peers) {
    const row = document.createElement("div");
    row.className = "peer";
    const name = document.createElement("span");
    name.className = "peer-name";
    name.textContent = peer.node_id;
    row.append(name, peerBadge(peer.status));
    peers.append(row);
  }
}

function renderActivity(data) {
  const list = byId("activity");
  list.replaceChildren();
  const state = byId("activity-state");
  state.textContent = data.state === "not_configured" ? "Unknown until bounded request history is enabled." :
    data.state !== "available" ? "Request history is unavailable." :
    data.events.length === 0 ? "No requests recorded in this window." : `${data.events.length} recent request records.`;
  for (const event of data.events) {
    const row = document.createElement("tr");
    const time = document.createElement("td");
    const backend = document.createElement("td");
    const status = document.createElement("td");
    const duration = document.createElement("td");
    time.textContent = new Date(event.time).toLocaleTimeString();
    backend.textContent = event.backend_id;
    status.textContent = event.status || "unknown";
    duration.textContent = typeof event.duration_ms === "number" ? `${Math.round(event.duration_ms)} ms` : "unknown";
    row.append(time, backend, status, duration);
    list.append(row);
  }
}

async function loadDashboard() {
  byId("load-error").hidden = true;
  const session = await adminApi("/admin/api/v1/session");
  if (!session.roles.includes("view")) {
    byId("login-panel").hidden = true;
    byId("dashboard").hidden = true;
    byId("denied-panel").hidden = false;
    byId("csrf-logout").value = session.csrf_token;
    byId("csrf-signout").value = session.csrf_token;
    return;
  }
  byId("login-panel").hidden = true;
  byId("denied-panel").hidden = true;
  byId("dashboard").hidden = false;
  byId("signed-in-as").textContent = session.subject;
  byId("csrf-signout").value = session.csrf_token;
  const [overview, activity] = await Promise.all([
    adminApi("/admin/api/v1/overview"), adminApi("/admin/api/v1/activity"),
  ]);
  renderOverview(overview);
  renderActivity(activity);
}

async function refreshDashboard() {
  try {
    await loadDashboard();
  } catch (error) {
    byId("dashboard").hidden = true;
    byId("denied-panel").hidden = error.status !== 403;
    byId("login-panel").hidden = error.status !== 401;
    if (!error.status) {
      byId("login-panel").hidden = true;
      byId("denied-panel").hidden = true;
      byId("dashboard").hidden = false;
      byId("load-error").hidden = false;
      byId("load-error").textContent = "Dashboard data is temporarily unavailable.";
    }
  }
}

async function signOut(event) {
  event.preventDefault();
  const csrf = event.currentTarget.querySelector("input").value;
  try {
    await adminApi("/admin/auth/logout", { method: "POST", headers: { "X-Llama-Catalog-CSRF": csrf } });
  } finally {
    await refreshDashboard();
  }
}

byId("signout-form").addEventListener("submit", signOut);
byId("logout-form").addEventListener("submit", signOut);
byId("refresh").addEventListener("click", refreshDashboard);
void refreshDashboard();
