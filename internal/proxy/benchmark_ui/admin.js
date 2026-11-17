const byId = (id) => document.getElementById(id);
let csrfToken = "";
let remoteOwner = null;
let remoteGrants = [];
let remoteGrant = null;
let remoteSnapshot = null;
let remotePreview = null;
let remoteSettings = null;
let remoteProposal = null;

async function adminApi(path, options = {}) {
  const response = await fetch(path, { credentials: "same-origin", cache: "no-store", ...options });
  if (!response.ok) {
    const error = new Error(response.status === 401 ? "Sign-in required" : response.status === 403 ? "Permission denied" : `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  if (response.status === 204) return null;
  return response.json();
}

function pairingNotice(message, isError = false) {
  const notice = byId("pairing-notice");
  notice.textContent = message;
  notice.classList.toggle("notice-error", isError);
  notice.hidden = !message;
}

async function pairingRequest(path, method = "GET", body) {
  const options = { method };
  if (method !== "GET") {
    options.headers = { "X-Llama-Catalog-CSRF": csrfToken };
    if (body !== undefined) {
      options.headers["Content-Type"] = "application/json";
      options.body = JSON.stringify(body);
    }
  }
  return adminApi(path, options);
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
	byId("request-summary-state").textContent = summaryAvailable ? "Aggregated history" :
	  data.request_summary_state === "not_configured" ? "Diagnostics API not configured" : "Request history unavailable";
  const duration = summaryAvailable ? requestSummary.duration_ms : null;
  byId("request-p50").textContent = duration && typeof duration.p50 === "number" ? `${Math.round(duration.p50)} ms` : "Unknown";
  byId("duration-samples").textContent = duration && typeof duration.sample_count === "number" ? `${duration.sample_count} samples` : "Sample count unknown";
	const outcomes = byId("request-outcomes");
	outcomes.replaceChildren();
	if (!summaryAvailable) {
		const unknown = document.createElement("p");
		unknown.className = "empty";
		unknown.textContent = data.request_summary_state === "not_configured" ? "Unknown until diagnostics API and bounded request history are enabled." : "Request history is unavailable.";
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
    const evidence = document.createElement("span");
    evidence.className = "peer-meta";
    const scopes = (peer.configured_scopes || []).join(", ") || "none";
    evidence.textContent = `Identity ${peer.identity_state || "unknown"}; scopes ${peer.authorization_state || "unknown"} (${scopes}); model readiness ${peer.model_readiness_state || "unknown"}`;
    row.append(name, peerBadge(peer.status), evidence);
    peers.append(row);
  }
  renderFederationPolicy(data.federation_policy);
}

function renderFederationPolicy(policy) {
  const state = byId("federation-policy-state");
  const rows = byId("federation-policy");
  rows.replaceChildren();
  if (!policy || policy.state === "not_configured") {
    state.textContent = "No federation CRL policy is configured.";
    return;
  }
  state.textContent = policy.state === "fresh" ? "Configured CRLs are within their freshness limits; mTLS requests fail closed when they expire." :
    policy.state === "stale" ? "A CRL is stale; affected mTLS requests are being rejected." : "Federation CRL policy is unavailable.";
  const addRow = (direction, entry) => {
    const row = document.createElement("tr");
    const directionCell = document.createElement("td");
    const statusCell = document.createElement("td");
    const updatedCell = document.createElement("td");
    const expiresCell = document.createElement("td");
    const ageCell = document.createElement("td");
    directionCell.textContent = direction;
    statusCell.textContent = entry.state;
    updatedCell.textContent = entry.updated_at ? new Date(entry.updated_at).toLocaleString() : "unknown";
    expiresCell.textContent = entry.expires_at ? new Date(entry.expires_at).toLocaleString() : "unknown";
    ageCell.textContent = entry.max_age_seconds < 3600 ? `${Math.round(entry.max_age_seconds / 60)} min` : `${Math.round(entry.max_age_seconds / 3600)} h`;
    row.append(directionCell, statusCell, updatedCell, expiresCell, ageCell);
    rows.append(row);
  };
  if (policy.client_crl) addRow("Inbound clients", policy.client_crl);
  for (const peer of policy.peer_crls || []) addRow(`Outbound: ${peer.node_id}`, peer);
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
  csrfToken = session.csrf_token;
  byId("csrf-signout").value = session.csrf_token;
  const [overview, activity] = await Promise.all([
    adminApi("/admin/api/v1/overview"), adminApi("/admin/api/v1/activity"),
  ]);
  renderOverview(overview);
  renderActivity(activity);
  await loadPairing(session.roles);
  loadRemoteSettings(session.roles);
}

async function loadPairing(roles) {
  const panel = byId("pairing-panel");
  const allowed = roles.includes("administer_federation");
  panel.hidden = !allowed;
  if (!allowed) return;
  try {
    const [members, invitations, grants, audit] = await Promise.all([
      pairingRequest("/admin/api/v1/federation/members"),
      pairingRequest("/admin/api/v1/federation/invitations"),
      pairingRequest("/admin/api/v1/federation/grants").catch(() => null),
      pairingRequest("/admin/api/v1/audit/events?limit=50").catch(() => null),
    ]);
    renderPairing(members, invitations, grants, audit);
  } catch {
    pairingNotice("Pairing state is unavailable on this node.", true);
  }
}

function loadRemoteSettings(roles) {
  const panel = byId("remote-settings-panel");
  panel.hidden = !roles.includes("configure_models");
}

function remoteSettingsNotice(message, isError = false) {
  const notice = byId("remote-settings-notice");
  notice.textContent = message;
  notice.classList.toggle("notice-error", isError);
  notice.hidden = !message;
}

async function loadRemoteGrants(event) {
  event.preventDefault();
  const ownerID = byId("remote-owner-id").value.trim();
  if (!ownerID) {
    remoteSettingsNotice("Enter the owner node ID.", true);
    return;
  }
  resetRemoteSettings();
  try {
    const result = await pairingRequest(`/admin/api/v1/management/grants?owner_node_id=${encodeURIComponent(ownerID)}`);
    remoteOwner = result.owner;
    remoteGrants = result.grants || [];
    remoteGrant = null;
    remoteSnapshot = null;
    remotePreview = null;
    remoteProposal = null;
    const select = byId("remote-grant-select");
    select.replaceChildren();
    for (const grant of remoteGrants) {
      const option = document.createElement("option");
      option.value = grant.id;
      option.textContent = `${grant.backend_id} / ${grant.target_id}: ${grant.model_ids.join(", ")} (expires ${new Date(grant.expires_at).toLocaleString()})`;
      select.append(option);
    }
    byId("remote-grant-selection").hidden = remoteGrants.length === 0;
    byId("remote-settings-form").hidden = true;
    byId("remote-settings-current").hidden = true;
    byId("remote-settings-preview").hidden = true;
    byId("remote-settings-review").hidden = true;
    byId("remote-operation").hidden = true;
    if (remoteGrants.length === 0) {
      remoteSettingsNotice("The owner has no active target/model grant for this node.", true);
      return;
    }
    populateRemoteModels();
    remoteSettingsNotice(`${remoteGrants.length} active owner grant(s) loaded. No settings have been changed.`);
  } catch {
    remoteSettingsNotice("Could not load owner grants. Check the peer, route permission, and current CRL policy.", true);
  }
}

function resetRemoteSettings() {
  remoteOwner = null;
  remoteGrants = [];
  remoteGrant = null;
  remoteSnapshot = null;
  remotePreview = null;
  remoteSettings = null;
  remoteProposal = null;
  byId("remote-grant-selection").hidden = true;
  byId("remote-settings-current").hidden = true;
  byId("remote-settings-form").hidden = true;
  byId("remote-settings-preview").hidden = true;
  byId("remote-settings-review").hidden = true;
  byId("remote-operation").hidden = true;
  byId("remote-create-proposal").disabled = false;
  byId("remote-recover-proposal").hidden = true;
  byId("remote-apply").disabled = false;
  for (const id of ["setting-context", "setting-fit-context", "setting-fit-target", "setting-cache-ram"]) byId(id).value = "";
  byId("setting-fit").value = "";
  for (const id of ["ack-review", "ack-disruption", "ack-exclusive"]) byId(id).checked = false;
}

function populateRemoteModels() {
  const select = byId("remote-grant-select");
  remoteGrant = remoteGrants.find((grant) => grant.id === select.value) || remoteGrants[0] || null;
  if (!remoteGrant) return;
  select.value = remoteGrant.id;
  remoteSnapshot = null;
  remotePreview = null;
  remoteProposal = null;
  byId("remote-settings-current").hidden = true;
  byId("remote-settings-form").hidden = true;
  byId("remote-settings-preview").hidden = true;
  byId("remote-settings-review").hidden = true;
  byId("remote-operation").hidden = true;
  byId("remote-create-proposal").disabled = false;
  byId("remote-recover-proposal").hidden = true;
  byId("remote-apply").disabled = false;
  for (const id of ["setting-context", "setting-fit-context", "setting-fit-target", "setting-cache-ram"]) byId(id).value = "";
  byId("setting-fit").value = "";
  for (const id of ["ack-review", "ack-disruption", "ack-exclusive"]) byId(id).checked = false;
  const models = byId("remote-model-select");
  models.replaceChildren();
  for (const modelID of remoteGrant.model_ids) {
    const option = document.createElement("option");
    option.value = modelID;
    option.textContent = modelID;
    models.append(option);
  }
  models.value = remoteGrant.model_ids[0] || "";
}

async function loadRemoteSnapshot() {
  if (!remoteOwner || !remoteGrant) return;
  try {
    const modelID = byId("remote-model-select").value;
    const result = await pairingRequest("/admin/api/v1/management/settings/snapshot", "POST", {
      owner_node_id: remoteOwner.node_id, backend_id: remoteGrant.backend_id,
      target_id: remoteGrant.target_id, model_id: modelID,
    });
    remoteOwner = result.owner;
    remoteSnapshot = result.snapshot;
    const settings = remoteSnapshot.settings || {};
    byId("remote-settings-current").textContent = `Configured values on ${remoteSnapshot.target_id} / ${remoteSnapshot.model_id}: ` +
      `context=${settings.context_size ?? "unknown"}, fit=${settings.fit ?? "unknown"}, ` +
      `fit_context=${settings.fit_context ?? "unknown"}, fit_target=${settings.fit_target ?? "unknown"}, ` +
      `cache_ram=${settings.cache_ram ?? "unknown"}. These are configured values, not runtime verification.`;
    byId("remote-settings-current").hidden = false;
    byId("remote-settings-form").hidden = false;
    byId("remote-settings-preview").hidden = true;
    byId("remote-settings-review").hidden = true;
    byId("remote-operation").hidden = true;
    byId("remote-apply").disabled = false;
    byId("ack-review").checked = false;
    byId("ack-disruption").checked = false;
    byId("ack-exclusive").checked = false;
    byId("remote-create-proposal").disabled = false;
    byId("remote-recover-proposal").hidden = true;
    remoteProposal = null;
    remoteSettingsNotice("Typed snapshot loaded. Blank fields preserve their current values.");
  } catch {
    remoteSettingsNotice("Could not read the typed target snapshot.", true);
  }
}

function settingsFromRemoteForm() {
  const settings = {};
  for (const [inputID, property] of [["setting-context", "context_size"], ["setting-fit-context", "fit_context"], ["setting-fit-target", "fit_target"], ["setting-cache-ram", "cache_ram"]]) {
    const text = byId(inputID).value.trim();
    if (!text) continue;
    const value = Number(text);
    if (!Number.isSafeInteger(value) || value < 0 || value > 2147483647) throw new Error("Settings must be non-negative 32-bit integers.");
    settings[property] = value;
  }
  const fit = byId("setting-fit").value;
  if (fit === "on") settings.fit = true;
  if (fit === "off") settings.fit = false;
  if (Object.keys(settings).length === 0) throw new Error("Set at least one supported setting.");
  return settings;
}

function renderRemoteChanges(preview) {
  const rows = byId("remote-preview-changes");
  rows.replaceChildren();
  for (const change of preview.changes || []) {
    const row = document.createElement("tr");
    for (const value of [change.setting, change.before, change.after]) {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.append(cell);
    }
    rows.append(row);
  }
  byId("remote-preview-revision").textContent = `Target ${preview.target_id} / ${preview.model_id}; revision ${preview.revision}; ${preview.validation}.`;
}

async function previewRemoteSettings(event) {
  event.preventDefault();
  if (!remoteOwner || !remoteGrant || !remoteSnapshot) {
    remoteSettingsNotice("Load an active grant and settings snapshot first.", true);
    return;
  }
  try {
    remoteSettings = settingsFromRemoteForm();
    const result = await pairingRequest("/admin/api/v1/management/settings/preview", "POST", {
      owner_node_id: remoteOwner.node_id, backend_id: remoteGrant.backend_id, target_id: remoteGrant.target_id,
      model_id: remoteSnapshot.model_id, settings: remoteSettings,
    });
    remoteOwner = result.owner;
    remotePreview = result.preview;
    renderRemoteChanges(remotePreview);
    byId("remote-settings-preview").hidden = false;
    byId("remote-settings-review").hidden = true;
    byId("remote-create-proposal").disabled = false;
    byId("remote-recover-proposal").hidden = true;
    remoteProposal = null;
    remoteSettingsNotice("Owner validated this exact typed diff. Review it before creating a proposal.");
  } catch (error) {
    remoteSettingsNotice(error.message || "Typed settings preview failed.", true);
  }
}

function invalidateRemotePreview() {
  if (!remotePreview && !remoteProposal) return;
  remotePreview = null;
  remoteProposal = null;
  byId("remote-settings-preview").hidden = true;
  byId("remote-settings-review").hidden = true;
  byId("remote-create-proposal").disabled = false;
  byId("remote-recover-proposal").hidden = true;
  remoteSettingsNotice("Settings changed. Preview the exact update again before creating a proposal.");
}

function delegatedProposalRequest() {
  return {
    owner: remoteOwner, backend_id: remoteGrant.backend_id, target_id: remoteGrant.target_id, model_id: remoteSnapshot.model_id,
    revision: remotePreview.revision, settings: remoteSettings,
  };
}

async function receiveRemoteProposal(recover) {
  if (!remotePreview || !remoteSettings || !remoteGrant || !remoteOwner) return;
  const path = recover ? "/admin/api/v1/management/proposals/recover" : "/admin/api/v1/management/proposals";
  if (!recover) byId("remote-create-proposal").disabled = true;
  try {
    remoteProposal = await pairingRequest(path, "POST", delegatedProposalRequest());
    byId("remote-review-expiry").textContent = `Proposal ${remoteProposal.proposal.id} expires ${new Date(remoteProposal.expires_at).toLocaleString()}.`;
    byId("remote-settings-review").hidden = false;
    byId("remote-recover-proposal").hidden = true;
    byId("remote-apply").disabled = false;
    byId("ack-review").checked = false;
    byId("ack-disruption").checked = false;
    byId("ack-exclusive").checked = false;
    remoteSettingsNotice("Proposal received. Review the exact diff and acknowledgments before applying.");
  } catch {
    byId("remote-recover-proposal").hidden = false;
    remoteSettingsNotice("Proposal outcome may be unknown. Recover the exact request before considering another proposal.", true);
  }
}

async function applyRemoteProposal() {
  if (!remoteProposal || !remoteOwner) return;
  if (!byId("ack-review").checked || !byId("ack-disruption").checked || !byId("ack-exclusive").checked) {
    remoteSettingsNotice("All apply acknowledgments are required.", true);
    return;
  }
  const operationID = randomOperationID();
  byId("remote-operation-id").textContent = operationID;
  byId("remote-operation-state").textContent = "Apply submitted; inspect this operation before any retry.";
  byId("remote-operation").hidden = false;
  byId("remote-apply").disabled = true;
  byId("remote-load-snapshot").disabled = true;
  for (const element of byId("remote-settings-form").querySelectorAll("input,select,button")) element.disabled = true;
  try {
    const record = await pairingRequest("/admin/api/v1/management/proposals/apply", "POST", {
      review: remoteProposal, operation_id: operationID, acknowledge_review: true,
      acknowledge_disruption: true, acknowledge_exclusive_window: true,
    });
    byId("remote-operation-state").textContent = `Mutation outcome: ${record.outcome}. This is not proof of runtime effectiveness.`;
    if (record.outcome !== "unknown") enableRemoteSettingsAfterTerminal();
  } catch {
    byId("remote-operation-state").textContent = "Outcome unknown. Check this exact operation before taking further action.";
  }
}

async function checkRemoteOperation() {
  if (!remoteOwner || !remoteProposal || !byId("remote-operation-id").textContent) return;
  const query = new URLSearchParams({
    owner_node_id: remoteOwner.node_id, proposal_id: remoteProposal.proposal.id,
    operation_id: byId("remote-operation-id").textContent,
  });
  try {
    const result = await pairingRequest(`/admin/api/v1/management/operations?${query}`);
    byId("remote-operation-state").textContent = `Mutation outcome: ${result.outcome}. No retry was sent.`;
    if (result.outcome !== "unknown") enableRemoteSettingsAfterTerminal();
  } catch {
    byId("remote-operation-state").textContent = "Operation lookup unavailable or unresolved. Do not retry automatically.";
  }
}

function randomOperationID() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

function enableRemoteSettingsAfterTerminal() {
  byId("remote-load-snapshot").disabled = false;
  for (const element of byId("remote-settings-form").querySelectorAll("input,select,button")) element.disabled = false;
}

function renderPairing(memberData, invitationData, grantData, auditData) {
  const members = byId("paired-members");
  members.replaceChildren();
  const list = memberData.members || [];
  byId("members-empty").hidden = list.length !== 0;
  for (const member of list) {
    const row = document.createElement("tr");
    const node = document.createElement("td");
    const fingerprint = document.createElement("td");
    const paired = document.createElement("td");
    const action = document.createElement("td");
    const revoke = document.createElement("button");
    node.textContent = member.node_id;
    fingerprint.textContent = member.certificate_sha256;
    paired.textContent = new Date(member.paired_at).toLocaleString();
    revoke.type = "button";
    revoke.className = "button quiet";
    revoke.textContent = "Revoke identity";
    revoke.addEventListener("click", () => {
      if (window.confirm(`Revoke ${member.node_id}? This blocks it immediately and requires a new identity certificate to pair again.`)) {
        void removePairing(`/admin/api/v1/federation/members/${encodeURIComponent(member.node_id)}`, "Identity revoked. A new certificate is required to pair this node again.");
      }
    });
    action.append(revoke);
    row.append(node, fingerprint, paired, action);
    members.append(row);
  }

  const invitations = byId("pending-invitations");
  invitations.replaceChildren();
  const pending = invitationData.invitations || [];
  byId("invitations-empty").hidden = pending.length !== 0;
  for (const invite of pending) {
    const row = document.createElement("tr");
    const node = document.createElement("td");
    const expires = document.createElement("td");
    const action = document.createElement("td");
    const cancel = document.createElement("button");
    node.textContent = invite.node_id;
    expires.textContent = new Date(invite.expires_at).toLocaleString();
    cancel.type = "button";
    cancel.className = "button quiet";
    cancel.textContent = "Cancel";
    cancel.addEventListener("click", () => removePairing(`/admin/api/v1/federation/invitations/${encodeURIComponent(invite.node_id)}`, "Invitation canceled."));
    action.append(cancel);
    row.append(node, expires, action);
    invitations.append(row);
  }

  const grantsTable = byId("owner-grants");
  grantsTable.replaceChildren();
  const grants = grantData?.grants || [];
  const grantsReady = grantData?.state === "available";
  byId("owner-grant-form").hidden = !grantsReady;
  byId("owner-grants-unavailable").hidden = grantsReady;
  byId("owner-grants-empty").hidden = !grantsReady || grants.length !== 0;
  for (const grant of grants) {
    const row = document.createElement("tr");
    const requester = document.createElement("td");
    const scope = document.createElement("td");
    const expires = document.createElement("td");
    const action = document.createElement("td");
    const revoke = document.createElement("button");
    requester.textContent = grant.requester_node_id;
    scope.textContent = `${grant.backend_id} / ${grant.target_id}: ${(grant.model_ids || []).join(", ")}`;
    expires.textContent = new Date(grant.expires_at).toLocaleString();
    revoke.type = "button";
    revoke.className = "button quiet";
    revoke.textContent = "Revoke grant";
    revoke.addEventListener("click", () => {
      if (window.confirm(`Revoke the settings grant for ${grant.requester_node_id} on ${grant.target_id}?`)) {
        void removePairing(`/admin/api/v1/federation/grants/${encodeURIComponent(grant.id)}`, "Settings grant revoked.");
      }
    });
    action.append(revoke);
    row.append(requester, scope, expires, action);
    grantsTable.append(row);
  }

  const auditTable = byId("audit-events");
  auditTable.replaceChildren();
  if (!auditData || auditData.state !== "available") {
    byId("audit-state").textContent = "Audit trail unavailable.";
    return;
  }
  const auditEvents = auditData.events || [];
  byId("audit-state").textContent = `${auditEvents.length} recent records; ${auditData.retention_days} day retention. Remote human identity is unknown unless verified locally.`;
  for (const event of auditEvents) {
    const row = document.createElement("tr");
    const time = document.createElement("td");
    const actor = document.createElement("td");
    const action = document.createElement("td");
    const target = document.createElement("td");
    const outcome = document.createElement("td");
    time.textContent = new Date(event.at).toLocaleString();
    actor.textContent = event.user_identity_state === "verified_local" ? `${event.verified_user_subject} (${event.role})` :
      event.requester_node_id ? `${event.requester_node_id} (human unknown)` :
        event.local_account ? `local CLI ${event.local_account}` : `${event.principal_type} (human unknown)`;
    action.textContent = event.action;
    target.textContent = [event.target_id, event.model_id].filter(Boolean).join(" / ") || "-";
    outcome.textContent = `${event.phase}: ${event.outcome}`;
    row.append(time, actor, action, target, outcome);
    auditTable.append(row);
  }
}

async function removePairing(path, message) {
  try {
    await pairingRequest(path, "DELETE");
    pairingNotice(message);
    await refreshDashboard();
  } catch {
    pairingNotice("Pairing outcome may be unknown. Refresh the member or invitation list before retrying.", true);
  }
}

async function createInvitation(event) {
  event.preventDefault();
  const nodeId = byId("invite-node-id").value.trim();
  try {
    const invite = await pairingRequest("/admin/api/v1/federation/invitations", "POST", { node_id: nodeId });
    byId("invite-node-id").value = "";
    byId("invitation-code").textContent = invite.code;
    byId("invitation-expiry").textContent = `For ${invite.node_id}; expires ${new Date(invite.expires_at).toLocaleString()}. Copy it now; it will not be shown again.`;
    byId("created-invitation").hidden = false;
    pairingNotice("Invitation created. It grants identity membership only, no operation scopes.");
    await refreshDashboard();
  } catch {
    pairingNotice("Invitation outcome may be unknown. Check pending invitations before retrying.", true);
  }
}

async function joinWithInvitation(event) {
  event.preventDefault();
  const url = byId("join-url").value.trim();
  const codeInput = byId("join-code");
  const code = codeInput.value.trim();
  try {
    const result = await pairingRequest("/admin/api/v1/federation/join", "POST", { url, invitation_code: code });
    codeInput.value = "";
    pairingNotice(`Paired with ${result.member.node_id}. No operation scopes were granted.`);
    await refreshDashboard();
  } catch {
    pairingNotice("Pairing outcome may be unknown. Refresh the member list before retrying; check the peer origin, invitation, and CA-issued identity certificate.", true);
  }
}

async function createOwnerGrant(event) {
  event.preventDefault();
  const confirmed = byId("grant-confirm").checked;
  if (!confirmed) {
    pairingNotice("Explicit grant confirmation is required.", true);
    return;
  }
  const input = {
    requester_node_id: byId("grant-requester").value.trim(),
    backend_id: byId("grant-backend").value.trim(),
    model_ids: byId("grant-models").value.split(",").map((value) => value.trim()).filter(Boolean),
    expires_in: byId("grant-expiry").value.trim(),
    confirm_grant: true,
  };
  try {
    await pairingRequest("/admin/api/v1/federation/grants", "POST", input);
    byId("grant-confirm").checked = false;
    pairingNotice("Target-scoped settings grant approved. Pairing did not grant this scope.");
    await refreshDashboard();
  } catch {
    pairingNotice("Owner grant outcome may be unknown. Refresh the grant list before retrying.", true);
  }
}

async function copyInvitation() {
  try {
    await navigator.clipboard.writeText(byId("invitation-code").textContent);
    pairingNotice("Invitation copied. Treat it as a one-time secret.");
  } catch {
    pairingNotice("Copy failed. Select the displayed code manually.", true);
  }
}

async function refreshDashboard() {
  try {
    await loadDashboard();
  } catch (error) {
    byId("dashboard").hidden = true;
    byId("created-invitation").hidden = true;
    byId("invitation-code").textContent = "";
    resetRemoteSettings();
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
byId("invite-form").addEventListener("submit", createInvitation);
byId("join-form").addEventListener("submit", joinWithInvitation);
byId("copy-invitation").addEventListener("click", copyInvitation);
byId("owner-grant-form").addEventListener("submit", createOwnerGrant);
byId("remote-grant-load").addEventListener("submit", loadRemoteGrants);
byId("remote-grant-select").addEventListener("change", populateRemoteModels);
byId("remote-load-snapshot").addEventListener("click", loadRemoteSnapshot);
byId("remote-settings-form").addEventListener("submit", previewRemoteSettings);
byId("remote-settings-form").addEventListener("input", invalidateRemotePreview);
byId("remote-settings-form").addEventListener("change", invalidateRemotePreview);
byId("remote-create-proposal").addEventListener("click", () => receiveRemoteProposal(false));
byId("remote-recover-proposal").addEventListener("click", () => receiveRemoteProposal(true));
byId("remote-apply").addEventListener("click", applyRemoteProposal);
byId("remote-check-operation").addEventListener("click", checkRemoteOperation);
void refreshDashboard();
