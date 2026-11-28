"use strict";

const byId = (id) => document.getElementById(id);
let token = "";
let baseline = "";
let candidate = "";
let currentJob = "";
let timer = null;

function message(text) {
  byId("message").textContent = text;
}

async function api(path, options = {}) {
	const headers = {};
	if (token) headers.Authorization = `Bearer ${token}`;
  if (options.body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(`/benchmark/v1${path}`, {
    method: options.method || "GET",
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    credentials: "same-origin",
    cache: "no-store",
    redirect: "error",
  });
  if (!response.ok) {
    const error = new Error(`Request failed (${response.status}).`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function cell(row, value) {
  const td = document.createElement("td");
  td.textContent = value == null ? "unknown" : String(value);
  row.appendChild(td);
  return td;
}

function heading(parent, value) {
  const h = document.createElement("h4");
  h.textContent = value;
  parent.appendChild(h);
}

function paragraph(parent, value) {
  const p = document.createElement("p");
  p.textContent = value;
  parent.appendChild(p);
}

function table(parent, labels) {
  const wrapper = document.createElement("div");
  wrapper.className = "table-wrap";
  const result = document.createElement("table");
  const head = document.createElement("thead");
  const row = document.createElement("tr");
  for (const label of labels) {
    const th = document.createElement("th");
    th.textContent = label;
    row.appendChild(th);
  }
  head.appendChild(row);
  const body = document.createElement("tbody");
  result.append(head, body);
  wrapper.appendChild(result);
  parent.appendChild(wrapper);
  return body;
}

function displayRun(record) {
  byId("evidence").hidden = false;
  byId("evidence-title").textContent = `Run ${record.run_id}`;
  const content = byId("evidence-content");
  const workloads = record.workloads || [];
  content.replaceChildren();
  paragraph(content, `Status: ${record.status} · ${record.requested.backend} / ${record.requested.model}`);
  paragraph(content, "Requested settings are not proof of effective runtime settings.");
  heading(content, "Workload results");
  const body = table(content, ["Workload", "Samples", "Passed", "Failed", "Not evaluated", "Incomplete"]);
  for (const workload of workloads) {
    const samples = workload.samples || [];
    const row = document.createElement("tr");
    cell(row, workload.name);
    cell(row, `${samples.length} / ${record.requested.repetitions}`);
    cell(row, samples.filter((s) => s.correctness === "passed").length);
    cell(row, samples.filter((s) => s.correctness === "failed").length);
    cell(row, samples.filter((s) => s.correctness === "not_evaluated").length);
    cell(row, samples.filter((s) => s.status !== "completed").length);
    body.appendChild(row);
  }
  if (workloads.length === 0) paragraph(content, "No workload evidence was recorded.");
  heading(content, "Sample evidence");
  paragraph(content, "Elapsed time is measured by the benchmark client around each request sequence. Missing proxy observations are unknown.");
  const details = table(content, ["Workload", "Iteration", "Status", "Correctness", "Elapsed ms", "Diagnostics", "Transport", "Generation"]);
  for (const workload of workloads) {
    for (const sample of workload.samples || []) {
      const requests = sample.requests || [];
      const row = document.createElement("tr");
      for (const value of [
        workload.name, sample.iteration, sample.status, sample.correctness,
        formatMetric(sample.duration_ms),
        requests.length ? requests.map((r) => r.diagnostic_state || "unknown").join(", ") : "not recorded",
        requests.length ? requests.map((r) => r.proxy_observation?.transport_outcome || "unknown").join(", ") : "not recorded",
        requests.length ? requests.map((r) => r.proxy_observation?.generation_outcome || "unknown").join(", ") : "not recorded",
      ]) cell(row, value);
      details.appendChild(row);
    }
  }
  heading(content, "Limitations");
  for (const item of record.limitations || []) paragraph(content, item);
}

function formatMetric(value) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "unknown";
}

function displayComparison(report) {
  byId("evidence").hidden = false;
  byId("evidence-title").textContent = "Run comparison";
  const content = byId("evidence-content");
  content.replaceChildren();
  paragraph(content, `Baseline: ${report.baseline_run_id}`);
  paragraph(content, `Candidate: ${report.candidate_run_id}`);
  paragraph(content, report.interpretation);
  heading(content, "Confounders and unsupported evidence");
  for (const item of report.confounders || []) paragraph(content, item);
  paragraph(content, report.context.per_round_acceptance_state);
  for (const workload of report.workloads || []) {
    heading(content, workload.name);
    const a = workload.coverage.baseline;
    const b = workload.coverage.candidate;
    paragraph(content, `Samples recorded / expected: baseline ${a.samples_recorded} / ${a.samples_expected}${a.recorded ? "" : " (workload missing)"}; candidate ${b.samples_recorded} / ${b.samples_expected}${b.recorded ? "" : " (workload missing)"}.`);
    const correctness = table(content, ["Side", "Passed", "Failed", "Not evaluated", "Failed samples", "Canceled", "Incomplete transport", "Unknown transport", "Incomplete generation", "Unknown generation", "Truncated"]);
    for (const side of ["baseline", "candidate"]) {
      const row = document.createElement("tr");
      const c = workload.correctness[side];
      const f = workload.failures[side];
      for (const value of [side, c.passed, c.failed, c.not_evaluated, f.failed_samples, f.canceled_samples, f.incomplete_transport, f.unknown_transport, f.incomplete_generation, f.unknown_generation, f.truncated_generation]) cell(row, value);
      correctness.appendChild(row);
    }
    if (!workload.metrics || workload.metrics.length === 0) {
      paragraph(content, "No performance measurements available.");
      continue;
    }
    const metrics = table(content, ["Metric", "Baseline mean (n)", "Candidate mean (n)", "Change", "Attribution"]);
    for (const metric of workload.metrics) {
      const row = document.createElement("tr");
      cell(row, metric.name);
      cell(row, `${formatMetric(metric.baseline.mean)} (${metric.baseline.sample_count})`);
      cell(row, `${formatMetric(metric.candidate.mean)} (${metric.candidate.sample_count})`);
      cell(row, metric.change_percent == null ? "unknown" : `${formatMetric(metric.change_percent)}%`);
      cell(row, metric.attribution);
      metrics.appendChild(row);
    }
  }
}

async function refreshRuns() {
  const result = await api("/runs");
  const rows = byId("runs");
  rows.replaceChildren();
  if (!result.runs.some((run) => run.run_id === baseline)) baseline = "";
  if (!result.runs.some((run) => run.run_id === candidate)) candidate = "";
  for (const run of result.runs) {
    const row = document.createElement("tr");
    cell(row, run.started_at);
    cell(row, `${run.backend} / ${run.model}`);
    cell(row, run.status);
      const inspect = document.createElement("button");
      inspect.type = "button";
      inspect.textContent = publicReadOnlyMode && !token ? "Unlock to inspect" : "View";
      inspect.addEventListener("click", async () => {
        if (publicReadOnlyMode && !token) {
          byId("unlock-panel").hidden = false;
          message("Enter the benchmark token to inspect full reports or run a benchmark.");
          byId("token").focus();
          return;
        }
        try { displayRun(await api(`/runs/${encodeURIComponent(run.run_id)}`)); message(""); }
        catch (error) { message(error.message); }
    });
    const action = document.createElement("td");
    action.appendChild(inspect);
    row.appendChild(action);
    for (const side of ["baseline", "candidate"]) {
      const td = document.createElement("td");
      td.className = "comparison-column";
      const radio = document.createElement("input");
      radio.type = "radio";
      radio.name = side;
      radio.value = run.run_id;
      radio.checked = (side === "baseline" ? baseline : candidate) === run.run_id;
      radio.setAttribute("aria-label", `${side} ${run.run_id}`);
      radio.addEventListener("change", () => {
        if (side === "baseline") baseline = run.run_id;
        else candidate = run.run_id;
        byId("compare").disabled = !baseline || !candidate;
      });
      td.appendChild(radio);
      row.appendChild(td);
    }
    rows.appendChild(row);
  }
  byId("compare").disabled = !baseline || !candidate;
  byId("history-note").textContent = result.truncated
    ? "Showing at most 100 reports, selected by file modification time."
    : result.runs.length ? "Select a report to inspect it or choose two runs to compare." : "No saved reports.";
}

let publicReadOnlyMode = false;

async function enterPublicReadOnlyView() {
  if (document.documentElement.dataset.publicReadOnly !== "true") return;
  publicReadOnlyMode = true;
  try {
    await refreshRuns();
    byId("unlock-panel").hidden = true;
    byId("workspace").hidden = false;
    byId("workspace").classList.add("read-only");
    byId("run-panel").hidden = true;
    byId("job-panel").hidden = true;
    byId("compare").hidden = true;
    byId("read-only-note").hidden = false;
    byId("connection").textContent = "READ ONLY";
    byId("history-note").textContent = "Saved run summaries are visible without a token. Unlock to inspect full reports or compare runs.";
  } catch (error) {
    publicReadOnlyMode = false;
    if (error.status !== 401) message(error.message);
  }
}

async function refreshJob() {
  clearTimeout(timer);
  let job;
  try {
    job = await api("/jobs/current");
  } catch (error) {
    if (error.status !== 404) throw error;
    currentJob = "";
    byId("job-state").textContent = "No active job";
    byId("cancel").hidden = true;
    byId("start").disabled = false;
    return;
  }
  currentJob = job.id;
  byId("job-state").textContent = job.state.toUpperCase();
  byId("job-detail").textContent = job.error_code
    ? `Job ${job.id}: ${job.error_code}${job.run_id ? `; inspect report ${job.run_id} before retrying` : ""}`
    : `Job ${job.id}${job.run_id ? ` · report ${job.run_id}` : ""}`;
  const active = job.state === "running" || job.state === "canceling";
  byId("cancel").hidden = !active;
  byId("cancel").disabled = job.state === "canceling";
  byId("start").disabled = active;
  if (active) {
    timer = setTimeout(() => refreshJob().catch((error) => message(error.message)), 2000);
  } else {
    await refreshRuns();
  }
}

byId("unlock-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!window.isSecureContext) return;
  token = byId("token").value;
  byId("token").value = "";
  try {
    await refreshRuns();
    await refreshJob();
    publicReadOnlyMode = false;
    byId("unlock-panel").hidden = true;
    byId("workspace").hidden = false;
    byId("workspace").classList.remove("read-only");
    byId("run-panel").hidden = false;
    byId("job-panel").hidden = false;
    byId("compare").hidden = false;
    byId("read-only-note").hidden = true;
    byId("connection").textContent = "UNLOCKED";
    message("");
  } catch (error) {
    token = "";
    message(error.message);
  }
});

byId("run-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!byId("consent").checked) return;
  byId("start").disabled = true;
  try {
    const job = await api("/jobs", { method: "POST", body: {
      backend: byId("backend").value, model: byId("model").value,
      repetitions: Number(byId("repetitions").value), max_output_tokens: Number(byId("max-output").value),
      temperature: Number(byId("temperature").value), top_p: Number(byId("top-p").value),
      time_budget_seconds: Number(byId("time-budget").value), token_budget: Number(byId("token-budget").value),
      cache_state: byId("cache-state").value, allow_interference: true,
    } });
    currentJob = job.id;
    byId("consent").checked = false;
    message("Run accepted. Readiness is checked before any inference request.");
    await refreshJob();
  } catch (error) {
    message(`${error.message} The outcome may be unknown; refresh job status before considering a retry.`);
  }
});

byId("cancel").addEventListener("click", async () => {
  if (!currentJob) return;
  try {
    await api(`/jobs/${encodeURIComponent(currentJob)}/cancel`, { method: "POST" });
    message("Cancellation requested. A partial report may be saved.");
    await refreshJob();
  } catch (error) { message(error.message); }
});

byId("refresh").addEventListener("click", () => refreshRuns().catch((error) => message(error.message)));
byId("refresh-job").addEventListener("click", () => refreshJob().then(() => message("Job status refreshed.")).catch((error) => message(error.message)));
byId("compare").addEventListener("click", async () => {
  if (!baseline || !candidate) return;
  try {
    displayComparison(await api("/compare", { method: "POST", body: { baseline, candidate } }));
    message("");
  } catch (error) { message(error.message); }
});

if (!window.isSecureContext) {
  byId("security-warning").hidden = false;
  byId("unlock-form").querySelector("button").disabled = true;
} else {
  void enterPublicReadOnlyView();
}
