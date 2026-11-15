# llama-catalog-proxy

`llama-catalog-proxy` is a small model-aware router for independent OpenAI-compatible inference
backends. It aggregates each backend's model catalog, gives each backend an optional configurable
public prefix, and routes inference requests by that prefix.

Each backend manages its own models and inference lifecycle. The proxy does not implement
scheduling, model lifecycle management, load balancing, or inference logic. It is tested with
[llama-swap](https://github.com/mostlygeek/llama-swap), but llama-swap is not required.

## Architecture

```text
                          +-> default OpenAI-compatible backend
Client -> router :9292 --|
                          +-> GPU OpenAI-compatible backend (prefix: gpu/)
```

`GET /v1/models` is handled locally by aggregating cached backend catalogs. All other requests are
inspected for a top-level JSON `model` string:

- `local-model` routes to the default backend unchanged.
- `gpu/other-model` routes to the GPU backend as `other-model`.
- Overlapping prefixes use the longest match.
- Unknown and non-model requests route to the default backend.

Routing depends only on configuration, not on whether a model is present in the catalog cache.

## Backend Compatibility

A backend is compatible with catalog aggregation when `GET /v1/models` returns an OpenAI-style
object containing a `data` array whose model objects have string `id` fields. Unknown catalog fields
are preserved. Routed APIs may use any path as long as JSON requests identify the model with a
top-level `model` string; non-model and non-JSON requests pass through to the default backend.

Response status, headers, bodies, streaming, and WebSocket upgrades are proxied without
backend-specific translation. Backend-specific differences in supported OpenAI request fields remain
the backend's responsibility.

Readiness checks each backend's configured `health_path` via `GET`, defaulting to `/health`.
Backends whose health path is unavailable can still serve routed traffic, but a default backend
with an unreachable health path makes `/ready` return 503.

## Ollama

[Ollama](https://ollama.com) exposes OpenAI-compatible endpoints and works as a backend without
translation. Point its health path at the liveness endpoint:

```yaml
default_backend: ollama
backends:
  ollama:
    url: http://ollama-host:11434
    health_path: /api/version
    prefix: ""
```

With a prefix such as `ollama/`, models appear as `ollama/qwen3:8b` in the aggregated catalog.
Both OpenAI-compatible requests and native `POST /api/chat`, `/api/generate`, and `/api/embed`
requests route by their top-level `model` and have the prefix stripped before reaching Ollama.
The client may send any API key, including the placeholder `ollama`; credentials are forwarded as
given and local Ollama ignores them.

### Ollama catalog enrichment

Set `ollama_show_enrichment: true` to fetch each model's native details via `POST /api/show`
during catalog refresh and merge them under an `ollama` key on the model object:

```yaml
backends:
  ollama:
    url: http://ollama-host:11434
    health_path: /api/version
    prefix: ollama/
    ollama_show_enrichment: true
```

```json
{
  "id": "ollama/qwen3:8b",
  "object": "model",
  "owned_by": "library",
  "ollama": {
    "context_length": 40960,
    "max_completion_tokens": 8192,
    "parameter_size": "8.3B",
    "quantization_level": "Q4_K_M",
    "family": "qwen3",
    "format": "safetensors",
    "capabilities": ["completion", "toolcalling"]
  }
}
```

Fields are included only when Ollama reports them. `context_length` is taken from
`details.context_length`, falling back to `model_info[<architecture>.context_length]`. Requests
run with bounded concurrency (8 parallel), so a 10-model Ollama with 50ms per `/api/show` adds
about 100ms to a catalog refresh instead of 500ms sequential. A failing `/api/show` for one model
omits its metadata without failing the catalog. Enrichment is off by default.

Ollama implements a subset of OpenAI request fields. Unsupported fields are passed through
unchanged and rejected or ignored by Ollama, including `tool_choice`, `logit_bias`, `user`, `n`,
`best_of`, `echo`, logprobs, remote image URLs (base64 images only), and stateful
`/v1/responses` fields.

## Configuration

The default configuration path is `/config/config.yaml`. Override it with `--config`.

```yaml
listen: 0.0.0.0:8080
log_level: INFO

default_backend: default
backends:
  default:
    url: http://openai-backend-default:8080
    prefix: ""
    # api_key: replace-with-default-api-key
  gpu:
    url: http://openai-backend-gpu:8080
    prefix: gpu/
    # api_key: replace-with-gpu-api-key
    # health_path: /api/version
    # headers:
    #   X-Custom-Backend-Header: value

path_routes:
  /comfyui: gpu

# Wait for active ComfyUI queue jobs before forwarding inference requests to
# the same backend endpoint. Requires path_routes /comfyui. Timeout returns HTTP 504.
# comfyui_wait_timeout: 45m

models_cache_ttl: 15s
request_timeout: 10s
# health_timeout: 2s   # inherits request_timeout when omitted
# catalog_timeout: 20s # inherits request_timeout when omitted
max_json_request_body_bytes: 67108864
```

The named default backend must exist and may use an empty or configured public prefix. Non-empty
routing prefixes must be unique. Model IDs without a matching prefix still route to the default
backend. Backend URLs must be absolute HTTP or HTTPS URLs. Optional `api_key` values inject
`Authorization` and `X-Api-Key` only when the client did not provide credentials. Optional backend
headers are also applied only when the request does not already contain that header. Optional
`health_path` values must be canonical absolute paths without a query or fragment; they default to
`/health` and control the readiness probe target only. Optional `ollama_show_enrichment` enables
Ollama `POST /api/show` catalog enrichment (see the Ollama section). Header values, API keys, and
request bodies are not logged.

`request_observation: true` independently opts in to bounded metadata observation for POST
`/v1/chat/completions` and `/v1/completions`; it is **off by default** and does not enable history.
It adds a `request_observation` summary log keyed by generated request ID and configured backend,
not raw model labels, request/response text, headers, or error strings. No usage options or client
generation settings are added. Configuration reload can toggle observation.

Summaries distinguish real upstream-header latency, the first successful body write, first
recognized streamed output (including reasoning/tools), first answer text, and total handler time.
These are proxy observations, not engine token timestamps or proof of delivery over the client
socket. Non-streamed JSON does not establish streaming output times. Reported usage/cache/reasoning
counts retain missing-versus-zero values; supported llama-server timing fields are separate from
proxy wall times. No token counts or throughput are invented.

Inspection supports uncompressed OpenAI-style chat/completion SSE and JSON. Upgrades, compressed
responses, status errors, and unknown content types remain uninspected. Framing/JSON buffers are
limited to 64 KiB; malformed/oversized metadata is ignored without changing forwarded bytes and
marks inspection partial. SSE completion requires `[DONE]` and clean transport; a choice-level
finish reason alone is not whole-stream completion. Length limits, incomplete streams, cancellation,
transport failure, and aborts remain distinct. Summaries release transient body buffers at finish
and never close the underlying response writer. Observation has CPU/allocation cost even though
forwarding stays streaming; keep it disabled when that metadata is not needed.

`request_history: true` opts in to bounded, in-memory proxy request history; it is disabled
by default. Retention defaults are 1,000 records, 24 hours, and 10 MiB of estimated retained
record size, with the oldest records evicted first. Records contain proxy-generated request IDs,
configured backend IDs, HTTP/transport outcomes, and wall-clock timing, not request bodies,
response text, URL paths or queries, credentials, or arbitrary client model labels. A successful
HTTP response does not establish that an inference generation completed. History is local to the
current configuration handler:
configuration reloads and proxy restarts start a new, empty history. This setting does not
enable the protected API by itself or start background polling of backend activity logs. The
protected query surface also requires the dedicated `diagnostics_api_key` below.
The inspected llama-swap activity API does not retain `X-Request-ID`, so forwarding that header
alone cannot establish an exact upstream activity match.

When **both** `request_history` and `request_observation` are enabled, eligible request records
also retain a sanitized `proxy_observation` projection. Observation alone produces its summary
log without a history store; history alone retains the existing HTTP/transport record without
response inspection. The projection separates proxy wall phases from upstream-reported usage
and engine timing, preserves unknown versus zero, and cannot create an upstream activity or model
identity. It follows the same retention limits, reload lifetime, and frozen-page copy rules as
the enclosing proxy record, with a separate fixed 4 KiB per-projection budget. Invalid or unfinished
summaries are rejected; uninspected bodies cannot supply usage or output-phase evidence. A timing
attachment is not an upstream request-ID or lifecycle join.

The internal `QueryPage` API freezes filtered history for mutation-stable pagination. Cursors are
authenticated and bound to their store, filter, and page size. Snapshot storage has a separate
budget of 100 snapshots and 10 MiB estimated size, at most 1,000 records per snapshot, and a
five-minute lifetime. Expired or capacity-evicted cursors return an error rather than silently
switching to live history. The legacy offset-based `Query` API does not offer this guarantee.

### Protected diagnostics query API

The HTTP query API is disabled when `diagnostics_api_key` is empty. Configure a dedicated random
bearer secret in the protected runtime config file; it is separate from all backend inference keys.
Do not place credentials in query strings. Both the legacy `/diagnostics` route and `/diagnostics/v1/`
namespace require `Authorization: Bearer <key>`; unauthenticated/disabled access returns 401/404.
Responses set `Cache-Control: no-store`.

Read-only GET routes include:

- `/diagnostics` or `/diagnostics/v1/backends` — backend capability snapshots.
- `/diagnostics/v1/models?backend=&model=&status=&limit=&cursor=` — typed per-model snapshots with
  authenticated stable cursors bound to filters/page size and short-lived frozen pages.
- `/diagnostics/v1/requests?since=&until=&backend=&status=&requestid=&limit=&cursor=` and
  `/diagnostics/v1/requests/{request_id}` — proxy-local request history when `request_history: true`.
  Request model filtering returns explicit `model_attribution_unsupported`; no join is inferred from
  overlapping model names or timestamps.
- `/diagnostics/v1/summary?since=&until=&backend=&status=` — bounded transport/generation outcome
  counts and duration statistics with explicit sample denominators.

Time queries are limited to 24 hours. Pagination is bounded; expired/evicted cursors fail explicitly
rather than falling back to live data. Diagnostics collection runs only in response to an authorized
query, with bounded worker concurrency, request rate, per-source timeouts, rows and body sizes. No
inference, model-load, swap, restart, reconfigure, logs, or arbitrary upstream paths are exposed.
Unknown, unavailable, stale, ambiguous and unsupported data is not silently reported as a successful
empty result.

`max_json_request_body_bytes` limits uncompressed JSON bodies that must be buffered for model-aware
routing. Oversized JSON requests return `413 Request Entity Too Large`. Explicitly non-JSON,
compressed, and path-routed request bodies remain streaming passthroughs.

Environment overrides:

| Variable                                | Meaning                              |
| --------------------------------------- | ------------------------------------ |
| `LLAMA_CATALOG_LISTEN`                  | HTTP listen address                  |
| `LLAMA_CATALOG_DEFAULT_BACKEND_URL`     | URL of the named default backend     |
| `LLAMA_CATALOG_DEFAULT_BACKEND_API_KEY` | API key of the named default backend |
| `LLAMA_CATALOG_MODELS_CACHE_TTL`        | Per-backend catalog cache duration   |
| `LLAMA_CATALOG_REQUEST_TIMEOUT`         | Fallback for health/catalog budgets; also sets HTTP `ReadHeaderTimeout` |
| `LLAMA_CATALOG_HEALTH_TIMEOUT`          | Readiness health-check budget        |
| `LLAMA_CATALOG_CATALOG_TIMEOUT`         | Backend catalog refresh budget       |
| `LLAMA_CATALOG_LOG_LEVEL`               | `debug`, `info`, `warn`, or `error`  |

Backend definitions remain in YAML so model namespaces and routing destinations stay explicit.

`request_timeout` is the compatibility fallback for health/catalog operation budgets and also
sets the HTTP server's `ReadHeaderTimeout`. It is not an inference wall-clock deadline: proxied
generation, SSE flushing, and WebSocket lifetimes are not bounded by it. When `health_timeout` or
`catalog_timeout` are omitted, they inherit `request_timeout`, preserving existing configurations.
Set `health_timeout` short so an offline homelab backend does not delay readiness, and set
`catalog_timeout` long enough for catalog discovery plus optional Ollama `/api/show` enrichment.

`path_routes` sends requests to a backend by URL path before request-body model routing. Paths must
be canonical absolute paths without a trailing slash. A route matches its exact path and
slash-delimited descendants; overlapping routes use the longest match. Local `GET /v1/models`,
`/health`, and `/ready` handlers take precedence. Path-route changes participate in configuration
hot reload.

When the GPU backend is llama-swap, the `/comfyui` example forwards the path unchanged, including
query strings, encoded path segments, backend credentials, and WebSocket upgrades. That optional
llama-swap configuration can define the reserved local model ID `comfyui_auto`; llama-swap then owns
the redirect, cold-start restrictions, path stripping, and ComfyUI compatibility settings.

`comfyui_wait_timeout` optionally prevents recognized inference requests routed to the same backend
endpoint as `/comfyui` from reaching that backend while ComfyUI has queued or running jobs. Requests
routed to other backend endpoints are not delayed. It polls the configured `/comfyui` backend's
`GET /comfyui/queue` and returns HTTP 504 rather than forwarding when the timeout expires. A queue
query failure returns HTTP 502 (fail-closed). The option requires `path_routes: /comfyui` and is
disabled when omitted or set to `0`. It does not delay ComfyUI requests or non-inference requests,
and only coordinates submissions that pass through this proxy; direct ComfyUI submissions are
visible while present in its queue but cannot be tracked during the brief submission/queue update
race. Clients must retry a 504 after the render completes.

## Configuration Hot Reload

The proxy hashes the config file every 2 seconds and applies content changes without a container
restart, including same-size edits whose modification timestamp did not change.

- Backend URLs, prefixes, API keys, headers, and health paths take effect on the next request.
- `log_level` changes take effect immediately.
- If a new config fails to validate, the running configuration is kept and an error is logged; a
  corrected file is picked up automatically on the next poll.
- Changing the `listen` address is not applied at runtime and is logged as requiring a restart.
- The per-backend catalog cache is rebuilt on reload; with the default 15-second TTL this is
  transient.

For hot reload to work with Docker bind mounts, mount the config's **directory** rather than the
file. A single-file bind mount can capture the file's inode at container start, so replacing the
file (e.g. with `sed -i`) leaves the container reading a stale copy. Mounting the directory makes
the container look up `config.yaml` by name on each access:

```yaml
volumes:
  - ./config-dir/:/config:ro
```

## Catalog Aggregation

The proxy fetches `/v1/models` from all configured backends concurrently. It preserves complete
model objects, including `context_length`, `meta`, `status`, `owned_by`, capabilities, and unknown
future fields. Only `id` is changed by adding the backend prefix.

```console
curl -s http://localhost:9292/v1/models | jq '.data'
```

Filter models exposed by the GPU backend:

```console
curl -s http://localhost:9292/v1/models \
  | jq '.data[] | select(.id | startswith("gpu/"))'
```

Public ID collisions are deterministic: the default backend wins, then optional backends are
considered by backend ID. Later duplicates are omitted and logged without exposing model metadata.

Each backend has a thread-safe in-memory cache. Fresh values avoid backend requests, stale values
remain available after refresh errors, and concurrent requests share one refresh per backend. A
backend with no usable catalog is omitted when optional. The default backend must have a current or
stale catalog, otherwise `/v1/models` returns `502 Bad Gateway`.

## Routed Requests

```console
curl http://localhost:9292/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpu/other-model",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true
  }'
```

The router buffers only an incoming uncompressed JSON request body long enough to inspect and, when
needed, rewrite its top-level `model`. Arbitrary JSON fields are preserved. Empty, malformed, and
compressed bodies are forwarded unchanged to the default backend. Methods, paths, query strings,
headers, response statuses, and response headers are preserved by Go's `httputil.ReverseProxy`.
Streaming and SSE response bodies are never buffered and are flushed immediately.

## Health

`GET /health` is local liveness and does not contact a backend:

```console
curl -i http://localhost:9292/health
```

`GET /ready` checks every backend's configured health path (default `/health`) and reports their
statuses. Only default backend failure makes readiness return `503`; optional backend failures are
reported but do not make the router unready.

```console
curl -s http://localhost:9292/ready | jq
```

## Docker Compose

```console
mkdir -p config
cp config.example.yaml config/config.yaml
docker compose pull
docker compose up -d
```

The included Compose service pulls the public GHCR image, publishes `9292:8080`, mounts the config
directory read-only for hot reload, drops Linux capabilities, uses a read-only root filesystem,
restarts unless stopped, and checks local liveness. Pin a release with
`LLAMA_CATALOG_PROXY_TAG=v0.1.0` instead of using `latest`.

Validate configuration without starting the server:

```console
docker run --rm \
  -v "$PWD/config:/config:ro" \
  ghcr.io/jakeroxs/llama-catalog-proxy:latest \
  --config /config/config.yaml --check-config
```

Display image build identity with `--version`.

### On-demand diagnostics

Run `llama-catalog-proxy --config config/config.yaml --diagnostics` for a one-shot JSON
report. Only backends configured with `diagnostics: llama_swap` receive metadata probes.
With `request_history: true`, the report also includes normalized backend activity references
and backend-host GPU samples from bounded reads of the activity and performance APIs.
The extra fields include `backend_activity`, `backend_gpu`, `activity_gpu_associations`,
`backend_aliases`, `activity_model_identity`, `backend_lifecycle`, and `activity_lifecycle`.
`backend_lifecycle_events` describes adapter support; `activity_lifecycle_events` reports
unavailable associations when that source cannot provide transition evidence.
This does not query the running proxy's
in-memory request history. The CLI command is separate from the protected HTTP diagnostics API
documented above.

Each invocation has a new collector-local epoch. That epoch is not a verified backend process
generation, and activity IDs are scoped to their configured backend and local epoch. The
inspected llama-swap API does not retain the forwarded proxy request ID, so no exact request
join is claimed. Raw model labels, arbitrary metadata, upstream error text, and conversation
content are omitted. No background subscription, model load, or lifecycle operation is started.

Collection uses at most four concurrent metadata requests, a two-second per-request timeout,
and a 1 MiB response limit. It reads the latest activity page (100 rows per backend by default),
reports incomplete/truncated coverage, and retains at most 1,000 references and 10 MiB estimated
size, with a 24-hour horizon based on source timestamps. Backend clock skew can therefore affect
that horizon; it is not evidence of request timing or a process restart. Token metrics are not
copied into these references. A successful HTTP status is labeled `http_success`, not completed
generation. Explicit local epoch resets clear references and discard in-flight old-epoch results.

GPU reports expose percent utilization and memory in the source API's MB units. Explicit zero
readings remain distinct from absent or invalid measurements. Device identifiers are opaque;
raw names, UUIDs, driver identifiers, and system statistics are not returned. Up to eight distinct
devices per backend and 4,096 samples globally are retained, with incomplete/truncated coverage
reported explicitly. Unavailable or unauthorized data is not reported as complete coverage.

Activity/GPU associations are **approximate timestamp adjacency**, using a declared two-second
window before and after the activity timestamp. This is not a reconstructed request span.
Observed minimum/maximum sample spacing is reported only when at least two distinct matching
timestamps exist; it is not the configured sampling interval or an inferred generation rate.
Associations never cross a configured backend or collector-local epoch. Even a single matching
activity cannot establish device ownership: the GPU is a shared backend-host resource and may
not be the device executing the workload, especially for peer/remote routes. Clock alignment,
upstream process generation, and model lifecycle remain unverified/unknown. No exact proxy
request match, queue-time decomposition, or cold-start attribution is claimed.

Alias identity uses only explicit `/v1/models` catalog metadata, including alias lists on canonical
rows when separate alias entries are disabled. `catalog_resolved` means declared catalog identity,
not verified loaded weights or an exact proxy-request match. Ambiguous mappings, alias chains,
missing canonical targets, and incomplete catalogs do not produce a resolved identity. Returned
model/alias references are backend-scoped opaque hashes, not raw labels or configuration text.

The read-only `/running` snapshot contributes current states of non-stopped local processes.
It does not expose transition times or a backend boot identifier. Omitted models remain unknown,
never inferred stopped/unloaded. A uniquely resolved catalog identity may link to a current
observation, but `activity_lifecycle` always reports `kind: current_metadata_only` and
`state_at_activity: unknown`; it cannot establish a historical transition, restart, execution,
queue time, or cold start. Commands, process proxy URLs, names/descriptions, and TTL values are
excluded from the typed report.

The inspected `/api/events` endpoint re-sends `modelStatus` snapshots when internal process
events occur. It does **not** expose the internal old/new transition payload, an event timestamp,
or a replay identifier. The adapter therefore does not subscribe to that endpoint or its log
stream. `backend_lifecycle_events` reports `source: adapter_capability`,
`transition_payload_not_exposed`, and `collection_performed: false`; `observed_transitions: null`
means unavailable—not a measured zero or proof of inactivity. This describes the inspected
adapter contract, not a live probe of a customized deployed backend.

The pure lifecycle association API accepts supplied transition evidence for supported future
sources. It matches unique catalog identities by backend, epoch, activity ID, and canonical model
reference—not array position or time coincidence. Receipt time never replaces a missing source
timestamp; replay IDs stay private, exact replays do not inflate counts, conflicting replays cannot
produce approximate matches, and observed versus in-window event counts remain distinct. The
current adapter supplies no such events and never claims historical state or an exact request join.

## Container image publishing

Pushing a semantic-version tag such as `v0.1.0` publishes multi-architecture `linux/amd64` and
`linux/arm64` container images to `ghcr.io/jakeroxs/llama-catalog-proxy`. This workflow publishes
the container package only; it does not create a GitHub Release or attach source archives. It
publishes version, major.minor, commit-SHA, and (for non-prerelease tags) `latest` image tags with
an SBOM, provenance, and a registry attestation.

For a manual publish, choose the source branch/ref in the Actions UI and enter the image version
(for example, `0.3.0` or `v0.3.0`). The workflow builds that selected source and does not require or
create a Git tag. It still publishes the container package only; it does not create a GitHub Release.

## Development

Requirements: Go 1.26 or Docker.

```console
gofmt -w cmd internal
go vet ./...
go test ./...
go test -race ./...
docker build -t llama-catalog-proxy:local .
docker compose -f compose.yaml -f compose.dev.yaml up --build
```

Catalog responses are limited to 64 MiB. The cache is intentionally in-memory and cold after a
restart. Structured request logs include a proxy-generated request ID, method, path, route kind,
selected backend ID, status, duration, and response size. The request ID is echoed back to the
client and forwarded upstream so request and error events correlate. Incoming request IDs are never
trusted or logged verbatim. Log fields omit query strings, model IDs, request bodies, and configured
credentials/secrets.

## License

Licensed under the [MIT License](LICENSE).
