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

models_cache_ttl: 15s
request_timeout: 10s
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
| `LLAMA_CATALOG_REQUEST_TIMEOUT`         | Backend request timeout              |
| `LLAMA_CATALOG_LOG_LEVEL`               | `debug`, `info`, `warn`, or `error`  |

Backend definitions remain in YAML so model namespaces and routing destinations stay explicit.

`path_routes` sends requests to a backend by URL path before request-body model routing. Paths must
be canonical absolute paths without a trailing slash. A route matches its exact path and
slash-delimited descendants; overlapping routes use the longest match. Local `GET /v1/models`,
`/health`, and `/ready` handlers take precedence. Path-route changes participate in configuration
hot reload.

When the GPU backend is llama-swap, the `/comfyui` example forwards the path unchanged, including
query strings, encoded path segments, backend credentials, and WebSocket upgrades. That optional
llama-swap configuration can define the reserved local model ID `comfyui_auto`; llama-swap then owns
the redirect, cold-start restrictions, path stripping, and ComfyUI compatibility settings.

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

## Releases

Pushing a semantic-version tag such as `v0.1.0` publishes signed, multi-architecture `linux/amd64`
and `linux/arm64` images to `ghcr.io/jakeroxs/llama-catalog-proxy`. The release workflow publishes
semantic-version, `latest`, and commit-SHA tags with an SBOM, provenance, and a registry
attestation.

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
restart. Structured logs include request method, path, status, duration, and response size, while
configured URL credentials are redacted.

## License

Licensed under the [MIT License](LICENSE).
