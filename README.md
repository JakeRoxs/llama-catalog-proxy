# llama-catalog-proxy

`llama-catalog-proxy` is a small model-aware OpenAI-compatible router for independent
[llama-swap](https://github.com/mostlygeek/llama-swap) backends. It aggregates each backend's model
catalog, gives each backend an optional configurable public prefix, and routes inference requests by
that prefix.

Each llama-swap instance manages only its local models. The backends do not need llama-swap peer
configuration, and the proxy does not implement scheduling, model lifecycle management, load
balancing, or inference logic.

## Architecture

```text
                          +-> default llama-swap (local model IDs)
Client -> router :9292 --|
                          +-> GPU llama-swap (public prefix: gpu/)
```

`GET /v1/models` is handled locally by aggregating cached backend catalogs. All other requests are
inspected for a top-level JSON `model` string:

- `local-model` routes to the default backend unchanged.
- `gpu/other-model` routes to the GPU backend as `other-model`.
- Overlapping prefixes use the longest match.
- Unknown and non-model requests route to the default backend.

Routing depends only on configuration, not on whether a model is present in the catalog cache.

## Configuration

The default configuration path is `/config/config.yaml`. Override it with `--config`.

```yaml
listen: 0.0.0.0:8080
log_level: INFO

default_backend: default
backends:
  default:
    url: http://llama-swap-default:8080
    prefix: ""
    # api_key: replace-with-default-api-key
  gpu:
    url: http://llama-swap-gpu:8080
    prefix: gpu/
    # api_key: replace-with-gpu-api-key
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
headers are also applied only when the request does not already contain that header. Header values,
API keys, and request bodies are not logged.

`max_json_request_body_bytes` limits uncompressed JSON bodies that must be buffered for model-aware
routing. Oversized JSON requests return `413 Request Entity Too Large`. Explicitly non-JSON,
compressed, and path-routed request bodies remain streaming passthroughs.

Environment overrides:

| Variable | Meaning |
| --- | --- |
| `LLAMA_CATALOG_LISTEN` | HTTP listen address |
| `LLAMA_CATALOG_DEFAULT_BACKEND_URL` | URL of the named default backend |
| `LLAMA_CATALOG_DEFAULT_BACKEND_API_KEY` | API key of the named default backend |
| `LLAMA_CATALOG_MODELS_CACHE_TTL` | Per-backend catalog cache duration |
| `LLAMA_CATALOG_REQUEST_TIMEOUT` | Backend request timeout |
| `LLAMA_CATALOG_LOG_LEVEL` | `debug`, `info`, `warn`, or `error` |

Backend definitions remain in YAML so model namespaces and routing destinations stay explicit.

`path_routes` sends requests to a backend by URL path before request-body model routing. Paths must
be canonical absolute paths without a trailing slash. A route matches its exact path and
slash-delimited descendants; overlapping routes use the longest match. Local `GET /v1/models`,
`/health`, and `/ready` handlers take precedence. Path-route changes participate in configuration
hot reload.

The `/comfyui` example forwards the path unchanged to the GPU llama-swap, including query strings,
encoded path segments, backend credentials, and WebSocket upgrades. The GPU llama-swap config
must define the reserved local model ID `comfyui_auto`. llama-swap then owns the `/comfyui` redirect,
cold-start restrictions, path stripping, and ComfyUI compatibility settings.

## Configuration Hot Reload

The proxy hashes the config file every 2 seconds and applies content changes without a container
restart, including same-size edits whose modification timestamp did not change.

- Backend URLs, prefixes, API keys, and headers take effect on the next request.
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

`GET /ready` checks every backend's `/health` endpoint and reports their statuses. Only default
backend failure makes readiness return `503`; optional backend failures are reported but do not make
the router unready.

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
attestation. After the first workflow run, the GHCR package visibility must be set to public in its
package settings.

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
