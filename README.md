# llama-catalog-proxy

[![CI](https://github.com/JakeRoxs/llama-catalog-proxy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/JakeRoxs/llama-catalog-proxy/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

One OpenAI-compatible base URL for several independent inference backends. `llama-catalog-proxy`
combines their model catalogs and routes requests by model ID. Give each backend an optional public
prefix so clients can select it without changing endpoints: `gpu/my-model` goes to the `gpu` backend
as `my-model`.

Works with [llama-swap](https://github.com/mostlygeek/llama-swap),
[Ollama](https://ollama.com), and other backends that return OpenAI-style model catalogs. It doesn't
run models, balance load, or translate API formats. Upstream servers still decide what to serve and
when to load or unload models.

## Getting started

You need at least one running backend that serves `GET /v1/models` as an object containing a `data`
array of model objects with string `id` fields. The proxy must be able to reach the backend from its
own network.

### Docker Compose

Clone the repository and run these commands from its root. This example binds the client port to
host loopback; the unmodified Compose file publishes it on all interfaces.

```sh
mkdir -p config
cp config.example.yaml config/config.yaml
export LLAMA_CATALOG_PROXY_PORT=127.0.0.1:9292
```

Edit `config/config.yaml`: replace the placeholder URLs with addresses reachable from the container
(`localhost` inside the container refers to the proxy container). For one backend, remove the `gpu`
entry and the example `path_routes` block. Keep `listen: 0.0.0.0:8080` so Compose can reach the
listener. Set a backend's `health_path` if it does not serve `/health` (for Ollama, use `/api/version`).

```sh
docker compose run --rm --no-deps llama-catalog-proxy --config /config/config.yaml --check-config
docker compose up -d
```

The [Compose file](compose.yaml) uses the [GHCR image](https://github.com/JakeRoxs/llama-catalog-proxy/pkgs/container/llama-catalog-proxy),
a read-only config directory mount, and a liveness check. Set `LLAMA_CATALOG_PROXY_TAG` to a
published image tag to pin the version. `--check-config` checks configuration, not backend
reachability.

### Build and run locally

The module declares Go 1.24.0 in [`go.mod`](go.mod); the [container build](Dockerfile) uses Go 1.27.
Use the same `config/config.yaml` as above, but set `listen: "127.0.0.1:9292"` and use backend
addresses reachable from your host.

```sh
go build -o ./bin/llama-catalog-proxy ./cmd/llama-catalog-proxy
./bin/llama-catalog-proxy --config config/config.yaml --check-config
./bin/llama-catalog-proxy --config config/config.yaml
```

### Check the proxy and send a request

```sh
curl -fsS http://127.0.0.1:9292/health
curl -sS http://127.0.0.1:9292/ready
curl -fsS http://127.0.0.1:9292/v1/models
```

Set your client's OpenAI base URL to `http://127.0.0.1:9292/v1`. Use an ID exactly as listed in
`/v1/models`, and supply a credential if your deployment requires one. For a chat-capable model:

```sh
curl -N http://127.0.0.1:9292/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu/my-model","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

Replace `gpu/my-model` with a listed ID. The model's supported fields and endpoints depend on the
backend.

## Architecture and routing

```text
                                  ┌── default backend  (my-model)
Client ──► llama-catalog-proxy ───┤
           /v1/models, /health    └── gpu backend      (gpu/my-model → my-model)
           /ready
```

| Request | Behavior |
| --- | --- |
| `GET /v1/models` | Merges cached catalogs, adds configured prefixes to IDs, and keeps backend-specific model fields. Duplicate public IDs prefer the default backend, then optional backends in backend-ID order. Failed refreshes can serve stale local entries; a failed optional backend without a usable catalog is omitted. A missing usable default catalog returns `502`. |
| `GET /health` | Checks proxy liveness without contacting backends. |
| `GET /ready` | Checks each backend's `health_path`. Default backend failure returns `503`; optional failures are reported without failing readiness. Readiness does not prove a model is loaded. |
| Configured path route | For ordinary local traffic, matches the longest configured path on a segment boundary before model routing. Forwards the path rather than stripping the match. |
| Model route | For ordinary local traffic, inspects an uncompressed JSON body's top-level `model`. The longest matching prefix selects the backend and is removed before forwarding. Local model routing does not consult the catalog. |
| Default route | Ordinary local requests without a matching path or model prefix go to `default_backend`. |

The reverse proxy streams responses, including SSE, and supports WebSocket upgrades. The default
JSON request inspection limit is 64 MiB; requests over that limit return `413` when inspection is
required. Opt-in federation applies [additional routing and request rules](OPERATOR-GUIDE.md#experimental-peer-federation).

## Configuration

Start with [`config.example.yaml`](config.example.yaml). `default_backend` must name an entry
in `backends`; URLs must be absolute HTTP(S) URLs without a query or fragment, and non-empty prefixes
must be unique. The default file is `/config/config.yaml`; use `--config` to select another or
`--version` to show the build identity.

| Setting | Default or behavior |
| --- | --- |
| `listen` | `0.0.0.0:8080` (Compose publishes host port `9292` by default) |
| `models_cache_ttl` | `5m` per backend |
| `request_timeout` | `10s`; fallback for health/catalog budgets and the startup HTTP header-read timeout, not a generation deadline |
| `health_timeout`, `catalog_timeout` | Inherit `request_timeout` when omitted |
| `backends.<id>.health_path` | `/health`; only affects readiness probes |
| `backends.<id>.api_key`, `backends.<id>.headers` | Optional outbound credential/header injection; doesn't replace credentials or headers already supplied by the client |

The proxy polls config contents every two seconds, keeps the last valid config after a rejected
edit, and clears local catalog caches on reload. Mount the **directory**, not a single file, so
replacements are visible inside a container. Listener address changes require a restart; some
optional features have their own restart rules. See the [configuration reference](OPERATOR-GUIDE.md#configuration-reference)
for environment overrides and path routing details.

**Protect the client listener.** Core catalog, health, and routing endpoints do not authenticate
clients. Backend keys are outbound credentials, not client access controls. Keep the HTTP listener
on loopback or a trusted network, and require access controls and HTTPS at the edge before exposing
it to untrusted clients. Keep runtime credentials out of source control. See the
[security policy](SECURITY.md) for vulnerability reporting.

## Optional features

These features are not needed for standalone routing.

| Feature | What it adds |
| --- | --- |
| [Ollama enrichment](OPERATOR-GUIDE.md#ollama-catalog-enrichment) | Adds model details from Ollama's `/api/show` to catalog entries when available. |
| [Diagnostics and observability](OPERATOR-GUIDE.md#diagnostics-and-observability) | Bounded proxy timing and request history, and a protected read-only API. The [`--diagnostics` CLI mode](OPERATOR-GUIDE.md#on-demand-diagnostics) collects supported backend metadata; the [stdio MCP adapter](OPERATOR-GUIDE.md#agent-facing-mcp-diagnostics) exposes fixed diagnostic queries. |
| [Peer federation](OPERATOR-GUIDE.md#experimental-peer-federation) | Direct HTTPS catalog exchange and separately authorized inference or path forwarding. No recursive re-export; stale peer entries cannot be routed. See the [node-agent contract](NODE-AGENT-CONTRACT.md) for report and identity semantics. |
| [Management and administrator UI](OPERATOR-GUIDE.md#experimental-local-configuration-read-and-preview) | Typed model-setting previews, durable proposals, and separately authorized writes and rollback. Optional [OIDC admin UI](OPERATOR-GUIDE.md#experimental-administrator-web-ui) for local users. File replacement can disrupt a backend watching its config; it does not prove settings became effective. |
| [GGUF ingestion](OPERATOR-GUIDE.md#experimental-local-gguf-downloads) | Linux-only Hugging Face downloads and local artifact adoption with durable records. Does not automatically register or load a model. |
| [Synthetic benchmarks](OPERATOR-GUIDE.md#controlled-synthetic-benchmark-runs) | Bounded CLI and optional web runs against a ready model. Runs send real inference and require explicit acknowledgement of possible interference. |
| [ComfyUI integration](OPERATOR-GUIDE.md#comfyui-integration) | Path forwarding, selected direct-service routes, and optional request admission for proxy-mediated GPU sharing. Not a host-wide GPU lock. |

Federated model IDs use `@peer/<node>/<backend>/<model>`; the backend and local model ID segments
encode the exact UTF-8 IDs using unpadded base64url. Copy the public ID from the catalog rather than
constructing it by hand.

## Development

The proxy uses Go's `net/http` and `httputil.ReverseProxy`, YAML v3 for configuration, and Docker
Compose for the container setup. Optional integrations use the Go MCP SDK, OIDC/OAuth 2.0, and mDNS.
The embedded web UIs use plain JavaScript, HTML, and CSS.

### Project structure

| Path | Responsibility |
| --- | --- |
| [`cmd/`](cmd/) | Proxy entry point, peer/operator commands, benchmark CLI, and stdio MCP adapter |
| [`internal/config/`](internal/config/) and [`internal/catalog/`](internal/catalog/) | Configuration validation and per-backend catalog caching |
| [`internal/proxy/`](internal/proxy/) | HTTP routing, reverse proxy, config reload, optional endpoints, and embedded web assets |
| [`internal/diagnostics/`](internal/diagnostics/), [`internal/correlation/`](internal/correlation/), and [`internal/timing/`](internal/timing/) | Bounded diagnostic evidence and request observations |
| [`internal/federation/`](internal/federation/) and [`internal/nodecontract/`](internal/nodecontract/) | Peer trust, grants, and report validation |
| [`internal/modelconfig/`](internal/modelconfig/) and [`internal/ingestion/`](internal/ingestion/) | Protected configuration changes and GGUF artifact handling |
| [`OPERATOR-GUIDE.md`](OPERATOR-GUIDE.md) and [`.github/workflows/`](.github/workflows/) | Operator documentation, CI, and image publishing |

### Testing

Use Go's standard test runner. Unit and HTTP integration tests use local fixtures and test servers;
they don't require a production backend. Some platform-specific tests run only on Linux. Node.js
is needed only to run the embedded browser code's unit tests.

```sh
go vet ./...
go test -coverprofile=coverage.out ./...
go test -race ./...
node --test internal/proxy/benchmark_ui/*.test.cjs
```

For a local container build, use
`docker compose -f compose.yaml -f compose.dev.yaml up --build`. The
[opt-in llama-swap config-watch test](OPERATOR-GUIDE.md#experimental-reviewed-apply-and-rollback)
has separate Linux, namespace, and reference binary prerequisites.

### Contributing and releases

- Follow [`AGENTS.md`](AGENTS.md): keep changes in this repository, use supported upstream APIs,
  and report unavailable request identity or lifecycle evidence as unknown or unsupported.
- Format edited Go files with `gofmt` and add tests near the code they cover. See
  [`internal/proxy/server_test.go`](internal/proxy/server_test.go) for routing tests and
  [`internal/config/config_test.go`](internal/config/config_test.go) for config validation tests.
  Keep request handling bounded and credentials and inference content out of diagnostic output.
- Run the relevant checks before opening a pull request. [CI](.github/workflows/ci.yml) runs
  formatting, vet, coverage, race checks, secret scanning, and a container build. Update the
  example config and [changelog](CHANGELOG.md) when public behavior changes.
- The [publish workflow](.github/workflows/release.yml) builds Linux `amd64` and `arm64` images on
  `v*` tags or manual dispatch, with SBOM and provenance. It publishes to GHCR; it doesn't create
  a GitHub Release.

## License

[MIT](LICENSE).
