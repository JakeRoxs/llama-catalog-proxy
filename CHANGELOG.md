# Changelog

All notable changes to this project will be documented in this file.

The project follows semantic versioning. Before version 1.0, minor releases may contain breaking
configuration changes.

## Unreleased

### Added

- Multi-backend model catalog aggregation and prefix-based request routing.
- Configurable path routing with WebSocket passthrough.
- Runtime configuration hot reload.
- Bounded JSON request-body inspection.
- Configuration validation and build version CLI modes.
- CI and multi-architecture GHCR image publishing.
- Configurable per-backend health paths (`health_path`, default `/health`) to support Ollama and other non-standard liveness endpoints.
- Optional Ollama catalog enrichment (`ollama_show_enrichment`) that fetches `POST /api/show` per model with bounded concurrency and merges context length, capabilities, and model metadata under an `ollama` key.

### Security

- Hardened public Compose defaults with a read-only root filesystem, dropped capabilities, and
  `no-new-privileges`.
