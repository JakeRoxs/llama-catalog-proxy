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

### Security

- Hardened public Compose defaults with a read-only root filesystem, dropped capabilities, and
  `no-new-privileges`.
