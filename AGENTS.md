# Project Scope

- Implement changes only in this catalog-proxy repository. Do not edit llama-swap or other upstream projects, unless specifically authorized.
- Upstream source available in the workspace is read-only reference material for verifying existing behavior.
- Use existing supported APIs as they are. Missing request identity, lifecycle timestamps, replay IDs, or
  other unavailable evidence must be represented as unsupported/unknown, not inferred or treated as an
  outstanding requirement to change upstream.
