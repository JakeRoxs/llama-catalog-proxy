# Node Agent Contract v1

**Status:** Contract, report validator, opt-in authenticated HTTPS report exchange, one-hop
catalog/inference federation, and a separate management listener for read-only configuration
settings/previews, opt-in durable proposals, separately authorized backup metadata/recovery, and
opt-in reviewed local apply and rollback.

This contract defines the identity and report boundaries for a colocated catalog-proxy node. It is
limited to this repository and uses currently supported local observations. It does not extend or
assume new llama-swap APIs.

## Compatibility and Scope

- Standalone catalog aggregation, routing, and the existing diagnostics API continue to work when
  node-agent features are not configured.
- Federation is opt-in. No peer discovery, required coordinator, second service, SSH, Docker socket,
  or privileged container is introduced.
- This is a node/report contract, not an endpoint layout, remote routing algorithm, model-settings
  editor, or delegated-action protocol. Those are implementation slices in todos 020, 021, and 023.
- A capability describes what a node has enabled and can provide. It does not grant a peer permission
  to use that capability.

## Protocol Versioning

Every peer report carries `protocol_version: { "major": 1, "minor": 0 }`.

- A receiver rejects an unsupported major version and does not consume that report.
- A receiver ignores unknown optional fields within a supported major version, within the report
  size and nesting limits. Duplicate or case-fold-equivalent JSON object keys are rejected.
- Unknown required features or requested scopes fail closed; a version mismatch never broadens access.
- The peer protocol version is independent of the proxy build version and upstream backend version.

## Identity and Ownership

### Node identity

- `node_id` is an operator-assigned, stable, non-secret identifier. It is required only when
  federation is enabled; it is not inferred from a hostname, IP address, backend URL, upstream
  `peer_id`, or model name.
- Treat the value as opaque and case-sensitive. The first implementation should bound it to 1–128
  ASCII characters from `[A-Za-z0-9._~-]`, and reject empty, malformed, or oversized values.
- A configured peer credential is bound to exactly one expected `node_id`. The ID in a report must
  match the authenticated peer or the report is rejected.

### Proxy and backend generations

- `proxy_instance_id` is a 32-character lowercase hexadecimal encoding of 16 cryptographically random
  bytes created when the proxy process starts. It identifies only that proxy process incarnation and
  changes on proxy restart. The validator checks the format, not the randomness.
- `config_generation` starts at 1 for the successfully loaded configuration and increments after
  each successful in-process configuration reload. It is comparable only with the same
  `proxy_instance_id` and resets on process restart.
- Neither field identifies a llama-swap process or proves that a model was restarted, reloaded, or
  made effective. `backend_generation` remains unknown unless a supported backend API provides
  verifiable evidence.
- V1 reports are snapshots, not a durable event log. They promise no persisted sequence, replay
  detection, or exactly-once delivery. Mutating action identity and retry behavior are outside this
  report contract and remain for todo 023.

### Resource identity

The canonical model key is the tuple:

```text
(owner_node_id, backend_id, local_model_id)
```

`backend_id` is the configured backend key; `local_model_id` is the exact catalog ID. A proxy's public
prefix is presentation, not identity. An alias maps to a canonical key only when the catalog uniquely
establishes that mapping; otherwise its resolution remains unknown or ambiguous. Federated public
IDs encode the owner and exact backend/local-ID tuple as described in the README.

Each node exports only resources it owns, using an explicit local-ownership configuration or equivalent
operator-controlled inventory. A configured backend is not necessarily local: URLs, prefixes, and
model names do not prove ownership. Imported resources retain their original owner and source
provenance, are stored separately from local resources, and are never re-exported as local data.
Conflicting or duplicate identities are ambiguous; this validator rejects a report containing duplicate
model keys rather than choosing a winner. Checking that a peer actually owns a backend is an export
policy responsibility; matching a source ID to an authenticated peer does not prove ownership.

## Report Envelope and Evidence

Each report includes the following logical envelope:

```json
{
  "protocol_version": { "major": 1, "minor": 0 },
  "required_features": [],
  "source": {
    "node_id": "node-a",
    "proxy_instance_id": "0123456789abcdef0123456789abcdef",
    "config_generation": 1
  },
  "generated_at": "2026-11-14T00:00:00Z",
  "capabilities": {
    "catalog_read": "enabled",
    "diagnostics_read": "disabled",
    "federation_exchange": "enabled",
    "artifact_read": "disabled",
    "artifact_write": "disabled",
    "config_write": "disabled"
  },
  "coverage": "partial",
  "observations": [{
    "identity": {
      "owner_node_id": "node-a",
      "backend_id": "local",
      "local_id": "example-model"
    },
    "state": "supported",
    "observed_at": "2026-11-14T00:00:00Z",
    "freshness": "fresh",
    "coverage": "complete"
  }]
}
```

The example values are illustrative. V1 model observations describe catalog membership, not verified
weights or runtime state. An optional `backend_readiness` array contains a separate, bounded health
check projection for explicitly owned backends, when the requesting peer has a `diagnostics_read`
grant in addition to `catalog_read`. Each entry includes `backend_id`, `status` (`ready`,
`unavailable`, or `unknown`), optional source `observed_at`, and `freshness`. A missing array means
readiness was not supplied; an unknown result has no timestamp. The last completed `/ready` probe is
the only source; report requests do not cause backend traffic. This is endpoint health evidence, not
proof that a model is loaded or inference will succeed. Unsupported measurements remain unknown.
Older v1 peers may omit or ignore this optional field.

Reports contain only bounded, typed projections, never raw upstream metadata,
commands, credentials, inference content, or arbitrary peer-supplied URLs. The validator limits the
report to 1 MiB, 1,000 observations, 256 backend readiness entries, 32 required features,
64 capability entries, and 32 nested JSON
levels; backend and local-model IDs follow existing model-identity bounds (256 bytes).

For each resource or measurement:

- `observed_at` is the source collector's observation time. It is absent/unknown when the source
  cannot provide it; it is never filled with report generation or receipt time.
- `generated_at` is when the source assembled the report, not when each observation occurred.
- The receiver records its own `received_at` outside the source report after successful
  authentication and validation. It does not rewrite source timestamps.
- `freshness` is `fresh`, `stale`, or `unknown`. A receiver may determine source age only when the
  source timestamp and clock assumptions support it. Receipt age is a separate local liveness fact
  and does not prove that the underlying observation is fresh. A source's freshness claim is not
  independent proof at the receiver. Missing source observation time requires `unknown` freshness.
- `coverage` is `complete`, `partial`, `truncated`, or `unknown`, at both report and relevant
  collection scope. Missing, disabled, timed-out, or truncated sources cannot be presented as a
  complete global view.
- Evidence `state` distinguishes `supported`, `unsupported`, `disabled`, `unavailable`,
  `unauthorized`, `unknown`, `partial`, and `ambiguous`. Missing values remain unknown; zero is not
  substituted for missing data. `supported` and `partial` observations require an observation time.

The six v1 capability entries and `required_features` are required. No extension feature is defined
yet, so any nonempty `required_features` list is rejected. Unknown optional capability names are
ignored. A complete report requires catalog capability enabled and every included observation to be
supported with complete collection coverage. A producer must also account for omitted or unavailable
sources; the validator cannot verify that an otherwise empty report is actually exhaustive.

The authenticated receiver keeps peer alias, authenticated `node_id`, and `received_at` as local
receipt metadata, separate from the source report and its coverage. Receipt metadata is never
re-exported as source evidence.

## Capabilities and Authorization

The report advertises whether each local feature is enabled. The initial capability names are
`catalog_read`, `diagnostics_read`, `federation_exchange`, `artifact_read`, `artifact_write`, and
`config_write`.

- Federation and all artifact/config management are disabled by default.
- Existing local catalog and routing behavior remains independent from federation or management.
- Enabling a feature does not authorize any peer to invoke it.
- The owner maintains an explicit per-peer allowlist with a stable peer identity and separate grants
  for catalog reads, diagnostics reads, artifact reads, artifact writes, and configuration writes.
- Artifact and configuration access use separate, explicit local path allowlists. Enabling or
  granting artifact writes does not enable config writes, and vice versa.
- A node rechecks its current local feature enablement, peer grant, target ownership, and configured
  path/settings policy for every request. A peer report or capability advertisement cannot select an
  arbitrary path, command, backend URL, or write target.
- Peer credentials, diagnostics credentials, inference client credentials, and backend credentials
  are distinct. Revoking a peer removes its access without rotating unrelated client or backend keys.

Advertised support is not proof of authorization, success, or effective runtime state. Read and write
capabilities use separate authorization decisions; federation read access never implies mutation
rights.

The experimental management listener uses its own TLS endpoint, credential, and configured local
target aliases; it is not exposed through federation or diagnostics. A separately authorized proposal
store persists redacted revision-bound proposals, but never writes the target configuration.
The separately authorized backup recovery surface returns metadata or deletes a named backup; it
does not expose backup contents or grant configuration-write permission.
An independent write credential authorizes reviewed local apply and rollback only with explicit
per-operation review, disruption, and exclusive-window acknowledgements. Peer reports keep
`config_write` disabled: the separate local management listener does not expose delegated peer
configuration writes.
Neither previews nor saved proposals authorize apply or prove upstream validation or effective
runtime state. Proposal verification is a point-in-time source check, not a lock or authorization
to apply. See the README for its current limits.

## Peer Trust and Transport

- Peers are explicitly configured; reports cannot add peers or supply routing destinations.
- Peer requests use authenticated HTTPS with certificate validation on outbound connections and TLS
  protection on ingress, either at the service or an explicitly trusted gateway. If a gateway
  terminates TLS, its connection to the proxy must also be protected, for example with TLS or a
  permission-restricted local Unix socket. Do not send bearer credentials over plaintext or assume a
  trusted LAN.
- Use a distinct credential per configured peer, bind it to that peer's expected node identity, and
  support revocation/rotation independently. Never place credentials in URLs, reports, logs, or
  client-visible errors.
- Do not follow redirects to a different origin with peer credentials.
- Peer exchanges require bounded request/response sizes, deadlines, concurrency, and configured peer
  counts. Concrete defaults and endpoint paths are implementation details for todo 020.

## Deployment and Failure Semantics

- Keep the same repository-owned image and one proxy container per device. Use explicit,
  least-privilege local mounts for optional artifact/config access; no SSH or Docker socket is needed.
- When federation or management is disabled or unavailable, local catalog/routing remains usable.
  Peer loss affects that peer's report coverage and freshness, not the health of local resources.
- Unsupported upstream evidence stays unsupported/unknown. In particular, current APIs do not
  establish an upstream boot identity, a durable event/replay ID, or a request-to-peer identity join.

## Contract Validation and Remaining Work

`internal/nodecontract` provides a pure bounded JSON decoder and validator, with fixtures for
incompatible or malformed envelopes, oversized identities, authenticated-peer/report identity
mismatches, unsupported required features, and invalid evidence states. It verifies that:

- receipt time never substitutes for source observation time;
- a report does not claim a different node as owner for its observations;
- duplicate model identities are rejected instead of silently selecting a winner.

The package also provides a local report builder over explicitly selected backend caches. Its live
catalog variant visits cached IDs without cloning complete catalogs or issuing upstream requests;
context cancellation interrupts cache-lock waits and scans. It limits retained observations and
encoded report size, and reports incomplete coverage when the cache cannot establish catalog
completeness. The caller must establish ownership and ensure the advertised capabilities reflect
the running service.

The package itself does not authenticate peers, publish reports, authorize operations, or verify that
an advertised resource is actually local. It accepts the authenticated identity and receipt time
from its caller, and the caller must authenticate independently before decoding. The optional
runtime listener authenticates per-peer tokens and exports only operator-selected cached backends.
Configured peers can import validated reports and expose their receipt status via protected
diagnostics; these reports are never re-exported. Peer-qualified inference requires explicit
grants, fresh evidence, and direct routing to an explicitly owned backend. The runtime changes in
020-023 must test
ownership configuration, authentication and authorization, revocation, standalone compatibility,
and report/export integration; an advertisement must never authorize an action.

Path allowlists and config-write transaction semantics belong to todo 021; federated catalog IDs,
routing, refresh limits, and outage handling belong to todo 020; delegated action replay and audit
semantics belong to todo 023.
