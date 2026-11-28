# Operator guide

Start with the [README](README.md) for installation, core routing rules, and health checks. This
guide covers optional features and their configuration, permissions, and operational limits.

## Contents

- [Peer federation](#experimental-peer-federation)
- [Managed settings and reviewed writes](#experimental-local-configuration-read-and-preview)
  - [Durable proposals](#experimental-durable-proposals)
  - [New model entries](#experimental-new-entry-review)
  - [Backup recovery](#experimental-backup-recovery)
  - [Apply and rollback](#experimental-reviewed-apply-and-rollback)
- [GGUF ingestion](#experimental-local-gguf-downloads)
- [Ollama](#ollama)
- [Observation, history, and protected diagnostics API](#diagnostics-and-observability)
- [Benchmarks](#controlled-synthetic-benchmark-runs)
- [Administrator UI](#experimental-administrator-web-ui)
- [MCP adapter](#agent-facing-mcp-diagnostics)
- [ComfyUI integration](#comfyui-integration)
- [One-shot diagnostics](#on-demand-diagnostics)
- [Configuration reference](#configuration-reference)
- [Request logs](#request-logs)

## Experimental peer federation

The optional HTTPS peer listener serves `GET /peer/v1/report` and `POST /peer/v1/inference`.
Reports export cached catalog IDs from explicitly selected local backends. The report request itself
does not contact backends; owner-side refresh runs separately. Missing and incomplete evidence
remains unknown or partial. Optional mDNS discovery lists untrusted candidates only; reports are not
forwarded recursively. See the [node-agent contract](NODE-AGENT-CONTRACT.md) for report semantics.

```yaml
peer:
  node_id: node-a
  listen: "0.0.0.0:9293"
  tls_cert_file: /config/peer-cert.pem
  tls_key_file: /config/peer-key.pem
  local_backends: [default]
  peers:
    node-b:
      token: "<unique-peer-token-at-least-32-characters>"
      catalog_read: true
      diagnostics_read: true
      inference: true
      url: "https://node-b.example:9293"
      outbound_token: "<different-token-issued-by-node-b>"
      ca_file: /config/node-b-ca.pem
      route_inference: true
```

Use a unique, randomly generated token per peer, stored in protected runtime configuration, and
use TLS certificates trusted by the peer. Tokens must be distinct from configured diagnostics and
backend credentials. Peers authenticate with `Authorization: Bearer <token>`; a grant without
`catalog_read: true` cannot read reports. Only declare backends this node owns; a backend URL or
prefix does not establish ownership.
`inference: true` separately grants that peer permission to send inference to explicitly owned
backends. `route_inference: true` separately permits this node to send inference to the peer;
both directions default to false.
`diagnostics_read: true` separately permits that peer to receive backend readiness evidence in
reports; it requires `catalog_read: true` and defaults to false. It does not grant access to the
local diagnostics API or other diagnostic data.

For the current manual onboarding flow, choose stable node IDs first, configure and verify each
node's certificate identity/trust and peer listener reachability, then exchange inbound and outbound
tokens out of band. The local `token` authorizes the remote node's requests to this node; the local
`outbound_token` is sent to the remote node and must match its `token`. Reciprocal links use distinct
tokens for the two directions. Enable only the scopes needed in each direction, configure the
counterpart independently, and pair identities separately; pairing alone does not create grants.
Validate the local file with `--check-config` and inspect a secret-free handoff summary with:

```sh
llama-catalog-proxy peer config plan --config /config/config.yaml
```

The planner reads and validates the existing configuration through the canonical config loader. It
reports configured directions, scope names, and counterpart field names, but does not show URLs,
credentials, or certificate paths, modify configuration, verify the other node's configuration, or
establish trust.

When `peer` is configured, `node_id`, `listen`, `tls_cert_file`, and `tls_key_file` are required;
`local_backends` and `peers` may be empty. `federation_id` opts into mTLS identity membership and
requires the local client CA/CRL files plus protected membership and audit directories. For each
peer grant, `token` is needed for inbound requests and `url` plus `outbound_token` for outbound
requests. Outside federation mTLS, outbound `ca_file` may be omitted only when system TLS roots are
appropriate. Federation mTLS requires both peer `ca_file` and `crl_file`. Every operation scope
defaults off; path service access/routes, owner-management grants, aliases, and backend ownership are
separately configured features.

Optional mDNS/DNS-SD candidate advertisements and browsing require a federation-enabled peer listener
and a selected multicast interface. Configure `peer.discovery` explicitly on each node:

```yaml
peer:
  catalog_grant_directory: /var/lib/catalog-proxy/catalog-grants
  discovery:
    enabled: true
    interface: eth0
    advertise_url: https://proxy-a.local:9293
```

`advertise_url` is that node's own HTTPS peer-listener origin; its port must match `peer.listen`, and
the URL host must match a DNS/IP subject alternative name on the peer certificate. It is announced
only on the selected interface. List up to 64 candidates with
`llama-catalog-proxy peer discover --config /config/config.yaml`; its JSON `truncated` field reports
when more results may be available. Discovery output is untrusted and does not enroll a member, create
credentials, configure a peer grant, or activate inference/path/settings routes. Pair using
`peer join --discover-node-id <node-id>` and a protected one-time invitation. The proxy starts its
advertisement only when the feature is enabled; if the selected interface cannot advertise, it logs a
warning and continues without discovery. Changes to the discovery settings require a proxy restart.
The separate `catalog_grant_directory` must be owner-only (mode `0700`); its grant file is mode `0600`.
To poll paired members
without static peer URLs or bearer credentials, each receiving node must also grant `catalog_read`
explicitly through the federation admin UI or the local CLI:

```sh
llama-catalog-proxy peer catalog-grants create --node-id node-b --acknowledge
```

Grant on both sides for bidirectional catalog exchange. These grants bind to the current paired
certificate fingerprint; certificate rotation requires a fresh grant. Inference, diagnostics,
settings, and path-service access are not implied. When a matching static outbound `PeerGrant` URL is
present it remains the polling source; remove that static URL to use the dynamically discovered mDNS
endpoint. Static token/scope grants remain available for other peer operations.

Dynamic inference requires two additional, independent approvals. On the caller, catalog read must
already be granted, then explicitly allow routing to that node:

```sh
llama-catalog-proxy peer inference-grants create --node-id node-b --route-outgoing --acknowledge
```

On the receiver, explicitly allow that caller to invoke local inference:

```sh
llama-catalog-proxy peer inference-grants create --node-id node-a --allow-incoming --acknowledge
```

The inbound and outbound inference flags are independent and bind to the current paired certificate.
Catalog read never grants inference; revoking catalog read also disables outbound dynamic inference
because the caller needs fresh model identities, but does not revoke an independent inbound inference
grant. A configured static outbound peer URL takes precedence for its node. Diagnostics, settings,
and path-service operations remain separately authorized.

mDNS is link-local. Verify multicast reachability from the proxy's own network namespace, not just
from the host. Separate Docker bridges can isolate mDNS traffic even when the hosts can discover one
another. If the proxy containers do not share a multicast-capable network, leave discovery disabled
and use static `PeerGrant` configuration.

### Optional operator-CA mutual TLS identity

For CA-verified peer identities, configure the same federation ID and operator CA bundle on every
member. Each node must generate and protect its own private key locally. Have the operator-managed CA
issue a certificate for that node with both `serverAuth` and `clientAuth` extended usages and exactly
one federation identity URI SAN, `spiffe://<federation-id>/node/<node-id>`. Never copy a node private
key to another host or put a CA private key in proxy configuration.

On the node, generate a key and CSR locally (replace the federation and node IDs with the configured
values). The CA must enforce the URI SAN and both TLS usages in its signing profile; requested CSR
extensions are not trusted as CA policy:

```sh
umask 077
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out node-a-key.pem
openssl req -new -key node-a-key.pem -subj "/CN=node-a" \
  -addext "subjectAltName=URI:spiffe://lab.example/node/node-a" \
  -out node-a.csr
```

After the external CA issues the certificate, install its leaf and any required intermediates in
`tls_cert_file`, keep the matching private key at `tls_key_file`, and restart to enable the mTLS
identity mode. The private key never leaves that node.

```yaml
peer:
  federation_id: lab.example
  client_ca_file: /config/federation-ca.pem
  client_crl_file: /config/federation.crl.pem
  crl_max_age: 24h
  membership_directory: /var/lib/llama-catalog-proxy/federation
  audit_directory: /var/lib/llama-catalog-proxy/audit
  catalog_grant_directory: /var/lib/llama-catalog-proxy/catalog-grants
  tls_cert_file: /config/node-a-cert.pem
  tls_key_file: /config/node-a-key.pem
```

`client_ca_file` and `client_crl_file` verify incoming client chains and revocation. For each outbound
peer, `ca_file` and `crl_file` verify that peer's server chain and revocation; the local
`tls_cert_file`/`tls_key_file` pair is also sent
as the client identity (include required intermediate certificates after the leaf in the PEM
certificate file). Both sides check the exact federation/node URI and the paired certificate
fingerprint. Outbound mTLS peer transports do not reuse connections, so a membership removal or CA
rotation is checked on the next request. Static `PeerGrant` requests still require their bearer token
and independently configured scope. Dynamic catalog polling accepts bearer-free `/peer/v1/report`
requests only when the mTLS identity is currently paired and an explicit `catalog_read` grant matches
that certificate fingerprint. Dynamic inference similarly requires separate caller `route_outgoing`
and receiver `allow_incoming` grants; neither scope is implied by the other or by pairing. Remove a
static outbound URL for a node when using its dynamically discovered endpoint. Existing peers must be
provisioned with certificates before enabling mTLS on either side.

Use short-lived certificates (maximum 30 days) and current CA-signed CRL bundles. Every issuing CA in
the chain must have a signed CRL in the configured bundle; missing, untrusted, revoked, future-dated,
or stale CRLs fail closed. Use full CRLs; delta/indirect CRLs and unsupported critical CRL extensions
are rejected. The default `crl_max_age` is 24 hours; configure between one minute and 30
days to match the CA's publication cadence. Global certificate revocation requires distributing the
new CRL to every node. A local membership revoke blocks that node immediately but is not a substitute
for CA revocation at other nodes.

Certificate issuance, trust-root distribution, CRL publication, and renewal remain operator-PKI
responsibilities; the proxy does not fetch CRLs or OCSP. CA, CRL, and node certificate files are loaded
at startup/config reload; replacing file contents alone does not reload them. Freshness is checked on
each mTLS request, so an in-memory CRL fails closed after its `nextUpdate` or configured max age.
Membership and permission grants remain separate: paired identity membership is stored in the protected
`membership_directory`, while operation scopes remain explicit peer grants. The directory must
already exist with owner-only permissions; updates are serialized across local CLI and proxy
processes, and the membership record file is written with mode `0600`. Pairing state permission/owner
checks are currently supported on Linux, macOS, FreeBSD, OpenBSD, and NetBSD; other operating systems
fail closed until an equivalent ACL verifier is available. Invitations are not scope grants.

### Local audit evidence

`audit_directory` is required when `peer.federation_id` is enabled. Its owner-only store keeps local
audit events for 30 days, up to 10,000 records, 16 MiB total, and 2 MiB per day. It records
action/outcome, relevant node/target/model IDs, and OIDC issuer/subject for
verified local browser actions. Local CLI events identify the service-account UID. Remote peer actions
record the verified requester node while the human user is explicitly `unknown`; no user assertion or
browser identity is forwarded between nodes. Bearer tokens, invitation codes, settings values, raw
YAML, and inference content are never stored in audit events.

The audit UI/API is restricted to `administer_federation`. Privileged actions fail before execution if
a start record cannot be durably stored; if completion cannot be recorded after an action, the event
remains `started/unknown` and the outcome must be inspected rather than retried automatically. The log
is protected local evidence, not a signed tamper-proof export; use a separate host audit/backup system
for independent retention. If its capacity is reached, new privileged actions fail closed.

### Federation backup and recovery

`membership_directory/pairing.json` contains paired node IDs, current certificate fingerprints,
revoked fingerprints, invitation hashes, and consumed-invitation replay evidence. It does not contain
private keys, raw invitation codes, peer bearer grants, or CA keys. Treat it as protected identity
state, and back it up separately from configuration, CA/CRL files, audit records, and operation stores.
The proxy cannot detect a restore of the entire host to an old snapshot; a stale but not-yet-expired
CA-signed CRL can permit revocation delay up to the configured `crl_max_age`.

After any restore where current identity/revocation state cannot be proven:

1. Keep peer communication disabled while checking the current CA CRL and the restored member list.
2. If state freshness is uncertain, do not restore the old `pairing.json` as active trust. Have the CA
   revoke prior certificates and issue new short-lived node certificates, then distribute the current
   CRL to every node.
3. Start with an empty protected membership store and re-pair using new one-time invitations.
4. Reapply peer operation scopes separately; pairing itself never restores scopes or bearer tokens.

If the current CA CRL is absent, invalid, or stale, mTLS verification fails closed. The local CLI can
revoke and re-pair identities when OIDC is unavailable, but it cannot replace the operator CA's global
revocation action.

During an IdP outage, the web panel denies login rather than falling back to anonymous access; the
local service-account CLI remains available for membership revoke/re-pair. During a CA or network
outage, existing peer traffic works only while its certificate and loaded CRLs remain valid; after
`nextUpdate`/`crl_max_age`, federation traffic fails closed and report receipts become stale. Local
catalog and non-peer operations remain independent of a central dashboard or CRL-distribution service.

### Certificate renewal

Renew before the current 30-day certificate expires. There is no implicit overlap: a local invitation
re-pair replaces the member fingerprint and records the old fingerprint as revoked. For a multi-node
federation, update each owner's member record with the new certificate and distribute/reload the CA CRL
on every node. Until the updated CRL is loaded, stale-policy acceptance is bounded by `crl_max_age`;
there is no automatic global propagation.

1. Generate a fresh key and CSR on the node, have the operator CA issue a certificate with the same
   federation/node URI, and prepare the updated CRL.
2. Create a new one-time invitation for that existing node ID on each peer that stores its identity.
3. Install the new cert/key on the renewing node, update its CRL files, and restart/reload it. Use the
   CLI `peer join` or the OIDC pairing panel to consume each invitation; peer grants/scopes remain
   unchanged.
4. Install and reload the current CRL and certificate bundle on every node. Verify the policy panel is
   fresh and the report receipt status recovers before removing old backup material.

An authorized local `administer_federation` user can create an invitation and join another node from
the `/admin/` pairing panel. Before joining from the panel, list the issuing node's HTTPS origin in
the joining node's server configuration:

```yaml
peer:
  pairing_join_origins:
    - https://node-a.example
```

The panel accepts only those configured destinations (an optional trailing slash is ignored).
An omitted or empty list disables web joins, but not invitation creation. The CLI does not require
this allowlist and uses the same pairing store. Run it as the proxy service account, which must own
the protected membership directory:

```sh
llama-catalog-proxy peer invite create --config /config/config.yaml --node-id node-b
llama-catalog-proxy peer invite list --config /config/config.yaml
llama-catalog-proxy peer join --config /config/config.yaml --url https://node-a.example \
  --expected-node-id node-a \
  --invitation-file /run/private/node-a-invitation.json
llama-catalog-proxy peer members list --config /config/config.yaml
llama-catalog-proxy peer members revoke --config /config/config.yaml --node-id node-b
```

When mDNS discovery is enabled, `peer join` can select a candidate without manually supplying its
URL:

```sh
llama-catalog-proxy peer join --config /config/config.yaml --discover-node-id node-a \
  --invitation-file /run/private/node-a-invitation.json
```

The selected node ID is checked against the peer's TLS identity before the invitation is submitted.
Pairing still requires the protected one-time invitation; truncated candidate lists and ambiguous
multiple endpoints for one node ID are rejected. This avoids manually supplying the URL for pairing,
but it does not itself grant catalog access. With `catalog_grant_directory` configured, report clients
are created automatically for currently paired identities that have an active, separate catalog-read
grant; endpoints are resolved from mDNS during refresh. A static outbound `PeerGrant` takes precedence
for its node. Dynamic inference uses separate per-peer caller and receiver grants, as documented below;
catalog access never implies inference permission.

For dynamic inference, grant caller routing only after catalog read is approved on that caller:

```sh
llama-catalog-proxy peer catalog-grants create --node-id node-b --acknowledge
llama-catalog-proxy peer inference-grants create --node-id node-b --route-outgoing --acknowledge
```

On `node-b`, separately approve inbound inference from `node-a`:

```sh
llama-catalog-proxy peer inference-grants create --node-id node-a --allow-incoming --acknowledge
```

`route_outgoing` and `allow_incoming` are distinct permissions bound to the paired certificate
fingerprint. Both sides must grant their direction; catalog-read access is still separate. Revoking
catalog read also disables outbound dynamic inference because model discovery depends on its reports;
it does not revoke inbound inference. Diagnostics, settings, and path-service operations require their
own grants.

The `invite create` JSON output includes the one-time secret code. Transfer it out of band and keep
any temporary file owner-only (for example, create it under `umask 077`). `peer join` accepts either
that JSON output or the raw code from a protected regular file; it deliberately does not accept the
secret as a command-line argument. The web panel displays a new code only once, and its invitation
list never returns codes. Create and join actions use the same pairing store as the CLI, so concurrent
operations serialize safely across processes.

Both interfaces perform only identity pairing. They do not add catalog-read or other operation grants.
An administrator must explicitly grant catalog read through the admin panel or `peer catalog-grants`
CLI; static bearer-token and operation grants remain separately configured. Revoking a paired member
immediately blocks it locally and prevents re-pairing with that revoked certificate. Also revoke any
active catalog-read grant, remove or rotate configured static bearer grants, and follow the operator
PKI's revocation procedure for complete credential revocation.

Readiness evidence is limited to the last completed local `/ready` health check for each explicitly
owned backend. Peer reports do not trigger probes. Before a check the status is `unknown`; canceled
checks do not replace the last completed observation. Results include their actual observation time
and source freshness, using `models_cache_ttl` as the freshness window. A ready health endpoint
does not prove a specific model is loaded or inference will succeed. Omitted readiness evidence
means it was not supplied, not that the backend is healthy.
The default `models_cache_ttl` is five minutes. For peer-published models, the owner applies this
TTL to the catalog observation and includes its absolute `expires_at` in each report. The owner
refreshes explicitly owned backend catalogs in the background before that TTL expires; this does
not depend on frontend `/v1/models` traffic. A receiver requires both a fresh peer report and
unexpired owner evidence. Report freshness is a separate 30-second connectivity check and does not
reset or extend the owner catalog's expiration. Failed owner catalog refreshes never extend it.

For outbound exchange, configure the peer's HTTPS origin and its separately issued token. The optional
`ca_file` replaces system trust with the specified PEM trust roots; without it, system roots are
used. Peers are polled at most every 15 seconds with bounded concurrency, a five-second timeout, no
redirects, and report validation against the configured node ID. Polling runs more frequently when
needed to refresh catalogs configured with a shorter `models_cache_ttl`. A failed refresh preserves
the last report as stale for at most 15 minutes. A successful receipt is considered fresh for
30 seconds; receipt freshness does not prove that the underlying source observations are fresh or
extend their owner-provided expiration. Peer outages do not change local readiness.

The public catalog includes peer models under `@peer/<node>/<backend>/<model>`, where backend and
local model ID are unpadded base64url encodings of their exact UTF-8 bytes. Identical names and
aliases on different owners/backends remain distinct. The `@peer/` namespace is reserved when the
peer listener is enabled at startup; conflicting local catalog IDs are omitted. Stale peer models
remain visible but cannot be routed. Routing also requires a fresh receipt, fresh source
observation, matching owner incarnation/configuration, and the explicit inference grants. A stale,
missing, malformed, or disabled peer never falls back to the default backend. Source clocks must be
sufficiently synchronized; future timestamps are treated as unknown for routing.

Peer inference supports JSON and multipart requests on the proxy's known inference POST paths,
without query parameters. Incoming client authorization and arbitrary headers are not sent to the
peer; the owner applies its own backend credentials. `X-Forwarded-For` and `X-Real-IP` are preserved
for llama-swap's activity `Source/Caller` and in-flight `Address` attribution. When neither address
header is provided, the requesting proxy supplies the connection address as `X-Forwarded-For`.
Forwarded addresses are informational and may be inaccurate; they do not identify or authorize a
caller. Multipart part headers other than
`Content-Disposition` and `Content-Type` are stripped for peer traffic. When peer routing is
enabled, multipart inference bodies are buffered for model inspection and limited by
`max_json_request_body_bytes`; local multipart requests within that limit remain unchanged.
Encoded or unsupported inference content types are rejected, and malformed JSON inference requests
return an error rather than falling back.

With `diagnostics_api_key` configured, `GET /diagnostics/v1/peers` lists peer receipt statuses;
`?node_id=node-b` includes that peer's bounded report and source evidence. These endpoints use the
existing diagnostics bearer token, not a peer token. Reloading configuration clears peer snapshots,
then starts new refreshes; removed peers disappear immediately.

Remote settings delegation requires an operator-owned target. An owner associates an explicitly
owned backend with a local management target using `peer.management_targets`, for example
`management_targets: {local: llama-swap}`. This binding is not evidence that the backend actually uses
that target. `manage_settings: true` is a coarse per-peer ceiling; mTLS peers additionally require an
unexpired owner grant bound to the paired requester, backend, target, and explicit model IDs. Grants
never enable inference, path services, trust changes, or arbitrary commands. Legacy bearer-only peers
retain the existing static scope behavior during migration. The outgoing `route_management: true`
setting only allows the requester node to contact that configured peer; it does not grant owner-side
permission.

The owner grants targeted model settings through the local OIDC administrator panel/API or the local
service-account CLI. Browser initiation requires the `configure_models` role; a non-browser caller
continues to use `management.delegation_token`. Grant creation/revocation requires the local
`administer_federation` role or service-account access. The existing 023 proposal/review/apply flow
remains authoritative, and both the static `ManageSettings` ceiling and current durable target/model
grant are checked on each proposal, recovery, apply, and operation lookup. Revocation blocks the next
authorization check; it does not cancel a write already past its authorization boundary.

The CLI grant commands are `peer grants create`, `peer grants list`, and `peer grants revoke`. The
create command requires a requester node, locally owned backend, comma-separated model IDs, and a
bounded expiry. A confirmation flag is required because this changes owner authorization.

For owner-side delegated management, `management.delegated_store` uses separate protected directories:

```yaml
management:
  delegated_store:
    proposal_directory: /state/delegated-proposals
    operation_directory: /state/delegated-operations
    grant_directory: /state/delegated-grants
```

An existing `backup_store` is required. The proposal, operation, grant, and backup directories must
already exist, be owned by the proxy user with no group/world permissions, and be separate from each
other, targets, ingestion data, and benchmark reports. In mTLS mode, `grant_directory` is required
when any peer has `manage_settings: true`. Preserve each store and the operation-store clock marker
across restarts. Stale backup restoration is not auto-detected; follow the credential-recovery
procedure above and re-issue identities if current state is uncertain. The protected owner-grant and
model-target store operations are currently supported on Linux; other platforms fail closed.

Adding the listener, changing its address or node ID requires a restart. Scope ceilings, target
bindings, CRL max age, and TLS/CRL paths reload with the configuration; removing `peer` disables peer
access. Certificate/CRL contents are reloaded when the configuration file changes. Invalid reloads
retain the previous configuration. The client listener reserves `/peer` while the peer listener is
configured at startup, preventing peer credentials sent to that path from reaching a backend.

## Experimental local configuration read and preview

The default-off management listener is independent of federation and diagnostics. Configure a
separate HTTPS address, certificate, and randomly generated preview token in protected runtime
configuration:

```yaml
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: /config/management-cert.pem
  tls_key_file: /config/management-key.pem
  preview_token: "<unique-random-token-at-least-32-characters>"
  targets:
    local:
      directory: /llama-swap-config
      filename: config.yaml
```

For read-only use, mount the llama-swap configuration **directory** read-only at the configured path;
the proxy process needs read permission. File access is currently supported only on Linux. Target IDs are configured
aliases, not client-supplied paths; filenames cannot be paths or symlinks. Use a token distinct from
diagnostics, peer, and configured backend credentials, and use a certificate trusted by clients.
Protect access to the listener and never send the token over plaintext.

Both endpoints use `Authorization: Bearer <token>` and `Content-Type: application/json`. To read
explicitly configured settings, send `POST /management/v1/settings`:

```json
{"target_id":"local","model_id":"example"}
```

The response contains the target and model IDs, the source SHA-256 revision, and a `settings`
object with only flags explicitly present in the command. Numbers and booleans are JSON values;
absent settings are omitted, not inferred from defaults. These are configuration values, not
evidence of effective runtime settings.

To preview changes, send `POST /management/v1/preview`:

```json
{"target_id":"local","model_id":"example","settings":{"context_size":4096,"fit":true}}
```

Supported settings are `context_size`, `fit`, `fit_context`, `fit_target`, and `cache_ram`.
Each requested flag must already exist in a recognized, single-line direct `llama-server` command;
block scalars, unknown arguments, shell syntax, and other ambiguous forms are rejected. The response
contains only target/model IDs, a SHA-256 source revision, before/after setting values, and
`validation: "yaml_syntax_and_supported_command_only"`. It does not expose the configuration or
command, execute llama-swap validation, verify runtime settings, save a proposal, or write a file.
Neither endpoint replaces the file.
Absolute GGUF model paths may be single-quoted when they use the supported path characters.
Supported numeric GPU-layer flags are preserved, but are not editable through these settings.

### Experimental durable proposals

Proposal storage is separately opt-in and does not permit configuration writes. Add this to the
`management` section to enable it:

```yaml
proposal_store:
  directory: /proposal-state
  token: "<unique-proposal-token-at-least-32-characters>"
```

Mount a dedicated persistent directory at that path. It must already exist, be owned by the proxy's
effective user, have no group or world permissions (for example `0700`), and be separate from all
configured target and backup directories. This feature currently supports Linux only. The store must not be
modified by unrelated processes; its advisory locking coordinates only cooperating proxy instances.
The proposal token must differ from the preview, diagnostics, peer, and backend credentials. The
preview credential cannot create, list, or delete proposals, and the proposal credential does not
grant access to the settings or preview endpoints.

Using the proposal token, `POST /management/v1/proposals` accepts the same target, model, and
settings as preview plus its `revision`:

```json
{"target_id":"local","model_id":"example","revision":"<preview-revision>","settings":{"context_size":4096}}
```

The source observed during creation must match that revision or the request returns 409. The saved record binds
the target alias and configured directory/filename fingerprint, model, requested settings, source
revision, and redacted changes; it contains no source or candidate configuration. `GET
/management/v1/proposals` lists IDs, `GET /management/v1/proposals/{id}` retrieves one, and `DELETE
/management/v1/proposals/{id}` explicitly deletes it. Proposals remain across restarts until deleted.
The store holds at most 256 records; when full, it rejects new proposals rather than evicting any.
Store errors are reported explicitly; after a failed or interrupted write, list records before
retrying because the outcome may be uncertain. A malformed record can be deleted by its listed ID.

A saved proposal is not permission to apply or proof of effective runtime settings.
`POST /management/v1/proposals/{id}/verify` (with the proposal
token and no request body) rechecks its configured target, source revision, and reviewed changes.
It returns `status: "matched_when_checked"` or 409 when the target, revision, or review no longer
matches. This is a point-in-time check, not an apply authorization or guarantee: external writers
are not locked and may change the source immediately afterward.

Proposal creation, retrieval, deletion, and verification never replace the target file.

### Experimental new-entry review

With Linux ingestion and a proposal store configured, use the **preview token** to send
`POST /management/v1/creation/preview` with a successful, durable ingestion record:

```json
{"target_id":"local","model_id":"new-model","artifact_record_id":"<ingestion-record-id>","executable":"llama-server","settings":{"context_size":4096},"gpu_layers":20}
```

Alternatively, replace `executable`, `settings`, and `gpu_layers` with
`"template_id":"existing-model"` to copy a restricted supported command and device environment
from an existing entry. Explicit mode may also supply `device_environment` as an array of
supported device visibility assignments. Unsupported options, template fields, and environments
are rejected, rather than silently discarded. No performance settings are supplied implicitly.
Only a nonempty block-style `models` mapping is supported. The proxy derives the model path from
the configured ingestion directory and record; the operator must mount the same path for the
backend and arrange access. A matching path does not prove matching mounts or access.

The response contains a redacted `preview`, an `artifact` observation, and a `review_revision`.
Inspect the requested source and command options before using the **proposal token** to send the
same request to `POST /management/v1/proposals/creation`, adding the three values returned above:

```json
{"target_id":"local","model_id":"new-model","artifact_record_id":"<ingestion-record-id>","executable":"llama-server","settings":{"context_size":4096},"gpu_layers":20,"revision":"<preview.revision>","artifact_revision":"<artifact.revision>","review_revision":"<review_revision>"}
```

The server recomputes the review; changed inputs, configured target binding, configuration or
artifact evidence return 409.
The review revision is a change detector, not proof that a person reviewed it. An artifact
observation is not proof of integrity, correspondence to the original download, backend access,
or loadability. The private proposal stores the explicitly selected executable path; responses do
not expose it or the generated command. Proposal-store errors may leave an uncertain creation
outcome; inspect the store before retrying.

The protected apply route below uses the **write token** and all acknowledgements. It rechecks the
proposal, target, and recorded artifact, and creates a backup before replacing the target. Advisory
locks coordinate cooperating processes only; the exclusive edit window must exclude other writers,
including writers to the model directory. Apply does not establish catalog exposure or model state.

After applying and allowing the backend to reload, the **proposal token** can request a separate
catalog check with `POST /management/v1/proposals/{id}/catalog`:

```json
{"backend_id":"local"}
```

The proxy derives the public ID from the selected configured backend's prefix and the proposal's
model ID, then checks the normal aggregated catalog, including peers. This explicitly requested
check may refresh backend `/v1/models` catalogs and, where configured, Ollama `/api/show`
enrichment. It uses configured backend credentials, not the management credential; backend
behavior outside the proxy's control may vary. The proxy does not request a model load, inference,
or benchmark. A default catalog failure returns an error, not an absent result.

`catalog` is `listed_when_checked` or `not_listed_when_checked` in the proxy response at
`observed_at`. Normal catalog caching, stale fallback, optional backend failures, earlier
client-supplied backend credentials, and duplicate public IDs can affect that response. Both
`upstream_freshness` and `selected_backend_relation` are `unknown`: choosing a backend does not
prove that it serves the management target or owns the matching ID. This check also does not prove
the proposal was applied or the backend reloaded. Loadability, loaded state, and effective settings
are reported as unknown.

### Experimental backup recovery

Backup recovery is separately opt-in. Add this to `management`:

```yaml
backup_store:
  directory: /backup-state
  token: "<unique-backup-token-at-least-32-characters>"
  max_backups: 8
```

The limit must be explicitly set from 1 to 32. The dedicated persistent directory must already
exist, be owned by the proxy's effective user, and have no group or world permissions. Keep it
separate from proposal and target directories, including bind-mount aliases that path checks may
not detect. Do not let unrelated processes change the store or its parent paths. Records are
unencrypted owner-only files and may contain secrets; protect the backing storage accordingly.
This feature currently supports Linux only. Recovery endpoints do not create backups; the configured
limit governs managed writes.

The backup token must differ from preview, proposal, diagnostics, peer, and backend credentials. It
grants metadata access and **irreversible deletion of backups**, not
permission to apply or roll back configuration. `GET /management/v1/backups` returns IDs and
metadata for valid records; an unreadable or malformed record is marked `unreadable_or_invalid`
without returning its contents. `GET /management/v1/backups/{id}` returns metadata for a valid
record. `DELETE /management/v1/backups/{id}` explicitly removes that record, including an
unreadable record or a symlink entry, without following a symlink. No endpoint returns backup
contents. A valid record does not prove that a rollback is currently possible.

The list accepts up to 33 entries, including a possible extra record after an interrupted write.
Unexpected filenames, more entries, or unsafe directories fail closed. An operator must inspect and
recover those cases directly in the protected store; never expose or copy its sensitive contents
to logs. After an unsuccessful deletion, the outcome may be uncertain: list or inspect the store
before retrying. Backup recovery operations never modify the target configuration.

### Experimental reviewed apply and rollback

Writes are disabled unless both stores above and a fourth, distinct credential are configured in
`management`:

```yaml
write_token: "<unique-write-token-at-least-32-characters>"
```

The write token must differ from the preview, proposal, backup, diagnostics, peer, and backend
credentials. It authorizes apply and rollback, but not proposal or backup management. Grant it only
to trusted operators. Mount the target **directory** read-write for managed writes; a read-only
mount continues to prevent them. Proposal and backup stores must remain private, persistent, and
separate from each other and all targets. The proxy does not start, stop, or reload a model itself.
However, replacing a configuration watched by llama-swap can reload it and stop running models,
interrupting inference.

Review the proposal before using the write token to send
`POST /management/v1/proposals/{id}/apply`. Review backup metadata before using
`POST /management/v1/backups/{id}/rollback`. Each request requires `Content-Type: application/json`
and all three explicit acknowledgements:

```json
{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}
```

The exclusive edit window means other writers must be paused or take the same exclusive advisory
`flock` on the target directory through replacement. The proxy cannot enforce this for
uncooperative writers. Acknowledgements are operator assertions, not proof that review or exclusion
occurred. Apply rechecks the saved proposal, target binding, current source revision and reviewed
changes while holding the relevant locks. Rollback requires the current content to match the
backup's recorded written revision. No-op and stale operations are rejected.

Before each replacement the proxy durably saves the current content in the protected backup store.
It stages and atomically renames a same-directory file, preserving the target's owner, group, mode,
and accessible extended attributes where supported. Symlinks, hard links, special permission bits,
unsafe metadata, and unsafe stores are rejected. Attribute visibility and writability depend on the
filesystem and OS. After a durable replacement, the oldest backup is rotated if the configured limit
was reached. Rollback also creates a backup of the version it replaces; this can permit a later
reverse rollback while its revision precondition holds.

Successful requests return `status: "written"`, `durability: "confirmed"`, a `backup_id` and
`candidate_revision`. They report `observed: "unknown"` and `effective: "unknown"`: local YAML and
supported-command checks are not llama-swap validation or proof that settings became effective.
A 409 indicates a stale, mismatched, or unchanged request. Other failures may return a JSON result
with `status: "not_written"`, `"written"`, or `"unknown"` and an error code. A known replacement
with uncertain directory sync reports `"written"` and `"uncertain"` durability. A rename error
reports `"unknown"` because some filesystems cannot prove whether it took effect. Backup rotation
can fail after a confirmed write; this returns an error rather than claiming the operation fully
succeeded. Backups are retained on uncertain outcomes, possibly exceeding the limit and requiring
explicit recovery before another write.

In an isolated test of llama-swap revision `85cbb9b`, a running fake model retained its original
startup argument after an apply with config watching disabled. With `-watch-config` enabled, apply
and rollback stopped the running fake process; a subsequent model request started a new process
with the updated argument. The model remained visible in the catalog while no process was running.
This does not verify actual llama-server settings or guarantee reload on other versions or filesystems.
That revision's optional watcher polls file modification time and size, so a write alone does not
guarantee that a change is detected.

The opt-in integration test requires Linux, Go, `unshare`, `ip`, permission to create user and
network namespaces, and a clean local llama-swap binary built from revision
`85cbb9b59ba71d7cbd5c73b3a9e345118f0cb217`. It starts the reference binary and a fake model only
inside a private network namespace:

```sh
LLAMA_SWAP_TEST_BINARY=/absolute/path/to/llama-swap go test ./internal/proxy -run '^TestLlamaSwapReloadWithManagedWrite$' -count=1
```

Do not blindly retry after an error, timeout, lost response, or process interruption: inspect the
target revision and backup metadata in the protected environment first. `backup_id` on an error
identifies an attempted backup whose existence or durability may itself be uncertain. A failed
temporary-file cleanup requires inspecting the target directory; a temporary file can contain
sensitive configuration. The API does not provide exactly-once execution or prove that llama-swap
accepted or loaded the written file.

Starting or changing the listener address requires a restart. Target policy, token, and TLS files
reload with the proxy configuration; removing `management` disables access unless ingestion is
enabled, in which case a restart is required. An invalid reload keeps the previous policy. The client
listener reserves `/management` when management was enabled at startup; the diagnostics API remains
read-only.

## Experimental local GGUF downloads

On Linux, optionally add this section under `management` to enable the HTTPS ingestion API:

```yaml
  ingestion:
    directory: /models
    record_directory: /ingestion-records
    max_records: 100
    token: "<unique-random-ingestion-token-at-least-32-characters>"
    # huggingface_token: "<optional-distinct-hugging-face-read-token>"
    max_bytes: 107374182400
    max_directory_bytes: 1099511627776
    min_free_bytes: 107374182400
    timeout: 4h
    file_mode: "0640"
```

Mount an existing, proxy-user-owned model directory read-write. It must not be a symlink or
group/world writable, and must be separate from management targets/stores and benchmark reports,
including resolved symlink aliases. Exclude uncooperative writers while ingestion runs. The backend
needs directory traversal and file read access; choose `"0600"` or `"0640"` and configure ownership
and group access accordingly. `max_bytes` limits each artifact. The required positive
`max_directory_bytes` limits the sum of logical regular-file sizes in the model directory and its
subdirectories, including hidden files and the staged download. Hard links are counted once per
directory entry; symlinks are not followed. Nested filesystems, unreadable entries, excessive depth
(over 64), or more than 100,000 entries cause the job to fail closed. Replacement must fit the old
and staged files simultaneously. This is not a filesystem quota: metadata, allocated blocks,
external hard links and outside writers are not controlled. The required positive `min_free_bytes`
reserves filesystem space available to the proxy user, checked before and during staging and
after synchronization. Existing ingestion configurations must add both settings before restart.
Other processes can consume space between checks, and
neither setting deletes files or replaces filesystem quotas and monitoring. Adoption is read-only
and does not enforce these download limits. Credentials belong in protected runtime configuration,
must be distinct from other configured credentials, and must never be placed in URLs or request
bodies.

Mount a separate existing persistent `record_directory` read-write. It must be owned by the proxy
user, private (mode `0700`), and separate from the model directory, management targets/stores, and
benchmark reports, including symlink aliases. Only one proxy process can own it at a time. The
filesystem must support atomic rename without replacement and file/directory synchronization.
`max_records` must be between 1 and 256.

Use the ingestion token as `Authorization: Bearer <token>` on the management HTTPS listener.
`POST /management/v1/ingestion/jobs` requires JSON, an explicit existing-file policy, and the
corresponding authorization. To download only when the destination does not exist:

```json
{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}
```

Alternatively, `source` can supply a supported Hugging Face `url` and, when needed, `revision`.
Only single files and single-component revisions are supported; nested and split artifacts are
rejected. Branches and tags may change and are not resolved commit evidence. At most one job runs
at a time. Use `GET /management/v1/ingestion/jobs/current` or
`GET /management/v1/ingestion/jobs/{id}` for status, and an empty-body
`POST /management/v1/ingestion/jobs/{id}/cancel` to request cancellation. Browser origins, when
present, must match the HTTPS management origin; query strings are not accepted.

To adopt an existing file without downloading or changing it:

```json
{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}
```

Adoption accepts only a safe, owned, single-link regular file in the configured directory. It
reads the entire file to calculate SHA-256, applies the per-file size and job timeout, and performs
bounded GGUF metadata inspection. Its record reports `source.kind: "local"`,
`publication: "existing"`, and `durability: "not_applicable"`; it does not assert that the file
came from Hugging Face or that its hash or integrity was verified. The file can then be used in the
reviewed new-entry workflow. Exclude uncooperative writers while it is inspected.

To explicitly replace an existing file with a downloaded artifact:

```json
{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"replace","allow_replacement":true,"acknowledge_exclusive_window":true}
```

Replacement requires an existing safe, owned, single-link regular file. It stages and inspects a
complete new artifact, rechecks the old file, and atomically replaces it; it does **not** create a
backup of the old artifact. A running backend may retain the old inode or behave differently; the
proxy cannot establish whether the model is loaded or stop outside writers. The exclusive window
must exclude uncooperative writers for the entire job. Both old and staged files occupy disk
space until replacement. Missing destinations are not silently treated as new downloads.

While a job runs, its publication state is unknown; after completion, inspect `publication`,
`durability`, and `error_codes` before retrying, especially for unknown publication or cleanup
failure. `not_published` describes the job, not proof that no existing file is present. The
current-job endpoint retains only the latest job in memory. Durable records are available through
`GET /management/v1/ingestion/records` and `GET /management/v1/ingestion/records/{id}`; they
contain the requested source and inspection result at that time, not evidence that the file remains
unchanged or is configured or loaded.

An unfinished record becomes `interrupted` on restart with publication still `unknown`. A job with
an unknown publication, uncertain durability, failed cleanup, or an error after publication also
requires review. New jobs are blocked until each such record has been inspected and acknowledged:
send `POST /management/v1/ingestion/records/{id}/acknowledge` with JSON
`{"acknowledge_uncertain_outcome":true}`. Acknowledgment records operator review; it does not verify
the artifact or resolve the unknown outcome. Inspect the model directory and any private
`.ingest-*.partial` files manually before acknowledging or retrying. A record-write failure after
publication is reported separately and blocks new jobs; restart and inspect the recovered record
and filesystem before acknowledgment. A crash during a record write may leave a private
`.record-*.tmp` file; the record store refuses startup until an operator examines and resolves the
unexpected file. Do not blindly delete it or assume it proves publication.

At the configured limit, the oldest completed record that needs no review or has been acknowledged
is automatically removed when a new job starts. Unacknowledged records block new jobs and are never
evicted. Acknowledged interrupted records are not automatically removed; if they fill the store, an operator must manage retention
offline after stopping the proxy. The store never deletes model artifacts. Ingestion does not
automatically register, load, or run models.

Downloads use HTTPS and a conservative allowlist of Hugging Face redirect hosts, not proxy
environment variables or native Xet reconstruction. Unsupported hosts, redirects, encoded or
unknown-length responses fail explicitly. SHA-256 is calculated but **not verified** against
trusted upstream evidence; bounded GGUF metadata parsing does not prove complete tensor data,
integrity, or loadability. Enabling, disabling, or changing ingestion configuration, including its
credentials and limits, requires a restart; an ordinary configuration reload preserves the current
job.

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

Ollama implements a subset of OpenAI request fields. The proxy forwards those fields unchanged;
check the backend version's documentation for request compatibility.

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

models_cache_ttl: 5m
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

## Diagnostics and observability

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

## Controlled synthetic benchmark runs

Build the separate benchmark command:

```console
go build -o ./bin/llama-catalog-proxy-benchmark ./cmd/llama-catalog-proxy-benchmark
```

The runner sends inference requests, so every run requires the explicit
`--allow-active-inference-interference` opt-in. The proxy API does not expose active inference state;
use the flag only when you accept possible interference with other users. The runner never loads or
unloads models, restarts services, changes configuration, or tunes settings. It accepts only a
loopback/private-network IP endpoint, does not follow redirects, runs at concurrency 1, and requires
the requested model to be uniquely observed ready with lifecycle evidence no older than five minutes.
Configure the protected diagnostics API and request history first. Enable both
`request_history: true` and
`request_observation: true` to collect proxy timing observations; missing observations remain
unknown. Supply the dedicated diagnostics key through the environment, not a command-line argument:

```console
export LLAMA_CATALOG_DIAGNOSTICS_URL=http://127.0.0.1:9292
read -rsp 'Diagnostics API key: ' LLAMA_CATALOG_DIAGNOSTICS_API_KEY
echo
export LLAMA_CATALOG_DIAGNOSTICS_API_KEY
./bin/llama-catalog-proxy-benchmark run \
  --backend default \
  --model your-public-model-id \
  --repetitions 2 \
  --time-budget 4m \
  --token-budget 4096 \
  --cache-state unknown \
  --allow-active-inference-interference
```

Each run uses versioned synthetic fixtures for a tool round trip, low/high reasoning controls,
literal `</think>` text, system-prompt defaults, and fenced examples. Limits are three repetitions,
128 output tokens per request, 4,096 reserved output tokens, and five minutes; cancellation saves
a partial report. Reports and comparisons are written under the Git-ignored `benchmark-runs/`
directory with owner-only permissions. They store settings, byte counts, request IDs, bounded
measurements, and correctness outcomes, never request/response bodies or reasoning text.
Comparisons of partial or canceled runs include per-workload recorded and expected sample counts;
missing workloads and observations remain unmeasured rather than being treated as successes.

Compare two saved run IDs with:

```console
./bin/llama-catalog-proxy-benchmark compare \
  --baseline 'bench-<baseline-id>' \
  --candidate 'bench-<candidate-id>'
```

### Experimental benchmark web UI

The optional UI uses the existing HTTP listener at `/benchmark/` and is independently protected
from diagnostics and management. It is disabled unless `benchmark` is configured:

```yaml
diagnostics_api_key: "<distinct-diagnostics-key>"
benchmark:
  token: "<unique-random-benchmark-token-at-least-32-characters>"
  proxy_origin: "http://127.0.0.1:8080"
  report_directory: /benchmark-runs
```

`proxy_origin` must be the configured proxy's loopback or private-IP HTTP(S) origin, reachable
from inside the container. Diagnostics must be enabled and the chosen backend/model must have
fresh, unique readiness evidence; if that evidence is unavailable, the run fails closed. The
proxy origin is fixed by the operator, not selected by the browser. The report directory must
already exist, be owned by the proxy user, have no group/world permissions, and be separate from
management targets and stores. Mount a dedicated persistent directory read-write there. The UI
currently supports Linux only.

**Secure the listener before use.** The proxy listener itself is HTTP: use HTTPS termination at
Traefik or another trusted reverse proxy and restrict direct untrusted access to the HTTP listener
and its network. A browser on localhost may also use loopback directly. Never send tokens over an
untrusted plaintext connection; do not rely on forwarded headers as proof of TLS. The benchmark
token must differ from diagnostics, management, peer, and backend credentials. The UI keeps it
only in page memory, not browser storage or URLs. The UI assets contain no credentials; the API
requires the dedicated bearer token by default. Setting `benchmark.public_read_only: true`
exposes the saved-run summary listing (`GET /benchmark/v1/runs`) without authentication; run
details, job actions, and comparisons still require the token. Leave it off unless those summaries
are safe to share. Changes to benchmark configuration or its diagnostics key,
including disablement and token rotation, require a proxy restart.

Opening or unlocking the UI does not start inference. Enter a backend and public model ID, review
the limits, explicitly acknowledge possible interference, and select **Start benchmark**. At most
one job runs per proxy process. Canceling is asynchronous and may produce a partial report. Job
status retains only the latest job in memory and is lost on restart; reports remain in the mounted
directory. A failed or interrupted report write has an unknown outcome and exposes the attempted
run ID for inspection. After a lost response or interruption, inspect current job status and report
history before retrying; execution is not exactly-once. Listing is limited to 100 reports selected
by file modification time and fails explicitly if the directory exceeds 2,048 entries. Run inspection
shows per-sample elapsed time and diagnostic outcomes; unavailable observations remain unknown.

The authenticated API provides `POST /benchmark/v1/jobs`, `GET /benchmark/v1/jobs/current`,
`GET /benchmark/v1/jobs/{id}`, `POST /benchmark/v1/jobs/{id}/cancel`,
`GET /benchmark/v1/runs`, `GET /benchmark/v1/runs/{id}`, and
`POST /benchmark/v1/compare`. The job request requires `backend`, `model`, and
`"allow_interference": true`; optional limits remain bounded by the existing runner.
Comparisons preserve missing sample counts and label unsupported evidence. Page load does not start
a benchmark, and the UI does not explicitly load models or change proxy configuration. If model
readiness changes after preflight, an inference request may nevertheless cause backend-side loading.

Comparisons keep correctness separate from performance and report repeated-run variation,
incomplete/truncated outcomes, and requested-setting confounders. Proxy timing is retrieved by the
exact returned proxy request ID after each inference request. Model-level aggregate draft acceptance
and approximate shared-device memory are contextual snapshots, not per-request attribution;
per-round acceptance,
active inference state, and applied runtime sampling/draft configuration are unsupported or unknown.
These synthetic checks are regression evidence for the tested behavior, not a general model-quality
score or proof that a configuration is faster.

## Experimental administrator web UI

The optional administrator UI is available at `/admin/` when `management_ui` is configured. It
authenticates each local user through an OIDC provider. The `view` role sees the catalog, peer receipt
summary, bounded request aggregates, and recent request activity. The activity panel is manually
refreshed from in-memory `request_history`; it is not a process-log viewer. Users with
`administer_federation` can create/join invitations, revoke paired identities, and approve/revoke
target-scoped settings grants. Pairing changes only identity membership; it does not grant operation
scopes. No general YAML editor or remote shell is available.

Register a confidential OIDC client with authorization-code flow and PKCE S256. Its exact callback URI
must match `redirect_url`:

```yaml
management_ui:
  issuer_url: "https://identity.example/"
  client_id: "catalog-proxy"
  client_secret: "<secret-from-protected-runtime-config>"
  redirect_url: "https://catalog.example/admin/auth/callback"
  groups_claim: groups
  role_groups:
    view: ["catalog-viewers"]
    administer_federation: ["federation-admins"]
    configure_models: ["model-configurators"]
```

`issuer_url` and `redirect_url` must use HTTPS. Store `client_secret` in protected runtime
configuration, keep it distinct from all machine credentials, and do not commit it. Map only groups
that should have local UI access. Supported local roles are `view`, `operate`, `configure_models`, and
`administer_federation`. The latter enables local pairing and grant administration but does not grant
peer operation scopes. `configure_models` authorizes same-origin typed BFF proposal/recovery/apply and
operation-status APIs; every remote mutation still requires an owner grant. The admin page exposes
typed settings only, never raw YAML. `operate` has no UI actions yet. The default OIDC scopes are
`openid profile email`; configure `groups_claim` when the provider uses a different claim name.

The default session lifetime is eight hours absolute and 30 minutes idle; configured absolute lifetime
must be 5 minutes to 24 hours and idle lifetime must be at least one minute and no greater than the
absolute lifetime. Sessions and login transactions are bounded and held only in process memory, so a
restart clears them. The session cookie is Secure, HttpOnly, SameSite=Lax, and session-bound. Changing
`management_ui` authentication settings requires a restart. Provider failure returns an error rather
than allowing anonymous access.

The application enforces the session and role checks itself; it does not trust forwarded-user headers.
Because cookies and the callback require HTTPS, serve `/admin/` over a trusted HTTPS origin and
restrict direct access to the plain-HTTP listener to trusted networks. Reverse-proxy authentication
alone does not protect a directly reachable listener. Existing machine APIs continue to use their
separate bearer credentials; those credentials must not be copied into browser code or storage.

## Agent-facing MCP diagnostics

Build the separate stdio MCP adapter:

```console
go build -o ./bin/llama-catalog-proxy-mcp ./cmd/llama-catalog-proxy-mcp
```

The adapter needs the proxy's protected diagnostics API enabled with a dedicated
`diagnostics_api_key`. Start it with the API origin and key supplied through its process environment;
the key is never a tool argument. Inject the key from a local secret manager or enter it without
echoing it, and do not save it in source control:

```console
export LLAMA_CATALOG_DIAGNOSTICS_URL=http://127.0.0.1:9292
read -rsp 'Diagnostics API key: ' LLAMA_CATALOG_DIAGNOSTICS_API_KEY
echo
export LLAMA_CATALOG_DIAGNOSTICS_API_KEY
./bin/llama-catalog-proxy-mcp
```

Configure an MCP client to launch that executable with those two environment variables. Standard
output is reserved for MCP stdio; diagnostics and errors are returned as bounded structured data.
The adapter makes only authenticated GET requests to fixed `/diagnostics/v1` routes and does not
provide generic HTTP, shell, filesystem, prompt/capture, or lifecycle tools. The protected API
continues to enforce configured backend and catalog model identities. Request-history tools also
require `request_history: true`.

The four tools are `get_model_snapshot`, `get_recent_failures`, `get_performance_summary`, and
`get_request_diagnostics`. Example calls:

```text
get_model_snapshot(backend="default", model="my-model", limit=20)
get_recent_failures(limit=50)
get_performance_summary()
get_request_diagnostics(request_id="req-123")
```

Time windows are capped at 24 hours, page sizes and responses are bounded, and continuation uses
opaque cursors. Results include provenance, freshness, uncertainty, and correlation limitations.
Diagnostic strings are untrusted observations, not instructions. Model lifecycle and GPU associations
remain current-state or approximate as labeled; upstream request identity is unsupported.

## Configuration reference

`max_json_request_body_bytes` limits uncompressed JSON bodies that must be buffered for model-aware
routing. Oversized JSON requests return `413 Request Entity Too Large`. Explicitly non-JSON,
compressed, and path-routed request bodies remain streaming passthroughs when peer routing is
disabled; see the peer federation section for its inspection and media-type restrictions.

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
sets the HTTP server's `ReadHeaderTimeout` at startup. It is not an inference wall-clock deadline: proxied
generation, SSE flushing, and WebSocket lifetimes are not bounded by it. When `health_timeout` or
`catalog_timeout` are omitted, they inherit `request_timeout`, preserving existing configurations.
Set `health_timeout` short so an offline homelab backend does not delay readiness, and set
`catalog_timeout` long enough for catalog discovery plus optional Ollama `/api/show` enrichment.

`path_routes` sends requests to a backend by URL path before request-body model routing. Paths must
be canonical absolute paths without a trailing slash. A route matches its exact path and
slash-delimited descendants; overlapping routes use the longest match. Local `GET /v1/models`,
`/health`, and `/ready` handlers take precedence. Path-route changes participate in configuration
hot reload.

## ComfyUI integration

When the GPU backend is llama-swap, the `/comfyui` example forwards the path unchanged, including
query strings, encoded path segments, backend credentials, and WebSocket upgrades. That optional
llama-swap configuration can define the reserved local model ID `comfyui_auto`; llama-swap then owns
the redirect, cold-start restrictions, path stripping, and ComfyUI compatibility settings.

An owner may optionally configure `peer.comfyui_result_read` with the persistent ComfyUI service
origin, separate from the llama-swap backend used by `path_routes: /comfyui`:

```yaml
peer:
  comfyui_result_read:
    url: http://comfyui:8188
    image_upload_max_bytes: 16777216 # optional; default 16 MiB, maximum 64 MiB
    # api_key: keep-owner-credential-in-protected-config
  exposed_path_routes:
    comfyui: /comfyui
```

When enabled, local and authorized peer bodyless `GET` requests for `/comfyui/history`,
`/comfyui/history/<prompt_id>`, and `/comfyui/view` with an output `filename` are served directly by
that owner-configured persistent service. Status reads for `/comfyui/prompt`, `/comfyui/queue`, and
`/comfyui/system_stats` also bypass llama-swap, with no query parameters allowed. Their core `/api`
aliases (for example `/comfyui/api/queue` and `/comfyui/api/history`) are supported, as are
`/comfyui/api/jobs` and `/comfyui/api/jobs/<id>` with bounded, validated listing filters. These handlers
were verified against installed ComfyUI source; there is no core `/health` handler in that snapshot.
System stats indicate server responsiveness and device metrics, not GPU-lease readiness.

The same target serves the UI root (`/comfyui/`, with a redirect from `/comfyui`) and `index.html`
directly when the persistent origin responds to a lightweight queue probe. If the origin is stopped or
unavailable, the root falls back through the scheduler to start the service. The target also serves
`favicon.ico`, `user.css`, `materialdesignicons.min.css`, and supported static file types under
`assets/`, `scripts/`, `extensions/`, `locales/`, `fonts/`, and `templates/` directly. Named GET metadata
routes (`features`, `extensions`, `embeddings`, `models`, `models/<folder>`, `object_info`,
`object_info/<node>`, `users`, `settings`, `settings/<id>`, `workflow_templates`) and core `/api` aliases bypass
llama-swap too. Asset queries accept only bounded `v`/`t` cache keys; metadata queries are disallowed.
The installed `object_info` handler invokes plugin metadata methods, so custom nodes must be trusted;
this is not a guarantee that arbitrary plugin code cannot touch a GPU.

Saved-workflow reads use named GET `userdata`/`v2/userdata` listing routes and `userdata/<file>` for
canonical relative JSON/CSS filenames. Listing queries accept only validated directories and the
installed boolean listing options. Single-encoded nested workflow filenames are preserved for the
upstream router; traversal and double-encoded paths are rejected. These requests use the configured
owner-side backend identity, not an authenticated per-browser identity: incoming `Comfy-User`, cookies,
and authorization are not passed through. Configure any owner `Comfy-User` header in protected backend
headers. This does not implement ComfyUI multi-user account switching or end-user isolation.

Settings saves (`POST /comfyui/settings` and `/settings/<id>`) and their `/api` aliases also route
directly with a 1 MiB JSON-body limit. Whole-settings saves require a JSON object; one-setting saves
accept any valid JSON value. Workflow saves use `POST /comfyui/userdata/<file>` (or its `/api` alias)
with a 4 MiB JSON-body limit, restricted to `workflows/*.json` (including nested folders),
`comfy.templates.json`, and `comfy.keybindings.json`. Nested filenames must be URL-encoded as one route
parameter, such as `workflows%2Fexample.json`; unencoded move paths and traversal are rejected.
Only boolean `overwrite`/`full_info` query options are accepted for file saves. JSON, plain-text JSON,
and missing content types are supported to match the installed frontend; UTF-8 JSON is validated and
forwarded with the correct content type. Saves retain owner identity and peer path authorization,
never fall back to llama-swap, and are not automatically retried after errors. After a timeout,
read back the setting/file to determine its state before retrying; a write may already have completed.

GET `/comfyui/ws` (and `/comfyui/api/ws`) supports a validated WebSocket upgrade and optional bounded
`clientId`. The installed handler negotiates feature flags and reports progress, not generation.
Upgrades retain handshake headers but not caller credentials. Owner-side frontend requests, result
reads, and WebSockets have separate bounded concurrency pools; peer path-service limits still apply.
Unavailable UI requests do not fall back to llama-swap. This is an explicit core-route allowlist,
not a catch-all for custom-node APIs, account changes, arbitrary file writes, deletes, or moves;
full browser compatibility with installed plugins still requires deployment verification.

Input-image `POST /comfyui/upload/image` and `/comfyui/api/upload/image` also bypass llama-swap
automatically when this target is configured; no separate upload enable flag is required. Multipart
bodies are fully size-bounded and validated before forwarding, with no temporary files. They must
contain exactly one image file with a supported image extension/signature and may include `type=input`,
a canonical relative `subfolder`, and `overwrite=true|false|1|0`. Output/temp destinations, unknown or
duplicate fields, traversal filenames, encoded separators, and query parameters are rejected. The
size limit includes multipart overhead. Actual image decoding remains ComfyUI's responsibility.

The proxy rejects unsupported queries and non-output result-file types; caller `Authorization`/cookies
are not forwarded, while only owner-configured backend credentials may be applied. Peer reads and
uploads still require the existing `comfyui` `path_access`
grant. Generation `POST /prompt` (including its `/api` alias), queue mutations, and other
non-allowlisted paths continue through the configured
`/comfyui` llama-swap path route, so result reads neither acquire a GPU lease nor bypass scheduling
for GPU-using operations. Peer requests using ComfyUI `blake3:` filenames are denied until their
multi-user identity behavior is verified; owner-local requests may use them. This setting does not
keep the ComfyUI process/container alive, unload its models, release GPU memory, or verify `/free`.
History remains in-memory and is lost on a real ComfyUI restart; interrupted jobs are not represented
as successfully completed. This setting only provides result routing when the configured persistent
service is already running.

An owner may configure the code-only release verifier with explicit free-VRAM evidence thresholds:

```yaml
peer:
  comfyui_lease:
    release_timeout: 2m
    poll_interval: 500ms
    queue_idle_samples: 2
    minimum_free_vram_bytes:
      "cuda:0": 2147483648
    # Optional: opt these exact local backend IDs into proxy-only admission.
    # Include the backend selected by path_routes: /comfyui.
    admission_backends: [comfyui_scheduler, other_gpu_backend]
```

With `admission_backends` omitted, this remains a code-only release verifier and must only be invoked
after an external scheduler has closed admission. With it configured, eligible requests forwarded
through catalog-proxy use the in-process coordinator: initial `unknown` state is reconciled on the
first eligible request; an accepted ComfyUI prompt activates its lease; and a competing request waits
for queue drain, one `/free` request, and repeated configured VRAM observations before forwarding.
The verifier retries observations, not `/free`. Non-2xx or ambiguous first-prompt outcomes leave the
state unknown. A competing backend load failure is returned normally and does not automatically retry
the request or release operation.

The coordinator applies to requests under the `/comfyui` scheduler path and recognized llama-swap
model-dispatch routes, including model inference endpoints, model-specific GET endpoints such as
`GET /props`, and `/upstream/{model}/...`. ComfyUI scheduler-path requests acquire or reuse the lease;
the UI root is served directly without a model-state transition when its persistent origin is already
available. Recognized model-dispatch requests release the lease before forwarding. Other
traffic, including the llama-swap UI/static files, read-only status APIs, and management/control
endpoints, passes through without lease transitions. Keep the classifier aligned with the deployed
llama-swap model route table: newly added or custom model-loading routes are not automatically gated.
This is a best-effort gate for requests that pass through this proxy. It cannot stop direct llama-swap
clients, operator/management actions, startup preloads, autonomous scheduler activity, or GPU work on
other paths. It does not establish host-wide exclusivity or preserve ComfyUI's process/container.

`admission_backends` is opt-in and requires the `/comfyui` path backend to be listed. Use backend IDs
that actually represent the conflicting workloads in this deployment; configuration cannot prove
that they cover all GPU users. A release failure leaves the coordinator blocked; automatic `/free`
retry is deliberately disabled, and recovery currently requires restarting the proxy and obtaining
fresh evidence. No llama-swap or ComfyUI changes are required or made by this proxy-only mode.

Without runtime admission configured, the verifier may still be invoked explicitly, for example from
a separately approved scheduler stop hook:

```sh
llama-catalog-proxy comfyui-lease release --config /config/config.yaml --acknowledge
```

The verifier requires repeated empty `/queue` samples before `/free`, then repeated empty queue and
per-device `vram_free` threshold samples afterward. HTTP 200 alone never confirms release. Missing or
unsupported device evidence and timeouts return `state: unknown` and a non-zero exit. The standalone
command does not stop ComfyUI or wire itself into llama-swap.

When `admission_backends` is configured, the coordinator is wired into common owner-side backend
dispatch, including authorized peer inference/path forwarding. It starts in `unknown`, verifies release
before competing admission, assigns opaque lease IDs, and drains submitted HTTP requests before invoking the
queue/VRAM verifier. A successful first ComfyUI prompt response activates its lease; ambiguous starts
leave admission unknown. Competing requests reserve the released domain while being forwarded. This
process-local state is reset to unknown on proxy restart or config reload; it is not durable recovery.

With one `admission_backends` entry, startup recovery for model requests automatically checks that
backend's llama-swap `/running` endpoint. The proxy can resume admission without `/free` if two snapshots
show the same sole ready model (other than `comfyui_auto`) and the direct ComfyUI queue stays
empty across the configured samples between them. This is scheduler evidence, not proof of
VRAM ownership or protection against renders submitted outside the scheduler. An inconclusive
response or multiple admission backends falls back to the full release verifier. An active or
ambiguous ComfyUI handoff still requires the normal release checks.

An explicit ComfyUI prompt or the llama-swap UI's `GET /upstream/comfyui_auto/` can acquire the
initial lease without checking free VRAM first. These requests still go to llama-swap so it can
evict a resident model before forwarding to ComfyUI. This does not reopen admission after an
ambiguous `/free` or a failed scheduler handoff. Keep `/comfyui` pointed at the scheduler,
not at the direct ComfyUI origin, for this GPU-sharing setup.

The separate Linux private Unix-socket transport is implemented and tested but is not started by the application
and has no configuration/CLI binding. It requires an existing owner-only directory, creates a
mode-`0600` socket, rejects symlink directories and existing paths, and never binds TCP or peer routes.
The version-1 protocol has GET `/v1/lease/status` and POST `acquire`, `activate`, `release`, and
`invalidate` under `/v1/lease/`. POSTs require `application/json` with `version: 1` and, except for
acquisition, a `lease_id`. Bodies are capped at 2 KiB, mutations at eight concurrent requests, and
operations at a caller-configured bounded timeout. The successful operation's `operation_lease_id`
binds its result/receipt; the separate `lease_id` reports the current snapshot, which may have advanced.

Transport startup and shutdown force `unknown`, discard receipts/lease IDs, preserve outstanding
in-process reservations, and cancel active verification. No startup `/free` or automatic recovery
occurs. POST `/v1/lease/reconcile` returns `501` until the owner binding can establish exclusive
admission; a client-supplied assertion is not sufficient. Unexpected existing sockets are not assumed
stale or removed. Cleanup removes only the same socket inode created by this transport.

This is not a deployment-level lock: it has no durable restart recovery, deployed helper endpoint,
or enforcement over direct llama-swap clients and autonomous loads. Do not treat these tests or the
standalone release CLI as proof of a safe shared-GPU handoff. `admission_backends` enables only the
documented best-effort gate over requests mediated by catalog-proxy; persistent-container lifecycle
management is not implemented.

`comfyui_wait_timeout` optionally prevents recognized inference requests routed to the same backend
endpoint as `/comfyui` from reaching that backend while ComfyUI has queued or running jobs. Requests
routed to other backend endpoints are not delayed. It polls the configured `/comfyui` backend's
`GET /comfyui/queue` and returns HTTP 504 rather than forwarding when the timeout expires. A queue
query failure returns HTTP 502 (fail-closed). The option requires `path_routes: /comfyui` and is
disabled when omitted or set to `0`. It does not delay ComfyUI requests or non-inference requests,
and only coordinates submissions that pass through this proxy; direct ComfyUI submissions are
visible while present in its queue but cannot be tracked during the brief submission/queue update
race. Clients must retry a 504 after the render completes.

### Configuration hot reload

The proxy hashes the config file every 2 seconds and applies content changes without a container
restart, including same-size edits whose modification timestamp did not change.

- Backend URLs, prefixes, API keys, headers, and health paths take effect on the next request.
- `log_level` changes take effect immediately.
- If a new config fails to validate, the running configuration is kept and an error is logged; a
  corrected file is picked up automatically on the next poll.
- Changing the `listen` address is not applied at runtime and is logged as requiring a restart.
- The per-backend catalog cache is rebuilt on reload. The default TTL is five minutes.

For hot reload to work with Docker bind mounts, mount the config's **directory** rather than the
file. A single-file bind mount can capture the file's inode at container start, so replacing the
file (e.g. with `sed -i`) leaves the container reading a stale copy. Mounting the directory makes
the container look up `config.yaml` by name on each access:

```yaml
volumes:
  - ./config-dir/:/config:ro
```

## On-demand diagnostics

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

## Request logs

Structured request logs include a proxy-generated request ID, method, path, route kind,
selected backend ID, status, duration, and response size. The request ID is echoed back to the
client and forwarded upstream so request and error events correlate. Incoming request IDs are never
trusted or logged verbatim. Log fields omit query strings, model IDs, request bodies, and configured
credentials/secrets.
