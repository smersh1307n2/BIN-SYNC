# FldBinSyncHost.Runtime — executable reference Host

This is the repository's executable Host reference and process-level integration target for BIN-SYNC + FLD. It is not an in-process mock: it owns a real Node/WAL and Metadata StorageDatabase, connects to a separate FLD Database Host process over IPC, accepts the actual `TcpSyncListener` TCP transport, negotiates real `SyncSession` instances, and routes inbound/outbound batches through `FldBinSyncAsyncHostAdapter`.

The executable has two deliberately separate HTTP operator modes. The default is **OIDC operator mode**; integration tests must explicitly select **Test mode**. Neither mode turns this reference executable into a complete production application: its peer runtime is a TCP integration adapter, and the sample does not provision an authenticated/encrypted peer transport or a deployment's pinned namespace TrustStore.

## OIDC operator mode (default)

Set the following environment:

- `BINSYNC_HOST_MODE=Oidc` (optional; this is the default).
- `BINSYNC_OPERATOR_AUTHORITY=https://identity.example/` — HTTPS OIDC authority; JWT signing keys and issuer metadata are obtained from its HTTPS discovery document.
- `BINSYNC_OPERATOR_AUDIENCE=binsync-operator` — required JWT audience.
- `BINSYNC_OPERATOR_SUBJECTS=subject-id-1,subject-id-2` — explicit allowlist of JWT `sub` values authorized to read or mutate the operator API.
- `BINSYNC_HOST_URLS=https://127.0.0.1:8443` — semicolon-separated HTTPS Kestrel URLs. Configure the Kestrel certificate using the standard ASP.NET Core Kestrel certificate settings.
- `BINSYNC_GC_ENABLE_ON_FRESH_STORE=true` — explicit opt-in to bootstrap all three proof-gated GC domains on a fresh Metadata store. If omitted or false, a fresh store remains stopped until an authorized operator commits a policy. An existing durable policy is never overwritten.
- `BINSYNC_HOST_MAINTENANCE_INTERVAL_MS=1000` — optional bounded scheduler interval (100–60000 ms).

JWT bearer validation uses OIDC discovery/signing keys, issuer, audience and token lifetime. The authenticated `sub` must also be in the explicit subject allowlist. The internal `system:host-bootstrap` actor is reserved for the Host's own first-store policy bootstrap and cannot be configured as an external operator.

Only the following operator routes are exposed in this mode:

- GET /health/live — process liveness.
- GET /health/ready — startup barrier, listener/session count, scheduler and GC state.
- GET /operator/peers — peer/session diagnostics.
- GET /operator/gc — readiness, blockers, durable policy and bounded audit snapshot.
- POST /operator/gc/policy — generation-checked per-domain policy update.
- POST /operator/gc/maintenance/once — one bounded maintenance pass.
- POST /operator/gc/emergency-stop — immediate local brake followed by audited durable all-domains-off policy.

Every `/operator/*` request requires a valid OIDC JWT and allowlisted subject. All `/admin/*` routes are absent (404) in this mode. Process shutdown is owned by the service manager/orchestrator; no HTTP process-shutdown endpoint is exposed.

## Test mode (integration tests only)

Tests must explicitly set:

- `BINSYNC_HOST_MODE=Test`
- `BINSYNC_TEST_HOST_OPERATOR_TOKEN` — at least 32 UTF-8 bytes.
- `BINSYNC_TEST_HOST_MAINTENANCE_INTERVAL_MS` — optional interval.
- `BINSYNC_TEST_HOST_ALLOWED_PEERS` — optional comma-separated NodeId allowlist.

Test mode binds its HTTP API to loopback on an ephemeral port. It exposes the test-only `/admin/*` probes used to inject local provider mutations, inspect pending batches, connect test peers, exercise the idle-maintenance fence and perform controlled process shutdown. The fixed bearer token and integration-test actor are not production credentials and must never be reused outside isolated tests.

## Startup and GC ordering

1. Opens and owns one BIN-SYNC Metadata StorageDatabase partition and Node WAL.
2. Connects to the separate FLD Database Host through the FLD IPC provider and remote-apply adapter.
3. Commits the FLD provider-authoritative Metadata publication cutover.
4. Creates `FldBinSyncAsyncHostComposition` and calls `StartAsync`.
5. Restores and validates the durable operator policy and completes provider/Node recovery before accepting peers or starting maintenance.
6. On a fresh Test-mode store (or an OIDC-mode store with the explicit `BINSYNC_GC_ENABLE_ON_FRESH_STORE=true` opt-in), commits three separate audited policy generations in ChangeId → Receipt → Tombstone order.
7. Never overwrites an existing policy on restart. A previously committed emergency stop therefore remains stopped.
8. The scheduler performs bounded maintenance only inside the Host idle-peer/reference-scan fence. Physical deletion still requires signed namespace retention/baseline evidence and exact candidate revalidation. The Stage 0 lifecycle fixture intentionally has no eligible signed candidates; Stage 1–4 canaries prove physical reclamation on disposable databases. Ghost GC remains excluded.

The library/composition defaults remain fail-closed. The reference Host's three startup proof switches are enabled, but runtime execution is controlled by the durable operator policy. In OIDC mode, a fresh-store policy bootstrap is separately opt-in; existing stores are never silently re-enabled.

## Important deployment boundaries

- OIDC protects the HTTP operator surface only. The sample's TCP peer adapter is intended for integration testing; it does not add TLS/mutual peer authentication or load a deployment's pinned namespace authority/member TrustStore. Do not expose this TCP listener to an untrusted network. A product Host must supply its authenticated/encrypted transport and trust provisioning.
- The OIDC operator subject allowlist is an application-level authorization boundary; use a dedicated issuer/audience and tightly scoped operator identities.
- The policy store has no independent anti-rollback authority. After restoring an offline backup, keep GC stopped until an independently retained operator record is checked and a new authorized all-domains-off policy generation is committed. The process-level test exercises this brake/restore procedure but does not create a cryptographic anti-rollback anchor.
- Process restart tests do not establish power-loss durability. Windows NTFS power-loss qualification and representative production retention/scale evidence remain separate release evidence.

## Process arguments

    <metadata-root> <fld-root> <metadata-partition> <node-id-hex>
    <scope-id> <metadata-scope-guid> <namespace-id> <provider-client-guid>

The FLD database must be owned by a separately running Fld1.Database.Host process. One process owns each StorageDatabase partition; do not open the same partition in a second process.

## Acceptance coverage

`FldBinSyncRuntimeHostTests` launches the actual FLD Database Host and this executable Host as separate processes. It verifies real TCP HELLO/HELLO_ACK sessions, two-Host provider exchange/reconnect, live-peer GC reference-lease pause/resume, durable staged policy activation, scheduler execution, emergency stop/restart restoration, offline backup rollback brake, missing-FLD-host startup failure, and rejection of OIDC mode when issuer/audience/subject/HTTPS configuration is absent.

The Linux/Windows `host-runtime-lifecycle.yml` workflow runs this process-level suite. Test mode is explicitly selected by the test launcher; it is not the executable's default.
