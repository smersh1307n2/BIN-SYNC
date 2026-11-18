# FldBinSyncHost.Runtime — disposable integration Host

This project is an **executable test Host**, not a production application and not a deployable production service. It exists because the repository has no production Host executable that can consume the BIN-SYNC/FLD composition.

The process intentionally resembles a long-running application:

1. Opens and owns one BIN-SYNC Metadata StorageDatabase partition.
2. Loads the persisted Node checkpoint (or creates and persists the initial test Node).
3. Connects to a separate, real FLD Database Host process through the FLD IPC provider and remote-apply adapter.
4. Creates FldBinSyncAsyncHostComposition and calls StartAsync.
5. Only after durable operator-policy restoration and provider/Node recovery does the startup callback start the test peer listener and maintenance scheduler.
6. Starts a Kestrel HTTP server with authenticated operator routes.

## Test-only endpoints

- GET /health/live — process liveness.
- GET /health/ready — startup barrier, listener and scheduler state.
- GET /operator/gc — diagnostics and durable policy/audit snapshot.
- POST /operator/gc/policy — generation-checked independent domain policy update.
- POST /operator/gc/emergency-stop — immediate process-local stop followed by audited durable all-domains-off commit.
- POST /admin/shutdown — durable all-domains-off commit, stop scheduler/listener, then process shutdown.

All /operator/* and /admin/* routes require a Bearer token supplied through BINSYNC_TEST_HOST_OPERATOR_TOKEN. The token is test input only. The authorizer accepts only the fixed integration-test actor; do not reuse this authentication or authorizer in a real application.

The peer listener speaks only a tiny test banner. It is a lifecycle probe, **not** a BIN-SYNC transport or SyncSession implementation. The scheduler is a lifecycle probe and does not execute physical GC. All StorageNamespaceGcFeatureOptions remain at their default-off values, and the operator API cannot override those immutable startup proof flags.

## Process arguments

    <metadata-root> <fld-root> <metadata-partition> <node-id-hex>
    <scope-id> <metadata-scope-guid> <namespace-id> <provider-client-guid>

The Metadata database and FLD database are separate disposable roots. The FLD database must be owned by a separately running Fld1.Database.Host process. One process owns each StorageDatabase partition; operator actions are sent to the owning Host's HTTP API, never by opening the same partition in a second process.

## Acceptance coverage

FldBinSyncRuntimeHostTests launches the actual FLD Database Host and this executable Host as separate processes. It verifies:

- listener/scheduler are not started until the composition startup barrier completes;
- unauthenticated and invalid-token operator requests are rejected;
- the HTTP API exposes diagnostics and generation-checked policy changes;
- a disabled startup proof flag cannot be overridden by the operator endpoint;
- emergency stop commits an audited durable all-domains-off policy;
- an ungraceful Host process death followed by a fresh process restores the same disabled policy;
- authenticated shutdown commits a durable stop before the process exits.

The Linux/Windows host-runtime-lifecycle.yml workflow builds and runs this focused process-level test. These results validate the repository's Host integration contract only. They do **not** claim production deployment, production authentication, transport interoperability, power-loss durability, or authorization to enable GC. Production feature switches remain false by default; physical reclamation remains disabled pending a real application integration and its own release review.