# FldBinSyncHost.Runtime — disposable integration Host

This project is an **executable test Host**, not a production application and not a deployable production service. It exists because the repository has no production Host executable that can consume the BIN-SYNC/FLD composition.

The process intentionally resembles a long-running application:

1. Opens and owns one BIN-SYNC Metadata StorageDatabase partition.
2. Opens a Node WAL and loads the persisted Node checkpoint (or creates and persists the initial test Node).
3. Connects to a separate, real FLD Database Host process through the FLD IPC provider and remote-apply adapter.
4. Commits the FLD provider-authoritative Metadata publication cutover, creates FldBinSyncAsyncHostComposition and calls StartAsync.
5. Only after durable operator-policy restoration and provider/Node recovery does the startup callback bind the TCP listener and start the peer-session accept loop and maintenance scheduler.
6. Each accepted or outbound TCP connection performs the real BIN-SYNC HELLO/HELLO_ACK handshake and becomes a `SyncSession`; inbound batches pass through `FldBinSyncAsyncHostAdapter.ReceiveAndPublishAsync`, and scheduled outbound rounds use `RunOutboundRoundAsync`.
7. Starts a Kestrel HTTP server with authenticated test-operator routes.

## Test-only endpoints

- GET /health/live — process liveness.
- GET /health/ready — startup barrier, TCP listener, active SyncSession count and scheduler state.
- GET /operator/peers — active peer count.
- POST /admin/peers/connect — initiate an outbound TCP SyncSession to another test Host (`{ host, port }`).
- GET /operator/gc — diagnostics and durable policy/audit snapshot.
- POST /operator/gc/policy — generation-checked independent domain policy update.
- POST /operator/gc/maintenance/once — one bounded pass through the real adapter/lease path (pageLimit 256, ChangeId page limit 4096).
- POST /operator/gc/emergency-stop — immediate process-local stop followed by audited durable all-domains-off commit.
- POST /admin/shutdown — durable all-domains-off commit, stop scheduler/listener, then process shutdown.

All /operator/* and /admin/* routes require a Bearer token supplied through BINSYNC_TEST_HOST_OPERATOR_TOKEN. The token is test input only. The authorizer accepts only the fixed integration-test actor; do not reuse this authentication or authorizer in a real application.

The peer listener uses the repository's `TcpSyncListener`/`TcpSync` transport and real `SyncSession` handshake. It exercises actual framed TCP transport, peer NodeId/epoch negotiation, session lifecycle, inbound batch processing, outbound publication and disconnect cleanup. `BINSYNC_TEST_HOST_ALLOWED_PEERS` can restrict admitted peer NodeIds (comma-separated 32-hex-character IDs); this is a test allowlist, not a production identity provider. The scheduler invokes both the real bounded GC maintenance path (pageLimit 256, ChangeId page limit 4096) and the adapter's outbound round. All StorageNamespaceGcFeatureOptions remain at their default-off values, so the process-level test verifies all three reclaimed counts stay zero and the operator API cannot override the immutable startup proof flags.

## Process arguments

    <metadata-root> <fld-root> <metadata-partition> <node-id-hex>
    <scope-id> <metadata-scope-guid> <namespace-id> <provider-client-guid>

The Metadata database and FLD database are separate disposable roots. The FLD database must be owned by a separately running Fld1.Database.Host process. One process owns each StorageDatabase partition; operator actions are sent to the owning Host's HTTP API, never by opening the same partition in a second process.

## Acceptance coverage

FldBinSyncRuntimeHostTests launches the actual FLD Database Host and this executable Host as separate processes. It verifies:

- TCP listener/session accept loop and scheduler are not started until the composition startup barrier completes;
- a real TCP connection completes the BIN-SYNC HELLO/HELLO_ACK exchange, exposes the negotiated peer NodeId and is removed cleanly on disconnect;
- unauthenticated and invalid-token operator requests are rejected;
- the HTTP API exposes diagnostics and generation-checked policy changes;
- the periodic scheduler starts only after recovery and invokes the real bounded adapter round;
- one authenticated bounded maintenance round runs through the real adapter with all startup proof flags off and reclaims zero items in every domain;
- a disabled startup proof flag cannot be overridden by the operator endpoint;
- emergency stop commits an audited durable all-domains-off policy;
- an ungraceful Host process death followed by a fresh process restores the same disabled policy;
- authenticated shutdown commits a durable stop before the process exits;
- offline backup is copied only after a clean close with a durable all-off policy; a later policy generation is created, then the older complete Metadata directory is restored;
- the restored generation/audit sequence is checked against a separate operator record, the process-local emergency brake is latched before policy restore, and a new authorized all-off generation is required before any re-enable;
- missing/unavailable FLD Database Host causes startup to fail without publishing readiness.

The Linux/Windows host-runtime-lifecycle.yml workflow builds and runs this focused process-level test. These results validate the emulated Host's actual BIN-SYNC TCP transport/session integration and lifecycle only. They do **not** claim production deployment, production authentication/identity, transport interoperability beyond the implemented TCP adapter, power-loss durability, or authorization to enable GC. Production feature switches remain false by default; physical reclamation remains disabled pending a real application integration and its own release review.

## Backup rollback boundary

The policy store has **no independent anti-rollback authority**: restoring an older complete Metadata database can roll policy generation and audit sequence backwards. The process-level test therefore keeps its approval record outside the database backup, verifies the restored generation and audit sequence against that record, starts the Host with the test-only backup-restore brake latched before policy restore, and commits a new authorized all-domains-off generation. The test simulates the operator procedure; it does not create a cryptographic anti-rollback anchor. Production must keep GC disabled after any restore until an independently retained operational record is checked and a new authorized all-off policy is committed.

Offline backup is permitted by the test Host only after a durable all-domains-off policy is read back. The test-only /admin/shutdown-clean endpoint then latches the local brake and closes the process without creating another policy generation, allowing the closed Metadata directory to be copied as one unit.
