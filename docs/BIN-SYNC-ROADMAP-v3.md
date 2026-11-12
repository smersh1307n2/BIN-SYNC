## 1.93. Signed three-member frontier and two-Host tombstone-GC proof — 2026-11-12

- [x C/T] The positive reclamation scenario uses two real durable FLD Database Hosts (A/B), with the tombstone authored on A, durably applied on B, and independently reclaimed by both Host IPC adapters only after their outbound/relay references are discharged.
- [x T] Negative retention checks remain in the scenario: while A's immutable outbound payload or B's relay payload is live, both physical GC passes return zero and both FLD databases retain the tombstone across shutdown/reopen.
- [x C/T] The downstream node C is now a third **Active** member in the signed namespace roster, not merely an unrostered transport consumer. C receives and applies the B→C batch through a SyncSession with its Node WAL attached; the test obtains C's OFR2 report only after that receive completes. The signed checkpoint is computed from A, B and C reports, and all three members have signed FULL_REPLACE installed ACKs. Both A and B independently validate the resulting three-member proof before physical reclamation.
- [x T] The relay-retention proof checks C's exact per-author ACK cursor and safe peer cursor, plus B's independent local relay-consumer ACK. Metadata relay compaction is not allowed to proceed on the downstream transport ACK alone.
- [x C] Exact-candidate diagnostics in `54d0aa1ae093d1f190e1adba34c84ed338727b41` established that Metadata references were empty, the FLD provider exposed exactly one tombstone candidate, and its origin/HLC matched the signed proof. The remaining Host-side blocker was the unsealed committed WAL prefix, which the FLD reference snapshot conservatively treated as a live ChangeId/RecordId reference.
- [x C/T] FLD-DB `51de772e4a677ab63ef6649440172fec21f89830` + `e0858b1e27c89d4d8fcfc2339d77e9db860d8ed5` add `TrySealCommittedWalForTombstoneGc`: under the Host commit guard, committed WAL is checkpointed before the final reference snapshot; an uncommitted overlay still blocks GC. The Host continues to reject non-empty draft horizons, snapshot pins, and live ApplyJournal/Outbox references.
- [x CI] BIN-SYNC head `393ad81bcb2d752992b0042c4cb3c1df90a15ae9`: all six BinSync.Net jobs passed, including Linux and Windows cross-repository FLD Host adapter tests (run `43234591294`). The docs-only follow-up head `0f91f7d5e888b8debcb23cb090103cc389dcb942` also passed both BinSync.Net runs `43234591294` and `43234606737`. The separate FLD Storage Integration run `43234607102` passed Ubuntu and Windows; macOS was cancelled and its job has been re-requested (attempt 2), so the full matrix is not yet green.
- [x T] The positive scenario reclaims the same tombstone on both durable FLD Hosts and verifies it remains absent after shutdown/reopen. C's signed frontier is required to advance the three-member checkpoint.
- [!] This is not a three-independent-FLD-Host test: C is a separate WAL-backed BIN-SYNC Node/transport member, while A and B are the two durable FLD Database Hosts. It does not yet close concurrent roster/frontier mutation, crash cuts between proof acceptance and GC, lost FULL_REPLACE ACK/rejoin behavior, or the complete shared async metadata-writer exclusion contract. Production GC remains disabled by default.
- [ ] Next: complete the three-member negative/recovery matrix (missing/lost member ACK, stale/rejoined member, frontier regression, restart between proof acceptance and GC, concurrent reference/pin changes); implement the proof-bound FULL_REPLACE per-author feed baseline vector from §1.132; then audit/enforce one shared namespace admission/commit barrier for every Metadata writer before considering release authorization.
## 1.92. Process-isolated signed tombstone-GC pass — 2026-11-12

- [x C] Corrected the IPC provider snapshot-handle mapping to use the durable provider handle's `Token`; candidate discovery still releases its snapshot pin before the Host compaction request, and the Host performs the final exact-candidate/reference/pin revalidation under its commit boundary.
- [x T] Added `Async_tombstone_gc_coordinator_requires_opt_in_and_signed_safe_horizon`: the async coordinator compacts only when the physical tombstone-GC option is explicitly enabled and the candidate origin is covered by the accepted signed roster/frontier/retention checkpoint plus installed FULL_REPLACE acknowledgements. Disabled GC and an origin beyond the safe frontier both return zero without provider compaction.
- [x T] Added a real FLD Database Host IPC test that constructs signed NRS2/OFR2/FRA2/RCP2 state and invokes `FldBinSyncAsyncHostAdapter.RunTombstoneGcPassAsync`; the Host is started with the explicit test-only `--enable-tombstone-gc` option. This exercises the signed proof gate, async adapter, IPC candidate scan, Host-side final revalidation and durable reopen.
- [x C/T] Updated the two-Host process test to wait for a fresh endpoint owner after each Host restart. A killed Host can leave its old endpoint descriptor behind; merely observing that a descriptor exists is not proof that it belongs to the new process. The first exact-head run exposed this race after the async GC IPC test passed.
- [~ CI] Exact-head validation for `57222e03e8a6fa1412bcd68cc2b62833ac4c7506` is running. The preceding `1d1a43fc18` run passed Core Linux/Windows tests, fuzz/lab, signed vectors and FLD Storage Integration (3/3); its cross-repository Host test suite had one unrelated two-Host endpoint-restart failure, now addressed by the fresh-owner fence. Do not treat the newer head as green until all six Core jobs and the FLD Host integration tests pass.
- [!] This is a single-namespace/single-GC-pass integration proof, not release authorization. It does not yet establish two independent Hosts exchanging and reclaiming the same tombstone under a real namespace roster, concurrent roster/frontier changes, all crash/rejoin/resurrection cuts, or the complete async metadata-writer exclusion contract. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled by default; the Host's physical-GC option remains explicit opt-in.
- [ ] Next: validate exact head; then add the two-independent-Host signed-GC scenario after bidirectional APPLY, including a tombstone authored on one Host, durable application on the other, shared signed safe frontier/FULL_REPLACE generation, and independent physical reclamation on both Hosts. Add restart/lost-ACK and concurrent writer/reference-pin cuts. Keep production GC disabled until these proofs and the final byte/API audit pass.

## 1.91. Async Metadata outbound delivery coverage — 2026-11-11

- [x C] Added `Async_metadata_outbound_delivery_uses_frozen_store_and_waits_for_remote_apply_ack`: the test freezes a local provider change into Metadata, sends it through `PushNextBatchAsync`, and verifies the receiver's async provider APPLY completes before the sender observes the transport ACK.
- [x C] Added a per-peer outbound gate to the coordinator. Multiple transport sessions for the same peer cannot race the durable reservation/ACK lifecycle; different peers remain independent. The namespace admission gate is still released before transport waits, preserving full-duplex progress.
- [!] The new integration test and per-peer gate are not yet validated by exact-head CI. This is a one-process integration test, not the required two-independent-Database-Host proof.
- [ ] Next: add multi-session same-peer contention and lost-ACK replay tests, then compose the coordinator in the actual host orchestration and run two independent Database Host processes bidirectionally.

## 1.90. Async Metadata outbound delivery entry point — 2026-11-11

- [x C] Added `AsyncStorageSyncCoordinator.PushNextBatchAsync`. It sends the already-frozen Metadata outbound/relay stream through the durable per-peer reservation and ACK path in `SyncSession.PushBatchAsync`; it does not read the FLD Outbox or create a second payload queue.
- [x C] Outbound delivery captures per-author Metadata high-watermarks under the same coordinator admission semaphore as async local publication, remote receive/APPLY, and PendingApply recovery, then releases admission before awaiting transport ACK. The immutable reservation and captured ceilings prevent the send from advancing beyond its publication boundary. Releasing admission before the network wait is deliberate: a peer may send in the opposite direction at the same time, and its receive/ACK path must not deadlock behind this outbound call.
- [!] This is a per-coordinator, per-namespace serialization boundary. It is not yet a process-wide/shared gate across multiple coordinator instances, direct Node/Metadata writers, or the synchronous scheduler. Production host composition must own one coordinator admission domain per namespace and must prevent bypass writers until the broader async/sync writer protocol is proven.
- [~ CI] Initial implementation `ba9b22be58611c0763844b2334912a4381716c49` was corrected by `59eec3b594c43478762d439689635c16e13c4b0a` to release admission before the transport wait. Exact-head build/test results are pending; do not treat the method as verified until BIN-SYNC and FLD Storage Integration workflows finish.
- [ ] Next: add integration coverage for two concurrent sessions contending for one namespace, outbound reservation retry after lost ACK response, and send interleaved with local publication/remote APPLY. Then compose this entry point into an explicit async host adapter and proceed to two independent FLD Host processes exchanging changes bidirectionally.

## 1.89. Real FLD Host process coverage for async local publication — 2026-11-11

- [x T] Added `Async_storage_coordinator_publishes_through_real_database_host_and_survives_host_restart` in `samples/FldBinSyncHost.Tests/FldDatabaseIpcAsyncDataProviderTests.cs`. It starts the actual FLD Database Host as a separate process, commits a local provider append, stops/restarts the Host before BIN-SYNC records the returned ClientSequence in its Node WAL, reconnects with the same stable ClientId, and runs `AsyncStorageSyncCoordinator.PublishLocalChangesAsync` through the IPC-backed provider.
- [x T] The process test verifies exactly one Metadata outbound operation, Node cache state, FLD current value/ChangeId/OriginSequence, an empty source feed after publication, and durable current state after a further Host restart. It explicitly acknowledges the provider operation receipt after reconnect. This is real process/IPC coverage for the local-publication path, not a two-node synchronization test.
- [x CI] Exact current branch head `21f4b9e4182bba02ee8350b099f1dd374347c7ed`: BIN-SYNC PR run `43034559837` passed all 6 jobs (Linux/Windows .NET tests, Fuzz/Lab, v3.1 signed control-plane vectors, and Ubuntu/Windows FLD host-adapter builds); FLD Storage Integration PR run `43034559505` passed all 3 jobs (Ubuntu, Windows and macOS). The matching push BIN-SYNC run `43034550026` also passed all 6 jobs.
- [~ T] The process test currently injects the Host restart after durable append and before Node-WAL receipt mapping. It does not yet inject process loss independently after Metadata freeze, published-origin stamping, Node checkpoint, source Outbox ACK, or operation-receipt ACK. It also does not run two independent FLD Host processes with two BIN-SYNC coordinators exchanging changes in both directions.
- [!] `FldBinSyncHostAdapter` and `FldBinSyncSessionScheduler` still compose the synchronous `StorageSyncCoordinator`; the new async coordinator is exercised directly by the process test but is not yet the production sample orchestration path. Do not claim complete async host integration or production readiness.
- [!] Async snapshot rehydration, async FULL_REPLACE/author-epoch lifecycle, namespace-wide admission across independent sessions, composed async commit barriers, two-host remote-APPLY/restart parity, and all v3.1 crash/race/rejoin/resurrection proofs remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.
- [ ] Next: implement a two-independent-Host/two-coordinator process test for bidirectional local publication and contextual remote APPLY; inject response loss/restart at each durable boundary; then audit the shared application-write/async-receive barrier and add an explicit async host composition without weakening the synchronous reference path. Keep FULL_REPLACE and physical GC disabled until their async-safe guards and independent safe-horizon proofs pass.

## 1.82. Async coordinator local publication and receipt recovery — 2026-11-11

- [x C] Added `BinSync.Storage.AsyncStorageSyncCoordinator` as a separate async-only path. It exposes startup recovery, finite-ceiling local feed publication, contextual remote receive-before-ACK, and PendingApply recovery; it does not call synchronous provider methods through sync-over-async.
- [x C] Local publication binds the stable provider receipt stream to the Metadata Store, validates the Metadata-owned publication cutover, captures a finite provider feed high-watermark, retries local append by stable ChangeId, and persists the returned ClientSequence in the existing Node WAL before publication can be acknowledged.
- [x C] Publication coalesces only unpublished changes, freezes immutable Metadata outbound operations, stamps immutable FLD origins in bounded IPC-sized batches, checkpoints the Node cache, marks receipt mappings published, then ACKs the provider Outbox and operation receipts independently and in that order.
- [x T] Integration tests cover coalescing and ACK ordering; response loss after Host auto-sequence append, published-origin stamping, source Outbox ACK and receipt ACK; restart after receipt-ACK response loss; receipt-stream identity rotation; appends beyond the captured feed ceiling; and startup recovery before sessions are accepted.
- [x T] Async PendingApply tests cover exact wire-payload replay, provider commit with lost response, Metadata finalization recovery and Node checkpoint reopen. The recovery path validates the immutable batch fingerprint and full sender/issuer context before retry.
- [x CI] The recovery-hardening exact-head workflow run `43012649107` passed all 6 jobs: Linux/Windows .NET tests, fuzz/lab, signed vectors and cross-repository FLD host-adapter builds/tests. Storage Integration run `43012649100` passed Ubuntu, Windows and macOS (3/3 jobs).
- [!] This async coordinator is not yet wired into `FldBinSyncHostAdapter` / `FldBinSyncSessionScheduler`; those sample production orchestration entry points still use the synchronous `StorageSyncCoordinator`. The async tests currently exercise the coordinator with in-process recording providers; they do not establish two independent FLD Host processes exchanging data through the production async path.
- [!] Async snapshot rehydration, FULL_REPLACE, author-epoch lifecycle, namespace-wide cross-session admission, composed async commit barriers and two-process restart/reconnect parity remain incomplete. Physical receipt, ChangeId and tombstone/ghost GC remain disabled.
- [ ] Next: wire an explicit async host composition without weakening the existing synchronous reference path; add real FLD IPC/Host process integration for local publication and remote APPLY; test concurrent sessions, response loss and Host/BIN-SYNC restart; then complete snapshot rehydration and the remaining optional capabilities. Re-run exact-head BIN-SYNC and FLD-DB CI on all supported platforms before marking this phase complete.

## 1.81. First asynchronous remote-APPLY receive path — 2026-11-11

- [x C] `SyncSession.ReceiveBatchAsync` now supports an awaited before-ACK callback. The transport ACK is not encoded or sent until the asynchronous durability callback completes; the existing synchronous callback/API remains available and mutually exclusive.
- [x C] `StorageSyncCoordinator.ReceiveBatchWithAsyncProviderBeforeAckAsync` stages the exact durable PendingApply decision before Node mutation, checkpoints Node, awaits the full-context `IAsyncProviderRemoteBatchCommitStore` APPLY, validates the returned ApplyToken, finalizes the Metadata receipt, and only then permits the transport ACK.
- [x C] No Monitor is held across the provider await. The async APPLY phase is serialized per coordinator with a `SemaphoreSlim`; uncertain provider responses are retried using the same deterministic ApplyToken and immutable batch context.
- [x T] Added an integration test exercising the asynchronous provider path over a real `SyncSession`/`PipeTransport` pair and checking Node/provider state, preserved sender/issuer context, PendingApply finalization and completion before ACK.
- [~ CI] Exact-head build and tests are required. This first slice covers ordinary contextual DELTA_BATCH remote APPLY only; it does not complete async local publication, receipt persistence/ACK, snapshot rehydration, FULL_REPLACE, author-epoch transitions or GC.
- [!] One receive stream per namespace/coordinator remains the supported orchestration assumption for this slice. Concurrent independent sessions sharing one coordinator still need an explicit namespace-wide admission/serialization boundary before production enablement. Do not claim full two-process parity.
- [ ] Next: verify this commit on Linux/Windows/macOS; add response-loss/restart tests around provider APPLY and Metadata finalization; then implement async local feed publication with immutable origin stamping and receipt sequence persistence before receipt ACK.

## 1.80. Async provider implementation and writer-lock audit — 2026-11-11

- [x C/T] Added the first process-isolated `IAsyncDataProvider` implementation in `samples/FldBinSyncHost/FldDatabaseIpcAsyncDataProvider.cs`, plus separate `IAsyncProviderRemoteBatchCommitStore` and `IAsyncProviderPublishedOriginStore` capabilities. It uses asynchronous IPC only; no sync-over-async.
- [x C/T] FLD IPC now provides point metadata/value reads (0x210F/0x2110), Host-assigned receipt sequence for local provider append (0x2111), authoritative LocalAuthor discovery (0x2113), and idempotent ChangeId-bound publication-origin stamping (0x2114). IPC receipt ACK (0x2103) and synchronization Outbox ACK (0x2106) remain independent; 0x2112 is reserved and must not couple them. The auto-sequence path reuses the existing durable operation receipt; a minimal completion cursor is still required, but no payload journal or client-side sequence allocator is added.
- [x A] Updated the async coordinator ADR with the actual writer-lock audit. `StorageSyncCoordinator` holds a Monitor, then Node Storage→Outbox→Meta Monitors, then host/provider gates; FULL_REPLACE additionally holds the Metadata Store Monitor. A semaphore at the IPC boundary alone cannot serialize direct Node/Metadata writers.
- [!] The new provider is not yet usable by the production coordinator. It does not implement async publication cutover, author-epoch lifecycle, snapshot-state apply, FULL_REPLACE, durable snapshot/GC scanner or `IAsyncProviderCommitGuard`; the synchronous coordinator remains Monitor-based. Published-origin stamping exists, but without a composed async commit lease it is not a cross-store atomic publication transaction. No async coordinator, FULL_REPLACE, or physical-GC support is claimed.
- [~ CI] Exact-head CI for the current BIN-SYNC branch is running after the async-provider tests and documentation updates. FLD-DB exact head `9943d57d67d71f47d22132667ccfcc1dc9042c80` passed build and tests on Ubuntu, Windows and macOS (runs `42988005885` and `42987998071`).
- [ ] Next: choose and prove one complete strategy: a shared sync+async gate participating in every conflicting writer, or durable PendingApply/intent with short-lock snapshot/revalidation across awaits. Do not add a dispatcher-only SemaphoreSlim. Then implement async coordinator phases and two-process crash/restart parity tests.
- [!] Physical receipt, ChangeId and tombstone/ghost GC remain disabled. Preserve exactly two runtime DLLs and the separate Database Host executable.
## 1.79. First process-isolated async FLD provider slice — 2026-11-11

- [x C] FLD Database IPC adds addressed current metadata reads (0x210F) and ChangeId-fenced bounded current-value chunks (0x2110). Point reads use the existing indexed LWW resolver across overlay + Runs; they do not scan the full provider snapshot or create WAL receipts for reads.
- [x C/T] FLD adds auto-sequenced identity-preserving provider append (0x2111). The Host assigns the next per-client receipt sequence under the commit lock; stable ClientId + OperationId derived from ChangeId + request fingerprint replays the same durable receipt across response loss and Host restart. No client-side sequence journal or Pending table is added.
- [x C/T] FLD exposes authoritative Host LocalAuthor discovery (0x2113), and the IPC provider verifies the configured BIN-SYNC local replica before accepting drafts. IPC receipt ACK (0x2103) remains deliberately independent from synchronization Outbox ACK (0x2106): an Outbox publication ACK must never retire a caller's uncertain append receipt.
- [x C/T] Auto-sequence append (`0x2111`) now returns the durable ClientSequence with CommitSeq. `FldDatabaseIpcAsyncDataProvider` exposes `IAsyncProviderOperationReceiptStore`: `AppendLocalChangeWithReceiptAsync` returns that sequence and `AcknowledgeOperationReceiptsAsync` uses the separate IPC receipt ACK (`0x2103`). It never couples receipt ACK to Outbox ACK (`0x2106`).
- [~ C/T] The future async coordinator must persist the returned ClientSequence in the existing Node WAL/commit record before calling receipt ACK. Retry after a lost response reuses OperationId derived from ChangeId and gets the same sequence. No client-side sequence allocator, payload journal, or second Pending data store is needed.
- [x C/T] `FldDatabaseIpcAsyncDataProvider` implements the core `IAsyncDataProvider` surface plus immutable published-origin stamping: stable-key resolution, local append, bounded Outbox reads/ACK, snapshot paging/value chunks, ChangeId-fenced current reads and legacy snapshot fallback. It uses asynchronous IPC throughout and does not hold Monitor locks across awaits.
- [x C/T] `FldDatabaseIpcRemoteBatchApplyStore` implements `IAsyncProviderRemoteBatchCommitStore` and forwards the complete sender/transport epoch, issuer/author epoch, BatchId, cursor, digest chain, RecordsRoot and protocol-state delta to the atomic FLD APPLY transaction. Context-free async APPLY deliberately fails closed. It validates the returned durable receipt's CommitSeq, cursor range, BatchId, ProtocolStateDelta and AppliedCount before acknowledging success.
- [x T] BIN-SYNC baseline head `b4d61ccb2bbf97ab3f41c38946b739bf229ba25f` passed the full BIN-SYNC workflow and cross-repository FLD host build/tests on Ubuntu and Windows (run `42987247560`). The later LocalAuthor/receipt-retirement additions are being revalidated at exact head `3b2b3314eae8ac17567e4a9262468444a9b7271e`; do not claim that newer head green until its workflows finish.
- [~ CI] FLD-DB head `9943d57d67d71f47d22132667ccfcc1dc9042c80` has Ubuntu and macOS build/tests green; Windows is still running. This is not yet an all-platform exact-head pass.
- [!] This is an IPC-backed provider primitive, not a complete process-isolated synchronization coordinator. It does not implement async publication-mode, published-origin, author-epoch closure, snapshot-state apply, FULL_REPLACE, durable snapshot/GC scanner or async commit-guard capabilities. `StorageSyncCoordinator` remains synchronous and Monitor-based; do not pass this adapter to it through sync-over-async.
- [ ] Next: audit every writer in Node, Metadata Store, FLD Host and provider; design one async-safe composed commit lease with crash/disconnect recovery; implement only the durable optional IPC capabilities required by the coordinator; then build the async coordinator and run two-process parity tests for concurrent local writes, remote APPLY, ACK/receipt retirement, reconnect, restart, snapshots and FULL_REPLACE.
- [!] Physical receipt, ChangeId and tombstone/ghost GC remain disabled. Preserve exactly two runtime DLLs and the Database Host executable split.
## 1.78. Explicit async optional provider capability contracts — 2026-11-11

- [x A] Added `BinSync.Core/AsyncProviderCapabilities.cs` with async counterparts for snapshot-state apply, publication cutover, published-origin stamping, author-epoch lifecycle, FULL_REPLACE, durable snapshot retention, async commit/snapshot-GC leases and host-owned pinned GC reference scans.
- [x A] Async commit leases use `IAsyncDisposable`; this avoids requiring a thread-affine Monitor to be disposed after an await. It does not by itself make current Node/Storage Monitor gates await-safe.
- [~ T] Exact-head CI is required for this API addition. No runtime implementation or process-isolation milestone is claimed.
- [!] Do not implement a catch-all optional-capability adapter that advertises unsupported methods and throws at runtime. Implement capability-specific adapters only when their durable FLD IPC operations exist.
- [ ] Next: compile/test the contract addition; audit every conflicting Node, Metadata Store and provider writer before selecting the async gate protocol. Then implement durable FLD IPC methods and parity tests capability by capability.

## 1.77. Async provider adapter spike and FLD process-host CI correction — 2026-11-11

- [x T] FLD-DB commit `f961f995f2d69749be12940095d744258ebf19e5` corrects the process-host recovery assertion: the durable Outbox high watermark is 2 while the independent acknowledged Outbox watermark remains 0. This preserves the intended no-ACK recovery scenario.
- [x CI] Exact FLD-DB commit `f961f995f2d69749be12940095d744258ebf19e5` passed run `42971381286` on Ubuntu, Windows and macOS. The preceding `b2bb4b1` run failed only because the test asserted the ACK watermark instead of the high watermark; no Outbox implementation change was needed.
- [x C/T] BIN-SYNC now has the initial `IAsyncDataProvider` and `IAsyncProviderRemoteBatchCommitStore` contracts, explicit in-process adapters, and tests for local feed, snapshot, current reads, remote apply, cancellation and full contextual batch forwarding. In-process adapters call the synchronous reference provider directly and do not block on asynchronous work.
- [~ CI] BIN-SYNC latest head at this audit is `584645cb725e4f5d69771ec4b2420b1884396b12`. Run `42971740146` has Linux tests, fuzz/lab, signed vectors and Ubuntu host adapter green; Windows host adapter is still running. Storage Integration run `42971740074` has Ubuntu and macOS green; Windows is still running. Do not mark exact-head CI complete yet.
- [x A] Added `docs/ASYNC-PROVIDER-COORDINATOR-ADR.md`: records the complete async capability surface, the Monitor/await hazard, required lock order, process-isolation boundary and exact verification gates. It does not mark the architecture implemented.
- [!] Async API coverage is not yet sufficient for process-isolated FLD. `IAsyncDataProvider` currently covers core feed/snapshot/read/apply methods, but the coordinator also requires async counterparts for publication mode, author-epoch closure, published-origin stamping, snapshot-state apply, FULL_REPLACE, durable snapshots, commit guards and GC reference scanning. `StorageSyncCoordinator` itself still uses synchronous provider calls and Monitor-based gates.
- [!] Do not await IPC while holding a thread-affine Monitor/Node gate. Preserve the required coordinator → Node → host WAL/FLD → shared Metadata/Storage lock order using async-compatible leases before implementing an async coordinator. Do not use sync-over-async.
- [ ] Next: close exact-head BIN-SYNC CI; design and test the complete optional async capability set and async-safe composed commit lease; then implement the process-isolated FLD provider/apply adapter and coordinator path. Keep the in-process adapter as the conformance reference until two-process parity tests pass.
- [!] Physical receipt, ChangeId and tombstone/ghost GC remain disabled. Preserve exactly two runtime DLLs and the Host executable split.

## 1.76. Latest branch audit; process-host IPC hardening and async provider boundary — 2026-11-11

- [x CI] BIN-SYNC branch `feature/fld-external-storage` advanced beyond §1.75 to exact head `85c820149b9acef82f70588e3fdc2aafc2a75fa7`. Workflow run `42970934518` passed on Ubuntu and Windows, including the cross-repository FLD host-adapter jobs; Storage Integration run `42970934562` passed on Ubuntu, Windows and macOS.
- [x T] The latest BIN-SYNC host-adapter test adds explicit coverage that a remote record tombstone retains its immutable ChangeId / OriginAuthorEpoch / OriginSequence when mapped to the FLD lifecycle row; local Outbox remains unchanged.
- [x C/T] FLD-DB branch `fix/crash-consistency-outbox` advanced to `b2bb4b1a0f21b49538239f3523cd3ee69b2ecede`. Its Database Host process tests now cover concurrent provider clients, stable operation retry across Host restart, durable Outbox recovery, snapshot survival and writer-fence release after process exit. These are FLD IPC/Host process tests; they do not yet prove a complete process-isolated BIN-SYNC IDataProvider/StorageSyncCoordinator.
- [~ CI] FLD exact-head workflow run `42971257956` is in progress for the latest FLD branch head. Do not count it as green until all OS jobs and tests complete.
- [!] Architecture gate remains open: the production BIN-SYNC `IDataProvider` and contextual remote-apply interfaces are synchronous, while the FLD Database IPC client is asynchronous. The IPC facade still lacks a complete async provider/coordinator surface, including provider-current metadata reads, published-origin stamping and cross-call commit-lease composition. Do not introduce sync-over-async or claim process isolation complete.
- [ ] Next in Roadmap order: finish the FLD exact-head CI gate; define the asynchronous provider/apply + commit-lease contract (or an explicitly durable host worker queue) against the existing coordinator call graph; implement the complete IPC-backed surface; then run two-process parity tests for local Outbox publication, remote APPLY, retries/duplicates, restart/reconnect, snapshots and FULL_REPLACE. Keep the in-process adapter as the reference until parity passes.
- [!] Physical receipt, ChangeId and tombstone/ghost GC remain disabled. Preserve the two-runtime-DLL boundary and Host-as-executable split.

## 1.75. FLD identity-preserving IPC and cross-repository host gate green — 2026-11-11

- [x CI] BIN-SYNC exact head e0cc43cb51d7e9df0a652c008e33ed5e4d7b30b9 passed workflow run 42970243666. Linux and Windows each passed all 678 BIN-SYNC tests; fuzz/lab passed 31 tests; v3.1 signed control-plane golden vectors passed; benchmark build, public samples and coverage threshold passed.
- [x CI] The public FLD-DB branch fix/crash-consistency-outbox was checked out without FLD_DB_READ_TOKEN on both Ubuntu and Windows. Cross-repository FldBinSyncHost build passed on both OSes.
- [x CI] Both cross-repository OS jobs passed FLD Core 23/23, Storage 66/66, Manifest 11/11, Run 26/26, WAL 21/21 and Engine 80/80; FldBinSyncHost.Tests passed 15/15 on Ubuntu and Windows.
- [x C] FLD IPC now has immutable provider snapshots (0x2108–0x210B), contextual remote APPLY (0x210C) and identity-preserving local provider batch append (0x210D). The latter preserves ChangeId/HLC/OriginAuthorEpoch, keeps Host LocalAuthor authoritative, assigns OriginSequence only at publication freeze, rejects duplicate identities/fields and unrelated dirty overlay state, and commits multi-field batches plus receipt in one GROUP_COMMIT.
- [x T] FLD exact head 2a8048516e7bec8142e860a0c7e5316f81b12421 passed run 42969990001 on Windows, Ubuntu and macOS: Engine 80/80 and every other FLD suite green on all three OSes.
- [ ] Process-isolated IDataProvider composition is still not complete. Do not wire IPC into the current synchronous provider interfaces with sync-over-async. The next architectural task is an async provider/apply contract and a complete IPC-backed provider surface, followed by two-process restart, duplicate, Outbox, snapshot and FULL_REPLACE parity tests. Keep the current in-process adapter as the reference until then.

## 1.74. FLD contextual Remote APPLY IPC boundary added; process-isolated provider audit — 2026-11-11

- [x C] FLD-DB branch fix/crash-consistency-outbox adds Database IPC operation 0x210C, a bounded versioned codec for the complete RemoteBatchCommit context, relay-subset indices, client API and dispatcher route to the existing atomic FldDatabase.ApplyRemoteBatch transaction.
- [x C] Remote APPLY IPC preserves sender peer/transport epoch, namespace, issuer/author epoch, batch id, cursor range, 16-byte digest identities, records root and opaque ProtocolStateDelta. ApplyJournal remains the durable deduplication authority; the IPC OperationId is correlation only.
- [x T] FLD adds codec round-trip, malformed/trailing payload, invalid relay-index and IPC duplicate-apply tests.
- [x CI] Exact FLD-DB commit 2931222ad84fce883476cafa24e19e880d640391 passed workflow run 42969134087 on Windows, Ubuntu and macOS: Core 23/23, Manifest 11/11, Run 26/26, WAL 21/21, Storage 66/66 and Engine 75/75 on all three OSes. Windows build completed with 0 errors; two pre-existing xUnit1031 warnings remain in SyncApplyTests.cs lines 467 and 475.
- [x A] Existing FLD snapshot IPC 0x2108–0x210B is already implemented and tested: immutable generation, metadata pages, bounded value chunks, ClientId/HostGeneration fencing and release. FLD IPC docs were corrected to match implementation.
- [x C] FLD adds Database IPC operation 0x210D for atomic identity-preserving local provider batches: caller ChangeId, HLC and OriginAuthorEpoch are retained, Host LocalAuthor remains authoritative, OriginSequence stays zero until publication freeze, and the local Outbox plus IPC receipt commit together.
- [x T] Added codec and IPC retry tests asserting stable ChangeId/HLC/epoch, canonical field/record tombstones, multi-field single-GROUP_COMMIT behavior, ordered ChangeIds and rejection of unrelated dirty overlay writes.
- [x CI] Exact FLD-DB head 2a8048516e7bec8142e860a0c7e5316f81b12421 passed workflow run 42969990001 on Windows, Ubuntu and macOS: Core 23/23, Manifest 11/11, Run 26/26, WAL 21/21, Storage 66/66 and Engine 80/80 on all three OSes. Windows build completed with 0 errors; two existing xUnit1031 warnings remain in SyncApplyTests.cs lines 467 and 475.
- [!] A complete process-isolated IDataProvider is not yet ready. Existing local IPC WriteBatch generates local HLC/ChangeId inside FLD and cannot accept the provider-supplied stable ChangeId, OriginAuthorEpoch and HLC required by FldDatabaseDataProvider.AppendLocalChange. Add a dedicated atomic identity-preserving local-provider batch operation before moving local writes behind IPC.
- [!] IDataProvider and IProviderRemoteBatchCommitStore are synchronous today, while FldDatabaseIpcClient is async. Do not hide IPC waits inside sync methods or use sync-over-async on the receive hot path. Introduce an async provider/apply boundary (or an explicit host worker queue with durable admission semantics) before wiring the production process-isolated adapter.
- [ ] Next: complete FLD CI, then add identity-preserving local provider IPC and async coordinator capability; only then compose the separate-process host and run two-process restart/duplicate/Outbox/snapshot tests. Keep the current in-process adapter as the reference implementation until parity is proven.

## 1.73. Phase 4.1 retirement crash proof and Phase 4.2 ChangeId restart proof green — 2026-11-11

- [x T] Peer retirement now has an injected crash test at StorageFaultPoint.AfterWalFlushBeforePublish. Reopen replays the complete one-batch retirement: replacement epoch remains active, old epoch rows are removed, the retired-through fence is durable, and repeated retirement creates no second commit.
- [x T] Global ChangeId equivocation now has a durable cross-peer/restart test. A finalized operation is reopened from ChangeLedger; a different peer reuses the same ChangeId with a different fingerprint, payload and next origin sequence; BeginPendingApply rejects it without adding a pending intent or changing relay state.
- [x CI] Exact BIN-SYNC head 7dae12a1b60fe481f52febfe4c5692400ca029d1 passed dotnet workflow run 42966709635 on Ubuntu and Windows. BIN-SYNC tests passed 678/678 per OS; benchmark build, public samples, coverage threshold, fuzz/lab and v3.1 signed vectors passed.
- [x CI] Exact Storage Integration run 42966709649 passed 226/226 on Ubuntu, Windows and macOS, including both new crash/reopen proofs.
- [x CI] Cross-repository FLD host adapter build and tests passed on Ubuntu and Windows: Core 23/23, Storage 66/66, Manifest 11/11, Run 26/26, WAL 21/21, Engine 73/73, FldBinSyncHost.Tests 15/15 per OS.
- [!] Two non-fatal xUnit1031 warnings remain in FLD tests/SyncApplyTests.cs lines 467 and 475. The abandoned-thread named-Mutex scenario is exercised on Windows; a true child-process-death harness for Unix remains follow-up work.
- [ ] Next T-pass: independently test field tombstone, record tombstone and ghost retention across offline peers, alternate routes and crash cuts. Keep physical receipt, ChangeId and tombstone/ghost GC disabled until all safe-horizon and reference-scan proofs pass.

## 1.72. FLD IPC snapshot handles and exact-head cross-repository gate green — 2026-11-11

- [x A] Snapshot consistency boundary is the pinned immutable Run generation plus the durable frozen overlay Run and its SourceFeedWatermark. Snapshot pages do not copy namespace value payloads per client.
- [x C] FLD provider snapshots expose metadata-only pages and bounded value chunks. The retry cache is keyed by absolute start index, page limit and metadata/value mode; chunk reads do not materialize whole values.
- [x C] Database IPC adds OpenSnapshot, ReadSnapshotPage, ReadSnapshotValueChunk and ReleaseSnapshot. Handles bind to ClientId and dispatcher HostGeneration; open retries are idempotent while a handle is active, release is idempotent, and limits are four active handles/client and 64/host.
- [x C] IPC generations write an atomic ipc.handle descriptor before manifest publication. Host restart invalidates old IPC handles and prunes only stale IPC-owned generations; non-IPC provider snapshots used by BIN-SYNC FULL_REPLACE are preserved.
- [x T] Added tests for metadata-only pages, chunk boundaries, different-limit retries, snapshot stability across writes/checkpoint/compaction, open retry, cross-client rejection, reconnect, idempotent release, host restart invalidation/pruning, and preservation of non-IPC snapshots.
- [x CI] BIN-SYNC exact head 9af43e1d1d6cd06c5859d456db682cecd4b197ad passed workflow run 42965583024 on Ubuntu and Windows. BIN-SYNC tests passed 678/678 on each OS; benchmark build, public samples, coverage threshold, fuzz/lab and v3.1 signed vectors passed.
- [x CI] FLD-DB branch fix/crash-consistency-outbox at e7c23082edfd0d65699e708d272f291e894ebcd9 passed on Ubuntu and Windows: Core 23/23, Storage 66/66, Manifest 11/11, Run 26/26, WAL 21/21, Engine 73/73, and FldBinSyncHost.Tests 15/15 per OS. Both host adapter builds completed with 0 warnings and 0 errors.
- [x CI] Separate Storage Integration run 42965583027 passed on Ubuntu, Windows and macOS.
- [x C] Backup refs created at exact branch heads:
  - BIN-SYNC backup/2026-11-11/main -> f0a18b8caa45efb0df5faa4a3e6e9653d160aa84
  - BIN-SYNC backup/2026-11-11/feature-fld-external-storage -> 9af43e1d1d6cd06c5859d456db682cecd4b197ad
  - FLD-DB backup/2026-11-11/main -> 4a3b589175630cb42fde8e12d217e04de896b6d2
  - FLD-DB backup/2026-11-11/fix-crash-consistency-outbox -> e7c23082edfd0d65699e708d272f291e894ebcd9
- [!] Two non-fatal xUnit1031 warnings remain in FLD tests/SyncApplyTests.cs lines 467 and 475. The abandoned-thread named-Mutex scenario is exercised on Windows; a true child-process-death harness for Unix remains follow-up work.
- [ ] Next: continue the remaining Phase 3–5 proof work. Keep physical receipt, ChangeId and tombstone/ghost GC disabled until safe-horizon, composed-lease, crash/reopen and adversarial proofs are independently complete.

## 1.71. FLD public host build passes; default-chunk test corrected — 2026-11-11

- [x CI] Public FLD checkout and FldBinSyncHost build succeeded on both Ubuntu and Windows without FLD_DB_READ_TOKEN; both host adapter builds completed with zero warnings/errors.
- [x CI] BIN-SYNC Linux/Windows build and test jobs, benchmark build, fuzz + lab, and v3.1 signed control-plane vectors all passed in run 42962678340.
- [x T] FLD Storage 66/66, Manifest 11/11, Run 26/26, and WAL 21/21 passed in the same run.
- [x FLD] The sole Engine test failure was a test-call mismatch, not an IPC implementation defect: the second client explicitly requested the 1 MiB chunkSize while asserting the 4,194,210-byte protocol maximum. FLD commit d6a6857e removes that argument so the test exercises the documented default maximum chunk.
- [x C] BIN-SYNC integration workflow now also runs Fld1.Core.Tests; commit 75746428.
- [x CI] Exact-head run 42963323793 completed successfully on Ubuntu and Windows: all six FLD projects plus FldBinSyncHost.Tests passed 232/232 per platform; both FldBinSyncHost builds completed with 0 warnings and 0 errors.

## 1.70. Public FLD-DB checkout gate enabled — 2026-11-11

- [x] smersh1307n2/FLD-DB is now public. Unauthenticated Git access verified the integration branch fix/crash-consistency-outbox at 03f988eeea3e0c8cc1f4209834ddf3fb1d157a37.
- [x C] Updated .github/workflows/dotnet.yml: the FLD checkout is now unconditional and no longer depends on FLD_DB_READ_TOKEN. The Ubuntu/Windows host job must build FldBinSyncHost, run the FLD Storage/Manifest/Run/WAL/Engine durability projects, and run FldBinSyncHost.Tests.
- [x T] Latest BIN-SYNC branch head 827d366bb23bd80c53dfdb434b0f77c2cf60e4e6: local Windows Release BinSync.Tests passed 678/678. The Windows state-file replacement test also passed after a bounded retry for transient destination access denial.
- [!] Local full-solution and FLD host builds are not claimed: this workstation cannot reach api.nuget.org (NU1301), and the host project's assets file is not present. GitHub Actions is now the required exact-head cross-repository build/test gate.
- Historical FLD_DB_READ_TOKEN references in older sections below describe the former private-repository state; they are no longer current setup instructions.
## 1.69. CI green after Windows waiter deadline correction — 2026-11-10

- [x CI] The full BIN-SYNC workflow for source commit `47e9f3df0b8c9a8ac0ed77cc6d31e1f068ed6711` completed successfully: Linux .NET tests 678/678, Windows .NET test job success, signed v3.1 control-plane vectors success, fuzz/lab success, and the workflow's host-adapter job completed.
- [!] The host-adapter job's detailed steps confirm that the private FLD checkout/build/test were skipped, not executed, because `FLD_DB_READ_TOKEN` is unset. Its green job status is therefore only a successful skip/notice path.
- [x CI] The separate durable BIN-SYNC Storage Integration matrix for source commit `e6f78a1c2b3c20c79b3c615ed168addd12404838` passed on Ubuntu, Windows and macOS.
- [ ] Still unverified: compilation and execution of `samples/FldBinSyncHost.Tests` against the private FLD-DB branch, including the new two-FLD-node session round-trip. No claim of end-to-end success until that job actually runs.

## 1.68. CI follow-up: isolate Windows scheduling flake and close the typed-node test setup — 2026-11-10

- [x CI] Exact-head run for §1.67 passed the Linux .NET test job, signed control-plane vectors and fuzz/lab; the Windows .NET test job had one failure only: `FlowControl_WaitForCredit_reserves_atomically_and_wakes_all_waiters` timed out at its 1-second test deadline. Build succeeded and 677/678 tests passed.
- [x T] Raised that test's scheduling deadline to 5 seconds (no protocol/runtime behavior changed) and corrected the two-node host test to pass typed `NodeId` values into `NodeBuilder`.
- [~ CI] New exact-head run is pending. The durable Storage Integration workflow for §1.67 completed successfully on Ubuntu, Windows and macOS; this validates the BIN-SYNC Storage provider integration suite, not the FLD host adapter project.
- [!] CI credential blocker confirmed from runner logs: `FLD_DB_READ_TOKEN` is empty. Consequently the cross-repository FLD checkout, host-adapter build and `FldBinSyncHost.Tests` execution are explicitly skipped. Configure a fine-grained read-only token with Contents: read access to `smersh1307n2/FLD-DB` as the BIN-SYNC repository Actions secret `FLD_DB_READ_TOKEN` to activate them.
- [ ] Do not mark the two-FLD-node round-trip complete until `FldBinSyncHost.Tests` has actually compiled and passed against the current FLD-DB branch. Local Commander workstations are still offline; no local test result is claimed.

## 1.67. T-pass: preserve FLD record-vs-field tombstone semantics through remote APPLY — 2026-11-10

- [x T] Added a contextual remote batch containing both a record tombstone and an ordinary field tombstone. The host adapter must map the former to FLD lifecycle field `0x0000 = Dead`, while preserving the latter as a tombstoned user field.
- [x C] This guards against collapsing record deletion and field deletion into one representation at the BIN-SYNC/FLD boundary. Exact-head execution remains pending until the private FLD checkout credential is configured.

## 1.66. T-pass: add two-FLD-node BIN-SYNC session round-trip — 2026-11-10

- [x T] Added an end-to-end integration scenario with two independent FLD databases, two BIN-SYNC Nodes, independent durable Storage/Metadata stores and a real paired `SyncSession`.
- [x T] Each side appends one local FLD Outbox mutation, freezes it through `StorageSyncCoordinator.PublishLocalChanges`, transfers it over the session, and receives/applies the opposite peer's batch through `ReceiveBatchBeforeAckAsync` plus the exact `FldRemoteBatchApplyStore`.
- [x T] Assertions cover both FLD database values and both provider current-state projections after bidirectional exchange.
- [~ T] Exact-head execution is pending. The cross-repository test gate remains skipped until `FLD_DB_READ_TOKEN` is configured; local machines remain offline.
- [x C] This is the first test that exercises the complete local-FLD-Outbox → BIN-SYNC session → remote-FLD-WAL path in both directions. Restart/reconnect and crash-boundary coverage remain separate.

## 1.65. T-pass: cover FLD local Outbox feed identity through the host adapter — 2026-11-10

- [x T] Added `Local_fld_outbox_feed_preserves_stable_change_identity_across_reopen`: append a local FLD mutation through `FldDatabaseDataProvider`, read the bounded provider feed, and verify the stable ChangeId, local author, author epoch, deferred origin sequence and payload.
- [x T] The same feed entry is read after closing/reopening FLD, proving the provider projection retains the durable Outbox identity and sequence.
- [~ T] Exact-head test execution is not yet available: the cross-repository CI job is currently skipped because `FLD_DB_READ_TOKEN` is not configured; local workstations are offline. The test project and workflow gate are committed and will execute when the read-only repository credential is supplied.
- [x C] Together with §1.64 this covers the two adapter boundaries separately (local FLD Outbox→provider feed and BIN-SYNC contextual APPLY→FLD WAL/ApplyJournal). The full two-node round-trip remains the next integration milestone.

## 1.64. T-pass: exercise FLD contextual remote-apply adapter against durable database — 2026-11-10

- [x T] Added a dedicated `FldBinSyncHost.Tests` project and an integration test that sends a complete BIN-SYNC `ProviderRemoteBatchCommit` through `FldRemoteBatchApplyStore` into a real FLD database.
- [x T] The test checks field identity/value, stable ChangeId-derived apply receipt fingerprint, no local Outbox echo, no duplicate FLD relay payload journal, idempotent replay and duplicate recognition after closing/reopening the FLD database.
- [x C] The cross-repository GitHub workflow now runs this test after building the host adapter whenever the read-only FLD checkout credential is available. This is a first executable cross-runtime test; it is not yet a two-peer network round-trip.
- [~ T] Exact-head workflow result pending. Physical receipt/ChangeId/tombstone GC remains disabled.

## 1.63. T-pass: assert recovered immutable reservation identity and payload — 2026-11-10

- [x T] Strengthened the restart reference-scan test to read the recovered peer stream and assert the exact pending BatchId, cursor range and byte-identical immutable payload before confirming the reservation still blocks the tombstone-GC reference scan.
- [~ T] Exact-head CI and local execution remain unverified; the current BIN-SYNC workflows are running for the preceding snapshot/adapter commits.
- [x C] This closes the reservation-payload assertion gap only; reservation retirement/rejoin horizons and physical tombstone deletion remain separate, unproven work.

## 1.62. T-pass: verify external GC reference-scan snapshot ownership — 2026-11-10

- [x T] Added `External_gc_reference_scan_owns_snapshot_and_exposes_reference_queries_until_disposed`: all three host-reference query delegates are exercised, the scan disposes its owned immutable snapshot, and queries after disposal fail with `ObjectDisposedException`.
- [~ T] This is adapter/lifetime coverage, not an end-to-end tombstone-compaction proof. Exact-head CI and local execution remain unverified.
- [x C] The external reference scanner remains a host-owned snapshot contract; no host database types enter BIN-SYNC and no physical tombstone deletion is enabled.

## 1.61. T-pass: require all provider snapshot pins to drain before tombstone-GC guard — 2026-11-10

- [x T] Added `Snapshot_gc_guard_waits_for_every_active_snapshot_pin_to_be_released`: one active snapshot blocks `TryAcquireSnapshotGcGuard`; releasing only one of two snapshots still blocks it; releasing both permits acquisition.
- [~ T] Exact-head CI and local execution are not yet verified. Local Commander remains offline; GitHub Actions status/log access is still insufficient to claim a test result.
- [x C] This validates the provider-side snapshot-pin admission guard only. It does not exercise cross-store GC, prove external application references, or enable physical tombstone deletion; the physical-GC switches remain hard false.

## 1.60. T-pass: recover immutable outbound reservations across restart — 2026-11-10

- [x T] Extended the tombstone-GC metadata restart test to persist a peer's immutable outbound reservation after completing its baseline, close the Metadata store, and reopen the same durable partition.
- [x T] The recovered namespace's live-reference scan must fail closed while that reservation is pending, preventing tombstone GC from proceeding across a payload copy retained by the reservation.
- [~ T] Exact-head CI and local execution remain unverified; the BIN-SYNC commits have no status checks exposed by the current GitHub connector, and Local Commander remains offline.
- [x C] This covers reservation recovery and fail-closed GC gating only; it does not enable physical GC or prove reservation retirement/rejoin horizons.

## 1.59. T-pass: verify tombstone-GC metadata references survive restart — 2026-11-10

- [x T] Added `Tombstone_gc_live_reference_scan_recovers_pending_apply_and_outbound_payloads_after_restart`. The test writes a PendingApply payload and an immutable outbound payload, closes the metadata store, reopens the same durable partition, and verifies both RecordIds remain live in their respective namespaces.
- [x T] The test also verifies namespace isolation after recovery: PendingApply references do not leak into the outbound namespace and outbound references do not leak into the PendingApply namespace.
- [~ T] Exact-head CI and local execution are not yet verified. Local Commander remains offline; the FLD-DB workflow diagnostic run still has failed Ubuntu/Windows jobs with unavailable logs and a queued macOS job.
- [x C] This is restart coverage for the Metadata live-reference scan only. It does not exercise physical tombstone compaction or enable any ChangeId, receipt, tombstone or Ghost GC switch; all physical-GC switches remain hard false.
- [~ T] Remaining §1.23.6 work: end-to-end external host/provider references, immutable reservation recovery, snapshot pins, receipt retirement/rejoin and replay fences, newer-field preservation through compaction, fault-injection boundaries and multi-member offline/rejoin horizons.

## 1.58. T-pass: pin tombstone-GC field-selection ordering — 2026-11-10

- [x T] Added `Tombstone_gc_field_selection_removes_only_fields_not_newer_than_record_tombstone` to the Storage provider conformance suite. It proves fields older than or equal to a record tombstone are eligible under the ordering predicate, while a strictly newer field survives; mismatched record identity, tombstone-as-field and non-tombstone comparison inputs fail closed.
- [x T] Exact BIN-SYNC head `0db113028c55ebc0db3ee5f0d6b84828230d06ac`: BinSync.Net and FLD Storage Integration GitHub Actions completed successfully.
- [x T] This closes the isolated field-selection predicate matrix only. It does not exercise physical tombstone compaction or authorize deletion; all physical ChangeId, receipt and tombstone/Ghost GC switches remain hard false.
- [~ T] Remaining §1.23.6: end-to-end tombstone compaction preserving newer Ghost fields, restart/fault injection, external host/provider/Metadata references, PendingApply and immutable reservations, snapshot pins, receipt retirement/rejoin and cursor/digest replay fences, and multi-member offline/rejoin horizons.
- [~ T] `FldBinSyncHost` exact-head build remains unverified while `FLD_DB_READ_TOKEN` is unavailable.
- [x C] Physical GC capability switches remain hard false.

## 1.57. T-pass: require complete draft-free issued-origin evidence for tombstone horizons — 2026-11-10

- [x T] `Full_replace_retention_checkpoint_rejects_missing_active_member_frontier_report` and `Full_replace_retention_checkpoint_rejects_active_member_with_unpublished_drafts` prove a FULL_REPLACE retention checkpoint cannot be formed without every Active member's report and a zero-draft report from each member.
- [x T] `Tombstone_gc_proof_requires_signed_issued_origin_maxima` proves applied frontiers alone do not establish an origin horizon: without a signed author-issued epoch maximum, no tombstone proof is produced.
- [x T] Together with §1.56, the signed tombstone protocol proof now rejects missing Active ACK/report, nonzero drafts, absent issued-origin maxima, incomplete common applied prefix, HLC overclaims and FULL_REPLACE generation/digest mismatch.
- [x T] Exact BIN-SYNC head `3b0481f78de836d560a23405df5acf834f04a105`: Core 678/678 on Linux and Windows; Storage Integration 219/219 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] These are protocol proof tests only. Physical ChangeId, receipt and tombstone/Ghost GC switches remain hard false; no rows were deleted.
- [~ T] Remaining §1.23.6: external host/provider/Metadata references, PendingApply and immutable reservations, snapshot pins, receipt retirement/rejoin and cursor/digest replay fences, tombstone newer-field preservation, restart/fault injection and multi-member offline/rejoin horizons.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical GC capability switches remain hard false.

## 1.56. T-pass: harden tombstone GC signed horizon proof cases — 2026-11-10

- [x T] `Tombstone_gc_proof_rejects_hlc_and_full_replace_identity_overclaims` proves a signed tombstone horizon is rejected for HLC wall/logical overclaims and for a different FULL_REPLACE generation or digest.
- [x T] `Tombstone_gc_proof_requires_every_active_member_installed_ack` proves a FULL_REPLACE retention checkpoint is rejected when any Active member's installed ACK is missing.
- [x T] `Tombstone_gc_proof_requires_all_active_replicas_to_apply_the_final_issued_prefix` distinguishes a valid lower common applied prefix from the final issued sequence: the former can be retained as a checkpoint frontier but is insufficient to reclaim a tombstone.
- [x T] Exact BIN-SYNC head `c4b870018a0f520b17d81e62d11727f222587165`: Core 678/678 on Linux and Windows; Storage Integration 216/216 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] Tests exercise signed protocol proof construction only. All physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; no rows were deleted.
- [~ T] Remaining §1.23.6: external host/provider/Metadata references, PendingApply and immutable reservations, snapshot pins, receipt retirement/rejoin and cursor/digest replay fences, tombstone newer-field preservation, restart/fault injection and multi-member offline/rejoin horizons.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical GC capability switches remain hard false.

## 1.55. T-pass: verify fail-closed ChangeId GC provider-feed scanning — 2026-11-10

- [x T] `Change_id_gc_provider_scan_rejects_malformed_feed_sequences` verifies duplicate, regressing and out-of-range provider sequence values throw `InvalidDataException` before a live-reference set can be used for reclamation.
- [x T] `Change_id_gc_provider_scan_keeps_later_live_ids_after_empty_advancing_page` proves an empty but advancing lifecycle page does not hide a later live ChangeId. `Change_id_gc_provider_scan_blocks_reclamation_for_legacy_unknown_change_id` confirms a zero/unknown ChangeId blocks the proof and clears the candidate live set.
- [x T] Exact BIN-SYNC head `2c3d09273d85616db37103b2222cfa4079e4a1b5`: Core 678/678 on Linux and Windows; Storage Integration 213/213 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] This tests the provider-feed validator independently because `IsChangeIdGcProofEnabled` remains hard false. No ChangeLedger rows were reclaimed and no physical-GC switch was enabled.
- [x T] §1.23.5 malformed-provider scan T-pass is closed.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.54. T-pass: recover prior MCP1 Metadata publication markers — 2026-11-10

- [x T] `Coordinator_migrates_legacy_metadata_publication_marker_into_provider_on_reopen` seeds the prior implementation's MCP1 marker in the Metadata keyspace with matching Node and outbound watermarks, then reopens the database. Coordinator construction migrates the marker to the provider-owned publication marker and reinstalls the Node write fence.
- [x T] `Coordinator_rejects_conflicting_legacy_and_provider_publication_markers` proves conflicting MCP1 and provider marker watermarks fail closed; neither marker is silently overwritten.
- [x T] Exact BIN-SYNC head `2a8d51d0a27a17a91c404ec1dc50ef4407dd70c8`: Core 678/678 on Linux and Windows; Storage Integration 208/208 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] §1.23.8 legacy Node digest migration T-pass is closed: base/alias row reopen, crash recovery before/after Node checkpoint, 4096+1 batch recovery, repeated cutover, `SeenChanges` preservation and prior MCP1 marker migration/conflict handling are covered in §§1.42, 1.44, 1.45, 1.53 and 1.54.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.53. T-pass: recover legacy alias cutover from the 4096/1 commit boundary — 2026-11-10

- [x T] `Provider_cutover_recovers_after_crash_between_4096_and_final_legacy_alias_batches` constructs 4097 Node ledger identities and reproduces the exact durable boundary after the first 4096-alias Metadata batch commits but before the final one-alias batch. It then closes and reopens Storage/Metadata/Node as if the process had terminated at that point.
- [x T] Reopen confirms the first batch is durable and conflict-protected. Retrying the real coordinator cutover idempotently replays that batch, writes the final alias, then completes Node ledger cleanup; conflicting aliases for both boundary entries are rejected.
- [x T] A second process-style reopen and repeated cutover remains idempotent; `SeenChanges` is preserved throughout.
- [x T] Exact BIN-SYNC head `5b197e2b69201dfff151f465b68c96a5b8c564d4`: Core 678/678 on Linux and Windows; Storage Integration 206/206 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] Remaining §1.23.8: recovery of markers written by the prior implementation. Current crash-boundary coverage now includes before/after the Node checkpoint, after the first 4096-alias batch and after all alias batches; physical GC remains disabled.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.52. T-pass: close provider frontier pagination and captured-prefix publication matrix — 2026-11-10

- [x T] `Applied_origin_frontier_scans_multiple_empty_advancing_pages_before_zero_or_later_drafts` verifies that two consecutive empty advancing pages do not terminate the captured-prefix scan; the final page is still inspected. Its zero-draft variant proves a zero report is emitted only after the full captured high-watermark has been scanned.
- [x T] `Applied_origin_frontier_continues_after_short_advancing_page` verifies a short page that advances the cursor does not terminate scanning; a later page's draft and maximum HLC are included.
- [x T] `Publish_local_changes_acknowledges_empty_advancing_provider_prefix_after_retry` verifies an empty but advancing lifecycle prefix is ACKed; an injected ACK failure leaves the prefix pending, and retry commits the same scanned-through cursor without creating Node Outbox entries.
- [x T] `Publish_local_changes_leaves_appends_beyond_captured_high_watermark_for_next_pass` appends a second provider row during the first page read. The first pass publishes/ACKs only the captured prefix; the appended suffix remains pending and is published by the next pass.
- [x T] Exact BIN-SYNC head `41733e0c33528cbd0462a2df147c33a1f458f649`: Core 678/678 on Linux and Windows; Storage Integration 205/205 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] §1.23.1 provider-page, empty-prefix ACK, captured-high-watermark and zero-draft coverage matrix is closed by §§1.50–1.52.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured; the workflow wrapper's green conclusion is not a host-adapter compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.51. T-pass: reject malformed provider pages during applied-frontier scans — 2026-11-10

- [x T] `Applied_origin_frontier_rejects_malformed_provider_scan_pages` exercises six hostile provider-page shapes: a stalled cursor, a regressed cursor, a cursor beyond the captured provider high-watermark, unordered rows, a row beyond the page's scanned prefix, and an oversized page.
- [x T] Every malformed page fails closed with `InvalidDataException`; the host reference scan is disposed and the non-reentrant host lease is released exactly once on every failure.
- [x T] Exact BIN-SYNC head `63502d93f67de72c73268916f9b1da3fb0940c6a`: Core 678/678 on Linux and Windows; Storage Integration 200/200 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] Remaining §1.23.1 provider-page and publication-prefix cases are closed in §1.52: later empty/short advancing pages, empty scanned-prefix ACK retry, post-capture appends deferred to the next pass, and zero-draft reports only after complete captured-prefix coverage.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.50. T-pass: verify applied-frontier scans continue after empty advancing provider pages — 2026-11-10

- [x T] `Applied_origin_frontier_scans_empty_advancing_page_before_later_provider_drafts` uses a provider wrapper that returns an empty page while advancing `ScannedThroughSequence` over the first feed row, then returns later draft rows on subsequent pages.
- [x T] The signed frontier reports both later drafts and their maximum HLC rather than incorrectly publishing a zero-draft report or treating the empty page as end-of-feed. The host reference scan is disposed and the non-reentrant host lease released.
- [x T] Exact BIN-SYNC head `acfe3be1b10b85135dc21e4092d2603146f9399b`: Core 678/678 on Linux and Windows; Storage Integration 194/194 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [x T] The broader §1.23.1 provider-page and publication-prefix matrix is closed in §§1.51–1.52; see §1.52 for empty/short page continuation, empty-prefix ACK retry, captured-high-watermark behavior and zero-draft coverage.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.49. T-pass: prove peer retirement retains receipts for rejoin replay — 2026-11-10

- [x T] Renamed and corrected `Retiring_peer_epoch_preserves_receipts_and_fences_only_the_target_namespace`. It proves `RetireInterestedPeer` fences the old transport incarnation without eagerly deleting its durable receipt; replay under the new transport epoch is recognized as the same duplicate.
- [x T] The target namespace's old-epoch replay is rejected, while its new-epoch replay reuses the receipt; a second namespace retains its independent receipt/fence state.
- [x T] Exact BIN-SYNC head `aa88175202148bf140508683f218a821b175615d`: Core 678/678 on Linux and Windows; Storage Integration 193/193 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] Receipt-GC eligibility with and without external receipt references, current/rejoined roster proof, missing/regressed fences, PendingApply, cursor gaps and signed origin-frontier coverage remains open. `IsReceiptGcProofEnabled` stays hard false, so no receipt deletion is authorized.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.48. T-pass: verify offline roster catch-up transitions preserve GC safety fences — 2026-11-10

- [x T] Extended `Offline_rejoin_accepts_signed_roster_gap_but_resets_every_gc_horizon` to exercise Retired → Joining → Active across signed generation gaps, including rejection when the rejoining epoch does not advance beyond the retired fence or a Joining → Active promotion regresses its epoch.
- [x T] Covers Active → Joining across a gap only with an advanced transport epoch; a newly observed member may arrive already Retired across a signed gap and may become Active only after advancing beyond its own retired fence.
- [x T] Verifies retired-origin closure deletion and member-key rotation are rejected; old reports, retention checkpoints, installed ACKs and origin coverage are not inherited across the new roster generation, including after reopen.
- [x T] Exact BIN-SYNC head `6956dec5b8abad53f93df09450d1c5c887ad432c`: Core 678/678 on Linux and Windows; Storage Integration 193/193 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] This verifies roster catch-up and fail-closed horizon reset only. Fresh current-generation reports/checkpoints/ACKs must still be established before any retention proof; physical GC remains disabled.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.47. T-pass: prove applied-frontier scan holds one non-reentrant host lease — 2026-11-10

- [x T] `Applied_origin_frontier_uses_one_non_reentrant_host_lease_and_releases_on_scan_failure` accepts a signed one-member roster, publishes an applied-origin frontier with a zero-draft host snapshot, and verifies the scanner opens while the non-reentrant host gate is already held. The scan is disposed before the single host lease is released.
- [x T] A scanner exception propagates while releasing the host lease exactly once; a subsequent application-guard acquisition succeeds, proving coordinator, Node, provider and host gates were all released. This also proves no nested host-gate acquisition occurs inside the scanner.
- [x T] Exact BIN-SYNC head `5eab643ac300e013f4067ee59582f5ddc3c7f429`: Core 678/678 on Linux and Windows; Storage Integration 193/193 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] This validates host lease/disposal ordering for frontier publication; it does not prove signed offline/rejoin retention horizons or authorize physical GC.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.46. T-pass: verify composed GC barrier under concurrent receive, provider and host commits — 2026-11-10

- [x T] `Composed_gc_barrier_serializes_receive_provider_host_and_all_three_gc_callbacks` holds the composed coordinator → Node → host → provider lease while independent receive, provider append, host commit, ChangeId GC, receipt GC and tombstone GC operations are started.
- [x T] Receive, provider and host writes remain blocked until the outer lease is released. ChangeId and receipt GC callbacks wait for and then acquire the same composed lease; both return without reclamation because their proof switches are disabled. Tombstone GC returns zero before lease acquisition while its proof switch is hard-disabled. No external reference scan is opened.
- [x T] Explicit null host-lease acquisition fails closed for ChangeId and receipt GC; tombstone GC requests no lease while disabled. Acquired host leases are released exactly once; all concurrent tasks complete without deadlock.
- [x T] Exact BIN-SYNC head `6b2215631511c919432794d3b0d5e126f8c187ef`: Core 678/678 on Linux and Windows; Storage Integration 192/192 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] This proves serialization and fail-closed lock acquisition only. Adversarial signed-frontier, external-reference, offline/rejoin and physical-deletion proofs remain open; no GC switch is enabled.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.45. T-pass: verify legacy Node alias migration across the 4096-entry batch boundary — 2026-11-10

- [x T] `Provider_cutover_migrates_legacy_aliases_across_the_4096_entry_batch_boundary` creates 4097 local immutable operations, aligns their semantic ChangeId identities and author watermarks in Metadata, then exercises the coordinator's 4096 + 1 alias persistence batches.
- [x T] Simulated process loss after both alias batches are durable but before the post-cutover Node checkpoint. Reopen proves all 4097 Node identities remain available for retry, `SeenChanges` is preserved, and conflicting aliases for both the first and final ChangeIds are rejected; exact aliases remain idempotent. Retrying cutover completes cleanup.
- [x T] Exact BIN-SYNC head `8fcdcb76a605736176a968f88423c1a1bec02071`: Core 678/678 on Linux and Windows; Storage Integration 191/191 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] Remaining §1.23.8 alias proof: inject failure/crash specifically between the first 4096-entry batch commit and the final one-entry batch commit, repeat multi-batch cutover after reopen, and recover markers written by the prior implementation.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured; the wrapper job's green status is not a compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.44. T-pass: recover legacy Node ledger alias cutover across Node checkpoint crashes — 2026-11-10

- [x T] `Provider_cutover_recovers_legacy_ledger_aliases_across_node_checkpoint_crash` exercises both crash boundaries after the provider-authority marker: process loss before the post-cutover Node checkpoint, and process loss immediately after that checkpoint is durable.
- [x T] Reopen proves the expected pre-/post-checkpoint Node ledger state, preserves `SeenChanges`, and retries cutover idempotently. A conflicting alias is rejected after reopen while reattaching the exact alias is idempotent, proving the legacy digest was durably attached before Node ledger cleanup.
- [x T] Exact BIN-SYNC head `cd2ba7b481b98843c8c4e51b6f5f9dcd307d50e5`: Core 678/678 on Linux and Windows; Storage Integration 190/190 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 signed control-plane golden vectors and fuzz/lab passed.
- [~ T] Remaining §1.23.8 alias proof: interruption/reopen around the 4096-entry alias-batch boundary, repeated multi-batch cutover, and recovery of markers written by the prior implementation. The current crash test uses one alias and closes only the Node-checkpoint boundaries.
- [~ T] `FldBinSyncHost` exact-head build remains skipped because `FLD_DB_READ_TOKEN` is not configured; green wrapper status is not a host-adapter compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.43. T-pass: verify single-journal ownership for both receive modes — 2026-11-10

- [x T] `Metadata_authoritative_receive_uses_pending_apply_without_duplicate_node_remote_batch_wal` compares the actual legacy `SyncSession.ReceiveBatchAsync` path with Metadata-authoritative coordinator receive. Legacy mode writes exactly one `RemoteBatch` WAL record and populates the Node `GlobalChangeLedger`; Metadata-authoritative mode writes zero Node `RemoteBatch` records, leaves Node `GlobalChangeLedger` empty and creates no Node relay-Outbox mirror.
- [x T] Existing `Process_kill_at_receive_durability_cut_recovers_pending_apply_and_provider_state` passes all four process-kill boundaries: after PendingApply, after Node checkpoint, after provider commit and after Metadata finalize. Reopen checks which side had committed, then recovers the exact payload into both provider and Node without a duplicate apply.
- [x T] Exact BIN-SYNC head `8e2f901c860056d37aceba8b1b8799af0956922b`: Core 678/678 on Linux and Windows; Storage Integration 188/188 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [~ T] The remaining cross-repository `FldBinSyncHost` build is still skipped until `FLD_DB_READ_TOKEN` is configured as a fine-grained read-only Contents token for `smersh1307n2/FLD-DB` in BIN-SYNC repository secrets.
- [x C] Metadata-authoritative receive remains single-journal: PendingApply is the protocol recovery journal; no duplicate Node RemoteBatch WAL, GlobalChangeLedger or relay-Outbox mirror is written.

## 1.42. T-pass: verify legacy Node digest alias codec and reopen — 2026-11-10

- [x T] `Legacy_node_ledger_alias_round_trips_and_rejects_conflicts_after_reopen` creates a durable ChangeLedger identity, reopens the base row, attaches the legacy Node digest alias, reopens the alias-extended row, and proves identical reattachment is idempotent. Conflicting, all-zero and duplicate ChangeId aliases fail closed.
- [x T] Exact BIN-SYNC head `0078c96e0edbf74cdd5944fe7b830df911f388be`: Core 678/678 on Linux and Windows; Storage Integration 187/187 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [~ T] Remaining §1.23.8 alias proof: crash cuts before/after bounded alias batches and Node checkpoint, repeated cutover/recovery, prove Node ledger clears only after every alias is durable while `SeenChanges` remains intact, and recover markers written by the prior implementation.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified because `FLD_DB_READ_TOKEN` is empty; private FLD-DB checkout/build is explicitly skipped. Configure a fine-grained read-only Contents token as the BIN-SYNC repository secret and rerun.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false.

## 1.41. T-pass: close receiver typed FULL_REPLACE lease identity matrix — 2026-11-10

- [x T] Parameterized `Full_replace_receiver_rejects_mismatched_typed_lease_before_provider_switch_or_ack` across five independent mismatches: scope, local replica, namespace, namespace-GC store instance and trust-store instance. Every case rejects and disposes the lease before opening the host reference scanner, committing provider state or writing an installed ACK.
- [x T] Exact BIN-SYNC head `eeb18deb80fe54f9429560c0a56c4e3cde631142`: Core 678/678 on Linux and Windows; Storage Integration 186/186 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [x T] The receiver-side typed-lease identity/scope/store/trust mismatch matrix from §1.21 is closed. The separate concurrent Metadata writer and namespace-GC binding checks are covered in §1.39.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified. The latest workflow explicitly reports `FLD_DB_READ_TOKEN` empty and skips private FLD-DB checkout/build. Configure a fine-grained read-only token with Contents: read access to `smersh1307n2/FLD-DB` as the BIN-SYNC repository secret `FLD_DB_READ_TOKEN`, then rerun the exact-head host-adapter build.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; completing receiver lease validation does not authorize physical deletion.

## 1.40. T-pass: reject receiver-side FULL_REPLACE lease scope mismatch — 2026-11-10

- [x T] `Full_replace_receiver_rejects_mismatched_typed_lease_before_provider_switch_or_ack` builds and accepts a signed two-member roster, begins an empty FULL_REPLACE transfer, then supplies an `IFullReplaceCommitLease` whose scope differs from the receiver. Commit rejects it, disposes the lease, does not open the host reference scanner, leaves provider staging uncommitted and writes no installed ACK.
- [x T] Exact BIN-SYNC head `9f916d45671d8cca9aee1931f5071d6123c973c6`: Core 678/678 on Linux and Windows; Storage Integration 182/182 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [x T] Receiver typed-lease mismatch matrix (scope, local replica, namespace, namespace-GC store instance and trust-store instance) is closed in §1.41.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified: `FLD_DB_READ_TOKEN` is absent, so the workflow skips private FLD-DB checkout and host-adapter build. The wrapper job's green conclusion is not a compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; receiver lease rejection does not authorize physical deletion.

## 1.39. T-pass: verify FULL_REPLACE capability binding and Metadata writer exclusion — 2026-11-10

- [x T] `Full_replace_commit_guard_requires_exact_namespace_gc_binding_and_releases_host_lease` proves that an unattached namespace-GC capability is rejected before the host lease callback runs; a store attached to the wrong scope is rejected after host acquisition, and the host lease is released on failure.
- [x T] `Full_replace_commit_guard_blocks_concurrent_metadata_writers_until_disposed` holds the typed FULL_REPLACE barrier while a concurrent `BeginPendingApply` writer attempts to enter Metadata. The writer remains blocked until lease disposal, then completes and persists its PendingApply.
- [x T] Exact BIN-SYNC head `f319ce88acbacf70bbae7248fbbed757812d8da0`: Core 678/678 on Linux and Windows; Storage Integration 181/181 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [x T] Receiver typed-lease mismatch matrix (scope, local replica, namespace, namespace-GC store instance and trust-store instance) is closed in §1.41.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified: `FLD_DB_READ_TOKEN` is absent, so the workflow skips private FLD-DB checkout and host-adapter build. The wrapper job's green conclusion is not a compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; these barrier tests do not authorize physical deletion.

## 1.38. T-pass: verify FULL_REPLACE reservation and completed-receipt barriers — 2026-11-10

- [x T] `Full_replace_commit_guard_blocks_pushed_unacknowledged_peer_reservations` proves that both a local outbound reservation and a foreign-author relay-stream reservation block FULL_REPLACE while pushed but unacknowledged, preserve their cursor/reservation state on rejection, and stop blocking only after their matching ACKs atomically advance the acknowledged cursors and clear both pending reservations. The host lease is released on rejection and normal disposal.
- [x T] `Full_replace_commit_guard_does_not_treat_completed_receipts_as_pending_work` stages and finalizes a Metadata PendingApply, then proves the retained completed receipt is idempotency history rather than pending provider work and does not block FULL_REPLACE.
- [x T] Exact BIN-SYNC head `7da5e680d63de7f13ef7b26b912ddb6463564a86`: Core 678/678 on Linux and Windows; Storage Integration 179/179 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [x T] Receiver typed-lease mismatch matrix (scope, local replica, namespace, namespace-GC store instance and trust-store instance) is closed in §1.41.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified: `FLD_DB_READ_TOKEN` is absent, so the workflow skips private FLD-DB checkout and host-adapter build. The wrapper job's green conclusion is not a compile result.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; these FULL_REPLACE barrier tests do not authorize physical deletion.

## 1.37. T-pass: verify namespace-scoped FULL_REPLACE PendingApply barrier — 2026-11-10

- [x T] Added `Full_replace_commit_guard_blocks_only_pending_apply_in_target_namespace`: a durable PendingApply in another namespace does not block the target namespace's FULL_REPLACE lease; a PendingApply in the target namespace rejects the lease, and the acquired host lease is released on rejection. Both pending intents remain durable and isolated.
- [x T] Exact BIN-SYNC head `ea1d0d1f51fa80c17aee0a3cfbb67b3453d2c8d2`: Core 678/678 on Linux and Windows; Storage Integration 177/177 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; fuzz/lab and v3.1 signed control-plane vectors passed.
- [x T] Receiver typed-lease mismatch matrix (scope, local replica, namespace, namespace-GC store instance and trust-store instance) is closed in §1.41.
- [~ T] The `FldBinSyncHost` cross-repository build remains unverified: the workflow's private FLD-DB checkout/build steps are skipped when `FLD_DB_READ_TOKEN` is absent. A green wrapper job is not a successful host-adapter build.
- [x C] Physical ChangeId, receipt and tombstone/Ghost GC proof switches remain hard false; this test does not authorize physical deletion.

## 1.23.14. C-pass closeout: transition v3/v3.1 implementation to exact-head T-pass — 2026-11-10

- [x C] The current v3/v3.1 implementation track is complete through the latest integration phases: provider publication/cutover, host scheduling, Metadata-authoritative receive, FULL_REPLACE sender recovery and receiver staging, generic Storage blobs, provider snapshot durability, and API/source/byte audits. Earlier `[~ C]` items in the chronological history are superseded by their later completion records; they are not active implementation blockers.
- [x C] Reconfirmed all three physical-GC proof switches remain hard `false`. Source-level eligibility and cross-store reference/lock-order audits are complete, but this does not prove safety under adversarial histories and does not authorize deletion.
- [~ T] Next phase is the exact-head T-pass on the paired BIN-SYNC/FLD-DB commits: clean builds, core/storage integration, sample execution, codec reopen, restart/fault-injection matrix, concurrent lock/lease and offline/rejoin GC-horizon proofs, then benchmarks. Keep physical receipt/ChangeId/tombstone/Ghost GC disabled until those proofs pass.

## 1.23.13. C-pass: prevent Run ID reuse against catalog-less blob directories — 2026-11-10

- [x C] `StorageBlobStore.ContainsBlob` now treats both catalog rows and catalog-less page directories as occupied IDs and serializes the check with Put/Delete through the shared per-prefix gate. `PutImmutableBlob` refuses to recreate a catalog row over an orphan page directory.
- [x C] This closes the restart case where an interrupted catalog-first delete removed the metadata row but failed to remove the page directory; a root-global FLD Run allocator can no longer reuse that ID and collide with stale immutable pages.
- [~ T] Add failure-injection coverage for catalog deletion followed by page-directory delete/fsync failure, reopen, allocator skip, orphan cleanup retry, and successful allocation only after the directory is gone. No build/tests run in this C slice.

## 1.23.12. C-pass: complete the BIN-SYNC metadata/provider/transport API-source-byte audit — 2026-11-10

- [x C] Public cutover contract now names both optional requirements (`IMetadataChangeIdentityStore` all-route lookup and `IMetadataLegacyNodeLedgerStore` alias persistence); `StorageMetadataStore` implements both and custom stores must fail closed.
- [x C] Source audit confirms `TcpSyncSession` is absent from source and samples; legacy Node-owned receive alone writes `RemoteBatch` WAL, while Metadata-authoritative receive uses PendingApply without a duplicate Node WAL/ledger/relay mirror.
- [x C] Byte audit reconciles ChangeLedger's 45-byte base and 61-byte cutover-alias value in the normative spec, source loader, architecture docs and byte inventory; sample checkpoint documentation now says NodePersistence v17.
- [x C] Reviewed current public sample entry points and the FLD host provider's scanned-cursor behavior. No sample build/run was performed in this C-pass.
- [~ T] Exact-head build/test, sample execution, byte-codec reopen tests, crash matrix, concurrency/lock-contention tests and performance measurements remain mandatory. Physical receipt/ChangeId/tombstone/Ghost GC remains disabled.

## 1.23.11. C-pass: reconcile public API and sample entry points — 2026-11-10

- [x C] Confirmed the removed `TcpSyncSession` has no source/sample references; the public transport surface is the generic `SyncSession` API. The only remaining name occurrence is the breaking-change note in `PUBLIC-API.md`.
- [x C] Audited the three user-facing paths: `StorageArraySync` remains the intentional legacy Node-Outbox transition example; `StorageArrayTcpSync` demonstrates provider-authoritative Metadata-owned outbound plus Metadata-authoritative receive; `TwoNodeTcp` demonstrates the generic transport-only session API.
- [x C] Updated `StorageArraySync/README.md` from the obsolete NodePersistence v15 label to the normative v17 checkpoint version. FLD host sample's provider cursor uses `ScannedThroughSequence` and treats lifecycle-only Outbox rows as scanned but non-publishable.
- [~ T] Build and run all three examples plus `FldBinSyncHost`; verify the legacy transition and provider-authoritative paths against current v17/WAL v3 binaries. No sample build/run in this C slice.

## 1.23.10. C-pass: close cross-store GC lock-order and liveness-snapshot audit — 2026-11-10

- [x C] Verified all three physical-GC entry points acquire the composed application lease before their internal store gates: ChangeId GC then pins provider snapshots before Metadata lock; receipt GC then enters Metadata lock; tombstone GC enters the provider gate after the composed lease. No GC callback reacquires the host lease while a scan is active.
- [x C] Verified the common order is coordinator → Node gates → host WAL/FLD lease → provider gate → Metadata/GC store gate; ordinary coordinator receive/publication paths take coordinator before Metadata, so the GC methods do not invert that order.
- [x C] Verified ChangeId and tombstone scans open one host reference snapshot under the composed lease, and provider snapshot pins are excluded for ChangeId GC while tombstone GC rejects active provider readers. Receipt GC retains receive cursor/digest-chain, PendingApply and host receipt-reference fences.
- [x T] Deterministic composed-barrier concurrency, host/provider lease ordering, receive/provider/host writer exclusion, all three GC entry points, null-lease fail-closed behavior and deadlock-free release are covered in §1.46. Physical GC remains disabled; signed-horizon and external-reference proofs are separate gates.

## 1.23.9. C-pass: reconcile Node-owned and Metadata-owned receive WAL boundaries — 2026-11-10

- [x C] Audited `Node.ApplyBatchInternal`: legacy Node-owned receive persists accepted `RemoteBatch` payloads in Node WAL; Metadata-Store-authoritative receive uses the durable `PendingApply` intent as its sole protocol recovery journal and deliberately skips duplicate Node `RemoteBatch` WAL, Node `GlobalChangeLedger` and Node relay-Outbox mirroring.
- [x C] Confirmed the metadata-authoritative path applies the durable per-operation ChangeId decision mask, checkpoints Node cache before provider apply, finalizes the Metadata receipt only after provider success, and replays the exact stored wire payload through Node/provider during PendingApply recovery.
- [x C] Reconciled `PUBLIC-API.md` so the two receive modes and their single-journal ownership are explicit; removed the ambiguous statement that Metadata-authoritative replay also writes a Node RemoteBatch WAL entry.
- [x T] Both receive modes are verified in §1.43: legacy Node-owned receive writes exactly one RemoteBatch WAL record; Metadata-authoritative receive writes none, does not mutate Node GlobalChangeLedger/relay-Outbox, and uses PendingApply as its only protocol recovery journal. All four process-kill boundaries recover the exact payload.

## 1.23.8. C-pass: migrate the legacy Node digest into the Metadata ChangeLedger row — 2026-11-10

- [x C] Added optional `IMetadataLegacyNodeLedgerStore`; `StorageMetadataStore` stores the old 16-byte origin-bound Node digest as an optional suffix on the existing ChangeLedger value (45-byte base / 61-byte alias encoding), not in a second key/table.
- [x C] Cutover preflight still validates every retained Node Outbox payload against the exact legacy digest and Metadata semantic fingerprint/origin/ownership. After all payload and relay checks pass, it idempotently persists an alias for every known Node ledger identity, including already-compacted history whose payload cannot be re-hashed.
- [x C] Only after all aliases are durable does the fenced Node clear its namespace `GlobalChangeLedger`; the caller checkpoints Node immediately. Crash before Node checkpoint leaves the old ledger and aliases for exact idempotent retry; crash after checkpoint leaves the legacy digest co-located in Metadata. `SeenChanges` remains untouched.
- [x C] Alias and semantic identity are encoded in one ChangeLedger row and are deleted together by the existing ChangeId GC path. Compacted legacy aliases are preserved, not falsely claimed equivalent to the semantic fingerprint.
- [x C] Fixed the partition loader's early legacy-row gate to accept both 45-byte base and 61-byte aliased ChangeLedger values; the later row decoder already validates the suffix shape and rejects zero aliases. Without this gate fix, a successfully committed alias would fail on Storage reopen.
- [x T] Legacy Node digest migration T-pass is closed by §§1.42, 1.44, 1.45, 1.53 and 1.54, including prior MCP1 marker migration and conflict rejection.

## 1.23.7. Cross-repository C-pass: narrow the FLD Storage public API — 2026-11-10

- [x C] Reconciled the old 2026-11-09 status snapshot: the approved two-DLL split and concrete FLD `IDataProvider`/FULL_REPLACE bridge are now implemented on the paired FLD/BIN-SYNC branches (FLD roadmap §§7.93–7.103); the old “remaining C work” text is explicitly historical.
- [x C] FLD source audit found generic WAL internals exposed as public types despite the architecture's small byte-KV API boundary. `WalManager`, `WalWriter`, `WalRecovery`, recovery result/status/record types and WAL header/envelope/constants are now internal on `FLD-DB/fix/crash-consistency-outbox`.
- [x C] FLD public API remains the byte-KV database/batch/snapshot/blob surface plus the explicit partition lease and shared directory durability helper required by Database-owned files. `Fld1.Database` has no dependency on generic Storage WAL implementation types; the integration test friend assembly retains implementation-level access.
- [~ T] Reflection/API-surface test, standalone Storage build, Database build and cross-repository `FldBinSyncHost` exact-head build remain pending. No build/test was run in this C slice.

## 1.23.6. C-pass: source-audit all three physical GC horizons — 2026-11-10

- [x C] ChangeId ledger reclamation is gated by the signed origin retention checkpoint and the union of provider-feed, relay, outbound, PendingApply and external host ChangeId references. It blocks on immutable peer reservations and active provider snapshot pins; provider scans run to the captured high-watermark and validate cursor progress, page bounds and strictly increasing row sequences.
- [x C] Receipt reclamation has one source deletion site (`CompactReceipts`). It requires the durable transport retirement fence, accepted-roster epoch fence, retained receive cursor/digest-chain replay fence, no PendingApply, no host receipt reference and a signed origin retention checkpoint. `RetireInterestedPeer` itself never deletes receipts.
- [x C] Tombstone reclamation requires a common installed FULL_REPLACE generation/digest/HLC floor, signed issued-origin maxima, signed safe-applied frontiers, every active member's draft-free report and contiguous applied coverage, plus no provider/metadata/host record or tombstone ChangeId references. Older field rows are removed before the tombstone row; newer field mutations survive.
- [x C] Confirmed all three physical-GC capability switches remain hard `false` (`IsChangeIdGcProofEnabled`, `IsReceiptGcProofEnabled`, `IsTombstoneGcProofEnabled`). This source audit does not authorize enabling any switch.
- [x T] Signed tombstone horizon protocol prerequisites are covered in §§1.56–1.57: Active reports/ACKs, zero drafts, signed issued-origin maxima, common applied prefix, HLC floor and FULL_REPLACE identity. [~ T] External host/provider/Metadata references, PendingApply/reservations, snapshot pins, receipt retirement/rejoin and replay fences, tombstone field-deletion ordering, restart/fault injection and multi-member offline/rejoin horizons remain mandatory.

## 1.23.5. C-pass: validate provider scan rows during ChangeId GC — 2026-11-10

- [x C] ChangeId GC now validates every returned provider sequence is strictly increasing and lies within the page's explicit scanned prefix before using its ChangeId as a live-reference proof.
- [x T] Malformed provider-feed sequence validation and empty-page/unknown-identity behavior are covered in §1.55; the scan validator is tested without enabling physical GC.

## 1.23.4. C-pass: separate transport retirement from receipt reclamation — 2026-11-10

- [x C] `RetireInterestedPeer` now persists the retired transport-epoch fence and removes old peer stream/baseline state without eagerly deleting receipt rows.
- [x C] `CompactReceipts` now requires the durable retired-epoch fence, confirms it against the accepted roster/current peer epoch, requires the signed origin retention checkpoint, and retains the existing PendingApply/host-reference checks. Receive cursors and digest chains remain the replay fence.
- [x C] Old receipts remain eligible for later GC after a peer rejoins under a newer transport epoch; their external references are no longer bypassed by the peer-retirement path. The byte audit now distinguishes receipt-key `SenderEpoch` (wire issuer/author epoch) from value `TransportPeerEpoch` (authenticated transport incarnation).
- [x C] Re-audited all Storage source call sites: `CompactReceipts` is the only durable inbox-receipt deletion path. It requires non-zero stored author/epoch/sequence/transport identity, a durable retirement fence, an accepted roster epoch fence, receive cursor + digest-chain retention, no `PendingApply`, no host receipt reference, and a signed origin retention checkpoint. The receipt value's `OriginSequence` is the last strictly increasing operation sequence in its single-author/single-author-epoch batch; no SenderEpoch/OriginEpoch substitution exists. `IsReceiptGcProofEnabled` remains hard false, so physical deletion is still unreachable in production.
- [x T] Retirement now proves old-epoch fencing without eager receipt deletion and successful duplicate replay under the new transport epoch; namespace isolation is covered in §1.49. [~ T] Receipt-GC eligibility with external references, current/rejoined roster proof, missing/regressed fences, PendingApply, cursor gaps and signed origin-frontier coverage remains open; the proof switch remains hard false.

## 1.23.3. C-pass: honor signed roster catch-up across generation gaps — 2026-11-10

- [x C] Membership transition validation now distinguishes consecutive roster generations from signed generation gaps. Consecutive transitions retain strict Joining/installed-ACK rules.
- [x C] Across a gap, the pinned authority attests omitted Joining/FULL_REPLACE/retirement steps; the receiver still enforces member-key immutability, non-regressing transport/retired-epoch fences and all locally known retired-origin closures.
- [x C] A Retired member may become Active across a gap only after advancing beyond its retired transport fence; Joining→Active requires a non-regressing epoch; Active→Joining requires an advanced epoch; newly seen members may already be Active/Retired only when generations were skipped.
- [x C] Updated v3.1 addendum §4.1 to make the authority-attestation boundary and consecutive-vs-gap transition rules normative.
- [x T] Offline catch-up across Retired→Joining→Active, Active→Joining, a new member already Retired, key/epoch regression and immutable closure checks is covered in §1.48. Current-generation reports/checkpoints/ACKs are confirmed absent after catch-up and reopen; physical GC remains disabled.

## 1.23.2. C-pass: keep host reference scans inside one composed lease — 2026-11-10

- [x C] `DelegateApplicationGcReferenceScan` now owns only the immutable host snapshot; the caller's composed application commit guard owns the host commit lease for the entire GC/frontier pass.
- [x C] FLD sample scanner opens `FldReferenceSnapshot` under the already-held guard and no longer reacquires `AcquireCommitGuard` through `AcquireReferenceSnapshotGuard`.
- [x C] FLD provider snapshot-GC capability now returns its reference snapshot under the caller-held guard instead of nesting a second host lease; lease disposal remains owned by the outer application barrier.
- [x C] Updated the host composition and public provider contracts to make the lease ownership explicit and avoid requiring reentrant host locks.
- [x T] Exception/disposal order and non-reentrant host-gate behavior for applied-frontier publication are covered in §1.47; ChangeId/receipt/tombstone GC serialization and fail-closed lease behavior are covered in §1.46. Tombstone GC remains hard-disabled before lease acquisition; no physical deletion is authorized.

## 1.23.1. C-pass: drive provider scans by explicit feed cursor — 2026-11-10

- [x C] Applied-origin frontier publication now scans through the captured provider high-watermark using `ScannedThroughSequence`, validates cursor progress/range and every returned sequence, and does not infer completion from a short page.
- [x C] `PublishLocalChangesCore` captures the provider high-watermark at entry and scans exactly that finite prefix, including short/empty pages that advance only the scanned cursor. Concurrent later appends remain pending for the next pass instead of extending the current scan indefinitely; malformed/stalled pages fail closed instead of acknowledging an incomplete prefix.
- [x C] If the entire captured prefix contains no syncable `ProviderChange` rows but the provider advanced `ScannedThroughSequence` over local-only lifecycle rows, the coordinator still acknowledges that empty scanned prefix. This prevents an empty prefix from being rescanned forever.
- [x C] Documented `ProviderChangePage.ScannedThroughSequence` as an independent scan cursor in the public Data Provider contract.
- [x T] Empty advancing page followed by later draft pages, with no premature zero-draft frontier, is covered in §1.50. [~ T] Cursor stalls/regression/overrun, unordered/out-of-range entries, additional short/empty pages, empty-prefix ACK in `PublishLocalChanges`, concurrent appends beyond the captured high-watermark, and full-prefix-before-zero-report proof remain open.

## 1.23. C-pass: prove retained Node Outbox payload ownership before provider cutover — 2026-11-10

- [x C] Found a destructive cutover gap: `DiscardNodeOutboxPayloadsAfterProviderCutover` removed every author stream, while the preflight compared only the local author's high-watermark. A retained Node relay payload could therefore be absent from Metadata Relay yet still be discarded.
- [x C] Added a bounded, read-only preflight over every retained Node Outbox author stream. Every row (local or foreign) must match the Metadata Store semantic ChangeId fingerprint, immutable origin and ownership mode; foreign-author rows must additionally resolve to a retained Metadata Relay sequence. Invalid/legacy identities, missing or compacted relay payloads, and mismatched fingerprints fail closed.
- [x C] Every known namespace entry in `Node.GlobalChangeLedger` must have a durable Metadata Store ChangeId identity and valid origin. Unknown or absent identities fail closed. This covers already-ACKed Node operations whose payload rows are no longer in Outbox.
- [x C] For every retained Node Outbox row, compare the exact origin-bound legacy Node ledger digest (recomputed from payload + origin) as well as the Metadata semantic fingerprint, immutable origin and ownership mode. Foreign-author rows must additionally resolve to retained Metadata Relay sequences.
- [x C] Run the proof before the durable cutover marker and again on marker recovery before startup/recovery cleanup discards the Node payload projection. A marker from an earlier process cannot silently authorize cleanup of unproven stale rows. Initial payload-proof commit: `7835c56ef289c72fd7a1e4bb833badbd447e68b5`; all-route identity capability and exact retained-row Node digest check committed as `a2efa9fd7327dd16bc5c6ac6efd61b9be8a3dd29`.
- [x C] Added a versioned legacy-digest alias to the existing Metadata ChangeLedger row (45-byte value without alias, 61-byte value with the optional 16-byte suffix). Cutover first validates every retained Node payload exactly, then durably co-locates each known legacy Node digest with its semantic fingerprint/origin row. For compacted history the two hash domains are not compared; the old digest is preserved as an explicit legacy alias, not claimed equivalent. Only after all aliases are durable does the fenced Node clear its namespace GlobalChangeLedger. Both digests are later reclaimed atomically with the same ChangeId row.
- [x C] Kept the initial cutover proof before the durable marker and the recovery proof before cleanup, but do not rerun the destructive preflight after `InstallProviderSnapshot`: that snapshot intentionally removes some pending remote IDs from Node's legacy ledger. The initial path now carries the already-proven ownership result through the fenced snapshot-to-cleanup interval; recovery still revalidates before it changes the stale Node projection.
- [~ T] Cover local/foreign fingerprint+origin+ownership match and mismatch, absent/unknown Node ledger identity, foreign relay present/missing/compacted, mixed author pages over 1,024 entries, interrupted cutover before/after marker, and restart cleanup with stale Node payloads. Assert legacy digest aliases survive Metadata reopen, the Node namespace ledger is cleared only after every alias is durable, `SeenChanges` remains intact, and interrupted cutover retries idempotently. No build/tests run in this C slice; no payload is deleted when proof is absent.

## 1.22. Cross-repository C-pass: FLD Run allocation and abort crash consistency — 2026-11-10

- [x C] FLD source audit found compaction bypassing the root-global Run allocator with `_nextRunId++`; it now uses `AllocateRunId()`, shared with checkpoint and FULL_REPLACE staging.
- [x C] FULL_REPLACE abort now persists its cursor before deleting Run bytes, recovers legacy pre-change partial-abort prefixes, excludes released prefixes from staged-run GC protection, and fails Run allocation closed when stage metadata recovery is unproven.
- [x C] FLD commit `860b25676d9a7ebe5212470e8d220505827c16b5` pushed to `fix/crash-consistency-outbox`; FLD roadmap §7.101 and the physical-format ADR record the root-global allocator and write-ahead abort contract. No WAL format or Run byte format changed.
- [~ T] Inject crash cuts around abort cursor persistence, blob/mirror deletion, cursor rewind and old-state recovery; verify corrupt stage metadata blocks both allocation and orphan GC, and root-global Run IDs remain unique across namespaces. No build/tests run in this C slice.

## 1.21. C-pass: FULL_REPLACE proves Metadata Store queues are drained — 2026-11-10

- [x C] Added the optional `IMetadataFullReplaceCommitGuard` capability. `StorageMetadataStore.AcquireFullReplaceCommitGuard` holds the durable Metadata/Storage transaction gate across the provider generation switch, requires the signed namespace-GC guard to be attached to the same StorageDatabase, and fails closed for target-namespace PendingApply intents or unresolved outbound/relay reservations (pending payload or pushed cursor beyond ACK).
- [x C] Added `StorageSyncCoordinator.AcquireFullReplaceCommitGuard(hostLeaseFactory)`, returning typed `IFullReplaceCommitLease` bound to the exact scope, local replica, namespace, namespace-GC store and trust-store instance. It composes coordinator → Node → host FLD/WAL → Metadata Store → provider gates and releases in reverse order. `StorageFullReplaceReceiver` checks the binding and accepts only this marker lease, so an ordinary `IDisposable`/application-GC guard or a lease for another namespace cannot bypass the stronger metadata proof.
- [x C] The FLD host adapter exposes `AcquireFullReplaceCommitGuard` for `StorageFullReplaceReceiver`. Source audit confirmed FLD ApplyJournal contains committed idempotency receipts, not a separate pending-provider-intent queue; the FLD commit lease excludes in-flight provider applies, and the unsealed-WAL barrier is already reflected in the host draft horizon.
- [x C] Updated the FLD integration contract, v3.1 host orchestration, roster/GC addendum and sample README with the exact API, lock order and retained-receipt distinction.
- [~ T] Add tests for PendingApply blockers (target vs other namespace), unpushed and pushed outbound/relay reservations, ACK-cleared reservations, completed receipt non-blocking, missing/unattached metadata capability fail-closed, typed receiver lease enforcement and scope/local-replica/namespace/store/trust mismatch rejection, and concurrent metadata writers blocked for the full switch lease. No build or tests run in this C slice; physical GC flags remain disabled.

## 1.20. C-pass: FULL_REPLACE commit enforces provider snapshot pins — 2026-11-10

- [x C] `StorageFullReplaceReceiver.Commit` now requires `IProviderSnapshotGcGuard` and holds its lease across host-reference scan, provider generation switch, HLC refresh and installed-ACK persistence. Missing capability or an active snapshot pin fails closed before the switch.
- [x C] `StorageDataProvider` already implements the guard; `FldDatabaseDataProvider` now implements it by acquiring an FLD reference-snapshot/commit lease and rejecting any in-memory or persisted ProviderSnapshot generation. `FldReferenceSnapshot.HasSnapshotPin` exposes the immutable proof to the adapter.
- [x C] Closed by §1.21 / BIN-SYNC commit `3206b49e`: the FULL_REPLACE-specific coordinator guard now holds the Metadata Store transaction gate and rejects target-namespace PendingApply and unresolved outbound/relay reservations. FLD source audit found no separate pending-provider-intent queue: ApplyJournal contains committed idempotency receipts, while the FLD commit lease and unsealed-WAL draft horizon cover in-flight provider work.
- [~ T] Test active StorageDataProvider snapshot, active/persisted FLD snapshot, no-pin lease excluding new opens, missing guard failure, and PendingApply/relay-reservation blocker behavior. No build/test was run in this C slice.

## 1.19. Cross-repository C-pass: repair FLD blob restart-cleanup API call — 2026-11-10

- [x C] Source audit found `FldDatabase.GcOrphanRuns` called nonexistent `StorageBlobStore.PruneOrphanPages`; FLD-DB now calls the actual bounded `PruneOrphanBlobs` API after catalog-backed unreferenced-blob pruning.
- [x C] The blob publication/deletion ordering was source-audited: pending catalog → durable pages → completion marker; catalog removal precedes page-directory cleanup, with orphan-directory collection on restart.
- [~ T] Exact-head build and crash/failure injection remain pending. Windows directory-entry durability is not claimed by the current storage directory-sync implementation and must be resolved/documented before claiming cross-platform power-loss durability.

## 1.18. C-pass: active StorageDataProvider snapshot handles block ChangeId GC — 2026-11-10

- [x C] Found a second snapshot-pin gap: `CompactChangeLedger` scanned provider feed and external references but did not check already-open `StorageDataProvider.ProviderSnapshotState` handles. The composed provider commit gate prevented new opens but did not reject an existing immutable snapshot.
- [x C] Added optional `IProviderSnapshotGcGuard`; `StorageDataProvider` acquires its gate, returns no lease while any snapshot handle is active, and holds the gate for the full ChangeId GC pass otherwise. `CompactChangeLedger` fails closed on an active pin. Host-side FLD snapshot checks remain independent.
- [~ T] Verify active snapshot prevents ledger deletion, no-snapshot guard excludes concurrent opens for the complete pass, release permits a later pass, and provider implementations without the optional capability remain protected by their host scanner. No build/test was run; physical GC flags remain false.

## 1.17. Cross-repository C-pass: persisted FLD snapshot pins block ChangeId/record GC — 2026-11-10

- [x C] Audited the FLD host reference scanner against v3.1 §1 invariant 5. It previously indexed dirty state, unacknowledged Outbox and ApplyJournal but omitted active and persisted immutable ProviderSnapshot generations.
- [x C] FLD-DB now makes every ChangeId/RecordId reference query fail closed while an in-memory or persisted snapshot generation remains. The host must recover sender token state and prune inactive generations before those horizons can advance; receipt references remain independent.
- [~ T] Verify active/restarted snapshot pins, prune/release, crash-left directory behavior and receipt-horizon independence. No build/test was run in this C slice; all physical GC flags remain false.

## 1.16. Cross-repository C-pass: validate FLD snapshot Run semantics before watermark acceptance — 2026-11-10

- [x C] Audited FLD-DB `FldDatabase.TryGetProviderSnapshotWatermark` and its persisted Run validation paths. Blob SHA-256/page integrity alone did not prove that the immutable bytes were a valid FLD Run in the expected namespace.
- [x C] FLD-DB now performs `RunReader.TryValidate` plus namespace validation for blob-backed pinned Runs before accepting the persisted snapshot watermark; incomplete blobs still fail closed and are not hidden by legacy mirrors. FLD-DB roadmap §7.98 records the implementation and the outstanding T-pass.
- [~ T] Add semantic-corruption and wrong-namespace regressions, then measure the materialization cost. No build/test was run in this C slice; no cross-repository validation is claimed.

## 1.15. C-pass audit: preserve offline NRS2 roster catch-up — 2026-11-10

- [x C] Cross-checked roster acceptance against `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` §4.1. The normative contract explicitly permits a higher signed full roster to be accepted after offline periods; requiring every intermediate local generation would break that recovery path.
- [x C] Reverted the experimental strict-contiguity change (`ee15822c`) in `e358b5a2`. The original higher-generation catch-up behavior and direct membership-transition checks are restored.
- [x C] Resolved the offline roster-gap proof under the existing trust model: a higher full roster signed by the out-of-band pinned authority is its attestation of the complete current membership state and omitted transition preconditions. Receivers retain monotonic/directly-checkable fences; skipped predecessor links are not locally verifiable without chain fetch. The addendum now states this assumption explicitly and reserves Byzantine-authority proof for a future chain/transition extension. No generation+1 rule was introduced.
- [~ T] Add offline catch-up cases spanning multiple roster generations, including retire/rejoin, transport epoch advancement, stale digest, wrong authority and current-generation replay. No tests/build were run in this C slice; physical GC remains disabled.

## 1.14. C-pass audit: preserve offline RCP2 checkpoint catch-up — 2026-11-10

- [x C] Reverted the experimental strict-contiguity change (`8de0cb2c`) in `1a68150d`. RCP2 remains monotonic and same-sequence forks are rejected; a higher signed checkpoint can still be accepted after an offline gap, subject to the existing safe-frontier monotonicity and overclaim checks.
- [x C] The paired recovery branch `backup/cpass-2026-11-10-retention-checkpoint-chain` remains available in both repositories as a snapshot of the experimental C slice; it is not the active implementation.
- [x C] Resolved offline RCP2 gaps under the pinned-authority trust model: a higher signed checkpoint with a current roster binding and monotonic safe frontier is the authority's attestation of the complete current minimum; PreviousCheckpointDigest is verified for immediately consecutive sequences, while gaps do not require fetching every intermediate checkpoint. The addendum now documents this and reserves stronger chain proof for a future extension. No sequence+1 rule was introduced.
- [~ T] Add restart/offline-gap, stale checkpoint, wrong predecessor for an immediately consecutive checkpoint, exact replay, same-sequence fork and safe-frontier regression cases. No tests/build were run in this C slice; physical GC remains disabled.

## 1.13. C-pass: generic Storage blob publication/cleanup source audit — 2026-11-10

- [x C] Created recovery branch `backup/cpass-2026-11-10-issued-origin-monotonicity` from the current feature branch before continuing the remaining C-pass audit.
- [x C] Re-read FLD-DB `StorageBlobStore` end-to-end. Immutable blob publication is ordered as pending catalog row → atomic page-file writes → page-directory fsync → complete catalog marker; readers pin the immutable generation and validate page geometry plus whole-blob SHA-256 when streaming.
- [x C] Confirmed deletion removes the catalog row before page-directory cleanup; interrupted cleanup is recoverable through orphan pruning. Pending catalog generations remain protected by catalog presence, and reader pins block deletion.
- [x C] Confirmed persisted page geometry is authoritative for existing generations; changing the configured default page size affects only new blobs. Metadata decode validates page count without overflow-prone round-up arithmetic.
- [x C] Created recovery branch `backup/cpass-2026-11-10-provider-feed-gc-fix` at the current implementation/documentation head before continuing the C-pass.
- [x C] Fixed a ChangeId-GC provider-feed scan hole: the previous loop stopped on an empty/short `Changes` collection, although FLD can advance `ScannedThroughSequence` over local-only lifecycle rows and later pages can still contain unpublished ChangeIds. GC now captures the provider high-watermark under the composed lease and scans until that cursor is reached, validating forward progress, the captured bound and page capacity. Commit: `5963d352fbae286db37d53b7f5097b58f72ddade`.
- [~ T] Add a regression case with one or more empty/short provider pages followed by a live ChangeId page, plus a concurrent-writer exclusion case. Verify no ledger row is reclaimed while the later feed identity remains pending.
- [~ T] Crash/fault proof remains open for every pending-row/page/temp-rename/directory-fsync/completion-marker boundary, catalog-delete/page-delete interruption, orphan cleanup and concurrent reader pins. Source review is not a durability test.
- [x C] Cross-store reference/liveness ordering is closed in §1.23.10: composed lease precedes store gates, host/provider reference snapshots are held consistently, and all physical-GC switches remain disabled.
- [x C] Scoped BIN-SYNC Metadata/provider/transport API-source-byte audit is closed in §1.23.12. Broader FLD Storage/Run/WAL format and FULL_REPLACE C-pass gates remain in §§1.24–1.32. Do not enable physical receipt, ChangeId or tombstone/Ghost GC.

## 1.12. C-pass: preserve issued-origin maxima across signed frontier reports — 2026-11-10

- [x C] Hardened `StorageNamespaceGcStore.AcceptOriginFrontier`: every previously reported author epoch must remain present, and its issued sequence plus maximum HLC (wall/logical tuple) may not regress in a later signed report. The applied-origin vector was already monotonic; the issued-origin vector is independently consumed by tombstone/GC proof construction and must be monotonic as well.
- [x C] Production fix committed as `47be8895046662a06481ac097229c59e2cb2ed47`.
- [~ T] Add adversarial Storage restart/conformance cases for decreasing issued sequence, decreasing HLC wall, decreasing HLC logical at equal wall, dropped epoch, and unchanged/increased values. No tests were run in this C slice; physical GC remains disabled.
- [x C] Reconciled `PUBLIC-API.md`, the host-orchestration guide and the FLD integration contract with the implemented `FldDatabaseDataProvider`/`FldRemoteBatchApplyStore` sample; stale statements that the provider bridge remained open were removed. [~ C] Complete the final XML/public API/source/byte audit; blob catalog/page failure-ordering and restart-cleanup source audit is complete in §1.19.

## 1.11. Recovery point and C-pass continuation reset — 2026-11-09

- [x C] Created `backup/pre-v3-code-sweep-2026-11-09` from `feature/fld-external-storage` before the next broad implementation pass.
- [x C] Reconciled the live branch status against `docs/PRODUCTION-STATUS.md`: latest inspected production code head is `73571b8210bf67ad7e53ba6f29bf6aa4c1056043`; durable sender recovery is in §§1.25 and 1.27, and FLD receiver staging/switch is in §1.26.
- [x C] Base FLD `IDataProvider` bridge, provider author-epoch closure, exact host composition and multi-session send/receive scheduler are now implemented in the optional host sample.
- [x C] Physical-format decision Option B is accepted: generic Storage owns opaque immutable blob pages plus its WAL-backed catalog; Database retains FLD1 Run v1/v2, manifest, field semantics and FLD WAL. The generic blob/page backend and FLD checkpoint/compaction/FULL_REPLACE paths are implemented on the paired branches.
- [x C] Blob catalog/page failure-ordering and restart-cleanup source audit completed in §1.19, including correction of the stale orphan-cleanup API call. [~ C] Complete the final public API/source/byte audit. Durable sender recovery is implemented in §1.25 and FLD receiver staging/atomic switch in §1.26; both await T-pass proof. Transport creation/reconnect/disposal remain application-owned.
- [~ T] Current validation remains red/unverified: latest completed evidence is older than the current source head; stale Core and Storage Integration fixtures/adapters remain a separate T-pass. Historical [~ C] entries below are chronological progress records; the current implementation status is §§1.23.6–1.23.14 and 1.24–1.33, which supersede older open-item summaries. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 0.74. Recovery branches created before the next integration slice — 2026-11-09

- [x C] Created `backup/cpass-2026-11-09-relay-gate-metadata-mode` at `1c7fcd82edbf2656f1e6af1ab0d4ed7eab94517c` before continuing with the cross-store host integration and metadata-backend audit.
- [x C] Retained earlier checkpoints `backup/pre-rpm3-compact-provenance-2026-11-09` and `backup/cpass-2026-11-09-after-relay-guards`.
- [~ T] Backup point contains unvalidated C-pass changes; it is a recovery branch, not a passing release candidate.

# BIN-SYNC v3.0 — implementation roadmap
**Дата:** 2026-11-09  
**Статус:** active; execution checklist is authoritative.  
**Base normative contract:** `BIN-SYNC-SPECIFICATION.txt`, v3.0.0 DRAFT.  
**Mandatory v3.1 extension:** `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` (implementation and conformance are required for project completion; wire activation is capability-negotiated and does not rewrite v3.0 frames).  
**Supporting host pattern:** `docs/BIN-SYNC-V3.1-HOST-ORCHESTRATION.md` (non-normative).  
**Required exact payload fixtures:** `docs/BIN-SYNC-v3.1-GOLDEN-VECTORS.md` and executable `samples/V31ControlPlaneVectors` (T-pass/CI gate).  
**Cross-repository integration:** FLD-DB docs/SYNC-STORAGE-COMPACTION-ROADMAP.md.

Mark [x] only after code, tests and exact-head CI prove it; [~] means active/partial; [ ] means not done. Documentation alone never closes a code gate. v3.1 is a mandatory implementation track, not an optional feature: its separate negotiation is a wire-compatibility boundary, not a project-scope choice. Latest exact-head green checkpoint before the v3.1 implementation pass: `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` — BinSync.Net Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS; the in-memory SyncSession, TCP and ScenarioHarness public samples passed. Subsequent v3.1 commits are in progress and are NOT green yet. Durable Metadata Bench completed, including one-million-row freeze: 218.1 µs / 7.77 KB for 1,024 local operations. This verifies strict BSP3 provider marker/feed identity, Node ChangeId-ledger cutover, generation-scoped provider rows, atomic FULL_REPLACE staging/switch, canonical RFC-4122 generation bytes, same-author-epoch reservation baseline proof, bounded streaming digest builder, field/record tombstone preservation, and the shared Core ProviderMutation codec. The 218.1 µs result is a measurement, not a proven improvement over the earlier 196.9 µs run. Latest production-code HEAD: `b69c830cbfcbcf310fd4ba14d9fd29d6a4e9830f` (typed, exact-binding FULL_REPLACE proof lease; pushed); latest production-code commits: `b69c830cbfcbcf310fd4ba14d9fd29d6a4e9830f` (typed exact-binding FULL_REPLACE lease), `3206b49ed38b451018dedae850920f7bf0baaab1` (FULL_REPLACE Metadata PendingApply/reservation barrier), `187e56bfb6291305101284aadb5c8367b48b553d` (serialized relay publication gate), `6f0da098c9ecf612dd6eaa918935235766d9fe1d` + `d2f263f41a75db4a63a0d23c544e31a46b5c00ce` (delegate-based host reference scan adapter and constructor ownership fix). Latest CI-tested code baseline: `4382c49bffe1608ebabc4a483771b83242cbc81d`; subsequent production code is not yet CI-validated. latest production-code commit: `963328d9635b641ad832a76878f48418bcecb577` (zero-Outbox/positive-FLD cursor rejection); latest sample commit: `97c9ea4c07f4ef867dc8deae74ee07b46fa4f793`; latest control-plane API commit: `16933bc7ffc33d6a7213d782c376581a6acc57bc9`; RPM3 compact sequence-pair implementation: `0831076f29624b521e2b3a5cd599c4fd72b1d3c0`; FLD Outbox byte audit: `b45bbc1a3462e9b11142dd6656a2eff55200b1c9`; FLD roadmap §7.45 update: `a962371ec293708f61f02453f9bc7c441eafa015`. FLD engine support is at `FLD-DB/fix/crash-consistency-outbox` @ `e6bec094807f660e07b70874254a5925bc8b6deb`. Cross-repository FLD host primitives are on `FLD-DB/fix/crash-consistency-outbox` @ `a52b1c2206e077cf57b04a36c676106dc6d24c93`. This remains a C-pass head, not an exact-head green checkpoint. The last exact-head runs on `1acdfa65979a` were red: Core build failed because `VolatileMetadataStoreAdapter` lacked `TryGetOutboundChangeIdentity(...)`; that test-only adapter compile blocker is now fixed in `bae10f4c2d32` but has not yet been validated by exact-head CI. Storage Integration had 80 failures, predominantly old fixtures assigning OriginSequence to provider drafts (v3.1 requires deferred `OriginSequence=0`), plus old test providers without `IProviderCommitGuard`; these remain T-pass migrations. The Durable Metadata Bench succeeded; Metadata Retry Comparison failed. Do not weaken production invariants. No new exact-head test result is claimed.

## 0.33. FLD recovery checkpoint branch created — 2026-11-09

- [x C] Created `smersh1307n2/FLD-DB:backup/pre-binsync-provenance-map-2026-11-09` at `a962371ec293708f61f02453f9bc7c441eafa015` before further cross-repository work. FLD roadmap §7.46 records the same checkpoint.

## 0.47. C-pass: current production-status pointer refreshed for RPM3 — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` to identify the compact RPM3 implementation/sample commits and the current unvalidated branch head.
- [x C] The historical green v3.0 baseline remains explicitly separate from all current C-pass work.

## 0.53. Recovery checkpoint branch created before further RPM3 work — 2026-11-09

- [x C] Created `backup/pre-rpm3-compact-provenance-2026-11-09` at `93a1cbd82c076ea5002d0e0a7eecccde5f856eed`, preserving the compact RPM3 map, sequence-only rows, empty-tail ACK behavior and current documentation state.

## 0.52. C-pass: production-status pointer refreshed for latest RPM3 behavior — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the empty-tail ACK code commit and the expanded RPM3 sample commit; current-head test status remains explicitly unvalidated.

## 0.51. C-pass: empty-tail ACK semantics documented at API surface — 2026-11-09

- [x C] Added XML documentation to `TranslateSafeOutboxSequence` for positive prior ACK + empty scanned tail progression and the zero-watermark prohibition.

## 0.50. C-pass: empty-page progress sample includes full-prune reopen — 2026-11-09

- [x C] Extended the public sample to close/reopen after all mapped rows are pruned, then advance a later empty author page using the already-positive Outbox ACK. It also verifies a fresh author with OutboxSeq=0 cannot advance a positive FLD ACK.
- [~ T] Execute this sample during T to validate the cached and reconstructed paths against real Storage persistence.

## 0.49. C-pass: empty-author scan cursor can progress after positive ACK — 2026-11-09

- [x C] `TranslateSafeOutboxSequence` now advances through newly scanned empty author pages when the supplied safe OutboxSeq equals an already-positive durable ACK and no unacknowledged mappings remain.
- [x C] A zero OutboxSeq still returns the prior FLD ACK only; it can never authorize a positive JournalSeq ACK. If mappings remain, the equal-watermark path cannot skip them.
- [x C] Extended `RelayProvenanceMap` sample for empty-page progression and the zero-watermark invariant; updated the FLD integration contract.
- [~ T] Verify after reopen, with outstanding mappings, with a positive prior ACK, and with a zero Outbox watermark.

## 0.48. C-pass: FLD adapter contract uses sequence-only entries — 2026-11-09

- [x C] Clarified §9.5 that the host constructs `StorageRelayProvenanceEntry(JournalSeq, resolved OutboxSeq)` after calling the global Node ChangeId resolver. ChangeId is not copied into the provenance map.
- [x C] This makes the no-duplicate-metadata rule explicit at the adapter call site, not only in the RPM3 row codec.

## 0.46. C-pass: public sample catalog aligned with compact map — 2026-11-09

- [x C] Updated `samples/README.md` to describe the JournalSeq↔OutboxSeq sequence map and explicitly state that ChangeId remains in the global ledger, not in the provenance rows.
- [~ T] Run the sample and verify its RPM3 row bytes against the v3 golden vector.

## 0.45. C-pass: eliminate duplicated ChangeId from provenance rows (RPM3) — 2026-11-09

- [x C] Reworked `StorageRelayProvenanceMap` to persist only the sequence pair: OutboxSeq in the ordered key and JournalSeq in the value. ChangeId remains solely in the durable global BIN-SYNC ChangeId ledger; the host must resolve every FLD ChangeId through `Node.TryResolveOutboxSequences` before appending the page.
- [x C] Removed the per-mapping ChangeId bytes and the in-memory `HashSet<BatchId>`. This removes 16 persistent bytes per unacknowledged mapping plus the corresponding per-entry managed hash-set overhead; uniqueness remains enforced by the global ledger and strictly increasing resolved OutboxSeq.
- [x C] Bumped cursor marker to RPM3. The 60-byte cursor checksum now covers the compact 76-byte key + 8-byte JournalSeq value rows. Append WAL cost falls from 109 to 93 bytes per mapping Put; compacted snapshot row falls from 108 to 92 bytes.
- [x C] Updated the sample to verify actual RPM3 rows and the checksum, added `RELAY-PROVENANCE-MAP-v3.txt`, and updated byte audit, public API, changelog, production status and FLD integration contract.
- [~ T] Verify resolver-before-append, duplicate ChangeId returning the original OutboxSeq, monotonic rejection after ACK pruning, RPM3 byte vector, and checksum recovery/corruption. Historical RPM2/HashSet entries above are implementation history only and are superseded by this section.

## 0.44. O-pass candidate recorded: Storage batch sort — 2026-11-09

- [x C] The byte audit now records that `StorageDatabase.EncodeBatch` sorts all mutations by StorageKey before WAL append using LINQ `OrderBy`, adding O(N log N) CPU/allocation overhead per write batch.
- [~ O] Benchmark realistic relay-page sizes and mixed metadata batches before changing the FLD Storage serializer; compare LINQ ordering with array sort and pre-sorted mutation collection, preserving deterministic byte order and atomic commit semantics.

## 0.62. C-pass: complete frame registry audited — 2026-11-09

- [x C] Audited `FrameTypeMap`, `NamespaceGcFrames`, `SessionWire` and `SyncSession` parser bounds. IDs 0x01–0x1C are contiguous and unique; the v3.1 extension occupies 0x14–0x1C exactly.
- [x C] Confirmed alias-map 0x03 uses its dedicated path and non-alias session frame parser bounds include 0x04–0x1C.
- [x C] Added the full frame inventory and 25-byte envelope facts to `BIN-SYNC-METADATA-BYTE-AUDIT.md`.
- [~ T] Signed roster/frontier/checkpoint/FULL_REPLACE payload vectors and per-message encode/decode tests remain open.

## 0.61. C-pass: production status refreshed for coordinator map helper — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the coordinator resolver/append helper, direct map page bound, current RPM3 sample and unvalidated current head.
- [x C] Historical green v3.0 baseline remains separate from current C-pass validation.

## 0.60. C-pass: direct provenance-map page bound aligned — 2026-11-09

- [x C] `StorageRelayProvenanceMap.AppendPage` independently enforces the same 4,096-row maximum as the coordinator resolver, so callers cannot bypass the allocation bound through the lower-level API.

## 0.83. C-pass: FLD adapter recipe preserves whole JournalSeq groups within RPM3 bound — 2026-11-09

- [x C] Changed the integration recipe to read one `RelayJournalBatch` at a time, so the source map page ends on a complete JournalSeq boundary.
- [x C] Added an explicit 4,096-operation guard before allocating the transient ChangeId array. An oversized single FLD JournalSeq group fails closed; it is not split across two durable map/cursor commits.
- [x C] The 4,096-operation bound is enforced at FLD remote-apply ingress and aggregate relay page reads; whole JournalSeq groups are never split. Boundary/resume tests remain T work.
- [~ T] Test empty batch, one group at 4,096, one group at 4,097 and multi-operation same-JournalSeq ordering.
## 0.82. C-pass: document exact Outbox projection dependency of RPM3 resolver — 2026-11-09

- [x C] Corrected the coordinator XML contract: `AppendRelayProvenancePage` resolves transient ChangeIds against **retained Node Outbox entries** to obtain OutboxSeq; the global ChangeId ledger remains the identity authority but is not itself a sequence index.
- [x C] The helper deliberately fails closed when an Outbox entry was already compacted; it writes no map row or source cursor. The host must acquire the composed application guard before reading the FLD page and must not send until the RPM3 map commit succeeds.
- [~ T] Prove resolver behavior before/after Outbox compaction, including that a missing retained projection never guesses a sequence from current field winners or ledger identity alone.
## 0.81. C-pass: legacy TCP session API search closed — 2026-11-09

- [x C] Searched the current BIN-SYNC repository for `TcpSyncSession`, `TcpSyncClient` and `LegacyTcp`; no remaining source references were returned.
- [x C] Confirmed the current TCP examples are transport adapters around the generic `SyncSession`/`ISyncTransport` surface, not a parallel TCP-specific session API.
- [~ T] Run full solution plus all sample builds and inspect the final public API diff; a search result alone is not a build/conformance proof.
## 0.80. Recovery checkpoints refreshed after persistence and sample C slices — 2026-11-09

- [x C] Created BIN-SYNC `backup/cpass-2026-11-09-0915` at `bf277d14aea91977feb5ea4ba57ce3234a681baa`.
- [x C] Created FLD-DB `backup/cpass-2026-11-09-0915` at `94fd1ebdfa15b95b55184a8eee92a5ebea0a4d5e`.
- [x C] Exact-head BIN-SYNC CI remains unverified: workflows triggered by the earlier code commit were cancelled by subsequent rapid documentation pushes. FLD-DB `.NET CI` is green at code-equivalent HEAD `d49f710c1e2992f753278836d521157813d8b503`; latest FLD changes since then are documentation-only.
## 0.79. C/T/O boundary refreshed after orchestration, vector and byte-audit completion — 2026-11-09

- [x C] Reclassified authority/member orchestration as complete at the protocol/host-recipe boundary; recurring scheduling is intentionally application-owned and must not become a protocol-core timer.
- [x C] Reclassified signed v3.1 payload vectors and executable sample as authored; exact C# execution, decode/trust-store checks and negative cases remain T.
- [x C] Reclassified fixed source-derived byte-layout inventory as complete across NodePersistence, Metadata Store, FLD Outbox/ApplyJournal/WAL, Storage, Manifest and Run; workload measurement and runtime amplification remain O/T.
- [~ C] Generic Storage backend/WAL compatibility and physical-format strategy remain open. The synchronized-FLD publication/ACK adapter and concrete FLD reference-snapshot wrapper were subsequently implemented in the optional host sample (§0.93–0.94); cross-repository sample build and T-pass proof remain.
## 0.78. Recovery checkpoints refreshed before further extraction work — 2026-11-09

- [x C] Created BIN-SYNC backup branch `backup/cpass-2026-11-09-0855` at `e0eb9f17ad40abe92da5155aaa86282404acd16e`.
- [x C] Created FLD-DB backup branch `backup/cpass-2026-11-09-0855` at `d49f710c1e2992f753278836d521157813d8b503`.
- [x C] Earlier pre-RPM3 and pre-integration backup branches remain preserved; these new checkpoints include the current C-pass documentation and signed-vector sample state.
## 0.77. C-pass: executable v3.1 signed-vector sample added — 2026-11-09

- [x C] Added `samples/V31ControlPlaneVectors` to construct the deterministic NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN objects, compare payload and signed bytes against the checked-in vectors, and verify Ed25519 signatures.
- [x C] Added the sample command and description to `samples/README.md`; it uses the public `BinSync.Core` API and the documented test-only seed.
- [~ T] Execute the sample on the exact branch head, add trust-store `DecodeAndVerify` assertions and negative mutation/truncation cases. The sample is authored but not yet run.
## 0.76. C-pass: NodePersistence v17 checkpoint byte layout audited — 2026-11-09

- [x C] Audited the 46-byte Node state envelope (magic/version/body length/tagged BLAKE3) and documented deterministic body section formulas for records/field states, Outbox, grouped authors, adaptive receipts, stream digests, peer progress/reservations, SeenChanges and compact Node ChangeId fingerprints.
- [x C] Confirmed the reader rejects every state version except 17 before digest/body parsing; removed unreachable legacy state-digest tag alternatives without changing accepted input behavior.
- [x C] Clarified that NodeToAlias is derived and not persisted, Outbox author IDs are stored once per namespace, and SeenChanges is distinct from the global ChangeId ledger.
- [~ O/T] Runtime dictionary/object overhead, varint/payload distributions, checkpoint size under realistic cardinalities and save/load allocation/latency remain measurement/conformance work.
## 0.75. C-pass: correct receipt row byte variants and GC interpretation — 2026-11-09

- [x C] Re-read `EncodeReceiptValue` / `DecodePartition`: the 76/80/92/96-byte receipt lengths are combinations of always-present OriginSequence plus optional 16-byte HeaderFingerprint and optional 4-byte TransportPeerEpoch; they are not alternate v2 migration encodings.
- [x C] Re-read `EncodeCompleted` / `DecodeCompleted`: 69/85 bytes are the base completed-apply receipt plus optional 16-byte HeaderFingerprint.
- [x C] Corrected the byte-audit table and documented the GC gate: missing/zero TransportPeerEpoch or OriginSequence prevents receipt reclamation; existing replay cursor/digest state remains the durable fence.
- [~ T] Add round-trip fixtures for all supported field-presence combinations and explicit no-GC cases when TransportPeerEpoch is absent.
## 0.74. C-pass: reject unsafe Inbox/receive-state row consolidation — 2026-11-09

- [x C] Documented why the Inbox receipt (sender/transport epoch/namespace/BatchId), receive cursor (same sender context plus author stream) and digest-chain tail (sender/transport epoch/namespace) remain three separate row kinds.
- [x C] A single per-batch receipt would duplicate shared cursor/digest state; a single stream row would lose per-BatchId duplicate-replay identity. The current layout keeps each fact once at its natural cardinality.
- [~ O] Only revisit a merged encoding if measured real workloads show a net byte/index win and a proof preserves independent retry, contiguous cursor and digest-chain semantics.
## 0.73. C-pass: verify outbound reservation retirement is one atomic peer-stream write — 2026-11-09

- [x C] Source audit confirmed `AcknowledgeOutboundBatch` clears `PendingBatch` in the same `PersistPeerStreamState` commit that advances `AckedCursor` and `LastBatchDigest`, after validating exact pushed reservation identity.
- [x C] The pending immutable reservation is embedded in the single kind-15 peer/author row; it is not a separate durable payload table or per-operation receipt copy.
- [~ T] Add crash injection immediately before/after the Storage commit, reopen/retry, duplicate ACK, wrong digest and wrong peer-epoch cases.
## 0.72. C-pass: v3.1 addendum links signed payload vectors and preserves GC gate — 2026-11-09

- [x C] Updated the v3.1 addendum status to point to the deterministic NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN vectors and distinguish documented vectors from C#-verified vectors.
- [x C] Explicitly retained the rule that golden vectors do not authorize receipt, ChangeId or tombstone/ghost physical GC.
## 0.71. C-pass: remove split ACK/high-watermark persistence fallback rows — 2026-11-09

- [x C] Audited `StorageMetadataStore` row writers and confirmed fresh v3 writes persist relay ACK+high-watermark together as kind 5 / 16-byte value and outbound ACK+high-watermark together as kind 8 / 16-byte value.
- [x C] Removed unreachable switch branches for split-row kinds 6/9 and 8-byte values from kinds 5/8. The earlier `DecodePartition` preflight already rejected these legacy encodings before the switch, so this is dead-code cleanup, not a behavior change; no current writer emitted kinds 6/9.
- [x C] This aligns the parser with the audited fixed row inventory and the clean-break rule. It does not change receipt semantics: the 76/80/92/96-byte receipt values are same-v3 field-presence variants (OriginSequence is always encoded; header fingerprint and transport epoch are optional). Receipt GC still requires non-zero transport epoch and OriginSequence plus all signed retirement/frontier/reference checks.
- [~ T] Add exact fixtures proving canonical kind 5/8 reopen and preflight rejection of kind 6/9 or 8-byte split values; run full Storage Integration only in the T pass.
## 0.70. C-pass: correct stale provenance byte-audit row after RPM3 cutover — 2026-11-09

- [x C] Found and corrected a stale RPM2-era row in `BIN-SYNC-METADATA-BYTE-AUDIT.md`: the table still showed a 24-byte mapping value although the RPM3 implementation stores only the 8-byte JournalSeq value.
- [x C] The audit now consistently reports 76-byte key + 8-byte value = 84-byte retained mapping row; the 60-byte cursor and XOR/SHA-256 mapping-set digest are unchanged. RPM2 is explicitly historical and not a fallback decoder.
- [~ T] Verify the documented 84-byte row against the actual persisted RPM3 sample after the C-pass; no test result is inferred from the documentation correction.
## 0.69. C-pass: deterministic signed v3.1 payload vectors authored — 2026-11-09

- [x C] Added `docs/BIN-SYNC-v3.1-GOLDEN-VECTORS.md` with deterministic NRS2, OFR2, RCP2, FRA2 and FULL_REPLACE_BEGIN payload/signature vectors, one shared roster digest, fixed test-only Ed25519 key and canonical field descriptions.
- [x C] Vectors cover the new signed authority/member control-plane messages and FULL_REPLACE manifest domain; the test seed is explicitly marked public-only and must never be used in production.
- [~ T] Cross-check every byte/signature against the actual C# encoders, `DecodeAndVerify`/`VerifyBegin`, trust-store binding and negative tamper/truncation cases. These are signed payload vectors, not full frame-envelope vectors.
## 0.68. C-pass: two-DLL FLD extraction dependency audit synchronized — 2026-11-09

- [x C] Cross-checked FLD source graph against `FLD-STORAGE-CORE-ARCHITECTURE.md`: `Fld1.Storage` is already a separate opaque byte-key/value engine, but `FldDatabase` still uses `Fld1.Engine` + FLD-specific `Fld1.Run`, `Fld1.Manifest`, `Fld1.Wal` and mixed `Fld1.Core` assemblies.
- [x C] Recorded that there is no `Fld1.Database.csproj` and the current `FldDatabase` does not use `StorageDatabase` as its backing store. The two-DLL extraction therefore needs a physical-format/dependency seam decision, not a cosmetic AssemblyName change.
- [~ C] FLD roadmap §7.50/§7.57 now records the extraction order. Physical `Fld1.Database` consolidation is complete; generic WAL/file-generation reuse, Core primitive classification, public API audit, Storage-only/Database test re-homing and package proof remain. No third Common DLL or linked-source shortcut.
- [x C] The physical `Fld1.Database` boundary now exists. Implement the adapter in a separate host/integration layer; do not introduce direct Engine/Database→BIN-SYNC dependencies.
## 0.67. C-pass: reject stale/duplicate relay scan pages before ChangeId lookup — 2026-11-09

- [x C] `AppendRelayProvenancePage` now reads the RPM3 cursor under the already-held application guard and rejects a regressing scan cursor or non-empty page reusing the committed cursor before allocating resolver arrays or consulting Node.
- [x C] JournalSeq validation now starts from the durable prior scan cursor, so the first source batch must be strictly after the last committed page while multiple operations in the same JournalSeq remain valid.
- [x C] Empty pages retain the explicit no-allocation cursor-advance path, including idempotent same-cursor no-op behavior.
- [~ T] Verify stale page, exact replay, empty same-cursor and empty advancing-cursor behavior under reopen; current code remains unvalidated until the broad T pass.
## 0.66. C-pass: current integration boundary and CI state refreshed — 2026-11-09

- [x C] Updated the active snapshot to distinguish the new coordinator ChangeId resolver from the still-unimplemented FLD Database adapter. The helper is a BIN-SYNC-side primitive; it does not add FLD semantic types or a third runtime DLL.
- [x C] The approved FLD two-DLL source consolidation is now complete: `Fld1.Storage.dll` + `Fld1.Database.dll`, with a one-way Database→Storage reference. The old Core/Engine/Manifest/Run/Wal projects are physically consolidated into `Fld1.Database`; exact-head build/test proof remains in T.
- [x C] FLD's guarded immutable reference-snapshot code and two-DLL source consolidation are present on the current `fix/crash-consistency-outbox` branch. The latest FLD CI attempt failed before runner allocation (`runner_id=0`, no steps), so no current-head build proof exists; this also does not validate the cross-repository sample.
- [~ T] BIN-SYNC prior head `7036e1544d0e8aa3f8b4a15b2c90d0f51fb13f35` failed both Core and Fuzz/Lab at build due the bad map binding check. Fixed at `cf08a22158fc1784fe225ef38efd0e2a165d7eb1`; subsequent workflow attempts were cancelled by later pushes, so the fix remains unverified.
- [x C] The relay/ACK/GC host adapter source now implements FLD page read→resolver→RPM3 append→ceiling-limited send→safe ACK→FLD ACK→RPM3 prune and the reference-snapshot scanner in `samples/FldBinSyncHost` (§§0.93–0.98). [~ C] The `FldDatabase`→`IDataProvider` snapshot/local-feed/full remote-apply bridge is still missing; generic Storage backend/WAL compatibility, final public API/byte audit and transport scheduling remain.
## 0.77. C-pass: bounded author discovery and all-author ACK orchestration — 2026-11-09

- [x C] FLD author discovery now has a process-local cache: first use rebuilds from ApplyJournal, each durable remote APPLY incrementally adds its relay authors, and a durable per-author ACK removes the author only when it covers that author's maximum unacknowledged JournalSeq. No persistent author index was added.
- [x C] BIN-SYNC `StorageRelayProvenanceMap.ReadAuthors(namespaceId)` enumerates persisted RPM3 cursor authors. `StorageRelayPublicationGate` now unions these internally with FLD's `ReadUnacknowledgedRelayAuthors()` callback under the composed host guard; the host cannot accidentally omit a map-only author in the FLD-ACK-before-prune crash window.
- [x C] Added `AcknowledgeAllAsync`: it discovers the same author union and performs per-author safe-sequence translation → durable FLD ACK → RPM3 prune. Partial completion is retryable because successfully pruned authors disappear and the remaining cursors are rediscovered.
- [~ T] Test author-cache rebuild/update/partial-ACK/removal/re-add, automatic FLD+RPM3 union, map-only crash recovery, partial multi-author ACK failure/retry, and zero duplicate author index bytes.
- [~ C] Remaining integration: implement the concrete `FldDatabase`→`IDataProvider` bridge (stable paged snapshots, local ChangeId feed and full remote-apply context) and bind the actual remote APPLY callback. The current sample assumes a preconstructed coordinator/provider and supplies the relay/ACK/GC side only; keep one coordinator/gate per namespace and no direct session sender bypass.
## 0.76. C-pass: make cutover-aware ChangeId resolution page-scalable — 2026-11-09

- [x C] Replaced the Metadata Store resolver's per-page full relay-list scan with a rebuildable in-memory index scoped to one `(namespace, originalAuthor)` stream. The first lookup validates retained sequence ordering/high-watermark and builds the index; later 4,096-operation source pages resolve in O(page size).
- [x C] The index is not persisted and adds zero metadata bytes. It is shared across copy-on-write partition clones, extended only after durable relay append, and invalidated for that author only after durable relay payload compaction.
- [x C] Added collision/conflicting-sequence checks when the in-memory index is extended; persistent ChangeId ledger remains the sole durable identity store.
- [~ T] Add large-cardinality multi-page resolver benchmark (10k/100k/1M relay rows), reopen rebuild equivalence, incremental append, compaction invalidation, and ChangeId duplicate/corruption tests.
- [~ O] Performance evidence remains open; the index is a structural complexity fix, not a measured throughput claim.
## 0.75. C-pass: namespace-wide all-author relay recovery and send barrier — 2026-11-09

- [x C] Added `FldDatabase.ReadUnacknowledgedRelayAuthors()` in FLD-DB. It scans ApplyJournal under the FLD commit lock and returns distinct remote authors with unacknowledged RelayFields; it persists no extra author index.
- [x C] Added `StorageRelayProvenanceMap.ReadAuthors(namespaceId)` to enumerate existing RPM3 cursor authors. Startup must union this with FLD's unacknowledged author set so a crash after FLD ACK but before RPM3 prune is not missed.
- [x C] Replaced the single-author publication API with `ApplyAndPublishAllAsync` / `RecoverAndPublishAllAsync`. Author discovery occurs after APPLY under the composed host guard; the gate drains every author/page to that author's observed source high watermark before running the namespace-wide sender callback.
- [x C] The gate rejects zero/local/duplicate discovered authors, starts closed, remains closed on any author resolver failure, and only opens after all discovered authors reconcile. This closes the multi-author bypass where a sender selecting another author stream could previously observe unmapped relay payload.
- [~ T] Test author discovery after reopen, map-only author recovery after FLD ACK crash, multi-author pages above 4,096 operations, empty pages, resolver failure on the second author, and sender callback suppression until all authors complete.
- [x C] `StorageRelayPublicationGate.DiscoverAuthors` unions `fld.ReadUnacknowledgedRelayAuthors()` with `map.ReadAuthors(namespaceId)` after APPLY; the sample supplies the FLD half and does not cache authors before APPLY.
## 0.73. C-pass: drain every source page before exposing relay Outbox — 2026-11-09

- [x C] Replaced the unsafe one-page send orchestration with `ApplyAndPublishAsync` and `RecoverAndPublishAsync`. Both repeatedly read whole JournalSeq-bounded pages under the composed FLD/Node/Storage guard, resolve all ChangeIds and commit RPM3 mappings until the observed source JournalSeq high watermark is reached; only then can the sender callback run.
- [x C] The page callback receives the current RPM3 cursor and must return `(Changes, ScannedThroughJournalSeq, SourceHighWatermark)` from one FLD-guarded read. A page that fails to advance while source history remains, a cursor beyond source high, or a regressing cursor fails closed.
- [x C] `StorageRelayPublicationGate` now starts in reconciliation-required state. `RunOutboundAsync` and ACK are blocked until `RecoverAndPublishAsync` succeeds; resolver failure or an exception after possibly durable APPLY closes the gate until a full startup rescan succeeds.
- [x C] Updated the integration contract: a single 4,096-operation page is not sufficient authorization to send when later FLD pages remain. The complete source scan cursor must reach the durable ApplyJournal high watermark before any outbound batch is exposed.
- [~ T] Add multi-page >4,096 relay tests, mixed-author empty pages, operation-cap resume, startup-gate default closed, resolver-failure gate closure, callback exception/recovery, and prove no sender observes rows from an unmapped later page.
- [x C] `FldBinSyncHostAdapter.ReadRelayPage` reads complete bounded `ReadRelayBatchPage` groups and the source high watermark; `StorageRelayPublicationGate` invokes it under the composed FLD/Node/Storage guard and reuses one `For(coordinator)` gate.
## 0.72. C-pass: resolve relay identity through the active outbound authority — 2026-11-09

- [x C] Found and corrected a cutover-mode integration gap: after `IProviderPublicationMode` switches outbound sequencing to `StorageMetadataStore`, Node Outbox is intentionally empty. `AppendRelayProvenancePage` now uses `StorageMetadataStore.TryResolveRelaySequences` in metadata-owned mode and retains `Node.TryResolveOutboxSequences` only before cutover.
- [x C] Added `TryResolveRelaySequences` to the durable metadata store. It resolves unique ChangeIds against retained per-author relay rows, returns the assigned metadata relay sequence, and fails closed/clears output if any source row is no longer retained. It stores no duplicate ChangeId index or payload.
- [x C] `AcknowledgeRelayProvenance` now obtains the per-author safe watermark from `GetSafePeerStreamCompactionCursor` after cutover and from Node before cutover. It preserves the already committed RPM3 ACK as a monotonic lower bound during peer retirement/rejoin transitions.
- [x C] Updated the integration contract and RPM3 docs: destination sequence is the active BIN-SYNC per-author outbound coordinate (Node Outbox before cutover, Metadata relay sequence after cutover); ChangeId remains solely in the global ledger.
- [~ T] Add tests for both publication modes, cutover with retained relay rows, missing/compacted ChangeId, safe ACK across peer rollover, and restart/reopen. This code has not been built or tested after the last exact tested baseline `4382c49bffe1608ebabc4a483771b83242cbc81d`.
- [~ C] Continue reviewing metadata-owned peer baseline/relay retention semantics and finish the concrete host composition reference. Physical GC remains disabled.
## 0.71. C-pass: enforce a single publication gate per coordinator — 2026-11-09

- [x C] Made `StorageRelayPublicationGate` construction private and added `For(StorageSyncCoordinator)`, backed by a `ConditionalWeakTable`, so callers using the supported API share one gate for the coordinator's fixed namespace.
- [x C] Updated the integration contract to require the shared factory for every receiver, sender, startup-recovery and ACK path. This removes accidental duplicate gate instances when the host follows the public API.
- [~ C] A host can still bypass the gate by directly calling Node/session APIs; integration must centralize transport scheduling and not expose an alternate sender path. Multiple coordinator instances for the same underlying Node/namespace must not be created by the host.
- [~ T] Verify concurrent `For` calls return the same instance, independent coordinators do not share gates, cancellation releases waiters, and failures always release the semaphore.
## 0.70. C-pass: retry durable frontier reports without minting new history — 2026-11-09

- [x C] Added `SendAcceptedAppliedOriginFrontierAsync`: loads the latest durable local report for the accepted roster and resends that exact report instead of incrementing ReportSequence on transport retry.
- [x C] The resend path verifies the persisted report signature against the accepted roster and checks that the supplied local member seed reproduces the persisted signature before sending. This prevents a retry with the wrong key from silently creating a different signed report.
- [~ T] Add lost-send/retry tests proving the report sequence and payload remain identical across retries, reject wrong member seed, and handle roster rollover with no accepted local report.
- [~ C] Review equivalent resend/retry behavior for other control-plane messages and verify FULL_REPLACE installed-ACK replay semantics before considering control-plane orchestration closed.
## 0.69. C-pass: fail-closed startup cursor reconciliation — 2026-11-09

- [x C] Added `StorageSyncCoordinator.ValidateRelayProvenanceRecovery`: under the composed FLD/Node/Storage guard it verifies RPM3 scope/replica/storage binding and rejects persisted scan or ACK JournalSeq cursors beyond `FldDatabase.GetRelayJournalHighWatermark()`.
- [x C] Added `StorageRelayPublicationGate.RecoverAndPublishPageAsync`: keeps the namespace gate closed while validating the source high watermark, reading/resolving the next durable page, committing RPM3 mapping+cursor, then invoking the sender callback. A stale/replaced source journal fails closed before any send.
- [x C] Updated the integration contract with the concrete FLD high-watermark API and startup ordering. This closes the coordinator-side startup cursor check; host restart policy must still keep all other sender paths disabled until recovery succeeds.
- [~ T] Startup cursor regression, source replacement, map reopen/checksum, crash-before-send and idempotent replay tests remain open. All code since `4382c49bffe1608ebabc4a483771b83242cbc81d` is still unvalidated by CI.
- [x C] Relay/ACK/publication-ceiling/GC host composition source now exists in `samples/FldBinSyncHost`; signed v3.1 vector sources and adapter call shapes are authored. [~ C] The `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply bridge is not implemented; generic Storage backend/format compatibility, final metadata byte/cardinality audit and transport scheduler also remain. Execution/conformance belongs to T.
## 0.68. C-pass: gate inbound APPLY and outbound relay exposure atomically — 2026-11-09

- [x C] Extended `StorageRelayPublicationGate` with `ApplyAndPublishPageAsync`, which holds the same async namespace gate across durable remote APPLY → host relay-page read → ChangeId resolution → RPM3 map/cursor commit → sender callback. A crash/failure after APPLY but before map commit cannot expose its relay entries to a sender through this gate.
- [x C] Retained `PublishPageAsync` as the startup/recovery path for already-durable source pages while the host keeps all senders closed; `AcknowledgeAsync` uses the same gate so ACK/pruning cannot race publication.
- [x C] Updated `FLD-DB-INTEGRATION-CONTRACT.md` to require all remote APPLY and outbound send/build paths for one namespace to share one gate instance. The helper is an enforceable host boundary, not a claim that arbitrary bypassing callers are prevented.
- [~ C] The host still must instantiate one gate per namespace, route every receiver and sender through it, compose actual FldDatabase callbacks, and keep startup sending closed until persisted FLD/RPM3/Node state is reconciled.
- [~ T] Commits `187e56b`, `6f0da09`, `d2f263f`, `3805cc3` and `b1e66df` are after the last CI-tested baseline `4382c49`; build/tests must be rerun after C-pass. The baseline's 6 Core and 80 Storage Integration failures remain as enumerated in §0.67.
## 0.67. C-pass: publication scheduler gate and host GC scan adapter — 2026-11-09

- [x C] Added `StorageRelayPublicationGate`: `PublishPageAsync` serializes page read → resolver → atomic RPM3 map/cursor commit → sender callback; `AcknowledgeAsync` serializes safe Outbox ACK → durable host ACK callback → RPM3 pruning; `RunOutboundAsync` is the common gate for other namespace sender/build paths.
- [x C] Added `DelegateApplicationGcReferenceScanner` and `DelegateApplicationGcReferenceScan`. A host can adapt `FldDatabase.OpenReferenceSnapshot()` without a direct FLD reference in BIN-SYNC. The scan owns both the host commit lease and immutable snapshot, disposes the snapshot first, and releases the host lease even if snapshot disposal throws.
- [x C] Constructor validates all delegates and the empty-draft/HLC invariant before taking ownership of either resource, preventing a failed adapter construction from leaking the FLD guard.
- [x C] `FldBinSyncHostAdapter` now composes the actual FLD commit guard, relay-page read, durable relay ACK and immutable reference snapshot; `RecoverAndPublishAllAsync` reconciles persisted FLD/RPM3 cursors before sender enablement. [~ C] The application must still instantiate one adapter/gate per namespace and route every transport sender path through its callbacks.
- [~ T] Exact CI on `4382c49bffe1608ebabc4a483771b83242cbc81d`: BinSync.Net build passed, 6/678 Core tests failed (five OFR1/RCP1 old member-report fixtures missing issued-watermark vectors, one provider mutation codec tombstone fixture missing canonical lifecycle metadata); Fuzz/Lab passed. Storage Integration build passed, 91/171 tests passed and 80 failed on legacy provider-draft OriginSequence / missing IProviderCommitGuard fixture contracts across Ubuntu/Windows/macOS. New gate/scan adapter commits after that baseline have not been built or tested.
- [x C] Startup recovery/publication gating and the concrete FLD host adapter example are implemented; actual FLD API/callback shapes are reconciled in the sample. [~ C] Finish the generic Storage backend/format decision and remaining metadata byte/cardinality audit; physical GC stays disabled.
## 0.65. C-pass: repair exact-head provenance binding compile blocker — 2026-11-09

- [x C] Inspected both failed exact-head workflows for `7036e1544d0e8aa3f8b4a15b2c90d0f51fb13f35`; both stopped at build with CS0103 because `StorageRelayProvenanceMap.IsBoundTo` referenced nonexistent `_scopeId` / `_localReplica` fields.
- [x C] Corrected the binding check to compare the stored canonical root's SHA-256 scope bytes and 16-byte local-replica bytes. This avoids duplicated mutable identity fields and uses the exact same root representation as the persisted map namespace.
- [~ T] Commit `cf08a22158fc1784fe225ef38efd0e2a165d7eb1` is not yet exact-head CI validated. Both previous workflows failed before tests started; no T-pass test result is inferred.
- [x C] FLD host adapter/scanner source and v3.1 control-plane protocol code are implemented. [~ C] Generic Storage backend/format compatibility and final source/byte audit remain before T; physical GC remains disabled.
## 0.64. C-pass: empty relay pages avoid resolver allocations — 2026-11-09

- [x C] `AppendRelayProvenancePage` now fast-paths an empty source page directly to the atomic map cursor update, avoiding zero-length identity/sequence arrays and a no-op Node ledger lookup.
- [x C] The same application guard, map binding and zero-watermark rules remain enforced.

## 0.63. C-pass: source JournalSeq order validated before ledger work — 2026-11-09

- [x C] `AppendRelayProvenancePage` now rejects zero, decreasing or beyond-scanned JournalSeq values before allocating/consulting the Node ledger; same-JournalSeq operations remain allowed as one complete source batch.
- [~ T] Verify malformed page rejection does not touch Node Outbox or Storage cursor.

## 0.59. C-pass: relay provenance resolver page bounded — 2026-11-09

- [x C] `AppendRelayProvenancePage` now rejects more than 4,096 source changes before allocating identity/sequence arrays, matching the coordinator's existing bounded page policy.
- [~ T] Verify boundary values 0, 1, 4,096 and 4,097; ensure rejection occurs before Node resolver or Storage mutation.

## 0.58. C-pass: coordinator helper XML cref made unambiguous — 2026-11-09

- [x C] Replaced an overloaded-method XML `cref` with an unambiguous code reference to avoid documentation compiler warnings.

## 0.57. C-pass: adapter example imports current FLD/BIN-SYNC namespaces — 2026-11-09

- [x C] Added explicit `using` directives to the guarded page-to-RPM3 integration snippet so the referenced FLD and BIN-SYNC types are unambiguous.

## 0.56. C-pass: guarded FLD page-to-RPM3 call sequence documented — 2026-11-09

- [x C] Added a concrete current-API adapter flow to §9.5: acquire `AcquireApplicationCommitGuard(database.AcquireCommitGuard)`, read `ReadRelayBatchPage`, collect `Batch.RelayWrites` ChangeIds with their JournalSeq, then call `AppendRelayProvenancePage`.
- [x C] Clarified that the coordinator helper performs global ChangeId resolution; the host must not duplicate that logic or infer OutboxSeq from field winners.
- [~ T] Compile the adapter after the approved two-DLL extraction and test guard ordering, page retry and missing-ledger identity behavior.

## 0.55. C-pass: coordinator resolves ChangeIds before RPM3 append — 2026-11-09

- [x C] Added `StorageSyncCoordinator.AppendRelayProvenancePage`: accepts transient `(JournalSeq, ChangeId)` source entries, resolves all ChangeIds through `Node.TryResolveOutboxSequences`, converts positive local OutboxSeq values to compact sequence-pair entries and appends mapping+RPM3 cursor atomically.
- [x C] It requires the coordinator application-commit gate, validates the map's scope/local replica/shared StorageDatabase binding, and returns false without writing anything if any ChangeId is no longer retained.
- [x C] The helper stores no ChangeId bytes; global ChangeId ledger remains the sole identity authority. Updated the FLD integration contract, public API guide and changelog.
- [~ T] Test missing ChangeId rollback/no cursor advance, duplicate ChangeId rejection, mismatched map binding, correct lock-order usage, and concurrent source ACK/compaction under the host lease.

## 0.54. C-pass: FLD Manifest and Run structural byte audit — 2026-11-09

- [x C] Audited ManifestCodec: 80-byte fixed base plus 4 bytes/namespace, 64/run, 16/retain, 52+bitmap/result-client and 40/provenance-index entry.
- [x C] Audited RunConstants: 256-byte header, 8-byte footer, 40-byte cluster header, 8-byte field descriptor, optional 24-byte HLC override and 16/32-byte structural slots/segments/refs.
- [x C] Clarified that Run is materialized field state, not a second synchronization history; tombstone is a field-descriptor flag and GhostCount is in the Run header.
- [x C] Added the formulas to `BIN-SYNC-METADATA-BYTE-AUDIT.md`. Full variable payload/PGM/dictionary amplification remains workload-dependent and is not presented as a fixed metadata cost.

## 0.43. C-pass: ApplyJournal and Micro-WAL byte audit — 2026-11-09

- [x C] Audited FLD `ApplyJournal.cs`: 16-byte file header; each record is 20-byte header + APPLY_COMMIT payload + 4-byte CRC32C; per-author relay-ACK sidecar is 12-byte header + 24 bytes/author + CRC; sparse index is 16 bytes/256 records in memory.
- [x C] Audited `ApplyCommitCodec` v3: 139-byte fixed body envelope including the field count; each mutation adds 69 bytes plus value; non-relay-only WAL payload adds relay-index count and varuint indexes; relay-only ApplyJournal records store only relay fields.
- [x C] Audited Micro-WAL v2: 64-byte segment header; each record is 20-byte envelope + payload + 4-byte CRC32C + 0–7 bytes alignment padding.
- [x C] Documented the bounded handoff duplication: full APPLY WAL payload and relay-only ApplyJournal payload coexist until WAL seal/truncation; acknowledged relay payload can be compacted while receipt/protocol metadata remains in ApplyJournal.
- [x C] Documented the FLD reference snapshot's in-memory ChangeId/RecordId/receipt indexes and conservative unsealed-WAL barrier.
- [~ C/O] Manifest/Run, ApplyJournal receipt lifetime/independent compaction, Fld1.Storage page/index/write amplification and full workload cardinalities remain open; do not claim a complete cross-store byte total yet.

## 0.42. FLD source API audit completed — 2026-11-09

- [x C] Located and inspected the current FLD source: `FldDatabase.ReadRelayBatchPage`, `AcknowledgeRelayBatches`, `AcquireCommitGuard`, `ReadUnpublishedLocalDraftHorizon`, `OpenReferenceSnapshot`, and `FldReferenceSnapshot` already exist in `Fld1.Engine.Db`.
- [x C] Confirmed `ReadRelayBatchPage` returns whole ApplyJournal batches for one author and advances `ScannedThroughJournalSeq` across nonmatching-author records; `AcknowledgeRelayBatches` persists the per-author source ACK.
- [x C] Confirmed `FldReferenceSnapshot` provides exactly the four required scanner surfaces: draft horizon, ChangeId, RecordId/tombstone and receipt reference checks. It builds one immutable index under the FLD commit gate and disposes without retaining a file lock.
- [x C] Updated `FLD-DB-INTEGRATION-CONTRACT.md` with exact current signatures and `SYNC-STORAGE-COMPACTION-ROADMAP.md` §7.47 with the source audit.
- [x C] The FLD Database source is now physically consolidated in `Fld1.Database`; its actual APIs are identified. The remaining host adapter must live outside FLD and must not create an Engine→BIN-SYNC dependency or premature package.

## 0.41. C-pass: FLD target API names not misrepresented as current — 2026-11-09

- [x C] Clarified in §9.5 that `ReadRelayBatchPage`, `AcknowledgeRelayBatches` and the ChangeId resolver are target neutral semantic API names, not APIs already exposed by the current pre-extraction FLD branch.
- [x C] The §9.6 sequencing gate remains controlling; the integration contract is a design target, not an implementation claim.

## 0.40. C-pass: source page boundaries made explicit — 2026-11-09

- [x C] The map API and FLD integration contract now require relay pages to end on whole JournalSeq boundaries. A journal transaction with multiple ChangeIds cannot be split across pages; the scanned-through cursor means all source journals up to that boundary were examined.
- [x C] This matches the invariant that a safe relay ACK cannot split a source JournalSeq group.
- [~ T] Add a page-split rejection test and a multi-operation same-JournalSeq page/restart test.

## 0.39. C-pass status head synchronized after test-plan consolidation — 2026-11-09

- [x C] Refreshed `docs/PRODUCTION-STATUS.md` to the current unvalidated head after consolidating the T-pass backlog.
- [x C] The T-pass is now enumerated in one place and remains intentionally unexecuted while C-pass work continues.

## 0.38. T-pass backlog consolidated (execution still deferred) — 2026-11-09

The following is a test plan only; no tests are run in this C pass.

1. **Compile/API:** build Core, Storage, all five samples, XML docs and analyzers on net10.0; resolve any new API/type/nullability/analyzer failures without weakening invariants.
2. **RPM3 persistence:** compare actual rows with `RELAY-PROVENANCE-MAP-v3.txt`; reopen/reconstruct; append empty and multi-entry pages; verify digest stability after append/prune; corrupt cursor marker/length/digest, row key/value, and non-monotonic coordinates. Verify the caller's Node ChangeId resolver prevents duplicate identities before map append.
3. **Relay ACK translation:** no mapping, safe=acked, below-first, exact sequence, within same JournalSeq group, between groups, exact-last, above-last, after partial prune, after full prune; verify zero Outbox ACK never advances positive FLD JournalSeq.
4. **Crash ordering:** crash before map batch, after map+cursor batch, after durable FLD ACK but before map prune, during map prune batch; replay must be idempotent and never over-ACK.
5. **Coordinator broadcast:** publish one checkpoint and send to N sessions; wrong authority seed, stale checkpoint, roster generation mismatch, superseding checkpoint between member sends, and cancellation/retry.
6. **Existing red integration fixtures:** migrate provider-draft OriginSequence to deferred `OriginSequence=0`; update providers for `IProviderCommitGuard`; test freeze-before-stamp and exact-head CI.
7. **GC proof:** adversarial live-reference scanner, lease ordering, membership retirement/rejoin, and independent safe horizons for receipts, ChangeId and tombstone/Ghost. Physical GC stays disabled until all proofs pass.
8. **O-pass:** benchmark metadata row/cardinality footprint, RPM3 cold recovery and hot append/translation, compact mapping-list memory, provider snapshot memory, Storage batch sort, and checkpoint broadcast overhead; revert any regression.

## 0.37. C-pass: production-status head refreshed — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the current unvalidated head and the latest coordinator/provenance/sample implementation commits.
- [x C] The historical v3.0 CI baseline remains explicitly historical; no test success is attributed to the C-pass head.
- [~ T] Exact-head build/test matrix remains deferred until the code sweep ends.

## 0.36. C-pass: Unreleased changelog synchronized — 2026-11-09

- [x C] Added the v3.1 C-pass implementation summary to `docs/CHANGELOG.md`, including RPM3 compact provenance mapping, persisted-row sample, byte audit, checkpoint broadcast and host integration sequencing.
- [x C] The changelog explicitly states that current-head validation is pending and physical ChangeId/tombstone/Ghost GC remains disabled.
- [~ T] Promote these notes to a release entry only after exact-head CI, sample execution and v3.1 wire-vector review.

## 0.35. C-pass: production status no longer overstates current CI — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` to distinguish the historical fully verified v3.0 baseline from the current v3.1 C-pass head. It explicitly states that new v3.1 code, RPM3 mapping/checksum, new sample and checkpoint-broadcast changes are not yet built/tested.
- [x C] Updated the public sample status: three pre-v3.1 samples were in the historical baseline; five current executable samples are not yet verified on the current branch.
- [x C] Kept physical ChangeId and tombstone/Ghost GC disabled. No green CI claim is made for the current head.
- [~ T] Exact-head build, full test matrix and sample execution remain deferred until C-pass closure.

## 0.34. C-pass: public API guide aligned with negotiated v3.1 code — 2026-11-09

- [x C] Updated `docs/PUBLIC-API.md`: v3.1 roster/frontier/checkpoint/FULL_REPLACE are no longer described as merely proposed; the guide now distinguishes the v3.1 negotiated extension from the v3.0 wire baseline and keeps physical GC disabled.
- [x C] Documented `StorageRelayProvenanceMap` RPM3 and the host-owned coordinator lifecycle/broadcast surface; linked the orchestration and FLD integration contracts.
- [~ T] Reconcile this guide against the compiled public API XML and exact-head build during T; no API docs are considered release-final before compilation.

## 0.32. Recovery checkpoint branch created — 2026-11-09

- [x C] Created `backup/pre-provenance-checksum-and-control-plane-2026-11-09` at `4026a741b8e19a0d82fe25e4c87fd47cf481852c` before continuing the C pass. Existing earlier backup branches remain intact.

## 0.31. C-pass: authority round serialization documented — 2026-11-09

- [x C] The host orchestration recipe now requires one serialized authority round at a time: publish once, send the same accepted checkpoint to every intended member, then begin the next checkpoint round.
- [x C] Each send revalidates the currently accepted sequence/digest and fails closed if a concurrent round supersedes it; the application host owns the round mutex and cancellation/retry.
- [~ T] Exercise a superseding checkpoint between two member sends and verify the host retries the round rather than silently mixing checkpoint generations.

## 0.30. C-pass: checkpoint broadcast state race closed — 2026-11-09

- [x C] Moved accepted-roster/checkpoint read, authority/generation validation, re-sign and idempotent durable acceptance under the coordinator gate. The verified checkpoint is copied out of the critical section before any awaited transport send.
- [x C] This prevents a concurrent roster/checkpoint transition from racing between validation and re-acceptance while avoiding holding locks across network I/O.
- [~ T] Add a race test with concurrent roster transition/checkpoint broadcast and verify no stale generation is sent.

## 0.29. Cross-repository roadmap synchronized — 2026-11-09

- [x C] Updated FLD-DB `SYNC-STORAGE-COMPACTION-ROADMAP.md` §7.45 to record the independent BIN-SYNC RPM2 provenance-map primitive, its sample and checksum semantics.
- [x C] FLD's roadmap explicitly keeps the full synchronized-database adapter in its later §7.20 implementation order; no premature bridge or database project dependency was introduced.
- [~ C] Cross-store audit remains partial: Outbox fixed sizes are now known, but ApplyJournal/WAL/Manifest/Run and Storage-engine amplification still require the source-level extraction/audit.

## 0.28. C-pass: FLD Outbox byte costs added to cross-store audit — 2026-11-09

- [x C] Audited FLD `OutboxLog.cs` directly: 16-byte file header; each record is 88-byte fixed header + value bytes + 4-byte CRC32C; ACK/state/lease sidecars are 12/20/36 bytes; in-memory sparse index is one 16-byte point per 64 records.
- [x C] Added this to `BIN-SYNC-METADATA-BYTE-AUDIT.md` and explicitly separated FLD Outbox intent from the BIN-SYNC 84-byte mapping row. No payload copy is introduced by the mapping.
- [~ C/O] ApplyJournal, WAL, Manifest/Run and Fld1.Storage page/index/write amplification remain unmeasured; the source paths/layouts must be audited before a complete cross-store total is claimed.

## 0.27. C-pass: reject impossible zero-Outbox positive-FLD cursor — 2026-11-09

- [x C] RPM3 cursor recovery now rejects a positive AckedThroughJournalSeq paired with AckedThroughOutboxSeq=0. The v3.1 adapter contract explicitly states that zero safe OutboxSeq never authorizes a positive FLD relay ACK.
- [~ T] Add malformed-cursor coverage for this impossible watermark pair and the valid partial-group case (positive OutboxSeq with JournalSeq safe-through still zero).

## 0.26. C-pass: public sample index links orchestration guidance — 2026-11-09

- [x C] Updated `samples/README.md` with links to the v3.1 host orchestration recipe and the FLD integration contract, so sample users can find the correct control-plane lifecycle and cross-repository dependency order.
- [~ T] Verify links and examples as part of the final docs/sample CI pass.

## 0.25. C-pass: sample verifies actual RPM2 rows (historical schema, superseded by RPM3) — 2026-11-09

- [x C] Extended `samples/RelayProvenanceMap` to inspect its actual Storage snapshot and compare the three persisted mapping rows, cursor marker/watermarks, endian order, ChangeIds and recomputed SHA-256/XOR mapping-set digest against the deterministic fixture.
- [x C] The sample now checks the durable row representation rather than only the public translation behavior.
- [~ T] Execute the sample and verify its actual bytes match `docs/golden/RELAY-PROVENANCE-MAP-v2.txt`; the code has not been run on this C-pass head.

## 0.24. C-pass: versioned mapping-set checksum and atomic cursor boundary — 2026-11-09

- [x C] Upgraded the provenance cursor to RPM2 (60 bytes; historical, superseded by RPM3): version marker, scanned JournalSeq, ACKed OutboxSeq, ACKed JournalSeq and a 32-byte retained-mapping-set digest.
- [x C] Each mapping row is hashed as SHA-256(key || value); hashes are XOR-aggregated. Append and post-FLD-ACK prune update mapping rows plus cursor/digest in one Storage write batch. Empty pages advance the cursor without changing the digest.
- [x C] Restart reconstructs rows through bounded 1,024-entry pages, recomputes the mapping-set digest and uses constant-time comparison against the durable cursor. Missing/torn/mismatched rows fail closed; no page can be silently skipped.
- [x C] Updated the byte audit, FLD integration contract and deterministic RPM2 golden vector (historical, superseded by RPM3).
- [~ T] Verify exact persisted bytes, append/prune/reopen checksum stability, row corruption, cursor corruption, empty-page digest stability and crash injection around the atomic batch.

## 0.23. C-pass: fail-closed persisted ChangeId validation — 2026-11-09

- [x C] Restart reconstruction now rejects a zero ChangeId in a persisted provenance row, in addition to duplicate IDs, invalid row lengths, out-of-range JournalSeq and non-monotonic destination/source coordinates.
- [~ T] Add corrupt-row tests for zero identity, duplicate identity, malformed cursor marker/length and sequence-order violations.

## 0.22. C-pass: explicit v3.1 host orchestration recipe — 2026-11-09

- [x C] Added `docs/BIN-SYNC-V3.1-HOST-ORCHESTRATION.md` with authority/member round ordering, one checkpoint published once and broadcast to all members, Joining→FULL_REPLACE→installed ACK→Active→new author epoch lifecycle, per-session reader/sequence ownership, and composed host commit-lease rules.
- [x C] The recipe uses the new `SendAcceptedRetentionCheckpointAsync` surface and explicitly keeps timers/scheduling in the application rather than protocol core.
- [x C] Application scheduling is intentionally host-owned: BIN-SYNC exposes explicit roster/frontier/checkpoint/FULL_REPLACE one-message operations and the host orchestration recipe documents round ordering, broadcast, lifecycle and cancellation. No timer/background scheduler is added to protocol core; this is a deliberate architecture boundary, not unfinished runtime work.
- [x C] Deterministic signed-payload vectors and an executable C# vector sample now cover NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN. [~ T] Run the sample, add trust-store decode/verify and negative frame/payload cases; full frame-envelope vectors remain T.

## 0.21. C-pass: one durable checkpoint per authority broadcast round — 2026-11-09

- [x C] Added `SendAcceptedRetentionCheckpointAsync` to the coordinator. It verifies the local authority role, accepted roster generation, currently persisted checkpoint sequence/digest and authority signature before sending.
- [x C] Refactored `PublishAndSendRetentionCheckpointAsync` to publish once and use the same verified send path. Multi-member hosts can now call `PublishRetentionCheckpoint` once and broadcast the same durable signed checkpoint to every Active member without incrementing checkpoint sequence per destination.
- [x C] This keeps checkpoint sequence a round-level authority artifact rather than a per-peer send counter; transport remains host-owned and no timer/background worker was added to protocol core.
- [~ T] Verify same checkpoint can be sent to multiple sessions, stale/forked checkpoint rejection, wrong authority seed rejection and exact accepted-roster generation binding.

## 0.20. C-pass: cross-repository sequencing contract mirrored — 2026-11-09

- [x C] Mirrored the FLD roadmap's required sequencing and two-DLL dependency into `docs/FLD-DB-INTEGRATION-CONTRACT.md` §9.6.
- [x C] The contract now explicitly distinguishes the BIN-SYNC Storage-only Metadata Store adapter from the later synchronized-FLD JournalSeq provenance adapter; neither may be placed inside `Fld1.Storage`.
- [x C] The host adapter is now bound to the public `FldDatabase` API in the optional sample. [~ C] Finish generic Storage backend/API compatibility and exact-head FLD/sample build validation.

## 0.19. Cross-repository adapter dependency revalidated — 2026-11-09

- [x C] Re-read FLD-DB `SYNC-STORAGE-COMPACTION-ROADMAP.md` §§7.19–7.20 and the current FLD branch project graph. The JournalSeq→ChangeId→OutboxSeq adapter is explicitly a later integration gate, after neutral semantic operations/Data Provider conformance, BIN-SYNC persistence refactor, FLD Storage extraction, and the Storage-only Metadata Store adapter.
- [x C] Confirmed FLD still builds the pre-extraction `Fld1.Core`, `Fld1.Engine`, `Fld1.Manifest`, `Fld1.Run` and `Fld1.Wal` projects separately; `Fld1.Storage` is not yet the persistence backend of `FldDatabase`. The approved two-public-DLL boundary is not yet the actual project/package graph.
- [x C] FLD roadmap explicitly forbids a premature direct Node↔FldDatabase bridge or early NuGet adapter. Therefore the current BIN-SYNC Storage map primitive and sample are valid independent C work, but a concrete synchronized-FLD adapter must remain blocked until the upstream architecture gates close. Do not violate the two-DLL boundary just to clear a checkbox.
- [x C] Host adapter/scanner source is implemented and remains separate from the Storage-only Metadata Store adapter. [~ C] Cross-repository build validation and generic Storage backend compatibility remain; Storage-only mode must not instantiate `FldDatabase`.

## 0.18. C-pass: relay provenance storage golden vector — 2026-11-09

- [x C] Added `docs/golden/RELAY-PROVENANCE-MAP-v2.txt` with deterministic root, cursor key/value and OutboxSeq-ordered mapping key/value bytes, including exact lengths and endian rules.
- [~ T] The vector is a source-derived fixture, not yet compared with actual persisted rows. T must run the sample against a temporary StorageDatabase and compare byte-for-byte; do not mark storage/wire golden-vector coverage complete from the document alone.
- [x C] Complete frame registry is audited and signed NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN payload vectors plus executable sample are authored. [~ T] Compare against C# encoder/decoder and full frame envelopes.

## 0.17. C-pass: namespace range matches Node API — 2026-11-09

- [x C] The relay provenance map now accepts the full unsigned 16-bit namespace range, including namespace 0, matching `StorageSyncCoordinator` and Node's namespace contract. The stricter positive-only rule belongs to the signed namespace-GC store, not this generic mapping primitive.
- [~ T] Add namespace 0 and 65535 boundary checks to the map conformance cases.

## 0.16. C-pass: sample catalog consistency — 2026-11-09

- [x C] Updated `samples/README.md` to remove the stale claim that there are only three executable samples after adding `RelayProvenanceMap`.
- [x C] The sample catalog now lists the relay mapping sample and its run command alongside the existing canonical v3 examples.
- [~ T] The complete sample set remains unexecuted on the current C-pass head by design.

## 0.15. C-pass: single ChangeId index on recovery — 2026-11-09

- [x C] Restart reconstruction now inserts each decoded ChangeId directly into the shared `HashSet<BatchId>` and uses that same set to detect duplicates. Removed a second temporary identity set that duplicated all ChangeIds during recovery.
- [~ T/O] Verify the shared set and ordered list remain consistent through restart, append and ACK prune; quantify memory/throughput trade-offs in O.

## 0.14. C-pass: compact value-type ChangeId membership — 2026-11-09

- [x C] Replaced the temporary string-hex membership index with `HashSet<BatchId>` and stored the value-type `BatchId` in each in-memory mapping entry. This avoids a separate 32-character string allocation per retained ChangeId while preserving O(1) average duplicate detection.
- [x C] Persisted mapping values remain the canonical 16-byte ChangeId; restart reconstruction builds the same value-type index.
- [~ T/O] Measure the actual managed heap cost of the list + value-type HashSet against the 16-byte-per-entry durable row; compare with a sorted binary-search identity check before accepting the memory trade-off.

## 0.13. C-pass: avoid per-page full-index ChangeId scans — 2026-11-09

- [x C] Added a shared in-memory ChangeId membership set alongside the OutboxSeq-ordered mapping index. AppendPage now checks existing and same-page duplicate identities in O(1) average time per ChangeId rather than rebuilding/scanning a set from every retained mapping on each page.
- [x C] Restart reconstruction populates both indexes from the same bounded cursor pages. Post-FLD-ACK pruning removes ChangeIds from the membership set together with their mapping rows.
- [~ T/O] Validate append/prune/reopen index consistency and measure retained-memory overhead of the extra 32-hex-character membership keys; if allocation/footprint is worse than an alternative compact identity index, revert or replace based on measurements.

## 0.12. C-pass: public relay provenance sample — 2026-11-09

- [x C] Added `samples/RelayProvenanceMap` as a standalone executable sample referencing only the approved BinSync.Core + BinSync.Storage runtime projects.
- [x C] The sample demonstrates multiple ChangeIds sharing one FLD JournalSeq, partial-ACK retention, durable map reopen, safe-through translation, map pruning after FLD-ACK confirmation and rejection of an unmapped watermark.
- [x C] Added the sample to `samples/README.md`. It is a storage mapping sample, not a claim that the concrete FLD host adapter or lifecycle scheduler is complete.
- [~ T] Build/run the sample with the rest of the public samples during the T pass; verify it on all supported CI platforms.

## 0.11. C-pass: source-derived BIN-SYNC metadata byte inventory — 2026-11-09

- [x C] Added `docs/BIN-SYNC-METADATA-BYTE-AUDIT.md` from the actual `StorageMetadataStore` encoders/key builders. It records partition-root framing, every current durable row kind 0–22, fixed key/value sizes, dynamic PendingApply/peer-stream/payload formulas and row cardinality ownership.
- [x C] Added the `StorageRelayProvenanceMap` cursor/mapping row sizes and key/value byte layout. Explicitly separated encoded row bytes from storage-engine page/index/WAL/object overhead.
- [x C] The audit confirms the current non-duplication model: one global 45-byte ChangeId identity/provenance value, one payload envelope per retained relay/outbound operation, stream-level peer progress, no per-peer payload copies and no per-operation ACK bitmap.
- [~ C/O] Cross-store totals remain open: FLD Outbox/ApplyJournal/WAL and Fld1.Storage page/index/write amplification are not included; representative cardinalities and payload distributions are required before claiming a total footprint or GC savings.
- [~ T] Audit formulas should be checked against golden persisted-row fixtures during T. No tests/build were run.

## 0.10. C-pass: fail-closed watermark rule documented — 2026-11-09

- [x C] Updated the FLD integration contract to state that an OutboxSeq above the last durable mapping cannot be translated and must wait for source-page scan/resolution catch-up.
- [x C] Reaffirmed the required order: durable FLD relay ACK first, provenance-map pruning second. A crash between them replays the idempotent FLD ACK.
- [~ T] The new above-last watermark rejection and ACK/prune crash ordering still require dedicated tests; no exact-head test result is claimed.

## 0.9. C-pass: fail closed on an unmapped safe watermark — 2026-11-09

- [x C] `TranslateSafeOutboxSequence` now rejects a safe OutboxSeq above the last durable mapping instead of translating it to the scanned-through JournalSeq. The host must first catch up the FLD page scan and ChangeId→OutboxSeq resolution; an unrepresented destination sequence can never advance the FLD relay ACK.
- [x C] The already-committed watermark remains idempotent, and a safe watermark equal to the latest mapped sequence may advance through the durable source scan cursor.
- [~ T] Add no-mapping, below-first, exact-boundary, between-two-mappings, equal-last, above-last and post-prune replay tests.

## 0.8. C-pass: remove recovery sort from provenance index — 2026-11-09

- [x C] Recovery now consumes the persisted OutboxSeq-ordered prefix directly and validates strict destination sequence plus nondecreasing source JournalSeq as each 1,024-row page arrives. Removed the O(N log N) post-load sort and second validation pass.
- [x C] The storage contract now documents the ordered key/value layout and O(log N) lower-bound translation.
- [~ T/O] Verify storage cursor lexicographic order, duplicate/out-of-order key rejection, restart reconstruction and large-stream cold-start cost in the dedicated T/O pass.

## 0.7. C-pass: order provenance rows by destination sequence — 2026-11-09

- [x C] Changed the durable mapping key order to `OutboxSeq` (big-endian), with `JournalSeq + ChangeId` in the value. Prefix-cursor recovery now naturally yields the exact order needed by the per-author destination stream.
- [x C] Safe-ACK translation now uses lower-bound binary search over the OutboxSeq-ordered index (O(log N)), instead of a linear scan. JournalSeq monotonicity is validated during page append and restart reconstruction.
- [x C] Mapping deletion uses the OutboxSeq primary key while retaining the source JournalSeq threshold semantics; no operation payload or peer-specific copy is introduced.
- [~ T/O] Restart/roundtrip, same-JournalSeq ordering, safe-watermark boundaries and large-cardinality translation benchmarks remain deferred to T/O. This changes the newly introduced mapping row layout before it is consumed by a released adapter; no legacy migration is needed.

## 0.5. C-pass: avoid global-commit-driven mapping reloads — 2026-11-09

- [x C] Replaced cache invalidation on global `StorageDatabase.LastCommitSeq` with a database-scoped shared in-process cache keyed by (scope/local-replica root, namespace, author). Every unrelated Metadata Store/provider commit previously forced a full reload of the unacknowledged mapping index; this no longer happens.
- [x C] Multiple `StorageRelayProvenanceMap` instances over the same `StorageDatabase` share the same state dictionary and the same `StorageDatabaseTransactionGate`, so same-process adapters see each other's committed mapping/ACK updates without duplicate scans.
- [x C] Restart still reconstructs the cache from durable rows using 1,024-entry prefix-cursor pages. The Storage single-writer fence remains the cross-process boundary; no independent process may mutate the same database concurrently.
- [~ T/O] Measure cold reconstruction, hot append/translation, memory per retained mapping and contention with Metadata Store commits. Verify shared-instance visibility and restart reconstruction in T.

## 0.4. C-pass: mapping ownership and restart validation hardening — 2026-11-09

- [x C] `AppendPage` now snapshots caller-owned ChangeId bytes before writing and retains those same owned bytes in the in-memory index; caller mutation cannot desynchronize the cache from the committed key.
- [x C] Restart reconstruction rejects duplicate persisted ChangeIds within one author stream and validates monotonic JournalSeq/OutboxSeq coordinates directly in OutboxSeq key order.
- [x C] Mapping recovery is paged at 1,024 rows, bounding temporary page allocations. The retained in-memory index remains O(unacknowledged mappings); this is explicit and must be included in the byte/cardinality audit.
- [x C] The concrete FLD caller, namespace publication gate and host GC scanner are implemented in the optional sample; the mapping store alone still does not authorize physical GC, which remains disabled pending T proof.
- [~ T] Build, restart, malformed-row and crash-cut tests remain deferred until the C pass is complete.

## 0.3. C-pass: bounded mapping recovery and cross-repository contract — 2026-11-09

- [x C] Mapping recovery now reads durable rows through 1,024-entry `StorageSnapshot.PrefixCursor` pages instead of materializing the complete persisted prefix in one temporary list. The in-memory ordered index remains proportional to unacknowledged mappings, which is required for logarithmic ACK translation; no claim of O(page) total index memory is made.
- [x C] Updated `docs/FLD-DB-INTEGRATION-CONTRACT.md` §9.5 with the exact host call order: page read → ChangeId resolution → atomic map+scan-cursor commit → publication gate/send → safe Outbox ACK translation → durable FLD relay ACK → mapping prune commit.
- [x C] The FLD Database source path and public relay/reference APIs are now identified under `src/Fld1.Database/Fld1.Engine/Db/FldDatabase.cs`. The RPM3 coordinator resolver is ready for a host adapter; no FLD types are added to `Fld1.Storage`.
- [~ T] No tests/build were run for these C commits, by design. Compile/API conformance, same-JournalSeq multi-operation, sparse author pages, crash before/after map commit, FLD-ACK-before-prune replay and restart reconstruction remain T-pass cases.

## 0.2. C-pass: durable JournalSeq provenance mapping primitive — 2026-11-09

- [x C] Added `StorageRelayProvenanceMap` in `BinSync.Storage`. It persists per-(scope, local replica, namespace, original author) `JournalSeq → ChangeId → OutboxSeq` rows and the `ScannedThroughJournalSeq` cursor in one Storage write batch. Empty author pages advance the scan cursor without inventing mappings.
- [x C] The mapping codec keeps source JournalSeq separate from BIN-SYNC OutboxSeq and protocol OriginSequence. OutboxSeq is stored big-endian in the ordered key; JournalSeq and the stable 16-byte ChangeId are stored in the value.
- [x C] Added monotonicity/identity validation, same-journal multi-operation support, duplicate ChangeId rejection, cursor rollback rejection and fail-closed malformed-row checks. Prefix-cursor reload reads OutboxSeq order directly and validates nondecreasing source JournalSeq without a post-load sort.
- [x C] Added safe ACK translation: the first source JournalSeq containing any OutboxSeq above the BIN-SYNC safe watermark is retained in full; the preceding JournalSeq is the FLD safe-through. If no such mapping remains, the durable scanned-through cursor is safe.
- [x C] Added `CommitAfterFldRelayAck`: it validates the caller's translated watermark and atomically persists the acknowledged source/destination cursors while pruning mapping rows. Its contract requires the caller to durably call FLD `AcknowledgeRelayBatches` first; after a crash before map pruning, replaying that FLD ACK is idempotent and mappings remain available.
- [x C] The durable mapping primitive is now wired to actual `ReadRelayBatchPage` groups, the namespace publication gate, per-author ceilings, safe ACK translation, durable FLD relay ACK, RPM3 prune and startup reconciliation by `samples/FldBinSyncHost`. [~ C] The separate `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply bridge remains open; T-pass must validate this relay adapter.
- [~ T] Compile/API conformance and crash-boundary tests are deferred to the dedicated T pass, as requested. No test result is claimed for this new code.

## 0.1. Resumption audit — 2026-11-09

- [x C] Re-read the active roadmap, v3.1 roster/GC addendum and FLD integration contract against the current branch files. The durable control-plane send/receive primitives are present in `StorageSyncCoordinator`: roster send/accept, OFR2 publish/send and receive/accept, RCP2 publish/send and receive/accept, and durable FULL_REPLACE installed-ACK exchange. These are callable APIs, not a periodic lifecycle scheduler; do not describe them as an autonomous control-plane loop.
- [x C] Confirmed `AcquireApplicationCommitGuard(hostLeaseFactory)`, host scan snapshot contracts, FULL_REPLACE host draft barrier, and all three mandatory GC scanner entry points are already wired in the coordinator. Physical GC remains disabled.
- [x C] Confirmed the provider snapshot path uses sorted-key lower-bound seeking and bounded 1,024-entry pages. It shares immutable value arrays but still copies an O(key-count) sorted index; therefore only payload duplication is closed, not total snapshot memory bounded to O(page).
- [x C] The synchronized-FLD adapter remains a separate integration boundary from the Storage-only Metadata Store adapter. The host sample now binds the FLD page read, RPM3 map append, publication/send gate, safe-ACK translation, durable FLD ACK and post-ACK map pruning. Do not put FLD semantic types into `Fld1.Storage` or add a third database runtime DLL. [~ T] End-to-end execution and restart proof remain.
- [x C] Historical implementation used `AcquireReferenceSnapshotGuard` inside the scanner; superseded by §1.25. The scanner now owns only the immutable reference snapshot while the outer composed application guard owns the FLD commit lease. [~ T] Verify disposal and composed lock ordering.
- [x C] Public coordinator APIs are explicit one-message operations and the host orchestration recipe covers member lifecycle (Joining→FULL_REPLACE→installed ACK→Active; Retired→fenced rejoin), one-checkpoint-per-round broadcast, cancellation, transport ownership and persistence. Periodic scheduling remains deliberately application-owned; no background timer/worker belongs inside protocol core.
- [x C] Public sample and canonical signed-payload vectors are now aligned to the audited 0x01–0x1C frame registry. [~ T] Execute the vector sample and verify encode/decode/trust-store/negative cases; no test pass is claimed yet.
- [x C] Fixed framing and row formulas are documented across NodePersistence v17, StorageMetadataStore, FLD Outbox/ApplyJournal/Micro-WAL, Fld1.Storage, Manifest and Run. [~ O/T] Actual workload cardinalities, adaptive receipt grouping savings, per-author progress compaction, PGM/dictionary amplification, in-memory indexes and write/snapshot amplification still require measurement.
- [ ] Do not mark any of these gates complete from documentation alone. The next C slices are the host integration adapter/mapping and public orchestration/sample surface; then the exact source/byte audit. T/O/F remain separate.

## 0. Historical active-status snapshot — 2026-11-09 (superseded by §§1.17–1.23.6)

- **Current branch/head at this refreshed status:** `c5347c6ecc89e49d727cce88215529b89ece79e3` (latest roadmap/status commit; latest production-code commit `7f50a0007ccec1f8a2c53b9782b76d5829dbb7bf`; exact-head CI not yet validated). C-pass commits added the durable relay provenance map (`b041f621`), identity/order hardening (`24b6b590`, `9b50bd0`), paged recovery (`2dcfacc`), shared cache (`51d96d1`) and OutboxSeq-ordered key/binary-search translation (`b9fcbf38`).
- **Last known fully green checkpoint:** `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` (Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS). Later C-pass code is not covered by that result.
- **Workflow:** C implementation first in small commits with immediate roadmap updates; then T (fixtures, fault injection and conformance), O (measured optimization), F (release). Build errors are repaired immediately; test feedback is recorded but test-suite redesign stays in T.
- **C implemented:** signed namespace roster/frontier/checkpoint codecs and durable storage; all-active FULL_REPLACE install ACK tracking; sender/receiver with durable staging/resume; forgotten-origin replay fence; receipt/ChangeId/tombstone compaction plumbing with all physical gates disabled; bounded provider prefix cursors and streamed snapshot row validation; member frontier and authority checkpoint production APIs; OFR2 signed issued-origin/max-HLC reports, pending-draft barriers and terminal author closure; NRS2 immutable retired-origin epoch closures; all-active applied-through-final-issued checkpoint derivation; mandatory post-FULL_REPLACE HLC floor refresh/Node rehydration callback; FRA2 signed installed ACK HLC floors and RCP2 authority checkpoint binding requiring identical floors from all Active members; ordered coordinator→Node→shared-Storage application commit barrier with shared-database identity enforcement and deadlock-safe Node checkpoint persistence; local author-epoch terminal closure/rejoin; freeze-time per-namespace/author/epoch OriginSequence allocator; durable stamping of published winners before provider feed ACK; FULL_REPLACE draft-to-published identity upgrade; durable coordinator control-plane exchange for NRS2/OFR2/RCP2/FRA2; immutable old-epoch closure enforcement after rejoin.
- **Origin contract:** provider feed cursor is not OriginSequence. Coalescible drafts carry ChangeId + author epoch and OriginSequence=0. Metadata Store allocates contiguous OriginSequence only at immutable freeze and persists it with the ChangeId ledger. Coalesced-away drafts consume no origin number. Remote operation apply requires a complete non-zero origin tuple.
- **GC status:** receipt, ChangeId and tombstone/ghost physical deletion remain fail-closed/disabled. Receipt additionally requires retired transport epoch, preserved receive cursor/digest chain, signed origin frontier and no PendingApply. Author-issued horizon, retired-origin closure and checkpoint-bound global HLC floor are represented by OFR2/NRS2/FRA2/RCP2. Physical receipt, ChangeId and tombstone/ghost GC remain disabled until host WAL/FLD plus PendingApply/relay/reservation/snapshot live references are covered by the composed lease and the T-pass proves crash/replay safety. Never enable physical GC in the C pass.
- **Latest observed CI before the current code slice:** exact build succeeded and Fuzz/Lab passed; Core had one outdated tombstone fixture failure and Storage Integration had 53 failures sharing the outdated zero-OriginAuthorEpoch fixture assumption. These are T-pass fixture updates, not a reason to relax production v3 validation. Exact-head CI for the current code is pending.
- **Remaining C work (historical snapshot, superseded):** at the time of this 2026-11-09 status the two-DLL FLD extraction and concrete provider/relay bridge were still open. They are now implemented on `FLD-DB/fix/crash-consistency-outbox` and `BIN-SYNC/feature/fld-external-storage`: `Fld1.Storage.dll` + `Fld1.Database.dll`, the `FldDatabaseDataProvider` bridge (§FLD roadmap 7.93), durable author-epoch closure (7.94), FULL_REPLACE receiver staging/switch (7.95), snapshot watermark binding (7.97), and subsequent Run/blob/snapshot hardening (7.98–7.102). Remaining source-level work is the final public Storage API/cross-repository contract audit and any explicitly open publication-mode decisions; T-pass build/conformance/crash tests remain separate. App scheduling remains deliberately host-owned. Preserve no legacy API/fallback decoder.
- **T pass is not complete.** Historical checklist entries below are execution history; this active section and latest dated execution entries supersede earlier snapshots.

### C-pass continuation — 2026-11-09

- [x C] Updated the test-only `VolatileMetadataStoreAdapter` to implement `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeIdentity`. It returns fingerprint and immutable non-zero Origin from the same ChangeId ledger entry, rejects identities owned by incoming routes, and fails closed for incomplete frozen identities. This restores interface completeness without weakening production validation.
- [ ] T remains deferred: migrate provider-draft fixtures to non-zero author epoch with `OriginSequence=0`; update test providers to implement `IProviderCommitGuard`; add freeze-before-stamp crash-window coverage. The exact-head CI must be rerun after the C pass.
- [x C] Authority/member one-round orchestration APIs and host recipe, signed-payload vectors/sample, fixed source-derived metadata byte audit and frame registry inventory are present.
- [~ C] Remaining upstream C gates: generic Storage backend/WAL compatibility, final public API/package conformance and transport-scheduler ownership. Concrete FLD relay publication/ACK and reference-snapshot adapter source are implemented in §0.93–0.94. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled until composed host live-reference proof and adversarial T pass.


## 1. Target architecture

- One immutable published operation/payload per (scope, namespace, ChangeId), shared by every peer.
- Origin tuple (authorNodeId, authorEpoch, originSequence) assigned once at original freeze and preserved unchanged.
- One compact progress row per (peer, namespace, author stream); no per-operation ACK bitmap or per-peer payload copy.
- One global ChangeId ledger/dedup authority independent of delivery route.
- One compact receipt per accepted batch; PendingApply exists only while provider apply is unresolved.
- WAL is recovery; FLD/provider feed is unpublished local-change authority; BIN-SYNC is published-operation and delivery authority.
- No duplicate pending journal or duplicate authoritative payload after handoff.
- Storage engine is opaque/reusable; FLD Database owns field semantics. Exactly two DLLs.
- No v2 compatibility, migration fallback, dual-write or legacy public API.
- Receipts, ChangeId ledger and tombstone/ghost have separate GC proofs.

## 2. Current baseline

### BIN-SYNC: feature/fld-external-storage

- Current HEAD: `7dae12a1b60fe481f52febfe4c5692400ca029d1`; exact-head dotnet workflow `42966709635` passed on Ubuntu and Windows. BIN-SYNC tests: 678/678 per OS; benchmark build, public samples, coverage threshold, fuzz/lab and v3.1 signed vectors passed.
- FLD-DB integration branch `fix/crash-consistency-outbox`: current HEAD `1b8312a4f95a128f9af2e66526c60e27fe01192e`; code checkpoint `e7c23082edfd0d65699e708d272f291e894ebcd9` passed. The later FLD commit contains ADR wording only.
- Storage Integration run `42966709649`: 226/226 passed on Ubuntu, Windows and macOS. Cross-repository FLD host adapter build and tests passed on Ubuntu and Windows.
- Database IPC snapshot streaming is implemented and tested; this is not the full Shared Engine Host/IPC/SHM multi-application mode, which remains a release gate. Multi-process concurrent client-operation and remaining WAL/Node/Metadata crash-cut proofs remain open.
- Physical receipt, ChangeId, tombstone and ghost GC remain disabled. No GC authority is inferred from the applied-origin frontier alone.
- Backup refs created: BIN-SYNC `backup/2026-11-11/main`, `backup/2026-11-11/feature-fld-external-storage`; FLD-DB `backup/2026-11-11/main`, `backup/2026-11-11/fix-crash-consistency-outbox`. Do not create additional working branches.
- Next implementation focus: Phase 4.3 tombstone/ghost retention proofs, then the remaining Phase 3 multi-process and Phase 5 public API/sample conformance gates.

## 3. Execution checklist

### Phase 0 — v3 contracts and authority boundaries

- [x] Review current BIN-SYNC v3 draft and provenance design.
- [x] Review metadata storage, Change staging/retention, metadata GC/retention, Metadata Store and Data Provider conformance, authority migration and FLD integration contracts.
- [x] Review FLD history-storage audit, compaction roadmap, Storage Core architecture, kernel spec and tombstone retention research.
- [x] Record clean-break policy: no v2 readers, migration, dual-write or fallback.
- [x] Record ownership split: WAL / unpublished provider feed / published BIN-SYNC operations / peer progress / receipts / PendingApply.
- [x] Record two-DLL decision and opaque Storage boundary.
- [ ] Reconcile historical v2 statements retained in main spec; clearly non-normative.
- [ ] Align FLD Kernel spec and cross-repository roadmap to exact v3 Origin/publication contract.
- [ ] Publish golden v3 wire and storage vectors.

### Phase 1 — Repair red build and finish immutable Origin end-to-end

- [~] Fixed the initial tuple/model mismatch in Node.DeltaBatch.cs by carrying Origin in the outbound metadata item tuple; continue auditing every clone and serialization boundary.
- [x] Introduced canonical OperationOrigin and integrated it into local Outbox creation, OutboxEntry, MetadataOperation, PendingApply encoding/decoding, Storage ledger comparisons, relay/outbound DTOs, Node checkpoint and inbound ledger fingerprinting. Provider freeze intentionally receives an incomplete pre-freeze draft (local author supplied separately; durable sequence allocated atomically by Metadata Store); snapshots are state images with separate snapshot-generation provenance, not PublishedOperations, and MUST NOT synthesize per-operation Origin.
- [~] Local outbound freeze assigns OriginSequence from the durable author stream sequence and preserves supplied Origin on replay; audit atomic allocator/retry behavior and provider freeze before closing.
- [x] Removed the invalid equality between OriginSequence and peer-relative sender cursor; direct-author validation only rejects an Origin epoch from the future.
- [x] Make provenance mandatory in every v3 DELTA_BATCH; remove optional ImmutableOriginProvenance capability and reject protocol-major mismatch. HELLO and HELLO_ACK both carry and validate ProtocolMajor=3.
- [x] Preserve immutable Origin through preflight, persisted PendingApply/recovery, provider apply decision, relay and Storage restart; integration tests compare the same OperationOrigin before interruption, after reopen/recovery and in the relayed item. Snapshot is a state image, not a list of published operations; it must not synthesize per-operation Origin. Its authoritative FULL_REPLACE generation is a separate Phase 4 requirement.
- [x] Cover direct author, A→B→C→D relay, alternate-route deduplication, duplicate delivery and Origin equivocation in Core scenarios/tests; exact-head CI passed.
- [x] Same ChangeId with changed semantic fingerprint or immutable Origin is rejected atomically; same operation via another route does not create a second relay payload (regression coverage green).
- [x] Exact-head Core test + Fuzz/Lab workflows passed; Storage Integration passed on Ubuntu, Windows and macOS. Samples/build are included in the Core workflow. Keep release-wide coverage/threshold and golden-vector gates in Phase 6.

### Phase 2 — Canonical metadata layout and redundant-fact removal

- [x C] Source-derived fixed framing/row formulas are documented for StorageMetadataStore, NodePersistence v17 checkpoint, FLD Outbox, ApplyJournal, Micro-WAL v2, Fld1.Storage snapshot/WAL, Manifest and Run. [~ O/T] Actual workload cardinalities, variable PGM/dictionary amplification, in-memory indexes, filesystem/page overhead and measured write/snapshot amplification remain open.
- [ ] Establish one canonical v3 format marker and row codec; reject absent/old/unknown markers.
- [x] Relay/outbound payload rows persist only ChangeId[16] + payloadLength[u32] + payload; Origin is hydrated from the single global 45-byte ChangeId identity row. Missing ledger rows and author/stream mismatches fail closed. Physical compaction now deletes current row kinds 20/21 after reopen; exact CI passed 155/155 on all three OS platforms. [~] Full cross-store cardinality/byte audit remains.
- [x] Node global ChangeId ledger stores a fixed 16-byte BLAKE3 identity digest that binds semantic mutation + immutable Origin tuple in one hash pass; no separate 29-byte Origin suffix is stored in the ledger value. [~] Storage metadata identity-row cardinality and cross-store duplication still require a byte-level audit and benchmark.
- [ ] Collapse repeated author IDs in ACK/pushed/offset maps into one sparse progress row only if benchmarks prove a win.
- [x C] `AcknowledgeOutboundBatch` validates the exact pushed reservation (peer/epoch/namespace/author stream, BatchId, cursor end and digest), then persists `AckedCursor`, digest-chain tail and `PendingBatch=null` together in the single peer-stream row/Storage batch. No separate reservation-delete transaction exists. [~ T] Inject crash/reopen around this commit and prove exact retry/duplicate ACK behavior.
- [x C] Audited Inbox receipt, per-author receive cursor and per-sender/epoch/namespace digest-chain tail: their keys/cardinalities and retry/ACK semantics differ (per BatchId vs per author stream vs per sender stream). A single receipt cannot replace these without duplicating stream state per batch or losing digest/cursor boundaries; keep the three compact row kinds separate. [~ O] Revisit only with measured cardinality/byte evidence and a proof preserving all three semantics.
- [ ] Remove duplicate relay payload copies and unnecessary WAL/ApplyJournal duplication only after recovery proof.
- [x C] On Metadata-Store-owned publication cutover, Metadata Store becomes the only active ChangeId authority and Node-owned writes/receives are fenced. Known Node ledger digests are durably migrated into optional legacy-alias suffixes on the corresponding Metadata ChangeLedger rows before the namespace Node ledger is cleared. Compacted legacy aliases remain distinct from the semantic fingerprint and are reclaimed with that same ChangeId row. `SeenChanges` WAL replay markers remain a separate recovery concern.
- [~] Record before/after bytes/op, bytes/peer-author, allocations, mutation latency and compaction CPU at 500/10k/100k/1m rows. Exact row saving is 12 bytes per retained relay/outbound operation (32+payload → 20+payload); the durable benchmark completed successfully: 10k rows 408.4 µs / 8.14 KB, 100k rows 375.9 µs / 10.34 KB, 1M rows (1,024-operation freeze) 196.9 µs / 7.77 KB. Against the prior recorded baselines (449.2 µs at 10k, 434.1 µs / 10,944 B at 100k, 232.8 µs / 7.84 KB at 1M), these measurements are faster with slightly lower allocation; compare only as same-workflow directional evidence, not as a cross-machine guarantee.

### Phase 3 — Publication and FLD Storage contract

- [x] Provider feed/FLD Outbox is the sole authority for unpublished local changes in Metadata-Store-owned publication mode; no parallel MetadataStore PendingLocal table. The cutover fence rejects a non-empty local feed/Node Outbox state that would make ownership ambiguous.
- [x] Verify LWW field coalescing across provider pages; field writes and record-lifecycle tombstones use distinct coalescing identities. Provider conformance separately covers lifecycle tombstone/ghost ordering.
- [x C] The public `IDataProvider.ApplyRemoteBatch` contract explicitly requires remote APPLY to leave the local unpublished feed and its ACK state untouched; `StorageDataProvider` follows this boundary. [~ T] Same-field remote-wins/restart conformance remains Phase T.
- [x] Freeze ChangeId + immutable Origin + payload into Metadata Store atomically; provider source-feed ACK follows durable freeze and Node checkpoint persistence.
- [x] Freeze replay is idempotent when metadata commit succeeds but provider ACK fails; frozen ChangeIds are recognized and only the later unfrozen suffix is coalesced/published.
- [x C] The optional FLD host adapter resolves each source ChangeId to the active BIN-SYNC immutable identity and appends the JournalSeq↔OutboxSeq RPM3 mapping; JournalSeq is never used as OriginSequence. [~ T] Verify end-to-end before/after cutover and restart.
- [~ T] Single-process Storage/DataProvider/MetadataStore conformance now covers full FLD/Metadata/Node reopen, metadata-owned sender cursor continuation, PendingApply recovery, duplicate replay/ACK and process kill after FLD commit (BIN-SYNC §1.86). The FLD direct-mode second-process ownership probe is added at FLD-DB `fc20577` and awaits CI. Shared Engine client/host, multi-process concurrent client operations and the remaining WAL/Node/Metadata crash cuts remain open.
- [x] Storage DLL remains protocol-agnostic; field/HLC/tombstone semantics stay in Database DLL.

### Phase 4 — Safe GC, independently proven

#### 4.1 Inbox receipts

- [x] Conservative retired-transport-epoch receipt GC and PendingApply retirement guard exist in earlier Storage-backed implementation.
- [x C] Retirement now blocks old PendingBatch reclamation unless the exact reservation is ACKed or the replacement baseline for the same author epoch covers `old StreamOffset + PendingBatch.CursorEnd`. Before the retired-through fence, a stale epoch may only resume/mark-pushed/ACK that exact reservation; it cannot reserve new data. Epoch-zero and cross-author-epoch baselines never discharge the reservation.
- [x] Revalidated canonical v3 receipt identity. Old receipt key/value formats are rejected; v3 rows with unknown transport epoch are retained and never assigned guessed provenance.
- [x T] Tests cover short-baseline blocking, exact stale-epoch retry/ACK, replacement-baseline supersession and author-epoch mismatch. Added an injected crash after WAL flush and before in-memory publish; reopen proves the complete one-commit retirement transaction. Exact-head Storage Integration run 42966709649 passed 226/226 on Ubuntu, Windows and macOS.

#### 4.2 ChangeId ledger

Mandatory extension: [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md). The v3.1 implementation is substantially present and remains under C/T validation; physical receipt, ChangeId and tombstone/Ghost GC remain separately disabled until their independent proof gates pass.

- [x D] Specified authority-signed namespace roster generations, explicit member keys/status/epoch, monotonic generation and roster digest.
- [x D] Specified signed monotonic per-member contiguous applied-OriginSequence vectors bound to roster generation/digest.
- [x D] Specified per-author safe frontier as the minimum across every Active member; Joining/missing/offline/stale/invalid member contributes zero.
- [x D] Specified durable member retirement fence and mandatory FULL_REPLACE before stale-peer rejoin.
- [x D] Specified reference scan across PendingApply, operation payload, relay/outbound reservation, provider intent, snapshot pin, feed lease and apply token.
- [x D] Specified crash-consistent deletion plus monotonic forgotten checkpoint and replay rejection below the frontier.
- [~ T] v3.1 codecs, capability negotiation, signed roster/frontier/checkpoint persistence and all-member install ACK are implemented. A global ChangeId equivocation test now proves cross-peer rejection after durable finalize/reopen (run 42966709649, 226/226 on three OS); remaining roster/frontier/checkpoint adversarial tests are still open. ChangeId GC remains physically disabled.

#### 4.3 Tombstone / ghost

- [x D] Defined the signed authoritative FULL_REPLACE manifest/ACK, roster binding and separate transfer frames in [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md); implementation requires negotiated v3.1 capabilities, not unversioned v3.0 messages.
- [x C] Provider `OpenSnapshot` captures the stable image and source feed watermark in one Storage snapshot; the full-replace digest binds the generation and watermark. Signed wire certificate and all-member binding remain blocked on the roster protocol revision.
- [x D] Specified that every Active member must provide a signed installed ACK for the same or newer generation/digest before tombstone/ghost reclamation.
- [~] All-member installed-generation tracker and provider/metadata proof checks are implemented; physical GC remains disabled pending complete horizon proof and exact-head tests.
- [x C] `IProviderFullReplace` and `StorageDataProvider` stage generation-scoped rows, verify the canonical SHA-256 manifest in bounded scans, fence commit to the captured base generation, and atomically switch the active-generation pointer. `FullReplaceDigestBuilder` streams canonical pages without full-image sorting/materialization; old generations are retained and physical reclamation remains blocked on cross-store safe-GC proof.
- [x D] Specified the forgotten deletion frontier, durable epoch fence and full-rebootstrap rule for retired members.
- [~] Durable frontier/checkpoint persistence and parts of generation fencing are implemented; periodic frontier production, retired-origin closure and full rebootstrap orchestration remain open.
- [ ] Test field tombstone, record tombstone and ghost independently, including offline peers, alternate routes and crashes at every transition.
- [x C] Tombstone/ghost physical GC is explicitly disabled; v3.1 sequence-only frontier is insufficient to rule out delayed pre-snapshot operations from other authors.

### Phase 5 — Public API and clean cutover

- [x C] The public `TcpSyncSession` surface is already removed by the v3 clean cut; `docs/PUBLIC-API.md` explicitly records the removal and generic `SyncSession` as the transport-independent API. No compatibility wrapper is to be restored.
- [x C] Repository code search for `TcpSyncSession`, `TcpSyncClient` and `LegacyTcp` returns no remaining references; current TCP samples use the generic `SyncSession`/`ISyncTransport` path through transport adapters. [~ T] Run the full solution/sample build and public API surface audit on the exact final head.
- [x] Removed optional immutable-Origin provenance negotiation: v3 HELLO/HELLO_ACK carry and require ProtocolMajor=3; `SessionCapabilities` contains no provenance/downgrade bit and decode rejects every other major.
- [x C] On Metadata-Store-owned cutover, Node drops its namespace `GlobalChangeLedger` and Outbox payload projections; `SeenChanges` remains only as the distinct WAL replay marker. Cutover/restart assertions pass.
- [x C] Mandatory `BSP3` provider marker and active-generation pointer are written atomically on fresh open; unmarked/unknown partitions fail closed. Current state keys are generation-scoped; LocalChange rows require exactly one nonzero ChangeId suffix and no read-time synthesis. Exact checkpoint CI green.
- [ ] Ensure all public samples use canonical v3 API.

### Phase 6 — Conformance, performance and release

- [ ] Map every v3.0 normative requirement and every mandatory v3.1 addendum requirement to unit/integration/fuzz/lab tests and exact golden vectors; include capability-negotiation downgrade/absence behavior.
- [ ] Add randomized multi-peer/relay model tests and restart/fault injection at freeze, apply, ACK, snapshot and GC.
- [ ] Verify Linux/Windows/macOS durability semantics; document actual fsync/device/power-loss limitations.
- [ ] Benchmark 500, 10k, 100k, 1m operations and peer/author cardinalities 1, 4, 16, 64.
- [ ] Measure bytes/op, bytes/(peer×author), mutation latency, allocation and compaction CPU.
- [ ] Accept optimization only when correctness is unchanged and target metric improves without material regression elsewhere.
- [ ] Release only when all CI is green on exact release commit, Fuzz/Lab actually ran, samples pass, coverage threshold is met and spec/test mapping is complete.

## 4. GC proof summary

| Domain | Earliest possible reclamation | Hard blockers |
|---|---|---|
| Outbound/relay payload | Minimum contiguous ACK of every required peer/author stream and local consumer | Missing baseline, unacked reservation, active provider feed/lease |
| Inbox receipt | New authenticated transport epoch + complete initial baseline + durable retired-through fence; no unresolved PendingApply | Unknown epoch, pending apply, missing baseline |
| ChangeId identity | Signed all-member origin frontier + no references + durable forgotten checkpoint | Missing provenance, offline member, unsigned/stale checkpoint, alternate-route replay |
| Tombstone/ghost | ChangeId proof + FULL_REPLACE installed by all members + stale-peer fence + forgotten frontier | Merge-only snapshot, offline member, snapshot/feed gap, resurrection path |

Uncertainty means retain. False-positive retention costs space; false-positive deletion can permanently resurrect deleted state or accept an equivocated operation.

## 5. Progress log

Append a dated entry after each meaningful implementation package. Update checklist in the same commit as code/docs it describes.

### 2026-11-08 — v3 roadmap initialized

- [x] Confirmed branch heads and failing workflow run.
- [x] Captured compiler failure at Node.DeltaBatch.cs:861-862; tests did not run because Build failed.
- [x] Captured prior green baseline 92f6631 as historical only.
- [x] Wrote v3 clean-break architecture, metadata ownership, provenance and GC requirements into the normative spec.
- [ ] Code implementation not complete; CI remains red until Phase 1.

### 2026-11-08 — immutable Origin implementation package (active)

- [x] Added OriginAuthorEpoch/OriginSequence to metadata outbound and relay DTOs and persisted stream payload rows.
- [x] Added Origin tuple to global ChangeId ledger value (45-byte canonical identity value) and compare fingerprint + author + epoch + sequence for pending/staged identity checks.
- [x] Added Origin to PendingApply v3 encoding (-7) and reject all older PendingApply encodings.
- [x] Bumped local WAL format to v3 and included Origin coordinates in local mutation records; old WAL versions are rejected.
- [x] Added BMS3 metadata partition identity marker and reject old ledger encodings.
- [x] Preserve Origin in detached Node batch clones; snapshot state roots use a separate non-operation encoding and do not invent OriginSequence.
- [~] CI tests are being updated to include Origin in manual/tampered test batches; last observed run before this fixture package: 640 passed / 14 failed. Re-run CI on exact current head before closing any implementation item.
- [ ] Full ChangeId equivocation, WAL crash-replay and PendingApply recovery tests still required.


### 2026-11-08 — continuation audit after ae5db37

- [x] Located the roadmap at `docs/BIN-SYNC-ROADMAP-v3.md` (not repository root) and re-read its execution checklist and progress history.
- [x] Compared `ae5db37ca4e5c32fe71d320a62c831260ea53fdb` with the current branch: 28 commits ahead, no divergence. The reviewed implementation head is now `56d5142c1a2f7050663a418d4882def50787c1e5`.
- [x] Re-read the v3 normative specification, immutable-Origin/safe-GC design, metadata architecture, Metadata Store and Data Provider conformance contracts, change-staging/retention policy, Metadata GC design, public API contract, spec coverage audit and FLD integration contract.
- [x] Confirmed the old tuple compiler error was real on the PR merge snapshot `a852206fc245c2a0f7469d0e18d19415a2b2c528`: the method accepted a 3-field metadata tuple but dereferenced Origin fields. The current branch version has the 5-field tuple, and the `e6826b4` CI attempt completed Build successfully.
- [x] Confirmed that the CI attempt for `e6826b4` was cancelled during Test, Fuzz/Lab and Storage Integration; this is not a green result and does not supersede the earlier 640 passed / 14 failed evidence.
- [ ] Re-run complete CI on the exact current branch head and capture the 14 test failures individually before changing behavior.
- [ ] Do not mark Phase 1 complete based on the compile fix: end-to-end Origin, PendingApply/recovery, multi-hop relay, equivocation and safe-GC proof gates remain open.


### 2026-11-08 — v3 Storage decoder clean-break package (verification pending)

- [x] Tightened the Storage metadata decoder to reject legacy receipt keys and the old combined receive-cursor/digest row shape; v3 requires the canonical receipt identity and separate author receive cursor / sender digest-chain rows.
- [x] Tightened v3 receipt decoding to reject pre-v3 values without the durable resolved author while preserving the currently optional header-fingerprint and transport-epoch fields used by non-authoritative direct conformance calls. Metadata-authoritative receive still requires the full header fingerprint.
- [x] Replaced the old test that expected legacy ledger/watermark migration with a regression asserting that a BMS3 partition containing a pre-v3 ChangeId ledger row fails closed and is not rewritten.
- [ ] CI has not yet reported for this package. Do not mark the decoder gate complete until Storage Integration and full Core/Fuzz/Lab workflows pass on the exact branch head.
- [ ] Audit and remove now-unreachable `HasLegacyEncoding` rewrite branches only after confirming no current v3 decoder path can set the flag and the regression suite proves old partitions fail closed.


### 2026-11-08 — current branch checkpoint

- [x] Updated the baseline to reviewed head `56d5142c1a2f7050663a418d4882def50787c1e5`; 28 commits ahead of `ae5db37`, no divergence.
- [x] Corrected the distinction between the old merge-build failure, the later successful compile, cancelled test workflows, and the unverified decoder package.
- [ ] Obtain a complete, non-cancelled CI result for this exact head before treating any tests as passed.


### 2026-11-08 — BMS3 first-write identity consistency

- [x] Audited every StorageKey(root, 0, ...) partition-identity write in StorageMetadataStore.
- [x] Found six first-write paths still persisting raw UTF-8 ScopeId while the v3 decoder requires the canonical BMS3 marker + exact scope length + scope bytes.
- [x] Replaced all six with the single EncodePartitionIdentity(partitionKey) encoder. This covers first writes via incoming receipt/batch finalization, outbound freeze, outbound lease, PendingApply, peer metadata delta, and peer stream baseline.
- [x] Existing PendingApply restart conformance test exercises the first-write/reopen path; broader storage integration tests are running.
- [ ] Close this gate only after Storage Integration passes on Windows/Linux/macOS and Core tests remain green on the same source commit.
- [ ] Continue auditing all persistence paths for first-write identity, marker validation and crash/reopen consistency.


### 2026-11-08 — v3 conformance fixture and reference-adapter alignment

- [x] Updated shared Metadata Store conformance factories to provide mandatory Origin epoch/sequence and a resolved single-author stream.
- [x] Reworked inherited multi-author conformance cases into separate author-stream batches; digest chaining remains sender/epoch/namespace ordered while cursors are author-specific.
- [x] Updated direct Storage integration fixtures to carry Origin, including relay, pending-apply, equivocation and coordinator recovery cases.
- [x] Replaced the obsolete PendingApply v3/v4 decode-success expectation with explicit rejection of all pre-v3 formats; current v3 encoding remains round-trippable.
- [x] Corrected snapshot test hashing to use the dedicated snapshot state-root form without operation Origin coordinates; retained mandatory Origin for DELTA_BATCH/Causal fixtures.
- [x] Updated Node persistence format and fixtures to v17 (including its tagged-hash domain); v16 checkpoint payload is rejected.
- [x] Core CI at b60c4a0 reduced the Core failures from 7 to 1; the sole remaining failure exposed the test-only Volatile adapter's stale sender-wide cursor model.
- [x] Updated VolatileMetadataStoreAdapter to model author-specific receive cursors and a separate sender digest chain, matching the v3 storage contract.
- [ ] Re-run Core, Fuzz/Lab, Storage Integration and samples on 4e2ba079678ce3fcaa0058ae43ef652faa33975a; do not close conformance until exact-head CI is green.


### 2026-11-08 — first fully green v3 storage/conformance checkpoint

- [x] Core Build, benchmark build, Core tests, public samples and coverage passed on `6969ba8085281f959ebb210bd481856343605e3a`.
- [x] Core test result: 654/654 passed.
- [x] Fuzz/Lab: 31/31 passed.
- [x] Storage Integration: 149/149 passed on Ubuntu, Windows and macOS (447 successful platform executions).
- [x] Both durable-array and Storage-backed TCP samples passed on all three platforms.
- [x] Strict BMS3 first-write identity was verified through Storage Integration; all six partition-creation paths now use the canonical encoder.
- [x] PendingApply v3 format rejects older -6/-5/-4/-3 encodings; metadata-authoritative recovery requires a full header fingerprint.
- [x] Legacy StorageMetadataStore rewrite branches and their unused whole-partition migration writer were removed in commit `b105868959b26c8d8ef54f90b51b769f07a51f89`.
- [x] Removed the unreachable legacy local-origin marker decoder branch and rejected completed PendingApply receipts that omit the durable author.
- [x] Reconciled the Origin design's direct-author cursor wording with v3's independent OriginSequence/SenderCursor model; no arithmetic inference from peer-relative cursor is permitted.
- [x] Marked older GC and staging analyses as historical/non-normative where they conflict with v3.0.0.
- [x] Exact-head CI on f91df0b4ef93f5a53f419420aa578dfe36bc293b is green: Core tests and Fuzz/Lab succeeded; Storage Integration succeeded on all three configured OS platforms.
- [~] Phase 1 remains open only for a shared canonical Origin value type across Core/Storage/FLD boundaries, provider freeze/replay atomicity, PendingApply/snapshot/restart preservation audit, and crash/recovery proofs. Multi-hop/alternate-route/equivocation regressions now pass.
- [ ] Phase 2 remains open: inventory actual persisted row cardinality and byte cost; benchmark 500/10k/100k/1M; prove Storage metadata does not duplicate the compact Node ledger or operation payload.
- [ ] Phase 3 remains open: FLD provider adapter and atomic cross-store crash matrix.
- [ ] Phase 4 remains open: receipt retirement proof, ChangeId GC proof and tombstone/ghost GC proof.
- [ ] Phase 5 remains open: final public API clean cut, removal of optional receive-authority fallbacks and docs/sample conformance.
- [ ] Phase 6 remains open: release gate, reproducible benchmarks, package/license audit and final clean CI.

### 2026-11-08 — Origin equivocation, v3 handshake and exact-head green verification

- [x] Reconciled 49 commits after the actual previous roadmap checkpoint 4e2ba079678ce3fcaa0058ae43ef652faa33975a; branch is ahead by 49 with no divergence (56d5142 was an earlier comparison anchor, not the last roadmap checkpoint).
- [x] Added/verified four-node A→B→C→D relay plus alternate-route dedup scenario (Scenario43_FourNodeOriginRelayAndAlternateRouteDedup).
- [x] Added atomic rejection coverage for reusing a global ChangeId with a different immutable Origin tuple; checks stored value, inbox/outbox and receive cursor remain unchanged.
- [x] Removed optional ImmutableOriginProvenance capability negotiation. HELLO and HELLO_ACK now carry ProtocolMajor=3; v2 payload length and major 2/4 are rejected.
- [x] Node checkpoint format is v17; v16 checkpoint payloads are rejected. WAL v3 rejection diagnostic corrected.
- [x] Replaced two-stage Fingerprint + BindOrigin hashing on Outbox, inbound ApplyBatch and WAL replay with LedgerFingerprint: semantic mutation and immutable Origin are encoded into one canonical BLAKE3 input. This removes one hash pass and avoids a second 16-byte digest intermediate on these paths.
- [x] Added direct tests for Origin author/epoch/sequence sensitivity of the single-pass ledger fingerprint.
- [x] Exact-head CI for f91df0b4ef93f5a53f419420aa578dfe36bc293b: BinSync.Net test workflow succeeded; Fuzz/Lab (§17) succeeded; FLD Storage Integration succeeded on Ubuntu, Windows and macOS.
- [~] Follow-up review: v17 fixture name/temp-file label and checkpoint image XML comment were corrected in commits 24d22b0 and e7e1eb4. Verify exact-head CI after these two small follow-ups. Do not infer test-count totals from successful workflow status alone.
- [ ] Next: finish audit of canonical fingerprint equivalence for all operation types and causal dependencies; add provider-origin freeze/replay and PendingApply crash-recovery tests; then continue Phase 2 row-cardinality/byte-cost audit.

### 2026-11-08 — handshake wire contract synchronized

- [x] Audited the implementation's HELLO and HELLO_ACK encoder/decoder against the v3 specification.
- [x] Added normative exact payload layout to §12.7–12.8: NodeId b128, epoch u32 LE, protocolMajor u16 LE (=3), capabilities u32 LE; exactly 26 bytes, control stream 0, flags 0; HELLO/ACK are distinct frame types.
- [x] Documented fail-closed major/length/frame validation and the canonical implementation sequence numbers.
- [ ] Verify CI on the latest branch head. CI for code head f91df0b was green; runs after the test-name/comment follow-ups were cancelled, not failed. No implementation behavior changed in this spec-only package.

### 2026-11-08 — latest CI reconciliation after follow-up commits

- [x] Rechecked the full commit graph from the previous roadmap checkpoint: 50 commits ahead, 0 behind.
- [x] Rechecked workflow attempts for the v17 fixture/comment follow-ups: test and Storage jobs were cancelled; Fuzz/Lab succeeded in the first attempt. A later rerun attempt was also cancelled.
- [x] Classified the state correctly as UNVERIFIED for the latest head, not red: the last completed full green run remains f91df0b (Core tests, Fuzz/Lab and Storage Integration on three OS platforms).
- [ ] Obtain a non-cancelled run on the current code head before closing exact-head verification.

### 2026-11-08 — architecture-note reconciliation

- [x] Re-read the five roadmap-linked design documents: Change Staging/Retention, Metadata Storage Architecture, Metadata Store GC, Origin Provenance v3 and FLD-DB Integration Contract.
- [x] Added an explicit v3 normative override to Metadata Storage Architecture so its historical v2–v16 format analysis cannot be mistaken for active compatibility/migration requirements.
- [x] Corrected the architecture status to NodePersistence v17 / WAL v3 and distinguished the Node-local 16-byte Origin-bound digest from the separate 45-byte Storage Metadata ChangeId identity row.
- [x] Recorded the latest completed green checkpoint (f91df0b) and classified subsequent cancelled CI as UNVERIFIED, not failed.

### 2026-11-08 — canonical OperationOrigin implementation package (verification pending)

- [x] Added immutable OperationOrigin value type with non-zero author/epoch/sequence validation and explicit invalid-default handling.
- [x] Added canonical Provenance projections to OutboxEntry, MetadataOperation, MetadataRelayItem and MetadataOutboundItem; relay-local Sequence remains separate from Origin.Sequence.
- [x] Routed local Outbox creation, batch relay staging, DeltaBatch validation, Node checkpoint serialization, WAL replay and Node global ledger fingerprinting through OperationOrigin.
- [x] Routed Storage ChangeId ledger matching, 45-byte identity encoding, PendingApply encoding/decoding, relay/outbound cloning and published payload encoding through OperationOrigin.
- [x] Added unit tests for invalid/default provenance, value equality and separation of local relay sequence from immutable origin sequence.
- [x] Exact-head CI on 6e5cdb7: Core 662/662, Fuzz/Lab 31/31, Storage Integration 150/150 on Ubuntu/Windows/macOS. Durable Metadata Bench completed successfully. Follow-up remains for byte-level metadata reduction and row-cardinality audit.

### 2026-11-08 — OperationOrigin verification and metadata benchmark baseline

- [x] Exact-head Core tests passed 662/662; Fuzz/Lab passed 31/31; Storage Integration passed 150/150 on Ubuntu, Windows and macOS (head 6e5cdb76be8e9322227561ca12012855a0910bdf).
- [x] Durable Metadata Bench completed successfully after enforcing nonzero OriginAuthorEpoch at outbound freeze, fixing all benchmark fixtures and changing the CI report detector to catch padded NA cells.
- [x] Baseline: freeze 1,024 operations with 1,000,000 existing ChangeId rows = 232.8 µs, 7.84 KB managed allocation per invocation.
- [x] Baseline: one-operation freeze at 10k rows = 449.2 µs; at 100k rows = 434.1 µs, 10,944 B allocated.
- [x] Baseline: coordinator retry scan = 1.883 ms / 256 pending rows, 4.430 ms / 1,024, 17.550 ms / 4,096 (allocations 750.87 KB, 3,021.66 KB, 12,193.41 KB respectively). This linear scan/allocation profile is a later optimization candidate; do not change it without a measured improvement.
- [x] Identified a concrete metadata duplication candidate for Phase 2: each relay/outbound payload row repeats OriginEpoch+OriginSequence already stored in the global 45-byte ChangeId identity row. Current stream payload value is 32+payload bytes; candidate is 20+payload bytes with Origin hydrated from the ledger in-memory, saving 12 bytes per operation without extra storage I/O. Benchmark before/after and retain only if throughput/allocation remain acceptable.

### 2026-11-08 — GC design audit corrected to current v3 implementation

- [x] Reconciled the GC design document with actual code: mandatory v3 Origin, 45-byte Storage ChangeId identity, 16-byte Node-local Origin-bound digest, PendingApply format -7, Node checkpoint v17 and WAL v3.
- [x] Corrected stale statements that claimed Origin was absent and optional capability negotiation was required.
- [x] Kept ChangeId GC and tombstone/ghost GC disabled. Origin is now available, but the authoritative signed namespace roster, per-member/per-author contiguous applied-origin frontier, FULL_REPLACE snapshot generation and stale-rejoin fence are not implemented.
- [x] Explicitly documented that Origin gaps are allowed and that a safe frontier must be contiguous; relay/receive/Outbox cursors cannot substitute for it.
- [x] Removed the duplicated 12-byte OriginEpoch/OriginSequence suffix from each relay/outbound payload row; hydration uses the already-loaded ChangeId ledger (no additional Storage read). Added exact persisted-row and fail-closed corruption tests. Benchmark confirmation remains open.

### 2026-11-08 — compact stream-row regression and CI correction

- [x] Implemented the 12-byte-per-operation reduction: relay/outbound payload rows now store only ChangeId (16 B) + payload length (4 B) + payload; immutable Origin is resolved from the global ChangeId ledger instead of duplicated in each stream row.
- [x] Updated remaining compact payload writers and added durable integration coverage for the exact 20-byte fixed overhead, payload round-trip, and rejection of legacy stream-row layouts.
- [x] Exact-head Core/Fuzz workflow succeeded on the implementation head; Storage Integration exposed one xUnit analyzer error (xUnit2031) in the new assertion, not a production-code or runtime failure.
- [x] Fixed the analyzer violation by using Assert.Single(collection, predicate) in commit `95140bb`.
- [x] Re-ran exact code head `f6f041b`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 on Ubuntu, Windows and macOS. The xUnit2031 assertion issue and the stale compaction key-kind bug are fixed.

- [x] Tightened the compact-row corruption boundary with explicit fail-closed tests for orphaned payload rows and ledger/stream author mismatches during hydration (see 2026-11-08 compact-row fix below).
- [x] Fixed the compact-row compaction issue: deletion now targets current row kinds 20/21, with reopen assertions proving physical removal while retaining ChangeId identity (see 2026-11-08 compact-row fix below).
- [x C] Namespace membership roster, per-author issued/applied frontiers, FULL_REPLACE-bound checkpoint, retired-origin closure and replay fence are implemented. [~ T] Physical GC remains disabled pending composed host live-reference proof, safe-horizon adversarial tests and storage/recovery validation.

### 2026-11-08 — compact-row compaction fix and exact-head green

- [x] Corrected compaction to delete current persisted relay/outbound payload row kinds 20/21 (previous code still targeted historical kinds 4/7).
- [x] Added a reopen assertion proving outbound payload rows are physically absent after compaction; retained ChangeId identity still prevents republishing the same operation.
- [x] Added fail-closed tests for compact payload rows with a missing global ChangeId ledger row and for author mismatch between stream key and ledger provenance.
- [x] Exact code head `f6f041bfc9b2841d33c2dbf4e39fff160fc6d4ee`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 independently on Ubuntu, Windows and macOS. Both push and PR workflow matrices passed.
- [~] Durable Metadata Bench triggered by the Storage implementation change is still running; keep throughput/allocation acceptance open until its report is inspected.

### 2026-11-08 — Origin recovery assertions and provider-feed audit

- [x] Extended PendingApply interruption and Node-checkpoint/restart integration tests to assert exact OperationOrigin equality from the source Outbox through the durable PendingApply batch and recovered relay row.
- [x] Exact code head `69b3710bd4bb66d0b85c86b2ff8008af4754263a`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 on Ubuntu, Windows and macOS; push and PR workflows both green.
- [x] Audited Metadata-Store-owned publication: fresh provider changes supply author epoch but no OriginSequence; Metadata Store allocates the durable author stream sequence during atomic freeze, then the coordinator checkpoints Node cache and only then ACKs the provider feed.
- [x] Confirmed coalescing across provider page boundaries and separation of field vs record-lifecycle coalescing keys.
- [x] Added crash/ACK-failure test for Metadata-Store-owned freeze: first operation remains immutable after provider ACK failure; a later same-field edit becomes a suffix operation; retry retains both ChangeIds/payloads and does not duplicate or renumber the frozen prefix.
- [x] Regression test seeds/ACKs provider feed through sequence 5, then freezes feed entries 6/7; Metadata Store assigns OriginSequence 1/2, proving source feed sequence is not copied into OriginSequence.

### 2026-11-08 — durable metadata benchmark after compact-row reduction

- [x] Durable Metadata Bench run `42487879970` completed successfully after the compact Storage layout and compaction-key correction.
- [x] Freeze one local operation at 10k ChangeId rows: 408.4 µs / 8.14 KB allocated (previous recorded baseline: 449.2 µs).
- [x] Freeze one local operation at 100k rows: 375.9 µs / 10.34 KB (previous baseline: 434.1 µs / 10,944 B).
- [x] Freeze 1,024 operations with 1,000,000 existing ChangeId rows: 196.9 µs / 7.77 KB (previous baseline: 232.8 µs / 7.84 KB).
- [x] The compact stream row saves exactly 12 persisted bytes per retained relay/outbound operation; correctness and physical compaction tests pass. No benchmark evidence of a freeze-time/allocation regression; keep the optimization.
- [~] Coordinator retry scan remains linear: 1.196 ms / 256 rows, 5.055 ms / 1,024, 13.823 ms / 4,096; allocations 744.59 KB, 2,997.63 KB, 12,097.38 KB. This is a measured future optimization candidate, but do not change without a better measured trade-off.

### 2026-11-08 — metadata-owned ACK-failure retry proof

- [x] Added a faulting IDataProvider wrapper that fails exactly the first feed ACK after Metadata Store freeze commits.
- [x] Test seeds and acknowledges provider feed rows 1–5, freezes ChangeId at feed sequence 6, injects ACK failure, appends a later same-field edit at feed sequence 7, then retries.
- [x] Verified feed sequences 6/7 map to immutable Metadata Store operations with OriginSequence 1/2; the first payload remains `first-frozen`, the suffix payload is `later-suffix`, both ChangeIds remain stable, and feed ACK completes only after the suffix freeze.
- [x] Storage Integration exact test code head `98b956d`: 156/156 passed on Ubuntu, Windows and macOS. Core exact head `1dfb7d6`: 662/662 and Fuzz/Lab 31/31 passed. A transient 1-second FlowControl test timeout was made scheduler-tolerant (5 seconds); no production FlowControl code changed.


## 6. Execution protocol — staged implementation (C → T → O → F)

The work is intentionally split into short, reviewable commits. The checklist remains authoritative; update it in the same commit as each implementation package.

- **C — broad code implementation:** traverse every remaining phase and implement the planned production-code structures/contracts in small coherent slices. Mark in-progress checklist entries `[~ C]` (or add a `C` marker to the corresponding progress-log item). Do not spend a whole cycle polishing one test suite while later phases remain untouched. Do not weaken invariants, introduce unsafe GC, or silently change normative contracts merely to make the broad pass compile.
- **T — correctness/conformance:** after the broad C pass, revisit each C-marked slice; add/fix unit, integration, model, fault-injection and restart tests; close only with exact-head CI evidence.
- **O — optimization:** profile and benchmark accepted behavior; reduce persisted bytes, allocations, copies, scans and lock contention only when measured results improve without material regression elsewhere.
- **F — finalization:** reconcile specification/docs/public API/samples, run complete CI and platform durability checks on the exact release commit, and close the release gates.

A C marker is explicitly provisional, not a claim of correctness or test completion. Existing [x] entries retain their evidence; do not reopen them without contradictory evidence. Every meaningful code commit must include the matching roadmap checklist/progress-log update. Commit slices should be small enough that progress remains visible and CI can identify regressions quickly. Never batch unrelated phases into one large commit.

### 2026-11-08 — staged C/T/O/F execution adopted

- [x] Reconciled the actual branch head as `1dfb7d686dd61eb80d3ade29b1392a6ffc5acdda`; it is 147 commits ahead of `ae5db37ca4e5c32fe71d320a62c831260ea53fdb`, with no divergence.
- [x] Verified exact-head `BinSync.Net` workflow run 3899 and `FLD Storage Integration` workflow run 1933 both completed successfully.
- [x] Re-read the v3 normative specification, roadmap, Change Staging/Retention, Metadata Storage Architecture, Metadata Store GC Design, Origin Provenance v3 Design and FLD-DB Integration Contract.
- [x] Confirmed the 12-byte stream-row reduction, physical compaction correction, immutable Origin recovery, provider freeze/ACK-failure retry proof and measured metadata benchmark are already implemented; do not redo these.
- [x] Adopted the user's staged execution order: broad implementation pass marked C, then focused correctness/conformance T, measured optimization O, and release finalization F.
- [ ] Begin C pass at the remaining Phase 3–5 implementation gaps; keep ChangeId and tombstone/ghost GC disabled until their independent safe-frontier proofs are complete.


### 2026-11-08 — C pass: receipt retirement reservation guard

- [x] Historical audit finding: `RetireInterestedPeer` once deleted receipts/peer rows while fencing an old epoch. The PendingBatch reservation check was added, and §1.23.4 now supersedes eager receipt deletion: receipt rows remain until separate signed-frontier + host-reference GC.
- [x] Added a fail-closed guard for any pending outbound reservation belonging to the peer and a retired-or-older epoch. A completed replacement baseline no longer silently discards the old incarnation's in-flight send/retry context.
- [~] C implementation committed as `aab64ad`; targeted lost-ACK, reservation-retirement and crash-boundary tests are explicitly deferred to Phase T. Receipt GC remains conservative; ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — C pass: provider local-intent ownership boundary

- [x] Clarified the public `IDataProvider.ApplyRemoteBatch` contract: remote materialization may change the visible LWW projection but MUST NOT append, rewrite or acknowledge the independent unpublished local-change feed.
- [x] Inspected `StorageDataProvider.ApplyRemoteBatch`: its transaction updates current-state rows and the idempotency receipt only; it does not mutate LocalChange rows or local feed high/ACK watermarks.
- [~] C contract committed as `f0c2bd8`; same-field remote-wins, local-intent publication and restart tests remain Phase T. No second PendingLocal journal is introduced.


### 2026-11-08 — receipt GC design synchronized with C-pass guard

- [x] Updated `METADATA-STORE-GC-DESIGN.md` to make unresolved outbound PendingBatch reservations an explicit receipt/peer-epoch retirement blocker, matching the new StorageMetadataStore guard and v3 §0.11.
- [x] Clarified that a replacement epoch's completed initial-sync baseline cannot discard the old epoch's unresolved send/lost-ACK retry context.
- [~] Current branch head is `4aac39b`; exact-head CI remains pending for C-pass code commits `aab64ad` and `f0c2bd8`.


### 2026-11-08 — C pass: public session clean-cut audit

- [x] Confirmed the v3 public API contract explicitly states that `TcpSyncSession` was removed and the TCP listener now returns the generic `SyncSession`; no legacy public session wrapper is to be preserved.
- [x] Updated `docs/PUBLIC-API.md` normative reference from the historical v2.4.17 text to BIN-SYNC-SPECIFICATION.txt v3.0.0 clean-break contract.
- [x C] Legacy TCP-specific session/client entry-point source search is clean; current TCP samples use the canonical generic session/transport API. [~ T] Complete compile/reference/API audit in Phase T.
- [~] Current branch head at this log entry: `d501785`; exact-head CI remains pending for the C-pass code changes `aab64ad` and `f0c2bd8`.


### 2026-11-08 — roadmap checkpoint after API documentation pass

- [x] Reconciled the branch head after public API and GC design documentation updates: `0b4259f`.
- [x] Preserved the exact distinction between the last green CI head (`1dfb7d6`) and the newer C-pass code commits (`aab64ad`, `f0c2bd8`).
- [ ] Do not promote C markers to complete until Phase T verifies exact-head build, tests, Fuzz/Lab and Storage Integration.


### 2026-11-08 — Phase 5 C pass: strict provider v3 format

- [x] Verified exact checkpoint `70cda009949f6255469fa957a9e545f3ec97da26`: BinSync.Net workflow run 3914 succeeded (Core tests, Fuzz/Lab, public samples, coverage threshold); FLD Storage Integration run 1948 succeeded on Windows, macOS and Ubuntu. This checkpoint includes receipt reservation retirement guard `aab64ad` and provider local-intent contract `f0c2bd8`.
- [x] Added mandatory provider format marker `BSP3` at reserved key kind `0x25`. Fresh provider partitions commit it before any provider API writes; unknown marker fails closed; existing provider-owned rows without marker fail closed rather than being reinterpreted as v3.
- [x] Removed local-feed identity migration/synthesis. Every v3 LocalChange row must carry exactly one nonzero ChangeId; malformed/truncated/identity-less rows fail closed.
- [x] Added Storage Integration regression tests for marker creation, unmarked legacy-row rejection and v3 feed rows missing ChangeId.
- [~] C implementation and tests committed as `56d818c` and `ad236fd`; exact-head CI remains pending. Do not treat the test code as executed until CI reports.


### 2026-11-08 — Phase 5 C pass: remove duplicate Node ChangeId authority

- [x] Audited the v3 handshake: HELLO and HELLO_ACK both encode `SessionWire.ProtocolMajor=3`; decode rejects any other major. The `SessionCapabilities` enum has no optional immutable-Origin provenance or downgrade capability. Marked the clean-cut requirement complete.
- [x] On durable Metadata-Store-owned publication cutover, `Node.EnableMetadataStoreOwnedPublicationFence` disables Node-owned writes/receives. The coordinator migrates known legacy Node digests into the same Metadata ChangeLedger rows and clears the namespace Node ledger only after those writes are durable. `SeenChanges` remains intact because it is a distinct local WAL replay marker, not a second active ChangeId authority.
- [~] Cutover and stale-Node-checkpoint restart assertions must verify the ledger remains inert and is not mutated/replayed after the provider/Metadata Store authority fence; physical reclamation is deferred to the versioned dual-fingerprint migration.
- [~] Code/test commits: `d6b02b2`, `6725463`. Exact-head Core + Storage Integration CI pending.
- [x] Updated the normative v3 §0.7 provider storage paragraph and Data Provider conformance contract to specify `BSP3`, strict nonzero ChangeId suffix and fail-closed legacy handling; no read-time identity synthesis.


### 2026-11-08 — Phase 3/4/5 C pass: provider generation and safe retirement

- [x] Added optional `IProviderFullReplace` capability with durable Begin/Stage/Commit/Abort operations. Staged rows remain invisible; Commit validates SHA-256 and atomically switches active generation.
- [x] Bound a staged replacement to its captured base generation so a stale concurrent replacement cannot overwrite a newer active generation. Repeated Begin/Commit after a successful switch is idempotent.
- [x] Made digest verification streaming in bounded 1024-row scans; staging and abort pages are capped at 4096 entries. Abort is resumable/bounded. Old generations remain physically retained until a separately proven cross-store GC path exists.
- [x] Generation ids use canonical RFC-4122 network byte order in digest, active pointer, session manifest and generation-scoped keys.
- [x] Added provider integration coverage for invisible staging, atomic switch, digest mismatch, stale-base rejection, restart/resume, old-generation snapshot stability and retention. The verified checkpoint before the latest three commits passed 165/165 on all OS.
- [x] Tightened peer retirement: exact stale-epoch PendingBatch may finish only before the durable retired-through fence; a replacement snapshot can discharge it only when same-author-epoch baseline covers the absolute end cursor. Unknown epoch and author-epoch mismatch fail closed.
- [x] Added tests for exact stale reservation retry/ACK, insufficient baseline, authoritative baseline supersession and cross-author-epoch mismatch.
- [x] Verified backup branches exist for both BIN-SYNC and FLD-DB; no backup branch was overwritten.
- [~] Latest exact-head CI pending for `1ec26b8`, `25224f4`, `0ba5876`; prior exact green `8564f5e` was Core 662/662, Fuzz/Lab success, Storage Integration 165/165 on Ubuntu/Windows/macOS.
- [ ] Signed namespace roster, all-member OriginSequence frontier, forgotten deletion checkpoint and tombstone/ghost GC remain disabled until the separate negotiated protocol revision and adversarial proof are complete.


### 2026-11-08 — exact green checkpoint: streamed FULL_REPLACE and GC reservation proof

- [x] Re-ran exact-head CI on `2f75b266c7c2d720d87b106573182393d5d15ea1`: Core 662/662; Fuzz/Lab SUCCESS; Storage Integration 168/168 on Ubuntu, Windows and macOS.
- [x] Added `ProviderFullReplaceDigestBuilder`: callers stream canonical snapshot pages; count is appended after the entry stream, so total count need not be known before the first page. The convenience helper remains for small/materialized images.
- [x] Receiver verifies the full-replace digest with bounded 1024-row scans; staging/abort pages are capped at 4096. No full-image materialization during commit.
- [x] Added field-tombstone and record-tombstone preservation coverage; fixed the fixture HLC so the record tombstone actually dominates the field write.
- [x] Added author-epoch equality to replacement-baseline retirement proof; a higher cursor from a different author epoch cannot discharge an old reservation.
- [x] Updated v3 spec, Public API and Data Provider Conformance docs with canonical RFC-4122 generation encoding, digest byte order, bounded page limits, caller safety obligations and old-generation retention.
- [x C] Promoted canonical ProviderMutation encode/decode/validation/order and `ProviderFullReplaceDigestBuilder` into `BinSync.Core/ProviderMutationCodec.cs`; `StorageDataProvider` delegates to the shared Core codec. Added round-trip, malformed encoding and canonical ordering tests; exact-head CI passed 665/665.
- [ ] Implement signed namespace roster, member applied-origin frontiers, authority-signed retention checkpoints and FULL_REPLACE installed ACKs in a separately negotiated v3.1 extension. Until then ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — v3.1 signed roster / safe-GC design checkpoint

- [x] Added `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` defining the pinned Namespace Authority, signed full roster, per-member applied-origin vectors, authority-signed retention checkpoint, FULL_REPLACE installed ACK and separate ChangeId/tombstone/receipt/outbound GC horizons.
- [x] Defined Joining/Active/Retired member states, missing-frontier=0, durable retired-through fencing and mandatory FULL_REPLACE on rejoin.
- [x] Preserved compact metadata: one shared ChangeId row per operation plus O(members × authors) frontier state; no per-change/per-peer duplicate history and no PendingLocal journal.
- [x] Defined v3.1 capability negotiation and proposed frame IDs; v3.0 Membership gossip remains advisory and cannot authorize GC.
- [ ] No v3.1 code or GC enablement yet. ChangeId and tombstone/Ghost GC remain disabled until codec, signature, crash, multi-peer and resurrection tests pass.


### 2026-11-08 — v3.1 design documentation synchronized

- [x] Linked the proposed v3.1 roster/GC addendum from the v3.0 specification, Metadata Store GC design and Public API docs without changing v3.0 wire behavior.
- [x] Updated Data Provider Conformance status to the exact green implementation head `2f75b26`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS, coverage threshold passed, in-process/TCP/ScenarioHarness samples passed.
- [x] Kept ChangeId and tombstone/Ghost GC disabled; no v3.1 protocol code is implied by the design document.


### 2026-11-08 — shared ProviderMutation codec exact-head verification and Storage source audit

- [x] Reconciled branch HEAD after the interruption: `3b9db5bf97550036fce6b5b145a8717b806f6d72`. The intervening commits cover codec promotion, canonical byte-order/spec updates, codec tests, duplicate-validation removal and StorageDataProvider delegation.
- [x] Verified exact-head CI: Core 665/665; Fuzz/Lab 31/31; Storage Integration 168/168 on Windows, Ubuntu and macOS; all five public samples passed.
- [x] Durable Metadata Bench completed, including one million existing ChangeId rows: freeze 1,024 local operations 218.1 µs / 7.77 KB. This is slower than the earlier 196.9 µs / 7.77 KB measurement; no performance improvement is claimed.
- [x] Compared all twelve common vendored `external/Fld1.Storage` source files with FLD-DB `src/Fld1.Storage` at current branch heads. Five are byte-identical; `StorageDatabase`, `StorageFaultPoint`, `StorageSnapshotFile`, `Fld1.Storage.csproj`, `WalManager`, and `WalWriter` differ; `StorageDirectorySync.cs` exists only in the BIN-SYNC vendor snapshot. The vendor README pins `dcd4334`, but the files are not a byte-identical snapshot of that commit either. Do not blindly overwrite either repository.
- [x C] Reconciled Storage divergence by porting the vendor-side directory-entry durability and WAL partial-write/fsync fault injection into authoritative FLD-DB `src/Fld1.Storage`; preserved `Fld1.Storage.Tests` and `BinSync.Storage.Integration` friend assemblies. FLD exact-head CI passed 169/169 on Windows and all six test assemblies passed on Ubuntu/macOS as well.
- [x C] Refreshed all 13 vendored Storage source/project files from FLD-DB code commit `a3abab3e0197f98aac12e9fdafcbfa204d9fbd77`; verified byte-identical file-by-file. Updated vendor README source pin and preserved the no-independent-semantic-changes rule. BIN-SYNC exact-head CI passed Core 665/665, Fuzz/Lab 31/31 and Storage Integration 168/168 on Ubuntu/Windows/macOS.
- [ ] Update the vendor snapshot from one exact tested FLD-DB commit after differences are reconciled; update `external/Fld1.Storage/README.md` source pin in the same package.
- [ ] Implement the FLD Metadata Store/Data Provider adapter using the shared Core codec; complete remote-apply/local-intent/reopen/crash conformance.


### 2026-11-08 — vendored Storage snapshot synchronized and exact CI green

- [x] FLD-DB Storage durability work passed exact code/test CI at `fd9f48f5ef7ec181bcacf2891bc45d8c1c11b28c`: 169 tests on Windows; Ubuntu/macOS also passed; `Fld1.Storage.Tests` 37/37 on all three OS.
- [x] BIN-SYNC vendor snapshot `external/Fld1.Storage` now matches all 13 source/project files from FLD-DB source commit `a3abab3e0197f98aac12e9fdafcbfa204d9fbd77` byte-for-byte. Vendor README pin and test assembly visibility are aligned.
- [x] Exact BIN-SYNC head after vendor refresh: `7cfddd45dec342f916a08f77a9521c831f6f8af7`; Core 665/665, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS. All StorageArraySync and StorageArrayTcpSync samples passed.
- [ ] Proceed with v3.1 signed roster/frontier/checkpoint codecs and persistence; no ChangeId or tombstone/Ghost GC is enabled until all-member proof, replay fences, restart and adversarial tests are complete.

### 2026-11-08 — v3.1 implementation pass started (not complete)

- [x C] Added `IMetadataNamespaceGcStore` as a durable metadata-store contract for accepted namespace roster, per-member Origin frontiers, retention checkpoints, FULL_REPLACE installation ACKs, and protocol-only tombstone-GC proof. This is a new production API, not merely test scaffolding.
- [x C] Added `StorageNamespaceGcStore` backed by the opaque Storage engine. It stores roster certificates, current-roster pointers, signed member frontiers, signed retention checkpoints and FULL_REPLACE ACKs under a separate key prefix; it shares the Storage transaction gate and does not rewrite ChangeId identity rows.
- [x C] Added checks for roster generation rollback/fork, authority continuity, member state transitions, Joining→Active only after a durable installed ACK, per-member frontier monotonicity, retention checkpoint monotonicity/previous-digest chain, checkpoint overclaim, all-Active-member installation, and protocol tombstone proof.
- [x C] Added v3.1 frame IDs and `SyncSession` send/receive adapters for signed roster, Origin frontier, retention checkpoint and FULL_REPLACE installed ACK. The methods fail closed unless the relevant v3.1 capabilities were negotiated; frontier/checkpoint and installed-ACK paths also require roster capability.
- [~ T] Added Storage integration tests for restart persistence, frontier and checkpoint safety horizons, Joining-member activation fencing, rollback/fork and overclaim rejection.
- [~ T] Added SyncSession tests for signed v3.1 frame exchange and missing-capability rejection. First CI exposed two test expectation failures: the handshake automatically negotiates `IncrementalAliasMap`; production code compiled and the remaining 676 tests passed. Corrected the assertions in commit `43efda1`; exact-head CI for `43efda1` is pending.
- [~] The v3.1 C implementation now includes signed FULL_REPLACE BEGIN/READY/CHUNK/COMMIT/ABORT codecs, sender/receiver state machines, durable resume, signed installed ACK, roster/frontier/checkpoint storage and ChangeId forgotten-origin replay fencing. Remaining: wire the transfer to authority/member lifecycle and SyncSession orchestration, produce/publish frontiers and checkpoints, complete retired-origin closure, provide a real application-wide commit/GC lease, and finish cross-store live-reference scans. Physical receipt/ChangeId/tombstone GC is disabled.
- [ ] Review the new StorageNamespaceGcStore implementation against the exact Storage transaction/snapshot semantics and add fault-injection/crash-boundary tests before treating persistence as production-ready.
- [ ] Update v3.1 addendum status and wire/storage vectors after implementation and adversarial tests; do not claim v3.1 conformance from the current adapter tests alone.


### 2026-11-08 — workflow reset: C-pass first, T-pass after code sweep

- [x] Adopted the execution rule: `C` means implementation pass completed for the named code slice; it does not certify behavior. `T` is reserved for focused correctness/conformance work and exact-head verification.
- [x] Reconciled the commits after the previous roadmap refresh: `d58e2e5`, `1d6878d`, `eff6e6d`, `43efda1`, `640af0d` (with `c56d2e2` as the prior green checkpoint). The v3.1 work now has durable roster/frontier/checkpoint/installed-ACK storage, signed message codecs, capability-gated SyncSession send/receive adapters, and a typed receive-frame range extended through `FULL_REPLACE_ABORT`.
- [x] Fixed the two initial SyncSession test expectation failures in `43efda1`: `IncrementalAliasMap` is automatically negotiated and must be included in expected capabilities.
- [x] On `640af0d610c7a3ee20d001210360d555c1f60c1f`, the BinSync.Net workflow Build and benchmark build succeeded; Fuzz/Lab succeeded; the test step failed; public samples were skipped as a consequence. Coverage threshold step reported success but does not make the failed test job green.
- [x C] Code slice: accept registered v3.1 GC frame IDs in `ReceiveExpectedExclusiveAsync`; the upper bound is now `NamespaceGcFrames.FullReplaceAbort` rather than hard-coded v3.0 frame `0x13`.
- [~ T] Exact-head test failure on `640af0d` is not yet diagnosed. Retrieve the test failure details / TRX before changing production behavior. FLD Storage Integration run `42498381271` was still in progress when this roadmap entry was written.
- [x C] FULL_REPLACE BEGIN/CHUNK/COMMIT/ABORT transfer state machine is implemented by `StorageFullReplaceReceiver`: signed roster/authority validation, durable bounded staging, contiguous page/index and frame-sequence enforcement, restart progress, ABORT-vs-CHUNK/COMMIT arbitration, durable bounded abort, digest-checked provider switch, composed host commit/reference scan, HLC-floor advancement and signed installed ACK. The receiver keeps the switch separate from GC.
- [x C] Namespace roster acceptance/lifecycle validation, signed OFR2 member reports (including per-epoch issued watermarks), authority retention checkpoints, all-Active-member minimum safe frontier, retired-origin closure, FULL_REPLACE-bound HLC floor and checkpoint replay fence are implemented in `StorageNamespaceGcStore`, `OriginFrontierProtocol` and `StorageSyncCoordinator`.
- [x C] Coordinator APIs publish/send/receive accepted rosters, member frontiers and retention checkpoints under the composed application commit guard. `StorageMetadataStore` consults `IsOriginCoveredByRetentionCheckpoint` at incoming/apply boundaries. Host reference scanner contract is enforced for FULL_REPLACE/frontier publication and physical GC proof.
- [x C] The concrete FLD Database host adapter/scanner is now implemented in the optional `samples/FldBinSyncHost` project; the coordinator/store primitives are bound to actual FLD APIs. [~ T] Build and conformance remain.
- [ ] Do not enable ChangeId or tombstone/ghost GC during the C pass. Their separate safe-horizon proofs and fail-closed behavior remain mandatory.
- [ ] After all production code slices have C markers, run the dedicated T pass: targeted tests, crash/fault injection, multi-peer/rejoin/resurrection, fuzz/resource bounds, samples, benchmarks and exact-head CI.


### 2026-11-08 — v3.1 FULL_REPLACE C-pass: wire envelopes and session transport

- [x C] Added canonical bounded codecs for FULL_REPLACE_BEGIN, FULL_REPLACE_CHUNK, FULL_REPLACE_COMMIT and FULL_REPLACE_ABORT in `FullReplaceTransferProtocol.cs`. BEGIN binds namespace, generation, accepted-roster generation/digest, provider feed watermark, expected entry count and digest. CHUNK carries a bounded page (≤4096 mutations, ≤16 MiB) with strict length/trailing-byte validation and canonical ProviderMutation decoding. Control frames carry only namespace + generation.
- [x C] Added capability-gated `SyncSession` send/receive adapters for the four transfer frames on a dedicated per-namespace transfer stream. Receiving frames returns validated envelopes only; it does NOT stage or commit provider state.
- [x C] Corrected all established-session receive type guards to admit registered v3.1 frame IDs through `FULL_REPLACE_ABORT`. The earlier 640af0d change had widened only `ReceiveExpectedExclusiveAsync`; `ReceivePacketAsync` and the single-reader `ReceivePumpAsync` still rejected frame IDs above 0x13. This was a real dispatcher defect and is now fixed.
- [~ T] No focused codec/transport tests or exact-head build have yet validated this package. The previous exact run on 640af0d built successfully but failed during the test step; do not infer that this package is green.
- [x C] Connected received BEGIN/CHUNK/COMMIT/ABORT to roster/authority validation, bounded durable provider staging, digest verification, restart progress, durable abort/recovery and installed-ACK publication. The receiver rejects wrong generation/namespace, non-contiguous chunk index, non-canonical page size, wrong frame sequence, stale roster and committed-stage mutation before provider switch.
- [~ T] Add adversarial tests for duplicate/interleaved frames, gaps/overlaps, concurrent ABORT/CHUNK, process restart between every frame, digest mismatch and stale generations. Existing exact-head tests are still red on fixture migrations.
- [ ] Keep FULL_REPLACE commit separate from GC: successful switch alone never authorizes old-generation deletion, ChangeId GC or tombstone/ghost GC.


### 2026-11-08 — dispatcher failure diagnosed and corrected

- [x] Retrieved the failed test job log for run `42498381290` (commit `640af0d`). The sole failure was `NamespaceGcSessionTests.Negotiated_v31_session_exchanges_signed_roster_frontier_checkpoint_and_install_ack`: `ReceivePacketAsync` still rejected established-session frame IDs above `0x13`.
- [x] Confirmed the earlier frame-range change had widened `ReceiveExpectedExclusiveAsync` only. Updated both `ReceivePacketAsync` and `ReceivePumpAsync` to use `NamespaceGcFrames.FullReplaceAbort` as the upper bound. The correction is committed as `e012460a6e3722979b4f136b6237091732ded692`.
- [~ T] The failing test has not yet been rerun on the corrected head. Keep the test gate open; no green status is claimed.


### 2026-11-08 — FULL_REPLACE manifest authenticity and ordered receive C-pass

- [x C] BEGIN now requires an Ed25519 authority signature over a domain-separated canonical manifest binding namespace, generation, roster generation/digest, source feed watermark, expected entry count and digest. Unsigned BEGIN payloads are rejected by the encoder/decoder path.
- [x C] Receiver verifies the manifest signature against the pinned, non-revoked PreShared Namespace Authority and independently verifies the supplied roster certificate before accepting BEGIN.
- [x C] FULL_REPLACE receive APIs now require an explicit expected frame sequence; stream identity and exact frame sequence are checked before payload use. This avoids silently defaulting every later transfer frame to sequence 1.
- [x C] Capped CHUNK bytes to `Frames.MAX_PAYLOAD_SIZE` (rather than the full 16 MiB frame size), leaving room for the frame header.
- [~ T] These new codec/authentication/sequence paths have not yet been covered by a fresh exact-head CI run. Do not mark transfer conformance complete.
- [ ] Next C slice: durable receiver orchestration over `IProviderFullReplace`, persisted staging progress/resume, authority/member transport binding, duplicate/replay handling, atomic commit guard, signed installed-ACK persistence and Node cache rehydration. Keep the old generation and all tombstones until the independent GC proof is complete.


### 2026-11-08 — FULL_REPLACE end-to-end C-pass implementation slice

- [x C] Added `FULL_REPLACE_READY` (0x1C) and extended all session dispatch guards through that frame. READY returns the durable staged cursor, manifest count/digest, committed state and next transfer-frame sequence.
- [x C] Added a canonical READY codec with strict fixed length, identity, digest, range, committed-state and page-boundary checks. Resume uses fixed 4096-entry pages (last page may be shorter), making the next frame sequence derivable and deterministic.
- [x C] Extended `IProviderFullReplace` with an optional durable progress query and implemented it in `StorageDataProvider`, reading the persisted staging cursor after reopen.
- [x C] Added `StorageFullReplaceSender`: stable provider snapshot, first bounded digest pass, signed authority manifest, READY/resume negotiation, bounded second pass, COMMIT and signed installed-ACK verification/persistence.
- [x C] Added `StorageFullReplaceReceiver`: authority peer/epoch and roster verification, local member signing-key preflight before any switch, durable BEGIN/resume, strict contiguous page staging, concurrent ABORT detection, guarded atomic provider commit, Node-cache refresh callback, and durable local installed-ACK persistence. The caller must provide an exclusive commit lease that proves no PendingApply, relay/outbound reservation, provider intent or reader pin can be invalidated.
- [x C] Added signed ACK re-encoding so the authority can persist the exact verified member signature received over the session.
- [~ T] None of these new FULL_REPLACE sender/receiver, READY, resume, ABORT, signature or commit-guard paths has yet been run through exact-head CI. The earlier 640af0d failure was diagnosed and the dispatcher guards fixed, but the current C-pass package remains unverified.
- [ ] Remaining C work: connect sender/receiver to the actual namespace authority/member lifecycle and `StorageSyncCoordinator` public orchestration; implement periodic origin-frontier publication/checkpoint generation, forgotten-origin replay fencing and a concrete cross-store live-reference GC scan. ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — FULL_REPLACE READY/resume and receiver hardening

- [x C] Registered `FULL_REPLACE_READY` as frame `0x1C`; all three established-session dispatch guards now accept it. Updated the v3.1 addendum frame registry.
- [x C] Receiver and sender now negotiate durable resume state. Fixed-size 4096-entry pages (except final) make `NextIndex` and the next frame sequence deterministic; receiver rejects misaligned progress, short non-final pages and conflicting same-sequence CHUNK/ABORT or COMMIT/ABORT frames.
- [x C] `StorageFullReplaceReceiver` verifies the roster is still current under the mandatory commit lease before switching the active provider generation. The receiver preflights its local signing key against the roster before staging/commit, and persists its signed installed ACK only after provider switch and Node-cache refresh.
- [x C] `StorageFullReplaceSender` verifies the authority seed against the pinned roster before sending and checks the second snapshot pass still exactly matches the signed entry count.
- [x C] Optimized provider progress reads to use the single-row Storage lookup rather than cloning a full Storage snapshot on every resume/page check.
- [~ T] No exact-head build or tests have run for this implementation slice. Build/test correctness remains explicitly unproven.
- [x C] Provider snapshot memory path is now bounded: `StorageDatabase.OpenSnapshot` copies the sorted key index but shares immutable stored value arrays; `StorageSnapshot` prefix cursors seek by lower-bound; `StorageDataProvider.ProviderSnapshotState` merges field/tombstone cursors using bounded 1,024-row pages and decodes one mutation at a time. Snapshot payload memory is no longer duplicated wholesale; the remaining index is O(number of keys). [~ T/O] Measure peak memory/allocations and page throughput on large snapshots; do not claim a benchmark win before measurement.
- [ ] Remaining C work: connect the receiver's mandatory commit lease to concrete StorageSyncCoordinator/MetadataStore/peer-reservation checks; wire authority/member lifecycle and periodic frontier/checkpoint production; implement forgotten-origin replay fence and cross-store live-reference GC scan. ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — C-pass: origin identity persistence and first safe GC implementations

- [x C] Extended `ProviderMutation` and its canonical codec to persist `ChangeId`, `OriginAuthorEpoch` and `OriginSequence` alongside current field/record tombstone rows and FULL_REPLACE snapshots. The canonical 81-byte-header encoding is required; the prior 53-byte layout is rejected with no fallback decoder.
- [x C] Local provider feed appends now allocate a stable ChangeId, persist the author epoch and provider-assigned monotonic origin sequence into both the current mutation and feed entry, and verify the duplicated feed identity on read. Outbound freeze rejects missing/mismatched source provenance.
- [x C] Both direct Batch and durable PendingApply paths now carry exact per-operation ChangeId/origin tuple into provider mutations. Provider apply fingerprints include the tuple; legacy apply receipts remain replayable using the prior fingerprint encoding.
- [x C] Receipt rows now persist the highest immutable origin sequence in each author-homogeneous batch. Batch validation binds operation origin epoch to the wire issuer epoch and requires strictly increasing origin sequence. Older receipt encodings decode with origin sequence zero and are retained fail-closed.
- [x C] Added same-partition namespace-GC replay fencing to `StorageMetadataStore`. When a ChangeId row is absent but its origin is below the signed retention frontier, the incoming operation is rejected instead of being applied again. A stored receipt with a missing ChangeId row is also rejected unless the signed checkpoint proves the origin was forgotten.
- [x C] Added `CompactReceipts`: receipts can be deleted only when the signed safe frontier covers their persisted maximum origin sequence and no PendingApply references them. Receive cursors and sender digest chains remain durable as the transport replay fence.
- [x C] Added `CompactChangeLedger`: ChangeId rows are reclaimed only below the signed origin frontier, with in-store relay/outbound/pending-batch references checked and a mandatory external lease for provider/outbox/reader references. The signed retention checkpoint is the durable forgotten-origin fence.
- [x C] Added `StorageDataProvider.CompactRecordTombstones`: requires the active provider generation to equal the checkpoint's FULL_REPLACE generation, all Active members to have installed that exact digest, and the tombstone origin to be below the safe frontier. It deletes hidden older fields in bounded 4096-row batches while retaining the tombstone until all hidden fields are gone; crash/retry is safe. It also blocks on provider snapshots, unacknowledged local feed and uncommitted FULL_REPLACE staging.
- [x C] Exposed attachment and compaction entry points through `StorageSyncCoordinator`; GC fails closed until the same-partition GC store/trust are attached and the caller supplies the cross-store lease.
- [~ T] No exact-head build, unit tests, crash injection or GC proof tests have run for these changes. The C markers describe implemented code only.
- [ ] Remaining C work: verify provider mutation identity propagation through every local Node/outbox/reconcile path; wire a concrete application commit/GC lease that serializes Node, MetadataStore, provider, pending apply and peer reservations; add a durable membership transition that prevents re-activation without a matching FULL_REPLACE ACK; connect frontier production/checkpoint signing to periodic peer exchange; review old metadata-row migration behavior. Keep physical GC disabled in production until the T pass validates the gates.


### 2026-11-08 — GC safety review correction: tombstone physical GC remains disabled

- [x C] The provider now persists ChangeId and origin epoch/sequence in current mutations and FULL_REPLACE snapshots, enabling a future tombstone proof to bind the actual tombstone operation.
- [x C] Implemented bounded, crash-repeatable tombstone cleanup plumbing, including deletion of older hidden fields before the tombstone marker.
- [x C] Re-reviewed the v3.1 proof and found it insufficient for safe physical tombstone deletion: a checkpoint covering only the tombstone author's sequence does not exclude a delayed, pre-snapshot operation from another author whose sequence is above the common frontier but whose HLC is older than the tombstone.
- [x C] Disabled `HasProtocolTombstoneGcProof` fail-closed. Therefore `CompactRecordTombstones` currently reclaims nothing, despite the cleanup implementation being present. Do not describe tombstone GC as operational.
- [ ] Required proof extension before enabling tombstone GC: signed per-origin author-issued high-watermarks, all Active members' applied-through values reaching those watermarks, a FULL_REPLACE-bound global HLC floor, and mandatory advancement of every installing Node's HLC before it becomes Active. Retired-origin closure must also be represented; otherwise tombstones from retired authors remain uncollectable.
- [ ] T pass must test delayed pre-snapshot writes from a second author, skewed HLCs, author restart/epoch change, joining/rejoining members, and interrupted bounded cleanup. Receipts and ChangeId GC remain separately guarded by their signed origin sequence horizons.


### 2026-11-08 — C-pass: provider origin metadata and receipt/ChangeId GC

- [x C] `ProviderMutation` now carries the stable `ChangeId` and immutable origin author epoch/sequence. The v2 FULL_REPLACE digest domain binds these fields; old 53-byte provider mutation rows remain decodable with zero provenance and are not eligible for tombstone collection.
- [x C] Local provider feed rows persist the same ChangeId twice (mutation plus feed identity) and validate equality on read. New local appends require a non-zero author epoch; provider assigns the monotonic origin sequence. Outbound freeze validates author, epoch, sequence and ChangeId before publishing.
- [x C] Remote direct-batch and PendingApply conversion paths now pass operation identity into provider mutations. Same-HLC migration preserves existing provenance and upgrades legacy rows only when authenticated new provenance is supplied. Legacy apply-receipt fingerprints remain accepted on exact replay.
- [x C] Receipt encoding now stores maximum origin sequence; new batches must have one author/epoch and strictly increasing origin sequence. Old receipt formats decode with zero origin sequence and are not collected.
- [x C] `CompactReceipts` and `CompactChangeLedger` are exposed through `StorageSyncCoordinator`; both require the same-partition signed-GC guard and an external lease. Receipt compaction retains receive cursors/digest chains. ChangeId compaction checks relay/outbound/pending references and retains the signed checkpoint as replay fence.
- [x C] Provider record-tombstone cleanup plumbing is bounded and crash-repeatable, but it is intentionally disabled (`IsTombstoneGcProofEnabled == false`). The current v3.1 sequence-only proof cannot rule out a delayed pre-snapshot operation from another author with an older HLC. Do not enable physical tombstone deletion.
- [~ T] No build or tests run; runtime and source-compatibility effects of requiring local provider `OriginAuthorEpoch` remain unverified.
- [x C] Global causal-horizon production code is present: per-author issued high-watermarks, every Active member's applied-through vector, signed retention checkpoint minimum, FULL_REPLACE-bound HLC floor, mandatory Node HLC advancement after install, and final retired-origin closure. Reports/checkpoints are produced and exchanged through `StorageSyncCoordinator` under `AcquireApplicationCommitGuard`.
- [x C] Incoming operation admission checks the signed retention checkpoint replay fence; `StorageMetadataStore` uses `IsOriginCoveredByRetentionCheckpoint` to reject forgotten-origin replay. Namespace GC proof requires every Active member's report and the durable closure of retired origins.
- [~ T] The new protocol invariants have not passed their full test suite: five OFR1/RCP1 tests currently construct stale reports without required issued-origin vectors. Migrate fixtures and add adversarial restart/rejoin/replay tests before treating this protocol as validated.


### 2026-11-08 — Canonical v3 encoding correction

- [x C] Removed the newly introduced legacy decode/fingerprint branches. Provider mutations now accept only the canonical 81-byte fixed header; local feed envelopes accept only that mutation encoding plus one 16-byte ChangeId; receipt values accept only the canonical origin-sequence-bearing lengths. No silent v2/v3 migration decoder is added.
- [x C] Record tombstones now require complete ChangeId/origin provenance at codec validation. Zero provenance remains allowed only for ordinary field materialization from Node LWW state, which is not an immutable operation/tombstone.
- [x C] Kept the explicit same-HLC provenance enrichment path for field materialization: a later authenticated immutable operation may attach its identity to an otherwise identical materialized field, without changing visible data. This is not a storage-format fallback.
- [~ T] Existing persisted rows using earlier in-development layouts will be rejected by the canonical v3.1 decoder; tests and any deliberate data-reset/rebuild procedure must account for this. No compatibility migration is hidden in the code.


### 2026-11-08 — receipt GC replay-fence strengthening

- [x C] `CompactReceipts` now additionally requires the durable per-author receive cursor to cover the receipt's `CursorEnd` and the sender digest-chain row to remain present. Receipt deletion therefore cannot remove the only transport replay fence; old cursor continuations fail even after the receipt is gone.
- [~ T] Exact-head tests for receipt deletion, duplicate retry after lost ACK, cursor continuity, digest-chain preservation and PendingApply races remain queued for the T pass.


### 2026-11-08 — C-pass: bounded provider snapshot enumeration

- [x C] Replaced the second full `StorageEntry[]` materialization in `StorageDataProvider.OpenSnapshot` with a pinned `StorageSnapshot` and two sorted 4096-row keyset cursors (fields + record tombstones). `ReadSnapshot` streams merged canonical order, supports sequential pages and absolute-offset reset/skip, and disposes the pinned snapshot on release/provider shutdown.
- [x C] `StorageDatabase.OpenSnapshot` no longer clones every value array: it shares immutable values and caches one sorted key/reference index per commit epoch. Repeated snapshots in an unchanged epoch reuse that index in O(1). [~ C] The first snapshot after a write still builds an O(number-of-keys) index; a persistent immutable root/MVCC tree would be required for O(page) total snapshot memory.
- [~ T] Exact paging boundary, random/backward seek, release/dispose, snapshot stability under concurrent writes, and fullreplace two-pass digest consistency remain untested on current HEAD.


### 2026-11-08 — C-pass: validate streamed snapshot rows

- [x C] The streaming provider snapshot cursor now validates every decoded row against its active-generation key, record id, field id and row kind before returning it. Corrupt/mixed-generation rows fail closed instead of entering the FULL_REPLACE digest stream.
- [~ T] Add malformed-key/value, mixed-generation, cursor disposal, exact page-boundary and two-pass snapshot digest tests in the later T pass.


### 2026-11-08 — C-pass build repair from exact-head CI

- [x C] Inspected exact failing Core/Fuzz and Storage Integration job logs for pre-repair HEAD `ebb33a3ef4380cb6b289e21fb8603bee1abcefcb`. All five jobs failed at Build (tests never started) with the same four compiler errors: missing `ProviderSnapshotState.LocalHighWatermark`; removed `CompareSnapshotEntries`; nullable `ReceiveState?` incorrectly used as a non-nullable value-type out parameter and then dereferenced.
- [x C] Restored the snapshot watermark on the pinned snapshot state because `ReadLocalChanges` uses it as the source-snapshot barrier; restored the canonical storage-key comparator used by FULL_REPLACE generation digest verification; corrected the value-type `TryGetValue` out variable.
- [~ T] These are compile-only repairs. No tests have run on the repaired HEAD yet. Next: continue C implementation, then build/test the complete C-pass on exact HEAD; do not count prior green checkpoint as current validation.
- **Pre-repair CI:** Core run `42502401957`, Storage Integration run `42502401982`; both failed at Build on Linux/macOS/Windows before their test steps.
- **Current branch HEAD at this roadmap update:** `561a51b7cdbf1c3207e975b22c56ec8c94281357` (roadmap commit follows).


### 2026-11-08 — C-pass: applied-origin and retention checkpoint production

- [x C] Added durable member frontier production. OFR2 applied entries derive only from immutable ChangeLedger identities already committed by the Metadata Store; unpublished provider feed drafts are reported separately as a count/max-HLC barrier and never treated as applied origins. Reports are signed by the Active member and persisted through the accepted roster guard. A previous report is retained as the baseline after safe compaction, and each author/epoch advances only through a contiguous sequence prefix.
- [x C] Added authority checkpoint production from durable member reports. Missing Active-member reports conservatively produce no safe frontier for the affected author; optional FULL_REPLACE binding is accepted only when all Active members' installed ACKs are already durable.
- [~ T] Add exact tests for gaps, duplicate/replayed feed rows, post-ACK frontier continuity, report rollback/fork, missing Active member, roster rollover, checkpoint digest chain, and FULL_REPLACE ACK gating. Exact-head CI for this slice is pending.


### 2026-11-08 — C-pass: enforce post-FULL_REPLACE HLC floor before installed ACK

- [x C] Added `Node.AdvanceClockToFloor`, a local-history restore operation that advances HLC without applying remote clock-skew rejection.
- [x C] Added `StorageSyncCoordinator.RefreshAfterFullReplace`: advance to the committed provider snapshot's maximum HLC floor, rehydrate the Node cache from the new generation, replay durable PendingApply intents, and persist Node state. This is the intended callback for `StorageFullReplaceReceiver`; the receiver invokes it before signing/persisting the installed ACK.
- [~ T] Verify callback wiring, interrupted refresh/retry, empty snapshot, HLC tie/logical floor, PendingApply replay and ACK ordering. Exact-head CI for this slice is pending.


### 2026-11-08 — C-pass: eliminate OriginSequence holes caused by pending-write coalescing

- [x C] Found a correctness gap: assigning OriginSequence from the provider feed cursor caused coalesced-away pre-publication edits to consume origin numbers, permanently blocking contiguous applied-origin frontiers. Replaced it with freeze-time allocation.
- [x C] Local provider drafts now carry stable ChangeId + author epoch and OriginSequence=0. Metadata Store allocates OriginSequence atomically per namespace/author/epoch in durable key-kind 22, independently of outbound transport sequence and provider feed sequence. Coalesced-away drafts consume no origin sequence.
- [x C] After immutable freeze, coordinator stamps the assigned origin onto the matching current provider winner before feed ACK. Stamp is idempotent and checks the durable feed row; failed stamp/ACK retries recover the same identity from the ChangeId ledger. Remote apply still requires a complete non-zero origin tuple; FULL_REPLACE state images may carry draft identity and later upgrade it without erasing an existing published origin.
- [x C] Member applied-origin frontiers now derive only from frozen ChangeId ledger rows; unpub drafts are not reported as applied/published origins.
- [~ T] Add tests for coalesced writes, epoch rollover, per-epoch allocator restart, crash after freeze/before stamp, crash after stamp/before feed ACK, draft FULL_REPLACE followed by published delta upgrade, and strict remote partial-origin rejection. Current exact-head CI pending.


### 2026-11-08 — C-pass: durable freeze-time origin assignment and provider stamp

- [x C] Added `IProviderPublishedOriginStore` and implemented it in `StorageDataProvider`. The coordinator pairs each coalesced winning feed entry with the exact immutable Metadata Store item, validates the still-present durable feed row, stamps the assigned origin onto the matching current-state winner, then persists the Node cache and only afterward ACKs the feed.
- [x C] A failed provider stamp or feed ACK leaves the feed replayable. The Metadata Store ChangeId ledger returns the same previously frozen OriginSequence; provider stamping is idempotent. A newer current-state winner is never overwritten by an older feed entry.
- [x C] FULL_REPLACE state images may carry a draft `(ChangeId, AuthorEpoch, OriginSequence=0)`. If its immutable DELTA arrives later at the same HLC, provider apply upgrades the exact same ChangeId/epoch draft to the assigned non-zero OriginSequence; replaying a draft cannot erase a published origin.
- [x C] Scoped the missing allocator-row fail-closed check by namespace, since one Metadata Store partition can contain multiple namespaces.
- [~ T] Add fault-injection around metadata freeze → provider stamp → Node checkpoint → feed ACK, plus concurrent newer-edit and FULL_REPLACE-draft upgrade cases. Exact-head CI is pending.


### 2026-11-08 — C-pass: signed HLC floor chain from receiver to retention checkpoint

- [x C] Upgraded the installed ACK to FRA2. Each Active member signs the maximum HLC in the committed provider generation after its mandatory Node HLC advance, provider rehydration, PendingApply replay and Node persistence.
- [x C] Upgraded the retention checkpoint to RCP2. The authority requires every Active member's durable ACK for the exact FULL_REPLACE generation/digest, rejects missing members or divergent floors, and binds the shared `(HlcWall,HlcLogical)` floor into the signed checkpoint payload.
- [x C] Added `StorageSyncCoordinator.CreateFullReplaceRefreshCallback(persistNode)` as the intended receiver callback factory. RCP1/FRA1 fallback decoding is not retained.
- [~ T] Add golden vectors and restart/tamper tests for FRA2/RCP2, floor mismatch, missing ACK, callback failure before ACK, and monotonic checkpoint chain. Exact-head CI pending.


### 2026-11-08 — C-pass: author-issued horizon, terminal retirement and commit barrier

- [x C] OFR2 reports the durable per-author-epoch issued sequence and maximum issued HLC, plus pending local draft count/max HLC. Frontiers continue to count only frozen ChangeLedger operations; drafts are a separate checkpoint barrier.
- [x C] Added terminal final-author reports. The coordinator holds its application commit guard, closes all durable local author epochs plus the supplied current epoch in the provider, rejects remaining feed drafts, and signs a terminal report. The store rejects later reports after final closure.
- [x C] NRS2 carries immutable retired-origin epoch closures. An Active member cannot be retired without a final signed OFR2 report; the authority must copy each epoch's final sequence and maximum HLC exactly. Existing closures survive member rejoin.
- [x C] RCP2 safe frontiers now advance only through each author's final issued high-watermark and only when every Active member reports application through that full range. A FULL_REPLACE-bound checkpoint requires zero unpublished drafts on every Active member and an HLC floor covering the maximum issued HLC across all epochs.
- [x C] Added `IProviderCommitGuard` and `StorageSyncCoordinator.AcquireApplicationCommitGuard` (coordinator→provider lock order). FULL_REPLACE commit refuses to switch while local feed drafts remain; the same lease can be supplied to GC entry points.
- [~ T] Exact-head CI is queued for this code slice. Golden vectors, report terminality, closure/rejoin, issued-HLC max, pending-draft race, all-member frontier and commit-guard fault tests remain in T.
- [~ C] Physical GC remains disabled pending the final cross-store live-reference scan, including provider state and host WAL/FLD references under the composed application commit lease.


### 2026-11-08 — C-pass: enforce terminal author identity and safe rejoin

- [x C] Provider now persists a terminal author-identity marker in addition to per-epoch closure markers. Once a final OFR2 report is published, all local appends are rejected, not merely appends using the last known epoch.
- [x C] Added explicit `StorageSyncCoordinator.ActivateLocalAuthorEpoch(newEpoch)`. It requires the local member to be Active in the accepted roster, clears the terminal marker, installs a fresh epoch, and never permits reuse of a previously closed epoch.
- [x C] The final-report path holds the ordered coordinator/provider commit guard while closing known epochs, checking that the feed is empty, closing the author identity and signing the final report.
- [~ T] Verify crash/restart after provider retirement but before report persistence, report persistence before roster retirement, rejoin with a fresh epoch, and rejection of writes on every old epoch.


### 2026-11-09 — C-pass: repair issued-origin state integration and compile blockers

- [x C] Reconciled the key-kind 22 durable allocator representation: the in-memory map now consistently stores `(OriginSequence, MaxHlcWall, MaxHlcLogical)` rather than mixing the new record with the old `ulong` value type. Freeze-time increments preserve all three fields and serialize the 20-byte value.
- [x C] Frontier projection now reads `OriginIssuedState.Sequence` and carries the persisted maximum HLC fields into OFR2.
- [x C] Fixed nullable roster handling in the explicit author-epoch reactivation path.
- [~ T] Exact-head CI must confirm compilation and then expose remaining behavioral/test failures. The last completed CI at `d8db149` failed at compile time on these type mismatches and nullable warnings; the repair commits are `ef75ea3` and `bcece5c`.
- [x C] Audited key-kind 22 call sites, durable 20-byte encoding/decoding, in-memory state, frontier projection and freeze-time retry path. Persistence/restart/fault proof remains in T-pass.


### 2026-11-09 — C-pass: widen application commit barrier to Node mutation gates

- [x C] `StorageSyncCoordinator.AcquireApplicationCommitGuard()` now acquires coordinator → Node `Storage→Outbox→Meta` gates → provider/shared-Storage transaction gate, and releases in reverse order. This closes the direct-Node-write race during FULL_REPLACE, author retirement and local GC when the provider/metadata share the Storage transaction gate.
- [x C] The receiver continues to require an explicit non-null commit lease before generation switch; the caller must still compose the host WAL/FLD transaction gate if it is distinct from the BIN-SYNC Storage database.
- [x C] Updated the Storage integration call sites for the new tombstone-HLC proof contract (compile/API alignment only; no new behavioral assertions were added in this C slice).
- [~ T] CI at `747ea0c` compiled production code and Fuzz/Lab passed, but Core tests had 7 failures in OFR1/RCP1 expectations and record-tombstone mutation validation. Storage Integration failed to compile on the changed tombstone-HLC signature; call sites are now aligned. Re-run exact-head CI and then migrate the obsolete v1 test fixtures to OFR2/RCP2 during T-pass.
- [x C] Concrete lifecycle/control-plane wiring now exists for accepted roster exchange, OFR2 publication/receipt, RCP2 publication/receipt, FRA2 ACK receipt/send and receiver HLC-refresh callbacks. Host peer scheduling/retry remains host-owned; all durable Metadata Store/Data Provider/GC adapters are required to share one StorageDatabase for the internal barrier. Physical GC stays disabled.


### 2026-11-09 — C-pass: shared-storage proof and lock-order correction

- [x C] `AcquireApplicationCommitGuard` now fails closed if the durable `StorageMetadataStore` and `StorageDataProvider` do not share the same `StorageDatabase`; the provider/metadata/GC transaction gate is therefore one concrete lock when these durable implementations are attached.
- [x C] Added internal shared-database identity checks without exposing storage handles through the public provider API.
- [x C] Audited `StorageNodePersistence.Save` against the new lock order and changed it to acquire Node `Storage→Outbox→Meta` gates before the shared Storage transaction gate. This prevents the inverse Storage→Node order from deadlocking against FULL_REPLACE/GC commit guards.
- [~ T] Verify parallel Node writes, Node checkpoint saves, provider appends, metadata commits and FULL_REPLACE commit under contention; add deterministic deadlock/race tests during T-pass.
- [x C] Snapshot values are shared immutably and the sorted index is cached per commit epoch; provider scans use borrowed single-entry/page cursors. [~ C] First snapshot after a write still materializes an O(number-of-keys) index. Physical GC remains disabled until host WAL/FLD and PendingApply/relay/reservation/snapshot references are proven under the composed lease.


### 2026-11-09 — C-pass: repair OFR2 decoder and align canonical documents

- [x C] Fixed OFR2 decoding: the parser now consumes and retains `FinalForAuthor`, `UnpublishedDraftCount`, `MaxUnpublishedDraftHlcWall` and `MaxUnpublishedDraftHlcLogical` before reading the applied-origin vector. Previously the encoder emitted these signed fields but the decoder skipped them, shifting the vector length and causing `OFR2 contains trailing payload bytes` on valid reports.
- [x C] Added canonical validation: final-author reports cannot contain unpublished drafts; an empty draft set must carry a zero maximum HLC.
- [x C] Updated v3.1 addendum wire layouts and domain strings from OFR1/NRS1 to OFR2/NRS2, including issued-origin entries and FRA2/RCP2 HLC-floor fields. Corrected the stale statement that author-issued/retired-origin closure remained open.
- [~ T] The seven Core failures at `747ea0c` included OFR1/RCP1 fixtures, OFR2 decode misalignment, and a record-tombstone fixture rejected by the stricter canonical mutation contract. The OFR2 parser defect is fixed in C; the legacy test fixtures/assertions remain for T-pass migration.
- [x C] Remaining lifecycle wiring was completed in the subsequent coordinator control-plane entries below. [~ C] Storage snapshot-memory audit remains open; retain all physical GC feature flags as false.


### 2026-11-09 — C-pass: durable v3.1 control-plane orchestration

- [x C] Added `StorageSyncCoordinator` orchestration over negotiated `SyncSession` frames: send/receive accepted namespace rosters, publish-and-send OFR2, receive-and-persist member frontiers, publish-and-send RCP2, receive-and-persist checkpoints, and receive-and-persist FULL_REPLACE installed ACKs.
- [x C] Incoming control messages are decoded/verified by the session against the accepted roster, then re-encoded with their original signature and passed through the durable `StorageNamespaceGcStore.Accept*` path. Roster changes during the await fail closed at the store's current-roster check.
- [x C] Outgoing OFR2/RCP2 objects are durably accepted before network send; a lost send can be retried with a newer monotonic report/checkpoint rather than losing the local proof state.
- [~ T] Validate authority/member role separation, capability negotiation, duplicate frames, stale roster during receive, failed send/retry and report/checkpoint monotonicity in the T-pass.


### 2026-11-09 — C-pass: installed-ACK send fence

- [x C] Added `SendDurableFullReplaceInstalledAckAsync`: it requires the ACK to belong to the local node/namespace, finds the exact ACK already durably accepted for that generation, compares transport epoch, roster generation, digest and HLC floor, verifies the member seed against the accepted roster, and only then sends it.
- [~ T] Exercise ACK lost-send/retry, wrong local member, stale roster, mismatched digest/floor and missing durable ACK cases.


### 2026-11-09 — C-pass: immutable retired-origin closure on rejoin

- [x C] OFR2 verification now compares every previously authority-closed epoch for the reporting member against the immutable NRS2 closure: epoch, final issued sequence and maximum HLC must all match, and a rejoined member cannot omit the old epoch from its report.
- [x C] Applied the check in both decode/verify and object verification paths, so both wire receive and local checkpoint construction enforce the same invariant.
- [~ T] Add tamper tests for omitted/changed old epochs after rejoin, unchanged closed epochs alongside a new epoch, and old-epoch attempts to advance issued sequence/HLC.


### 2026-11-09 — C-pass: provider retirement requires closing the active epoch

- [x C] `CloseLocalAuthorIdentity` now validates the persisted active-epoch marker and refuses terminal retirement unless that exact epoch already has a durable closure marker. It also rejects an impossible active marker on an already-retired identity.
- [x C] `ActivateLocalAuthorEpoch` refuses to proceed if the retired identity still carries an active-epoch marker, then enforces a monotonically fresh epoch above all prior closures.
- [~ T] Test wrong `finalAuthorEpoch`, missing closure marker, malformed active marker, crash/retry and valid rejoin.


### 2026-11-09 — C-pass: bound OFR2/RCP2 validation cost

- [x C] Replaced repeated linear lookups in OFR2 own-applied/issued epoch validation with one per-report epoch map; retired-epoch closure checks use an indexed issued-epoch map.
- [x C] RCP2 safe-frontier derivation now builds each member's `(author, epoch) → applied sequence` map once and checks every origin/member pair in O(1) average time instead of rescanning each report vector.
- [x C] This removes the avoidable O(origins × active-members × entries) scan from checkpoint creation while retaining bounded 4096-entry report limits and identical fail-closed conditions.
- [~ T] Add equivalence/property tests and benchmark 1/16/256/4096 entries × 1/2/64/4096 members; confirm allocations and CPU before accepting the optimization.


### 2026-11-09 — C-pass: make every frontier report a consistent application snapshot

- [x C] `PublishAppliedOriginFrontier` now always acquires the coordinator→Node→shared-Storage application commit barrier, not only for final-author reports. The ChangeLedger frontier and provider unpublished-draft count/max-HLC are therefore sampled against one local commit boundary.
- [x C] This closes the race where a local Node/provider write could arrive after a zero-draft scan but before a non-final OFR2 report was signed and later used by a FULL_REPLACE retention checkpoint.
- [~ T] Stress concurrent local writes, remote apply, publication freeze, provider ACK and frontier publication; prove the report is either before or after each operation, never a mixed snapshot.


### 2026-11-09 — C-pass: guard provider rehydration and publication cutover

- [x C] `RefreshAfterFullReplace`, `RehydrateNodeFromProviderSnapshot` and `EnableMetadataStoreOwnedPublication` now acquire the same coordinator→Node→shared-Storage barrier before reading provider snapshots, changing Node cache state or switching publication authority.
- [x C] Nested use from the receiver's already-held commit lease is reentrant on the acquiring thread; Node checkpoint persistence follows the same lock order.
- [~ T] Verify direct and nested invocation, concurrent Node writes/provider appends, failed persistence and retry, and deadlock freedom under contention.


### 2026-11-09 — C-pass: enforce shared storage at FULL_REPLACE receiver construction

- [x C] The durable `StorageFullReplaceReceiver` now rejects a concrete `StorageDataProvider` + `StorageNamespaceGcStore` pair that do not share the same `StorageDatabase`. This aligns receiver generation switching and installed-ACK persistence with the coordinator/metadata transaction gate.
- [~ T] Verify rejection for split databases and successful commit/ACK persistence when all three durable adapters share one database.


### 2026-11-09 — C-pass: reduce snapshot payload duplication

- [x C] `StorageDatabase.OpenSnapshot` now clones the sorted key index but shares the immutable stored value arrays. `Commit` copies caller-owned buffers on publication and later commits replace references rather than mutating arrays; snapshot reads still return owned copies through `TryGet`/bounded cursor pages.
- [x C] Repeated snapshots now share a cached immutable sorted index for the current commit epoch; successful commits invalidate the cache while old snapshots retain their previous index. [~ C] The first snapshot after each commit epoch still allocates O(number-of-keys) index storage; persistent immutable root/MVCC remains a future option, not a current O(page) claim.
- [~ T] Verify snapshot stability across replace/delete/reopen, concurrent commit and cursor paging; benchmark allocations/RSS with large values and 1M rows.


### 2026-11-09 — C-pass: prefix cursor seeks into sorted range

- [x C] `StorageSnapshot.PrefixCursor` now computes the exclusive lexicographic upper bound of a byte prefix and uses `SortedDictionary.GetViewBetween` to seek directly to the matching key range. With `afterKey`, it starts at that cursor position instead of scanning unrelated keys from the partition beginning.
- [x C] Empty/all-0xFF prefixes retain the safe fallback; the existing page/trailing-key checks remain in place.
- [~ T] Verify prefix boundaries (`00`, `FF`, carry across suffix bytes), exact `afterKey`, empty pages, disposal and concurrent commits; benchmark scan cost for 1M rows with sparse prefixes.

- [x C] Follow-up hardening: an `afterKey` at/above the prefix upper bound now retains a valid completed enumerator, so `ReadPage` returns an empty page rather than treating the cursor as disposed.

- [x C] Roster send helper now signs and verifies against the pinned authority trust entry before transmitting, rejecting a mismatched authority seed locally rather than sending a roster peers will reject.


### 2026-11-09 — C-pass: enforce monotonic HLC within an author epoch

- [x C] `StorageMetadataStore.FreezeOutbound` now rejects a new immutable operation whose `(HlcWall,HlcLogical)` is lower than the persisted max for that author epoch. The allocator no longer silently retains a higher max while accepting an older operation that could later resurrect data after tombstone reclamation.
- [x C] Equal HLC is allowed for same-transaction operations; the per-epoch max advances only when the new HLC is greater.
- [~ T] Test HLC wall regression, logical regression at equal wall, equal-HLC multi-field batch, restart persistence and concurrent freeze ordering.

- [x C] Extended the freeze-time HLC monotonicity fence across all epochs of the same namespace/author, not just within one epoch. A rejoined author cannot publish a lower HLC under its fresh epoch than the maximum issued HLC from a previously closed epoch.


### 2026-11-09 — C-pass: tombstone proof binds row HLC to issued epoch maximum

- [x C] `HasProtocolTombstoneGcProof` now rejects a candidate tombstone whose `(HlcWall,HlcLogical)` exceeds the signed maximum HLC for its exact author epoch, in addition to requiring the tombstone HLC and the entire issued-epoch maximum to be covered by the common FULL_REPLACE floor.
- [~ T] Add tamper tests for tombstone HLC above issued max, equal-wall logical overflow, below max, and retired versus active author closure.


### 2026-11-09 — C-pass: replace unsupported range view with binary-seek snapshot index

- [x C] Replaced the unsupported `SortedDictionary.GetViewBetween` attempt with a `SortedList` snapshot index and a bounded lower-bound binary search over its sorted keys. Prefix and `afterKey` cursors now seek in O(log n) and read only matching pages; the cursor does not allocate a view or enumerate unrelated rows.
- [x C] Snapshot payload arrays remain shared immutable references; each `StorageEntry` page returns owned key/value arrays. Empty/all-0xFF prefixes and afterKey beyond the range naturally return empty pages.
- [~ T] Exact-head build and prefix-boundary/stability tests required; the prior CI failure was a compile-time API mismatch, now removed.


### 2026-11-09 — C-pass: make host live-reference scans mandatory for physical GC

- [x C] Added `IApplicationGcReferenceScanner`, a host-owned contract for external ChangeId and record references in WAL/FLD outbox, pending application state and other durable stores. GC calls fail closed if the scanner is absent.
- [x C] `CompactChangeLedger` now checks both BIN-SYNC-owned live references and the host scanner before deleting a ChangeId ledger row.
- [x C] `StorageMetadataStore.HasLiveRecordReference` checks pending apply payloads, relay payloads, outbound payloads and conservatively blocks on any pending immutable peer reservation in the namespace. Malformed retained payloads count as live.
- [x C] `CompactRecordTombstones` now requires the durable Metadata Store and external scanner, verifies all three durable adapters share one StorageDatabase, and checks both internal and host record/ChangeId references under the supplied composed lease.
- [x C] The FLD reference scanner and host lease composition are implemented by the optional sample; current ownership is documented in §1.25. All physical GC feature flags remain false until T-pass proof exists.

- [x C] Threaded the required host scanner through the public `StorageSyncCoordinator` GC entry points; there is no coordinator wrapper that can silently invoke ChangeId or record-tombstone compaction without the scanner.

- [x C] Extended the mandatory host scanner to receipts as well: receipt GC now checks external `(namespace, sender, author epoch, transport epoch, batch id)` references in addition to durable PendingApply, retired transport fence, receive cursor/digest chain and signed origin frontier. All three physical GC entry points require the scanner.

- [x C] External scanner contract now receives `(scopeId, localReplica, namespaceId)` on every callback, so one host adapter can safely serve multiple BIN-SYNC partitions without cross-scope ambiguity.


### 2026-11-09 — C-pass: close the host WAL/FLD draft-horizon gap

- [x C] Added \`ApplicationDraftHorizon\` and extended \`IApplicationGcReferenceScanner\` with \`ReadUnpublishedLocalDraftHorizon(scopeId, localReplica, namespaceId)\`. The host must report all local operations not yet frozen into BIN-SYNC, including WAL commits beyond the Outbox materialization watermark and unacknowledged Outbox entries.
- [x C] OFR2 production now requires both the host scanner and host commit-lease factory. The signed draft barrier combines host WAL/FLD drafts with provider-feed drafts and carries the maximum HLC; source-local CommitSeq/JournalSeq/OutboxSeq are never promoted to OriginSequence.
- [x C] Final author retirement checks the host draft horizon before closing provider epochs or terminally retiring the author identity. A non-zero host draft count fails before any provider closure marker is written.
- [x C] Added \`AcquireApplicationCommitGuard(hostLeaseFactory)\` with lock order coordinator → all Node gates → host WAL/FLD gate → shared BIN-SYNC Storage transaction gate; release is reverse-order. This overload is required for FULL_REPLACE and physical GC when the host has separate durable state.
- [x C] Updated the FLD integration contract and v3.1 addendum with exact draft accounting, partition scoping, live-reference checks and lock composition.
- [x C] The optional host adapter now scans the immutable FLD reference index covering WAL/Outbox/ApplyJournal-derived references and supplies the FLD lease to the BIN-SYNC scan contract. FLD remains protocol-neutral; the adapter lives outside both runtimes.
- [~ T] Previous exact CI at \`870f56b\` was red. Core failures include OFR1/RCP1 fixtures that do not provide OFR2 issued-origin vectors and a record-tombstone mutation fixture rejected by the canonical codec. Storage Integration has broad legacy fixture/API mismatches (local drafts still supply an assigned OriginSequence instead of deferred zero; immutable freeze fixtures contain non-contiguous OriginSequence; additional provenance fixtures need migration). A new run on \`236bfb1\` is in progress. Do not fix by weakening production validation; migrate tests in the dedicated T pass.
- [~ C] Snapshot sorted-index memory remains O(number of keys); physical receipt/ChangeId/tombstone GC flags remain false until the host adapter is built and the cross-store reference horizons are proven in T.


### 2026-11-09 — C-pass: FLD engine host-side scan primitives landed

- [x C] FLD-DB now exposes a same-thread \`FldDatabase.AcquireCommitGuard()\` and a neutral \`FldDraftHorizon\` scan over dirty local edits plus the unacknowledged Outbox suffix.
- [x C] FLD's receipt scanner matches durable ApplyJournal identities across sender peer, transport epoch, issuer author epoch, namespace and batch id. ChangeId/RecordId scanners inspect dirty state, pending Outbox and retained ApplyJournal field/relay payloads.
- [x C] Any unsealed WAL prefix blocks external ChangeId/RecordId/receipt reclamation conservatively until checkpoint/seal; a faulted database throws and cannot authorize GC.
- [x C] Added \`OutboxLog.VisitUnackedBatch\` borrowed-span iteration. FLD horizon/reference scans now inspect IDs and HLC without copying potentially large field-value payloads, using bounded 512-entry pages; ApplyJournal scans use 256-record pages.
- [x C] The optional `samples/FldBinSyncHost` adapter implements `IApplicationGcReferenceScanner` and returns an immutable `FldReferenceSnapshot`; the host composes `AcquireApplicationCommitGuard(hostLeaseFactory)` and owns the single FLD commit lease. Current scanner ownership supersedes the earlier nested-guard implementation; FLD remains protocol-neutral.
- [~ T] FLD exact-head CI is running for code head \`488150e\`; BIN-SYNC Core remains red on the previous exact code head due OFR1/RCP1 fixtures and one obsolete record-tombstone fixture. Fuzz/Lab passed. Do not weaken the production OFR2/ProviderMutation invariants to make legacy fixtures pass.
- [~ O] Zero-copy Outbox scans are expected to reduce temporary allocations, but benchmark acceptance is pending.


### 2026-11-09 — C-pass: make final author retirement epoch-exact

- [x C] \`IProviderAuthorEpochClosureStore.CloseLocalAuthorIdentity\` now requires the declared \`finalAuthorEpoch\`.
- [x C] StorageDataProvider verifies that the supplied epoch exactly matches its persisted active-epoch marker and has a durable per-epoch closure before terminal retirement.
- [x C] The provider persists the exact final retired epoch in a dedicated marker. A retry after crash must name that same epoch; an older closed epoch cannot be substituted to complete retirement.
- [~ T] Add wrong-final-epoch, crash-after-retirement-before-report-persist, retry, and rejoin tests. Exact-head CI for \`cd849db\` is queued.


### 2026-11-09 — C-pass: indexed host scan and tombstone proof snapshots

- [x C] Replaced per-candidate host scanner calls with \`IApplicationGcReferenceScanner.OpenScan(...)\` and one stable \`IApplicationGcReferenceScan\` per frontier/GC operation. This lets the host build indexes once under the composed WAL/FLD lease.
- [x C] \`StorageMetadataStore.CompactChangeLedger\` builds the live ChangeId set once from relay, outbound and PendingApply rows; any pending immutable peer reservation blocks the namespace once, rather than being rechecked for each candidate.
- [x C] Receipt compaction builds the pending-receipt identity set once and indexes roster members once.
- [x C] Record-tombstone compaction builds one fail-closed live RecordId set from pending apply/relay/outbound payloads; malformed retained payloads or pending reservations block the entire pass.
- [x C] Added \`TombstoneGcProofSnapshot\`: signed roster/checkpoint, all-active installed HLC floor, issued/retired author epochs, safe frontiers and every Active member's applied-through vector are loaded/indexed once per tombstone pass. Each tombstone proof check is O(1) average time.
- [x C] \`StorageDataProvider.CompactRecordTombstones\` now reuses the same signed proof snapshot and metadata/host reference snapshots across candidates; no repeated per-tombstone full frontier or payload scans.
- [~ T] Exact-head BIN-SYNC CI is queued for \`02dbe8e\`. The previous exact run at \`ed5d449\` had 6 Core failures (legacy OFR1/RCP1 fixtures and one old record-tombstone fixture) and Storage Integration failures dominated by old provider-draft/OriginSequence fixtures and old non-contiguous freeze inputs. Fuzz/Lab passed. Keep production validation strict; test migration is the T pass.
- [x T] Cross-repository FLD engine host primitives passed exact-head CI on Ubuntu/Windows/macOS at \`0db64f8d8f78f714fb197e0f375a536dcdec47d6\`.
- [~ O] Benchmark host scan snapshot allocations, tombstone proof snapshot build cost and per-candidate query cost before enabling any physical GC.


### 2026-11-09 — C-pass: FULL_REPLACE now observes host WAL/FLD state

- [x C] \`StorageFullReplaceReceiver\` now requires the exact scope and host reference scanner; under the composed coordinator→Node→host WAL/FLD→Storage lease it opens one host scan before generation switch.
- [x C] FULL_REPLACE refuses to commit if the host draft horizon is non-zero, including the conservative unsealed-WAL barrier, or if provider-local feed entries remain. This prevents a full replacement from overwriting unmaterialized local FLD/WAL edits.
- [x C] Integration contract and v3.1 addendum now require the same composed lease and host scan for FULL_REPLACE, OFR2, and all physical GC passes.
- [~ T] Exact-head CI for \`9e730d7\` is in progress; update receiver constructor fixtures during the dedicated T pass.


### 2026-11-09 — C-pass: final host and proof snapshot hardening

- [x C] \`StorageFullReplaceReceiver\` validates the host draft horizon shape (\`Count == 0\` requires zero HLC maximum) and refuses to switch when host drafts or provider feed entries remain.
- [x C] Tombstone proof snapshot now validates checkpoint digest presence/size before comparing; redundant installed-ACK rescans were removed because the common installed-HLC-floor scan already verifies every Active member's installed ACK, epoch, generation and digest.
- [x C] \`StorageMetadataStore\` now uses indexed ChangeId/receipt/RecordId sets and the host scan object; per-candidate protocol proof/frontier scans were removed from tombstone GC.
- [~ T] Exact-head CI is in progress for \`78a7080\`. The last completed BIN-SYNC test runs remain red only on known stale T fixtures: OFR1/RCP1 reports without issued-origin vectors, record-tombstone fixture violating canonical lifecycle metadata, local provider drafts still supplying assigned OriginSequence, and old freeze/remote batch fixtures violating immutable contiguous provenance. No production validation was relaxed.
- [x T] FLD engine reference-snapshot primitives passed exact-head CI on Ubuntu/Windows/macOS at \`0db64f8d8f78f714fb197e0f375a536dcdec47d6\`; later unsealed-WAL barrier changes are on \`e6bec094807f660e07b70874254a5925bc8b6deb\` and have CI pending.


### 2026-11-09 — C-pass: bounded provider snapshot path verified

- [x C] Confirmed the source snapshot implementation now retains a point-in-time Storage snapshot, uses seekable prefix cursors, and merges field/record-tombstone rows through bounded 1,024-entry pages. Immutable value arrays are shared by the Storage snapshot; only the sorted key index is copied. This closes the earlier stale roadmap item claiming a second full `StorageEntry[]` materialization.
- [~ T/O] Large-snapshot peak-memory, allocation and throughput measurements remain open; no performance improvement is claimed without benchmark evidence.

### 2026-11-09 — C-pass continuation: conformance adapter contract

- [x C] `VolatileMetadataStoreAdapter` now implements the combined frozen ChangeId identity lookup. It reads fingerprint and immutable Origin from one ledger entry, verifies local-outbound ownership and rejects incomplete identity; no production behavior or validation was weakened.
- [~ T] The adapter compile blocker reported at `1acdfa65979a` has a code fix committed; exact-head CI has not yet validated it. The 80 Storage Integration failures remain T fixtures/adapters (deferred OriginSequence drafts and `IProviderCommitGuard`).

### 2026-11-09 — C-pass continuation: crash-safe origin stamping and provider-feed GC references

- [x C] Provider publication no longer retains outbound payload objects solely to recover immutable origins. The coordinator resolves the frozen ChangeId identity (fingerprint + OperationOrigin) from one Metadata Store ledger lookup and stamps only the matching current provider winner before ACK.
- [x C] Recovery now handles the freeze-before-provider-stamp crash cut: after restart, a pending provider draft whose ChangeId is already frozen is verified against the durable fingerprint/origin ledger and stamped idempotently; conflicting fingerprints fail closed.
- [x C] Added a combined frozen ChangeId identity lookup and consolidated metadata reads to one ledger lookup per candidate; removed redundant standalone origin/fingerprint lookups.
- [x C] ChangeId GC live-reference scan now includes the local provider feed, so a ChangeId cannot be reclaimed while a local draft still references it. FLD integration contract and v3.1 addendum document the freeze-before-stamp recovery boundary and provider-feed GC fence.
- [x C] FULL_REPLACE now checks the host WAL/FLD draft horizon under the composed application commit barrier before generation switch. Host draft count/HLC shape is validated; provider local feed must also be empty.
- [x C] Host/GC proof work now builds indexed immutable scan snapshots, avoiding repeated full payload/roster/frontier scans per tombstone or ChangeId candidate. The physical GC feature gates remain disabled.
- [~ T] Exact-head `1acdfa65979a4338e9206c5617c3696c3fee4e75` is red. Core does not compile: `VolatileMetadataStoreAdapter` is missing `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeIdentity(...)`. Storage Integration has 80 failures, predominantly fixtures that still append provider drafts with assigned `OriginSequence` rather than zero/deferred origin, plus a few legacy providers that do not implement `IProviderCommitGuard`. Durable Metadata Bench passed; Metadata Retry Comparison failed. These are test adapter/fixture migrations; do not weaken production validation.
- [x C] Concrete BIN-SYNC↔FLD host adapter now supplies the FLD scan snapshot and commit lease through the optional sample. FLD provides neutral draft/reference primitives and remains protocol-neutral.
- [~ O] Measure the combined ledger lookup, indexed host scans and proof-snapshot build cost before accepting optimizations; do not infer improvement from code shape alone.

### 2026-11-09 — C-pass refresh: exact HEAD and concrete red test inventory

- [x C] Current branch HEAD is `29eea904621768175d1021f2de487d144c7d2d50`. Latest completed workflows on this exact HEAD: BinSync.Net and FLD Storage Integration both failed; Fuzz/Lab job passed.
- [~ T] Core test run: 678 total, 672 passed, 6 failed. Five OFR1/RCP1 tests use legacy member reports without the issued-watermark vector required for locally authored applied epochs; one provider mutation codec test builds a record tombstone without canonical lifecycle field/key-epoch-zero/no-value metadata. These are fixture migrations; keep production validation strict.
- [~ T] Storage Integration: 171 total, 91 passed, 80 failed. Failures are concentrated in old provider-draft fixtures supplying assigned OriginSequence instead of deferred zero, and test providers missing `IProviderCommitGuard`. Update fixtures/adapters to current provider contract; do not relax production guards.
- [x C] Fuzz/Lab passed on this exact head. This does not substitute for Core or Storage Integration success.
- [x C] Host integration now binds the coordinator resolver to the FLD relay API, publication gate, ACK and reference scanner. The two-runtime-assembly source extraction is complete. [~ T] Exact compile/test verification remains pending.
- [~ T] Exact-head CI is red; broad test migration remains deferred until C-pass closes. The known failures are now enumerated rather than represented as a generic red status.

### 2026-11-09 — C-pass refresh: FLD two-DLL scaffold

- [x C] FLD-DB now has a consolidated `Fld1.Database.csproj` linking Core/Run/WAL/Manifest/Engine source and referencing the independent generic `Fld1.Storage` project. The solution now exposes only these two runtime source projects; Core/Engine/etc. folders remain code-organization boundaries.
- [~ T] FLD exact-head CI on `a0ea90b9490cc1d93eafd048008327a5bcb4a54f` failed on all three OS jobs, but diagnostic log retrieval returned unavailable blobs. The consolidation has not been accepted as built until actionable errors are inspected and Ubuntu/macOS/Windows pass.
- [x C] The cross-repository host adapter now reads complete JournalSeq pages under the composed FLD commit guard, invokes the BIN-SYNC resolver, sends only immutable OutboxSeq batches under captured ceilings, translates safe ACK to FLD RelayJournalBatch ACK, prunes RPM3 rows afterward, and binds one FLD reference snapshot under the composed guard.
- [~ T] BIN-SYNC exact-head `29eea904621768175d1021f2de487d144c7d2d50` remains red: 6/678 Core tests and 80/171 Storage Integration tests fail for known stale fixture/provider-contract reasons; Fuzz/Lab passes. Do not relax production checks.

### 2026-11-09 — C-pass: reject replayed JournalSeq at resolver boundary

- [x C] `AppendRelayProvenancePage` now rejects a first source JournalSeq equal to or below the already committed scan cursor before allocating resolver arrays or touching the Node ledger. Subsequent entries may share a JournalSeq (one source batch with multiple operations) but may not decrease.
- [x C] This makes the coordinator's early validation match `StorageRelayProvenanceMap.AppendPage`'s strict page-boundary rule; the durable map remains the final atomic invariant check.
- [~ T] Add direct tests for first-entry-equals-cursor, decreasing JournalSeq, same-JournalSeq multi-operation page, and assert rejected pages do not mutate Node Outbox or map cursor.

### 2026-11-09 — C-pass: FLD source now physically consolidated

- [x C] FLD-DB commit `d63a8c49a889c28838305ce15aa4ca5e0b189201` physically moved all 40 Core/Engine/Manifest/Run/WAL source files under `src/Fld1.Database`; removed the five component project files. This replaces the earlier linked-source scaffold and obeys the no-shortcut/two-runtime-DLL decision.
- [x C] The supported solution surface is now `Fld1.Database` + `Fld1.Storage`; tests and benchmarks target Database, Storage-only tests/consumers target Storage.
- [~ T] The physical consolidation has not yet passed exact-head three-OS CI. Continue to treat the FLD project graph as unverified until build/test succeeds.
- [x C] The optional host adapter completes the source composition: complete-JournalSeq relay read, resolver/map append, ceiling-limited downstream publication, safe ACK translation, durable FLD ACK, RPM3 prune and reference-snapshot scanner lease. [~ T] Execution remains pending.

### 2026-11-09 — Recovery point after relay cursor hardening

- [x C] Created `backup/cpass-2026-11-09-after-relay-guards` at `7342dc7f8634d64b8911d0ab4ff93f4750127f97` before the next host-adapter/control-plane changes. Earlier `backup/pre-rpm3-compact-provenance-2026-11-09` remains intact.
- [x C] Relay resolver now rejects first JournalSeq at/below the durable scan cursor before array allocation; same-JournalSeq entries remain valid within a page.
- [~ T] Exact-head CI remains red on the previously observed 6 Core + 80 Storage Integration stale-fixture failures. Continue the C-pass without weakening validation; T-pass remains a separate gate.

### 2026-11-09 — C-pass status file synchronized to exact test evidence

- [x C] `docs/PRODUCTION-STATUS.md` now reflects current branch HEAD `57edd1f0032d24b17e51d265c16c3134472f3d66` and latest production-code commit `ea6968106e697f9d81ef94331cbf6dd986f71b29`.
- [~ T] Latest completed code-equivalent run on `7342dc7f8634d64b8911d0ab4ff93f4750127f97`: Core 672/678 (6 stale OFR1/RCP1 + tombstone fixtures), Storage Integration 91/171 (80 provenance/OriginSequence/commit-guard fixture failures), Fuzz/Lab passed. Exact current HEAD differs only by documentation since the tested code snapshot.
- [x C] FullReplace receiver, causal frontier/checkpoint production, retired-origin replay fence and concrete FLD Database host adapter/reference scanner source are implemented. [~ T] Host restart/race proof remains; physical GC stays disabled.

### 2026-11-09 — C-pass: complete FULL_REPLACE wire-vector sample

- [x C] Expanded `samples/V31ControlPlaneVectors` with exact canonical vectors and encode/decode round-trips for FULL_REPLACE_CHUNK, READY, COMMIT and ABORT in addition to signed BEGIN. The sample now covers all four transfer frame payload forms alongside NRS2/OFR2/RCP2/FRA2.
- [~ T] The sample has not yet been run on the current branch; validate the fixed bytes and signature vectors in the dedicated T pass and include it in CI sample execution.

### 2026-11-09 — exact-head CI triage and RPM3 namespace validator repair

- [x C] Current branch HEAD before this fix was `45160c1fbb07d9ac408c67ff49e857ffa5423015`. The latest completed Core and Storage Integration workflows both stopped at build, not tests.
- [x C] Root cause from both runner logs: `StorageRelayProvenanceMap.ReadAuthors` called missing `ValidateNamespaceId` (CS0103 at line 80). Added the private validator and reused it from `ValidateStream`; code fix `66ae3ac50833db9f4226f33490c780176bac0b14`.
- [~ T] Exact-head CI after `66ae3ac` has not yet completed. No test pass/fail counts are available for `45160c1f` because build failed before test execution.
- [~ T] The older 6/678 Core and 80/171 Storage Integration fixture failures remain historical evidence only; do not carry those counts forward as current-head results.
- [x C] The relay/ACK/GC host adapter contract is composed in `samples/FldBinSyncHost`, including all-author discovery, page drain, resolver, shared publication gate, safe ACK→FLD ACK→RPM3 prune, restart reconciliation and delegate reference scan. [~ C] The sample does not yet implement `IDataProvider` over `FldDatabase` or the full remote-apply bridge; transport/session scheduling and full Storage/API/byte audit remain.
- [~ C] FLD's two-project source boundary is physically consolidated as `Fld1.Database` + `Fld1.Storage`; generic Storage backend reuse and final binary-format/package compatibility audit are still open.
- [~ O] Benchmarks for snapshot/index build and end-to-end relay path remain pending. Physical GC remains disabled until integrated host scan plus proof tests establish safe horizons for receipts, ChangeId and tombstones.

### 2026-11-09 — T inventory refresh after RPM3 build repair

- [x C] The missing `ValidateNamespaceId` helper is fixed; exact-head run on the subsequent roadmap commit built successfully and entered tests.
- [~ T] Core: 678 total, 672 passed, 6 failed. Five failures are stale OFR1/RCP1 fixtures that do not supply the v3.1 issued-origin watermark vector for locally authored applied epochs; the sixth tombstone mutation fixture omits canonical lifecycle metadata (field/key epoch zero/no value).
- [~ T] Storage Integration: 171 total, 91 passed, 80 failed on Ubuntu, Windows and macOS. The common first failure is old provider drafts with assigned `OriginSequence` rather than deferred zero; another repeated failure is legacy test providers without `IProviderCommitGuard`. Update fixtures and adapters to the current contract; do not weaken runtime checks.
- [x T] Fuzz/Lab (§17) passed on the same code snapshot. Coverage was 68.1%, above the 50% gate; this does not offset Core/Storage failures.
- [x C] The two-DLL FLD source consolidation, BIN-SYNC relay gate/author recovery/ACK primitives, concrete host composition/sample and restart reconciliation are implemented. [~ C] Final generic Storage backend/format/API and metadata-cardinality audit remain.
- [~ O] Performance acceptance is still pending for bounded FLD scans, author cache rebuild/update, RPM3 mapping and the full relay publish/ACK path.

### 2026-11-09 — progress estimate and next C-pass boundary

Estimated completion (engineering assessment, not CI pass percentage):

- Protocol/core v3 and durable metadata invariants: **~90%**.
- RPM3 relay mapping, publication gate, all-author recovery and ACK primitives: **~85%**.
- FLD two-project split and durable scan primitives: **~80%** (physical project split done; generic backend integration and compatibility audit remain).
- End-to-end host composition: **~80%** (optional adapter source now binds FLD relay paging, startup reconciliation, publication ceilings, safe ACK and GC reference snapshots; concrete transport scheduler and compile/CI validation remain).
- T-pass/conformance/CI: **~45%** (Core 672/678; Storage Integration 91/171; Fuzz/Lab passed; coverage 68.1% on the last completed code-equivalent run).
- Benchmarks and final acceptance: **~25%**.

Overall estimate: **~78–82%**. Remaining C-pass is not a rewrite of BIN-SYNC v3: it is the last host transport/scheduler binding, generic Storage backend/API and format audit, plus a final public-surface review. Then switch to T-pass, followed by O/F. These are engineering estimates, not test-pass percentages. Physical GC remains disabled until safe horizons for receipts, ChangeId and tombstones are proven end-to-end.

### 2026-11-09 — concrete FLD host composition guide

- [x C] Added `docs/FLD-BINSYNC-HOST-COMPOSITION.md` with the concrete public API binding: shared BIN-SYNC Storage partition, FLD commit lease, all-author discovery, whole-JournalSeq page projection, RPM3 source mapping, startup recovery gate, safe ACK ordering and one immutable FLD reference snapshot.
- [x C] Audited the guide against the actual FLD public API (`ReadRelayBatchPage`, `ReadUnacknowledgedRelayAuthors`, `GetRelayJournalHighWatermark`, `AcknowledgeRelayBatches`, `AcquireCommitGuard`, `OpenReferenceSnapshot`) and BIN-SYNC gate/delegate signatures.
- [x C] Added optional `samples/FldBinSyncHost` relay/ACK/GC host adapter. It binds all-author discovery, whole-JournalSeq page projection, startup recovery, publication ceilings, FLD-safe-ACK-before-RPM3-prune and the immutable FLD reference scanner. It wraps but does not implement the actual `FldDatabase` remote APPLY or `IDataProvider` bridge. It is a host/sample assembly, not a third FLD runtime DLL.
- [x C] Both `BinSync.Storage` and `Fld1.Database` now accept the same `Fld1StorageProject` MSBuild property. The optional sample points both project references at the canonical FLD `Fld1.Storage.csproj`, avoiding duplicate Storage assembly/type identities.
- [x C] The adapter now supplies `PublishReadyOutboxAsync`; Core ceiling-aware sender overloads enforce local and relay sequence limits in both Node-owned and Metadata-Store-owned modes. [~ C] Session creation, multi-peer scheduling and routing every application sender through the gate remain host responsibilities; do not call legacy ceiling-free send overloads.
- [~ T] Compile the optional sample with sibling FLD-DB checkout (or explicit project paths), migrate host callback call sites and add crash-cut/reopen tests for startup reconciliation, multi-author page drain, FLD-ACK-before-RPM3-prune and scanner disposal/lock ordering.


## 0.84. C-pass: allocation-free deterministic relay-author ordering — 2026-11-09

- [x C] `StorageRelayPublicationGate.DiscoverAuthors` now preserves the exact ordinal order of `NodeId.ToString()` by comparing the two 64-bit identity limbs in reversed-endian order. This avoids allocating a 16-byte array and 32-character hex string for each comparison in the O(A log A) author sort.
- [x C] FLD `FldDatabase.ReadUnacknowledgedRelayAuthors` now sorts by its already-materialized canonical hex dictionary key instead of recomputing a hex string per author; code commit `bf72f0d8c4e82303612f079e3aefa790d6794451` and FLD roadmap §7.74.
- [~ T] Verify comparator equivalence against the canonical hex order for zero/high-bit/byte-boundary identities and verify FLD discovery order/identity on reopen.
- [~ O] Measure startup/recovery discovery CPU and allocations across representative author counts; these source-level allocation reductions are not yet benchmark claims.

BIN-SYNC code commit: `9d187f6cbe6c496ec013d1d39c969f8820ed1dab`. Current C-pass remains unvalidated; no CI success is claimed.


## 0.85. C-pass: enforce per-author publication ceilings at every sender callback — 2026-11-09

- [x C] `StorageRelayProvenanceMap.ReadPublicationWatermark` exposes the highest per-author sequence backed by durable RPM3 mappings (or the already-ACKed lower bound when no rows remain).
- [x C] `ApplyAndPublishAllAsync`, `RecoverAndPublishAllAsync` and `RunOutboundAsync` capture both the local Metadata Store outbound high watermark and every FLD relay author's RPM3 ceiling under one composed application commit guard. The local author is included explicitly even though it has no FLD JournalSeq map.
- [x C] Added the ceiling-aware `SyncSession.PushBatchAsync(..., IReadOnlyDictionary<NodeId, ulong> publicationCeilings, ...)` overload. It filters eligible authors, trims local/relay pages before batch construction and rejects any immutable pending reservation whose absolute sequence exceeds its author's captured ceiling. The old overload preserves its original behavior.
- [x C] The optional `FldBinSyncHostAdapter.PublishReadyOutboxAsync` converts the complete watermark set and drains a peer session only through the enforcing Core overload. The callback no longer has to reimplement the sequence-cap logic.
- [x C] Updated `docs/FLD-DB-INTEGRATION-CONTRACT.md` to make the cap mandatory and distinguish it from the adapter async gate.
- [~ T] Migrate host callback implementations and test concurrent local commit between final page commit and send, unknown/new author exclusion, zero ceiling, metadata-owned cutover and ACKed-tail behavior. API migration is intentionally deferred until C-pass closure.
- [~ O] Measure the added per-author map lookup/array cost against the safety benefit; do not remove the ceiling without an equivalent enforceable publication fence.

BIN-SYNC commits: `9d33cddbbcf2af5281fe580d6fdd5885f3efe9b1`, `60489796218cdd39a84f8cbc93ce4f53be90dfd3`, `97609fb3c91e15aac27ff8d2eccf576381993c1c`, contract `87a8b7a0d364f80982696f1b748294bf042424a4`. Exact-head CI is not yet run; do not mark T complete.


## 0.86. Recovery checkpoints after publication-ceiling C slice — 2026-11-09

- [x C] Created `backup/cpass-2026-11-09-publication-ceilings` in BIN-SYNC at `701077ed9a13ce476aaa1e679d9c17f16f0ced5f`.
- [x C] Created the matching FLD-DB backup branch at `01617ecaa06fef4bd4ec16e913f24ffd449158ae`.
- [~ T] These branches preserve the current unvalidated code/API state. They are recovery points, not green baselines.


## 0.87. C-pass: validate map ownership before every relay gate entry — 2026-11-09

- [x C] Added `StorageSyncCoordinator.ValidateRelayProvenanceMap` to fail closed unless the RPM3 map matches the coordinator's metadata scope, local replica and exact durable StorageDatabase.
- [x C] Applied this validation to APPLY/publication, startup recovery, single/all-author ACK and `RunOutboundAsync`. The no-author case can no longer bypass ownership checks and emit a misleading empty/foreign ceiling set.
- [x C] Created recovery branch `backup/cpass-2026-11-09-publication-ceilings-v2` at `0f46e444529b77997ce1ba190838de3c4f4ecb6d`.
- [~ T] Add wrong-scope, wrong-replica, different-StorageDatabase and empty-author-list rejection tests; migrate all callback call sites to the new ceiling-bearing API. No tests have been run for this C-pass.


## 0.88. C-pass: allocation-free RPM3 author enumeration — 2026-11-09

- [x C] Added shared `NodeIdCanonicalComparer`, an allocation-free comparer exactly equivalent to ordinal `NodeId.ToString()` order (little-endian byte identity order).
- [x C] `StorageRelayPublicationGate.DiscoverAuthors` and `StorageRelayProvenanceMap.ReadAuthors` now use the comparer; RPM3 discovery no longer creates a hex string per author or a `SortedDictionary<string,NodeId>`.
- [x C] Created `backup/cpass-2026-11-09-nodeid-ordering` at `2c7007f059c2da1878c24e64a76dec902f9e8c73`.
- [~ T] Compare exact ordering with canonical hex strings for random and edge-case IDs; verify duplicate/corrupt cursor handling and author ordering across reopen.
- [~ O] Measure large-author startup scan allocations/CPU; no benchmark claim yet.


## 0.89. C-pass: bounded Storage snapshot/feed scans and crash-resumable ACK compaction — 2026-11-09

- [x C] Reconciled the authoritative FLD `StorageDatabase.cs` with BIN-SYNC's vendored copy; both now share the same blob `fa31c0e6395ad6bc999bf4a3fee8570b6acef20a`. `OpenSnapshot` shares immutable value arrays and materializes only a sorted key/reference index; `PrefixCursor` supports bounded pages and the borrowed `StorageEntryView`/`VisitPage` API.
- [x C] `StorageMetadataStore.TryGetPartition` now decodes persisted rows through 512-row pages instead of materializing the entire partition in one `ScanPrefix(..., int.MaxValue)` result; persisted-entry indexing is built during decode.
- [x C] `StorageRelayProvenanceMap` uses borrowed spans for author and mapping recovery scans, avoiding copied `StorageEntry` key/value arrays.
- [x C] `StorageDataProvider.ReadLocalChanges` uses borrowed prefix views; `AcknowledgeLocalChanges` deletes at most 4,096 rows per atomic transaction while advancing the durable ACK watermark in the same transaction, so reopen resumes at the last committed page. Tombstone-GC session validation is also paged.
- [x C] Created `backup/cpass-2026-11-09-storage-streaming` at `7b77065bd2904943585e254d3368c99fb01a1b85`.
- [~ T] Add cursor-boundary, visitor-stop, snapshot-lifetime, >100k feed ACK, crash-between-ACK-pages, partition-reopen and exact FLD/BIN byte-parity tests. No tests were run in this C-pass.
- [~ O] Measure snapshot open allocations, metadata partition reopen peak memory, provider ACK throughput and map recovery allocations; the sorted key index remains O(number of keys), not O(page).


## 0.90. C-pass: zero-copy single-entry cursor and provider snapshot merge — 2026-11-09

- [x C] `StorageSnapshot.PrefixCursor.TryReadNext(out StorageEntryView)` exposes one borrowed row without allocating a page, `StorageEntry` or key/value arrays. `VisitPage` is now built on this primitive; FLD authoritative source and BIN vendor remain byte-identical (blob `a28cd58fbb72a1453a3ffb04a1ab1d5ed43ddaa1`).
- [x C] `StorageDataProvider.ProviderSnapshotState` now merges field/tombstone snapshot streams by decoding one borrowed row per cursor, removing two 1,024-row copied page buffers and the per-row `StorageEntry` copies.
- [x C] Added span-based generation-key validation for the borrowed row path.
- [x C] Created `backup/cpass-2026-11-09-provider-snapshot-streaming` at `7c8e160352c08756721a278dac25c63e90e8b541`.
- [~ T] Verify merge ordering, Reset/absolute-offset behavior, cursor exhaustion/disposal, record-tombstone ordering and snapshot lifetime while commits/checkpoints advance.
- [~ O] Benchmark snapshot sender allocations and throughput at 1k/100k/1m rows; compare against prior page-buffer implementation before claiming improvement.


## 0.91. C-pass: preserve FLD1 WAL format while reusing Storage writer fence — 2026-11-09

- [x C] Added public `StorageDatabase.AcquirePartitionLease` / `StoragePartitionLease` to `Fld1.Storage`. It reuses the exact cross-process `WalWriterFence` used by `StorageDatabase.Open` without opening a generic Storage WAL or changing any FLD1 on-disk bytes.
- [x C] FLD `FldDatabase.Open` now acquires one root-wide reserved partition lease before manifest load, WAL recovery, Outbox/ApplyJournal open and run reconciliation; `Dispose` releases it only after all FLD file handles are closed. Multiple namespace handles for one physical root cannot race the shared manifest/runs.
- [x C] FLD Storage and BIN-SYNC vendor remain byte-identical at blob `4582c446ef11f308f9442c59438c5aca691614a8`.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fl-database-lease` in both repositories (BIN-SYNC `88de4289abaaf9b954f5c54be4d34590733f9f55`, FLD-DB `408399e4a465aecfa21c7dde245c297c85595191`).
- [~ T] Verify same-root second process/second namespace rejection, release after failed open, release after disposal from another thread, abandoned-owner recovery and no generic WAL files/format mutation.
- [~ C] This is the first shared ownership seam, not a completed `FldDatabase` migration to `StorageDatabase`. FLD-specific WAL/Run/Manifest encoding remains authoritative and unchanged pending a separate format/recovery ADR.


## 0.92. C-pass: cache immutable Storage snapshot index per commit epoch — 2026-11-09

- [x C] `StorageDatabase.OpenSnapshot` now caches the immutable `SortedList` index for the current commit epoch. Multiple snapshots with no intervening write share the same index and immutable value arrays in O(1), rather than rebuilding O(number-of-keys) arrays on every snapshot.
- [x C] Every successful commit invalidates the live cache after publishing the new values. Existing snapshots retain their previous sorted index and values; the next snapshot builds the new epoch index once.
- [x C] FLD authoritative source and BIN-SYNC vendor are byte-identical at blob `d5fee8be06b9193a294ce4f7c37ba84b5f37da15`.
- [x C] Matching recovery branches `backup/cpass-2026-11-09-snapshot-index-cache` exist in both repositories (BIN-SYNC `f1232647518ff2801c96a2c7a8af3e1e2e47a08c`, FLD-DB `9869aa81f2cbe964163d21922e1949f470496e42`).
- [~ T] Verify snapshots opened in one epoch remain stable across later commits/checkpoints/deletes and that cache invalidation happens only after durable publication. No test/benchmark claim yet.
- [~ O] Measure repeated snapshot creation in unchanged epochs and first snapshot after commit; O(number-of-keys) index creation remains on a new epoch.


## 0.93. C-pass: concrete FLD host adapter and canonical Storage project injection — 2026-11-09

- [x C] Added optional `samples/FldBinSyncHost/FldBinSyncHostAdapter.cs` implementing the public FLD↔BIN-SYNC composition: all-author discovery, complete JournalSeq page projection, recovery-before-send, APPLY/publication gate, ceiling-bearing outbound callback, durable FLD ACK before RPM3 prune, and one immutable FLD reference snapshot held under the FLD commit guard.
- [x C] Added an optional sample project and build guide. It is not added to the default BIN-SYNC solution because FLD-DB is a separate checkout; it is not a database runtime DLL and creates no FLD→BIN-SYNC dependency.
- [x C] BIN-SYNC `BinSync.Storage` and FLD `Fld1.Database` now both accept `Fld1StorageProject`, allowing the host sample to resolve one canonical FLD Storage project instead of duplicate vendor/authoritative assembly identities.
- [x C] Created matching pre-adapter recovery branches `backup/cpass-2026-11-09-host-adapter-prep` in both repositories (BIN-SYNC at `ef410ca05d8230618273c2907c52b106ba51d671`; FLD-DB at `cf4c454d539f96766ef66febb3d117c9a7c5965b`).
- [~ T] Compile the optional sample against the sibling FLD checkout; verify the shared Storage project resolves once, then add host crash/reopen tests for recovery, publication ceilings, multi-author drain, ACK-before-prune and scanner disposal/lock order. No test/build claim is made for this sample yet.
- [~ C] Transport/session construction and process-level startup scheduler remain application responsibilities; the sample now supplies the safe sender drain helper. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 0.94. C-pass: enforce publication ceilings inside the Core sender — 2026-11-09

- [x C] The publication gate captures the local-author high watermark and all relay-author RPM3 ceilings under one composed application commit guard. Before provider cutover the local ceiling is read from Node Outbox; after cutover it is read from Metadata Store. The captured authority-mode bit accompanies every watermark, preventing a cutover race between ceiling capture and send.
- [x C] Added ceiling-aware `SyncSession.PushBatchAsync` overloads for both Node-owned and Metadata Store-owned transport. The Node path filters Outbox entries by author ceiling; the Metadata Store path filters outbound/relay pages and rejects immutable pending reservations above the captured absolute sequence (`CursorEnd + StreamOffset`). Both exclude absent/zero-ceiling authors; legacy overloads preserve their prior behavior.
- [x C] `FldBinSyncHostAdapter.PublishReadyOutboxAsync` selects the matching sender path from the captured mode, converts the watermark list into the Core ceiling map and drains a peer session only through the enforcing overload.
- [~ T] Verify local and relay streams at zero/exact/above-ceiling boundaries, pending-reservation replay, page truncation and concurrent local freeze between ceiling capture and send. No tests/build have been run for this C-pass.
- [~ C] The host still owns session creation, transport lifecycle, multi-peer scheduling and ensuring every send uses the gated helper. Physical GC remains disabled.


## 0.95. C-pass reconciliation of earlier host-adapter checklist entries — 2026-11-09

- [x C] Reconciled the earlier open items in §§0.79–0.80, 0.84–0.89 and the integration/API checklist: the FLD relay-page reader, RPM3 resolver/append, startup recovery, publication gate, local+relay author ceilings, Core-enforced sender, durable FLD ACK-before-RPM3-prune and reference-snapshot scanner are implemented in the optional host sample and Core/Storage APIs (§§0.93–0.94).
- [~ C] Still open in the broad C-pass: the `FldDatabase`→`IDataProvider` bridge (stable paged snapshots, local feed/ChangeId identity and full remote-apply context), generic Storage page/run/WAL backend compatibility decision and implementation, final public/package/source/byte audit, and host-owned transport/session scheduler. The existing FLD1 field-aware WAL/Run/Manifest format is not to be replaced without the required versioned compatibility decision.
- [~ T] The optional cross-repository sample and new sender overload have not been built or tested. The earlier red BIN-SYNC Core/Storage tests remain T-pass work; no physical receipt, ChangeId or tombstone/Ghost GC is enabled.


## 0.97. C-pass: preserve the active outbound authority across cutover — 2026-11-09

- [x C] The local publication ceiling now comes from Node Outbox before provider-authoritative cutover and from Metadata Store after cutover. Relay ceilings remain RPM3-derived in both modes.
- [x C] The gate captures the authority-mode bit under the composed application commit guard and carries it with the complete watermark set. The sender no longer re-reads mutable cutover state after the barrier is released.
- [x C] Added ceiling-aware Node-owned transport filtering and updated the host sample to choose Node or Metadata Store send path from the captured mode. This preserves the pre-cutover Node Outbox path and the post-cutover Metadata Store path without mixing sequence domains.
- [~ T] Verify both modes before/after cutover, concurrent cutover attempts, local and relay ceiling boundaries, retry/reservation replay and process restart. No tests/build have been run for this C-pass.
- [~ C] Host still owns session lifecycle and multi-peer scheduling; all sends must go through the gate callback. Physical GC remains disabled.


## 0.98. C-pass: expose provider-feed freeze in the concrete host adapter — 2026-11-09

- [x C] `FldBinSyncHostAdapter.PublishLocalChanges(persistNode, limit)` exposes the coordinator's local-feed freeze operation. Convenience overloads for `RecoverAndPublishAllAsync`, `ApplyAndPublishAllAsync` and `RunOutboundAsync` invoke it before entering the gate, draining provider changes into Node Outbox before cutover or Metadata Store after cutover.
- [x C] The integration guide and sample lifecycle require a local-feed freeze before startup recovery/publication and before each later outbound gate invocation. Provider edits not frozen at the captured barrier remain above the ceiling and are deferred safely.
- [~ T] Verify feed drain, idempotent retry, persistence callback ordering and concurrent provider writes around ceiling capture in both publication modes. No tests/build have been run for this C-pass.


## 0.99. C-pass correction: relay/GC adapter is not the full IDataProvider bridge — 2026-11-09

- [x C] The optional sample does implement the FLD relay-page/RPM3/publication/ACK/reference-scan side and both Node-owned/Metadata-Store-owned ceiling-enforced sender paths.
- [x C] The full remote-apply-context half is now implemented separately by `IProviderRemoteBatchCommitStore` + `FldRemoteBatchApplyStore` (§§1.04–1.05); this preserves the FLD `RemoteBatchCommit` header without forcing it into `IDataProvider`.
- [~ C] It does **not** yet adapt `FldDatabase` to BIN-SYNC's `IDataProvider`. `FldDatabase` still lacks a generation-pinned paged full-data snapshot handle, Outbox-backed local feed/author-epoch mapping, record/field key-resolver policy and current-state `TryReadCurrent` semantics. Do not invent a lossy adapter or materialize the entire database into RAM as a shortcut.
- [~ T] After that contract/code is complete, build the optional sample and test snapshot stability, remote-apply idempotency, local feed identity, cutover, crash recovery and GC horizons. Physical GC remains disabled.


## 1.00. Mandatory v3.1 document hierarchy and release gate — 2026-11-09

- [x C] The roadmap distinguishes the v3.0.0 base normative contract, the mandatory v3.1 Roster/GC Addendum, the non-normative host orchestration pattern, and the required deterministic golden-vector fixtures.
- [x C] Clarified the compatibility boundary: v3.1 implementation/support is mandatory for project completion; v3.1-only behavior remains explicitly capability-negotiated and does not modify v3.0-only wire frames. A required member without v3.1 capability contributes no safe GC frontier.
- [x C] Added direct cross-references from the v3.0 specification, Public API, addendum and host guide to the correct companion documents. Corrected the stale host-guide statement that treated the synchronized-FLD relay adapter as wholly future work.
- [~ T] Execute `samples/V31ControlPlaneVectors`, assert exact payload/signature bytes and negative tamper cases in CI, and complete authority/member, joining/rejoin, FULL_REPLACE, crash/restart and safe-horizon conformance. Existing source implementation is not a conformance pass.
- [~ C] The `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/full remote-apply bridge, generic Storage format decision and host transport scheduler remain open. Physical GC remains disabled.


## 1.01. C-pass: execute v3.1 golden vectors as a required CI sample — 2026-11-09

- [x C] Extended `samples/V31ControlPlaneVectors` beyond encode-only checks: NRS2/OFR2/RCP2/FRA2 now decode and verify against trust/roster context and re-sign to byte-identical payloads; FULL_REPLACE_BEGIN now decode/verifies against the accepted roster; chunk/READY/COMMIT/ABORT round-trip coverage remains.
- [x C] Added fail-closed sample cases for truncated signatures, trailing bytes, tampered payloads, wrong pinned authority key, wrong signing domain, wrong roster generation/digest and invalid FULL_REPLACE_READY flags.
- [x C] Added the vector sample to `.github/workflows/dotnet.yml` and included `samples/**` in push/PR path filters so vector-only changes cannot bypass CI.
- [~ T] Exact-head CI for the sample change is running; close only after the executable returns PASS and Core/Fuzz plus Storage Integration results are captured for the same code head.
- [~ T] Add dedicated unit tests for malformed roster/checkpoint/frontier edge cases and the complete FULL_REPLACE sender/receiver crash/rejoin state machine; the sample does not replace the full adversarial test matrix.


## 1.02. C-pass: executable v3.1 authority/member host-round composition — 2026-11-09

- [x C] Added `samples/FldBinSyncHost/V31NamespaceGcHostOrchestrator.cs` as a reusable one-shot host helper over the explicit v3.1 coordinator APIs. It sequences accepted-roster send, per-session OFR2 receive/accept, exactly one durable RCP2 publication and broadcast of the same checkpoint.
- [x C] Added Active-member round composition: receive/accept roster, reject non-Active local members, publish/send OFR2 under the supplied composed host lease, then receive/accept RCP2. Added retries for an already-durable OFR2 and an already-accepted RCP2 without allocating new report/checkpoint sequence identities.
- [x C] The helper validates unique remote sessions and binds each received OFR2's member NodeId and transport epoch to the authenticated session peer. Missing Active roster members remain safe at frontier zero; caller-provided session lists are not treated as authoritative membership.
- [~ C] FULL_REPLACE Joining→installed ACK→Active transition remains a separate explicit receiver/authority operation; the helper intentionally refuses to run an Active-member frontier round for a Joining/Retired member. Actual `FldDatabase`→`IDataProvider` snapshot/local-feed/full remote-apply bridge remains open.
- [~ T] Build the cross-repository sample and test partial authority-round failure, checkpoint rebroadcast idempotency, report resend, duplicate/mismatched session identity, stale transport epoch, missing Active member frontier zero, cancellation and concurrent receive ownership. No build/test pass is claimed for this new helper.


## 1.03. C-pass: cross-repository FLD host build added to CI — 2026-11-09

- [x C] Added independent `fld-host-integration-build` job to `.github/workflows/dotnet.yml`. It checks out the BIN-SYNC branch and `FLD-DB/fix/crash-consistency-outbox`, vendors the required Ed25519 source, and builds the optional host adapter against explicit canonical `Fld1.Database.csproj` and `Fld1.Storage.csproj` paths.
- [x C] The job is independent of the red Core/Storage test jobs, so the cross-repository compile result is visible even while fixture migration remains outstanding.
- [~ T] Exact-head job for workflow commit `6ff484f2730d22aacb224163628ff528417fb3a7` was queued/in progress at this update; no build pass is claimed until the job completes.


## 1.04. C-pass: preserve full remote APPLY context across provider boundary — 2026-11-09

- [x C] Added optional Core capability `IProviderRemoteBatchCommitStore` and immutable `ProviderRemoteBatchCommit`. It retains authenticated SenderPeer/transport epoch separately from wire issuer/epoch, and carries BatchId, cursor range, previous/current digest, RecordsRoot, protocol-state delta and immutable provider mutations.
- [x C] `StorageSyncCoordinator` dispatches to the contextual capability after Node apply and during PendingApply recovery. It verifies the exact full-header fingerprint against the durable PendingApply before calling the provider; non-capability providers retain the existing `IDataProvider.ApplyRemoteBatch(token, mutations)` fallback.
- [x C] Added `samples/FldBinSyncHost/FldRemoteBatchApplyStore`, which maps the full context to FLD `RemoteBatchCommit` and `FldDatabase.ApplyRemoteBatch`. Record tombstones map to FLD lifecycle field 0/DeadValue; field tombstones remain distinct.
- [x C] The FLD adapter deliberately passes an empty `RelayWrites` set because BIN-SYNC Metadata Store is the sole v3 owner of relay payload and delivery retention; it does not create a duplicate FLD relay history. The current v3 Batch DTO has no protocol-state-delta member, so none is synthesized.
- [~ C] Full `IDataProvider` integration is not complete: stable generation-pinned paged snapshots, Outbox-backed local feed/author-epoch mapping, record/field key resolver policy and current-state `TryReadCurrent` semantics remain open.
- [~ T] Build the cross-repository sample using `FLD_DB_READ_TOKEN` (fine-grained Contents: read for FLD-DB) and test lossless header propagation, same-apply-token retry, crash after FLD WAL/ApplyJournal before Metadata finalization, record/field tombstone mapping and duplicate replay. No sample build is claimed until the private checkout job actually executes.


## 1.05. C-pass: inject remote-apply capability separately from IDataProvider — 2026-11-09

- [x C] `StorageSyncCoordinator` accepts an optional `IProviderRemoteBatchCommitStore` dependency separate from `IDataProvider`. Contextual remote APPLY and PendingApply recovery use the explicit capability first, then a provider-implemented capability, then the compatibility fallback.
- [x C] Added `UsesRemoteBatchCommitStore` and `FldRemoteBatchApplyStore.IsBoundTo`; `FldBinSyncHostAdapter` requires and verifies the exact same capability instance and exact same `FldDatabase` instance, failing closed during construction on split-brain wiring.
- [x C] Added `FldBinSyncHostAdapter.ReceiveAndPublishAllAsync`, which routes inbound frames through `ReceiveBatchBeforeAckAsync`: PendingApply → Node checkpoint → contextual FLD `RemoteBatchCommit`/ApplyJournal → Metadata receipt finalization → incoming ACK → all-author relay mapping/publication. Host guides now use this entry point rather than a generic/lossy provider callback.
- [~ C] The full `FldDatabase`→`IDataProvider` snapshot/local-feed/key-resolver bridge remains open; this dependency split closes only the lossless remote-apply half.
- [~ T] Verify recovery calls use the injected adapter after process reopen, wrong-store/wrong-database construction is rejected, duplicate ApplyToken is idempotent, and Node/FLD/Metadata finalize order survives each crash cut. The current cross-repository build job skipped the private FLD checkout because no read credential was available.


## 1.06. C-pass: exact FLD contextual apply-store identity and receive gate — 2026-11-09

- [x C] Added `StorageSyncCoordinator.UsesRemoteBatchCommitStore` and `FldRemoteBatchApplyStore.IsBoundTo`; the host adapter now requires the same capability object to be injected into the coordinator and the same exact `FldDatabase` instance to be held by the adapter. Split-brain/wrong-instance setup fails before any network work.
- [x C] Added `ReceiveAndPublishAllAsync` so the concrete host receives through `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync`, ensuring the authenticated PendingApply context is delivered to `FldRemoteBatchApplyStore` before the source ACK; relay pages are drained only after that receive transaction returns.
- [x C] README and host/integration guides now show construction order and use the lossless receive method; public API docs describe the optional contextual capability.
- [~ C] The full `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge, generic Storage ordered page/run backend decision and host-owned transport scheduler remain.
- [~ T] Cross-repository build still needs an authenticated FLD checkout; add wrong-instance, duplicate ApplyToken and crash-cut ordering coverage before T-pass closure. Physical GC remains disabled.


## 1.07. C-pass: serialize v3.1 checkpoint retry and preflight round inputs — 2026-11-09

- [x C] Public `BroadcastAcceptedCheckpointAsync` now acquires the same one-shot `_roundGate` as authority/member rounds. `RunAuthorityRoundAsync` uses a private already-gated core so it does not deadlock; partial broadcast retry remains bound to the exact current durable RCP2 and cannot mint another checkpoint sequence.
- [x C] `PublishAuthorityMemberFrontier` uses a non-blocking gate acquisition and fails closed if another namespace round is active, rather than racing its issued/applied frontier with checkpoint publication.
- [x C] Authority round validates authority/member Ed25519 seed lengths, page bounds, FULL_REPLACE generation/digest pairing and session identities before sending the accepted roster; invalid input no longer partially mutates remote sessions before failing.
- [x C] Created recovery branch `backup/cpass-2026-11-09-contextual-receive-v31-gate` at `3c256b0cd9d7df778874659cf5482519889cd476`.
- [~ T] Verify concurrent authority-round + checkpoint-retry exclusion, partial-broadcast recovery, cancellation at each session boundary, non-member/Joining session rejection, durable frame-sequence monotonicity and same-checkpoint digest identity. Latest CI for `3c256b0` was queued/in progress at this update; no result is claimed.
- [~ C] The `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge and generic Storage backend/format strategy remain open. Physical receipt, ChangeId and tombstone/Ghost GC stay disabled.

## 1.08. C-pass: immutable prepared FULL_REPLACE snapshot and shared manifest — 2026-11-09

- [x C] Added `PreparedFullReplaceSnapshot`, which owns one provider snapshot token plus the one signed FULL_REPLACE manifest (roster generation/digest, source change-feed watermark, entry count and digest). Disposal releases the provider snapshot exactly once.
- [x C] Refactored `StorageFullReplaceSender` with `PrepareSnapshot` and `SendPreparedSnapshotAsync`. The digest scan and manifest signing now happen once; each recipient streams the same immutable snapshot and verifies the same signed digest. Existing single-recipient `SendAsync` remains as a convenience wrapper.
- [x C] Added `V31NamespaceGcHostOrchestrator.TransferFullReplaceToMembersAsync`: validates the full non-retired roster/session set, broadcasts the accepted roster, captures one source snapshot and fans the same generation out to all intended peers.
- [~ T] Exact-head compile and partial-fanout/retry tests remain pending. Verify that every recipient receives identical watermark/count/digest and that an already-installed recipient can safely retry the same generation. This is not yet a durable cross-process source snapshot.
- [~ C] The `FldDatabase`→`IDataProvider` source snapshot/local-feed/key-resolver bridge and durable source-snapshot recovery across process restart remain open. Physical GC remains disabled.

## 1.09. C-pass: retain prepared FULL_REPLACE source snapshot for partial fanout retry — 2026-11-09

- [x C] Added `PrepareFullReplaceSnapshot` to return ownership of the immutable prepared snapshot to the host instead of tying its lifetime to one fanout call.
- [x C] Added `TransferPreparedFullReplaceToMemberAsync`. A host can retry only a missing/failed recipient using the exact same source watermark, roster digest, generation and FULL_REPLACE digest; the method revalidates the current accepted roster and target transport epoch before sending.
- [x C] The convenience all-member fanout remains available; the explicit prepare/retry API is the resumable in-process path. The caller owns and must dispose the prepared snapshot only after the intended fanout is complete or abandoned.
- [~ T] Verify partial failure after each recipient, repeat send to already-installed receiver, accepted-roster change during retry, and snapshot disposal after cancellation. No exact-head test result is claimed.
- [~ C] In-process prepared snapshot lifetime does not yet survive process restart; crash-resumable fanout requires a durable provider snapshot generation. The missing FLD provider adapter must supply that without synthesizing ChangeId/origin metadata.

## 1.10. C-pass: fail-fast FULL_REPLACE manifest/roster preflight — 2026-11-09

- [x C] Added `StorageFullReplaceSender.ValidatePreparedSnapshot` to verify the prepared snapshot watermark, accepted roster generation/digest, authority identity and Ed25519 manifest signature before a sender callback can touch the network.
- [x C] `TransferPreparedFullReplaceToMemberAsync` now validates the retained manifest against the current accepted roster before sending a roster or FULL_REPLACE frame. A stale roster or invalid authority key fails locally.
- [x C] The convenience single-recipient and all-member fanout paths now capture and verify the provider snapshot/manifest before broadcasting the roster. This avoids partial remote state changes when the source snapshot or authority signature is invalid.
- [~ T] Validate no network frame is sent on invalid seed, wrong authority, stale roster, disposed snapshot, corrupt page, and wrong target epoch; verify partial retry after a successful preflight. CI for the newest code head is pending.
- [~ C] Prepared source snapshot survives retries only while retained in process memory. Durable FULL_REPLACE source generation recovery after process restart remains open; no physical GC is enabled.

## 1.11. C-pass: bind every FRA2 acknowledgement to the exact source snapshot HLC floor — 2026-11-09

- [x C] `StorageFullReplaceSender.CreateManifest` computes the maximum `(HlcWall,HlcLogical)` while scanning the exact immutable provider snapshot used for the signed manifest/digest. `PreparedFullReplaceSnapshot` retains the pair for multi-recipient fanout and retry.
- [x C] Both `SendPreparedSnapshotAsync` and the compatibility `SendSnapshotAsync` path now pass the source snapshot floor into the common transfer routine. After FRA2 signature/identity/roster/digest checks, the sender also requires the installed ACK's HLC floor to equal the source floor exactly before persisting the ACK into the namespace GC store.
- [x C] This prevents a validly signed but inconsistent receiver acknowledgement from inflating or lowering the common RCP2 FULL_REPLACE HLC floor. All members must install the same immutable source HLC horizon, not merely report mutually equal values.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fullreplace-hlc-ack` in BIN-SYNC and FLD-DB (BIN-SYNC anchored at `d4ad36fe4fc1a3660c43cc4209a71f400ed224fe`, FLD-DB at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add tests for exact wall/logical equality, wall mismatch, logical mismatch at equal wall, empty snapshot `(0,0)`, source changes after snapshot, signed inconsistent FRA2 from a custom peer and retry after a rejected ACK. No tests were changed or claimed in this C slice.
- [~ C] Durable FULL_REPLACE source snapshot recovery across sender process restart remains open; this code only guarantees process-local immutable snapshot identity. `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge also remains open.

## 1.12. C-pass: make prepared FULL_REPLACE snapshot disposal safe during async fanout

- [x C] `PreparedFullReplaceSnapshot` now tracks active transfer leases. `Dispose()` requests release immediately but defers `IDataProvider.ReleaseSnapshot` until the final in-flight transfer exits; release remains exactly-once and is safe after cancellation/failure.
- [x C] `StorageFullReplaceSender.SendPreparedSnapshotAsync` acquires the transfer lease before preflight and holds it across the awaited network transfer. A concurrent dispose can no longer invalidate provider pages after BEGIN/READY or between CHUNK frames. New transfer attempts after disposal are rejected.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fullreplace-send-lease` in BIN-SYNC (anchored at `545041f4d35a9e9ed5414d1619ffedba6559312d`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Verify dispose-before-send rejection, dispose during READY wait, dispose mid-chunk, cancellation, concurrent transfers and exactly-once ReleaseSnapshot. No exact-head CI result exists for these commits.
- [~ C] Prepared snapshots remain process-local; restart-resumable source generation persistence is still open.

## 1.13. C-pass: Joining promotion requires the exact RCP2 FULL_REPLACE HLC floor

- [x C] `V31NamespaceGcHostOrchestrator.PromoteInstalledJoiningMemberAsync` now checks the Joining member's durable FRA2 `HlcWallFloor` and `HlcLogicalFloor` against the already accepted prior-roster RCP2 before changing the member to Active.
- [x C] Promotion now requires agreement on the exact generation, digest, roster generation, transport epoch, installed state and both HLC floor components. A signed ACK for the same digest but a different HLC horizon cannot activate the member.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-joining-hlc-promotion` in BIN-SYNC (anchored at `10a4e17492641a3911acdbdf8e4aadc865dad371`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add promotion rejection tests for HLC wall mismatch, logical mismatch at equal wall, wrong prior roster generation, stale transport epoch, wrong digest and valid matching ACK. No exact-head CI result is claimed.
- [~ C] Durable prepared FULL_REPLACE snapshot recovery across process restart and the complete FLD provider snapshot/local-feed/key-resolver bridge remain open.

## 1.14. C-pass: enforce v3.1 capability dependencies symmetrically

- [x C] OFR2 send and receive now both require `NamespaceRosterV31 | OriginFrontierV31`; RCP2 send and receive require the same pair because a frontier/checkpoint is meaningful only under the accepted roster context.
- [x C] All FULL_REPLACE transport frames now require `NamespaceRosterV31 | FullReplaceV31`. FRA2 send and receive use the same dependency; a peer cannot advertise FULL_REPLACE support while omitting the roster capability needed to authenticate member identity/epoch.
- [x C] v3.1 capability negotiation remains independent of v3.0 frame behavior. Partial capability sets can establish a session, but dependent v3.1 operations fail closed before sending/receiving frames.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-v31-capability-dependencies` in BIN-SYNC (anchored at `7a8c7f2eea341b879d6f39b747e19d3b3e5c0357`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add asymmetric capability-matrix tests for every v3.1 send/receive API, exact negotiated intersections, partial peers and v3.0-only interoperability. No exact-head CI result is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery, complete FLD provider snapshot/local-feed/key-resolver bridge, generic Storage physical format decision and host transport scheduler remain open.

## 1.15. C-pass: validate v3.1 capability dependencies in HELLO/HELLO_ACK

- [x C] `SessionWire.EncodeHello` and `EncodeHelloAck` reject local capability advertisements that include `OriginFrontierV31` or `FullReplaceV31` without `NamespaceRosterV31`.
- [x C] `SessionWire.DecodeHello` and `DecodeHelloAck` reject the same invalid dependency mask from a remote peer with `FRAME_REJECTED`, before constructing a session. Unknown future capability bits remain forward-compatible; valid v3.0 and v3.1 bitmasks retain identical wire bytes.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-v31-hello-capability-validation` in BIN-SYNC (anchored at `7e763f0f5939ce2b134e0d5b7a55e92361e51b3b`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add HELLO/HELLO_ACK encode/decode tests for each partial/valid capability mask, unknown bits, malformed peer advertisements and byte-for-byte valid HELLO compatibility. No exact-head CI result is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery, FLD FULL_REPLACE receiver staging and generic Storage physical-format decision remain open; base provider bridge, author closure, publication cutover and host scheduler are implemented in §§1.20–1.24.


## 1.16. C-pass: make Metadata Store the exclusive relay authority after cutover — 2026-11-09

- [x C] Added a direct Metadata Store-owned publication watermark capture: local author from `GetOutboundHighWatermark` and relay authors from `IMetadataPeerStateStore.ReadRelayAuthors` + `GetRelayHighWatermark`, all captured under the composed coordinator → Node → FLD → Storage barrier. No FLD JournalSeq or RPM3 data is consulted after cutover.
- [x C] Added metadata-owned startup recovery, APPLY→send, later outbound and ACK-compaction paths in `StorageRelayPublicationGate`. Startup remains closed until PendingApply recovery succeeds; network send retry does not incorrectly require recovery again.
- [x C] Routed `FldBinSyncHostAdapter` by captured publication mode. After cutover, contextual FLD `RemoteBatchCommit` has an intentionally empty `RelayWrites` set; Metadata Store finalization owns the only relay payload/sequence. Per-peer ACK is committed by `SyncSession`; safe relay payload compaction uses the minimum durable peer cursor and `CompactRelayThrough`. Legacy Node-owned mode retains FLD JournalSeq→RPM3→FLD ACK→RPM3 prune.
- [x C] Added an exact Metadata Store identity check for the sender's `IMetadataPeerStateStore` instance; split Storage partitions fail closed before metadata-owned send.
- [x C] Corrected startup flow so `RecoverPendingApplies` and local-feed freeze run under the composed commit lease, then metadata-owned startup calls `RecoverMetadataOwnedAndPublishAsync` instead of the legacy RPM3 recovery path.
- [x C] Updated the sample README, host composition guide and FLD integration contract to distinguish the two publication authorities and their independent ACK/compaction rules.
- [~ T] Add focused tests for metadata relay-author discovery, local/relay high-watermark capture, exact Metadata Store identity, startup recovery gating, send failure/retry, per-peer minimum ACK, compaction restart and legacy-mode non-regression. The current run at `513188e` is still in progress; Storage Integration on Ubuntu is red on existing provider/origin fixture migrations (91/171), not a test pass for the new relay path.
- [~ C] Full `FldDatabase`→`IDataProvider` bridge remains open: generation-pinned paged snapshot, stable current-state ChangeId/origin mapping, Outbox-backed local feed/author-epoch contract, key resolver and `TryReadCurrent`. Generic Storage page/run/WAL format decision and host transport scheduler also remain. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.17. C-pass: add indexed immutable-run access for the FLD provider snapshot — 2026-11-09

- [x C] FLD-DB now exposes `RunReader.TryReadRecordIdAt(uint, Span<byte>)` in `fix/crash-consistency-outbox` (commit `a6e20ce15c7d13ed63579db019d00e52b7b7e63f`). It reads a canonical run slot by index without allocating or scanning earlier clusters.
- [x C] Verified against `RunBuilder`: immutable run cluster slots are emitted in canonical RecordId order, enabling a bounded k-way merge across run generations.
- [~ C] Continue the FLD generation-pinned paged snapshot over captured immutable run buffers and a bounded overlay snapshot; then implement the complete `IDataProvider` current-state, local-feed and key-resolution bridge. No whole-database RAM materialization.
- [~ T] FLD RunReader index-boundary/order tests and cross-repository snapshot/compaction stability tests remain deferred to the dedicated T-pass. No build or test pass is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery across sender restart, generic Storage page/run/WAL format decision and host transport/session scheduler remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.18. C-pass: implement generation-pinned paged FLD provider snapshot primitive — 2026-11-09

- [x C] FLD-DB `FldDatabase.OpenProviderSnapshot()` now captures an immutable Run/overlay generation and the source Outbox watermark under its commit guard. The handle retains immutable run buffers across concurrent compaction and releases them exactly once on disposal/database close.
- [x C] Snapshot pages use an O(keys) compact sorted RecordId index and bounded page materialization; no whole-database field/value list is built. The implementation is in FLD-DB commit `86088614d06297f5c9a44ffcc9784b33abbb0961` plus follow-up correctness/performance commits `c010333c019001bab35af29b879e5757020309ee`, `8f54b6c09396897bc88ecb5c2da6e65d634b99fe`, `08fcad22169664c2d9f1ab204278b247c2111215` and `8202c6c5b7085071f6df3d2db81389d76e4e4a84`.
- [x C] Current-state resolution preserves Run v2 ChangeId/Origin and keeps Run v1 identity-zero semantics; field tombstones and record lifecycle tombstones remain distinct.
- [~ C] Bind this snapshot to BIN-SYNC `IDataProvider`, including FLD Outbox local feed, durable published-origin stamping, key resolution and current-state lookup. Durable FULL_REPLACE source-generation recovery across sender restart remains open.
- [~ T] Snapshot/compaction concurrency, ordered page retry, tombstone mapping, mixed Run v1/v2, release-on-dispose and cross-repository host build tests remain deferred. No build/test result is claimed.
- [~ C] Generic Storage page/run/WAL physical-format decision and host transport/session scheduler remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.19. C-pass: permit identity-zero legacy record tombstones only as snapshot state — 2026-11-09

- [x C] Relaxed `ProviderMutationCodec.Validate` so a canonical lifecycle tombstone with zero ChangeId/Origin can represent a legacy materialized Run v1 snapshot row. This aligns the codec with the FLD snapshot ADR: legacy state is retained without inventing identity.
- [x C] `ValidatePublished` remains strict and still rejects identity-zero record tombstones from outbound freeze or remote APPLY; immutable operations continue to require complete ChangeId/Origin.
- [~ T] Add explicit codec/digest/FULL_REPLACE vectors for legacy record tombstones and negative tests proving `ValidatePublished` rejects them. No test/build result is claimed.
- [~ C] Complete the BIN-SYNC `IDataProvider` host adapter and source-feed/key-resolution bridge; durable FULL_REPLACE sender restart recovery, generic Storage physical-format decision and host session scheduler remain open.


## 1.20. C-pass: add the base FLD Database IDataProvider bridge and enforce exact host composition — 2026-11-09

- [x C] Added optional-sample `FldDatabaseDataProvider`: direct 16-byte RecordId / 2-byte FieldId resolution, fenced FLD Outbox feed, stable ChangeId and deferred origin epoch/sequence mapping, generation-pinned paged snapshots, current-state reads, published-origin WAL stamping and FLD commit guard.
- [x C] Context-free `IDataProvider.ApplyRemoteBatch` deliberately fails closed. The host must inject the exact `FldRemoteBatchApplyStore` so sender/transport epoch/cursor/digest context is not discarded.
- [x C] Added `StorageSyncCoordinator.UsesDataProvider` and constructor checks in `FldBinSyncHostAdapter`: the coordinator must use the exact FLD provider, the provider must use the exact FLD database, and the contextual apply store must target that same database.
- [x C] Updated the host README with construction order and the explicit remaining boundaries. FLD runtime still has exactly two DLLs; the adapter remains optional host code.
- [~ C] Remaining v3/v3.1 gates: FLD FULL_REPLACE receiver staging/atomic generation switch, durable sender snapshot generation recovery across restart, and generic Storage page/run/WAL physical-format decision. Author-epoch closure and host multi-session scheduling are implemented in §§1.21 and 1.23.
- [~ T] Exact cross-repository sample build and provider conformance/crash tests are deferred to the dedicated T-pass. No test/build pass is claimed.
- [~ C] Physical receipt, ChangeId and tombstone/Ghost GC remain disabled until the complete cross-store reference proof and T-pass.


## 1.21. C-pass: close local FLD author epochs only after drafts and Outbox are drained — 2026-11-09

- [x C] `FldDatabaseDataProvider` now implements `IProviderAuthorEpochClosureStore`. An epoch closes only while holding the FLD commit guard and only when `ReadUnpublishedLocalDraftHorizon().Count == 0` and Outbox high watermark equals its acknowledged watermark.
- [x C] Local append and epoch activation use the same FLD→provider lock order as closure, preventing a writer that passed an early check from racing into a just-closed epoch.
- [x C] The adapter obtains the active epoch from the host's durable BIN-SYNC identity authority; it does not duplicate protocol epoch state in FLD. New epochs must be strictly greater than the last locally closed epoch.
- [~ C] FLD FULL_REPLACE receiver staging/atomic generation switch, sender restart snapshot recovery and generic Storage physical-format decision remain open. Publication cutover is implemented in §1.24 and host session scheduling in §1.23.
- [~ T] Verify pending drafts, unacked Outbox, replayable WAL horizon, writer-vs-close race, restart after closure, reactivation and stale-epoch rejection in the T-pass. No test/build result is claimed.


## 1.22. C-pass: preserve pre-freeze ChangeId while binding the active local epoch at the provider edge — 2026-11-09

- [x C] `ProviderMutationCodec.Validate` now accepts a stable ChangeId with an unassigned Origin tuple (epoch/sequence both zero) for a materialized or unpublished provider row. It still rejects half-populated Origin tuples and Origin without ChangeId.
- [x C] `ValidatePublished` remains strict: outbound freeze and remote APPLY still require nonzero ChangeId, OriginAuthorEpoch and OriginSequence.
- [x C] `FldDatabaseDataProvider` binds the host's durable active author epoch to local ChangeId-bearing current-state/snapshot rows whose FLD Run provenance has not yet been stamped. Legacy Run v1 rows with zero ChangeId remain identity-zero and receive no synthetic identity.
- [x C] The local Outbox feed similarly binds the active epoch while keeping OriginSequence deferred until Metadata Store freeze; author-epoch closure prevents a feed from crossing epoch transitions.
- [~ T] Add codec/digest vectors for ChangeId-without-Origin, published-operation rejection, local epoch binding, legacy Run v1 rows, stamp-before/after-supersede and restart after epoch closure. No build/test result is claimed.


## 1.23. C-pass: add host-owned multi-session send/receive scheduling — 2026-11-09

- [x C] Added `FldBinSyncSessionScheduler`: one receive owner per session, serialized sends per session, concurrent fanout across peers and one captured publication-ceiling set shared by every peer in a namespace round.
- [x C] Added one-shot receive/apply/publish, continuous inbound loop and host-cadence periodic outbound loop. Both outbound paths freeze the provider local feed through the supplied Node persistence callback before capturing publication ceilings. A false publication-gate result stops the receive loop so the host can run reconciliation/recovery rather than silently continuing.
- [x C] Scheduler shutdown clears its gate registry without disposing semaphores still held by active duplex callbacks; lock-order inversion during receive-triggered fanout is avoided.
- [~ C] Application still owns transport/session creation, reconnection policy and final `SyncSession` disposal. Generic Storage page/run/WAL physical-format decision and durable FULL_REPLACE sender restart recovery remain open.
- [~ T] Validate one-reader-per-session, concurrent duplex send/receive, multi-peer ceiling consistency, failure retry, cancellation and scheduler shutdown with exact-head sample tests. No build/test result is claimed.


## 1.24. C-pass: persist FLD provider publication cutover in the exact Metadata Store keyspace — 2026-11-09

- [x C] Added `StorageMetadataPublicationMode`, a namespace-scoped `IProviderPublicationMode` view over the exact `StorageMetadataStore` instance used by the coordinator.
- [x C] The immutable 16-byte MCP1/version-1 cutover marker is persisted under a dedicated root (0x71) and namespace key, separate from decoded metadata partition rows so ordinary metadata rewrites cannot erase it. Partition identity, marker magic/version/length, immutable watermark and Storage commit-sequence conflict are validated.
- [x C] `FldDatabaseDataProvider` delegates publication-mode reads/writes to that wrapper and refuses cutover while FLD dirty/replayable drafts or unacknowledged Outbox entries remain.
- [x C] `FldBinSyncHostAdapter` now requires exact provider/FLD database/Metadata Store/scope/local-replica/namespace/remote-apply identities; a provider cutover marker cannot silently point at a different namespace or Metadata Store.
- [~ T] Add reopen/restart, concurrent metadata-write conflict, immutable marker mismatch, missing/corrupt identity row, namespace isolation, exact-composition and cutover crash-boundary tests. No build/test result is claimed.
- [x C] FLD FULL_REPLACE receiver staging/atomic generation switch is implemented in §1.26; durable sender snapshot recovery is implemented in §1.25. Option B generic Storage format and the scoped BIN-SYNC API/source/byte audit are closed in §§1.28–1.32 and §1.23.12. Exact-head crash/restart proof remains T-pass.


## 1.25. C-pass: persist and restore FULL_REPLACE sender source generations across restart — 2026-11-09

- [x C] Added a bounded, versioned durable state record for each prepared source generation: generation ID, provider snapshot token, source-feed watermark, HLC floor and exact signed FULL_REPLACE_BEGIN bytes. The record has a SHA-256 integrity checksum and is indexed by scope/local replica/namespace in a dedicated Storage keyspace.
- [x C] Added idempotent immutable save, indexed lookup/list and conflict-checked deletion. Index/state disagreement, malformed lengths, invalid checksums, duplicate generations and missing indexed rows fail closed.
- [x C] Added `PrepareSnapshotAndPersist`, `RestorePreparedSnapshot`, `ReleasePreparedSnapshot` and `PruneUnreferencedPreparedSnapshots`. Restore checks the durable provider token, snapshot page shape/count, signed manifest, current accepted roster/authority and source watermark before returning a sendable handle.
- [x C] Added `IProviderDurableSnapshotStore`; the FLD host provider delegates token-presence and pruning to FLD's persisted provider-snapshot generations. Prepared handles detach on ordinary Dispose while durable state remains; permanent release requires the exact original provider instance and removes sender progress after provider release.
- [x C] Hardened failure ordering: active transfer leases block permanent release; sender progress is retained if provider release fails; interrupted release is reconciled by pruning; stale provider snapshot tokens are removed without loading full Run payloads; generation restore/validation failures retain the persisted source generation for explicit recovery or cleanup.
- [~ T] Verify process restart/reopen, resumed multi-recipient transfer with byte-identical manifest/digest, missing/corrupt token, index/state mismatch, concurrent metadata write conflict, dispose during transfer, release failure at each durability boundary, stale-token pruning and wrong-provider release. No build/test result is claimed.
- [x C] FLD FULL_REPLACE receiver staging is implemented in §1.26; Option B is the accepted generic Storage boundary in §1.28; scoped API/source/byte audit is closed in §1.23.12 and §§1.28–1.32. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.26. C-pass: implement FLD-backed v3.1 FULL_REPLACE receiver staging and atomic generation switch — 2026-11-09

- [x C] FLD-DB now exposes protocol-neutral durable replacement staging: ordered immutable Run pages, checksummed stage progress, bounded resumable abort, staged-page readback and atomic per-namespace manifest switch. The active FLD generation remains unchanged until the single manifest commit point.
- [x C] The optional BIN-SYNC `FldDatabaseDataProvider` implements `IProviderFullReplace`. It maps mutations to FLD's neutral snapshot entry model, re-reads staged pages and verifies the canonical signed FULL_REPLACE digest with `ProviderFullReplaceDigestBuilder` before the FLD switch.
- [x C] Preserved namespace isolation, WAL/Outbox/ApplyJournal state and old Run generations pinned by durable provider snapshots. Restart recovery detects a manifest switch that completed before the durable stage commit marker; orphan collection protects uncommitted staged Runs.
- [~ T] Exact cross-repository build, receiver restart, page retry/order, lifecycle/field tombstones, v1/v2 provenance, digest tamper, manifest-switch crash boundary, old-snapshot pins, concurrent writer fencing and abort/reopen coverage remain deferred. No build/test result is claimed.
- [x C] Option B is accepted in §1.28: generic Storage owns byte-KV, opaque immutable blobs and its own catalog WAL; FLD1 Run encoding, manifest, field semantics and FLD WAL remain Database-owned. The scoped API/source/byte audit is closed in §§1.23.12–1.23.13 and §§1.28–1.32. No third DLL; any future WAL/Run format migration requires a separate ADR. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.27. C-pass: bind durable FULL_REPLACE sender token to the provider's persisted source watermark — 2026-11-09

- [x C] Extended the optional `IProviderDurableSnapshotStore` capability with a fail-closed token→source-watermark query. Providers that can only say a token exists but cannot prove its barrier do not qualify for restart-durable FULL_REPLACE.
- [x C] `FldDatabase` reads the watermark from its checksummed, namespace-bound provider snapshot manifest, verifies pinned Run presence and overlay integrity, and exposes the binding through the host adapter without introducing a BIN-SYNC dependency into FLD.
- [x C] `PrepareSnapshotAndPersist` now verifies that the provider durably owns the exact token/watermark pair before writing sender progress. `RestorePreparedSnapshot` verifies the same pair again before restoring the snapshot and checking its signed manifest/roster.
- [~ T] Verify absent/default capability, token present with mismatched watermark, wrong namespace, missing Run, corrupt overlay, reopen on a new provider instance and token release between lookup/read. No build/test result is claimed.

## 1.28. C-pass: introduce generic immutable Storage blobs and bind FLD Run generations — 2026-11-09

- [x C] Accepted physical-format Option B in FLD-DB ADR: Storage owns generic opaque immutable page/blob durability; Database retains FLD1 Run v1/v2 encoding, manifest catalog, field/tombstone/provenance semantics and FLD WAL. No third runtime DLL and no BIN-SYNC types in Storage.
- [x C] Added `Fld1.Storage.Engine.StorageBlobStore`: bounded page geometry, stable opaque Guid IDs, checksummed metadata (length/page count/SHA-256), pending→complete publication, idempotent resume, immutable-content conflict rejection, snapshot readers, page/whole-blob verification and reader pins.
- [x C] Blob payload pages are durable immutable files rather than large Storage KV values. The small catalog and publication marker remain in StorageDatabase/WAL, avoiding O(total Run bytes) growth of the generic in-memory KV index. Unix page publication now uses the shared native `StorageDirectorySync` helper and fails closed if directory-entry fsync fails; Windows directory-entry durability remains explicitly unclaimed.
- [x C] FLD checkpoints and compaction publish generic blobs before the manifest and retain exact legacy `runs/{RunId}.fld1` mirrors. Reads prefer complete blobs and fall back to legacy files. FULL_REPLACE stages immutable blobs and materializes the compatibility mirror before advancing its durable stage cursor; the empty replacement marker is also published as a blob.
- [x C] Run IDs are allocated root-wide against both legacy files and the generic blob catalog, with explicit exhaustion rejection. This closes the restart collision where an uncommitted FULL_REPLACE stage had a blob ID not yet represented by the active manifest. Normal checkpoint and FULL_REPLACE now share the same allocator.
- [x C] The legacy Run mirror writer now uses the shared native `StorageDirectorySync` path and fails closed if Unix directory-entry fsync fails. Checkpoint, compaction and FULL_REPLACE cannot publish a manifest/stage reference until both the blob publication and compatibility mirror rename are durable on Linux/macOS; Windows directory-entry durability remains unclaimed.
- [x C] Orphan collection combines the full manifest Run set, durable provider-snapshot pins and incomplete FULL_REPLACE stage Run IDs before pruning catalog blobs and unreferenced page directories. Reader pins prevent deletion while a live reader is open.
- [x C] `StorageBlobReader` serializes page reads, whole-blob streaming/materialization and disposal under one reader gate, so concurrent disposal cannot release the page-directory pin while a read is in flight.
- [~ T] Exact-head FLD build, blob catalog/page crash cuts, pending-write resume, corrupt/missing page, same-ID conflict, reader/delete pin, cross-namespace Run ID, stage restart/collision, compaction pin and directory-fsync fault coverage remain deferred. No build/test result is claimed.
- [x C] Source audit closed: pending→page temp write/flush/rename→directory flush→complete marker ordering is consistent; checkpoint, compaction and FULL_REPLACE all use the root-global allocator, immutable blob publication and durable legacy mirror before manifest/stage references. Catalog-less orphan-directory ID reuse is closed in §1.23.13 and FLD commit `fc11a16`. WAL remains Database-owned; no third DLL.
- [x C] Source-level GC eligibility, external-reference coverage and lock-order audits are complete (§§1.23.6, 1.23.10); all three physical-GC switches remain hard false. Adversarial v3.1 reference-horizon proof tests remain T-pass.

## 1.29. C-pass: make FLD provider snapshot generations directory-durable — 2026-11-09

- [x C] `OpenProviderSnapshot` now creates the generation directory through `StorageDirectorySync.CreateDirectoryDurable`, flushing newly created parent entries on Linux/macOS.
- [x C] Overlay Run bytes and temporary snapshot manifest files are flushed to disk and their containing directory entries are synced; the final manifest rename is followed by a directory fsync. A snapshot token is therefore not returned before its durable manifest publication completes.
- [x C] Snapshot release and stale-generation pruning flush the snapshot-root directory after deleting generation directories. A failed Unix directory sync propagates instead of being silently ignored.
- [x C] Exposed the domain-neutral `StorageDirectorySync` helper as a documented public API so the separate `Fld1.Database.dll` can share the exact same Unix durability primitive without duplicating native P/Invoke logic. FLD open-failure cleanup now releases the root writer lease even if disposing the generic Storage database itself throws.
- [x C] `CreateDirectoryDurable` retains parent-entry paths whose fsync failed and retries them on the next call, even though the directory now exists in-process. This prevents a retry from mistaking an incompletely durable mkdir for a durable path.
- [~ T] Verify process-kill/reopen at every boundary: generation mkdir, overlay file, temp manifest, final rename, release and prune; verify Windows remains explicitly outside the directory-entry durability claim. No exact-head build/test result is claimed.
- [x C] Full Run/blob/snapshot-pin source audit and generic Storage public boundary are closed in §§1.23.13 and 1.28–1.32. Exact-head restart/pin/failure tests remain T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.30. C-pass: make persisted provider snapshots consume the generic Run backend — 2026-11-09

- [x C] `RestoreProviderSnapshot` now prefers the generic immutable Run blob and validates the reconstructed FLD1 Run; the legacy `.fld1` mirror remains a fallback only when no completed blob is published.
- [x C] Durable snapshot-token checks now require each pinned Run to have a readable completed blob or a legacy mirror only when no blob catalog row exists. A pending/corrupt blob is never hidden by a compatibility mirror. The watermark-binding path streams each blob to `Stream.Null` to verify every page and the whole-blob SHA-256 without materializing the Run; legacy fallback Runs are fully decoded/validated at this boundary. The cheap presence-only check does not perform a redundant full read.
- [x C] Normal Run loading now attempts one catalog snapshot first and opens a second only on the legacy/pending fallback path, avoiding duplicate Storage snapshot creation on the common blob-backed read path.
- [~ T] Verify snapshot reopen with blob-only, mirror-only and mixed Run v1/v2 generations; missing page, wrong digest, pending catalog row, corrupt mirror fallback and concurrent release while checking the token. No exact-head build/test result is claimed.
- [x C] Blob-backed provider snapshot source/API/byte audit is closed; the exact-head recovery, corruption and concurrent-release matrix remains T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.31. C-pass: harden blob metadata arithmetic and reader lifetime — 2026-11-09

- [x C] Blob metadata page-count validation now uses quotient/remainder rather than `length + pageSize - 1`, preventing unsigned overflow from making a corrupt near-`UInt64.MaxValue` length appear to have zero pages.
- [x C] Whole-blob streaming checks the exact copied byte count in addition to SHA-256; malformed length/page geometry fails closed before a valid reader can be constructed.
- [x C] Reader page access, full streaming/materialization and disposal share a reader gate. Disposal cannot release the page-directory pin while a read operation is in flight; independent readers remain concurrent.
- [x C] The public directory-sync helper validates its path argument and documents the platform boundary.
- [x C] `StorageDatabase.Open` now normalizes the partition directory to an absolute path before opening WAL/catalog files; `DirectoryPath` is stable even if the process working directory later changes.
- [~ T] Add malformed metadata vectors around `UInt64.MaxValue`, page-count boundary, zero-length blob, simultaneous `CopyTo`/`Dispose`, concurrent independent readers and deletion while pinned. No exact-head build/test result is claimed.
- [x C] Final Storage public API/source/byte audit is closed in §§1.23.12–1.23.14 and 1.28–1.33. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled pending T-pass proof.

## 1.32. C-pass: reclaim provider snapshot generations that never published a manifest — 2026-11-09

- [x C] Audit rejected automatic deletion of GUID-named snapshot directories without a final `snapshot.manifest`: after a crash, absence can mean either an unpublished generation or loss/corruption of a previously durable pin manifest. FLD's pin scan now returns an indeterminate result for this case, and Run/blob orphan GC fails closed.
- [x C] `PruneProviderSnapshotsExcept` is the only bulk cleanup path for directories whose sender liveness must be reconciled; it may run only after the host supplies the complete durable sender-generation set. Explicit `ReleaseProviderSnapshot` may still delete its exact token after the sender's durable release ordering. Both paths flush the snapshot-root directory on Linux/macOS.
- [x C] Valid manifests are parsed, their generation is checked against the containing directory name, and their Run IDs are retained; malformed names, generation mismatches and corrupt/missing manifests never become implicit permission to delete Runs. Windows directory-entry durability remains unclaimed.
- [x C] Created current recovery points before further C-pass work: BIN-SYNC `backup/cpass-2026-11-09-storage-blob-roadmap` at `998a41d51e3b64dbf40735b5bdc3efa4f1c1c1c1` and FLD-DB `backup/cpass-2026-11-09-storage-blob` at `f4f850a6ca35a998a5625482ec83f70946b8a6f2`.
- [~ T] Add crash cuts before/after final manifest rename, restart cleanup of temp-only generations, preservation of valid token generations, corrupt-manifest fail-closed and interaction with sender progress pruning. No exact-head build/test result is claimed.
- [x C] Source/API/byte audit is closed; exact-head orphan-generation crash/restart and fail-closed pin tests remain T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.33. C-pass: preserve immutable blob geometry across Storage configuration changes — 2026-11-09

- [x C] The configured page size now applies only when creating a new blob. Existing pending/complete catalog rows retain their persisted page size for resume, verification and reads, so a later process can change its default page size without making older blobs unreadable.
- [x C] Public reader API documentation now distinguishes per-page length validation from whole-blob SHA-256 verification and warns that a streaming destination can contain partial bytes if I/O or final integrity validation fails.
- [x C] Created refreshed paired recovery points after page-geometry and durability hardening: BIN-SYNC `backup/cpass-2026-11-09-storage-roadmap-v2` at `d58cd806aeadad4cd40e35720de3a2d938998410` and FLD-DB `backup/cpass-2026-11-09-storage-blob-v2` at `79e384e67f3bcf06024a759e0b4bfd7dc2c62d4f`.
- [x C] Created the latest paired checkpoint for the final Storage API/source audit: both repositories now have `backup/cpass-2026-11-09-storage-final-audit`, anchored at the current FLD-DB and BIN-SYNC branch heads respectively.
- [x C] Immutable identity is the blob length plus SHA-256 content digest; reusing the ID with identical bytes is idempotent even if the caller's current default page size differs. Existing page files are verified using their original durable geometry.
- [x C] `ContainsBlob` and `TryOpenRead` no longer reject valid generations merely because the current writer's preferred page size changed.
- [x C] Blob page file contents are flushed individually, but directory entries are now fsynced once per completed blob generation rather than once per page. The pending catalog row remains authoritative until that single directory flush succeeds, so interrupted writes remain resumable while avoiding thousands of redundant directory fsyncs.
- [x C] Final Storage public API/source/byte audit is closed in §§1.23.12–1.23.13 and 1.28–1.33; catalog-less orphan-directory ID occupancy is also covered by §1.23.13.
- [~ T] Verify reopen/read/resume with page sizes 4 KiB, 1 MiB and 4 MiB across process versions, same-ID same-content different geometry, same-ID different-content rejection, partial pending-write recovery, and exact-head crash/failure matrices. No exact-head build/test result is claimed. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.34. C-pass: separate materialized snapshot state from immutable remote operations — 2026-11-10

- [x C] Added optional IProviderSnapshotStateApply; SNAPSHOT_CHUNK state is no longer routed through the immutable IDataProvider.ApplyRemoteBatch contract. Providers without the explicit capability fail closed at snapshot reconciliation.
- [x C] StorageDataProvider.ApplySnapshotBatch validates materialized state without inventing ChangeId/Origin identity and stores idempotency receipts under a dedicated SnapshotApplyReceipt prefix, registered in the provider-owned keyspace. ApplyRemoteBatch continues to require complete published provenance.
- [x C] Both apply paths share the same LWW materialization logic. A same-HLC/content snapshot replay cannot erase a previously stored immutable ChangeId/Origin tuple; snapshot and DELTA_BATCH receipts remain independent even when their tokens match.
- [x C] StorageSyncCoordinator now routes received snapshots through IProviderSnapshotStateApply. Added a conformance test proving zero-provenance snapshot acceptance, strict DELTA_BATCH rejection, receipt-domain separation and provenance preservation.
- [x C] Updated both Storage array samples to bind local drafts to the active author epoch. The in-process sample now demonstrates Metadata-Store-owned publication over PipeTransport/SyncSession, matching the durable provider contract instead of mixing it with the legacy Node-only path.
- [x T] Tested code head 2dc16a8621bd74eda9256e94d79d04e553a3bdb8: core suite 678/678 on Linux and Windows; durable provider integration 172/172 on Linux, macOS and Windows; both in-process and real-TCP Storage array samples passed on all three platforms; v3.1 signed control-plane vectors and fuzz/lab jobs passed.
- [~ T] The FLD cross-repository workflow job was green, but its private FLD-DB checkout and host-adapter build steps were skipped; the exact cross-repository build remains unverified.
- [x C/T] Snapshot-state/remote-operation contract and this exact-head cross-platform integration gate are closed. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled pending the separate complete reference-horizon proof.

## 1.35. T-pass: prove signed offline rejoin cannot inherit stale GC horizons — 2026-11-10

- [x T] Added an end-to-end durable-store test that seeds a retired member's prior-generation applied frontier, retention checkpoint and FULL_REPLACE-installed ACK, then exercises a signed roster-generation gap and rejoin.
- [x T] The test rejects Retired→Active on a consecutive generation, rejects reactivation at or below the retired transport-epoch fence across a gap, and accepts the higher signed full roster only after the rejoining member advances beyond that fence while preserving its immutable retired-origin closure.
- [x T] After the gap, prior-generation OFR2 reports, RCP2 checkpoint and FRA2 ACK are not inherited: current-generation frontiers/ACKs are empty, no current checkpoint exists, and origin/tombstone proof queries remain false. Reopen preserves the new roster and the fail-closed horizon.
- [x T] Exact head 631136ebcc36512c62c176bbf4a1a36db3baafbf: Core 678/678 on Linux and Windows; Storage Integration 173/173 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 golden vectors and Fuzz/Lab passed.
- [x C/T] Offline/rejoin generation-gap horizon reset is covered. This is not permission to enable physical GC: the three physical-GC switches remain hard false pending the broader adversarial multi-member/offline/rejoin and external-reference proof matrix.

## 1.36. T-pass: verify composed application-commit lease ordering and writer exclusion — 2026-11-10

- [x T] Added a deterministic coordinator test proving host lease acquisition precedes the provider transaction gate and disposal releases provider before host. A missing host lease fails closed before provider acquisition.
- [x T] While the composed lease is held, a concurrent provider append remains blocked; after lease release it completes and the durable current-state row is visible. The test uses the real StorageDataProvider monitor, not a mock-only lock.
- [x T] Exact head 63df3003ecb26054ddbcf1363d3e9efcda7c2da1: Core 678/678 on Linux and Windows; Storage Integration 174/174 on Linux, macOS and Windows; both Storage array samples passed on all three platforms; v3.1 golden vectors and Fuzz/Lab passed.
- [x C/T] The base coordinator→Node→host→provider lease order and a concurrent local-writer exclusion case are covered. Full multi-party deadlock/fault injection across concurrent receive, FULL_REPLACE, host WAL commit and all three physical-GC callbacks remains open; physical GC stays disabled.


### 1.70 — FLD cross-repository build unblocked; corrected RunBuilder namespace (2026-11-10)

- The repository secret `FLD_DB_READ_TOKEN` is now present and valid: the integration workflow successfully checked out private `smersh1307n2/FLD-DB` branch `fix/crash-consistency-outbox`.
- The first real host-adapter build exposed a compile error in `FldProviderSnapshotPersistence.cs`: it imported `Fld1.Run.Writer`, while `RunBuilder` is declared in `Fld1.Run.Builder`. The FLD-DB branch was corrected in commit `9471656f92cc68b9b5464e9c6fc3af85f7b7a00c`.
- BIN-SYNC core, Windows, fuzz/lab and v3.1 vector jobs passed in the same run; the host-adapter build failed at the above compile error, so integration tests were not reached. A fresh cross-repository run is required against the corrected FLD-DB head before claiming the adapter builds or the end-to-end link works.
- No physical tombstone/Run GC was enabled by this fix.


### 1.71 — First FLD host-adapter compile diagnostics and source corrections (2026-11-10)

- With `FLD_DB_READ_TOKEN` configured, the cross-repository job checked out the private FLD-DB branch successfully. The first real compile exposed four independent source errors in FLD-DB: incorrect `RunBuilder` namespace (fixed in `9471656`), passing `HashSet<ushort>.Add` as a void callback, positional arguments after named `binaryVersion`, and references to a missing `TryFsyncDirectory` helper.
- Corrected the remaining three compile errors in FLD-DB commits `bc605bd`, `bc56f80`, and `4aeb843`: adapt the callback with a void statement lambda, explicitly name optional RunHeader arguments, and use the existing durable `StorageDirectorySync.Flush` primitive for staging-directory durability.
- FLD-DB CI was automatically re-queued for these source changes. The cross-repository BIN-SYNC host-adapter build has not yet been rerun against the corrected FLD-DB head; no integration pass is claimed.


### 1.72 — Host adapter compile passes; draft-origin WAL contract aligned (2026-11-10)

- The cross-repository host adapter now compiles successfully against the current FLD-DB branch. The first adapter test run then exposed a real contract mismatch: FLD local drafts intentionally persist `OriginAuthorEpoch != 0` with `OriginSequence == 0` until BIN-SYNC freezes/publishes the operation, while the GROUP_COMMIT WAL codec incorrectly rejected that state.
- Corrected the FLD GROUP_COMMIT encoder and decoder to permit the documented unpublished-draft state and continue rejecting the invalid sequence-without-epoch state. FLD-DB commit: `398802f`.
- Added an FLD WAL recovery regression test for a local identity draft with an unassigned origin sequence: `881a8f8`.
- Fixed BIN-SYNC host adapter analyzer CA2014 by moving the 16-byte stack buffer outside the loop: `a5f8419`.
- At the same source revision, the host adapter build passed; its tests had 2 failures caused by the WAL draft-origin invariant above. Revalidation against FLD-DB commit `398802f` is pending. The separate durable provider integration workflow passed on Ubuntu; Windows and macOS are still running/pending at the time of this note. No end-to-end FLD/BIN-SYNC pass is claimed yet.


### 1.73 — First green FLD/BIN-SYNC host-adapter roundtrip (2026-11-10)

- Cross-repository workflow run `42859662182` completed successfully against BIN-SYNC commit `a9b9b6a`; the FLD private checkout and host-adapter build both succeeded. The host-adapter test step reports **4 passed, 0 failed**. This includes the two-FLD-database exchange through BIN-SYNC `SyncSession`.
- BIN-SYNC core workflow run `42859677867` passed on Linux and Windows, including signed control-plane vectors and fuzz/lab. Durable provider integration passed on Ubuntu and Windows in run `42859677865`; macOS remained queued when this note was written.
- FLD-DB standalone CI for commit `881a8f8` currently reports failures on Ubuntu and Windows; GitHub's job-log download returned BlobNotFound and no test artifact was published, so the cause is not yet established. This does not negate the passing host-adapter run, which used the FLD branch head available at its checkout time; standalone FLD CI still needs diagnosis and a green run.
- The WAL change permits durable local draft identity (`OriginAuthorEpoch != 0`, `OriginSequence == 0`) and the added recovery regression test is in commit `881a8f8`. Do not declare the complete FLD-DB branch green until its own CI passes. Physical metadata/tombstone GC remains disabled.


### 1.74 — FLD standalone CI diagnosis: failed jobs expose no steps/logs; durable matrix settled (2026-11-11)

- Durable provider integration run `42859677865` is now terminal: Ubuntu and Windows passed; macOS was cancelled. The earlier cross-repository host-adapter run `42859662182` remains green with 4/4 tests, including the two-database SyncSession exchange.
- FLD-DB run `42859425644` for exact source head `881a8f8` reports all three matrix jobs failed within seconds. A subsequent run `42859799559` for the same head reports Ubuntu and Windows failed while macOS remains queued; the overall run is still queued. Its job records expose no step summaries (empty steps), no artifacts were uploaded, and job-log downloads return Azure `BlobNotFound`.
- Earlier FLD run `42857518118` also failed across all three platforms; its job logs likewise return `BlobNotFound` and its artifact collection is empty. The check-run metadata exposes annotation counts but the connected GitHub interface does not permit reading the annotations endpoint. Therefore the evidence does **not** establish a C# compile/test regression, nor does it prove a runner outage; the underlying failure remains unclassified.
- Do not modify FLD source speculatively to chase these opaque check failures. The next diagnostic must obtain the check annotations or a fresh job-log/artifact payload from a completed/retried run. The current `dotnet.yml` already uses `if: always()` artifact upload steps for restore/build/test logs and TRX results, so zero artifacts plus empty steps means those diagnostic steps did not become observable in the returned run data.
- No new FLD source changes are inferred from this CI metadata. Keep the WAL draft-origin contract and regression test intact; keep physical GC disabled. The two-FLD-node roundtrip is proven by the 4/4 adapter run, but restart/reconnect session coverage and a fully green standalone FLD CI remain open.


### 1.75 — Exact-head FLD adapter revalidation and BIN-SYNC workflow green (2026-11-11)

- [x CI] BIN-SYNC workflow run `42861043119` for commit `34cf92248f5d840a6590c5183994ad04b3b16578` completed successfully: Linux core tests **678/678**, Windows core tests **678/678**, signed v3.1 control-plane vectors passed, Fuzz/Lab **31/31**, and all workflow jobs succeeded.
- [x CI] The cross-repository job checked out the exact FLD-DB branch head `881a8f823e597ab83994473e80eb3f8860e42931`, built `FldBinSyncHost` with **0 warnings / 0 errors**, then ran `FldBinSyncHost.Tests`: **4 passed, 0 failed**. This revalidates the unpublished-draft WAL codec and recovery-test commit through the actual private cross-repository build, not a skipped path.
- [x T] The four host tests include durable remote APPLY with duplicate recognition after reopen, local Outbox identity recovery, two independent FLD databases exchanging mutations through paired BIN-SYNC `SyncSession` instances, and distinct record-vs-field tombstone semantics.
- [!] FLD standalone CI remains an execution-environment problem, not a demonstrated source/test failure: on failed Ubuntu/Windows jobs the raw Actions job payload reports `runner_id: 0`, empty `runner_name`, and `steps: []`; the macOS job remains queued. A retry of completed run `42859425644` shows the same pre-run shape. No job log or test artifact exists because no runner step executed. The precise GitHub scheduling/account-side reason is still unavailable from the connected API; do not attribute it to C# code or change FLD source speculatively.
- [~ T] The standalone FLD matrix is still not green. Restart/reconnect of the full paired SyncSession after both stores and Nodes are reopened remains the next integration test. No physical ChangeId/receipt/tombstone/Ghost GC is enabled.


### 1.76 — Full-process restart and bidirectional session resume verified (2026-11-11)

- [x T] Replaced the earlier one-lifetime two-FLD exchange test with `Two_fld_databases_resume_bidirectional_sync_after_full_process_restart`. The test executes two complete rounds against the same durable directories. Round 1 creates both FLD databases, both BIN-SYNC Storage/Metadata stores and both Nodes; after the round every session, Node, provider, metadata store, Storage database and FLD database is disposed. Round 2 reopens both FLD databases, both Storage/Metadata stores, restores both complete Nodes via `StorageNodePersistence.Load`, creates fresh providers/coordinators and fresh transport sessions, then exchanges new changes in both directions.
- [x D] The test now checkpoints each sender's Node after successful outbound ACK as well as after handshake and receiver-side durable APPLY. This captures the sender's persisted stream/ACK progress rather than only the pre-send checkpoint. Each handshake, push and receive wait is bounded to 20 seconds so a protocol regression fails diagnostically instead of hanging CI indefinitely.
- [x CI] Exact BIN-SYNC commit `89447acc14d35a9042d1dd3abe74985a8373cd70`, cross-compiled against exact FLD-DB head `881a8f823e597ab83994473e80eb3f8860e42931`: host adapter build **0 warnings / 0 errors**, integration suite **4/4 passed** (run `42922500684`, job `130200716498`). Linux core suite **678/678** and fuzz/lab **31/31** also passed in that run; Windows core and the duplicate PR workflow were still in progress at the time of this entry.
- [!] FLD standalone workflow retry attempt 3 (`42859425644`) again failed before runner assignment on all three matrix jobs: direct job records still show `runner_id: 0`, empty runner names and empty steps. Logs/artifacts remain unavailable. This confirms the issue is not fixed by rerun; it remains an Actions scheduling/repository-side blocker, not a source-level test result.
- [~ T] This is the first full durable restart/reconnect proof for the FLD↔BIN-SYNC host path. It does not yet cover injected process death between WAL commit, APPLY receipt, Node checkpoint and ACK, nor multi-process FLD access. Continue with crash-boundary/idempotent replay tests before treating the Storage contract as complete. Physical GC remains disabled.


### 1.77 — FLD APPLY crash-boundary recovery and duplicate ACK verified (2026-11-11)

- [x T] Added `Pending_apply_recovers_when_fld_commit_precedes_metadata_receipt_and_ack`. A fault-injection remote-apply store first commits the immutable remote mutation through the real FLD WAL/ApplyJournal, then throws before `StorageSyncCoordinator` can finalize Metadata PendingApply or emit the sender ACK.
- [x T] The test proves the exact intermediate state: FLD contains the value and durable receipt; FLD local Outbox remains empty (no remote echo); Metadata retains one PendingApply intent; receiver Node was checkpointed before provider commit; sender has no ACK and retains the immutable outbound batch.
- [x T] All handles are closed and both FLD/Storage/Metadata/Node stores are reopened. `RecoverPendingApplies` replays the pending intent; FLD recognizes the ChangeId as already committed, so no second mutation is created; Metadata finalizes the receipt. A fresh session then receives the sender's retry as a duplicate, returns the duplicate ACK, and preserves the single value with no Outbox echo.
- [x CI] Exact commit `1d4ce19aa78cf54ce44e7af62f562ccc709c3348`, exact FLD-DB head `881a8f823e597ab83994473e80eb3f8860e42931`: host adapter build **0 warnings / 0 errors**, integration suite **5/5 passed** (run `42923051001`, job `130202508535`). BIN-SYNC Linux and Windows core suites both **678/678**, Fuzz/Lab **31/31**, signed control-plane vectors passed; every job in the push workflow succeeded.
- [!] FLD standalone CI attempt 3 still fails before runner assignment on Ubuntu, Windows and macOS (`runner_id: 0`, empty runner names and no steps/logs). The cross-repository build and tests against the same FLD branch are green, so this standalone Actions issue remains external to the tested source path and must not be "fixed" with speculative code edits.
- [~ T] The FLD-specific crash test injects an exception after the real FLD commit rather than forcibly killing the OS process. It closes/reopens all stores and verifies recovery/duplicate delivery. The generic BIN-SYNC Storage suite separately has process-kill crash-cut coverage; add a process-kill child probe for the FLD adapter path before declaring all crash windows closed. Keep physical GC disabled.


### 1.78 — Durable sender reservation and FLD snapshot draft contract (2026-11-11)

- [!] The first FLD process-kill child probe reached the critical crash cut and recovered the receiver, but replaying from the sender's legacy Node-owned Outbox after restart did not preserve the exact immutable batch reservation. The receiver correctly rejected the regenerated payload because its author cursor/digest chain no longer matched the finalized receipt. This exposed a test-path/contract gap: transport replay must use the durable Metadata peer-stream reservation, not reconstruct an uncheckpointed legacy Node reservation after process death.
- [x D] Updated the FLD process-kill probe to execute the actual metadata-owned publication cutover before sending. The sender now uses `SyncSession.PushBatchAsync(..., IMetadataPeerStateStore, scope)`, which durably reserves the exact encoded batch before transport; the test checks that the reservation survives reopening before retry. This is the production-grade path required for restart-safe replay.
- [!] That cutover exposed a separate FLD bug: `FldProviderSnapshotPersistence` materializes unpublished local identity drafts as Run `FieldWrite` entries with `OriginAuthorEpoch != 0` and `OriginSequence == 0`. `FieldWrite` still enforced the obsolete all-zero/all-nonzero pair invariant, despite the WAL codec already allowing this valid pre-publication state. The cross-repository test failed in `FldDatabase.OpenProviderSnapshotPersisted` before reaching the crash cut.
- [x C] FLD-DB commit `062dc74420a06843a82304556ba8e81806e28e2d` changes the invariant to allow epoch-present/sequence-zero drafts while still rejecting sequence-without-epoch. FLD-DB commit `c27c1d6c464b0a6943616755855d16f1af54d2e2` adds a provider-snapshot regression that opens, persists, closes and reopens the snapshot, asserting the stable ChangeId, author epoch and unassigned sequence survive unchanged.
- [!] The FLD standalone Actions run for the source fix (`42924251276`) again failed before runner allocation on all three matrix jobs (`runner_id: 0`, empty runner names and no steps). The latest FLD test commit is queued for the same standalone workflow. Revalidation must therefore come through the BIN-SYNC private cross-repository build/test job, which checks out the current FLD branch head.
- [~ T] The process-kill test has not yet passed end-to-end with durable sender reservations and the corrected FLD snapshot contract. Do not claim process-kill replay is closed until the updated cross-repository test is green. Existing exception-injection crash-boundary test remains green at **5/5** on BIN-SYNC commit `1d4ce19`; BIN-SYNC core on that commit was **678/678** on Linux and Windows, Fuzz/Lab **31/31**.


### 1.79 — Snapshot verifier contract and expanded cross-repository FLD test gate (2026-11-11)

- [!] The first metadata-owned process-kill probe still stopped before the crash cut: after allowing epoch-present/sequence-zero in `FieldWrite`, the generated overlay Run was rejected by `RunVerify.VerifyProvenance`, which independently enforced the same obsolete paired-zero invariant. This confirms the draft-origin contract must be consistent at construction **and** on-disk validation.
- [x C] FLD-DB commit `09609279729d91f4cedf0dc246484e30b5190ed1` updates provenance verification to allow `OriginAuthorEpoch != 0, OriginSequence == 0`, while still rejecting a nonzero sequence with zero epoch. FLD-DB commit `da089623ba95dcbc5c952c08306b1685c1832256` adds a RunBuilder/RunReader/RunVerify regression for that exact state. The earlier Engine test also verifies snapshot manifest/overlay persistence through close/reopen.
- [x CI] BIN-SYNC commit `d2a507ce3e062db1c2643f1cc37817f611bad03d` expands the private cross-repository job to run FLD-DB's Run, WAL and Engine test projects before the FLD host adapter integration suite. This provides an executable validation path while the standalone FLD Actions jobs remain unassigned; the first run against the latest FLD head is pending.
- [~ T] The process-kill probe now uses durable Metadata-owned outbound reservations so the exact immutable batch can be retried after restart. It has not yet passed with the corrected RunVerify contract; next run must confirm the crash child reaches the post-FLD-commit cut, recovery finalizes PendingApply, and the replay receives a duplicate ACK.


### 1.80 — FLD Run test gate exposed two additional header-layout defects (2026-11-11)

- [!] The expanded cross-repository gate successfully checked out the current FLD branch and built the host adapter, but stopped in `Fld1.Run.Tests` before WAL/Engine or the host integration tests. 15/26 Run tests failed for two independent header-layout reasons; the standalone FLD CI still has no assigned runner, so these defects had not been surfaced by its workflow.
- [x C] Legacy RunBuilder output passed `footerOff` as `provenanceOff` even when no provenance entries existed. `RunHeader.WriteCore` correctly rejected provenance metadata in a legacy header. FLD-DB commit `0be8adfd398e1b90eab548dcdec5f5d140e0f32f` now writes a zero provenance offset for legacy Runs while retaining the physical footer offset for the file layout.
- [x C] Provenance header fields occupy 24 bytes (`off`, `len`, `count`, `entrySize`), but `RunVerify.ProvenanceHeaderBytes` reserved only 20 bytes, causing the last four bytes to be rejected as nonzero reserved padding. FLD-DB commit `4244cd07b841d2ed999f4d18a3881e14b2c2c95a` corrects the validated header span to 24 bytes.
- [~ T] Re-run the complete FLD Run/WAL/Engine test gate and the FLD host process-kill test against these latest FLD commits. Do not consider the process-kill replay proof complete until the sender's durable Metadata reservation is restored, the crash child reaches the post-FLD-commit cut, and the duplicate ACK completes.


### 1.81 — Run provenance and legacy-header validation green; WAL test assertion refreshed (2026-11-11)

- [x CI] The expanded cross-repository gate ran against FLD head `4244cd07b841d2ed999f4d18a3881e14b2c2c95a`. `Fld1.Run.Tests` passed **26/26**, including the new unpublished-draft provenance test and all legacy RunBuilder/reader tests. This validates both fixes: legacy Runs now omit provenance offsets, and provenance Run verification reserves the full 24-byte extension header.
- [!] The next FLD gate stopped in `Fld1.Wal.Tests`: 16/17 passed; one old assertion still expected APPLY_COMMIT body version 3, while the current codec emits version 4 to carry the origin metadata/relay subset contract. FLD-DB commit `f371d0a89d004295529ee5fd9466ce8a5e5f441c` updates that stale version assertion. This is a test expectation correction, not a codec rollback.
- [~ T] The same gate must now complete WAL and Engine tests, then execute the FLD host adapter suite against the latest FLD head. Only after that should the metadata-owned process-kill probe be evaluated again.


### 1.82 — FLD process-kill recovery verified; cross-repository FLD gate fully green (2026-11-11)

- [x T] `Process_kill_after_fld_commit_before_metadata_finalize_recovers_and_duplicate_acks` now launches a child test process, waits until the real FLD remote APPLY has durably committed and the receiver is blocked before Metadata receipt finalization/ACK, then kills the child process tree. The parent reopens both FLD databases, both Storage databases and both persisted Node images; the receiver has one PendingApply intent and the committed value, then `RecoverPendingApplies` finalizes it exactly once. The receiver FLD Outbox remains empty (no remote echo), and the sender's pre-send Node checkpoint still contains its pending local operation.
- [x T] The process-kill probe intentionally verifies recovery after a real OS process kill. Exact duplicate retry/ACK after recovery is separately verified by `Pending_apply_recovers_when_fld_commit_precedes_metadata_receipt_and_ack`, which injects a post-FLD-commit exception while retaining the sender and confirms duplicate recognition and ACK. These two tests cover complementary failure boundaries; the process-kill test does not claim to replay the sender's exact immutable reservation after process death.
- [x CI] BIN-SYNC commit `41e27b20689dbaf4e43ef2ba0c07fb3d0506a6f9`, cross-checked against current FLD branch head `f371d0a89d004295529ee5fd9466ce8a5e5f441c`: host adapter build **0 warnings / 0 errors**; FLD Run **26/26**, WAL **17/17**, Engine **60/60**, and FLD host integration **7/7**. BIN-SYNC Linux **678/678**, Windows **678/678**, Fuzz/Lab **31/31**, signed control-plane vectors passed. Every job in run `42925889902` succeeded.
- [!] Standalone FLD Actions still has a runner-allocation failure (runner id zero/no steps); however the expanded private cross-repository gate now executes the affected FLD Run, WAL and Engine test projects and the real FLD host adapter suite successfully against the same branch. Continue tracking the standalone Actions issue separately; do not block source integration on opaque runner assignment.
- [~ T] The core FLD↔BIN-SYNC path now passes full restart, pending-apply recovery, duplicate replay/ACK and actual process-kill recovery. Remaining before Storage DLL separation: durable metadata-owned sender replay through the full host publication gate (including peer snapshot baseline), multi-process FLD contract and crash tests at every boundary. Physical ChangeId/receipt/tombstone/Ghost GC remains disabled.


### 1.83 — Metadata-owned restart test exposed draft-stamping invariant (2026-11-11)

- [!] The new metadata-owned sender/cursor restart test completed its first exchange, restored both FLD and Metadata stores, then failed when publishing the second local draft. The failure was precise: `FldDatabase.CommitPublishedOrigins` treated `OriginAuthorEpoch != 0, OriginSequence == 0` as an already immutable origin and rejected the assigned sequence.
- [x C] FLD-DB commit `bea7dbb5a1b166d808476fd2a67ce95d5de9daab` now treats only a nonzero OriginSequence as immutable. An epoch-stamped draft can receive its first sequence if the epoch matches; a different epoch still fails closed, and conflicting nonzero sequences remain rejected.
- [x T] FLD-DB commit `c13786161a37389c5d1a1db1265aacfa5ce8cc3c` adds a regression covering first sequence assignment, exact idempotent restamping, conflicting sequence rejection and snapshot persistence after reopen.
- [~ T] Re-run the metadata-owned sender restart test against these commits. It must complete two exchanges across a full process/store reopen and advance the same peer stream from ACK cursor 1 to 2 without losing or restamping the first ChangeId.


### 1.84 — Identity-stamp WAL record was not enabled in the writer allowlist (2026-11-11)

- [!] The latest FLD Engine regression reached the intended publication-stamp path but failed at `WalWriter.Append`: `KindIdentityStamp (0x05)` was already declared, encoded, recognized by WAL recovery and replayed by FLD, yet the writer's accepted-kind allowlist still omitted it. Run **26/26** and WAL **17/17** passed; Engine **60/61** failed only on this new regression.
- [x C] FLD-DB commit `e50157fe687bf9573b47d972e88748fe9c1263f5` adds `KindIdentityStamp` to the WAL writer allowlist. The existing recovery path already decodes and replays this record idempotently; no format/version change is required.
- [~ T] Re-run the FLD Run/WAL/Engine gate and metadata-owned restart integration. The target is Engine **61/61**, host adapter **8/8**, and durable local author cursor **2** after the second post-restart publication.


### 1.85 — Identity-stamp replay must also recognize epoch-only drafts (2026-11-11)

- [!] The metadata-owned restart gate passed Run **26/26** and WAL **17/17**, then Engine **60/61** failed in the new stamping regression. The write-side `CommitPublishedOrigins` now accepted an epoch-only draft, but `ApplyIdentityStampsUnlocked` (also used during WAL replay) still treated any nonzero epoch as immutable and rejected its own identity-stamp WAL record.
- [x C] FLD-DB commit `0f8f9740e51d58088d7b8590435cadd1c8b0e6b8` applies the same rule to WAL replay: a nonzero sequence is immutable; an epoch-only draft may be stamped if its epoch matches; a mismatched epoch or conflicting nonzero sequence is rejected.
- [~ T] Re-run FLD Engine and the metadata-owned two-round restart test. The test now exercises both live stamp application and reopen/replay of the identity-stamp WAL record.


### 1.86 — Metadata-owned sender cursor survives restart; FLD/BIN-SYNC validation green (2026-11-11)

- [x T] `Metadata_owned_sender_reservation_and_peer_cursor_survive_full_restart` uses the real `FldBinSyncHostAdapter.RecoverAndPublishAllAsync` + `PublishReadyOutboxAsync` path. It completes an initial empty-peer baseline, publishes and ACKs the first FLD change, disposes every FLD/Storage/Metadata/Node/provider/coordinator/adapter/session object, reopens both databases and all durable metadata, appends a second local draft, and publishes through the same host gate. Receiver B retains both values; sender A's durable peer stream advances from `AckedCursor=1` to `AckedCursor=2`, with no pending batch reservation remaining.
- [x T] The earlier OS process-kill probe still passes: the child is killed after FLD's remote WAL/ApplyJournal commit but before Metadata receipt finalization/ACK; parent recovery reopens all stores and `RecoverPendingApplies` finalizes the receipt once. The exception-injection test independently verifies duplicate replay/ACK after recovery.
- [x CI] Exact FLD head `0f8f9740e51d58088d7b8590435cadd1c8b0e6b8` validated by BIN-SYNC commit `1a7457895e3e05306fe695e043e371c9ae403d95`: host adapter build **0 warnings / 0 errors**; FLD Run **26/26**, WAL **17/17**, Engine **61/61**, FLD host integration **8/8**. BIN-SYNC Linux **678/678**, Windows **678/678**, Fuzz/Lab **31/31**, signed control-plane vectors passed. Every job in run `42927171426` succeeded.
- [x] The draft-origin contract is now consistent end-to-end: epoch-present/sequence-zero drafts survive WAL, RunBuilder, RunVerify and provider snapshot persistence; publication assigns the first sequence; identity-stamp WAL writes and recovery replay the stamp idempotently.
- [~ T] The first durable metadata-owned sender restart path is now green. Remaining major work is multi-process FLD ownership/locking, process-kill coverage at the remaining WAL/Node/Metadata cut points, and the planned two-DLL separation (Storage DLL independent of FLD Database overlay). Do not enable physical ChangeId/receipt/tombstone/Ghost GC before those invariants are proven.


### 1.87 — Direct-mode FLD cross-process ownership probe (2026-11-11)

- [x C] The architecture explicitly distinguishes direct embedded mode (one owning process; all other direct openers fail before manifest/WAL recovery) from Shared-file mode (one Engine Host process plus multiple IPC/SHM clients). Do not weaken `WalWriterFence` to simulate multi-writer access.
- [x T] Added `FldDatabaseProcessOwnershipTests` to FLD-DB commit `fc205776c19887fa159e034cb1b78f2dbdb9a84c`. A child process opens the real FLD database, commits a value and holds the owner fence; a second process must receive `IOException` when opening the same canonical path. The parent then kills the owner process, reopens the database and verifies the committed value survived abandoned-mutex recovery.
- [~ T] This test proves the direct-mode exclusion/recovery boundary only. It does **not** implement or claim multiple applications sharing the file. Run it through the cross-repository Engine test gate before beginning Shared Engine implementation.
- [ ] Shared Engine phase must implement a single host owner, bootstrap/discovery, versioned local IPC framing, bounded request queues, session-generation fencing, durable request identity/result recovery, read-only snapshot lifetime/EBR and two independent client processes. Storage-only clients must not load Database; FLD clients use the Database facade; no third runtime DLL.


### 1.88 — FLD process-ownership probe aligned to the public Put API (2026-11-11)

- [!] The new cross-process ownership probe was discovered by the expanded gate, but the first compile stopped at its child fixture: it used timestamp parameters from `PutWithIdentity` on the simpler `Put` overload. The Run suite remained **26/26** and WAL **17/17**; Engine tests did not execute.
- [x C] FLD-DB commit `f95eee5270530f6dcbd1ff0a7b968bdac0667903` changes the fixture to the current public `Put(recordId, fieldId, value, keyEpoch)` API. The child still commits a stable value before signalling that it owns the database.
- [~ T] Re-run the direct-mode cross-process ownership test through the Engine gate. Expected behavior: the second opener fails with `IOException`; after the owner process is killed, a new opener recovers and reads the committed value.


### 1.89 — Direct-mode cross-process ownership and abandoned-owner recovery green (2026-11-11)

- [x T] FLD-DB `FldDatabaseProcessOwnershipTests.Second_process_cannot_open_database_owned_by_another_process` passed through the private cross-repository Engine gate. A child process opened the real database, committed a value and held the writer fence; a second process was rejected with `IOException`; after killing the owner process, the parent reopened the database and recovered the committed value.
- [x CI] The job checked out FLD-DB exact head `8d170c1b15bee4de8d8db1439e80d5a6f1982f07` and passed: FLD Run **26/26**, WAL **17/17**, Engine **63/63** (including the cross-process owner probe), host integration **8/8**, build **0 warnings / 0 errors**. BIN-SYNC Linux/Windows **678/678**, Fuzz/Lab **31/31**, signed control-plane vectors passed. Every job in run `42927917206` succeeded.
- [x] Direct embedded mode is now explicitly tested as single-owner across processes. The writer fence remains fail-closed; abandoned owner recovery does not compromise WAL/manifest state.
- [~ T] Shared Engine multi-client access is still unimplemented. The next phase must introduce a single Engine Host process and IPC/SHM clients rather than weakening the direct-mode lock. Keep exactly two runtime DLLs; the host is an executable, not a third library.


### 1.90 — Storage IPC v1 framing foundation (2026-11-11)

- [x C] Added `Fld1.Storage.Ipc.StorageIpcProtocol` inside the existing Storage DLL; no third runtime DLL is introduced. The fixed 32-byte little-endian header carries magic `FLS1`, protocol version, frame type, request id, session generation, bounded payload length and CRC32C. Payload limit is 4 MiB.
- [x C] Transport frame types are `Hello`, `HelloAck`, `Request`, `Response`, `Error` and `Goodbye`. Handshake frames enforce generation zero/nonzero boundaries; request/response frames require nonzero request and session identities. Reads reject partial frames, unsupported versions, invalid lengths and CRC mismatch.
- [x T] Added Storage tests for round-trip framing, handshake/generation boundaries, Castagnoli CRC check vector, corruption, truncation and payload/session limits. FLD-DB commit `de6e18e935b5e835e2c2d50ee58f8cbecf991410` adds the test suite; source protocol is in commit `5db14b5b288da3bb842fa89669ee9e9f6b39904b`.
- [x CI] BIN-SYNC commit `d1b682ec84d89c9ea23a10ace4b2dda10073a0ff` adds `Fld1.Storage.Tests` to the private cross-repository gate before Run/WAL/Engine and FLD host tests. First run against the IPC implementation is pending.
- [~ T] This is only the frame codec foundation. Still required: named-pipe/Unix-domain transport listener/client, host discovery and ownership handoff, bounded per-session request queue, reconnect/session-generation fencing, durable idempotent request ledger, remote write batches, pinned read snapshots and snapshot reclamation/EBR, FLD Database client facade, and the single-owner Host executable. No SHM mapping is claimed yet.


### 1.91 — Storage IPC frame tests added; public API allowlist aligned (2026-11-11)

- [x C] FLD-DB commit `2c88cd531ad8e9698fa0fcf3378d311d88cc56f4` adds the approved `StorageIpcFrame`, `StorageIpcFrameType` and `StorageIpcProtocol` exports and permits the explicit `Fld1.Storage.Ipc` namespace alongside `Fld1.Storage.Engine`. CRC calculation remains an internal helper, not an extra public API.
- [x T] Storage IPC tests cover v1 round-trip, Hello/HelloAck/Goodbye session-generation rules, CRC32C check vector, corrupted payload, truncated payload, oversized payload and invalid request/session identity.
- [x CI] Exact cross-repository gate passed on BIN-SYNC head `0c7efc6be741d287411d951cc3c690710bd5a83d` with FLD-DB branch `fix/crash-consistency-outbox` at `b7f3c3f0b2951bbadba8325f3e6e592f3be07b81`: Storage 44/44, Run 26/26, WAL 17/17, Engine 63/63, `FldBinSyncHost.Tests` 8/8, host build 0 warnings / 0 errors. BIN-SYNC .NET 10, coverage, Fuzz/Lab and v3.1 signed control-plane vector jobs also passed. Workflow runs: `42928689401` and `42928689534`.
- [~ T] Frame codec and cross-repository compatibility gate are complete; shared multi-process Storage access is not. Next implementation phase belongs in the existing FLD Storage DLL (preserve the two-DLL runtime split): named-pipe/Unix-domain listener and client, single-owner Host executable, bounded per-session request queue, host discovery/ownership handoff, reconnect/session-generation fencing, durable idempotent request ledger, remote write batches, pinned read snapshots and snapshot reclamation/EBR, then FLD Database client facade. Add crash/reconnect/concurrency tests at each boundary. Do not claim SHM or multi-client support until implemented and tested.



### 1.92 — First local Storage IPC request/response transport; test analyzer cleanup — 2026-11-11

- [x C] FLD-DB branch `fix/crash-consistency-outbox`, source head `5cdd10f220971ea297afbdc563b4018bf54fd83c`, now contains a first usable local named-pipe IPC server/client in the existing `Fld1.Storage.dll`. The server accepts multiple independent client sessions; each session receives a fresh nonzero generation and enforces strictly increasing request IDs. The client serializes requests, validates response identity/generation, keeps the session usable after a handler-level remote error, and faults/disposes the connection after ambiguous transport/protocol failures. The request handler remains opaque to Storage.
- [x T] `StorageIpcConnectionTests` cover client/server round-trip, remote handler failure followed by a successful request on the same session, and concurrent independent clients with distinct client IDs/session generations. The exact cross-repository run for BIN-SYNC PR head `fa12aed2ab4dd8c13c380a077e19848939934f98` checked out FLD head `5cdd10f220971ea297afbdc563b4018bf54fd83c`: Storage **46/46**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **8/8**, host build **0 warnings / 0 errors**. BIN-SYNC Linux/Windows, Fuzz/Lab, signed vectors and the three-platform Storage Integration matrix all passed (runs `42931117608` and `42931117583`).
- [!] The Storage test compile emitted four xUnit1031 analyzer warnings because the multi-client test read completed tasks through `.Result`. FLD-DB commit `26000817a4fad58836a3b614e15de27c920924ac` replaces those blocking reads with awaited results; exact-head revalidation is pending.
- [~ T] This is transport plumbing, not the Shared Engine contract. Still open: bounded server-wide/per-session admission and request queues; cancellation/shutdown behavior under many clients; durable idempotency ledger keyed by stable ClientId/request identity with result recovery; reconnect/replay semantics across host restart; endpoint discovery and single-owner Host lifecycle; Unix-domain socket behavior explicitly tested on Unix (the current implementation uses the .NET named-pipe abstraction); Storage operation DTO/dispatch and bounded remote write batches; pinned read snapshots and EBR/reclamation; FLD Database client facade; two independent client processes exercising real database reads/writes. Do not infer these features from the generic echo handler tests.
- [~ T] Next: rerun the exact cross-repository gate against FLD head `26000817a4fad58836a3b614e15de27c920924ac`; then add bounded admission and cancellation/host-shutdown tests before introducing durable request-result replay. Keep the two-runtime-DLL rule: Storage and Database only; Host remains an executable. Physical GC remains disabled.



### 1.93 — Bound Storage IPC client admission and reject overload before session allocation — 2026-11-11

- [x C] FLD-DB commit `42248b527100e6d0d400c30b05b8c80aeb69042c` adds a configurable `maxConcurrentClients` limit (default 64, validated 1..254) to `StorageIpcServer`. Admission is non-queued: when all slots are occupied, the newly accepted pipe is closed before a session generation or handler task is allocated. Active sessions release their slot on every handler exit path. This prevents an unbounded client-task population and avoids depending on the OS named-pipe instance ceiling as the application admission policy.
- [x T] FLD-DB commits `741b4554e242c5f449a22d1863f2cba175e3dcc9`, `47d313bcc844bcb1b7e818939f92df17d27401cf`, `bb1abaa1a82526436fc07ce1aedcb3d7016abed3`, `ef5efe436f3092c7f0f838469db8b4d16c773c77` and `7307a9f4c27c0d21d44886aa98c41e14bdfd3efd` add saturation, slot-reuse and lifecycle coverage with a one-client limit: the first client remains usable, a second client is rejected during handshake, the slot is reclaimed after disconnect (with bounded retry for asynchronous peer-close observation), a replacement client is admitted, and the server disposes its admission semaphore after all handlers exit.
- [~ CI] Re-run the BIN-SYNC cross-repository gate against FLD head `7307a9f4c27c0d21d44886aa98c41e14bdfd3efd`; verify Storage test analyzer warnings are gone and the new admission test passes. The previous exact gate at FLD `5cdd10f220971ea297afbdc563b4018bf54fd83c` passed Storage 46/46, Run 26/26, WAL 17/17, Engine 63/63 and host adapter 8/8.
- [~ T] Admission is now bounded at the connection/session level, while requests within one session remain serialized (one in-flight request, no unbounded per-session request queue). Still open: explicit cancellation/shutdown stress with many clients; durable idempotency/result ledger; reconnect/replay across host restart; endpoint discovery and single-owner Host lifecycle; Unix-specific transport tests; Storage operation DTO/dispatch and bounded write batches; pinned snapshots/EBR; FLD Database client facade; and real two-process database read/write tests. Do not represent generic IPC echo as Shared Engine completion.
- [~ T] Continue with bounded shutdown/cancellation stress and session-generation fencing tests, then design the durable request identity/result ledger before adding operation dispatch. Preserve exactly two runtime DLLs; Host remains an executable. Physical GC remains disabled.



### 1.94 — Exercise the local IPC transport against a real FLD database from the BIN-SYNC host gate — 2026-11-11

- [x T] Added `samples/FldBinSyncHost.Tests/StorageIpcHostCompositionTests.cs` in BIN-SYNC. The test opens a real `FldDatabase`, starts the Storage IPC server with a host-owned request handler, sends a bounded write request through `StorageIpcClientConnection`, commits via the handler, and verifies the value through FLD's read API. The server is cancelled and awaited in a `finally` path so assertion failures cannot leave a listener running.
- [~ CI] BIN-SYNC PR head `b66f1771d6896501a2222d72ce1244aadefd954e` triggered the .NET and three-platform Storage Integration workflows (runs `42932471279` and `42932471238`). The exact cross-repository job result against the latest FLD head must be read before marking this test green.
- [~ T] This is an application-owned handler integration test, not yet the production FLD Database IPC facade or the single-owner Engine Host. Next validation must include the current FLD source head, bounded admission/slot reuse, cancellation under concurrent clients, then durable request identity/result replay. Keep the existing two-DLL and no-physical-GC constraints.



### 1.95 — Make IPC overload rejection assertion portable across Unix and Windows — 2026-11-11

- [!] The first exact cross-repository run containing the admission-limit test checked out FLD-DB head `741b4554e242c5f449a22d1863f2cba175e3dcc9`. Storage compiled, but **46/47** tests passed: on the Unix runner, the intentionally rejected second pipe surfaced as `IOException: Connection reset by peer`, while the test asserted an exact `InvalidDataException`. This is the expected fail-closed transport rejection; the platform exposes the disconnect differently. The cross-repository job stopped at Storage tests, so Run/WAL/Engine/host adapter were not executed in that attempt.
- [x T] FLD-DB commit `42ec3da498dc7977a64e1c7eecadd6383d3930bf` changes the test to accept either `IOException` or `InvalidDataException`, while still requiring the connection attempt to fail. It does not weaken the server's rejection behavior.
- [~ CI] Re-run the full BIN-SYNC cross-repository gate against FLD head `42ec3da498dc7977a64e1c7eecadd6383d3930bf`. Required: Storage **47/47**, analyzer warnings absent, slot reuse succeeds, then Run/WAL/Engine and the new BIN-SYNC real-FLD IPC host-composition test pass.
- [~ T] The real-FLD host-composition test added in BIN-SYNC §1.94 has not yet been executed: the preceding cross-repository gate stopped in Storage before reaching `FldBinSyncHost.Tests`. Do not mark the IPC-to-FLD durable-write path complete until this exact gate passes.



### 1.96 — Extend the host integration probe across overload recovery — 2026-11-11

- [x T] BIN-SYNC commit `182e28ec847397817bdcb9bb91078ba418c592ac` extends `StorageIpcHostCompositionTests`: after a second client is rejected at the one-client admission limit, the admitted client sends a second request on the same session and commits a second field to the real FLD database. The test verifies both durable values after server shutdown. This proves overload rejection does not poison the admitted session's request/response stream.
- [~ CI] The new BIN-SYNC test and latest FLD source/test commits require a fresh exact-head cross-repository run; do not infer success from the earlier run that stopped at Storage 46/47.



### 1.97 — Fix host integration test namespace collision; Storage IPC gate passes 47/47 — 2026-11-11

- [x CI] Cross-repository run `42933192110` checked out FLD-DB head `7307a9f4c27c0d21d44886aa98c41e14bdfd3efd`. Host adapter build succeeded with **0 warnings / 0 errors**. FLD Storage passed **47/47** (including portable overload rejection and bounded slot reuse), Run **26/26**, WAL **17/17**, Engine **63/63**. The earlier four xUnit1031 warnings in `StorageIpcConnectionTests` are gone.
- [!] The same run then stopped compiling the new BIN-SYNC host-composition test: in namespace `BinSync.FldHost.Tests`, `Record` resolves to the BIN-SYNC `Record` model, so `Record.ExceptionAsync` is not the xUnit helper. The FLD storage suite and all preceding projects had already passed; `FldBinSyncHost.Tests` did not execute.
- [x C] BIN-SYNC commit `a4456989b5888c6739584c16a9cb3ddde0491212` replaces the ambiguous helper call with `Assert.ThrowsAnyAsync<Exception>` plus an explicit transport-exception type assertion (`IOException` or `InvalidDataException`). The overload rejection remains required; arbitrary exceptions do not satisfy the assertion.
- [~ CI] Re-run the exact cross-repository gate against FLD head `7307a9f4c27c0d21d44886aa98c41e14bdfd3efd` and BIN-SYNC head containing `a4456989b5888c6739584c16a9cb3ddde0491212`. Required: the new `FldBinSyncHost.Tests` real-database IPC write/overload/session-continuation test passes.



### 1.98 — Exact cross-repository IPC host integration gate green — 2026-11-11

- [x CI] BIN-SYNC run `42933464398` validated PR head `c811a7b5d38b42b9c8bfb4a790ddf42c56a20d08` against exact FLD-DB head `7307a9f4c27c0d21d44886aa98c41e14bdfd3efd`. Host adapter build: **0 warnings / 0 errors**. FLD Storage **47/47**, Run **26/26**, WAL **17/17**, Engine **63/63**, and `FldBinSyncHost.Tests` **9/9** all passed.
- [x T] The ninth host integration test is `Local_ipc_request_commits_through_host_handler_into_real_fld_database`: it confirms a real FLD write through the IPC request handler, rejects an excess client at the configured admission limit, then successfully sends a second write on the still-live admitted session. Both values are read back from FLD after the server has shut down.
- [x CI] BIN-SYNC core passed **678/678** on Linux and **678/678** on Windows; v3.1 signed control-plane vectors and Fuzz/Lab passed in the same run. The Storage Integration matrix passed on Ubuntu and macOS; Windows was still running at the time this entry was written.
- [!] Two existing xUnit1031 blocking-task analyzer warnings remain in `Fld1.Engine.Tests/SyncApplyTests.cs` (lines 467 and 475). They are outside the new Storage IPC tests; no test failed. Keep them tracked for cleanup, but do not confuse them with the four IPC-test warnings already removed.
- [~ T] The local transport plus one real host-owned FLD write path is now cross-repository verified. Still not complete: production operation dispatcher/DTO, single-owner Host executable and discovery, durable idempotency/result ledger, restart-safe request replay, explicit Unix-domain transport tests, high-concurrency cancellation/shutdown stress, pinned snapshot/EBR IPC and FLD Database client facade. No SHM or full multi-process Shared Engine claim yet.



### 1.99 — Test real reconnect generation fencing and orderly multi-client shutdown — 2026-11-11

- [x T] FLD-DB commit `034578136d7def36d2ae964236604999c9013d19` adds Storage tests for orderly server cancellation with eight admitted client sessions and for rejecting a request that presents a previous connection's session generation after reconnecting with the same stable ClientId. The stale-generation request must be closed before the request handler is invoked.
- [x T] BIN-SYNC commit `cbf6a449a009bc81865e3e62a34e2619749077e0` extends the host integration test to perform the same reconnect scenario against a real FLD database and assert that no handler call or database mutation occurs when the old generation is replayed.
- [x C] FLD-DB commit `e6802c4e2f666a13869dbba58030f7669b6c24f4` ensures the database is disposed before temporary-directory cleanup in the new host test, including Windows file-handle semantics.
- [~ CI] Re-run the exact cross-repository gate against FLD head `4a254a93500b777be6c673a062b3f297dd2fe1df` and BIN-SYNC head containing `cbf6a449a009bc81865e3e62a34e2619749077e0`. Required: Storage shutdown/fencing tests and the real-FLD stale-generation integration test pass on the CI platform; the existing three-platform Storage Integration matrix must remain green.
- [~ T] These tests cover session-generation fencing and bounded shutdown, not durable request idempotency. The next architectural item remains the durable request identity/result ledger and host restart replay contract, followed by the single-owner Host/discovery and FLD Database facade.



### 1.100 — Expose per-session request correlation to the IPC host handler — 2026-11-11

- [x C] FLD-DB commits `861ea7c92449374f7ab1280855560aa6eabef6bd` and `31c84462f1f2b8969aab3ff2d60c73a371f74e4d` extend `StorageIpcRequestContext` with the frame's `RequestId`. The XML contract explicitly states this value is only a per-session transport correlation id; it resets after reconnect and must not be used alone as the durable idempotency key.
- [x T] FLD-DB commit `47c509ed273f4fe492dd4635be1d332cffaed280` verifies the handler receives request IDs 1, 2 in order on one connection. BIN-SYNC commit `050fac24f4e41389946069c6a29392f9d04ee838` extends the real-FLD host integration test to assert that the handler sees those exact IDs across the two durable writes.
- [~ CI] Re-run against FLD head `47c509ed273f4fe492dd4635be1d332cffaed280` and the latest BIN-SYNC PR head. After this test is green, define a separate stable operation/idempotency identity in the host request envelope; do not conflate it with the session-local frame RequestId.
- [~ T] Durable exactly-once effects require the host to persist a stable operation identity and request fingerprint with the operation's durable result (atomically with Storage mutations, or via FLD's existing durable apply receipt for Database operations). A retry after reconnect must reuse that same operation identity and identical request fingerprint; conflicting reuse must fail closed. This is the next design/implementation item.



### 1.101 — Reconnect fencing, shutdown, and request correlation pass the full cross-repository gate — 2026-11-11

- [x CI] BIN-SYNC run `42934307412` validated §1.99: FLD Storage **49/49**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **10/10**; BIN-SYNC Linux/Windows **678/678**, Fuzz/Lab and signed vectors passed. The Storage Integration matrix passed on Ubuntu, macOS and Windows.
- [x CI] BIN-SYNC run `42934655761` validated §1.100 against the updated FLD IPC request-context contract: FLD Storage **50/50**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **10/10**; host build **0 warnings / 0 errors**. BIN-SYNC Linux/Windows **678/678**, Fuzz/Lab and signed control-plane vectors passed. Storage Integration run `42934655730` passed on all three platforms.
- [x T] The new handler correlation assertion passed: two writes on one live session reach the FLD host handler with request IDs 1 then 2. The IDs are explicitly transport-scoped; they are not reused as durable operation identities.
- [!] Two existing xUnit1031 warnings remain in `Fld1.Engine.Tests/SyncApplyTests.cs`; the Storage IPC tests compile without their earlier blocking-task warnings.
- [~ T] Session lifecycle and correlation are now tested through the cross-repository gate. Next: specify the durable operation envelope and idempotency ledger contract (stable OperationId + canonical request fingerprint + durable result), then implement crash-safe replay without conflating OperationId with session RequestId.



### 1.102 — Define stable OperationId separately from transport RequestId

- [x C] FLD-DB commit `c1d2be6b2b7862be61c2e62a990dd58b548ad4af` adds `StorageIpcOperationEnvelope` and `StorageIpcOperationProtocol` in the existing Storage DLL. The canonical 28-byte little-endian envelope header carries magic `SOP1`, version, nonzero operation code, a nonempty RFC-4122/network-byte-order 128-bit OperationId, and bounded payload length. The operation payload remains opaque to Storage; the outer IPC frame CRC32C covers the complete envelope.
- [x T] FLD-DB commit `aab340416683de3d9c3ed53f2a413a945f62d7b4` adds round-trip/canonical-GUID and malformed-header/length tests. FLD-DB commit `a4363a8c22b17bf7c362d283b5255a4cb6c9200b` updates the strict Storage public API allowlist for the two new approved types.
- [x T] BIN-SYNC commit `e2b9b77248c04e7d18e34cdfa715b2a8415e4054` updates the real-FLD host integration probe to wrap writes in stable OperationIds, decode the envelope in the host handler, and assert both operation IDs reach the database adapter while transport RequestIds remain 1 and 2.
- [~ CI] Re-run the exact cross-repository gate against FLD head `a4363a8c22b17bf7c362d283b5255a4cb6c9200b` and the latest BIN-SYNC PR head. Required: public API allowlist, envelope codec tests, and real FLD write-through-envelope integration all pass.
- [~ T] The envelope establishes stable identity but does not itself provide deduplication. Next implement the durable result ledger: key by stable ClientId + OperationId; store a canonical SHA-256 fingerprint over operation code and payload; exact replay returns the stored result; reusing an OperationId with a different fingerprint fails closed. Storage-only mutations must commit their data and result row atomically in one StorageWriteBatch. FLD Database operations must use the same OperationId as the durable FLD apply identity/receipt so a crash after the FLD WAL commit but before ledger finalization can safely replay and recover the original result. Never key durable idempotency by the session-local RequestId.



### 1.103 — Implement atomic durable result ledger for Storage-backed IPC operations

- [x C] FLD-DB commit `c562cfdc2c2ed3e614b82c3f4b6be7ea41b72e59` adds `StorageIpcOperationLedger` in the existing Storage DLL. Ledger identity is stable `ClientId + OperationId`; each row stores a SHA-256 request fingerprint and the exact result bytes. The fingerprint is domain-separated and covers operation code plus canonical payload, not the transport RequestId.
- [x C] The ledger exposes a read/replay path and `TryStageResult` against the caller's existing `StorageWriteBatch`. The caller stages all operation data mutations and the completed result row in the same batch, then commits once. Exact replay returns the prior result without staging a second commit; reuse of the same identity with a different fingerprint fails closed. A concurrent race is fenced by StorageDatabase CommitSeq; after a write conflict the host re-reads the ledger.
- [x T] FLD-DB commit `1bad945a59247f7f7703b411eb925ee74021e265` adds tests for durable replay after reopen, conflict rejection and the crash cut after WAL flush but before in-memory publish. Recovery must expose both the data mutation and result row together.
- [x T] BIN-SYNC commit `45db17c771f78d79d430c4d24ecd92caf5613ec2` adds a cross-repository host test for the ledger's public contract: data and result commit together, the exact result is replayed after Storage reopen, and conflicting OperationId reuse is rejected.
- [~ CI] Re-run the full cross-repository gate against FLD head `1bad945a59247f7f7703b411eb925ee74021e265` and the latest BIN-SYNC PR head. Required: the atomic WAL crash-recovery test and the BIN-SYNC ledger reopen/replay test pass.
- [~ T] This ledger currently provides atomic exactly-once effects for mutations performed through the same StorageWriteBatch. It does not yet make FLD Database WAL mutations atomic with the Storage ledger. For Database operations, the host must use a durable FLD ApplyJournal/receipt keyed by the same OperationId, then finalize the Storage-side result record; crash between the two stores must replay the FLD operation idempotently and recover its original receipt. Implement and test that cross-WAL recovery adapter before advertising generic FLD Database IPC idempotency.



### 1.104 — Enforce same-database atomicity for the operation ledger

- [x C] FLD-DB commits `18296f5e78c4e37470d82e3c6909b37ab5f5dace` and `dfa0741fb105aa2fc5b4bb14fc9d1d7bba49612d` add an internal StorageWriteBatch ownership check and make `StorageIpcOperationLedger.TryStageResult` reject any batch not created by the exact StorageDatabase instance that owns the ledger. This prevents a host from accidentally persisting the result row in one database while committing the data mutation in another.
- [x T] FLD-DB commit `2e378a21ed3ccb61fe77e0f9a5c15beb849bac8c` tests foreign-database batch rejection. BIN-SYNC commit `feb660bebd68dc14f4434abcddc88ed3e08d747a` adds the same ownership assertion to the cross-repository host integration test.
- [~ CI] Re-run the full cross-repository gate against FLD head `2e378a21ed3ccb61fe77e0f9a5c15beb849bac8c` and BIN-SYNC head containing `feb660bebd68dc14f4434abcddc88ed3e08d747a`. Required: atomic ledger/recovery, wrong-store rejection, and real host tests all pass.



### 1.105 — Durable Storage IPC operation ledger and same-database guard pass CI

- [x CI] BIN-SYNC run `42936501802` checked the operation-ledger implementation and cross-repository host adapter: FLD Storage **57/57**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **11/11**; host build **0 warnings / 0 errors**. BIN-SYNC Linux/Windows tests, Fuzz/Lab and signed vectors passed. Storage Integration run `42936501809` passed on Ubuntu, macOS and Windows.
- [x T] The gate covers stable envelope encoding, durable result replay after reopen, conflicting OperationId rejection, WAL-flush-before-publish recovery, and rejection of a ledger batch from a different StorageDatabase. The cross-repository sample exercises the same public ledger API.
- [!] Two existing xUnit1031 blocking-task warnings remain in `Fld1.Engine.Tests/SyncApplyTests.cs`; no IPC or ledger test warnings remain.
- [~ T] The Storage-only exactly-once transaction contract is now validated. The next test targets concurrent duplicate operations that both begin before either commits; Storage CommitSeq must allow one atomic winner and force the other to re-read the durable result after conflict.


### 1.106 — Concurrent duplicate operation commit fence

- [x T] FLD-DB commit `9f15c05bfb8690a53e321cfab9de485efe97387e` adds a race test with two write batches created at the same base CommitSeq. Both stage the same ClientId + OperationId + fingerprint and different data mutations; the first commits, the second must fail with `StorageConflictException`, and only the first mutation plus one durable result may remain.
- [x T] BIN-SYNC commit `cbe6c6c22926447b3914eef04bb8ab6a4341e874` mirrors the concurrent duplicate-operation contract in the cross-repository host test project.
- [~ CI] Re-run the exact cross-repository gate against FLD head `9f15c05bfb8690a53e321cfab9de485efe97387e` and BIN-SYNC head containing `cbe6c6c22926447b3914eef04bb8ab6a4341e874`.



### 1.107 — Concurrent duplicate operation commit fence passes the cross-repository gate

- [x CI] BIN-SYNC run `42936934333` validated the same-CommitSeq duplicate-operation race: FLD Storage **58/58**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **12/12**; host build **0 warnings / 0 errors**. BIN-SYNC core passed **678/678** on Linux and **678/678** on Windows; Fuzz/Lab and signed vectors passed.
- [x CI] Storage Integration run `42936934399` passed on Ubuntu, Windows and macOS.
- [x T] The race test proves two batches may stage the same identity from the same base CommitSeq, but only one can commit. The loser receives `StorageConflictException`; its distinct data mutation is absent, and the durable winning result is available for replay. The BIN-SYNC sample repeats this public contract.
- [~ T] Storage-only operation idempotency is now covered for exact replay, conflicting fingerprint, process-reopen recovery, WAL-flush crash recovery, wrong-store batch rejection and concurrent duplicate commits. Next: wire the ledger into an IPC handler and simulate a committed operation whose response is lost during server shutdown, then reconnect with the same ClientId + OperationId and prove the durable result is replayed without a second commit.



### 1.108 — End-to-end lost-response recovery across IPC host restart

- [x T] BIN-SYNC commit `17b634ee82ecb2e3ec2f87b99e39c75d34b51b1d` adds an end-to-end Storage IPC test that commits a StorageWriteBatch containing both a data mutation and the durable operation result, then cancels the server before it can send the response. The client observes transport loss, the StorageDatabase is closed/reopened, and a new server accepts the same stable ClientId + OperationId. The retry returns the exact stored result and proves LastCommitSeq remains 1 with only one data effect.
- [~ CI] Re-run the exact cross-repository gate against FLD head `2e378a21ed3ccb61fe77e0f9a5c15beb849bac8c` and BIN-SYNC head containing `17b634ee82ecb2e3ec2f87b99e39c75d34b51b1d`.
- [~ T] This validates the Storage-only request ledger across a lost response and server restart. It does not yet prove the separate FLD Database WAL path; that still needs a database-native durable operation receipt tied to the same OperationId.



### 1.109 — Lost-response replay after Storage host restart passes; clarify FLD local-WAL receipt boundary

- [x CI] BIN-SYNC run `42937446009`: FLD Storage **58/58**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **13/13**; host build **0 warnings / 0 errors**. BIN-SYNC run `42937446009` core jobs passed Linux/Windows **678/678**, Fuzz/Lab and signed vectors. Storage Integration run `42937445930` passed on Ubuntu, Windows and macOS.
- [x T] The new end-to-end host test commits the data mutation and result row together, deliberately cancels the server before the response frame is written, closes/reopens StorageDatabase, reconnects with the same ClientId + OperationId, and receives the original result. LastCommitSeq remains 1 and the data effect exists exactly once.
- [!] Architecture correction to §1.103: do **not** reuse FLD's existing `ApplyJournal` as the receipt for local Database IPC writes. Source audit shows it is keyed by remote issuer/epoch/batch identity, and `ApplyRemoteBatch` intentionally does not create local-author Outbox entries. Reusing it for local writes would cross the remote/local authority boundary and lose the required Outbox semantics.
- [~ C] The FLD Database IPC path therefore needs a database-native local operation receipt committed atomically with the local field mutations and their Outbox entries in the same FLD WAL transaction (extend the local commit record or add a dedicated local-operation commit record). Recovery must rebuild the receipt index from that WAL record. A retry with the same ClientId + OperationId + fingerprint returns the original result; conflicting reuse fails closed. The receipt must not be discarded by ordinary Outbox ACK/compaction while the operation retry horizon remains active.
- [~ T] The generic StorageDatabase ledger is complete for StorageWriteBatch-backed operations. The next FLD Database phase is this local WAL receipt contract and crash matrix, not an ApplyJournal adapter. Keep one source of truth and avoid a second pending journal.



### 1.110 — End-to-end durable Storage IPC retry passes all platforms

- [x CI] BIN-SYNC run `42937446009`: FLD Storage **58/58**, Run **26/26**, WAL **17/17**, Engine **63/63**, `FldBinSyncHost.Tests` **13/13**; host build **0 warnings / 0 errors**. BIN-SYNC core **678/678** on Linux and Windows, Fuzz/Lab and signed vectors passed. Storage Integration run `42937445930` passed on Ubuntu, Windows and macOS.
- [x T] Lost-response restart replay is now verified end-to-end over the actual IPC transport and reopened StorageDatabase: same stable ClientId + OperationId returns the durable prior result and does not advance CommitSeq or repeat the data effect.
- [~ C] Next database-specific work: add a local-operation receipt to FLD's existing GROUP_COMMIT WAL payload (backward-compatible body version bump), so local field mutations, Outbox publication entries and the stable OperationId/fingerprint/result become one durable WAL unit. Recovery rebuilds the receipt index from the same WAL; ordinary Outbox ACK/compaction must not erase receipt history inside the retry horizon. A single-owner host write dispatcher serializes FLD overlay mutations. Do not reuse remote ApplyJournal for local writes.



### 1.111 — Add the Storage-only IPC operation dispatcher

- [x C] FLD-DB commit `7723d641b12c54003e2a4bbae5458213b4d83769` adds `StorageIpcOperationDispatcher` and operation codes GET/PUT/DELETE in the existing Storage DLL. GET returns a bounded status/length/value response. PUT and DELETE use the stable OperationEnvelope, the durable result ledger, and the same StorageWriteBatch as the data mutation; optimistic commit conflicts re-read the ledger or retry up to a bounded limit. Read responses are capped to the outer frame capacity; oversized values require a future paged-read operation.
- [x T] FLD-DB commits `4ada5b18001018e1f9d04256eccd09b43ea35bff`, `e42392760a26b84524a431e7b7919e2205984fbb` and `9f7e4263c3158ca951c3aa6c1548581c5a359424` add frame-bound checks, strict public API allowlisting, and dispatcher tests for PUT/GET/DELETE, exact replay, conflicting identity, malformed payload and unknown operation codes.
- [x T] BIN-SYNC commit `c35898dbf48960b34f544b27ece02269a6d6e733` adds an actual StorageIpcServer → StorageIpcOperationDispatcher → StorageDatabase test, including reconnect replay of PUT, GET readback and DELETE.
- [~ CI] Re-run the cross-repository gate against FLD head `9f7e4263c3158ca951c3aa6c1548581c5a359424` and BIN-SYNC head containing `c35898dbf48960b34f544b27ece02269a6d6e733`.
- [~ T] The dispatcher is a library component, not yet the standalone single-owner Host executable. Next: create the Host executable with canonical endpoint naming, exclusive StorageDatabase ownership, orderly Ctrl+C/shutdown and a discovery/owner record; then exercise two independent client processes.



### 1.112 — Standalone Storage Host executable and endpoint discovery

- [x C] FLD-DB commits `c57c7bcf16cdb8cc6819ff841aff213ae6148346`, `1b31805990e17c065b2c4f6a76b21569d52a9c1b` and `387a7922e399450acc096d18c502cf496c8bcc02` add `Fld1.Storage.Host` as a separate executable project and include it in `Fld.Db.slnx`. It references only `Fld1.Storage.dll`, owns one StorageDatabase partition lease, runs the operation dispatcher and handles Ctrl+C shutdown. It does not load `Fld1.Database.dll`.
- [x C] FLD-DB commits `d480e2caee28fe56a1bbcb9696764aa408032b57`, `1253785560b42abf53e939cd43716649d51ba96a` and `0bf96c28141359555a88613c4f28e78df06b0b04` add deterministic pipe naming (normalized root + partition + OS user), an atomic owner-published endpoint record, owner-safe endpoint deletion, a server Ready signal, and client `ConnectToDatabaseAsync`.
- [x T] FLD-DB commits `b5a81ac764d088ecb5238ac8f4d6026fee3e733e`, `a4fd57022e30e6cdf55c3dfbcd857db9a4082984`, `60c2e5325214e75713f29189feb70d202316ed16`, `5bfbbde9edf7c1d7fdbf339f3ac6b88fde1d7f2d` and `47227e780c2160255a0acafc3976bc0afc0228c1` add endpoint validation and a real process test: start Host, connect via endpoint, reject a competing Host for the same partition, kill/restart the owner, replay the same operation, then reopen StorageDatabase and verify one durable commit.
- [x T] BIN-SYNC commit `887340a043fdb44420ace263345a42b6540a0903` updates the cross-repository dispatcher integration probe to discover and connect through the published endpoint rather than a hard-coded pipe name.
- [~ CI] Re-run the exact cross-repository gate against the latest FLD branch and BIN-SYNC PR head. Required: Host executable builds with warnings-as-errors, endpoint tests pass, and the multi-process owner/restart test succeeds on Linux, Windows and macOS.
- [~ T] The Storage-only Host is now implemented as an executable, but the multi-process test must pass before considering ownership/discovery complete. The FLD Database facade remains a separate later phase and must not be loaded by Storage-only clients.



### 1.113 — Partition-scope endpoint records to support multiple partitions per root

- [x C] FLD-DB commit `c2f9aff0b41c709ca9951cb4e627372e2f2f75bb` changes the discovery filename to `.fld1-storage-ipc-{PartitionId:X16}.endpoint`. The deterministic pipe name was already partition-scoped; the endpoint file now is too, so independent partitions in one root cannot overwrite each other's discovery record.
- [x C] FLD-DB commit `c6501f85dc12fd15d6a8ddda186c8b963ce07a2b` updates Host cleanup to delete only its exact partition endpoint. Tests `5a1c59e021436092f691d0e70a115a0085b08770` and `c626d8e1bd49ec86d0c0fcfc9ee9237862a0f2cb` cover same-root partition isolation and process-host endpoint lookup.
- [x T] BIN-SYNC commit `77feb94a24abe795ab2057e3eab3b8e1a8c13740` updates the host composition probe to use partition-specific endpoint deletion.
- [~ CI] Re-run the full cross-repository and three-platform Storage Integration gates against FLD head `c626d8e1bd49ec86d0c0fcfc9ee9237862a0f2cb` and the latest BIN-SYNC PR head.



### 1.114 — Fix endpoint isolation and stale-record race found by process CI

- [!] First process-host gate `42939569291` exposed two test defects, not Storage data corruption: the wrong-partition lookup now correctly returns `FileNotFoundException` because each partition has a distinct endpoint filename; the restart poll had accepted the old endpoint file before the new owner atomically replaced it. The process test now waits for a different OwnerId. An xUnit2009 assertion warning was also removed.
- [x T] FLD-DB commit `90819636a2af381a799b5d1247988f1483848abb` fixes the partition endpoint expectation and analyzer warning. FLD-DB commit `52ba42fd32e109bb858e1943cb3e7eab5f4ba726` fixes the process restart poll to wait for the new owner's endpoint record.
- [x T] BIN-SYNC commit `04f788cf8a051ecbfb9b0d0529c8f129f62a7ca4` asserts the public endpoint path includes the exact partition identifier.
- [~ CI] Re-run against the corrected FLD Storage tests and the latest BIN-SYNC PR head. Required: process restart must observe a new OwnerId, second Host must be rejected while the first owns the partition, and CommitSeq must remain 1 after replay.



### 1.115 — FLD local-operation receipt in GROUP_COMMIT and checkpoint recovery

- [x C] FLD-DB commits `58a2b947b43bc19177f882370b5e7ea37cd88248`, `ed32cfe4cc972a74348a234bdb6e98aa7b2a96da` and `30fbed6370e50fd9beb0c4d8573ca6e0993b67a3` add GROUP_COMMIT body v4. It carries a namespace-scoped stable ClientId + OperationId, 32-byte canonical request fingerprint and opaque result alongside the field mutations in the same WAL record. v1-v3 decode remains supported; malformed and duplicate receipt identities fail closed.
- [x C] FLD-DB commits `3c7c61d7b8e7901213eb0be2e9244eaabafec13f`, `481eeb4079e963b49782393f7e0a25cde1633da8` and `773d3cda1b5f6b70964a8e81b569335dc46acae0` add `FldDatabase.CommitWithReceipt` and `TryGetLocalOperationReceipt`. The API returns the original durable result for an exact retry, rejects fingerprint reuse, and commits receipt-only operations without fabricating Outbox entries.
- [x C] FLD-DB commits `8bf4862a5e5c6806647d13466ca2eb769b471a19`, `0ce47cf133211d8927e4d738d7e2ff404e2327fc`, `4c7ec3c74065d3e5f2f4e9c1da6e71b9bfc8f472` and `6a2ab4ee993c37f7b593ad33ff033b2cfa036629` add an optional backward-compatible `FLOR` manifest extension. Checkpoint atomically carries the receipt index forward before sealed WAL segments are truncated; ordinary Outbox ACK/compaction does not erase local operation receipts. The manifest index is a checkpointed materialization of WAL receipts, not a second pending journal.
- [x T] FLD-DB commits `0ae479473c06d6f2b0fe0d587fbaa5cc94a0571c`, `beee6a51e2ad064c11a6832e1ac20d1adfa77caf` and `f20ef9eefebc167a6a34630acc44265ca0100380` cover manifest extension compatibility, GROUP_COMMIT v4 roundtrip/legacy v3 decode, exact retry, fingerprint conflict, receipt-only commit, Outbox single-effect behavior and replay after checkpoint/WAL truncation.
- [x T] BIN-SYNC commit `dbfaae3ba0cb5cfac7c574143a5dc0a397bd0bc2` updates the real IPC→FLD host composition probe to derive ClientId + OperationId + SHA-256 request fingerprint, persist the result with the field write, and replay the original result when the same operation is resent after another operation.
- [x CI] BIN-SYNC run `42946743644` (head `ba4a8e33c26789a9547fa537ecfb7abb83ca4bc7`) passed Linux and Windows .NET jobs, Fuzz/Lab and v3.1 signed vectors. The cross-repository FLD host job built with **0 warnings / 0 errors** and passed Storage **66/66**, Run **26/26**, WAL **20/20**, Engine **65/65**, and `FldBinSyncHost.Tests` **14/14**. The host test now verifies exact retry over IPC and receipt recovery after closing/reopening the real FLD database. The Engine and host reopen tests exercise the FLOR manifest write/read path; the standalone Manifest test project was added to the PR workflow for subsequent runs.
- [x CI] Storage Integration run `42946743590` passed the durable provider integration matrix on Ubuntu, Windows and macOS.
- [x C] Initial receipt-retention caution resolved by §1.116: receipts are now bounded by an explicit contiguous client acknowledgement watermark. No receipt is deleted merely because of Outbox ACK/compaction.



### 1.116 — Bound local-operation receipts with a durable per-client ACK watermark

- [x C] FLD-DB commit `8d9db7cd79d48a2d12efdcb34f904c3a495d0cc7` adds a namespace-scoped `ClientSequence` to GROUP_COMMIT v4 receipts and WAL kind `LOCAL_OPERATION_ACK`. A client's sequence is strictly contiguous; the same OperationId, sequence and fingerprint are reused on retries. Gaps, sequence reuse and stale acknowledged operations fail closed.
- [x C] FLD-DB commits `501dba76c22846d88745f81d2e199a19a3f9eb29`, `50cb070f07607a3aecc1f8f179bd9310239b0436`, `7b2f3f1f7b1bdad9cdd807895fb437b87113660c` and `6be185e02a0fe480752c685c33be34f5a1095702` add the per-client `LastCommittedSequence/AcknowledgedThroughSequence` manifest state, contiguous-prefix validation, replay-safe ACK application and persistence through checkpoint, FlushManifest and Dispose.
- [x C] `AcknowledgeLocalOperationReceipts(ClientId, throughSequence)` writes and flushes a WAL acknowledgement before removing result receipts from the active retry window. It is idempotent; a lost ACK can be resent. A retry at or below the durable watermark is rejected rather than re-applied. The manifest retains one small watermark per client and only unacknowledged result receipts; this avoids indefinite per-operation result growth without a second journal.
- [x T] FLD-DB Engine tests cover sequence gaps, exact retry, contiguous ACK, ACK idempotence, rejection of late retries, receipt-window integrity and recovery after Dispose/checkpoint/WAL replay. WAL tests cover the new acknowledgement codec; Manifest tests cover versioned `FLOR/LOR2` receipts and `FLOC/LOC1` client watermarks.
- [x T] BIN-SYNC commit `e25d499a4de111498b699c891ea2b78f6d719afd` updates the real IPC→FLD host probe to send ClientSequence 1/2, replay sequence 1 after sequence 2, reopen FLD, acknowledge through sequence 2, and verify the acknowledged watermark survives another reopen while Outbox entries remain unchanged.
- [x CI] Run `42948824903` passed the FLD cross-repository build with **0 warnings / 0 errors** and Storage **66/66**, Manifest **11/11**, Run **26/26**, WAL **21/21**, Engine **66/66**, Host IPC **14/14**. Storage Integration run `42948830962` passed on Ubuntu, Windows and macOS. BIN-SYNC Linux and Windows unit tests, Fuzz/Lab and signed control-plane vectors all passed.



### 1.117 — Complete WAL writer support for local receipt ACK records

- [x C] FLD-DB commit `5ee2c1c430e4a0ec3ddf322338ebc2c51f53a0cd` adds `KindLocalOperationAck` to the WAL writer's known-kind allowlist. Recovery already recognizes the kind; the first ACK integration run exposed that append was still rejected before record encoding.
- [x T] The ACK test reaches the real `WalManager.Append` path and exercises WAL flush, restart replay and checkpoint watermark persistence; this was a runtime integration defect rather than a codec or recovery-decoder defect.
- [x CI] Run `42948824903` verified the ACK path end-to-end: writer build passed with **0 warnings / 0 errors**, all five FLD suites passed (Storage 66, Manifest 11, Run 26, WAL 21, Engine 66), and `FldBinSyncHost.Tests` passed **14/14**, including sequence replay, database reopen, ACK and post-ACK reopen. Storage Integration run `42948830962` passed on all three operating systems.



### 1.118 — Separate FLD Database Host and remote Database client facade

- [x C] FLD-DB commit `d8479a55f47a4a8539fb9678f885f5e52f610724` adds atomic `FldDatabase.ApplyLocalOperation` and `ReadFieldWithReceipt`, plus `FldDatabaseIpcProtocol`, `FldDatabaseIpcDispatcher` and `FldDatabaseIpcClient`. The wire contract carries stable OperationId and contiguous ClientSequence; write batches are one database/WAL operation, reads store their exact result receipt, and ACK retires a contiguous prefix.
- [x C] FLD-DB commit `f27b7aa123f93545fe6dfb1944cf327192819a70` adds `Fld1.Database.Host`, a separate executable that owns one FldDatabase namespace and publishes the partition-0 endpoint. It references the Database DLL (and transitive Storage DLL); the existing `Fld1.Storage.Host` remains Storage-only and does not load Database. This is a second Host executable, not a third runtime DLL; the two-DLL boundary is preserved.
- [x T] FLD-DB commits `7833d10048d977bcb31b2eb878a986b96dafc456` and `ba82e15414b2f3d4c2641f6c8fd972a06de25b6e` add in-process IPC integration coverage for batch writes, duplicate retry, receipt-backed read, ACK, reopen and Outbox isolation. Process-level coverage starts the Database Host, rejects a competing owner, kills/restarts the owner, retries an operation, acknowledges the prefix and verifies the watermark/data after another restart.
- [x C] The first cross-repository build exposed integration mismatches before tests could run: the IPC protocol imported the wrong WAL namespace, and `FldWriteBatch` did not yet carry the wire KeyEpoch into the actual Database overlay. FLD-DB commit `624a1d258163ba4c85c58875242e55d49956286c` fixes the namespace and extends batch Put/Delete with an optional KeyEpoch preserved by `FldDatabase.Apply`; existing call sites retain the default epoch 0.
- [x CI-Linux] BIN-SYNC run `42953114166` passed the corrected cross-repository gate on Ubuntu: build **0 warnings / 0 errors**, Storage **66/66**, Manifest **11/11**, Run **26/26**, WAL **21/21**, Engine **69/69** (including the Database Host process restart/single-owner test), and `FldBinSyncHost.Tests` **14/14**. BIN-SYNC .NET Linux/Windows, Fuzz/Lab and v3.1 signed vectors also passed.
- [x C] The cross-repository FLD gate now runs on both `ubuntu-latest` and `windows-latest`; all command steps explicitly use Bash so the same scripts and path quoting are deterministic on both runners.
- [x CI] BIN-SYNC run `42953384787` passed the expanded cross-repository gate on **Ubuntu and Windows**. Each OS built the FLD host adapter with **0 warnings / 0 errors** and passed Storage **66/66** (including Storage-only Host process tests), Manifest **11/11**, Run **26/26**, WAL **21/21**, Engine **69/69** (including Database Host single-owner/restart/replay tests), and `FldBinSyncHost.Tests` **14/14**. The same run passed BIN-SYNC .NET Linux/Windows, Fuzz/Lab and v3.1 signed control-plane vectors. The FLD-DB standalone workflow still reports jobs failing before runner allocation (runner_id=0, no steps); it produced no code/test result and is not used as the acceptance signal for this cross-repository gate.
- [~ C] The first client facade intentionally exposes atomic WriteBatch, receipt-backed ReadField and ACK. Snapshot handles, paged Outbox APIs, remote APPLY and subscription/streaming are later facade operations; they must not be implemented as separate Put/Commit calls that expose the shared dirty overlay.

### 1.119 — Design the lease-fenced Outbox IPC surface before exposing synchronization data

- [x A] Audited the current FLD Database boundary: `FldDatabase.AcquireOutboxLease`, `VisitOutboxBatch` and lease-aware `AcknowledgeOutbox` already provide the correct external-consumer fence. The legacy `ReadOutboxBatch(afterSeq, limit)` and unfenced ACK must not be exposed as the remote BIN-SYNC API.
- [x A] `VisitOutboxBatch` supplies an `OutboxEntryView` ref struct whose spans are valid only inside the visitor callback. IPC serialization must copy the required fields while still inside that callback; borrowed spans must never escape to a queued response or async continuation.
- [x A] Storage IPC frames are bounded at 4 MiB, while an Outbox value may itself be 4 MiB before the fixed entry metadata and operation-envelope overhead. A one-shot `OutboxEntry` response cannot represent every valid stored value; the facade needs an explicit bounded/chunked value transfer rather than silently lowering the Database value limit or allocating an unbounded batch.
- [x A] The direct `FldDatabase` public API has no stable OpenSnapshot/ReadSnapshot/ReleaseSnapshot handle today. Repeated point reads are not a multi-page snapshot. Existing provider snapshot machinery must not be mistaken for a Database IPC snapshot contract.
- [~ C] Define and implement the narrow Outbox IPC lifecycle: acquire a durable lease bound to a stable client owner, return a fenced lease epoch, read bounded batches with explicit value-chunk continuation, ACK only a caller-computed contiguous prefix, and release the lease. Keep IPC receipt ACK separate from synchronization Outbox ACK.
- [x C] FLD-DB commit `5e5f11ee5a3d38f9f1dea5a773bafdc51619f43a` adds Database IPC operations for lease acquisition, fenced Outbox entry reads, Outbox ACK and lease release. The read protocol returns one entry at a time and copies only a bounded value slice from the borrowed view while inside the synchronous visitor callback; continuation repeats the exact OutboxSeq and offset.
- [x C] FLD-DB commit `ce88ee5c21ae5535f50554f54283ea51964b2cd9` adds a 4 MiB Outbox-value chunking test, lease fencing between two clients, and an assertion that Outbox ACK does not retire local-operation receipts.
- [x CI-Linux/Windows] BIN-SYNC run `42954249394` passed both cross-repository jobs. Each OS built with **0 warnings / 0 errors** and passed Storage **66/66**, Manifest **11/11**, Run **26/26**, WAL **21/21**, Engine **71/71** (including maximum 4 MiB Outbox value chunk reassembly, two-client lease fencing and proof that Outbox ACK leaves IPC receipts intact), and `FldBinSyncHost.Tests` **14/14**.
- [x T] FLD-DB commit `4cf30353f1d9cb0bf0b508e9d1afce1fea8195df` extends the separate-process Host restart test to acquire an Outbox lease before shutdown, reacquire after restart, reject the old epoch, ACK/release the new lease, restart again, and verify the Outbox ACK watermark and remaining cursor after recovery.
- [x A] First process-level run `42954669321` confirmed that `OutboxLog.Open` deliberately invalidates a non-empty persisted lease on Host restart by advancing the epoch and clearing the owner. The new Host's subsequent acquire advances the epoch again; the test now asserts this two-step transition rather than assuming restart alone leaves the prior token active.
- [x C] FLD-DB commit `bac22640a8aac65eb1181f5fe7085a96f3cac635` makes the default Outbox chunk fill the largest legal IPC response. The visitor validates/reads the whole durable record before copying a slice, so the maximum default minimizes repeated full-record reads; callers can still request smaller chunks.
- [x T] FLD-DB commit `9ea5301eae7532d47b4a8a2b9062eaf4184694a7` verifies the default chunk reaches the exact response-frame bound and the remaining value is continued at the returned offset.
- [x CI-Linux/Windows] BIN-SYNC run `42954990928` passed the cross-repository gate on both operating systems. Each build had **0 warnings / 0 errors** and passed Storage **66/66**, Manifest **11/11**, Run **26/26**, WAL **21/21**, Engine **71/71** (including maximum-size chunking, stale lease fencing and Host restart/reacquisition), and `FldBinSyncHost.Tests` **14/14**. BIN-SYNC Linux/Windows, Fuzz/Lab and v3.1 signed vectors also passed. The initial run otherwise passed the BIN-SYNC Linux/Windows, fuzz/vector and cross-repository builds; only the new lease-epoch assertion failed on both FLD OS jobs.

### 1.120 — Specify stable Database snapshot handles for IPC clients

- [x A] The direct `FldDatabase` API currently has no OpenSnapshot/ReadSnapshot/ReleaseSnapshot contract. Point reads serialized one at a time cannot guarantee a stable multi-page view. The BIN-SYNC `FldDatabaseDataProvider` generation-pinned provider snapshot path is not itself a public Database IPC snapshot API.
- [~ A] Audit the FLD Run/overlay/checkpoint/compaction lifecycle and choose the snapshot consistency boundary: immutable Run generation plus a frozen overlay view, or another explicit mechanism. Do not materialize the whole namespace per client and do not expose repeated `TryGetBytes` calls as a snapshot.
- [~ C] Define an opaque snapshot handle bound to ClientId and the Host generation, with stable page cursors, bounded value chunks, explicit release, and clear Host-restart semantics. Any pin that survives restart must be durable before it can protect Run files from GC/compaction; otherwise restart must invalidate the handle and reject stale use.
- [~ T] Verify concurrent local writes, checkpoint, compaction and tombstone retention while paging; prove no page sees post-open writes, no pinned Run is reclaimed, release permits reclamation, and Host restart follows the documented handle policy.



### 1.80 — Close the IPC snapshot restart-semantics gap before async provider implementation

- [x A] Re-read the dispatcher lifetime path: _snapshotHandles and _snapshotOpenOperations are in-memory maps owned by one FldDatabaseIpcDispatcher; Dispose releases every remaining snapshot and clears both maps. HostGeneration is newly generated for each dispatcher.
- [x A] Corrected the IPC contract: snapshot page/value/release calls are reconnect-safe only within the same live Host generation. They are not restart-resumable handles. Replaying the same (ClientId, OpenOperationId) after restart is not deduplicated by the old dispatcher and may create a new snapshot/watermark.
- [x D] Updated FLD-DB docs/FLD-DATABASE-IPC.md to distinguish host-generation-scoped snapshot handles from WAL-durable local write receipts and Outbox state.
- [x T] Added `Snapshot_handle_is_rejected_after_dispatcher_restart_and_new_open_uses_new_generation` in `StorageIpcHostCompositionTests.cs` (commit `687a3046521b90394db49c73de5d8ff00cb5557c`): stale token is rejected, new open returns a new HostGeneration, and its snapshot is released. BIN-SYNC and FLD Storage Integration workflows passed on Linux/Windows (`42985402639`, `42985402696`; duplicate main run `42985394545` also passed).
- [x C/T] Implemented the IPC-backed `IAsyncDataProvider` with stable OperationId derived from ChangeId and Host-assigned contiguous ClientSequence. The exact retry identity is durable in the existing FLD receipt; no client-side sequence allocator or payload journal is needed.
- [~ C/T] Receipt ACK still needs a minimal host-supplied completion cursor. It must record the one unresolved operation identity and contiguous completed/acknowledged sequence watermarks, and ACK only after the caller-side completion marker is durable. Outbox ACK is not a substitute.
- [x C/T] Snapshot behavior is explicit and tested: retry with the same open operation id is valid only within one Host generation; Host-generation change invalidates the handle and requires a new snapshot operation. The provider does not claim cross-restart snapshot continuity.
- [ ] A/D Continue with the remaining publication-mode/author-epoch IPC contracts and a composed async commit barrier. Published-origin stamping is implemented; keep FULL_REPLACE, snapshot-state apply and GC disabled until their contracts and safe-horizon proofs exist.

### 1.79 — Reconcile async-provider IPC inventory against implemented snapshot contract

- [x A] Re-audited `FldDatabaseIpcClient`, dispatcher and `FldProviderSnapshot`: snapshot-open returns the exact source-feed watermark captured with the immutable generation; metadata pages expose field provenance and bounded value chunks read from the same pinned generation.
- [x C] FLD also exposes indexed current-state metadata plus ChangeId-fenced value chunks (`0x210F`/`0x2110`). The async provider uses these for normal point reads and falls back to immutable snapshot traversal only for legacy values without ChangeId; this fallback is linear and does not create a snapshot ACK barrier.
- [x C/T] The BIN-SYNC sample now has `FldDatabaseIpcAsyncDataProvider` and `FldDatabaseIpcRemoteBatchApplyStore`; it preserves local ChangeId/HLC/author identity, validates Host LocalAuthor, reads feed/snapshots/current state, and forwards contextual remote APPLY. `CommitPublishedOrigins` (`0x2114`) is also implemented and tested.
- [x C/T] Auto-sequenced local provider append (`0x2111`) derives OperationId from ChangeId and allocates ClientSequence under the FLD commit lock. Retry after response loss and Host restart reuses the same receipt without a client-side sequence allocator.
- [x C/T] The provider exposes independent receipt sequence/ACK operations; receipt ACK remains separate from Outbox ACK.
- [~ C/T] The async coordinator must persist the returned ClientSequence in the existing Node WAL/commit record before retiring receipts. Do not infer caller receipt completion from BIN-SYNC publication progress; no new completion journal, payload journal or second Pending data store is allowed.
- [ ] C/T Add durable publication-mode and author-epoch transitions, then FULL_REPLACE/snapshot-state/GC only with complete capability contracts and safe-horizon proofs.
- [ ] A/D Prove the async commit-barrier design across every conflicting writer before implementing the async coordinator; no thread-affine Monitor may cross an await.

### 1.81 — Prove local provider append replay across response loss and Database restart

- [x T] Added `Auto_sequence_provider_append_replays_same_receipt_after_response_loss_and_database_restart` in `samples/FldBinSyncHost.Tests/StorageIpcHostCompositionTests.cs`. The test commits an identity-preserving auto-sequenced provider append, terminates the IPC server before the response reaches the client, reopens the FLD Database and dispatcher, then retries with the same stable ClientId, OperationId derived from ChangeId and exact mutation payload. FLD returns the original receipt as a duplicate; the Outbox high watermark and entry count remain exactly one.
- [x C] Fixed the xUnit collection-size analyzer failure (`Assert.Equal(1, collection.Count)` → `Assert.Single(collection)`) in commit `670344ed35798b9014c38eeff4452ba0bb118bc0`.
- [x CI-Linux/macOS/Windows] Commit `670344ed35798b9014c38eeff4452ba0bb118bc0` passed `BinSync.Net` and `FLD Storage Integration`; runs `42986281852`, `42986281968` and `42986273589` all completed successfully. The cross-repository FLD host adapter and durable provider integration passed on all three operating systems.
- [x C/T] The IPC-backed `IAsyncDataProvider` and optional `IAsyncProviderOperationReceiptStore` are implemented in the sample. Auto append returns the Host-assigned ClientSequence; receipt ACK is a separate call.
- [~ C/T] The async coordinator must persist that sequence in its existing Node WAL/commit record before calling receipt ACK. The Node's existing durable ChangeId is the retry identity; do not add a second identity or payload journal.
- [ ] C/T Test interruption at each boundary: before send, FLD WAL commit before response, response received before completion marker, completion marker before receipt ACK, receipt ACK before completion watermark, and restart during recovery. Outbox ACK must remain independent and must not retire IPC receipts.
- [ ] A/D Only after those semantics are proven, design the async coordinator around a cross-call async-safe commit barrier; do not bridge the synchronous Monitor-based coordinator with sync-over-async.


### 1.83 — Select the process-isolated async coordinator consistency model

- [x A] Chose durable PendingApply/intent plus short-lock snapshot/revalidation for ordinary remote APPLY and local feed publication. Do not hold Node, coordinator or MetadataStore Monitor locks across IPC awaits.
- [x A] Reuse the existing `StorageRelayPublicationGate` SemaphoreSlim to serialize remote APPLY, relay reconciliation and publication. The FLD APPLY_COMMIT plus MetadataStore PendingApply/finalize sequence is the crash-recovery boundary.
- [x C] Implemented the async remote-apply path: stage PendingApply, apply Node and checkpoint under short synchronous gates; release gates; await `IAsyncProviderRemoteBatchCommitStore`; reacquire the MetadataStore gate and finalize before transport ACK. Commit `f9dda30dbc4df26d508c46bf03ece79237cc8652` adds the awaited before-ACK path; commit `534cdcb1761bb0460ee5a4d4400989740fa889b0` adds async recovery using the exact retained wire payload and stable ApplyToken. The implementation fails closed for incomplete/non-metadata-authoritative intents and never falls back to the lossy synchronous provider API.
- [ ] C Implement async local-feed publication using a captured FLD source-feed ceiling. Read only through that ceiling; writes after capture remain for the next pass. Persist cutover/publication intent under short locks and do not hold them while sending.
- [ ] C/T Persist the returned IPC ClientSequence in the existing Node WAL/commit record, then call the separate receipt ACK. Advance source Outbox ACK only after durable publication; prove that neither ACK can retire the other's state.
- [ ] T Inject crashes before/after Node checkpoint, FLD APPLY_COMMIT, MetadataStore finalization, origin stamping, peer ACK, Node receipt-sequence persistence, IPC receipt ACK and Outbox ACK. Add concurrent local-write tests across each await boundary.
- [ ] A/C Keep FULL_REPLACE, snapshot-state replacement and physical GC disabled until they have dedicated async-safe guards and complete crash-recovery proofs.



### 1.84 — Recover asynchronous remote APPLY intents after process interruption

- [x C] BIN-SYNC commit `534cdcb1761bb0460ee5a4d4400989740fa889b0` adds `StorageSyncCoordinator.RecoverPendingAppliesAsync`. It reads durable intents, requires the exact retained metadata-authoritative wire payload and full-header fingerprint, replays the payload with the stored per-operation decision mask, checkpoints Node, then awaits contextual provider APPLY with the original ApplyToken and immutable batch identity before finalizing Metadata.
- [x C] The async recovery path deliberately rejects incomplete/legacy intents instead of falling back to synchronous `IDataProvider.ApplyRemoteBatch`; a response lost after provider commit is retried with the same ApplyToken.
- [x T] Added `Async_pending_apply_recovery_replays_exact_wire_payload_before_finalizing_metadata`: interruption after durable intent but before Node mutation, then async recovery verifies Node/provider state and clears PendingApply.
- [x C] Commits `db275c7c7f24cf7058dc18b9e91a6b0d6ef6f099` and `5edbe77f503344bdd316fb261035bcc9b02e68b7` require the async provider receipt to match both the original ApplyToken and BatchDigest, and compare the recovered wire batch against namespace, issuer epoch, author, cursor range, digest chain, header fingerprint and deterministic ApplyToken.
- [x T] Extended the recovery test to simulate provider commit followed by a lost IPC response. The first recovery leaves PendingApply intact after the provider has committed; the retry sends the identical ApplyToken, receives the idempotent receipt and finalizes Metadata. The test also reloads the Node checkpoint between the simulated response loss and retry. Commit `e9cecc170caa1390b5d4cd52ab86eb5fac875e5e` adds the fault injection; `9e22c7505449dc83ec6745119eb93b543652bff4` fixes the exact collection assertion.
- [x CI] Exact-head commit `9e22c7505449dc83ec6745119eb93b543652bff4` passed BIN-SYNC run `43012649107`: .NET Linux/Windows, Fuzz/Lab, signed vectors and FLD host adapter builds on Ubuntu/Windows all succeeded (6/6). Storage Integration run `43012649100` passed Ubuntu, Windows and macOS (3/3).
- [x T] Added provider commit followed by lost IPC response and restart-from-Node-checkpoint coverage; same ApplyToken is retried and PendingApply remains until Metadata finalization. [ ] Still required: Metadata finalization failure after provider receipt. Next: first-class async-only coordinator and local-feed publication; keep receipt ACK separate from Outbox ACK.


### 1.85 — Async local publication and independent FLD receipt ACK design

- [x D] Audited `FldDatabaseIpcAsyncDataProvider`: append is idempotent by ChangeId-derived OperationId and returns a FLD ClientSequence; the feed cursor is a distinct Outbox sequence; `AcknowledgeOperationReceiptsAsync` and `AcknowledgeLocalChangesAsync` are separate operations.
- [x D] Decided not to add a second Pending journal or duplicate payloads. BIN-SYNC Node WAL will retain only `NamespaceId + ChangeId + ClientSequence + Published` until the FLD receipt prefix is acknowledged.
- [x D] Ordered publication: recover ClientSequence by exact append retry -> WAL receipt record -> Metadata freeze -> FLD origin stamp -> Node cache checkpoint -> WAL Published marker -> provider Outbox ACK -> provider receipt ACK -> WAL receipt-ACK record. This makes Outbox ACK and receipt ACK independently retryable.
- [x D] WAL v4 is required for receipt records. WAL/v3 and older/unknown versions are rejected, not upgraded; a fresh v4 Node WAL/namespace is required. This preserves the specification's no-migration/no-dual-write rule. WAL checkpoint/truncate is blocked while unresolved receipt mappings remain, so NodePersistence v17 does not need to duplicate this metadata.
- [x C] Added compact Node-WAL receipt mappings, Published transitions, contiguous-prefix validation, provider ACK replay and checkpoint retention guard. WAL v4 is required; WAL/v3 is rejected rather than migrated. Implementation commits: `680477335c81d11e053a680742c43e3d25cf1240`, `54bf2759a26c88ab6d677d49120fab516d616e19`, `ece6ab867745cbe692b4b64330f3c7ac30d8eb0f`, `eab3da56530fdb081dbd84633040cde09266989b`.
- [x T] WAL/v3 rejection, receipt mapping replay, Published-prefix/gap validation, provider receipt-ACK replay, and checkpoint-with-pending-receipt tests passed.
- [x CI] Exact-head commit `785d67782b285b2147e9b3ad3dcde8bde2001fbb` passed BIN-SYNC run `43015451286` (6/6 jobs) and FLD Storage Integration run `43015451210` (3/3 jobs). The Windows/Linux .NET suites, Fuzz/Lab, signed vectors, cross-repo host adapters, and Ubuntu/Windows/macOS storage integration all passed.
- [x C] Added `AsyncStorageSyncCoordinator` as a first-class async-only receiver coordinator over Node, durable Metadata Store and `IAsyncProviderRemoteBatchCommitStore`; it has no `IDataProvider` dependency and uses an async admission semaphore rather than sync-over-async. Commit `c1854afebab0e1a495b352b0787ff5bb7484c82a`. BIN-SYNC and FLD Storage Integration exact-head workflows passed (`43017033504`, `43017033512`).
- [x C/T] Implemented `AsyncStorageSyncCoordinator.PublishLocalChangesAsync` in commits `c628ae399a8d22d2a55bb494c42fc1c8ed861098` and `40c2a9dd17f507139a5c25961dea47928796d428`: finite captured feed ceiling, stable ChangeId→ClientSequence recovery, LWW coalescing, immutable Metadata freeze, async origin stamping, Node cache checkpoint, WAL Published markers, source Outbox ACK followed by independent receipt ACK and WAL receipt-ACK. Appends beyond the captured ceiling remain for the next pass; no Monitor is held across IPC awaits.


### 1.86 — First-class async-only receiver coordinator

- [x C] Extracted the async receive/recovery path into `AsyncStorageSyncCoordinator` (`BinSync.Net/src/BinSync.Storage/AsyncStorageSyncCoordinator.cs`). It does not require an `IDataProvider`, does not use sync-over-async, and awaits contextual remote APPLY before transport ACK.
- [x T] Existing integration tests exercise successful receive, exact PendingApply recovery, response loss after durable provider commit, stable ApplyToken retry, and Node checkpoint reload.
- [x CI] Commit `c1854afebab0e1a495b352b0787ff5bb7484c82a` passed BIN-SYNC and FLD Storage Integration (`43017033504`, `43017033512`).
- [x C/T] The async-only coordinator now supports both contextual remote receive/recovery and finite-ceiling local-feed publication with independent source Outbox and IPC receipt ACKs. See §1.87 for the local-publication implementation and verification.


### 1.87 — Async local-feed publication and independent receipt completion

- [x C] Added `AsyncStorageSyncCoordinator.PublishLocalChangesAsync`. It requires the async provider's durable publication-mode, published-origin and operation-receipt capabilities; it deliberately rejects an incomplete provider contract rather than falling back to synchronous APIs.
- [x C] Captures the provider feed high-watermark once and scans only through that ceiling. Provider appends that race with the scan remain pending for the next pass. It validates ordered feed pages, stable ChangeIds, local author epoch, deferred OriginSequence and Node/Metadata cutover fences.
- [x C] Recovers the Host-assigned ClientSequence by retrying the same ChangeId-derived operation identity when the WAL mapping is absent, then persists `NamespaceId + ChangeId + ClientSequence` in the existing Node WAL before advancing publication.
- [x C] Reuses the synchronous publication semantics for LWW coalescing and immutable outbound identity: already-frozen ChangeIds are fingerprint-checked and re-stamped idempotently; only unfrozen winners are added to Metadata Store; all scanned winners update the Node application cache.
- [x C] Awaits FLD origin stamping outside Node/Metadata Monitor locks, checkpoints Node, records WAL Published markers, acknowledges the source Outbox prefix, then acknowledges the contiguous provider receipt prefix and appends the WAL receipt-ACK record. The order leaves receipt mappings available after response loss at either ACK boundary.
- [x T] Added `Async_local_publication_coalesces_feed_and_acks_outbox_before_receipts`: two edits to one field coalesce to the HLC winner; Metadata contains one outbound operation; source Outbox ACK precedes receipt ACK; Node cache and WAL are checked.
- [x T] Added `Async_local_publication_retries_host_auto_sequence_after_lost_append_response`: the Host commits the ChangeId-derived auto-sequence receipt but loses the response before the Node WAL mapping; retry recovers the same ClientSequence and publishes exactly once.
- [x T] Added `Async_local_publication_leaves_appends_after_captured_ceiling_for_next_pass`: a local write racing after the feed read remains beyond the captured ceiling and is published by the next pass, not acknowledged early.
- [x T] Extended `Async_local_publication_recovers_receipt_ack_after_outbox_ack_and_response_loss` to reload Node from its persisted checkpoint and reopen/replay the WAL after FLD committed receipt ACK but the response was lost. Retry sees the empty source feed, repeats the idempotent receipt ACK and clears the recovered WAL mapping without duplicating Metadata.
- [x C/T] Added `RecoverBeforeAcceptingSessionsAsync` to run PendingApply recovery first and local-feed publication second, returning an explicit recovery result. `Async_startup_recovery_publishes_local_feed_before_accepting_sessions` verifies the startup orchestration. This does not yet include snapshot rehydration or FULL_REPLACE.
- [x D] Documented the receipt-stream ownership constraint in `IAsyncProviderOperationReceiptStore` and `FldDatabaseIpcAsyncDataProvider`: FLD ClientSequence is per stable ClientId, so each namespace/coordinator must use its own stable ClientId/provider instance across reconnects. Runtime enforcement remains open.
- [x CI] Exact-head commit `a6f77fb7caa7f58dac859ef1efecb6cae9d11c2d` passed BIN-SYNC run `43027736315` (Linux/Windows .NET suites: 681 tests each, Fuzz/Lab, signed vectors and Ubuntu/Windows cross-repository host adapter builds) and FLD Storage Integration run `43027736290` (Ubuntu/macOS/Windows: 238 tests per OS). The async publication, restart-recovery and startup-recovery tests passed on all three operating systems.
- [x C/T] Added the process-isolated coordinator test using the real FLD Database Host. It restarts the Host after the durable auto-sequence append but before BIN-SYNC records the ClientSequence mapping, reconnects with the same ClientId, and verifies exact receipt recovery. Runtime receipt-stream ownership is bound to the Metadata Store scope/replica/namespace and rejects ClientId rotation or cross-namespace reuse.
- [~ A/T] Remaining before calling the async provider integration complete: audit and test async commit-guard coverage for concurrent remote APPLY and application writes. FULL_REPLACE, snapshot-state replacement and physical GC remain disabled until async-safe guards and safe-horizon proofs exist.

### 1.88 — Close async local-publication response-loss windows and bind receipt streams

- [x C] Runtime publication now requires a non-empty stable ReceiptStreamId and durably binds it to (scope, local replica, namespace) through IMetadataProviderReceiptStreamStore. Reusing one FLD ClientId across namespaces or rotating it after restart fails closed; this prevents a different Host receipt stream from being mistaken for the same ClientSequence history.
- [x T] Added Async_local_publication_rejects_receipt_client_id_rotation and Metadata_store_rejects_reusing_one_receipt_client_id_across_namespaces.
- [x T] Added fault injection for response loss after durable FLD published-origin stamping and after durable source Outbox ACK. Retries must reuse the existing Metadata ChangeId identity, repeat idempotent origin stamping or ACK, and must not create another outbound operation.
- [x T] Added restart recovery for the boundary after durable Node checkpoint but before the WAL Published marker: the test reloads Node from FLD persistence, reopens/replays the Node WAL, and verifies the frozen Metadata operation and source feed are completed exactly once.
- [x T] Existing tests continue to cover lost auto-sequence append response, lost receipt-ACK response, independent Outbox/receipt ACK, captured feed ceiling, coalescing, and startup recovery.
- [x C/T] Published-origin stamps are sent in batches of at most 4096 to match the FLD IPC frame limit; immutable stamps make partial-batch retry idempotent. Commit 15cf25e2a4c97afffa5b5abddd183223492076db; a 4097-entry integration test asserts a 4096 + 1 split (f73e927d24d6080a6cf80ef850b936916541f9bc).
- [x T] Extended the real Database Host process test to restart FLD after durable auto-sequence append but before BIN-SYNC publication/WAL receipt mapping, reconnect with the same ClientId, and recover the original receipt. The second restart is fenced against the immediately preceding Host OwnerId. Commits 06c71cb8eb8d6aeb21a915c108ebebfa58c24864 and 61a46df715935d5d8f8a2c5a1b5196ac6ec91751.
- [x C/T] Test commits: acd4910ecf05c2611d996880391be00f0a402b18 and c2d8e009be483f49dc24139ba34ce38d2437f36a.
- [x CI] Exact code head 61a46df715935d5d8f8a2c5a1b5196ac6ec91751 passed BIN-SYNC .NET 10 Linux/Windows (681 tests each), Fuzz/Lab, signed vectors and cross-repository FLD host adapter builds on Ubuntu/Windows (Storage 66, Manifest 11, Run 26, WAL 21, Engine 88, Host IPC 22 on each OS). FLD Storage Integration passed on Ubuntu, macOS and Windows (244 tests per OS). Runs: BIN-SYNC push 43032816937, PR 43032825938; FLD Storage Integration 43032826211.
- [ ] T Process-isolated end-to-end test using the real FLD Database Host and IPC client, including Host restart between append, Node WAL mapping, Metadata freeze, origin stamp and the two independent ACKs.
- [ ] A/T Audit async commit-guard coverage for concurrent remote APPLY, relay reconciliation and application writes. FULL_REPLACE, snapshot-state replacement and physical GC remain disabled until async-safe guards and safe-horizon proofs exist.

### 1.89 — Process-isolated async remote APPLY adapter

- [x C] Added `FldDatabaseIpcAsyncRemoteBatchApplyStore` as the FLD IPC implementation of `IAsyncProviderRemoteBatchCommitStore`. It preserves the complete contextual commit identity and mutation provenance and awaits the FLD Host's durable APPLY receipt before BIN-SYNC can acknowledge the transport batch.
- [x T] Added a real Database Host process test that applies a remote batch, reconnects through a new IPC ClientId, retries the identical commit and verifies the duplicate receipt plus persisted ChangeId/origin provenance.
- [x CI] The adapter and its cross-repository build are included in the verified BIN-SYNC CI checkpoint recorded in §1.92.

### 1.90 — Durable Metadata outbound delivery

- [x C] Added `AsyncStorageSyncCoordinator.PushNextBatchAsync` to send immutable Metadata-owned outbound/relay operations through the existing durable peer reservation and transport ACK path.
- [x C] Added per-peer outbound serialization. The namespace admission gate is used only to capture the finite outbound/relay ceilings and is released before waiting for network ACK; a peer gate serializes reservation/ACK lifecycle for concurrent sessions to the same peer.
- [x T] Added an integration test that verifies a frozen Metadata operation is delivered, remotely applied before ACK, and reflected in receive cursor, durable peer ACK cursor and provider state.
- [x C/T] Corrected the test fixture to publish through the async provider feed and to establish the initial peer snapshot baseline before incremental delivery. The test is not weakened: it still verifies the actual durable delivery and ACK path.

### 1.91 — Async host composition and two-process exchange

- [x C] Added `FldBinSyncAsyncHostAdapter` to compose startup recovery, local publication, inbound batch receive and outbound rounds while leaving process-lifetime Node, Metadata Store, IPC providers and sessions owned by the host.
- [x T] Added a bidirectional integration test with two independent FLD Database Host processes, separate Node/Metadata/WAL roots and a paired transport. It verifies both directions' provider state, Node cache, receive cursors and durable peer ACK progression.
- [ ] T Extend this scenario with restart of both peers after exchange; reload Node, Metadata and FLD Host state, reconnect with stable provider receipt identities, then prove duplicate/replay behavior and cursor/ACK parity.
- [ ] T Add a two-host interruption matrix for lost transport ACK, remote APPLY committed before ACK response, and restart between receive intent, Node checkpoint, FLD APPLY receipt and Metadata finalization.

### 1.92 — Admission deadlock and snapshot-baseline corrections; exact CI checkpoint

- [x C] Fixed full-duplex async admission ordering: inbound network wait occurs without holding namespace admission; after a complete batch arrives, the receive path acquires admission for durable processing. Outbound delivery captures finite ceilings under admission and releases it before transport ACK wait.
- [x T] Corrected both incremental-delivery fixtures to persist the empty initial peer stream baseline before attempting DELTA_BATCH. The two-host test now models the post-initial-snapshot state explicitly.
- [x CI] Exact branch head `bc5cc5c59d3b2111032de51f0fd51288d3b60123` passed `BinSync.Net` run `43043987396` (6/6 jobs: .NET Linux/Windows, Fuzz + Lab, V3.1 signed control-plane golden vectors, FLD host adapter builds on Ubuntu/Windows) and `FLD Storage Integration` run `43043987421` (3/3: Ubuntu, Windows, macOS).
- [ ] A/T Audit the async commit-guard boundary across remote APPLY, relay reconciliation, local publication and application writes. The current namespace admission gate is coordinator-instance scoped; host composition must enforce one coordinator/admission domain per namespace and prevent bypass writers until a process-wide writer protocol is proven.
- [ ] T Complete the two-host restart/crash matrix before marking async provider integration complete.
- [ ] V3.1 roster/frontier/checkpoint/rejoin/FULL_REPLACE and safe-GC proofs remain separate required work. Physical GC and FULL_REPLACE remain disabled until their adversarial and crash-recovery proofs are complete.

### 1.121 — Real Database Host remote-APPLY restart durability

- [x T] Extended the real FLD Database Host IPC test to commit a contextual remote APPLY, stop the complete Host process, wait for the FLD writer fence to be released, restart the Host against the same database root, reconnect with fresh IPC clients, and retry the identical immutable batch.
- [x T] The restarted Host returns the original ApplyToken and BatchFingerprint with `IsDuplicate=true`; the persisted field value, ChangeId, OriginAuthorEpoch and OriginSequence remain unchanged, and the remote write does not enter the local Outbox.
- [x CI] Exact branch head `e950cd6e72d525c63ca56bb5551e4cf53dcd8b9c` passed `BinSync.Net` run `43046189626` (6/6 jobs: Linux/Windows .NET tests, Fuzz + Lab, V3.1 signed vectors, FLD host adapter builds on Ubuntu/Windows) and `FLD Storage Integration` run `43046189665` (3/3: Ubuntu, Windows, macOS).
- [x D] This process-restart proof complements, but does not replace, the coordinator-level fault-injection test for a lost provider APPLY response and exact ApplyToken retry (§1.84). The real-host test currently receives the first receipt before restarting; it does not yet simulate termination after durable commit but before the IPC response reaches the caller.
- [ ] T Extend the two-host bidirectional exchange test with orderly shutdown/restart of both Database Host processes and reopen of both Node/Metadata/WAL stores. Preserve each provider receipt ClientId, recover PendingApply before accepting sessions, re-handshake, and assert provider data, Node cache, receive cursors and peer ACK cursors remain consistent.
- [x T] Implemented the real-process lost-APPLY-response test in §1.123: the Host terminates after durable APPLY and before response framing; restart/retry assertions are committed. Exact-head CI remains pending.
- [ ] A/T Audit the async commit-guard boundary across remote APPLY, relay reconciliation, local publication and application writes. The current namespace admission gate is coordinator-instance scoped; host composition must enforce one coordinator/admission domain per namespace and prevent bypass writers until a process-wide writer protocol is proven.
- [ ] V3.1 roster/frontier/checkpoint/rejoin/FULL_REPLACE and safe-GC proofs remain separate required work. Physical GC and FULL_REPLACE remain disabled until their adversarial and crash-recovery proofs are complete.

### 1.122 — Two-host full restart and resumed-session proof

- [x T] Extended the two-host bidirectional integration scenario to shut down both complete FLD Database Host processes and all client-side composition state after a successful exchange. The test closes sessions, IPC providers and remote APPLY stores, then disposes both Nodes, WALs, Metadata Stores and StorageDatabase handles before stopping the two Host processes.
- [x T] Restarts both independent Hosts against their original database roots, waits for fresh IPC endpoints and writer-fence release, reopens both StorageDatabase/Metadata/WAL roots, reloads each Node through `StorageNodePersistence.Load`, and reattaches the recovered WAL.
- [x T] Reconnects each provider with its original stable receipt ClientId, rebuilds both async coordinators/adapters, and runs `RecoverBeforeAcceptingSessionsAsync` before opening transport sessions. Verifies both remotely received field values, ChangeIds, Node cache values, receive cursors, peer ACK cursors and empty local Outboxes after restart.
- [x T] Follow-up hardening re-establishes a fresh paired transport session after recovery, runs both outbound rounds and requires zero replayed operations while the persisted receive and ACK cursors remain at one. Exact-head BIN-SYNC run `43055002147` passed the two-host restart/resumed-session assertions on Linux and Windows.
- [ ] T Add process-level interruption coverage for transport ACK loss and the full receive-intent → Node checkpoint → FLD APPLY receipt → Metadata finalization boundary on both peers.
- [ ] T Add a real-process lost-APPLY-response test that commits durably but suppresses the IPC response before caller observation; distinguish this from existing client-side/coordinator fault injection.
- [ ] A/T Audit the async commit-guard boundary across remote APPLY, relay reconciliation, local publication and application writes. The namespace admission gate remains coordinator-instance scoped; host composition must enforce one coordinator/admission domain per namespace and prevent bypass writers until a process-wide writer protocol is proven.
- [ ] V3.1 roster/frontier/checkpoint/rejoin/FULL_REPLACE and safe-GC proofs remain separate required work. Physical GC and FULL_REPLACE remain disabled until their adversarial and crash-recovery proofs are complete.

### CI follow-up — Windows IPC endpoint replacement

- [x A] Diagnosed the red Windows cross-repository host-adapter job on commit `d065d63`: the FLD host restart test failed while publishing its endpoint after the previous Host process had been force-stopped. `StorageIpcEndpoint.Publish` attempted `File.Move(..., overwrite: true)`; Windows returned `UnauthorizedAccessException` at the endpoint replacement step. The FLD Core/Storage/Manifest/Run/WAL/Engine durability tests and the BIN-SYNC test suites in that run passed.
- [x A] Updated FLD-DB `fix/crash-consistency-outbox` in commit `b852329`: after the new Host has acquired the exclusive database partition lease, it normalizes and removes the stale endpoint name, then publishes the already-flushed temporary endpoint file without overwrite. This is safe because the partition lease excludes a live prior owner; startup readers already tolerate the brief absent-endpoint interval.
- [x CI] Exact BIN-SYNC head `9eb17845eb213440d4a6fc1b7e24712f591e08e0` passed run `43055002147`: Windows and Ubuntu cross-repository FLD host adapter builds/tests both passed against FLD-DB branch `fix/crash-consistency-outbox` (including the endpoint-restart scenario); the main Linux/Windows .NET suites, Fuzz/Lab, and v3.1 signed vectors also passed. FLD Storage Integration passed on Windows, Ubuntu and macOS in run `43055002297`. FLD-DB commit `b852329` independently passed its Windows and Ubuntu jobs in runs `43054964897` and `43054978281`; the macOS job on the latter remains queued.


### 1.123 — Real-process remote APPLY response-loss boundary

- [x C/T] Added an explicit opt-in test failpoint to the FLD Database Host. It terminates the Host with exit code 86 only after the remote APPLY dispatcher has completed and returned its durable receipt, before the IPC server can write the response frame; normal Host invocations do not enable it.
- [x T] Reworked the real Host process integration to use that failpoint: the caller must observe IPC response loss, the Host process must exit, and after restart against the same database root a fresh IPC client retries the exact immutable batch. The ApplyJournal must return the original ApplyToken/fingerprint as a duplicate; persisted value, ChangeId, origin epoch/sequence and zero local Outbox watermark are asserted.
- [x CI] Exact BIN-SYNC head `8adf57d34b493f9d85f73581969641475a160a66` passed PR run `43056768685` (6/6 jobs), including the real-process response-loss/restart test on both Ubuntu and Windows, Linux/Windows .NET suites, Fuzz/Lab and v3.1 vectors. The matching FLD-DB host branch head `b0504fd2186eb3ea299f32b920122843ec931971` passed its independent Ubuntu/Windows/macOS CI run `43056180963` (3/3).
- [ ] T Extend the same process-level approach to lost transport ACK and the complete receive-intent → Node checkpoint → FLD APPLY receipt → Metadata finalization interruption matrix on both peers.


## 1.124. Transport ACK loss and immutable replay — 2026-11-11

- [x C/T] Added `Lost_transport_ack_replays_same_batch_after_reconnect_without_reapplying` to the real `SyncSession` transport tests. The receiver applies the DELTA_BATCH, then an opt-in test transport drops the first ACK and closes the channel before delivery. The sender therefore has no durable ACK cursor and must replay the same pending batch after a fresh handshake.
- [x T] The retry assertions require the receiver to recognize the duplicate without applying the operation again, retain the exact value, and return the ACK that advances the sender's per-author cursor. This complements §1.123's real FLD Host process crash after durable APPLY but before IPC response; it is a transport/session test, not yet a two-Host FLD ACK-loss process test.
- [ ] CI Exact-head BIN-SYNC Linux/Windows, Fuzz/Lab, signed vectors and FLD cross-repository builds/tests must pass before this transport-loss proof is considered verified.
- [x C/T] Extended ACK-loss coverage through the existing two-independent-FLD-Host process composition and async Metadata/FLD adapter; see §1.126. Exact-head CI remains pending.


## 1.125. Process-wide async admission and peer-send gates — 2026-11-11

- [x C] Replaced coordinator-instance-owned async admission with a process-wide registry keyed by durable `(NodeId, namespaceId)`. Separate `AsyncStorageSyncCoordinator` facades for the same local replica/namespace now share the same receive/publication/recovery `SemaphoreSlim`.
- [x C] The short Metadata coordination lock and per-peer outbound reservation/ACK gates are shared by the same registry as well, so multiple coordinator facades cannot interleave multi-call metadata registration or concurrently reserve/send the same peer stream; different peers remain independent.
- [x T] Added `Async_coordinator_instances_share_one_namespace_admission_gate`: one coordinator blocks inside provider publication while a second coordinator attempts PendingApply recovery; the second must remain blocked until the first releases the shared admission lease.
- [x A] The Node metadata-store-owned publication fence already rejects legacy direct Node-owned local writes, receive validation and Node Outbox transport after cutover. This protects the main Node bypass path. The shared registry does not serialize arbitrary direct calls into public Metadata Store mutation APIs; production host composition must keep that writer capability private and route namespace mutations through the coordinator. Do not claim a universal application-writer barrier yet.
- [ ] CI Exact-head BIN-SYNC and FLD Storage Integration workflows must pass for the shared-gate implementation and test before marking this change verified.
- [ ] A remaining limitation is process scope: the registry coordinates coordinator instances in one process, not multiple processes. Cross-process coordination remains the responsibility of the single-owner Host/partition lease and durable Metadata transactions.


## 1.127. Async PendingApply recovery after real FLD Host and coordinator process death — 2026-11-12

- [x C/T] The two-independent-Host async composition test proves the real FLD Database Host can commit contextual provider APPLY and terminate before the IPC response is delivered. The coordinator restarts the same FLD database, reconnects fresh IPC clients, and runs startup recovery before accepting another session.
- [x C/T] Added an async coordinator process-kill matrix for four receive cuts: after durable PendingApply staging but before Node apply; after the Node checkpoint but before provider APPLY; after provider APPLY commits but before Metadata finalization; and after Metadata finalization but before the transport ACK is sent. Each cut kills the actual child test-host process and reopens the same Storage partition.
- [x C/T] For the first three cuts, recovery replays the exact immutable ApplyToken and wire payload, preserves Node/provider state, finalizes the existing PendingApply exactly once, and advances the receive cursor only after provider durability. The provider-before-finalize cut observes a duplicate durable provider receipt; the earlier cuts apply the provider mutation during recovery. For the fourth cut, the receive cursor and provider/Node state are already durable while the sender ACK cursor remains unchanged; after restart, the same batch is replayed as duplicate/no-op and only the fresh ACK advances the sender cursor.
- [x CI] Exact-head commit `4786e5ce87826f38293c07377c83786b534467c0`: all 18 checks completed successfully, including Linux/Windows .NET tests, durable provider integration on Ubuntu/Windows/macOS, Fuzz/Lab, signed v3.1 vectors and both Linux/Windows FLD cross-repository adapter jobs. This supersedes the earlier intermediate green heads.
- [x T] The fourth process-kill cut now isolates the intended durable boundary: Metadata receive finalization and provider/Node state are durable, while the sender's ACK cursor remains unchanged because the receiver is killed immediately before emitting transport ACK. The test does not conflate this crash cut with replay behavior; exact immutable-batch replay and single ACK advancement remain independently covered by §1.124 and §1.126.
- [ ] Continue independent crash/reopen and rejoin/resurrection proofs. Keep physical GC and FULL_REPLACE disabled until all required proofs are complete.

## 1.126. Two-Host FLD transport ACK loss and retry — 2026-11-11

- [x C/T] Extended `Async_host_composition_exchanges_changes_bidirectionally_between_two_database_host_processes` after both Hosts and their Node/WAL/Metadata state have been restarted and recovered. A new local change is published from Host A; Host B durably applies it through the real IPC provider and finalizes its Metadata receive cursor, while the transport wrapper deliberately drops the ACK and closes the session channel.
- [x T] The sender's durable peer ACK cursor must remain at the previous value while the receiver's durable receive cursor advances. The test reconnects both BIN-SYNC sessions without restarting either Host, replays the reserved batch, requires a duplicate/no-op result and a successful ACK, then verifies the sender ACK cursor advances exactly once and the receiver's FLD value/ChangeId remain unchanged.
- [ ] CI Exact-head BIN-SYNC Linux/Windows, Fuzz/Lab, signed vectors and FLD cross-repository build/test workflows must pass before this two-Host ACK-loss proof is verified.
- [ ] Extend the same matrix with process termination/restart after durable receive intent, after Node checkpoint but before provider APPLY, after provider APPLY receipt but before Metadata finalization, and after Metadata finalization but before transport ACK. Existing §1.123 separately proves process death after durable APPLY and before IPC response.

## 1.128. Physical-GC enablement audit — 2026-11-12

- [x A] Audited the exact BIN-SYNC branch head `361b7b9e666bad3e183dc1c8ad07ae9f9f98d778`: the signed Namespace Roster, Applied-Origin Frontier, Retention Checkpoint, installed FULL_REPLACE ACK, retired-epoch/rejoin fences and proof snapshot are present. The exact head passed all 15 reported GitHub checks.
- [x A] Audited the current FLD-DB host branch head `05463e76ea71683ef6a72b1abd77cc11f59a3754`: it contains an immutable in-process `FldReferenceSnapshot` over dirty state, Outbox, ApplyJournal, receipt identities and provider-snapshot pins. The branch's prior exact-head platform CI checkpoint passed Ubuntu, Windows and macOS.
- [x A] BIN-SYNC already has separate fail-closed compaction entry points for ChangeId ledger rows, transport receipts and provider record tombstones. All three namespace-GC enablement properties still return false; no physical reclamation is currently enabled.
- [ ] C/T Add field-tombstone reclamation. The current provider compactor handles record tombstones and fields hidden by them, but there is no independent safe reclamation path for a standalone field tombstone.
- [ ] A/C/T Complete the process-isolated FLD Host boundary. `FldReferenceSnapshot` and the synchronous host commit guard are currently in-process. The IPC async provider does not yet expose an exclusive GC lease plus reference-snapshot query capability; do not substitute sync-over-async or release a host snapshot before the corresponding BIN-SYNC deletion commits.
- [ ] A/C/T Prove async GC snapshot/revalidation across the Metadata Store, provider state and FLD Host. No thread-affine Monitor may be held across an await. Concurrent writes must either be excluded by an async-compatible host lease or detected by versioned snapshot revalidation before the deletion transaction.
- [ ] C/T Add one host-owned GC maintenance orchestration path that validates exact scope/replica/namespace/store/trust bindings and invokes the three independent compaction domains in bounded batches. It must report blocked reasons and reclaimed counts; protocol readiness alone must never enable deletion.
- [ ] T Add physical-compaction crash tests at each durable boundary: before replacement Run publication, after immutable Run/blob creation, after manifest switch, during old-Run cleanup, and during each bounded Metadata deletion transaction. Reopen must select exactly one valid generation and never resurrect deleted rows from WAL or orphan Runs.
- [ ] T Add adversarial stale-replay/rejoin tests after actual deletion: stale DELTA_BATCH, old transport epoch, lost ACK/replay, offline member rejoin, missed roster/checkpoint generations, missing/lagging Active member, and FULL_REPLACE installation failure. A failed or ambiguous proof must yield zero deletions.
- [ ] A/T Close the writer-bypass audit. The process-wide async admission registry does not fence arbitrary direct public Metadata Store mutations; production composition must keep namespace writer capabilities private or provide a proven shared writer protocol. The Host lease must cover every mutation path, not only the GC RPC.
- [ ] P/A Measure retained metadata cardinality, GC scan allocations, bounded delete throughput and restart cost on representative large namespaces before enabling defaults.
- [ ] Release gate: enable no physical GC flag until every item above is implemented and exact-head CI is green on BIN-SYNC Linux/Windows, Fuzz/Lab, signed vectors, FLD adapter Linux/Windows and FLD-DB Ubuntu/Windows/macOS. Keep FULL_REPLACE and all physical-GC switches fail-closed until their own complete proofs pass.

## 1.129. Snapshot-aware Metadata relay retirement — 2026-11-12

- [x T] Added a real two-Host signed-GC assertion for both local payload-retention domains. Even after Host B durably ACKs Host A's outbound operation, Host A's immutable outbound payload remains retained in Metadata until its stream prefix is compacted; Host B independently retains the relay payload. Both Hosts therefore report a live record reference, return zero deletions and retain their provider tombstones. This is the required fail-closed result, not a flaky test. A durable peer ACK alone is not equivalent to a committed Metadata compaction baseline.
- [x A] The failure exposed a missing v3.1 bridge: signed FULL_REPLACE installed ACKs and the common retention checkpoint are validated by `StorageNamespaceGcStore`, but they are not translated into a durable Metadata peer-stream baseline. `CompletePeerInitialSync` currently rejects baselines that skip relay history, and ordinary `CompactRelayThrough` does not permit deleting an unconsumed relay stream when no non-origin peer currently requires it.
- [ ] C/T Add a proof-bound FULL_REPLACE baseline operation for Metadata. It must validate the exact accepted roster/checkpoint/generation/digest and every Active member's installed ACK, match transport epochs, reject pending APPLY or immutable peer reservations, and atomically advance the relevant peer-stream baselines plus compact only the contiguous payload prefixes represented by that snapshot. A future/new peer must still be forced through the current FULL_REPLACE before it can consume deltas beyond the baseline.
- [x A] Source audit confirms outbound and relay sequence allocators/high-watermarks are distinct (`OutboundKey(namespace, author)` versus `RelayKey(namespace, author)`), but transport delivery is intentionally modeled as one peer stream per `(peer, epoch, namespace, author)`, not as two simultaneously active feeds. `SyncSession.PushBatchCoreAsync` selects exactly one source for an author: local-origin `Outbound` when `author == sender.NodeId`, otherwise `Relay`; both are encoded as the same immutable DELTA_BATCH and use the same durable reserve/push/ACK lifecycle. `AcknowledgeOutboundBatch` is therefore currently a misleading method name for a generic peer-batch ACK, not proof that relay has no ACK. The shared cursor is valid only if the storage invariant prevents the local replica's author stream from also being populated as a new incoming relay stream. Do not add a second relay cursor or copy legacy state into two cursors unless a future protocol explicitly permits simultaneous outbound+relay streams for one author.
- [ ] C/T Enforce the one-source-per-author invariant at every ingress: a new incoming ChangeId whose `AuthorNodeId == LocalReplica` must be rejected unless it is an exact duplicate already present in the local-outbound ChangeId ledger; local publication must use the local replica identity as its author stream. Preserve duplicate echo handling and test conflicting/new local-author relay attempts. Add a generic peer-batch ACK API name (and migrate internal callers) so relay batches are not semantically mislabeled as outbound. Keep key kind 15 byte-for-byte backward compatible; no cursor split/migration is required for this invariant-preserving model.

- [ ] T Extend the two-Host scenario to show that, after both Metadata stores commit the proof-bound baseline, both relay/outbound reference scans are empty and both hosts reclaim the same tombstone. Include crash/reopen between baseline commit and provider deletion; retry must remain idempotent.
- [ ] Physical GC remains disabled until this Metadata retirement bridge, async Host reference snapshot/revalidation, crash matrix and stale-rejoin/resurrection proofs all pass exact-head CI.

## 1.130. Peer-stream ACK/source-selection audit — 2026-11-12

- [x A] Traced the complete `SyncSession.PushBatchCoreAsync` path. Pending immutable reservations are replayed first; otherwise authors are selected from outbound and relay author lists. Each author is read from exactly one source based on membership in the Metadata Store's outbound-author or relay-author set. The selected page is encoded as the same immutable DELTA_BATCH and uses one durable peer reserve/push/ACK lifecycle.
- [x A] Confirmed the existing relay compaction conformance test deliberately reserves and ACKs a relay author's page using the shared per-author peer-stream state. This is not an accidental ACK advancing an unrelated relay feed: the reservation belongs to the relay page selected for that author. A separate relay cursor would duplicate state and break immutable replay semantics.
- [x C/T] Enforced exclusive source ownership per `(namespace, author)`: outbound freeze rejects an author that already has relay history; direct incoming acceptance, pending-apply staging and finalization reject new relay operations for an author that already has outbound history. Exact duplicate ChangeId echoes continue through the existing ledger deduplication path.
- [x C/T] Closed the staging/freeze race: outbound freeze is also fenced while a durable PendingApply contains a relay-eligible operation for the same author. The fence survives reopen; incoming staging already rejects an author with outbound history, and finalization rechecks before publishing relay metadata.
- [x C/T] `SyncSession.PushBatchCoreAsync` now determines source ownership from Metadata outbound/relay author sets rather than assuming `author == sender.NodeId`; it fails closed if an author appears in both sets.
- [x C/T] Renamed the peer-stream ACK API to `AcknowledgePeerBatch` across Core, Storage, SyncSession and tests. No legacy alias is retained for the misleading outbound-only name.
- [x T] Added tests for outbound-first and relay-first conflicts, exact duplicate echo handling, source-fence persistence after reopen, and pending relay apply fencing after reopen.
- [x P] One-million-row Metadata freeze benchmark on the new code: 1,024 local operations over 1,000,000 existing ChangeId rows = 242.1 us/op, 10.11 KB allocated. Previous baseline was 244.7 us/op, 10.1 KB; this is within benchmark noise and shows no material regression.
- [ ] C/T Implement the proof-bound FULL_REPLACE Metadata baseline from §1.129 using one cursor per author stream. Validate exact accepted roster/checkpoint/generation/digest, all Active installed ACKs and matching transport epochs; reject pending APPLY or immutable reservations; atomically advance only author cursors represented by the snapshot and compact only contiguous payload prefixes. Include crash/reopen and retry idempotency.
- [ ] Physical GC remains disabled until the Metadata retirement bridge, async Host reference snapshot/revalidation, crash matrix and stale-rejoin/resurrection proofs pass exact-head CI.

## 1.131. Per-author receive digest chain and FULL_REPLACE receive baseline — 2026-11-12

- [x A] Traced the metadata-authoritative receive path end to end. The sender persists `LastBatchDigest` in `PeerStreamState` per `(peer, epoch, namespace, author)`; the receiver also persists `ReceiveState` per author, but its `ReceiveChainKey` was only `(sender, epoch, namespace)`. The metadata-owned transport builds each author stream from its own durable digest checkpoint, so the receiver must key the digest checkpoint by author as well. The older Node-owned receive path retains its separate sender-wide chain and is not used when Metadata owns receive state.
- [x C/T] Changed Metadata receive digest checkpoints to per-author keys (new storage key kind 25), matching the sender's peer-stream digest chain and the per-author receive cursor. Legacy sender-wide key kind 14 remains readable. When exactly one author stream is established, its legacy digest is migrated atomically to that author even if the next accepted batch belongs to a new author; multiple established authors remain ambiguous and fail closed, requiring FULL_REPLACE rather than guessing.
- [x T] Added an interleaved two-author test with independent first-batch digests, subsequent batches and reopen persistence; updated prior fixtures that had accidentally chained one author's digest into another author's first batch.
- [x T] Added fixture-based legacy key migration tests: a single established author migrates even when the next batch is from another author, and multiple established authors reject with the explicit FULL_REPLACE requirement without mutating stored rows.
- [ ] C/T Extend the proof-bound FULL_REPLACE baseline to update both sides of every peer-author stream: outgoing `PeerStreamState` cursor/digest and incoming `ReceiveState.CursorEnd` plus per-author receive digest. Only reset streams whose old cursor is at or below the checkpoint-proven snapshot frontier; preserve already-advanced streams. Commit the full receive/send baseline atomically with the proof marker and contiguous payload compaction.

## 1.132. FULL_REPLACE stream-baseline input contract — 2026-11-12
- [x A] Confirmed the current `IDataProvider.OpenSnapshot()` contract exposes only the provider-local scalar change-feed watermark; it cannot enumerate BIN-SYNC outbound and relay feed coordinates. `StorageFullReplaceSender.PrepareSnapshot` therefore cannot safely manufacture the required vector from the provider alone. Do not synthesize author offsets from snapshot mutations or OriginSequence: those are different sequence domains.
- [x A] Confirmed `CompletePeerInitialSync` currently commits only outbound `PeerStreamState` baselines and the completion marker. It does not atomically establish per-author incoming `ReceiveState.CursorEnd` plus receive digest-chain state. The baseline application must become one StorageMetadataStore transaction covering both directions, with exact-generation idempotence and fail-closed handling of pending APPLY/reservations.
- [x A] The async coordinator's shared admission semaphore covers its own async paths, but its code explicitly does not cover direct Metadata mutations; the synchronous coordinator uses a separate commit-guard path. A snapshot cut cannot be considered atomic until the host composition supplies one exclusion boundary spanning provider snapshot capture, both Metadata feed vectors, local publication, remote APPLY, and FULL_REPLACE/GC commit.

- [x A] Audited the v3.1 FULL_REPLACE manifest: `FULL_REPLACE_BEGIN` carries one `SourceChangeFeedWatermark`, not the complete set of author feed offsets. The retention checkpoint carries origin frontiers, but origin sequence is not interchangeable with the independent outbound and relay feed sequence domains.
- [x A] A receiver cannot infer the relay cursor for a snapshot-covered origin frontier from its own local relay sequence or from the checkpoint alone. The baseline needs an explicit, signed, snapshot-bound per-author feed cursor vector.
- [ ] C/T Extend the signed retention checkpoint (or introduce a versioned Metadata-baseline control message) with a canonical sorted vector of `(author, feed kind, issuer epoch, stream offset)`, bound to the accepted roster, FULL_REPLACE generation/digest and HLC floor. Capture it under the shared provider/Metadata commit gate.
- [ ] C/T After accepting the checkpoint, each host must atomically apply both outgoing peer-stream baselines and incoming per-author receive cursor/digest baselines. Preserve streams already beyond the snapshot offset; reset only streams covered by the exact installed generation. Reject pending APPLY and immutable reservations.
- [ ] T Add a two-host test proving both stores apply the same baseline vector, compact only contiguous outbound/relay prefixes, survive crash/reopen between baseline commit and compaction, and reclaim the same tombstone only after both Metadata reference scans are empty.
