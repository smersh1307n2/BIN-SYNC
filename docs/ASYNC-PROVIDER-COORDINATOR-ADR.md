# Async Data Provider and Coordinator Architecture

Status: Proposed; implementation gate for process-isolated FLD integration.

Date: 2026-11-11

## Context

BIN-SYNC's production `IDataProvider`, `IProviderRemoteBatchCommitStore`, and `StorageSyncCoordinator` are synchronous. FLD Database IPC is asynchronous. The initial `IAsyncDataProvider` and `IAsyncProviderRemoteBatchCommitStore` contracts now exist, together with in-process adapters and conformance tests.

`InProcessAsyncDataProviderAdapter` is a reference bridge only: it invokes the synchronous provider directly and returns completed `ValueTask` instances. It does not make a synchronous provider process-isolated and must not be used to disguise asynchronous IPC behind blocking calls.

The current FLD IPC facade already supports:
- stable-retry local provider batch append (0x210D);
- contextual atomic remote APPLY (0x210C);
- lease-fenced Outbox reads/ACK;
- immutable snapshots with metadata pages and bounded value chunks;
- durable client operation receipts and host-generation fencing.

It does not yet expose every operation and lease required by the complete BIN-SYNC coordinator.

## Non-negotiable invariants

1. Keep the existing synchronous provider and coordinator as the in-process reference path until the async path passes equivalent conformance and integration tests.
2. No sync-over-async, blocking waits on IPC tasks, or hidden background worker that reports success before durable host admission/commit.
3. A cancellation before durable admission may cancel an operation. Once a request may have committed, retry must use the same durable operation identity and exact payload; cancellation must never be interpreted as proof that the operation did not commit.
4. Remote APPLY must carry the complete immutable `ProviderRemoteBatchCommit` context. Do not project it to (ApplyToken, mutations) across the process boundary.
5. Provider local append must preserve caller ChangeId, HLC and OriginAuthorEpoch; OriginSequence is assigned only at publication freeze.
6. Published-origin stamping, provider feed ACK, Metadata Store publication and Node cache update must remain recoverable and idempotent across every crash boundary.
7. Preserve the lock order documented by `StorageSyncCoordinator`: coordinator → Node domain gates (Storage → Outbox → Meta) → host WAL/FLD gate → shared BIN-SYNC Storage/Metadata gate. Do not acquire the same locks in a different order.
8. Physical receipt, ChangeId and tombstone/ghost GC remain disabled until their independent safe-horizon proofs are complete.

## Required asynchronous capability surface

The initial `IAsyncDataProvider` covers identity resolution, local append/feed/ACK, feed high watermark, snapshots, remote apply and current-state reads. Process isolation additionally requires explicit async counterparts for every optional synchronous capability used by the coordinator:

| Existing synchronous capability | Required async capability |
|---|---|
| `IProviderPublicationMode` | Read/enable durable publication cutover |
| `IProviderAuthorEpochClosureStore` | Close/activate author epochs with durable draft-horizon checks |
| `IProviderPublishedOriginStore` | Atomically stamp immutable published origins |
| `IProviderSnapshotStateApply` | Apply authenticated snapshot state |
| `IProviderFullReplace` | Begin/stage/commit/abort/query FULL_REPLACE |
| `IProviderDurableSnapshotStore` | Query and prune persisted snapshots |
| `IProviderCommitGuard` | Acquire an async-disposable exclusive commit lease |
| `IProviderSnapshotGcGuard` | Acquire/release snapshot GC guard asynchronously |
| `IProviderRemoteBatchCommitStore` | Apply full contextual remote batch asynchronously |

Host-owned GC reference scanning also needs an asynchronous, snapshot-pinned contract before it is used by any future physical-GC implementation. It is not a reason to enable GC now.

Do not make an async adapter implement an optional capability merely to throw at runtime. Capability presence must truthfully mean that the backing provider supports the operation. Use separate capability adapters or an explicit capability set.

## Async commit barrier: unresolved design gate

The current Node domain gates and coordinator gate use thread-affine `Monitor` locks. They cannot be held across an `await` that may resume on another thread. Therefore simply changing provider calls to `await` while retaining the current synchronous commit-guard implementation is invalid.

Before implementing the process-isolated coordinator, choose and prove one of these designs:

- **Async-compatible gate chain:** introduce async-disposable gates for the coordinator, Node domain mutation barrier, host Database/WAL and shared Metadata/Storage. All writers that can conflict with the barrier must participate in the same gate protocol. Synchronous APIs may remain as a separate path, but they must not bypass the barrier.
- **Durable two-phase/revalidation protocol:** take immutable snapshots under short synchronous locks, release them before IPC awaits, persist an idempotent intent where needed, and re-acquire/revalidate generations and watermarks before publication. Every crash cut must recover without exposing a partial cross-store state.

Do not mix these approaches opportunistically. Do not hold a Monitor across an await. The selected design must demonstrate deadlock-free ordering and bounded cancellation/recovery behavior.

## Chosen async consistency strategy — 2026-11-11

Choose the **durable PendingApply/intent plus short-lock snapshot/revalidation** path for the first process-isolated coordinator. Do not replace every existing Node/Metadata Monitor with a shared async gate as a prerequisite for ordinary local feed and remote APPLY.

### Remote APPLY

1. Under the existing short synchronous coordinator/Node gates, stage `MetadataStore.BeginPendingApply` and apply the decoded batch to Node; persist the Node checkpoint before provider APPLY.
2. Release every Monitor/Node lease before any IPC await. The immutable PendingApply contains the exact batch identity, operations, header fingerprint and apply token.
3. Await the FLD contextual `ApplyRemoteBatchAsync` operation. FLD's WAL APPLY_COMMIT and apply receipt make an uncertain retry idempotent.
4. Re-enter only the short MetadataStore gate and call `FinalizePendingApply`. A crash before this step is recovered by replaying the same apply token/batch; a crash after it is handled by the completed receipt.
5. Keep the existing `StorageRelayPublicationGate` SemaphoreSlim around remote APPLY plus relay publication. It serializes the async operation without carrying thread-affine locks across awaits.

### Local feed and publication

1. Capture an immutable FLD source-feed ceiling with the async provider. Enumerate only through that ceiling; writes committed later have higher Outbox sequence and remain for the next pass.
2. Persist the publication/cutover decision under short Node/Metadata locks. Do not hold those locks while sending a batch or awaiting peer ACK.
3. Stamp each published local origin by stable ChangeId through `IAsyncProviderPublishedOriginStore`; FLD ignores a superseded ChangeId and rejects a conflicting stamp for the current one.
4. Persist the returned IPC receipt ClientSequence in the existing Node WAL/commit record before calling `IAsyncProviderOperationReceiptStore.AcknowledgeOperationReceiptsAsync`. This is independent from source-feed `AcknowledgeLocalChangesAsync`; Outbox ACK must never retire caller receipts.
5. Revalidate the durable publication intent and feed watermark before final ACK. If the process crashes, recover from Node WAL, MetadataStore intent and FLD receipts; never invent a new ChangeId/OperationId.

This strategy is suitable only for ordinary local feed and contextual remote APPLY. FULL_REPLACE, snapshot-state replacement and physical GC still require dedicated async-safe guards and complete recovery proofs; keep them disabled in the IPC adapter until those contracts are implemented.

## Coordinator implementation boundary

Add an asynchronous coordinator path that depends on `IAsyncDataProvider` and the async optional capabilities. Do not make the current synchronous `StorageSyncCoordinator` call async methods synchronously.

The async coordinator must cover, at minimum:
- local Outbox scan, publication freeze, immutable origin stamping and feed ACK;
- contextual remote APPLY with durable PendingApply / finalize ordering;
- provider snapshot traversal and Node rehydration;
- restart recovery of pending applies and publication cutover;
- FULL_REPLACE staging, commit and recovery;
- capability-checked composed commit barriers.

The in-process implementation should use adapters only as a test/reference bridge. The production FLD implementation calls `FldDatabaseIpcClient` asynchronously. ClientId is stable host/Node identity; OperationId is derived from the durable ChangeId; FLD assigns ClientSequence atomically and returns it with CommitSeq. The coordinator must persist that returned sequence in the existing Node WAL/commit record before calling the separate receipt ACK. No client-side sequence allocator or second payload journal is required.



## IPC receipt lifecycle update — 2026-11-11

The FLD IPC receipt and synchronization Outbox have distinct consumers and distinct acknowledgement watermarks. `AcknowledgeThroughAsync` (0x2103) retires caller operation receipts; `AcknowledgeOutboxThroughAsync` (0x2106) advances the BIN-SYNC source feed. One must never imply the other.

The async FLD provider now implements `IAsyncProviderOperationReceiptStore`. `AppendLocalChangeWithReceiptAsync` returns the Host-assigned ClientSequence, and `AcknowledgeOperationReceiptsAsync` performs the independent receipt ACK. The async coordinator must persist the returned sequence in its existing Node WAL/commit record before ACK. On uncertain append response, retry with the same ChangeId-derived OperationId and exact payload; FLD returns the original sequence. Do not add a second pending payload journal or infer receipt completion from Outbox publication progress.

## Required verification before marking complete

1. Run the provider conformance suite against both the in-process adapter and the FLD IPC-backed async provider.
2. Two independent FLD Host processes and two independent BIN-SYNC coordinators exchange local changes in both directions.
3. Retry every mutating IPC operation after response loss; prove no duplicate Outbox append, remote APPLY or publication stamp.
4. Kill/restart the FLD Host at each boundary: before request admission, after WAL flush, after receipt commit, after response send, during snapshot traversal, and during remote APPLY.
5. Prove Outbox feed cursor, ChangeId, HLC, OriginAuthorEpoch and frozen OriginSequence survive restart exactly.
6. Prove remote apply preserves sender peer/transport epoch, issuer/author epoch, batch id, cursor range, digests, RecordsRoot, relay subset and ProtocolStateDelta.
7. Prove snapshot generation and source-feed watermark are atomic; FULL_REPLACE remains invisible until commit and recovers after restart.
8. Exercise concurrent clients, cancellation, client reconnect, host-generation fencing, lease expiry/release and deadlock timeouts on Linux, Windows and macOS.
9. Run the exact-head BIN-SYNC and FLD-DB CI workflows. No stage is complete from compilation alone.

## Assembly and storage boundaries

The runtime remains exactly two DLLs:
- `Fld1.Storage.dll`: generic storage and local IPC primitives.
- `Fld1.Database.dll`: FLD database, WAL/manifest integration, IPC protocol and dispatcher.

Database Host remains a separate executable, not a third runtime DLL. BIN-SYNC owns transport, delivery, ACK and causal metadata; FLD owns durable application field state, WAL recovery and its apply receipt. Do not add a duplicate pending journal or relay journal in FLD.

## Contract implementation progress — 2026-11-11

The first explicit optional async capability contracts are now present in `BinSync.Core/AsyncProviderCapabilities.cs`:
- snapshot-state apply;
- provider publication cutover;
- immutable published-origin stamping;
- author-epoch closure/activation;
- FULL_REPLACE lifecycle and progress;
- durable snapshot lookup/watermark/pruning;
- async commit and snapshot-GC leases;
- host-owned async GC reference scan with a pinned disposable view.

These are API contracts only. They are not yet implemented by the FLD IPC client or an async coordinator, and they do not authorize GC. Optional capability presence must remain truthful; do not add a catch-all adapter that implements unsupported capabilities and throws at runtime.

The async commit lease contract explicitly returns `IAsyncDisposable` so a future coordinator can release it after awaited IPC work. This does not make existing `Monitor`-based Node/Storage gates await-safe. The coordinator implementation remains blocked until every conflicting writer participates in a proven async-compatible gate protocol or a durable intent/revalidation protocol.


## Snapshot capability audit correction — 2026-11-11

A closer read of the current FLD IPC implementation establishes that two items previously described as missing are already available through the immutable snapshot contract:

- `OpenSnapshotAsync` returns `SourceFeedWatermark` captured with the snapshot generation. No separate live high-watermark RPC is required for snapshot-consistent traversal.
- `ReadSnapshotPageAsync` exposes all current-field metadata required by `ProviderMutation`; `ReadSnapshotValueChunkAsync` reads the value from that same pinned generation. This supports a correct snapshot-backed `ReadCurrentAsync` fallback by bounded traversal, although it is not an efficient point lookup.

The next provider implementation should use these existing operations and explicitly document the performance cost. Do not expand the IPC protocol with a duplicate watermark operation. The genuinely missing capabilities remain published-origin stamping, durable publication/author-epoch transitions, snapshot-state/FULL_REPLACE operations, and the async-safe composed commit barrier. A dedicated versioned point-read may be added later only if profiling justifies it and it preserves generation consistency.

## IPC-backed async provider implementation update — 2026-11-11

The first separate-process provider slice is now implemented in the optional cross-repository sample:

- `FldDatabaseIpcAsyncDataProvider` implements the core `IAsyncDataProvider` contract using asynchronous Database IPC for key resolution, identity-preserving local append, Outbox scan/ACK, snapshots and point reads.
- `FldDatabaseIpcRemoteBatchApplyStore` implements `IAsyncProviderRemoteBatchCommitStore`; it preserves the complete remote batch context. Context-free APPLY remains rejected.
- FLD IPC 0x210F/0x2110 provides indexed current metadata plus bounded value chunks fenced by ChangeId. This removes the need to scan the whole snapshot for ordinary point reads.
- FLD IPC 0x2111 assigns the local-provider receipt sequence inside the Host commit lock. The caller reuses stable ClientId and OperationId derived from ChangeId; the existing durable receipt and fingerprint provide response-loss/restart idempotency. No second client-side sequence journal is required.
- FLD IPC 0x2112 performs provider-aware Outbox ACK and retires only the contiguous auto-sequenced receipt prefix covered by the acknowledged feed. 0x2113 returns the Host-authoritative LocalAuthor, which the async provider verifies before acquiring its feed lease.

This implementation closes the core provider/apply transport boundary, but it is not the async coordinator. It does not implement optional async publication cutover, published-origin stamping, author-epoch lifecycle, snapshot-state apply, FULL_REPLACE, durable snapshot retention, async commit guards or GC reference scans. Do not advertise any of those capabilities from this adapter.

## Lock audit result — 2026-11-11

The current synchronous lock chain is confirmed in code:

1. `StorageSyncCoordinator` uses a thread-affine Monitor for its coordinator gate. `AcquireApplicationCommitGuardCore` enters that gate first.
2. It then calls `Node.EnterAllGates`, which acquires Node domain gates in Storage → Outbox → Meta order. Node write/apply paths also enter those Monitor locks directly.
3. The optional host lease is acquired after Node gates. The in-process FLD provider/host lease is Monitor-based.
4. FULL_REPLACE then acquires the Metadata Store transaction gate; the provider commit guard is acquired last. `StorageMetadataStore` methods and its FULL_REPLACE lease use a Monitor on the store gate.

Therefore a SemaphoreSlim added only to the IPC dispatcher is insufficient: direct Node and Metadata Store writers would bypass it, and holding any existing Monitor across an IPC await is invalid. A future async commit barrier must either (a) replace/augment every conflicting writer path with one shared sync+async gate protocol, or (b) use the durable PendingApply/intent plus short-lock snapshot/revalidation protocol and prove each crash cut. Do not implement a partial lease that serializes only IPC requests.

Until that design is selected and tested, `FldDatabaseIpcAsyncDataProvider` deliberately does not implement `IAsyncProviderCommitGuard`, and the synchronous `StorageSyncCoordinator` must not consume it. Physical GC and FULL_REPLACE remain unavailable through this async adapter.


## Published-origin IPC capability update — 2026-11-11

FLD Database IPC operation 0x2114 now exposes the existing identity-only `CommitPublishedOrigins` WAL path. The process-isolated sample implements `IAsyncProviderPublishedOriginStore` and maps BIN-SYNC's immutable `(ChangeId, OperationOrigin)` stamps without creating another Outbox row or pending journal. The FLD operation is idempotent for an exact stamp, ignores a superseded ChangeId, and rejects conflicting origins for a still-current ChangeId.

This is a real async optional capability, but it is not sufficient for safe end-to-end publication by itself: without the composed async commit barrier, a caller cannot claim atomicity across Metadata Store publication and the FLD current-state projection. The remaining optional capabilities and coordinator lock gate remain open.


## First asynchronous remote-APPLY receive slice — 2026-11-11

`SyncSession.ReceiveBatchAsync` now accepts an asynchronous before-ACK callback. The callback is awaited after Node applies the decoded batch and before the ACK frame is constructed/sent. The synchronous callback remains supported; supplying both is rejected.

`StorageSyncCoordinator.ReceiveBatchWithAsyncProviderBeforeAckAsync` composes the existing durable PendingApply staging with `IAsyncProviderRemoteBatchCommitStore`:

1. Stage the complete immutable batch and per-operation decisions in Metadata Store.
2. Let SyncSession apply the decoded batch to Node.
3. Persist the Node checkpoint before crossing the IPC boundary.
4. Await contextual provider APPLY with the original sender, transport epoch, issuer/author epoch, BatchId, cursor range, digest chain and RecordsRoot.
5. Validate the returned ApplyToken, finalize the Metadata receipt, and return to SyncSession.
6. SyncSession emits the transport ACK only after the awaited callback succeeds.

No thread-affine Monitor is held across the provider await. A per-coordinator `SemaphoreSlim` serializes the async provider/finalization phase. The stable ApplyToken and provider-side durable receipt are the retry identity after an uncertain response.

This is deliberately a narrow first slice, not a complete async coordinator. The current orchestration assumes one receive stream per namespace/coordinator. Multiple independent sessions sharing one coordinator still require namespace-wide admission before production use. Local feed publication, Node-WAL persistence of the IPC receipt ClientSequence, independent receipt ACK, async snapshot rehydration, FULL_REPLACE and all physical GC remain out of scope and disabled. Exact-head CI plus response-loss/restart tests are required before this path is considered production-ready.


## Async PendingApply recovery update — 2026-11-11

The first asynchronous receive slice now has a matching startup recovery method:
`StorageSyncCoordinator.RecoverPendingAppliesAsync(IAsyncProviderRemoteBatchCommitStore, Action<Node>, CancellationToken)`.

Recovery follows the same durable ordering as live receive:

1. Read the durable PendingApply set while holding only the short Metadata/Coordinator gate.
2. Require `ReceiveStateOwnedByMetadataStore`, the exact retained `WirePayload`, a full-header fingerprint, and a complete operation-decision mask. Missing context is a hard failure; async recovery never downgrades to `IDataProvider.ApplyRemoteBatch(ApplyToken, mutations)`.
3. Replay the exact payload into Node with the persisted decision mask and checkpoint Node before provider IPC.
4. Verify the decoded batch against the stored namespace, BatchId, digest chain, header fingerprint and deterministic ApplyToken.
5. Await contextual provider APPLY using the original sender/transport epoch, issuer epoch, BatchId, cursor range, digests and RecordsRoot.
6. Validate the returned ApplyToken, then finalize the Metadata PendingApply receipt and validate its durable author/header fingerprint.

The async admission semaphore serializes this recovery pass against the coordinator's async receive path. The implementation does not hold a thread-affine Monitor across provider IPC. A provider commit whose response is lost is retried with the same ApplyToken and immutable context; provider idempotency remains mandatory.

The recovery API is a startup barrier, not a claim that every synchronous writer has joined the async admission protocol. Hosts must run it before accepting sync sessions or application writes. The new interruption test stages PendingApply and interrupts before Node mutation, then verifies replay into Node/provider and metadata finalization. Response-loss-after-provider-commit and multi-process restart fault injection remain required before production readiness.
