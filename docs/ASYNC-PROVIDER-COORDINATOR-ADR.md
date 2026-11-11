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

## Coordinator implementation boundary

Add an asynchronous coordinator path that depends on `IAsyncDataProvider` and the async optional capabilities. Do not make the current synchronous `StorageSyncCoordinator` call async methods synchronously.

The async coordinator must cover, at minimum:
- local Outbox scan, publication freeze, immutable origin stamping and feed ACK;
- contextual remote APPLY with durable PendingApply / finalize ordering;
- provider snapshot traversal and Node rehydration;
- restart recovery of pending applies and publication cutover;
- FULL_REPLACE staging, commit and recovery;
- capability-checked composed commit barriers.

The in-process implementation should use adapters only as a test/reference bridge. The production FLD implementation must call `FldDatabaseIpcClient` asynchronously and persist its IPC ClientId, operation ids and contiguous ClientSequence across reconnects.

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
