# Changelog — BinSync.Core

## 2.2.23 — 2026-11-03

**BatchResult is a struct (zero heap result on ApplyBatch).**

- Public API: `class BatchResult` → `struct BatchResult`
- Idempotent replay no longer allocates a result object on the heap
- `DeliverBatch` still returns `BatchResult?` (nullable value type)

---

## 2.2.22 — 2026-11-03

**CreateDeltaBatch speed (no alloc regression).**

- Call AliasOfInternal / EnsureAliasInternal / SignBatchForInternal under existing gates (no re-entrant lock per entry)
- Cache self-alias once for the common all-self-authored path
- AliasOfInternal uses EmptyAliasMap / EmptyRevAlias instead of new empty Dictionary on miss

N=500 CreateDelta: ~280 µs / 64 KB (was ~480 µs after 2.2.21; baseline ~359 µs / 554 KB)

---

## 2.2.21 — 2026-11-03

**Fix encode latency regression (ArrayPool double-copy).**

Root cause: EncodeBatchPayload/EncodeOp/EncodeRecordsArray rented a pool buffer then **always** copied into a new exact-sized byte[]. That added an extra full-payload memcpy on every encode vs the prior path which returned the estimate buffer when the size matched.

- byte[] encode path: allocate estimate once; return as-is when exact
- Span encode path: Writer.Rent scratch → one copy into destination
- EmptyAliasMap static to avoid empty Dictionary alloc in CreateDeltaBatch

---

## 2.2.20 — 2026-11-03

**PeerState.PeerNodeId is NodeId; residual hex sorts cleaned.**

- PeerState.PeerNodeId typed as NodeId
- DrainAndRetryCausal dedupes by HashSet<BatchId> (no ToHexString)
- GcNotice / Membership member sort via span compare (no hex alloc)

---

## 2.2.19 — 2026-11-03

**Transport PeerNodeId is NodeId; session API typed.**

- ISyncTransport / PipeTransport / StreamSyncTransport / TcpSyncSession.PeerNodeId → NodeId
- ApplyAliasMap / BuildAliasMapTable take NodeId peer

---

## 2.2.18 — 2026-11-03

**HelloPayload / AliasMapEntry use NodeId.**

- Session wire structs carry typed NodeId (no bare byte[] on the session control plane)
- MergeAliasMap uses HashSet<NodeId> (no hex-string keys)

---

## 2.2.17 — 2026-11-03

**SessionWire ArrayPool encode; FragmentReassembler BatchId keys.**

- EncodeHello/HelloAck/AliasMap/Error rent payload buffers from ArrayPool
- FragmentReassembler keys by BatchId (no Convert.ToHexString on hot path)

---

## 2.2.16 — 2026-11-03

**KeyRotation + fragments on pooled send path.**

- SendKeyRotationAsync uses SendFrameAsync (ArrayPool frame)
- SendFragmentedAsync splits and sends via SendFrameAsync with FLAG_FRAGMENT
- FragmentCodec.EncodeOne / Split: no intermediate body copy; ArrayPool encode buffer
- SendFrameAsync accepts flags

---

## 2.2.15 — 2026-11-03

**ReadFrame pooled header; PipeTransport uninit copy; TcpSync SendFrameAsync everywhere.**

- FrameStream.ReadFrameAsync rents header from ArrayPool
- PipeTransport uses GC.AllocateUninitializedArray for channel ownership copies
- Reconcile/GC/IBLT/Membership/Schema send via SendFrameAsync (ArrayPool frames)

---

## 2.2.14 — 2026-11-03

**TcpSync pooled frames; StreamSync zero-copy send; EncodeAck pool.**

- FrameStream.WriteFrameAsync accepts ReadOnlyMemory (no forced copy)
- StreamSyncTransport.SendAsync no longer ToArray()s the packet
- TcpSync Push/Receive builds frames into ArrayPool buffers
- EncodeAck uses ArrayPool + typed BatchId/NodeId (no dual ToArray)

---

## 2.2.13 — 2026-11-03

**FieldState.Value is ReadOnlyMemory<byte>.**

- Avoids forcing owned byte[] on every field in storage when the source is already a slice
- Merge/digests/compare operate on spans
- Snapshot branch: main-copy-2.2.12

---

## 2.2.12 — 2026-11-03

**In-process DeliverBatch without frame tax; HeldCausalBatch typed.**

- Transport.DeliverBatch applies payload and ACK body directly (no BuildFrame/ParseFrame for loopback)
- HeldCausalBatch.SenderPeer is NodeId; ApplyBatchCausal(NodeId) overload

---

## 2.2.11 — 2026-11-03

**CausalDeps typed; BuildFrame Span API.**

- OutboxEntry.CausalDeps is List<BatchId>
- CausalWire / persistence encode B128(BatchId) without CanonicalB128 copies
- ApplyBatchCausal no longer converts deps at the gate
- FrameCodec.BuildFrame accepts ReadOnlySpan and can write into a caller Span

---

## 2.2.10 — 2026-11-03

**CausalTracker on BatchId keys; EncodeRecordsArray ArrayPool.**

- CausalTracker known/pending/edges use HashSet/Dictionary of BatchId (no hex-string keys on hot path)
- PendingCausalOp.ChangeId/Deps are BatchId
- EvaluateBatch typed overload; Node.ApplyBatchCausal uses it
- EncodeRecordsArray rents scratch from ArrayPool

---

## 2.2.9 — 2026-11-03

**ArrayPool encode path + CausalTracker typed ids.**

- EncodeBatchPayloadInternal / EncodeOp rent scratch from ArrayPool; return exact-sized byte[]
- CausalTracker.Key/NoteApplied/EnqueuePending overloads for BatchId (stackalloc hex key)
- Snapshot branch: main-copy-2.2.8 on GitHub

---

## 2.2.8 — 2026-11-03

**OutboxEntry.RecordId / ChangeId are value types.**

- RecordId is RecordId; ChangeId is BatchId
- MaterializeEntries only copies Payload (ids/authors already owned as structs)
- WriteRecordsArray / persistence write B128(RecordId/BatchId) without ToArray
- OwnQueueHolds / SeenChanges use typed equality

---

## 2.2.7 — 2026-11-03

**Digest16 + OutboxEntry.AuthorNodeId as NodeId.**

- New Digest16 value type for PrevBatchDigest / RecordsRoot / BatchDigest and chain maps
- LastBatchDigest / LastStreamDigest / PeerState.Ids prev store Digest16
- OutboxEntry.AuthorNodeId is NodeId (no ROM/byte[] on author hot path)
- CreateDeltaBatch stream selection uses NodeId dictionaries

---

## 2.2.6 — 2026-11-03

**Author stream + ACKs typed.**

- Batch.AuthorStreamNodeId / DurableAuthor are NodeId
- BatchResult.DurableAuthor is NodeId
- LastStreamAuthor values are NodeId
- AcksOut is List<BatchId> (no ToArray on apply)
- ApplyBatch ResolveAuthor / ApplyOpInternal use NodeId end-to-end
- EncodeAck overload for BatchId + NodeId

---

## 2.2.5 — 2026-11-03

**Batch.IssuerNodeId / Batch.BatchId are typed (NodeId / BatchId).**

- In-memory batch DTO no longer carries byte[] identities for issuer and batch id
- BatchResult.BatchId is BatchId; SyncException accepts BatchId
- ComputeBatchDigest / encode use stackalloc B128(NodeId/BatchId)
- Digests (prev/root/batch) remain byte[16] (wire BLAKE3 truncated)

---

## 2.2.4 — 2026-11-02

**FieldState.AuthorNodeId and Record.LifecycleAuthor are NodeId.**

- LWW / merge / digests compare authors as value types (no byte[] content compare)
- HlcOps.CompareHlc(NodeId) overload
- Persistence and op-encoding convert at the boundary only

---

## 2.2.3 — 2026-11-02

**More hot-path typed ids.**

- `Record.RecordId` is `RecordId` (no duplicate byte[] of the storage key)
- `PeerState.Ids` keyed by `(NodeId, start, end)` with `BatchId` values
- `AuthorStreamId()` for zero-alloc id-cache lookup; digests write RecordId via stackalloc
- LocalWrites `new Record(recordId)` without `.ToArray()`

---

## 2.2.2 — 2026-11-02

**Typed map keys on the apply/sync hot path (allocation cut).**

- Inbox / LastBatchDigest / LastStreamDigest / SeenChanges / ReceivedStreams use NodeId/BatchId
- ApplyBatch no longer allocates senderPeer.ToArray() for session maps
- Persistence updated; wire still b128

---

## 2.2.1 — 2026-11-02

**Hot-path allocation hygiene.**

- `ApplyBatch`: lazy `senderPeer.ToArray()` only when byte[]-keyed maps need it; use `NodeId` for session maps
- `LocalWrites` / `OwnQueueHolds`: span compares (`RecordId.EqualsBytes`, `NodeId.EqualsBytes`) instead of `ToArray` on every scan
- `CreateDeltaBatch`: avoid extra `Mem.ToArray` when forming `NodeId` for alias lookup
- Allocation regression tests: ApplyBatch duplicate replay + WriteField steady-state budgets

Remaining `ToArray` on ownership boundaries (Record ctor, SeenChanges keys, crypto APIs) is intentional.

---

## 2.2.0 — 2026-11-02

**Breaking: engine state encapsulation + limits + TCP read timeout.**

- `Storage`, `Outbox`, `Inbox`, alias maps, `PeerStates`, `SeenChanges`, digests, quarantine, etc. are **`internal`** (tests/samples via `InternalsVisibleTo`)
- Public query API: `TryGetRecord`, `TryGetField`, `GetRecordCount`, `GetOutboxCount`, `RegisterLoopbackPeer`
- `Node.Limits` (`ResourceLimits?`) enforced on outbox enqueue / seen-set / storage writes
- `StreamSyncTransport.ReadTimeout` + `TcpSyncSession.ReadTimeout` for idle/stuck peers
- Bare `catch` in E-13 outbox scans documented (skip corrupt payloads)
- `Ns()` is internal

Hosts that mutated `node.Storage` / `node.Outbox` directly must use engine APIs or request InternalsVisibleTo.

---

## 2.1.13 — 2026-11-02

**XML-doc: finish remaining thin summaries.**

- Replaced leftover template/humanize/`Name: words` summaries across Codec, Digests, Merge, Model, Production (IBLT, Reconcile, TcpSync, Trust, WAL, …)
- Non-Crypto public surface: no `Represents X` / single-token / colon-humanize stubs left

---

## 2.1.12 — 2026-11-02

**XML-doc text quality pass.**

- Replaced ~650 template summaries (`Represents X` / `Name.`) with meaningful descriptions
- Codec, Writer/Reader, Ids, Node, Batch, Transport, Ops, Frame, HLC, Production surface covered
- Weak template count on non-Crypto public surface: 0

---

## 2.1.11 — 2026-11-02

**Public API XML-doc coverage + exception hygiene.**

- Nearly complete `/// <summary>` on non-Crypto public surface (~980 members)
- `WatermarkGate` documented; empty `InvalidOperationException` messages filled
- Style policy unchanged (`docs/STYLE.md`)

---

## 2.1.10 — 2026-11-02

**Review follow-up: exceptions, StyleCop honesty, docs.**

- `SyncException` / `FrameException`: standard `(code)`, `(code, message)`, `(code, message, inner)` constructors; properties documented
- Empty `catch (SyncException)` in relay-eligibility: explicit comment (must not fail the batch)
- StyleCop: high-signal rules kept live (`SA1107`); layout/order/`this.`/member-doc noise listed in `docs/STYLE.md` — no “fake strict” claim
- XML-doc: type-level + more DTO members; CS1591 still allowed for incomplete members
- `nodeId` length → `ArgumentException`

---

## 2.1.9 — 2026-11-02

**StyleCop.Analyzers + public API XML-doc anchors.**

- PackageReference `StyleCop.Analyzers` + `stylecop.json` + `.editorconfig`
- SA* suppressed in build (TreatWarningsAsErrors) — enable rules incrementally
- Vendored `Crypto/**` excluded from StyleCop restyle
- Type-level docs: `Node`, `Batch`, `Transport`, `Ops`, `NodeId`/`RecordId`/`BatchId`, DTOs
- `PublicApiDocs.cs` partials for IntelliSense; CS1591 still allowed for incomplete members

---

## 2.1.8 — 2026-11-02

**API hygiene + docs/CI (code review).**

- `NodeId` / `Clock` init-only; `Epoch` internal set; remove public `Storage` wholesale setter
- Dictionary surface (`Outbox`, aliases, `PeerStates`, …) is get-only (mutate in place)
- `Gate` marked `[Obsolete]` → use `EnterAllGates()`
- README / PRODUCTION-STATUS → 2.1.8; DecodeAck catch documented
- CI: test timeout 25m; optional coverage line-rate ≥50% gate
- `AllowUnsafeBlocks` false (no unsafe outside stackalloc)

---

## 2.1.7 — 2026-11-01

**Fix duplicate-apply ACK author stream (hang / test host abort).**

- On idempotent replay (`Inbox` hit), `BatchResult.DurableAuthor` is filled from
  `LastStreamAuthor` (or batch durable author) so `ProcessAck` advances the correct
  per-author watermark (Errata E-4).
- Without this, lost-ACK replay + `SyncRound` never drained → hang → testhost crash
  after ~257 tests (17 `InvariantsAndAdvanced` never finished).

274 tests green.

---

## 2.1.6 — 2026-11-01

**Safe decode by default.**

- Public `DecodeBatchPayload` always `MaterializeEntries()` (owned buffers; no frame lifetime)
- `ApplyBatchPayload(byte[], NodeId)` — zero-copy decode+apply; batch does not escape
- Transport / Loopback / TcpSync use `ApplyBatchPayload`
- `BatchResult` carries `CursorEnd`, `NamespaceId`, `DurableAuthor` for ACK

274 tests green.

---

## 2.1.5 — 2026-11-01

**OutboxEntry on ReadOnlyMemory — decode without per-entry copies.**

- `OutboxEntry.Payload/RecordId/ChangeId/AuthorNodeId` → `ReadOnlyMemory<byte>`
- `DecodeBatchPayload` slices into the frame buffer (`B128Memory` / `BytesMemory`)
- `Mem.ToArray` when ownership is required; `NodeId`/`RecordId` accept Memory
- Lifetime: keep the frame alive while using decoded `Batch.Entries` slices

274 tests green.

---

## 2.1.4 — 2026-11-01

**Reader / DecodeOp / EncodeOp on Span & ReadOnlyMemory (wire unchanged).**

- `Reader` over `ReadOnlyMemory<byte>`; `TakeSpan` / `B128Span` / `BytesSpan`
- `Codec.DecodeVarint(ReadOnlySpan<byte>)`
- `DecodeBatchPayload(ReadOnlyMemory<byte>)`
- `DecodeOp(ReadOnlyMemory<byte>)`
- `EstimateEncodeOpSize` + `EncodeOp(..., Span<byte>)`; `byte[] EncodeOp` uses exact-sized buffer

274 tests green.

---

## 2.1.3 — 2026-11-01

**Span / ArrayPool for encode + CreateDelta (wire unchanged).**

- `EncodeBatchPayload(Batch, Span<byte>)` + `EstimateBatchPayloadSize`
- `byte[] EncodeBatchPayload` writes into exact-sized buffer (fixed Writer, no growth)
- `Writer` fixed-buffer / `ArrayPool` rent support
- `CreateDeltaBatch` reuses outbox entries when origin already matches (no second entry list)
- `ComputeRecordsRoot` / `ComputeBatchDigest` via ArrayPool (no steady LOH)

274 tests green.

---

## 2.1.2 — 2026-11-01

**Perf (decode + CreateDeltaBatch), wire unchanged.**

- `DecodeBatchPayload`: no tail `Raw` + no intermediate tuple list — one copy into `OutboxEntry`
- `ComputeRecordsRoot`: incremental BLAKE3 (no full records buffer)
- `TryCloneWithOrigin`: share outbox payload when origin already matches
- Local outbox stamps `origin=1` (self alias) so the common session path avoids payload clones

274 tests green.

---

## 2.1.1 — 2026-11-01

Performance patch. Wire format and public API unchanged. **274** tests green.

- `CreateDeltaBatch`: stamp compact-op `origin` by cloning the payload instead of decode/encode
- `Writer` uses a byte buffer; exact-sized encodes skip the extra copy
- `EncodeBatchPayload` / `recordsRoot` sized exactly

---

## 2.1.0 — 2026-10-31

Completes the **2.0 API-shape** migration (still wire-compatible with 1.x).

### Breaking (source vs 2.0.0)
- `Node.NodeId` is **`NodeId`** (not `byte[]`)
- Session/maps: `PeerStates`, `KnownEpochs`, `LastStreamAuthor`, `_outboxSeq` keyed by **`NodeId`**
- `CreateDeltaBatch` / `SignBatchFor` / `AdvanceStream` / `AuthorEpoch` / `NextOutboxSeq` take **`NodeId`**
- Alias APIs already on `NodeId` (full path)

### Helpers
- `NodeId.EqualsBytes` / `RecordId.EqualsBytes` for wire compares without alloc

### Tests
- **274** green

---

## 2.0.0 — 2026-10-31

**Major (API shape only).** Wire protocol unchanged vs 1.0.1.

**Target:** `net10.0` · **Package:** `BinSync.Core` `2.0.0` · **Tag:** `v2.0.0`

### Breaking (source)
- Public **fields → properties** on DTOs and `Node` maps (`OutboxEntry`, `Batch`, `PeerState`, `BatchResult`, `Record`, …)
- `WriteField` / related local-write APIs take **`RecordId`** (implicit from `byte[]`)
- `ApplyBatch` sender parameter is **`NodeId`** (implicit from `byte[]`)
- New **`BatchId`** struct (alongside `NodeId` / `RecordId`); `Writer.B128` overloads for typed ids

### Non-breaking
- Same batch wire layout; 274 tests green; samples still work via implicit conversions

### Migration
- Recompile consumers; replace field initializers only if using reflection on fields
- Prefer `RecordId` / `NodeId` at call sites; `byte[16]` still accepted via implicit conversion

---

## 1.0.1 — 2026-10-31

**Patch release** on top of `1.0.0`. **No public API breaks.**

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.1` · **Tag:** `v1.0.1`

### Hygiene / structure
- Node engine split into partials (`Alias`, `LocalWrites`, `DeltaBatch`, `ApplyBatch`, `BatchCodec`, `Helpers`, …)
- Dead diagnostic `Diag` harness removed; duplicate byte-array comparers unified
- `dotnet format` on engine partials + tests; test project warnings-as-errors

### Engine (internal, wire-compatible)
- ApplyBatch: copy-on-write overlays for touched records; single `DecodeOp` pass; leaner digests
- Fewer `ToArray` on digest / writer span paths (encode-friendly)

### Repo / CI
- CI: XPlat Code Coverage (cobertura artifact `coverage`)
- Sample: `samples/TwoNodeLoopback` (MakeSession + SyncRound)
- Tests: **274** green

### Unchanged contract
- Same public surface as `1.0.0` / [PUBLIC-API.md](PUBLIC-API.md)
- Wire-compatible with 1.0.0 / rc.2 / rc.1

---

## 1.0.0 — 2026-10-31

**First stable release.** Public API frozen as in [PUBLIC-API.md](PUBLIC-API.md).

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.0` · **Branch:** `main`

### Product
- Promoted from `1.0.0-rc.2` without API breaks
- 277 tests green; CI `BinSync.Net` workflow green on the RC tip

### Engine (from rc.2 / post-alpha.7 main)
- Domain sub-gates + `EnterAllGates`; alias maps on `NodeId`
- Hot-path encode/decode/apply optimisations
- Full Merge §8, opt-in E2EE / causal / schema, TrustStore TOFU/PreShared/CA-as-nodeId

### Unchanged contract
- Wire-compatible with rc.1 / rc.2 / alpha.6 / alpha.7
- Full X.509 path validation remains out of scope

### Optional next
- nuget.org publish
- Post-1.0: X.509 PKI (if needed)

---

## 1.0.0-rc.2 — 2026-10-30

**Second release candidate.** Same API freeze as rc.1; engine hardening only.

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.0-rc.2` · **Branch:** `main`

### Engine
- Domain sub-gates (`_lockStorage` → `_lockOutbox` → `_lockMeta`) + `EnterAllGates` / `AllGateScope`
- Alias maps migrated to `NodeId` keys and `(NodeId Id, int Epoch)` values
- Persistence snapshots under `EnterAllGates`; Diag/Hardening updated
- 277 tests green; batch hot-path benches show no regression vs pre-migration tip

### Unchanged contract
- Public API same as `1.0.0-rc.1` / `PUBLIC-API.md`
- Wire-compatible with alpha.6/alpha.7 and rc.1

### Next
- Soak as `v1.0.0-rc.2` → promote to **`1.0.0`** without further API breaks

---

## 1.0.0-rc.1 — 2026-10-29

**First release candidate toward 1.0.** API surface frozen as documented in [PUBLIC-API.md](PUBLIC-API.md).

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.0-rc.1` · **Branch:** `main`

### Product
- SemVer policy and public API freeze for the 1.x line
- Docs aligned: PRODUCTION-STATUS, PUBLIC-API, README, this changelog

### Engine (carried from post-alpha.7 main)
- Domain sub-gates (`_lockStorage` → `_lockOutbox` → `_lockMeta`) + `EnterAllGates`; alias maps on `NodeId`
- EncodeBatchPayload single-pass + Writer capacity / zero-alloc Varint write
- DecodeBatchPayload in-place records; Reader fixed-width zero-alloc reads
- ApplyBatch decode-once + O(1) change flags; ComputeRecordsRoot single buffer
- Lazy `FieldTombs`; State capacity hint; Blake3 truncated digest without extra 32-byte heap hop
- Zero-alloc FieldWrite framing probe in `Ops`

### Unchanged contract
- Protocol wire compatible with alpha.6/alpha.7
- Trust CA mode remains node-id allow-list (not full X.509)
- Causal / E2EE / schema / signature gates remain **opt-in**

### Next
- Soak as `v1.0.0-rc.1` → promote to **`1.0.0`** without further API breaks

---

## 0.1.0-alpha.7 — 2026-10-28

Packaging / release hygiene cut on `main` (engine same as alpha.6 performance pass).
**Target:** `net10.0` · **Package:** `BinSync.Core` `0.1.0-alpha.7`

### Package
- Version bump `0.1.0-alpha.6` → `0.1.0-alpha.7`
- PRODUCTION-STATUS / release notes aligned with tagged alpha.6 engine tip
- One-shot CI path for annotated tag + GitHub prerelease

### Engine
- No protocol changes vs alpha.6 (comparers, zero-alloc codec, HLC µs remain)

### Package id
```
PackageId: BinSync.Core
Version:   0.1.0-alpha.7
```
Single external dependency: `Blake3.Managed` 1.10.0.

## 0.1.0-alpha.6 — 2026-10-28

Production performance pass on `main` after alpha.5 protocol surface.
**Target:** `net10.0` · **Package:** `BinSync.Core` `0.1.0-alpha.6`

### Engine / hot paths
- Shared comparer singletons; `HashCode.AddBytes`; Codec WriteVarint / stackalloc; HLC µs

## 0.1.0-alpha.5 — 2026-10-24

Production-hardening cut. See prior release notes under `docs/RELEASE-alpha.5.md`.
