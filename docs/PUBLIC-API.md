**Current package: 3.0.0** — Node identity is ctor-only; `EnterAllGates()` is the supported aggregate gate scope.

# BinSync.Core — Public API surface

**Package:** `BinSync.Core` **`3.0.0`** · **Target:** `net10.0`  
**Protocol:** **BIN-SYNC CORE**  
Normative: **BIN-SYNC-SPECIFICATION.txt v3.0.0** (breaking clean cut; no v2.x wire/storage compatibility or legacy session API).  
Status: [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md) · Changelog: [CHANGELOG.md](CHANGELOG.md) · Naming: [NAME.md](NAME.md).

## SemVer policy (3.x)

| Change | Version impact |
|--------|----------------|
| Wire/protocol break, remove/rename public type or member | **major** |
| New public API, additive frame/op support | **minor** |
| Bugfix, docs, internal optimisations, non-breaking type-shape fixes agreed before external consumers | **patch** |

**Public** = types and members in this document and in XML docs shipped with the package.  
Everything `internal`, under `*.Tests`, `bench/`, and unmarked helpers is **not** a compatibility contract.

**Breaking API note:** This 3.0.0 package removes `TcpSyncSession`, changes the TCP listener's session return type, removes unsafe low-level sequence reservation, and replaces untyped Outbox helpers with typed operation overloads.

Opt-in surfaces (causal, E2EE, schema, trust gates) stay opt-in: enabling them is configuration, not a silent default change.

**GC extension note:** signed namespace rosters, member applied-origin frontiers, authority-signed retention checkpoints and FULL_REPLACE wire frames are proposed for a separately negotiated v3.1 extension (`BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md`). They are not v3.0 public/wire contracts; ChangeId and tombstone/Ghost GC remain disabled.

---

## Shape of the 3.0 API (vs early 2.x / 1.x)

Breaking hygiene (no wire/protocol change):

| Area | Contract |
|------|----------|
| DTO fields | Public **properties** (not bare fields): `OutboxEntry`, `Batch`, `Record`, `SnapshotField`, `SnapshotRecord`, `SnapshotChunkMessage`, `GcNoticeMessage`, `MembershipEntry`, `MembershipMessage`, … |
| Identities | Value types: **`NodeId`**, **`RecordId`**, **`BatchId`**, **`Digest16`** (value equality / hash). Implicit conversion from/to `byte[]` remains available for identity types; use `From(...)` / `ToArray()` when an explicit boundary is clearer. |
| `BatchResult` | **`struct`** (not a class) — ApplyBatch/ApplyBatchPayload return by value; no heap result object on the success path. |
| Peer identity on transport/session | **`ISyncTransport.PeerNodeId`**, **`HelloPayload.NodeId`**, **`AliasMapEntry.NodeId`**, **`SyncSession.PeerNodeId`** are **`NodeId`**. Internal peer cursor state is not exposed. |
| Engine collections | **`Storage`**, **`Outbox`**, **`Inbox`**, alias maps, digests, peer states are **`internal`**. Hosts drive the engine through public methods (`WriteField`, `CreateDeltaBatch`, `ApplyBatch`, `TryGetRecord` / `TryGetField`, …). Test assemblies use `InternalsVisibleTo`. |
| `Zero128` | Returns a **fresh copy** each access (callers cannot corrupt a shared buffer). |
| Locks | **`EnterAllGates()`** (`AllGateScope`) acquires Storage → Outbox → Meta. Individual engine locks are not public. |
| Decode safety | Public **`DecodeBatchPayload`** materializes owned entry buffers. **`ApplyBatchPayload`** may use zero-copy internally then drop the frame. |

Wire remains b128 / canonical codec on the frame payload; typed ids are an API/layout concern, not a protocol change.

---

## Supported (embedder contract)

### Identities (`Ids.cs`)

| Type | Role |
|------|------|
| `NodeId` | Fixed 16-byte node identity (`Lo`/`Hi`); `From` canonicalizes short/long API input (wire remains exactly 16 bytes), `WriteTo` / `ToArray` / `EqualsBytes` |
| `RecordId` | Fixed 16-byte record identity (same b128 normalization rules) |
| `BatchId` | Fixed 16-byte batch identity (same b128 normalization rules) |
| `Digest16` | 16-byte digest (records root, batch digests, stream chain); `From` requires exactly 16 bytes, `Truncate` explicitly takes the leading 16 bytes |

### Engine — `Node`

**Construction:** `Node(NodeId? nodeId = null, int epoch = 1, Func<long>? nowFn = null, long maxSkewUs = …)`  
**Public state:** `NodeId` (ctor-only), `Epoch`, read-only `ClockState` snapshot, optional `ResourceLimits? Limits`.

| API | Role |
|-----|------|
| `WriteField(ns, RecordId, fieldId, value, Hlc?)` | Local LWW write → outbox |
| `TombField` / `DeleteRecord` / `UndeleteRecord` | Local field and record lifecycle mutations → outbox |
| `RepublishRecords(ns, IEnumerable<RecordId>)` | Re-enqueue visible record state for a namespace |
| `PushOutbox(ns, RecordId, OpFieldWrite / OpFieldTomb / OpRecordTombstone)` | Low-level local Outbox enqueue; the node supplies its own author identity and session peer routing is not an enqueue argument |
| `AuthorEpoch(NodeId)` | Resolve the current durable epoch for an author |
| `CreateDeltaBatch(NodeId target, ns, NodeId? filterAuthor, …)` | Build one-author delta for peer |
| `SignBatchFor` / `AdvanceStream` | Wire bind + stream cursor after send |
| `EncodeBatchPayload` / `EncodeBatchPayload(…, Span<byte>)` | Batch → payload bytes |
| `WriteRecordsArray` / `EncodeRecordsArray` / `DecodeRecordsArray` | Canonical records codec; decoded identities are typed `RecordId` / `BatchId`, trailing bytes rejected |
| `ComputeBatchDigest` / `ComputeRecordsRoot` | Public digest helpers for canonical batch headers and ordered records |
| `DecodeBatchPayload` | Payload → `Batch` with **owned** entry buffers |
| `ApplyBatch(Batch, NodeId sender)` | Apply + inbox idempotency |
| `ApplyBatchPayload(byte[], NodeId sender)` | Decode+apply (preferred on the wire path) |
| `ProcessAck(NodeId, …)` | Advance per-author watermarks |
| `ApplySnapshot(Batch)` / `BuildSnapshot(ns)` | Apply or build a validated snapshot baseline |
| `EnterAllGates()` | Acquire Storage → Outbox → Meta gates; dispose on the acquiring thread (idempotent) |
| `RegisterSessionEpoch` / `RegisterAlias` / `ResolveAlias` / `AliasOf` / `EnsureAlias` / `ResetSession` / `LearnAuthorEpoch` / `KnownAuthorEpoch` / `SessionEpoch` | Session & alias registry |
| `DurableId` / `WriteDurableId` | Encode the typed node identity and epoch into the durable identity representation |
| `RepublishUnderPrefix` / `RepublishRecords` | Re-enqueue visible record state for a namespace, by digest prefix or typed record IDs |
| `OutboxSize` / `GetOutboxCount` | Read-only pending Outbox count for a namespace |
| `TryGetRecord` / `TryGetField` | Read detached snapshots; returned mutable DTOs cannot mutate live storage |
| `GetRecordCount` / `RecordDigestOf(RecordId)` / `AllRecordDigests` | Typed record lookup, count and detached digest map |
| `RegisterLoopbackPeer` | In-process peer map for alias negotiation |
| `BatchApplied` / `FieldWritten` / `OutboxEntryPushed` / `FieldTombstoned` / `RecordDeleted` / `RecordUndeleted` / `SchemaRegistered` / `SchemaMapLearned` / `GcNoticeApplied` / `GarbageCollected` / `TrustChanged` / `SnapshotApplied` | Data and lifecycle events (handlers run outside engine locks) |
| `Dispose` | Idempotently clear engine state and detach caller-owned services/events; subsequent stateful calls throw `ObjectDisposedException` |
| `NodeOptions` | Structured construction with typed `NodeId?` (limits at create) |
| `SyncErrorCode` / `SyncException.ErrorCode` | Typed error codes |
| `AckProcessed` | ACK watermark advanced |

**Event delivery:** events are raised synchronously after the corresponding state transition and outside engine gates. A throwing observer is isolated and traced; it cannot turn a committed write, batch, snapshot, ACK, trust or schema update into an apparent failure. When no observer is attached, dispatch returns without allocating. Keep handlers short and enqueue expensive application work elsewhere; callbacks are not run on an internal per-record loop.
| `MembershipApplied` / `KeyRotationApplied` | Roster / key epoch events |
| `ApplyKeyRotation` | Apply notice + raise event |
| `NodeBuilder` | Fluent construction with typed `WithNodeId(NodeId)` |
| `TrustPeer` / `RevokeTrust` (`NodeId`) | Typed trust pins |

**Resource bounds:** `ResourceLimits`, `BoundedNode.WriteField(Node, ResourceLimits, ns, RecordId, …)`.

### Apply result — `BatchResult` (**struct**)

| Property | Role |
|----------|------|
| `BatchId` | Applied / replayed batch id |
| `Applied` | Ops newly applied (`0` on pure duplicate) |
| `Acked` / `Duplicate` | ACK / idempotent replay flags |
| `CursorEnd` / `NamespaceId` | ACK metadata |
| `DurableAuthor` | Author stream for ACK (Errata E-4) |

`Transport.DeliverBatch` returns **`BatchResult?`** (`null` = nothing pending). After a null-check use `.Value` (nullable value type).

### Models

| Type | Notes |
|------|--------|
| `OutboxEntry` | `RecordId`, `BatchId ChangeId`, `NodeId AuthorNodeId`, `ReadOnlyMemory<byte> Payload`, … |
| `Batch` | Typed issuer/id/digests; `Entries`; `MaterializeEntries()` for owned payloads |
| `MetadataBatchReceipt` | Durable accepted cursor/digest/relay result; `DurableAuthor` is restored from the stored receipt and is authoritative for duplicate ACK handling |
| `Record` / `FieldState` | In-memory record model; `Record` is constructed with `RecordId` (reached via engine apply paths; maps are internal on `Node`) |

### Wire & codecs (§6, §12)

`FrameCodec`, `Frames`, `Frame` / `FrameException`, `Codec` / `Writer` / `Reader`, `Ops`, `Hlc` / `HlcClock` / `HlcOps`, `Digests` / `Digest16` domains, `FragmentCodec` / `FragmentReassembler` / `FrameFragment`, `FrameStream`, `FlowControl`, `Mem`.

`Frames.FrameTypes` and `Frames.ErrorCodes` are read-only lookup maps. `Node.ClockState` and `HlcClock.Snapshot()` expose a consistent immutable clock snapshot. The mutable clock instance and its tick state are not exposed by `Node`. `FlowControl` has immutable window configuration and thread-safe per-stream credit accounting.

### Merge (§8)

| Policy | Code | API |
|--------|------|-----|
| LWW_FIELD | wire 0x02; legacy 0 accepted | WriteField, Node.ApplyBatch |
| COUNTER_G | helper 0x03 | CounterG, MergeFieldWithPolicy |
| COUNTER_PN | helper 0x04 | CounterPn, MergeFieldWithPolicy |
| SET_OR | helper 0x05 | SetOr, MergeFieldWithPolicy |
| SEQ_FRACTIONAL | helper 0x06 | SeqFractional, MergeFieldWithPolicy |
| BLOB_REF | helper 0x08 | BlobRef, MergeFieldWithPolicy |
| MANUAL | helper 0x09 | MergePolicy.Manual; no runtime FieldResolve workflow |
| Max/Min bytes/u64 | helper extensions 0x81–0x84 | MergeFieldWithPolicy |

MergePolicy.LwwField has enum value 0 for the helper API; it is not the canonical wire code. Node.CreateDeltaBatch emits LWW_FIELD wire code 0x02. The runtime accepts 0 only as a legacy spelling of 0x02 and rejects other policy codes with SCHEMA_MISMATCH.

MergePolicyRegistry is a standalone helper registry; it is not connected to Node.ApplyBatch. SchemaDescriptor.MergePolicy gates metadata compatibility but does not select the merge algorithm.

### Reconcile (§13)

`SyncSession.RunReconciliationAsync<TResult>` provides an exclusive transaction for custom multi-frame reconciliation protocols. Its `ReconciliationOperations` context exposes digest, NEED_RANGE, NEED_KEYS, and snapshot primitives without reacquiring their frame gates. The context is valid only during the callback; using it afterward throws `ObjectDisposedException`. Batch push/receive remains independent.

`Reconcile` ladder messages, `Iblt`, `GcNotice`, `Membership` / roster frames, snapshot build/apply, AutoReconcile / IBLT over generic `SyncSession` (not TCP-specific). `WatermarkGate` rejects incremental batches from peers behind the local safe watermark; such peers require a full resynchronization. Snapshot DTOs use `RecordId` / `NodeId`; `ApplySnapshotChunks` detaches caller-owned DTO graphs before validation and publication.

### Session / transport

| API | Role |
|-----|------|
| `ISyncTransport` | `NodeId PeerNodeId`, `SyncTransportCapabilities`, optional `MaxPacketSize`, `SendAsync` / `ReceiveAsync` |
| `IPeerAwareSyncTransport` | Optional mutable peer identity binding for transports that learn the peer during HELLO |
| `PipeTransport` | In-memory paired transport for tests and embedded scenarios; `CreatePair()` returns peer-aware endpoints |
| `StreamSyncTransport` | Complete-packet adapter over a caller-supplied `Stream`; `ownsStream` controls whether disposal closes that stream; peer identity is bound after HELLO |
| `SessionWire` | HELLO / HELLO_ACK / ALIAS_MAP / ERROR; `HelloPayload` / `AliasMapEntry` use **`NodeId`**; includes low-level `HandshakeAsync` for role-specific setup |
| `SyncSession` | Transport-independent protocol session; `ConnectAsync` / `AcceptAsync` use role-specific handshakes, while `EstablishAsync` lets both endpoints start symmetrically and selects the initiator by `NodeId`; `ownsTransport` controls transport disposal | `ReceiveBatchAsync` has an opt-in `receiveStateOwnedByMetadataStore` mode that requires a durable `beforeApply` callback; Storage hosts should use `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync` rather than setting this flag without a matching Metadata Store.
| Fragment receive | `ReceiveReassembledAsync` returns a complete `Frame` (type, stream, sequence, flags, payload); fragments sharing a message id must retain the same type/stream/base sequence/flags |
| IBLT receive | `ReceiveIbltAsync(namespaceId)` requires the expected namespace so the response stream is validated before decoding |
| `TcpSync` / `TcpSyncListener` | TCP/TLS host adapter; listener defaults to loopback unless an address is supplied; TLS is enabled by supplying server/client authentication options; both create generic `SyncSession` instances |
| `AsyncTransport` | Async batch delivery, sync-round orchestration, and async Node save/load helpers |
| `Transport` | In-process `MakeSession`, `DeliverBatch`, `SyncRound`, typed ACK codec, NodeId-based sneakernet helpers |
| `LoopbackTransport` | Lab loopback with `NodeId` peer routing; routing dictionaries are private to the adapter |

### Persistence

The `NodePersistence` / `Wal` surface remains Node-owned persistence. The optional durable external metadata/provider implementation is shipped in the separate `BinSync.Storage` assembly: `StorageMetadataStore`, `StorageDataProvider` and `StorageSyncCoordinator` provide row-level durable metadata transitions, provider receipts and the Storage-backed transport path. The Core package remains usable without this assembly. The volatile reference adapter is test-only; durable storage behavior is exercised by the Storage Integration suite. See `METADATA-STORE-AUTHORITY-MIGRATION.md` and `METADATA-STORE-BENCHMARKS.md` for ownership, migration and measured scaling details.

### Metadata Store / durable Storage integration (opt-in)

- `StorageMetadataStore` is the durable `IMetadataStore` / `IMetadataPeerStateStore` implementation. BMS3 is mandatory; legacy/unknown row formats fail closed. Metadata mutations use row-level atomic transactions.
- `StorageDataProvider` stores generation-scoped provider values, a mandatory BSP3 format marker, stable local-feed ChangeIds and idempotent apply receipts. `StorageSyncCoordinator` orders PendingApply staging, Node checkpoint, provider apply, metadata finalization and ACK.
- `MetadataIncomingBatch.ReceiveStateOwnedByMetadataStore` marks a PendingApply intent whose receive cursor/digest/inbox authority belongs to the Metadata Store. StorageSyncCoordinator sets it; callers must not set it without a durable Metadata Store acceptance callback.
- `MetadataIncomingBatch.HeaderFingerprint` carries the 16-byte canonical full-wire-header fingerprint. New metadata-authoritative intents require it; the durable receipt and completed PendingApply retain it with `MetadataBatchReceipt.DurableAuthor`. A duplicate BatchId with a different full header is rejected, even if BatchDigest alone would not distinguish the header fields.
- `MetadataPendingApplyIntent.IsDuplicate` reports whether the batch identity already has a matching durable receipt. `MetadataBatchReceipt.HeaderFingerprint` returns the committed fingerprint.
- `IProviderFullReplace` is an optional provider capability for authenticated authoritative FULL_REPLACE. `StorageDataProvider` stages generation-scoped pages, validates a canonical SHA-256 manifest, fences commit to the captured base generation and atomically switches the active generation. `StorageDataProvider.FullReplaceDigestBuilder` computes the manifest incrementally from canonical snapshot pages; `ComputeFullReplaceDigest` is the convenience helper. Old generations are retained; this API does not authorize ChangeId/tombstone GC. The caller must validate roster authority and ensure no PendingApply, relay reservation, provider intent or reader pin depends on the old generation.
- PendingApply uses the normative v3 encoding only. Older PendingApply and receipt encodings are rejected; a v3 receipt without authenticated transport-epoch provenance is retained but never assigned guessed provenance. Metadata-authoritative duplicate replay requires the full canonical header fingerprint.

`NodePersistence` stores the complete Node snapshot. `Wal` is a v3-only append log; older WAL markers and records are rejected. Hosts use `Checkpoint(Node, snapshotPath)` for the atomic snapshot-and-reset transition; raw truncate and checkpoint-marker append are internal. The sequence is read-only and synchronized:
each local mutation is one integrity-protected frame containing both the operation
and its exact Outbox identity (`ChangeId`, `Seq`, author, payload, creation time and
causal dependencies). There is no field-only WAL record or v1 writer/reader.

Accepted incoming DELTA_BATCH frames are also recorded as `RemoteBatch` entries
before the receiver publishes new record effects or sends the ACK. Each entry preserves
the exact batch payload together with the sending peer identity, session epoch and
alias map needed to resolve operation authors during replay. A replay whose ChangeIds
are already present in the atomically checkpointed Node `GlobalChangeLedger` does not
create another WAL entry. If a Metadata Store receipt is replayed into a newly created
Node whose local ledger is empty, the batch is still written to WAL so that the Node
record cache can be rebuilt; Metadata Store and Data Provider remain the protocol
and durable-value authorities. This closes the crash window in which a sender could
receive an ACK for data that the receiver had not yet made recoverable.

Snapshot transfers remain atomic in memory but are not WAL records: they have no
acceptance ACK, do not advance the sender's Outbox acknowledgement cursor, and can
be transferred again from the sender's retained state after a receiver restart.
Node WAL/checkpoint state is a recoverable application-cache projection; after
Metadata-Store-owned cutover the Node `GlobalChangeLedger` is cleared for that
namespace and the Metadata Store remains the sole durable ChangeId authority.

Startup order (before exposing the Node to application work):

```csharp
var node = NodePersistence.Load(snapshotPath); // or construct a new Node
using var wal = Wal.Open(walPath);
node.AttachWal(wal);                           // replays first, then enables durable writes
```

`AttachWal` performs replay and attachment under the Node gates as one startup
transition. This prevents a local write from racing between recovery and enabling
the WAL. If replay fails, the WAL is not attached.

`AttachWal` does not take ownership of the WAL; keep it alive for as long as the
Node uses it. `Wal.Checkpoint(node, snapshotPath)` saves the full snapshot and
atomically replaces the active WAL segment. `Wal.Replay(node)` and `Wal.ReplayFile(path, node)` are explicit standalone replay entry points; do not manually replay the same log after `AttachWal` has already performed recovery. Do not dispose the WAL while it remains attached to a live Node.

### Schema (§18, opt-in)

`SchemaDescriptor`, `SchemaRegistry`, `Node.Schemas`, `RegisterSchema`, `ApplyBatchWithSchema`, `LearnSchemaMap`, `ExportSchemaMap`, SCHEMA_MAP frame 0x04.

### Causal deps (§16.19, opt-in)

| API | Role |
|-----|------|
| `CausalTracker` | Process-local typed `BatchId` known/pending/lost sets; cycle, timeout and lost-dependency outcomes; lost IDs propagate to transitive dependents; restore known/lost state after restart; `DepTimeout` and `UtcNowMs` configure expiry |
| `Node.Causal` | Optional tracker |
| `EncodeBatchPayloadCausal` / `DecodeBatchPayloadCausal` | Causal dependency trailer codecs; batch flags are set on an encoded copy, not by mutating the caller's batch |
| `Node.ApplyBatchCausal` / `DrainCausal` (released items are not known until applied) / `DrainAndRetryCausal` / `NoteCausalApplied(BatchId)` | Gate before apply, retry held batches, and note local causal commits |
| `CausalWire` | CCD1 trailer with footer-length framing, sorted unique dependencies, bounded counts and atomic decode |

**Causal mode constraints:** `CausalTracker` state is not persisted by Node WAL/snapshot; the application must rebuild it after restart. The CCD1 trailer is outside `RecordsRoot`/`BatchDigest` and must only be used over an integrity-authenticated channel.
| `CAUSAL_DEP_MISSING` | 0x0012 |

`ApplyBatch` ignores causal; use `ApplyBatchCausal` when enabled.

### E2EE (§14, opt-in)

| API | Role |
|-----|------|
| `E2ee.SealField` / `OpenField` | ChaCha20-Poly1305 |
| `E2eeKeyRing` / `Node.EnsureE2ee` | Per-namespace keys + epochs |
| `WriteFieldSealed` / `ReadFieldOpen` / `TryOpenField` | Convenience write and authenticated read helpers |
| `KeyRotation` / `KeyRotationNotice` / `KeyEpochStore` | Frame 0x11 + strict 64-byte proof bound to typed `NodeId`; exact payload length required |
| `KeyEpochStore.TrustIdentity` / `TryApply` / `GetEpoch` | Pin identity verification keys and accept only proof-verified epoch advances |
| `KeyEpochStore.SeedEpoch` | Explicit local bootstrap/restore hook without a proof; seed only from trusted local persisted state, never from remote notices |

Blind merge compares ciphertext with open HLC + author.

### Signatures & trust (§14.8, §15.6)

| API | Role |
|-----|------|
| `BatchSignature` | Ed25519 sign/verify (vendored); sign payload takes `NodeId`, `BatchId`, and `Digest16` |
| `TrustStore` | TOFU / PreShared / CA roots / revoke; TOFU candidates are pinned only after a valid batch signature |
| `Node.EnsureTrust` / `TrustPeer` / `RevokeTrust` / `SaveTrust` / `LoadTrust` | Configure, pin, revoke, and persist trust state |
| `VerifyAndApplyBatch` | Verify and publish under the Node + trust-store gates; concurrent revocation is ordered before or after the commit |
| `SIGNATURE_INVALID` / `TRUST_REVOKED` | 0x0013 / 0x0014 |

`TrustStore.ResolvePublicKey` may return an unpinned TOFU candidate for inspection; lookup alone never establishes trust. `TrustPersistence` strictly validates stored identities, modes, counts, flags, and trailing bytes, and flushes the temporary file before atomic replacement.

### Errors

| Type | Role |
|------|------|
| `SyncException` | Domain codes (`CodeName` / `Code`); optional typed `BatchId?` context (no byte-array identity overloads) |
| `FrameException` | Frame parse / reject |
| `InvalidVarintException` / `InvalidFloatException` | Codec |
| `ClockDriftException` | HLC skew |
| `SessionException` / `NoSessionException` / `PeerRejectedException` | Session / loopback |
| `MetadataPendingApplyAlreadyFinalizedException` | Explicit idempotent retry signal; not a generic metadata state conflict |

### Lab / test helpers (not product contract)

`LabFuzzTests` scenarios, `TlsTestCert`, flow-control hooks, assemblies listed in `InternalsVisibleTo`.

---

## Explicitly out of scope

- Full X.509 certificate path validation (CA roots are **node-id** allow-list only)
- Automatic WAL creation/attachment; durability is opt-in through `Node.AttachWal`
- Forcing causal / E2EE / trust on every `ApplyBatch` (all opt-in)
- Guaranteed allocation-free paths under all policies (hot paths optimised; domain objects still allocate)
- Public mutable access to `Storage` / `Outbox` / `Inbox` maps (internal by design in 2.2)
- NuGet.org publish automation (use `scripts/pack-nupkg.sh`)
