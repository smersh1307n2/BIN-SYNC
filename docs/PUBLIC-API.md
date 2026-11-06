**Current package: 3.0.0** — Node identity is ctor-only; `EnterAllGates()` is the supported aggregate gate scope.

# BinSync.Core — Public API surface

**Package:** `BinSync.Core` **`3.0.0`** · **Target:** `net10.0`  
**Protocol:** **BIN-SYNC CORE**  
Normative: **BIN-SYNC-SPECIFICATION.txt v2.4.16**.  
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
| `SyncSession` | Transport-independent protocol session; `ConnectAsync` / `AcceptAsync` use role-specific handshakes, while `EstablishAsync` lets both endpoints start symmetrically and selects the initiator by `NodeId`; `ownsTransport` controls transport disposal |
| `TcpSync` / `TcpSyncListener` | TCP/TLS host adapter; listener defaults to loopback unless an address is supplied; TLS is enabled by supplying server/client authentication options; both create generic `SyncSession` instances |
| `AsyncTransport` | Async batch delivery, sync-round orchestration, and async Node save/load helpers |
| `Transport` | In-process `MakeSession`, `DeliverBatch`, `SyncRound`, typed ACK codec, NodeId-based sneakernet helpers |
| `LoopbackTransport` | Lab loopback with `NodeId` peer routing; routing dictionaries are private to the adapter |

### Persistence

`NodePersistence` stores the complete Node snapshot. `Wal` is a v2-only append log. Hosts use `Checkpoint(Node, snapshotPath)` for the atomic snapshot-and-reset transition; raw truncate and checkpoint-marker append are internal. The sequence is read-only and synchronized:
each local mutation is one integrity-protected frame containing both the operation
and its exact Outbox identity (`ChangeId`, `Seq`, author, payload, creation time and
causal dependencies). There is no field-only WAL record or v1 writer/reader.

Accepted incoming DELTA_BATCH frames are also recorded as `RemoteBatch` entries
before the receiver publishes their effects or sends the ACK. Each entry preserves
the exact batch payload together with the sending peer identity, session epoch and
alias map needed to resolve operation authors during replay. An idempotent duplicate
batch does not create another WAL entry. This closes the crash window in which a
sender could receive an ACK for data that the receiver had not yet made recoverable.

Snapshot transfers remain atomic in memory but are not WAL records: they have no
acceptance ACK, do not advance the sender's Outbox acknowledgement cursor, and can
be transferred again from the sender's retained state after a receiver restart.

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
| `CausalTracker` | Typed `BatchId` known set, pending, cycle detection and KЗ1–KЗ5; `DepTimeout` and `UtcNowMs` configure expiry policy |
| `Node.Causal` | Optional tracker |
| `EncodeBatchPayloadCausal` / `DecodeBatchPayloadCausal` | Causal dependency trailer codecs |
| `Node.ApplyBatchCausal` / `DrainCausal` (released items are not known until applied) / `DrainAndRetryCausal` / `NoteCausalApplied(BatchId)` | Gate before apply, retry held batches, and note local causal commits |
| `CausalWire` | CCD1 trailer encode/decode |
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
