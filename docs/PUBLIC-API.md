**Current package: 3.0.0** — Node identity is ctor-only; prefer `EnterAllGates()` over obsolete `Gate`.

# BinSync.Core — Public API surface

**Package:** `BinSync.Core` **`3.0.0`** · **Target:** `net10.0`  
**Protocol:** **BIN-SYNC CORE**  
Normative: **BIN-SYNC-SPECIFICATION.txt v2.4.5**.  
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
| DTO fields | Public **properties** (not bare fields): `OutboxEntry`, `Batch`, `PeerState`, `Record`, … |
| Identities | Value types: **`NodeId`**, **`RecordId`**, **`BatchId`**, **`Digest16`** (value equality / hash). Implicit conversion from/to `byte[]` remains where noted. |
| `BatchResult` | **`struct`** (not a class) — ApplyBatch/ApplyBatchPayload return by value; no heap result object on the success path. |
| Peer identity on transport/session | **`ISyncTransport.PeerNodeId`**, **`HelloPayload.NodeId`**, **`AliasMapEntry.NodeId`**, **`PeerState.PeerNodeId`**, **`SyncSession.PeerNodeId`** are **`NodeId`**. |
| Engine collections | **`Storage`**, **`Outbox`**, **`Inbox`**, alias maps, digests, peer states are **`internal`**. Hosts drive the engine through public methods (`WriteField`, `CreateDeltaBatch`, `ApplyBatch`, `TryGetRecord` / `TryGetField`, …). Test assemblies use `InternalsVisibleTo`. |
| `Zero128` | Returns a **fresh copy** each access (callers cannot corrupt a shared buffer). |
| Locks | Prefer **`EnterAllGates()`** (`AllGateScope`). `Node.Gate` is **obsolete** (Meta only). |
| Decode safety | Public **`DecodeBatchPayload`** materializes owned entry buffers. **`ApplyBatchPayload`** may use zero-copy internally then drop the frame. |

Wire remains b128 / canonical codec on the frame payload; typed ids are an API/layout concern, not a protocol change.

---

## Supported (embedder contract)

### Identities (`Ids.cs`)

| Type | Role |
|------|------|
| `NodeId` | 16-byte node identity (`Lo`/`Hi`), `From` / `WriteTo` / `ToArray` / `EqualsBytes` |
| `RecordId` | 16-byte record identity (same layout rules) |
| `BatchId` | 16-byte batch identity |
| `Digest16` | 16-byte digest (records root, batch digests, stream chain) |

### Engine — `Node`

**Construction:** `Node(byte[]? nodeId = null, int epoch = 1, Func<long>? nowFn = null, long maxSkewUs = …)`  
**Public state:** `NodeId` (ctor-only), `Epoch`, `Clock`, optional `ResourceLimits? Limits`.

| API | Role |
|-----|------|
| `WriteField(ns, RecordId, fieldId, value, Hlc?)` | Local LWW write → outbox |
| `WriteFields(...)` | Batch of writes under one lock (distinct HLC stamps) |
| typed `PushOutbox` overloads | Low-level outbox helpers |
| `CreateDeltaBatch(NodeId target, ns, …)` | Build one-author delta for peer |
| `SignBatchFor` / `AdvanceStream` | Wire bind + stream cursor after send |
| `EncodeBatchPayload` / `EncodeBatchPayload(…, Span<byte>)` | Batch → payload bytes |
| `DecodeBatchPayload` | Payload → `Batch` with **owned** entry buffers |
| `ApplyBatch(Batch, NodeId sender)` | Apply + inbox idempotency |
| `ApplyBatchPayload(byte[], NodeId sender)` | Decode+apply (preferred on the wire path) |
| `ProcessAck` | Advance per-author watermarks |
| `ApplyRemoteField` / `ApplySnapshot` | Snapshot / anti-entropy apply paths |
| `RegisterSessionEpoch` / `RegisterAlias` / `ResolveAlias` / `AliasOf` / `EnsureAlias` / `ResetSession` / `LearnAuthorEpoch` / … | Session & alias registry |
| `OutboxSize` / `GetOutboxCount` | Bound/diagnostic helper |
| `TryGetRecord` / `TryGetField` | Read detached snapshots; returned mutable DTOs cannot mutate live storage |
| `GetRecordCount` | Records in a namespace |
| `RegisterLoopbackPeer` | In-process peer map for alias negotiation |
| `BatchApplied` / `FieldWritten` | Events (handlers run outside engine locks) |
| `Dispose` | Clear maps / detach events |
| `NodeOptions` | Structured construction (limits at create) |
| `SyncErrorCode` / `SyncException.ErrorCode` | Typed error codes |
| `AckProcessed` | ACK watermark advanced |
| `MembershipApplied` / `KeyRotationApplied` | Roster / key epoch events |
| `ApplyKeyRotation` | Apply notice + raise event |
| `NodeBuilder` | Fluent construction |
| `TrustPeer` / `RevokeTrust` (`NodeId`) | Typed trust pins |

**Resource bounds:** `ResourceLimits`, `BoundedNode` factory helpers.

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
| `PeerState` | `NodeId PeerNodeId`; stream ACK maps keyed by `NodeId` |
| `Record` / `FieldState` | In-memory record store (reached via engine apply paths; maps are internal on `Node`) |

### Wire & codecs (§6, §12)

`FrameCodec`, `Frames`, `Frame` / `FrameException`, `Codec` / `Writer` / `Reader`, `Ops`, `Hlc` / `HlcClock` / `HlcOps`, `Digests` / `Digest16` domains, `FragmentCodec` / `FragmentReassembler` / `FrameFragment`, `FrameStream`, `FlowControl`, `Mem`.

### Merge (§8)

| Policy | Code | API |
|--------|------|-----|
| LWW_FIELD | 0 / default | `WriteField`, `Merge` field write |
| COUNTER_G | 0x03 | `CounterG` |
| COUNTER_PN | 0x04 | `CounterPn` |
| SET_OR | 0x05 | `SetOr` |
| SEQ_FRACTIONAL | 0x06 | `SeqFractional` |
| BLOB_REF | 0x08 | `BlobRef` |
| MANUAL | 0x09 | `MergePolicy.Manual` + FieldResolve 0x00FF |
| Max/Min bytes/u64 | 0x81–0x84 | engine extensions |

`MergePolicyRegistry` maps `(ns, fieldId)` → policy.

### Reconcile (§13)

`SyncSession.RunReconciliationAsync<TResult>` provides an exclusive transaction for custom multi-frame reconciliation protocols. Its `ReconciliationOperations` context exposes digest, NEED_RANGE, NEED_KEYS, and snapshot primitives without reacquiring their frame gates. The context is valid only during the callback; using it afterward throws `ObjectDisposedException`. Batch push/receive remains independent.

`Reconcile` ladder messages, `Iblt`, `GcNotice`, `Membership` / roster frames, snapshot build/apply, AutoReconcile / IBLT over generic `SyncSession` (not TCP-specific).

### Session / transport

| API | Role |
|-----|------|
| `ISyncTransport` | `NodeId PeerNodeId { get; }`, `SendAsync` / `ReceiveAsync` |
| `PipeTransport` / `StreamSyncTransport` | Test / stream adapters (`PeerNodeId` settable on concrete types) |
| `SessionWire` | HELLO / HELLO_ACK / ALIAS_MAP / ERROR; `HelloPayload` / `AliasMapEntry` use **`NodeId`** |
| `SyncSession` | Transport-independent protocol session; generic `ConnectAsync` / `AcceptAsync`, batch/ACK, reconciliation, snapshots, IBLT, membership, schema, key rotation, fragmentation |
| `TcpSync` / `TcpSyncListener` | TCP/TLS host adapter; creates generic `SyncSession` instances |
| `Transport` | In-process `MakeSession`, `DeliverBatch`, `SyncRound`, sneakernet helpers |
| `LoopbackTransport` | Lab loopback |

### Persistence

`NodePersistence` stores the complete Node snapshot. `Wal` is a v2-only append log:
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
atomically replaces the active WAL segment. Do not dispose the WAL while it remains
attached to a live Node.

### Schema (§18, opt-in)

`SchemaDescriptor`, `SchemaRegistry`, `Node.Schemas`, `ApplyBatchWithSchema`, SCHEMA_MAP frame 0x04.

### Causal deps (§16.19, opt-in)

| API | Role |
|-----|------|
| `CausalTracker` | Known set, pending, KЗ1–KЗ5 |
| `Node.Causal` | Optional tracker |
| `Node.ApplyBatchCausal` / `DrainCausal` | Gate before apply |
| `CausalWire` | CCD1 trailer encode/decode |
| `CAUSAL_DEP_MISSING` | 0x0012 |

`ApplyBatch` ignores causal; use `ApplyBatchCausal` when enabled.

### E2EE (§14, opt-in)

| API | Role |
|-----|------|
| `E2ee.SealField` / `OpenField` | ChaCha20-Poly1305 |
| `E2eeKeyRing` / `Node.EnsureE2ee` | Per-namespace keys + epochs |
| `WriteFieldSealed` / `ReadFieldOpen` | Convenience |
| `KeyRotation` / `KeyRotationNotice` / `KeyEpochStore` | Frame 0x11 + proof |

Blind merge compares ciphertext with open HLC + author.

### Signatures & trust (§14.8, §15.6)

| API | Role |
|-----|------|
| `BatchSignature` | Ed25519 sign/verify (vendored) |
| `TrustStore` | TOFU / PreShared / CA roots / revoke |
| `Node.TrustPeer` / `RevokeTrust` | Pin keys |
| `VerifyAndApplyBatch` | Signature gate then apply |
| `SIGNATURE_INVALID` / `TRUST_REVOKED` | 0x0013 / 0x0014 |

### Errors

| Type | Role |
|------|------|
| `SyncException` | Domain codes (`CodeName` / `Code`); optional batch id |
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
