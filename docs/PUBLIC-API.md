# BinSync.Core — Public API surface (freeze for 0.1.x-alpha)

**Package:** `BinSync.Core` `0.1.0-alpha.2`  
**Assembly:** `BinSync.Core`  
**Namespace:** `BinSync` (flat; no sub-namespaces yet)  
**Target:** `net10.0`

This document freezes the **supported** public surface for embedders during the 0.1 alpha line.  
Anything not listed under **Supported** is either internal, or **lab/unstable** and may change without a major version bump.

`InternalsVisibleTo("BinSync.Tests")` is intentional — tests may touch internal seq/cursor maps.

---

## Supported (embedder contract)

### Engine

| Type | Role |
|------|------|
| `Node` | Primary replica: write fields, outbox/inbox, apply batches, alias maps |
| `BoundedNode` | `Node` + opt-in `ResourceLimits` |
| `ResourceLimits` | Memory / queue bounds (opt-in) |
| `OutboxEntry`, `Batch`, `PeerState` | Batch / peer state models used by apply + ACK |
| `BatchResult` | Result of `ApplyBatch` |
| `SyncException` and subclasses as thrown | Protocol / engine errors (`CodeName`) |

**Core operations (must remain callable):**

- `WriteField` / `WriteFields` (atomic multi-write under the node gate)
- `CreateDeltaBatch` / `EncodeBatchPayload` / `DecodeBatchPayload` / `ApplyBatch`
- `SignBatchFor` / `AdvanceStream` / `ProcessAck`
- `AliasOf` / `RegisterSessionEpoch` / durable-author registry access used by session wire
- Snapshot: `NodePersistence.Save` / `Load`
- `RepublishUnderPrefix` (reconcile republish helper)

### Wire & codecs

| Type | Role |
|------|------|
| `FrameCodec`, `Frames`, `FrameException` | 25-byte frame header + CRC32C |
| `Codec`, `Writer`, `Reader`, `InvalidVarintException` | Binary primitives |
| `Ops`, `OpsConst`, `OpFieldWrite`, `OpFieldTomb`, `OpRecordTombstone` | Op encode/decode |
| `HlcClock`, `HlcOps`, `ClockDriftException` | Hybrid logical clocks |
| `Merge`, `Model`, `Record` | LWW / lifecycle merge |
| `Digests`, `Domains`, `BucketAccumulator`, `StateHash` | Digest helpers + state root |

### Reconcile ladder (§13)

| Type | Role |
|------|------|
| `ReconcileFrames` | Wire type constants `0x06`…`0x0B` |
| `Reconcile` | `BuildStateDigest`, encode/decode NEED_*, `DivergentPrefixes`, `LocalKeysUnderPrefix`, snapshot build/encode |
| `StateDigestMessage`, `DigestBucket`, `NeedRangeMessage`, `NeedKeysMessage` | Ladder payloads |
| `Iblt`, `IbltCell` | Invertible Bloom table; `BuildFromNode` / `Peel` / `Encode` |
| `GcNotice`, `GcNoticeMessage`, `GcQuarantineEntry` | GC_NOTICE codec + `Apply` / `PurgeBelowCutoff` |
| `MembershipRoster`, `MembershipDelta` | Membership set + delta encode/apply |

### Session / transport

| Type | Role |
|------|------|
| `ISyncTransport` | Byte send/receive contract (§3.2) |
| `SessionWire`, `SessionCapabilities`, `HelloPayload`, `AliasMapEntry` | HELLO / HELLO_ACK / ALIAS_MAP |
| `StreamSyncTransport`, `FrameStream` | Length-prefixed frame stream adapter |
| `PipeTransport` | In-process duplex for tests and local pairs |
| `AsyncTransport`, `SyncRoundResult` | Async pairwise drain loop |
| `Transport` | In-process `MakeSession` / `SyncRound` / ACK helpers |
| `TcpSyncListener`, `TcpSyncSession` | TCP host; optional TLS via `SslStream` parameters |
| `LoopbackTransport` | Lab mesh / multi-node in-process |

**`TcpSyncSession` operations (must remain callable):**

- Delta: `PushBatchAsync` / `ReceiveBatchAsync`
- Digest: `ExchangeStateDigestAsync`
- Need: `SendNeedRangeAsync` / `ReceiveNeedRangeAsync` / `FulfillNeedRangeAsync` / `SendNeedKeysAsync` / `ReceiveNeedKeysAsync` / `ReplyNeedKeysAsync`
- Snapshot: `SendSnapshotAsync` / `ReceiveSnapshotAsync`
- IBLT: `ExchangeIbltAsync` / `ReconcileViaIbltAsync` / `FulfillMissingKeysAsync`
- Membership: `ExchangeMembershipAsync`
- GC: `ExchangeGcNoticeAsync` / `SendGcNoticeAsync` / `ReceiveGcNoticeAsync`
- Orchestration: `AutoReconcileAsync` / `BilateralReconcileAsync`

### Persistence

| Type | Role |
|------|------|
| `NodePersistence` | Atomic `NODESTATE/v1` snapshot (BLAKE3-tagged) |

---

## Lab / unstable (may move or narrow)

| Type | Notes |
|------|------|
| `TlsTestCert` | Ephemeral self-signed certs for loopback tests only — **not** a production CA API |
| `FlowControl` | Present; not fully wired on every send path |
| `Blake3Core` | Thin wrapper; prefer digest APIs on `Digests` / persistence |

Future 0.2 may move lab helpers to a separate `BinSync.Lab` package.

---

## Explicitly out of scope (not public promises yet)

- E2EE / app-level auth (§14–15)
- Causal dependency tracking (§16.19)
- Additional merge policies beyond LWW_FIELD + lifecycle (§8)
- WAL / incremental persistence (only full snapshot today)
- Schema evolution (§18)
- Stable binary layout versioning beyond current frame + batch + ladder codecs (capability bit required for layout changes)

---

## Compatibility policy (0.1.x-alpha)

1. **Supported** types and the listed operations will not be removed without a note in `docs/PRODUCTION-STATUS.md` and a package minor bump within alpha (or 0.2).
2. Adding optional parameters with defaults is allowed.
3. Changing wire layouts requires a protocol version / capability bit, not a silent change.
4. Normative behaviour follows **BYN-SYNC-SPECIFICATION.txt v2.3.6** + **[ERRATA-v2.3.7](ERRATA-v2.3.7.md)**.

---

## Minimal embedder sketch

```csharp
var node = new Node(nodeId, epoch: 1);
// or: var node = new BoundedNode(nodeId, 1, ResourceLimits.Default);

await using var listener = TcpSyncListener.Start();
var accept = listener.AcceptAsync(serverNode /*, serverCertificate: cert */);
await using var client = await TcpSyncSession.ConnectAsync(clientNode, "127.0.0.1", listener.Port);
await using var server = await accept;

// Delta path
clientNode.WriteField(1, recordId, fieldId, value);
var recv = server.ReceiveBatchAsync(serverNode);
await client.PushBatchAsync(clientNode, namespaceId: 1);
await recv;

// Reconcile ladder (when digests diverge)
var serverReco = server.BilateralReconcileAsync(serverNode, 1, initiate: false);
await client.BilateralReconcileAsync(clientNode, 1, initiate: true);
await serverReco;

// Optional: IBLT-driven set-diff, membership, GC_NOTICE
// await client.ReconcileViaIbltAsync(clientNode, 1, initiate: true);
// await client.ExchangeMembershipAsync(clientNode, initiate: true);
// await client.ExchangeGcNoticeAsync(clientNode, 1, initiate: true);

NodePersistence.Save(node, path);
```
