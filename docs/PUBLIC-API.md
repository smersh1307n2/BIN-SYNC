# BinSync.Core — Public API surface (freeze for 0.1.x-alpha)

**Package:** `BinSync.Core` `0.1.0-alpha.2`  
**Assembly:** `BinSync.Core`  
**Namespace:** `BinSync` (flat; no sub-namespaces yet)  
**Target:** `net10.0`  
**Branch tip docs:** `installing-.net-10-ea6be`

This document freezes the **supported** public surface for embedders during the 0.1 alpha line.  
Anything not listed under **Supported** is either internal, or **lab/unstable** and may change without a major version bump.

`InternalsVisibleTo("BinSync.Tests")` is intentional — tests may touch internal seq/cursor maps.

Normative behaviour: **BYN-SYNC-SPECIFICATION.txt v2.3.6** + **[ERRATA-v2.3.7](ERRATA-v2.3.7.md)**.  
Gap matrix: **[PRODUCTION-STATUS.md](PRODUCTION-STATUS.md)**.

---

## Supported (embedder contract)

### Engine

| Type | Role |
|------|------|
| `Node` | Primary replica: write fields, outbox/inbox, apply batches, alias maps |
| `BoundedNode` | `Node` + opt-in `ResourceLimits` |
| `ResourceLimits` | Memory / queue bounds (opt-in) |
| `OutboxEntry`, `Batch`, `PeerState` | Batch / peer state models |
| `BatchResult` | Result of `ApplyBatch` |
| `SyncException` and subclasses | Protocol / engine errors (`CodeName`) |

**Core operations:** `WriteField` / `WriteFields`, `CreateDeltaBatch` / encode-decode / `ApplyBatch`, `SignBatchFor` / `AdvanceStream` / `ProcessAck`, alias / durable-author registry, `RepublishUnderPrefix`, `ApplyRemoteField`.

### Wire & codecs

`FrameCodec`, `Frames`, `Codec` / `Writer` / `Reader`, `Ops`, `HlcClock` / `Hlc`, `Merge` / `MergePolicy` / `MergePolicyRegistry`, `Digests` / `StateHash`.

**Codec note:** `B128` = fixed 16-byte identity; `Bytes()` = varint length + payload (used by WAL field values).

### Merge policies (§8)

| Policy | Semantics |
|--------|-----------|
| `LwwField` | Default: higher HLC wins; author tie-break |
| `MaxBytes` / `MinBytes` | Lexicographic; equal → LWW |
| `MaxU64` / `MinU64` | LE uint64; equal → LWW |

API: `Merge.MergeField`, `Merge.MergeFieldWithPolicy`, `Merge.ApplyFieldWrite(..., policy)`.  
Echo suppression [E2] applies to all policies.  
`Node.WriteField` remains **LWW only**; Max/Min are opt-in.

### Reconcile ladder (§13)

`ReconcileFrames` (`0x06`…`0x0B`), `Reconcile` (digest / NEED_* / divergent prefixes), `Iblt`, `GcNotice`, `MembershipRoster`, snapshot helpers.

### Session / transport

`ISyncTransport`, `SessionWire`, `StreamSyncTransport` / `FrameStream`, `PipeTransport`, `AsyncTransport`, `TcpSyncListener` / `TcpSyncSession`, `LoopbackTransport`.

**`TcpSyncSession` (callable):** delta push/receive; digest exchange; NEED_RANGE/KEYS fulfill; snapshot; `ExchangeIbltAsync` / `ReconcileViaIbltAsync` / **`BilateralReconcileViaIbltAsync`** / `FulfillMissingKeysAsync`; membership; GC; **`AutoReconcileAsync`** / **`BilateralAutoReconcileAsync`**.

### Persistence

| Type | Role |
|------|------|
| `NodePersistence` | Atomic full snapshot `BIN-SYNC/NODESTATE/v1` |
| `Wal`, `WalRecordType` | Append-only WAL between checkpoints |

WAL: `Open`, `AppendFieldWrite`, `AppendCheckpoint`, `Replay` / `ReplayFile`, `Truncate`, `Checkpoint`.  
**Not** auto-attached to `WriteField`.

### Causal dependencies (§16.19 experimental)

| Type | Role |
|------|------|
| `CausalTracker` | Known changeIds, quarantine, cycle detect, timeout, batch gate |
| `CausalOutcome` | `Ready` / `Pending` / `TimedOut` / `Cycle` / `LostDep` |
| `PendingCausalOp` | Queued op waiting on parents |
| `CausalError.CausalDepMissing` | Wire code `0x0012` |

**Invariants covered by tracker:**

- **KЗ1** — `DepTimeout` → `TimedOut` (not added to known set)
- **KЗ2** — cycle / self-dep → `Cycle`
- **KЗ3** — `MarkLost(parent)` drops dependents
- **KЗ4** — pending must not advance cursors (caller contract)
- **KЗ5** — `EvaluateBatch` all-or-nothing

**Not** auto-wired into `ApplyBatch` / `WriteField`. Embedders call `Evaluate` → `EnqueuePending` / `NoteApplied` / `Drain` explicitly.

```csharp
var tracker = new CausalTracker { DepTimeout = TimeSpan.FromSeconds(30) };
var outcome = tracker.Evaluate(changeId, deps, out _);
if (outcome == CausalOutcome.Pending)
    tracker.EnqueuePending(changeId, deps, payload);
// after parent applied:
tracker.NoteApplied(parentId);
foreach (var (op, o) in tracker.Drain())
    if (o == CausalOutcome.Ready) { /* apply op.Payload */ }
```

---

## Lab / unstable

`TlsTestCert`, `FlowControl` (partial), `Blake3Core` wrapper.

---

## Explicitly out of scope (not public promises yet)

- E2EE / app-level auth (§14–15) — TLS transport only
- Auto-hook of causal tracker into `ApplyBatch`
- Auto-WAL / auto-`MergePolicyRegistry` on `WriteField`
- MANUAL resolve field (`0x00FF`) wire path
- Schema evolution (§18)

---

## Compatibility policy (0.1.x-alpha)

1. **Supported** types/ops not removed without a note in PRODUCTION-STATUS and a package bump.
2. Optional parameters with defaults are allowed.
3. Wire layout changes require protocol version / capability bit.
4. Normative behaviour: spec v2.3.6 + ERRATA-v2.3.7.

---

## Minimal embedder sketch

```csharp
var node = new Node(nodeId, epoch: 1);
await using var listener = TcpSyncListener.Start();
var accept = listener.AcceptAsync(serverNode);
await using var client = await TcpSyncSession.ConnectAsync(clientNode, "127.0.0.1", listener.Port);
await using var server = await accept;

clientNode.WriteField(1, recordId, fieldId, value);
var recv = server.ReceiveBatchAsync(serverNode);
await client.PushBatchAsync(clientNode, namespaceId: 1);
await recv;

var serverReco = server.BilateralAutoReconcileAsync(serverNode, 1, initiate: false);
await client.BilateralAutoReconcileAsync(clientNode, 1, initiate: true);
await serverReco;

NodePersistence.Save(node, "node.state");
using var wal = Wal.Open("node.wal");
// optional: tracker for causal-gated app logic
var tracker = new CausalTracker();
```
