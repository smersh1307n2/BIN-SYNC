// Production hardening: cross-assembly access to engine internals for the test suite
// (thread-safety invariants: per-author outbox seq counters, peer batch-id caches).
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("BinSync.Tests")]
[assembly: InternalsVisibleTo("ScenarioHarness")]
[assembly: InternalsVisibleTo("TwoNodeLoopback")]
[assembly: InternalsVisibleTo("TwoNodeTcp")]

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// Read-only view of per-(author, namespace) durable sequence counters [П1].
    /// Mutation only via engine APIs (tests must not write through this).
    /// </summary>
    internal IReadOnlyDictionary<(NodeId, int), long> OutboxSeqRef => _outboxSeq;

    /// <summary>
    /// Atomic multi-entry write (production hardening): the whole batch of field writes
    /// takes the node gate ONCE, so every entry gets a distinct HLC stamp
    /// ([П1] monotonicity: wall+logical must be unique per author). Calling WriteField
    /// in a loop from another thread is NOT equivalent — concurrent writers may then
    /// observe equal (wall, logical) pairs because SendTick and PushOutbox are separate
    /// critical sections.
    /// </summary>
    public void WriteFields(int namespaceId, IEnumerable<(byte[] RecordId, int FieldId, byte[] Value)> writes)
    {
        lock (_lockStorage) lock (_lockOutbox)
        {
            foreach (var (rid, fid, val) in writes)
                WriteFieldInternal(namespaceId, rid, fid, val);
        }
    }

    /// <summary>
    /// Apply a remote field state (snapshot path): LWW merge with original author/HLC.
    /// Does not push outbox — peer already holds the op; local anti-entropy republishes separately.
    /// </summary>
    public bool ApplyRemoteField(int namespaceId, RecordId recordId, ushort fieldId,
        byte[] value, Hlc hlc, NodeId authorNodeId, ushort keyEpoch = 0)
    {
        lock (_lockStorage)
        {
            var store = NsInternal(namespaceId);
            if (!store.TryGetValue(recordId, out var rec))
                store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
            if (fieldId == Model.LifecycleFieldId)
                return Merge.ApplyLifecycle(rec, hlc, authorNodeId, value.Length > 0 ? value[0] : Model.Alive);
            var fs = new FieldState(value, hlc, authorNodeId, keyEpoch);
            return Merge.ApplyFieldWrite(rec, fieldId, fs);
        }
    }

    /// <summary>Apply a remote field tombstone from a snapshot without creating an Outbox entry.</summary>
    public bool ApplyRemoteFieldTombstone(int namespaceId, RecordId recordId, ushort fieldId,
        Hlc hlc, NodeId authorNodeId, ushort keyEpoch = 0)
    {
        lock (_lockStorage)
        {
            var store = NsInternal(namespaceId);
            if (!store.TryGetValue(recordId, out var rec))
                store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
            return Merge.ApplyFieldTomb(rec, fieldId, new FieldState(Array.Empty<byte>(), hlc, authorNodeId, keyEpoch));
        }
    }

    /// <summary>Apply a remote record lifecycle from a snapshot without creating an Outbox entry.</summary>
    public bool ApplyRemoteLifecycle(int namespaceId, RecordId recordId, Hlc hlc,
        NodeId authorNodeId, byte lifecycleValue)
    {
        lock (_lockStorage)
        {
            var store = NsInternal(namespaceId);
            if (!store.TryGetValue(recordId, out var rec))
                store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
            return Merge.ApplyLifecycle(rec, hlc, authorNodeId, lifecycleValue);
        }
    }

    /// <summary>Record that the peer has applied our complete snapshot baseline for this namespace.</summary>
    internal void MarkInitialSyncComplete(NodeId peer, int namespaceId)
    {
        lock (_lockMeta)
        {
            var key = (peer, namespaceId);
            if (!PeerStates.TryGetValue(key, out var state))
                PeerStates[key] = state = new PeerState(peer, namespaceId);
            state.InitialSyncComplete = true;
        }
    }
}

/// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
public sealed partial class PeerState
{
    /// <summary>Read-only view of cached (batchId, prevDigest) triples (tests).</summary>
    internal IReadOnlyDictionary<(NodeId Author, long Start, long End), (BatchId Id, Digest16 Prev)> IdsRef => Ids;
}
