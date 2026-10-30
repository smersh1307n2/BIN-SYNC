// Production hardening: cross-assembly access to engine internals for the test suite
// (thread-safety invariants: per-author outbox seq counters, peer batch-id caches).
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("BinSync.Tests")]

namespace BinSync;

public sealed partial class Node
{
    /// <summary>Internal per-(author, namespace) durable sequence counters [П1].</summary>
    internal Dictionary<(byte[], int), long> OutboxSeqRef => _outboxSeq;

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
    public bool ApplyRemoteField(int namespaceId, byte[] recordId, ushort fieldId,
        byte[] value, Hlc hlc, byte[] authorNodeId, ushort keyEpoch = 0)
    {
        lock (_lockStorage)
        {
            var store = NsInternal(namespaceId);
            if (!store.TryGetValue(recordId, out var rec))
                store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
            var fs = new FieldState(value, hlc, authorNodeId, keyEpoch);
            return Merge.ApplyFieldWrite(rec, fieldId, fs);
        }
    }
}

public sealed partial class PeerState
{
    /// <summary>Internal per-stream cached (batchId, prevDigest) triples.</summary>
    internal Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> IdsRef => Ids;
}
