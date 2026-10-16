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
        lock (_gate)
        {
            foreach (var (rid, fid, val) in writes)
                WriteFieldInternal(namespaceId, rid, fid, val);
        }
    }
}

public sealed partial class PeerState
{
    /// <summary>Internal per-stream cached (batchId, prevDigest) triples.</summary>
    internal Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> IdsRef => Ids;
}
