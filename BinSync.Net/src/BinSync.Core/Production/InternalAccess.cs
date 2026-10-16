// Production hardening: cross-assembly access to engine internals for the test suite
// (thread-safety invariants: per-author outbox seq counters, peer batch-id caches).
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("BinSync.Tests")]

namespace BinSync;

public sealed partial class Node
{
    /// <summary>Internal per-(author, namespace) durable sequence counters [П1].</summary>
    internal Dictionary<(byte[], int), long> OutboxSeqRef => _outboxSeq;
}

public sealed partial class PeerState
{
    /// <summary>Internal per-stream cached (batchId, prevDigest) triples.</summary>
    internal Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> IdsRef => Ids;
}
