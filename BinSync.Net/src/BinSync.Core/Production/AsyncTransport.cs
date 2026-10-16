// Production hardening (Шаг 4): async transport wrappers over the synchronous engine.
//
// Deadlock prevention (§12.18): a transport hop NEVER holds the gates of two nodes
// simultaneously. Transport.DeliverBatch is structured exactly that way — every call
// into a single node (CreateDeltaBatch / SignBatchFor / EncodeBatchPayload / ApplyBatch /
// ProcessAck) takes and releases that node's re-entrant gate before the next node is
// touched; no nested cross-node lock acquisition exists in the core. The async layer
// therefore only needs to (a) run each batch hop as one atomic unit on the thread pool
// and (b) re-check the cancellation token at every await point between hops.
namespace BinSync;

/// <summary>Outcome of one full anti-entropy round (A→B then B→A).</summary>
public sealed record SyncRoundResult(int BatchesFromA, int BatchesFromB)
{
    public int Total => BatchesFromA + BatchesFromB;
}

public static class AsyncTransport
{
    /// <summary>
    /// Deliver ONE pending delta batch from <paramref name="sender"/> into
    /// <paramref name="receiver"/> (full frame round-trip + ACK), asynchronously.
    /// Returns null when the sender has nothing pending for this namespace/author.
    /// Throws <see cref="SyncException"/> on protocol rejection (chain break, epoch
    /// regression, forbidden namespace, clock drift).
    /// </summary>
    public static Task<BatchResult?> DeliverBatchAsync(
        Node sender, Node receiver, int namespaceId,
        byte[]? filterAuthor = null, int schemaVersion = 1, bool dropAck = false,
        CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();
            // One hop = one self-contained sequence of SINGLE-GATE operations.
            var result = Transport.DeliverBatch(sender, receiver, namespaceId,
                                                filterAuthor, schemaVersion, dropAck);
            ct.ThrowIfCancellationRequested();
            return result;
        }, ct);
    }

    /// <summary>
    /// Drain all pending deltas A→B, then B→A (the async analogue of
    /// <see cref="Transport.SyncRound"/>). The two directions are strictly sequential,
    /// so the gates of A and B never overlap in time; the token is honored between
    /// individual batches.
    /// PRODUCTION FIX: DeliverBatch returns null both when the sender has nothing
    /// pending AND when the produced batch contains zero NEW ops (Applied == 0 —
    /// e.g. an echo/replay round-trip). The drain loop must therefore keep going
    /// while the outbox still holds unacknowledged entries; stopping on the first
    /// Applied==0 result silently strands data (a fresh receiver answers every push
    /// with Applied==0 because it already saw the ops via the reverse direction).
    /// </summary>
    public static Task<SyncRoundResult> SyncRoundAsync(
        Node a, Node b, int namespaceId, int schemaVersion = 1,
        CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            int ab = DrainDirection(a, b, namespaceId, schemaVersion, ct);
            int ba = DrainDirection(b, a, namespaceId, schemaVersion, ct);
            return new SyncRoundResult(ab, ba);
        }, ct);
    }

    private static int DrainDirection(Node src, Node dst, int ns, int schemaVersion, CancellationToken ct)
    {
        int n = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var r = Transport.DeliverBatch(src, dst, ns, schemaVersion: schemaVersion);
            if (r is null) return n;   // sender has nothing pending for this peer (§10.2)
            n++;
        }
    }

    /// <summary>
    /// Atomically persist <paramref name="node"/> off the calling thread. The snapshot
    /// is taken under the node's own gate inside NodePersistence.Save, so concurrent
    /// mutations cannot tear the image.
    /// </summary>
    public static Task SaveAsync(Node node, string path, CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();
            NodePersistence.Save(node, path);
        }, ct);
    }

    /// <summary>Load a persisted node state asynchronously (integrity-checked).</summary>
    public static Task<Node> LoadAsync(string path, CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();
            return NodePersistence.Load(path);
        }, ct);
    }
}
