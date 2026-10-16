namespace BinSync.Core;

/// <summary>Outcome of one full synchronization round between two nodes.</summary>
public sealed record SyncRoundResult(
    int BatchesFromA, ApplyResult AToB,
    int BatchesFromB, ApplyResult BToA);

/// <summary>
/// Async wrappers over the synchronous §12.18 engine.
///
/// Deadlock prevention: a transport hop NEVER holds the gates of two nodes simultaneously.
/// Each hop takes exactly one node gate to copy/peek state out, releases it, and only then
/// takes the other node's gate to apply. Because <see cref="Node"/> operations are individually
/// atomic under their own gate, this interleaving is safe and cycle-free.
///
/// Every method accepts a <see cref="CancellationToken"/> and honors cooperative cancellation:
/// work runs on the thread pool via <see cref="Task.Run(Func{T}, CancellationToken)"/> and the
/// token is re-checked at each await point between hops.
/// </summary>
public static class AsyncTransport
{
    /// <summary>
    /// Deliver one batch produced by <paramref name="from"/> into <paramref name="to"/>,
    /// then ack the sender. Returns the apply verdict on the receiver.
    /// </summary>
    public static Task<ApplyResult> DeliverBatchAsync(Node from, Node to, CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();

            // Hop 1: take ONLY the sender's gate, build+copy the batch, release.
            var batch = from.CreateDeltaBatch(to.Id);
            if (batch is null) return ApplyResult.Duplicate; // nothing to send
            ct.ThrowIfCancellationRequested();

            // Stamp the wire batch with the SENDER's stream id so the receiver keys its inbox
            // cursor by the true stream owner (re-stamped copy keeps digest consistent).
            batch = Restamp(batch, from.Id);

            // Sender's gate is released here — now we may touch the receiver's gate.
            // Hop 2: take ONLY the receiver's gate.
            var res = to.ApplyBatch(batch);
            ct.ThrowIfCancellationRequested();

            // Hop 3: back to the sender alone (ack drops the delivered prefix).
            if (res == ApplyResult.Applied)
                from.ProcessAck(to.Id, batch.Seq);
            return res;
        }, ct);
    }

    /// <summary>
    /// One full anti-entropy round: A→B delta exchange followed by B→A delta exchange.
    /// The two hops are strictly sequential so no two node gates overlap in time.
    /// </summary>
    public static Task<SyncRoundResult> SyncRoundAsync(Node a, Node b, CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();

            ApplyResult ab = ApplyResult.Duplicate;
            int nAb = 0;
            for (int i = 0; i < MaxBatchesPerRound; i++)
            {
                ct.ThrowIfCancellationRequested();
                var batch = a.CreateDeltaBatch(b.Id);          // lock a, copy, unlock
                if (batch is null) break;
                nAb++;
                batch = Restamp(batch, a.Id);                 // sender stream id on the wire
                var r = b.ApplyBatch(batch);                  // lock b, unlock
                if (r == ApplyResult.Applied) a.ProcessAck(b.Id, batch.Seq);
                ab = r;                                       // single-node locks only — never both
                if (r != ApplyResult.Applied) break;
            }

            ct.ThrowIfCancellationRequested();

            ApplyResult ba = ApplyResult.Duplicate;
            int nBa = 0;
            for (int i = 0; i < MaxBatchesPerRound; i++)
            {
                ct.ThrowIfCancellationRequested();
                var batch = b.CreateDeltaBatch(a.Id);
                if (batch is null) break;
                nBa++;
                batch = Restamp(batch, b.Id);
                var r = a.ApplyBatch(batch);
                if (r == ApplyResult.Applied) b.ProcessAck(a.Id, batch.Seq);
                ba = r;
                if (r != ApplyResult.Applied) break;
            }

            return new SyncRoundResult(nAb, ab, nBa, ba);
        }, ct);
    }

    /// <summary>Copy of a batch re-stamped with the delivering node's stream id (Peer),
    /// with BatchId/Digest recomputed over the new canonical bytes.</summary>
    private static DeltaBatch Restamp(DeltaBatch b, string senderId)
    {
        if (b.Peer == senderId) return b;
        var copy = new DeltaBatch
        {
            Peer = senderId,
            Seq = b.Seq,
            DepSeq = b.DepSeq,
            RelaySeq = b.RelaySeq,
            RelayAuthor = b.RelayAuthor,
            Ops = b.Ops,
            Tombs = b.Tombs,
            AliasRenames = b.AliasRenames,
            HlcTs = b.HlcTs,
            HlcLog = b.HlcLog,
            HlcNode = b.HlcNode,
        };
        copy.ComputeDigest();
        copy.BatchId = $"{senderId}:{copy.Seq}:{copy.Digest[..16]}";
        return copy;
    }

    /// <summary>Cap on batches shipped per direction in one round (keeps rounds bounded).</summary>

    /// <summary>Cap on batches shipped per direction in one round (keeps rounds bounded).</summary>
    public const int MaxBatchesPerRound = 1024;

    /// <summary>
    /// Take a full snapshot of <paramref name="source"/> and merge it into <paramref name="target"/>.
    /// Snapshot capture happens under the source gate only; the merge happens later under the
    /// target gate only — the two gates are never held together.
    /// </summary>
    public static Task SendSnapshotAsync(Node source, Node target, CancellationToken ct = default)
    {
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();
            var snap = source.TakeSnapshot();   // lock source, deep-copy, unlock
            ct.ThrowIfCancellationRequested();
            target.ApplySnapshot(snap);         // lock target only now
            ct.ThrowIfCancellationRequested();
        }, ct);
    }
}
