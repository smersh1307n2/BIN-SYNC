// Optional causal apply path (§16.19): gates ApplyBatch on OutboxEntry.CausalDeps.
// Default ApplyBatch is unchanged (no tracker → no gate). Opt in via Causal + ApplyBatchCausal.
namespace BinSync;

public sealed partial class Node
{
    /// <summary>
    /// When set, <see cref="ApplyBatchCausal"/> evaluates <see cref="OutboxEntry.CausalDeps"/>
    /// before applying (KЗ5 all-or-nothing). Plain <see cref="ApplyBatch"/> ignores this.
    /// </summary>
    public CausalTracker? Causal { get; set; }

    /// <summary>
    /// Apply a batch only when every entry's causal deps are Ready (or no <see cref="Causal"/>).
    /// On Pending: quarantine entries (KЗ4 — does not call <see cref="ApplyBatchInternal"/>),
    /// then throw <c>CAUSAL_DEP_MISSING</c>. On Cycle / LostDep / TimedOut: throw same code
    /// without enqueuing. On Ready: apply, then <see cref="CausalTracker.NoteApplied"/> each changeId.
    /// </summary>
    public BatchResult ApplyBatchCausal(Batch batch, byte[] senderPeer)
    {
        lock (_gate)
        {
            var tracker = Causal;
            if (tracker is not null && batch.Entries.Count > 0)
            {
                var members = new List<(byte[] ChangeId, IReadOnlyList<byte[]>? Deps)>(batch.Entries.Count);
                foreach (var e in batch.Entries)
                    members.Add((e.ChangeId, e.CausalDeps));

                var outcome = tracker.EvaluateBatch(members, out var detail);
                if (outcome != CausalOutcome.Ready)
                {
                    if (outcome == CausalOutcome.Pending)
                    {
                        // KЗ4: hold without advancing cursors / inbox
                        foreach (var e in batch.Entries)
                            tracker.EnqueuePending(e.ChangeId, e.CausalDeps, payload: null);
                    }
                    throw new SyncException("CAUSAL_DEP_MISSING", batch.BatchId);
                }
            }

            var result = ApplyBatchInternal(batch, senderPeer);

            if (tracker is not null && !result.Duplicate)
            {
                foreach (var e in batch.Entries)
                    tracker.NoteApplied(e.ChangeId);
            }

            return result;
        }
    }

    /// <summary>
    /// After parents arrive (via a successful apply or local write), drain the quarantine
    /// and return changeIds that are now Ready. Embedder re-delivers those batches.
    /// Timed-out / cycle outcomes are reported but not auto-applied.
    /// </summary>
    public List<(PendingCausalOp Op, CausalOutcome Outcome)> DrainCausal()
    {
        lock (_gate)
        {
            return Causal?.Drain() ?? new List<(PendingCausalOp, CausalOutcome)>();
        }
    }
}
