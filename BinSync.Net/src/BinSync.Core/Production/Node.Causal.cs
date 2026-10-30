// Optional causal apply path (§16.19): gates ApplyBatch on OutboxEntry.CausalDeps.
// Default ApplyBatch is unchanged (no tracker → no gate). Opt in via Causal + ApplyBatchCausal.
namespace BinSync;

/// <summary>Held batch waiting on causal parents (KЗ4).</summary>
public sealed class HeldCausalBatch
{
    public required Batch Batch { get; init; }
    public required byte[] SenderPeer { get; init; }
}

public sealed partial class Node
{
    /// <summary>
    /// When set, <see cref="ApplyBatchCausal"/> evaluates <see cref="OutboxEntry.CausalDeps"/>
    /// before applying (KЗ5 all-or-nothing). Plain <see cref="ApplyBatch"/> ignores this.
    /// </summary>
    public CausalTracker? Causal { get; set; }

    /// <summary>
    /// Apply a batch only when every entry's causal deps are Ready (or no <see cref="Causal"/>).
    /// On Pending: quarantine the whole batch (KЗ4 — does not call ApplyBatchInternal),
    /// then throw <c>CAUSAL_DEP_MISSING</c>. On Cycle / LostDep / TimedOut: throw same code
    /// without enqueuing. On Ready: apply, then NoteApplied each changeId.
    /// </summary>
    public BatchResult ApplyBatchCausal(Batch batch, byte[] senderPeer)
    {
        lock (_lockMeta)
        {
            var tracker = Causal;
            if (tracker is not null && batch.Entries.Count > 0)
            {
                var members = new List<(byte[] ChangeId, IReadOnlyList<byte[]>? Deps)>(batch.Entries.Count);
                foreach (var e in batch.Entries)
                    members.Add((e.ChangeId, e.CausalDeps));

                var outcome = tracker.EvaluateBatch(members, out _);
                if (outcome != CausalOutcome.Ready)
                {
                    if (outcome == CausalOutcome.Pending)
                    {
                        // KЗ4: hold full batch for later DrainAndRetryCausal
                        var held = new HeldCausalBatch { Batch = batch, SenderPeer = (byte[])senderPeer.Clone() };
                        foreach (var e in batch.Entries)
                            tracker.EnqueuePending(e.ChangeId, e.CausalDeps, payload: held);
                    }
                    throw new SyncException("CAUSAL_DEP_MISSING", batch.BatchId);
                }
            }

            var result = ApplyBatchInternal(batch, senderPeer);

            if (tracker is not null && !result.Duplicate)
            {
                foreach (var e in batch.Entries)
                {
                    if (e.ChangeId.Length > 0)
                        tracker.NoteApplied(e.ChangeId);
                }
            }

            return result;
        }
    }

    /// <summary>
    /// After parents arrive, drain quarantine. Timed-out / cycle outcomes are reported but not applied.
    /// </summary>
    public List<(PendingCausalOp Op, CausalOutcome Outcome)> DrainCausal()
    {
        lock (_lockMeta)
        {
            return Causal?.Drain() ?? new List<(PendingCausalOp, CausalOutcome)>();
        }
    }

    /// <summary>
    /// Drain Ready held batches and re-apply them via <see cref="ApplyBatchCausal"/>.
    /// Returns how many batches successfully applied. Timed-out/cycle items are skipped.
    /// </summary>
    public int DrainAndRetryCausal()
    {
        lock (_lockMeta)
        {
            if (Causal is null) return 0;
            int applied = 0;
            var released = Causal.Drain();
            // Deduplicate by batch id (one held batch may be enqueued under several changeIds)
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (op, outcome) in released)
            {
                if (outcome != CausalOutcome.Ready) continue;
                if (op.Payload is not HeldCausalBatch held) continue;
                var key = Convert.ToHexString(held.Batch.BatchId);
                if (!seen.Add(key)) continue;
                try
                {
                    // Re-enter causal gate; should be Ready now that parents are known
                    ApplyBatchCausal(held.Batch, held.SenderPeer);
                    applied++;
                }
                catch (SyncException)
                {
                    // Still blocked or SEQ issues — leave for next drain
                }
            }
            return applied;
        }
    }

    /// <summary>
    /// Mark a locally authored changeId as known (call after WriteField when using causal).
    /// </summary>
    public void NoteCausalApplied(byte[] changeId)
    {
        lock (_lockMeta)
        {
            Causal?.NoteApplied(changeId);
        }
    }

    /// <summary>
    /// Encode standard batch payload and, when entries carry CausalDeps, append the trailer.
    /// Sets <see cref="CausalWire.FlagHasCausalTrailer"/> on the batch flags in the body.
    /// </summary>
    public byte[] EncodeBatchPayloadCausal(Batch batch)
    {
        lock (_lockMeta)
        {
            if (CausalWire.HasDeps(batch.Entries))
                batch.Flags |= CausalWire.FlagHasCausalTrailer;
            var body = EncodeBatchPayloadInternal(batch);
            return CausalWire.EncodeWithTrailer(body, batch.Entries);
        }
    }

    /// <summary>
    /// Decode batch payload; if a causal trailer is present, restore ChangeId / CausalDeps.
    /// </summary>
    public Batch DecodeBatchPayloadCausal(byte[] data)
    {
        lock (_lockMeta)
        {
            // First decode as standard (Reader stops at records end via known lengths).
            // Standard decoder reads exactly the declared structure — trailing bytes may confuse it.
            // Strategy: find trailer magic, decode body prefix only.
            int bodyLen = data.Length;
            for (int i = 0; i + 4 <= data.Length; i++)
            {
                uint m = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(i));
                if (m == CausalWire.Magic) { bodyLen = i; break; }
            }
            var body = bodyLen == data.Length ? data : data.AsSpan(0, bodyLen).ToArray();
            var batch = DecodeBatchPayloadInternal(body);
            if (bodyLen < data.Length)
                CausalWire.TryApplyTrailer(data, batch.Entries);
            return batch;
        }
    }
}
