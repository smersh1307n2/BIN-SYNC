// Optional causal apply path (§16.19): gates ApplyBatch on OutboxEntry.CausalDeps.
// Default ApplyBatch is unchanged (no tracker → no gate). Opt in via Causal + ApplyBatchCausal.
namespace BinSync;

/// <summary>Held batch waiting on causal parents (KЗ4).</summary>
public sealed class HeldCausalBatch
{
    /// <summary>DELTA_BATCH logical unit: headers, digests, and list of OutboxEntry records.</summary>
    public required Batch Batch { get; init; }
    /// <summary>Peer that sent the held batch.</summary>
    public required NodeId SenderPeer { get; init; }
}

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// When set, <see cref="ApplyBatchCausal(Batch, NodeId)"/> evaluates <see cref="OutboxEntry.CausalDeps"/>
    /// before applying (KЗ5 all-or-nothing). Plain <see cref="ApplyBatch"/> ignores this.
    /// </summary>
    private CausalTracker? _causal;

    /// <summary>Optional causal tracker. Access is serialized with causal apply operations.</summary>
    public CausalTracker? Causal
    {
        get { lock (_lockMeta) { ThrowIfDisposed(); return _causal; } }
        set { lock (_lockMeta) { ThrowIfDisposed(); _causal = value; } }
    }

    /// <summary>
    /// Apply a batch only when every entry's causal deps are Ready (or no <see cref="Causal"/>).
    /// On Pending: quarantine the whole batch (KЗ4 — does not call ApplyBatchInternal),
    /// then throw <c>CAUSAL_DEP_MISSING</c>. On Cycle / LostDep / TimedOut: throw same code
    /// without enqueuing. On Ready: apply, then NoteApplied each changeId.
    /// </summary>
    /// <summary>Apply with typed sender peer.</summary>
    public BatchResult ApplyBatchCausal(Batch batch, NodeId senderPeer)
    {
        ArgumentNullException.ThrowIfNull(batch);
        // Causal evaluation, quarantine and apply operate on one detached object graph.
        return ApplyBatchCausalCore(CloneBatchForPublicRead(batch), senderPeer, expectedTracker: null);
    }

    private BatchResult ApplyBatchCausalCore(
        Batch ownedBatch, NodeId senderPeer, CausalTracker? expectedTracker)
    {
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            var tracker = _causal;
            if (expectedTracker is not null && !ReferenceEquals(tracker, expectedTracker))
                throw new InvalidOperationException("Causal tracker changed while pending batches were being drained.");

            if (tracker is not null && ownedBatch.Entries.Count > 0)
            {
                var members = new List<(BatchId ChangeId, IReadOnlyList<BatchId>? Deps)>(ownedBatch.Entries.Count);
                foreach (var e in ownedBatch.Entries)
                    members.Add((e.ChangeId, e.CausalDeps.Count > 0 ? e.CausalDeps : null));

                var outcome = tracker.EvaluateBatch(members, out _);
                if (outcome != CausalOutcome.Ready)
                {
                    if (outcome == CausalOutcome.Pending)
                    {
                        // KЗ4: hold full batch for later DrainAndRetryCausal.
                        // Keep a detached graph; no caller-owned buffers cross this boundary.
                        var held = new HeldCausalBatch
                        {
                            Batch = ownedBatch,
                            SenderPeer = senderPeer,
                        };
                        foreach (var e in ownedBatch.Entries)
                            tracker.EnqueuePending(e.ChangeId, (IReadOnlyList<BatchId>)e.CausalDeps, payload: held);
                    }
                    else
                    {
                        // A rejected batch can never become applicable later. Mark each
                        // not-yet-known ChangeId lost so its downstream dependents are
                        // transitively discarded instead of waiting until timeout.
                        foreach (var e in ownedBatch.Entries)
                            if (!e.ChangeId.IsZero && !tracker.IsKnown(e.ChangeId))
                                tracker.MarkLost(e.ChangeId);
                    }
                    throw new SyncException("CAUSAL_DEP_MISSING", ownedBatch.BatchId);
                }
            }

            result = ApplyBatchInternal(ownedBatch, senderPeer);

            // A duplicate is already durably present in the receiver Inbox. Keep the
            // causal tracker aligned with that accepted state as well.
            if (tracker is not null)
            {
                foreach (var e in ownedBatch.Entries)
                {
                    if (!e.ChangeId.IsZero)
                        tracker.NoteApplied(e.ChangeId);
                }
            }
        }

        // Match ApplyBatch: application callbacks run only after Storage → Outbox → Meta
        // have all been released.
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return result;
    }

    /// <summary>
    /// After parents arrive, drain quarantine. Timed-out / cycle outcomes are reported but not applied.
    /// </summary>
    public List<(PendingCausalOp Op, CausalOutcome Outcome)> DrainCausal()
    {
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            return Causal?.Drain() ?? new List<(PendingCausalOp, CausalOutcome)>();
        }
    }

    /// <summary>
    /// Drain Ready held batches and re-apply them via <see cref="ApplyBatchCausal(Batch, NodeId)"/>.
    /// Returns how many batches successfully applied. Timed-out/cycle items are skipped.
    /// </summary>
    public int DrainAndRetryCausal()
    {
        // Never hold Meta while entering the normal Storage → Outbox → Meta apply order.
        CausalTracker? tracker;
        lock (_lockMeta) { ThrowIfDisposed(); tracker = _causal; }
        if (tracker is null) return 0;

        var released = tracker.Drain();
        int applied = 0;
        // One batch is enqueued once per entry. Keep the complete group so a failed
        // retry can restore every pending ChangeId, not just the first one.
        var batches = new Dictionary<BatchId, (HeldCausalBatch Held, List<PendingCausalOp> Ops)>();
        foreach (var (op, outcome) in released)
        {
            if (outcome != CausalOutcome.Ready || op.Payload is not HeldCausalBatch held)
                continue;
            if (!batches.TryGetValue(held.Batch.BatchId, out var group))
                group = (held, new List<PendingCausalOp>());
            group.Ops.Add(op);
            batches[held.Batch.BatchId] = group;
        }

        foreach (var group in batches.Values)
        {
            try
            {
                ApplyBatchCausalCore(CloneBatchForPublicRead(group.Held.Batch), group.Held.SenderPeer, tracker);
                applied++;
            }
            catch (InvalidOperationException) when (!ReferenceEquals(Causal, tracker))
            {
                // The host replaced the causal tracker while this drain was in progress.
                break;
            }
            catch (SyncException)
            {
                // The batch did not commit (for example, its stream cursor is not ready).
                foreach (var op in group.Ops) tracker.RequeuePending(op);
            }
            catch
            {
                foreach (var op in group.Ops) tracker.RequeuePending(op);
                throw;
            }
        }
        return applied;
    }

    /// <summary>
    /// Mark a locally authored changeId as known (call after WriteField when using causal).
    /// </summary>
    public void NoteCausalApplied(BatchId changeId)
    {
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            Causal?.NoteApplied(changeId);
        }
    }

    /// <summary>
    /// Encode standard batch payload and, when entries carry CausalDeps, append the trailer.
    /// Sets <see cref="CausalWire.FlagHasCausalTrailer"/> on the batch flags in the body.
    /// </summary>
    public byte[] EncodeBatchPayloadCausal(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            bool hasDeps = CausalWire.HasDeps(batch.Entries);
            int flags = hasDeps
                ? batch.Flags | CausalWire.FlagHasCausalTrailer
                : batch.Flags & ~CausalWire.FlagHasCausalTrailer;
            // Encode a shallow header copy so adding/clearing the trailer bit never
            // mutates the caller's Batch. Entry buffers are read-only during encoding.
            var wireBatch = new Batch(
                batch.NamespaceId, batch.SchemaId, batch.SchemaVersion, batch.MergePolicy, flags,
                batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
                batch.BatchId, batch.PrevBatchDigest, batch.RecordsRoot, batch.BatchDigest,
                batch.Entries)
            {
                AuthorStreamNodeId = batch.AuthorStreamNodeId,
            };
            var body = EncodeBatchPayloadInternal(wireBatch);
            return CausalWire.EncodeWithTrailer(body, wireBatch.Entries);
        }
    }

    /// <summary>
    /// Decode batch payload; if a causal trailer is present, restore ChangeId / CausalDeps.
    /// </summary>
    public Batch DecodeBatchPayloadCausal(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            if (data.Length < 8)
                throw new EndOfStreamException("Truncated DELTA_BATCH header.");

            bool hasTrailer = (data[7] & CausalWire.FlagHasCausalTrailer) != 0;
            if (!hasTrailer)
            {
                var standard = DecodeBatchPayloadInternal(data);
                standard.MaterializeEntries();
                return standard;
            }

            int bodyLength = CausalWire.GetBodyLength(data);
            var batch = DecodeBatchPayloadInternal(data.AsMemory(0, bodyLength));
            CausalWire.ApplyTrailerStrict(data, bodyLength, batch.Entries);
            batch.MaterializeEntries(); // public API — owned buffers
            return batch;
        }
    }
}
