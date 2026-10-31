// CreateDeltaBatch / SignBatch / AdvanceStream. Behavior unchanged.

namespace BinSync;

public sealed partial class Node
{
    // --------------------------------------------------------------------- batching
    /// BUGFIX (scenario 23): a relay must stamp ops of a THIRD-PARTY author with that
    /// author's own epoch, not the relay's self.epoch (see Python port docstring).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AuthorEpochInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int AuthorEpoch(NodeId author)
    {
        lock (_lockMeta) return AuthorEpochInternal(author);
    }

    internal int AuthorEpochInternal(NodeId author)
    {
        foreach (var m in AliasToNode.Values)
            foreach (var (nid, ep) in m.Values)
                if (nid == (NodeId)author) return ep;
        if (NodeId.EqualsBytes(author)) return Epoch;
        return KnownEpochs.TryGetValue((author, -1), out var ke) ? ke : Epoch;
    }

    /// Build DELTA_BATCH from outbox beyond peer's per-stream watermark.
    /// §13.6.1 [Р3]: NO coalescing across authors — one author per batch.
    /// §13.6.2 [R4]: translate each op's `origin` into the alias agreed for THIS session.
    /// SPEC HARDENING: unknown author ⇒ deterministic UNKNOWN_ALIAS rejection (never a
    /// silent skip that would hole the seq stream). Full rationale in the Python port.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>CreateDeltaBatchInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Batch? CreateDeltaBatch(NodeId targetPeer, int namespaceId,
                                   byte[]? filterAuthorNodeId = null,
                                   int schemaId = 0, int schemaVersion = 1)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return CreateDeltaBatchInternal(targetPeer, namespaceId, filterAuthorNodeId, schemaId, schemaVersion);
    }

    internal Batch? CreateDeltaBatchInternal(NodeId targetPeer, int namespaceId,
                                       byte[]? filterAuthorNodeId = null,
                                       int schemaId = 0, int schemaVersion = 1)
    {
        var pkey = (targetPeer, namespaceId);
        if (!PeerStates.TryGetValue(pkey, out var ps)) PeerStates[pkey] = ps = new PeerState(targetPeer, namespaceId);
        long wm;
        byte[] stream;
        List<OutboxEntry> pending;
        var all = Outbox.TryGetValue(namespaceId, out var ob) ? ob : new List<OutboxEntry>();
        if (filterAuthorNodeId is not null)
        {
            stream = filterAuthorNodeId;
            wm = ps.AckedFor(stream);
            pending = all.Where(e => e.AuthorNodeId.AsSpan().SequenceEqual(stream) && e.Seq > wm).ToList();
            // Errata E-4: gap normalisation also applies on the explicit-filter path.
            if (pending.Count > 0)
            {
                long gap = pending[0].Seq - (wm + 1);
                if (gap > 0) foreach (var e in pending) e.Seq -= gap;
            }
        }
        else
        {
            // pick the CONTIGUOUS stream first (first un-acked seq == acked+1), earliest
            // outbox position; gapped stream as fallback (tail renumbered). BUGFIX/HARDENING
            // (scenario 42 livelock) — see Python port comment.
            var firstPending = new Dictionary<byte[], long>(BytesEq.Instance);
            foreach (var e in all)
            {
                long w = ps.AckedFor(e.AuthorNodeId);   // Errata E-4: per-author watermark
                if (e.Seq > w && (!firstPending.TryGetValue(e.AuthorNodeId, out var cur) || e.Seq < cur))
                    firstPending[e.AuthorNodeId] = e.Seq;
            }
            stream = Array.Empty<byte>();
            bool found = false;
            foreach (var kv in firstPending)
                if (kv.Value == ps.AckedFor(kv.Key) + 1) { stream = kv.Key; found = true; break; }
            if (!found && firstPending.Count > 0) { stream = firstPending.Keys.First(); found = true; }
            if (!found) return null;
            wm = ps.AckedFor(stream);
            pending = all.Where(e => e.AuthorNodeId.AsSpan().SequenceEqual(stream) && e.Seq > wm).ToList();
        }
        if (pending.Count == 0) return null;
        // BUGFIX: cursorStart = first still-un-ACKed seq of THIS stream, not lastPushedSeq+1.
        // SPEC HARDENING (§10.5): close gaps deterministically by renumbering the un-acked
        // tail so it starts exactly at lastAckedSeq+1 ([П2]/[S2]).
        {
            long gap = pending[0].Seq - (wm + 1);   // Errata E-4: per-author watermark `wm`
            if (gap > 0) foreach (var e in pending) e.Seq -= gap;
        }
        long start = pending[0].Seq, end = pending[^1].Seq;
        var translated = new List<OutboxEntry>();
        foreach (var e in pending)
        {
            int? alias = AliasOf(targetPeer, (NodeId)e.AuthorNodeId, Epoch);
            if (alias is null && NodeId.EqualsBytes(e.AuthorNodeId))
                alias = 1;   // BUGFIX: self-entry fallback keeps [А1] intact
            if (alias is null)
            {
                // Errata E-11 (scenario 23): try incremental ALIAS_MAP negotiation before failing closed.
                // NOTE: a resumed node (NodePersistence.Load) has registry KEYS but no live Node
                // handles; TryGetValue then yields a null value — treat it as "peer not attached".
                var peerNode = PeersByNid is not null && PeersByNid.TryGetValue(targetPeer, out var pn) ? pn : null;
                if (peerNode is not null && !NodeId.EqualsBytes(e.AuthorNodeId))
                {
                    int epTry = KnownAuthorEpoch((NodeId)e.AuthorNodeId) ?? Epoch;
                    alias = EnsureAlias(peerNode, (NodeId)e.AuthorNodeId, epTry);
                }
                else if (peerNode is null)
                {
                    // Resumed node: the registry already holds an alias for this author in the
                    // target session (persisted), but there is no live Node handle to negotiate
                    // ALIAS_MAP with. Reuse the durable mapping instead of failing closed.
                    if (AliasToNode.TryGetValue(targetPeer, out var existingMap))
                        foreach (var (a_, (nid_, _)) in existingMap)
                            if (nid_ == (NodeId)e.AuthorNodeId) { alias = a_; break; }
                }
            }
            if (alias is null)
                throw new SyncException("UNKNOWN_ALIAS");   // [R4'] + Errata E-11
            var op = Ops.DecodeOp(e.Payload).Op;
            SetOrigin(op, (ushort)alias.Value);   // ПЕРЕЗАПИСЬ of session alias (§13.6.2)
            // Persistence byte-stability: an outbox FieldWrite payload may be truncated to its
            // fixed header (the value bytes live in the record's FieldState after a save/load
            // round-trip). Restore the current stored value so retransmission carries real data.
            if (op is OpFieldWrite fw && fw.ValueLenPos >= 0 && fw.Value.Length == 0 &&
                Ns(e.NamespaceId).TryGetValue(e.RecordId, out var wrec) &&
                wrec.State.TryGetValue(fw.FieldId, out var wfld))
            {
                fw.Value = wfld.Value ?? Array.Empty<byte>();
                fw.ValueLenPos = -1;   // we now have the bytes — encode a normal varint
            }
            var newPayload = Ops.EncodeOp(op, fullAuthor: false);
            translated.Add(new OutboxEntry
            {
                NamespaceId = e.NamespaceId,
                Seq = e.Seq,
                ChangeId = e.ChangeId,
                RecordId = e.RecordId,
                OpKind = e.OpKind,
                Payload = newPayload,
                AuthorNodeId = e.AuthorNodeId,
                CreatedAt = e.CreatedAt,
            });
        }
        if (translated.Count == 0) return null;
        var recordsRoot = ComputeRecordsRoot(translated);
        // BUGFIX (scenario 23): stamp with the epoch LEARNED for the author in the target
        // session (ALIAS_MAP entry / per-session watermark), not self.epoch.
        // BUGFIX (errata E-6 follow-up): stream key is `stream`, not a stale loop variable.
        int ep = Epoch;
        byte[] author = stream;
        if (!NodeId.EqualsBytes(author))
        {
            var m = AliasToNode.TryGetValue(targetPeer, out var am) ? am : new Dictionary<int, (NodeId Id, int Epoch)>();
            bool hit = false;
            foreach (var (alias_, (nid2, ep2)) in m)
                if (nid2 == (NodeId)author) { ep = ep2; hit = true; break; }
            if (!hit)
                foreach (var kv in KnownEpochs)
                    if (kv.Key.Item2 >= 0 && kv.Key.Item1 == targetPeer)
                        ep = Math.Max(ep, kv.Value);
        }
        var batch = new Batch(namespaceId, schemaId, schemaVersion, 0x02, 0,   // 0x02 = LWW_FIELD
                              NodeId, ep, start, end, Zero128, Zero128, recordsRoot, Zero128, translated);
        ps.LastPushedSeq = Math.Max(ps.LastPushedSeq, end);
        // BUGFIX (implementation finding): a freshly built batch must be fully signed —
        // sign_batch_for is idempotent w.r.t. the id cache; the per-stream chain checkpoint
        // advances only after successful delivery (AdvanceStream).
        return SignBatchFor(batch, targetPeer);
    }


    /// Bind batchId + prevBatchDigest + batchDigest at WIRE time for one stream.
    /// SPEC HARDENING (§12.17.2 / [Т3]): the chain is PER STREAM ((issuer, ns, epoch), targetPeer).
    /// Deterministic replay: the SAME (stream, cursor span) always yields the SAME batchId
    /// and byte-identical digest (§10.2.2 step 3, [П7]).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>SignBatchForInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Batch SignBatchFor(Batch batch, NodeId targetPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return SignBatchForInternal(batch, targetPeer);
    }

    internal Batch SignBatchForInternal(Batch batch, NodeId targetPeer)
    {
        // SPEC CORRECTION (errata E-2 follow-up): the stream chain is keyed by the ACTUAL
        // stamped issuerEpoch of this batch (author's epoch when relaying).
        var chainKey = (NodeId, batch.NamespaceId, batch.IssuerEpoch);
        var streamKey = (chainKey, targetPeer.ToArray());
        var ps = PeerStates[(targetPeer, batch.NamespaceId)];
        // BUGFIX (port divergence): the reference port keys the id cache by the batch's
        // OWN author stream (entries[0].AuthorNodeId) — NOT the mutable receiver-resolved
        // AuthorStreamNodeId. Keying by the latter made a re-decoded copy of the SAME wire
        // payload mint a fresh random batchId (the decoded entries carry the ISSUER's
        // nodeId as placeholder author, so sender/receiver keys diverged), breaking the
        // deterministic-replay invariant §10.2.2 [П7] and every out-of-order / lost-ACK
        // scenario (duplicate deliveries escaped the inbox check).
        var key = (batch.AuthorStream(), batch.CursorStart, batch.CursorEnd);
        if (!ps.Ids.TryGetValue(key, out var cached))
        {
            cached = (Guid.NewGuid().ToByteArray(),
                      LastStreamDigest.TryGetValue(streamKey, out var p) ? p : Zero128);
            ps.Ids[key] = cached;
        }
        var (batchId, prev) = cached;
        batch.BatchId = batchId;
        batch.PrevBatchDigest = prev;
        batch.BatchDigest = ComputeBatchDigest(batchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, prev);
        return batch;
    }

    /// Called after the frame was successfully handed to targetPeer's transport: the
    /// stream chain checkpoint moves to this batch's digest.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AdvanceStreamInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void AdvanceStream(NodeId targetPeer, int namespaceId, Batch batch)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) AdvanceStreamInternal(targetPeer, namespaceId, batch);
    }

    internal void AdvanceStreamInternal(NodeId targetPeer, int namespaceId, Batch batch)
    {
        LastStreamDigest[((NodeId, namespaceId, batch.IssuerEpoch), targetPeer)] = batch.BatchDigest;
    }
    // EncodeBatchPayload / DecodeBatchPayload → Production/Node.BatchCodec.cs

}
