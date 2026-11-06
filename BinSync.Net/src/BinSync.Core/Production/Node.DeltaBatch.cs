// CreateDeltaBatch / SignBatch / AdvanceStream. Behavior unchanged.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    // --------------------------------------------------------------------- batching
    /// BUGFIX (scenario 23): a relay must stamp ops of a THIRD-PARTY author with that
    /// author's own epoch, not the relay's self.epoch (see Python port docstring).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AuthorEpochInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int AuthorEpoch(NodeId author)
    {
        lock (_lockMeta) { ThrowIfDisposed(); return AuthorEpochInternal(author); }
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
                                   NodeId? filterAuthorNodeId = null,
                                   int schemaId = 0, int schemaVersion = 1)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            // The internal builder can detach payload buffers while materializing the
            // selected stream. This avoids building a second complete public DTO graph.
            return CreateDeltaBatchInternal(targetPeer, namespaceId, filterAuthorNodeId,
                schemaId, schemaVersion, detachPublicBuffers: true);
        }
    }

    internal Batch? CreateDeltaBatchInternal(NodeId targetPeer, int namespaceId,
                                       NodeId? filterAuthorNodeId = null,
                                       int schemaId = 0, int schemaVersion = 1,
                                       bool detachPublicBuffers = false)
    {
        var pkey = (targetPeer, namespaceId);
        if (!PeerStates.TryGetValue(pkey, out var ps)) PeerStates[pkey] = ps = new PeerState(targetPeer, namespaceId);
        long wm;
        NodeId stream;
        var all = Outbox.TryGetValue(namespaceId, out var ob) ? ob : new List<OutboxEntry>();
        if (filterAuthorNodeId is not null)
        {
            stream = filterAuthorNodeId.Value;
            wm = ps.AckedFor(stream);
        }
        else
        {
            var firstPending = new Dictionary<NodeId, long>();
            foreach (var e in all)
            {
                long offset = ps.StreamOffsets.TryGetValue(e.AuthorNodeId, out var savedOffset) ? savedOffset : 0;
                long localAck = checked(ps.AckedFor(e.AuthorNodeId) + offset);
                if (e.Seq > localAck &&
                    (!firstPending.TryGetValue(e.AuthorNodeId, out var cur) || e.Seq < cur))
                    firstPending[e.AuthorNodeId] = e.Seq;
            }
            bool found = false;
            stream = default;
            foreach (var kv in firstPending)
            {
                long offset = ps.StreamOffsets.TryGetValue(kv.Key, out var savedOffset) ? savedOffset : 0;
                long localAck = checked(ps.AckedFor(kv.Key) + offset);
                if (kv.Value == localAck + 1) { stream = kv.Key; found = true; break; }
            }
            if (!found && firstPending.Count > 0) { stream = firstPending.Keys.First(); found = true; }
            if (!found) return null;
            wm = ps.AckedFor(stream);
        }

        long streamOffset;
        if (ps.StreamOffsets.TryGetValue(stream, out var savedStreamOffset))
        {
            if (savedStreamOffset > 0 && !ps.InitialSyncComplete)
                throw new SyncException("SNAPSHOT_REQUIRED");
            streamOffset = savedStreamOffset;
        }
        else
        {
            streamOffset = 0;
            if (!ps.StreamAcked.ContainsKey(stream))
            {
                long firstLocal = long.MaxValue;
                foreach (var e in all)
                    if (e.AuthorNodeId == stream && e.Seq < firstLocal) firstLocal = e.Seq;
                if (firstLocal > 1 && !ps.InitialSyncComplete)
                    throw new SyncException("SNAPSHOT_REQUIRED");
                if (firstLocal != long.MaxValue) streamOffset = Math.Max(0, firstLocal - 1);
            }
            ps.StreamOffsets[stream] = streamOffset;
        }

        long localWm = checked(wm + streamOffset);

        // First pass: determine the selected stream span without allocating one
        // temporary OutboxEntry per outbox item. The source outbox remains untouched.
        int selectedCount = 0;
        long start = 0, end = 0;
        for (int i = 0; i < all.Count; i++)
        {
            var source = all[i];
            if (source.AuthorNodeId != stream || source.Seq <= localWm) continue;
            long seq = checked(source.Seq - streamOffset);
            if (selectedCount == 0) start = seq;
            end = seq;
            selectedCount++;
        }
        if (selectedCount == 0) return null;

        // Every selected entry belongs to the same author stream. Resolve its session
        // alias once, not once per operation (AliasOfInternal scans the reverse map).
        int? streamAlias = stream == NodeId
            ? AliasOfInternal(targetPeer, NodeId, Epoch) ?? 1
            : AliasOfInternal(targetPeer, stream, Epoch);
        if (streamAlias is null)
        {
            // Errata E-11 (scenario 23): negotiate a missing alias once per stream.
            // A resumed node has registry keys but no live peer handle; reuse its
            // persisted forward mapping instead of attempting negotiation.
            var peerNode = PeersByNid is not null && PeersByNid.TryGetValue(targetPeer, out var pn) ? pn : null;
            if (peerNode is not null && stream != NodeId)
            {
                int epTry = KnownAuthorEpoch(stream) ?? Epoch;
                streamAlias = EnsureAlias(peerNode, stream, epTry);
            }
            else if (peerNode is null)
            {
                if (AliasToNode.TryGetValue(targetPeer, out var existingMap))
                    foreach (var (a_, (nid_, _)) in existingMap)
                        if (nid_ == stream) { streamAlias = a_; break; }
            }
        }
        if (streamAlias is null)
            throw new SyncException("UNKNOWN_ALIAS");   // [R4'] + Errata E-11

        // Build the final DTO graph directly: one OutboxEntry wrapper per selected
        // operation, rather than pending + translated wrapper lists.
        var entries = new List<OutboxEntry>(selectedCount);
        ushort origin = (ushort)streamAlias.Value;
        for (int i = 0; i < all.Count; i++)
        {
            var e = all[i];
            if (e.AuthorNodeId != stream || e.Seq <= localWm) continue;
            var stamped = Ops.TryCloneWithOriginShared(e.Payload, origin);
            byte[] newPayload;
            if (stamped is not null)
            {
                newPayload = stamped;
            }
            else
            {
                var op = Ops.DecodeOp(e.Payload).Op;
                SetOrigin(op, origin);   // ПЕРЕЗАПИСЬ of session alias (§13.6.2)
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
                newPayload = Ops.EncodeOp(op, fullAuthor: false);
            }
            // Share when TryClone returned the same array backing e.Payload.
            bool shared = System.Runtime.InteropServices.MemoryMarshal.TryGetArray(e.Payload, out var paySeg)
                && object.ReferenceEquals(paySeg.Array, newPayload)
                && paySeg.Offset == 0 && paySeg.Count == newPayload.Length;
            entries.Add(new OutboxEntry
            {
                NamespaceId = e.NamespaceId,
                Seq = checked(e.Seq - streamOffset),
                ChangeId = e.ChangeId,
                RecordId = e.RecordId,
                OpKind = e.OpKind,
                Payload = shared ? e.Payload : newPayload,
                AuthorNodeId = e.AuthorNodeId,
                CreatedAt = e.CreatedAt,
                PayloadNeedsDetachment = detachPublicBuffers && shared,
            });
        }
        if (detachPublicBuffers)
            DetachPublicBatchPayloads(entries);
        var recordsRoot = ComputeRecordsRoot(entries);
        // BUGFIX (scenario 23): stamp with the epoch LEARNED for the author in the target
        // session (ALIAS_MAP entry / per-session watermark), not self.epoch.
        // BUGFIX (errata E-6 follow-up): stream key is `stream`, not a stale loop variable.
        int ep = Epoch;
        NodeId author = stream;
        if (author != NodeId)
        {
            if (!AliasToNode.TryGetValue(targetPeer, out var m)) m = EmptyAliasMap;
            bool hit = false;
            foreach (var (alias_, (nid2, ep2)) in m)
                if (nid2 == author) { ep = ep2; hit = true; break; }
            if (!hit)
                foreach (var kv in KnownEpochs)
                    if (kv.Key.Item2 >= 0 && kv.Key.Item1 == targetPeer)
                        ep = Math.Max(ep, kv.Value);
        }
        var batch = new Batch(namespaceId, schemaId, schemaVersion, 0x02, 0,   // 0x02 = LWW_FIELD
                              NodeId, ep, start, end, default, default, recordsRoot, default, entries);
        ps.LastPushedSeq = Math.Max(ps.LastPushedSeq, end);
        // BUGFIX (implementation finding): a freshly built batch must be fully signed —
        // sign_batch_for is idempotent w.r.t. the id cache; the per-stream chain checkpoint
        // advances only after successful delivery (AdvanceStream).
        return SignBatchForInternal(batch, targetPeer);
    }



    // Public Batch payloads must not alias mutable engine-owned outbox arrays.
    // Payloads already rewritten into fresh arrays are caller-owned and need no copy.
    // Pack only the still-shared payloads into one backing array, avoiding a second
    // copy of every payload that origin translation has already materialized.
    private static void DetachPublicBatchPayloads(List<OutboxEntry> entries)
    {
        int totalLength = 0;
        for (int i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.PayloadNeedsDetachment)
                totalLength = checked(totalLength + entry.Payload.Length);
        }

        if (totalLength == 0)
        {
            for (int i = 0; i < entries.Count; i++)
            {
                if (!entries[i].PayloadNeedsDetachment) continue;
                entries[i].Payload = ReadOnlyMemory<byte>.Empty;
                entries[i].PayloadNeedsDetachment = false;
            }
            return;
        }

        var backing = new byte[totalLength];
        int offset = 0;
        for (int i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (!entry.PayloadNeedsDetachment) continue;

            var payload = entry.Payload;
            int length = payload.Length;
            if (length != 0)
            {
                payload.Span.CopyTo(backing.AsSpan(offset, length));
                entry.Payload = new ReadOnlyMemory<byte>(backing, offset, length);
                offset += length;
            }
            else
            {
                entry.Payload = ReadOnlyMemory<byte>.Empty;
            }
            entry.PayloadNeedsDetachment = false;
        }
    }

    /// <summary>
    /// Drop outbox entries that every known peer of this namespace has acknowledged.
    /// A session peer that has no <see cref="PeerState"/> yet has not acked — those
    /// entries stay (relay to a peer not yet contacted). No peers at all: keep the
    /// log (sneakernet / not-yet-exported).
    /// </summary>
    internal void CompactOutbox(int namespaceId)
    {
        if (!Outbox.TryGetValue(namespaceId, out var list) || list.Count == 0) return;

        var interested = new List<NodeId>(4);
        if (PeersByNid is not null)
        {
            foreach (var id in PeersByNid.Keys)
                interested.Add(id);
        }
        foreach (var id in AliasToNode.Keys)
        {
            if (!interested.Contains(id)) interested.Add(id);
        }
        foreach (var kv in PeerStates)
        {
            if (kv.Key.Item2 == namespaceId && !interested.Contains(kv.Key.Item1))
                interested.Add(kv.Key.Item1);
        }
        if (interested.Count == 0) return;

        int w = 0;
        for (int i = 0; i < list.Count; i++)
        {
            var e = list[i];
            bool pending = false;
            for (int p = 0; p < interested.Count; p++)
            {
                if (!PeerStates.TryGetValue((interested[p], namespaceId), out var ps))
                {
                    pending = true;
                    break;
                }
                long offset = ps.StreamOffsets.TryGetValue(e.AuthorNodeId, out var savedOffset) ? savedOffset : 0;
                long localAck = checked(ps.AckedFor(e.AuthorNodeId) + offset);
                if (localAck < e.Seq)
                {
                    pending = true;
                    break;
                }
            }
            if (pending) list[w++] = e;
        }
        if (w < list.Count)
            list.RemoveRange(w, list.Count - w);
    }

    /// Bind batchId + prevBatchDigest + batchDigest at WIRE time for one stream.
    /// SPEC HARDENING (§12.17.2 / [Т3]): the chain is PER STREAM ((issuer, ns, epoch), targetPeer).
    /// Deterministic replay: the SAME (stream, cursor span) always yields the SAME batchId
    /// and byte-identical digest (§10.2.2 step 3, [П7]).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>SignBatchForInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Batch SignBatchFor(Batch batch, NodeId targetPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) { ThrowIfDisposed(); return SignBatchForInternal(batch, targetPeer); }
    }

    internal Batch SignBatchForInternal(Batch batch, NodeId targetPeer)
    {
        // SPEC CORRECTION (errata E-2 follow-up): the stream chain is keyed by the ACTUAL
        // stamped issuerEpoch of this batch (author's epoch when relaying).
        var chainKey = (NodeId, batch.NamespaceId, batch.IssuerEpoch);
        var streamKey = (chainKey, targetPeer);
        var ps = PeerStates[(targetPeer, batch.NamespaceId)];
        // BUGFIX (port divergence): the reference port keys the id cache by the batch's
        // OWN author stream (entries[0].AuthorNodeId) — NOT the mutable receiver-resolved
        // AuthorStreamNodeId. Keying by the latter made a re-decoded copy of the SAME wire
        // payload mint a fresh random batchId (the decoded entries carry the ISSUER's
        // nodeId as placeholder author, so sender/receiver keys diverged), breaking the
        // deterministic-replay invariant §10.2.2 [П7] and every out-of-order / lost-ACK
        // scenario (duplicate deliveries escaped the inbox check).
        var key = (batch.AuthorStreamId(), batch.CursorStart, batch.CursorEnd);
        if (!ps.Ids.TryGetValue(key, out var cached))
        {
            Span<byte> gbuf = stackalloc byte[16];
            Guid.NewGuid().TryWriteBytes(gbuf);
            cached = (BatchId.From(gbuf),
                      LastStreamDigest.TryGetValue(streamKey, out var p) ? p : default);
            ps.Ids[key] = cached;
        }
        var (batchId, prev) = cached;
        batch.BatchId = batchId;
        batch.PrevBatchDigest = prev;
        batch.BatchDigest = ComputeBatchDigest(batch.BatchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, prev);
        return batch;
    }

    /// Called after the frame was successfully handed to targetPeer's transport: the
    /// stream chain checkpoint moves to this batch's digest.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AdvanceStreamInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void AdvanceStream(NodeId targetPeer, int namespaceId, Batch batch)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) { ThrowIfDisposed(); AdvanceStreamInternal(targetPeer, namespaceId, batch); }
    }

    internal void AdvanceStreamInternal(NodeId targetPeer, int namespaceId, Batch batch)
    {
        var author = batch.AuthorStreamId();
        if (author.IsZero || batch.CursorStart <= 0 || batch.CursorEnd < batch.CursorStart)
            throw new SyncException("ACK_OUT_OF_RANGE", batch.BatchId);
        LastStreamDigest[((NodeId, namespaceId, batch.IssuerEpoch), targetPeer)] = batch.BatchDigest;
        if (!PeerStates.TryGetValue((targetPeer, namespaceId), out var ps))
            PeerStates[(targetPeer, namespaceId)] = ps = new PeerState(targetPeer, namespaceId);
        ps.StreamPushed[author] = Math.Max(ps.StreamPushed.TryGetValue(author, out var pushed) ? pushed : 0,
                                           batch.CursorEnd);
        ps.LastPushedSeq = Math.Max(ps.LastPushedSeq, batch.CursorEnd);
    }
    // EncodeBatchPayload / DecodeBatchPayload → Production/Node.BatchCodec.cs

}
