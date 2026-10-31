// ApplyBatch / ApplyOp / ProcessAck / Snapshot. Behavior unchanged.

namespace BinSync;

public sealed partial class Node
{
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ApplyBatchInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public BatchResult ApplyBatch(Batch batch, byte[] senderPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return ApplyBatchInternal(batch, senderPeer);
    }

internal BatchResult ApplyBatchInternal(Batch batch, byte[] senderPeer)
    {
        var result = new BatchResult(batch.BatchId);
        // STEP 0: namespace isolation (§3.7) — forbidden ns kills only that stream
        if (ForbiddenNamespaces.Contains(batch.NamespaceId))
            throw new SyncException("NAMESPACE_FORBIDDEN", batch.BatchId);
        // STEP 0.5: epoch regression (§4.4) — SPEC CORRECTIONS (errata E-2/E-3):
        // the watermark is keyed by the RECEIVING SESSION ((sender_peer, ns)) and compares
        // against the sender's CURRENT session incarnation epoch learned from HELLO/ALIAS_MAP.
        var chain = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch);
        var sesKey = (senderPeer, batch.NamespaceId);
        var curKnown = SessionEpoch(senderPeer, batch.NamespaceId);
        if (curKnown is not null && batch.IssuerEpoch < curKnown.Value)
            throw new SyncException("EPOCH_REGRESSION", batch.BatchId);
        // STEP 0.6: schema pending (§18.3 / [СХ7]) — do NOT advance lastReceivedSeq
        int supported = SchemaVersions.TryGetValue(batch.NamespaceId, out var sv) ? sv : 1;
        if (batch.SchemaVersion > supported)
            throw new SyncException("SCHEMA_PENDING", batch.BatchId);
        // STEP 1: idempotency FIRST ([S7], [П7], §10.2.2 lost-ACK replay)
        var key = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch, batch.BatchId);
        if (Inbox.Contains(key))
        {
            AcksOut.Add(batch.BatchId);   // duplicate → no-op but ACK mandatory [П4]
            result.Duplicate = true;
            result.Acked = true;
            return result;
        }
        // STEP 2: cursor check [S2]/[П2] — errata E-3: keyed per SESSION stream.
        var pkey = (batch.IssuerNodeId, batch.NamespaceId);
        if (!PeerStates.TryGetValue(pkey, out var ps)) PeerStates[pkey] = ps = new PeerState(batch.IssuerNodeId, batch.NamespaceId);
        // Errata E-4: resolve the batch's AUTHOR STREAM durably and key the receive cursor
        // per stream — §13.6.1 [Р3] makes seq numbering per-author.
        // BUGFIX (persistence resume): restored ops carry Origin=0 as a stale sentinel but
        // embed the TRUE author in full-author mode (§13.6). Author resolution helper:
        // full-author ops resolve via their embedded nodeId; otherwise fall back to the
        // session alias map, fail-closed on UNKNOWN_ALIAS [А2]/[А3].
        byte[] ResolveAuthor(object op)
        {
            bool full = op switch { OpFieldWrite x => x.FullAuthorMode, OpFieldTomb x => x.FullAuthorMode, OpRecordTombstone x => x.FullAuthorMode, _ => false };
            var emb = GetAuthor(op);
            if (full && emb is not null && emb.Length == 16) return emb;
            try { return ResolveAliasInternal(senderPeer, GetOrigin(op)); }
            catch (SyncException) when (full && emb is not null && emb.Length == 16) { return emb; }
        }
        // Decode ops once — reused for stream author, apply loop, and relay (no batch.Ops()).
        int nEntEarly = batch.Entries.Count;
        var decodedOps = nEntEarly > 0 ? new object[nEntEarly] : Array.Empty<object>();
        for (int di = 0; di < nEntEarly; di++)
            decodedOps[di] = Ops.DecodeOp(batch.Entries[di].Payload).Op;
        byte[] streamAuthor;
        try
        {
            streamAuthor = nEntEarly > 0 ? ResolveAuthor(decodedOps[0]) : batch.IssuerNodeId;
        }
        catch (SyncException) { streamAuthor = batch.IssuerNodeId; }
        batch.AuthorStreamNodeId = streamAuthor;
        LastStreamAuthor[(senderPeer, batch.NamespaceId)] = streamAuthor;   // E-11
        var streamKey = (senderPeer, batch.IssuerEpoch, batch.NamespaceId, streamAuthor);
        long expected = (ps.ReceivedStreams.TryGetValue(streamKey, out var ex) ? ex : 0) + 1;
        if (batch.CursorStart != expected)
            throw new SyncException("SEQ_GAP", batch.BatchId);
        // STEP 3: batch digest chain (§12.17.2, [Т3]).
        // BUGFIX (port fidelity): the reference port checks prevBatchDigest UNCONDITIONALLY
        // after the cursor gate of STEP 2 has already passed. The earlier "streamEmpty"
        // guard was wrong in both directions: an out-of-order batch on a stream that is
        // still EMPTY at the receiver skipped the chain check entirely (b2 delivered before
        // b1 applied cleanly instead of raising), while a legitimate FIRST batch of a fresh
        // per-(issuer,ns,epoch) chain could hit a stale non-zero checkpoint. With STEP 2
        // guaranteeing cursor contiguity, an unconditional check is exactly [Т3].
        {
            byte[] expectPrev = LastBatchDigest.TryGetValue(chain, out var ep2) ? ep2 : Zero128;
            if (!batch.PrevBatchDigest.AsSpan().SequenceEqual(expectPrev))
                throw new SyncException("BATCH_CHAIN_BROKEN", batch.BatchId);
        }
        var root = ComputeRecordsRoot(batch.Entries);
        if (!batch.RecordsRoot.AsSpan().SequenceEqual(root))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        var recomputed = ComputeBatchDigest(batch.BatchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, batch.PrevBatchDigest);
        if (!batch.BatchDigest.AsSpan().SequenceEqual(recomputed))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        // STEP 4: atomic application [T1] — copy-on-write only for touched records.
        // Untouched records stay as shared references in nsStore; overlays discard on throw.
        var nsStore = Ns(batch.NamespaceId);
        var overlays = new Dictionary<RecordId, Record>();
        int applied = 0;
        int nEnt = nEntEarly;
        var changedFlags = nEnt > 0 ? new bool[nEnt] : Array.Empty<bool>();
        // One wall sample for the whole batch (skew window is large vs apply latency).
        long nowWall = Clock.Now();
        long maxSkew = Clock.MaxSkewUs;
        for (int idx = 0; idx < nEnt; idx++)
        {
            var e = batch.Entries[idx];
            var op = decodedOps[idx];
            var opAuthor = ResolveAuthor(op);   // [А2]; unknown → raise
            // clock skew clamp on incoming HLC wall (§5.3)
            if ((long)WallOf(op) > nowWall + maxSkew)
                throw new SyncException("CLOCK_DRIFT", batch.BatchId);
            byte[] recId = op is OpRecordTombstone rt ? rt.RecordId : e.RecordId;
            if (!overlays.TryGetValue(recId, out var rec))
            {
                if (nsStore.TryGetValue(recId, out var existing))
                    rec = Clone(existing);
                else
                    rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
                overlays[recId] = rec;
            }
            bool changed = ApplyOp(rec, op, opAuthor);
            if (changed) { applied++; changedFlags[idx] = true; }
        }
        // commit atomically (rollback path: exceptions above leave storage untouched [Т1])
        foreach (var (rid, rec) in overlays)
            nsStore[rid] = rec;
        Inbox.Add(key);
        ps.LastReceivedSeq = batch.CursorEnd;   // [П3]/[Т2] atomic cursor jump (compat copy)
        ps.ReceivedStreams[streamKey] = batch.CursorEnd;   // errata E-3 per-session cursor
        LastBatchDigest[chain] = batch.BatchDigest;
        // SPEC CORRECTION (errata E-2): track epochs PER SESSION (sender_peer, ns); the
        // commit path only ever RAISES the watermark for batches that pass every gate.
        int knownS = KnownEpochs.TryGetValue(sesKey, out var ks) ? ks : 0;
        KnownEpochs[sesKey] = Math.Max(knownS, batch.IssuerEpoch);
        AcksOut.Add(batch.BatchId);
        result.Applied = applied;
        result.Acked = true;
        // Relay [Р1, Р2]: ONLY entries that actually changed state are appended to OUR
        // outbox under the ORIGINAL authorNodeId (SPEC HARDENING §8.3 [E2] + Errata E-10:
        // provenance-based relay eligibility — FULL rationale in the Python port).
        if (!batch.IssuerNodeId.AsSpan().SequenceEqual(NodeId))
        {
            for (int idx = 0; idx < nEnt; idx++)
            {
                var e = batch.Entries[idx];
                byte[] cid = e.ChangeId.AsSpan().SequenceEqual(Zero128) ? Guid.NewGuid().ToByteArray() : e.ChangeId;
                var ck = (senderPeer, batch.NamespaceId, cid);
                bool known = SeenChanges.Contains(ck);
                SeenChanges.Add(ck);
                if (!known && !changedFlags[idx])
                {
                    bool relayOk = true;
                    var rop = decodedOps[idx];
                    ushort? rfid = rop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                    if (rfid is not null)
                    {
                        byte[]? rAuthor = null;
                        try { rAuthor = ResolveAuthor(rop); }
                        catch (SyncException) { }
                        if (rAuthor is not null && rAuthor.AsSpan().SequenceEqual(NodeId) &&
                            SeenState(batch.NamespaceId, e.RecordId, rfid.Value) is not null &&
                            OwnQueueHolds(batch.NamespaceId, e.RecordId, rop))
                            relayOk = false;   // own durable state echoed back [E2]/[Ш2]
                    }
                    if (relayOk) changedFlags[idx] = true; // new provenance ⇒ relay even as loser
                }
            }
            for (int idx = 0; idx < nEnt; idx++)
            {
                if (!changedFlags[idx]) continue;
                var e = batch.Entries[idx];
                var op = decodedOps[idx];
                if (GetAuthor(op) is not null) continue;   // full-author mode keeps durable author
                var resolved = ResolveAuthor(op);
                // BUGFIX (scenario 23): the relay keeps the op payload VERBATIM (§13.5 [Р1]/[И4]).
                var entry = new OutboxEntry
                {
                    NamespaceId = e.NamespaceId != 0 ? e.NamespaceId : batch.NamespaceId,
                    Seq = NextOutboxSeq(resolved, batch.NamespaceId),
                    ChangeId = e.ChangeId.AsSpan().SequenceEqual(Zero128) ? Guid.NewGuid().ToByteArray() : e.ChangeId,
                    RecordId = e.RecordId,
                    OpKind = e.OpKind,
                    Payload = e.Payload,
                    AuthorNodeId = resolved,
                    CreatedAt = (long)WallOf(op),   // BUGFIX: stamp the OP's own HLC wall time
                };
                if (!Outbox.TryGetValue(batch.NamespaceId, out var ol)) Outbox[batch.NamespaceId] = ol = new();
                ol.Add(entry);
                // Errata E-10/E-11: propagate the third-party binding learned through THIS
                // session to our OTHER live sessions AND mirror it into the sender's copy
                // (incremental ALIAS_MAP update, §4.3/§12.7). FULL comment in Python port.
                int? epm = null;
                var smap = AliasToNode.TryGetValue(senderPeer, out var ss) ? ss : new Dictionary<int, (NodeId Id, int Epoch)>();
                foreach (var (_al, (n, epv)) in smap)
                    if (n == (NodeId)resolved) { epm = epv; break; }
                if (epm is not null)
                {
                    foreach (var otherPeerKv in AliasToNode.Keys.ToList())
                    {
                        var otherPeer = PeersByNid is not null && PeersByNid.TryGetValue(otherPeerKv, out var on) ? on : null;
                        if (otherPeer is not null && !(otherPeerKv == (NodeId)senderPeer))
                            EnsureAlias(otherPeer, resolved, epm.Value);
                    }
                    var senderNode = PeersByNid is not null && PeersByNid.TryGetValue(senderPeer, out var sn) ? sn : null;
                    if (senderNode is not null && !(resolved == (NodeId)NodeId))
                        EnsureAlias(senderNode, resolved, epm.Value);
                }
            }
        }
        return result;
    }

    /// Errata E-13 follow-up 4 (scenario 37): does OUR OWN un-ACKed queue already carry a
    /// byte-equal winner for (recordId, fieldId)? If yes, the ring merely mirrors our own
    /// state back — enqueueing again would duplicate propagation [И2].
    private bool OwnQueueHolds(int ns, byte[] rid, object qop)
    {
        var list = Outbox.TryGetValue(ns, out var l) ? l : new List<OutboxEntry>();
        foreach (var q in list)
        {
            if (!q.RecordId.AsSpan().SequenceEqual(rid) || !q.AuthorNodeId.AsSpan().SequenceEqual(NodeId)) continue;
            object sOp;
            try { sOp = Ops.DecodeOp(q.Payload).Op; } catch { continue; }
            ushort? sfid = sOp switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
            ushort? qfid = qop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
            if (sfid != qfid) continue;
            if (TagOf(sOp) == TagOf(qop) && WallOf(sOp) == WallOf(qop) &&
                LogicalOf(sOp) == LogicalOf(qop) &&
                ValueOf(sOp).AsSpan().SequenceEqual(ValueOf(qop)))
                return true;
        }
        return false;
    }

    private static ushort GetOrigin(object op) => op switch
    { OpFieldWrite x => x.Origin, OpFieldTomb x => x.Origin, OpRecordTombstone x => x.Origin, _ => (ushort)0 };
    private static byte[]? GetAuthor(object op) => op switch
    { OpFieldWrite x => x.AuthorNodeId, OpFieldTomb x => x.AuthorNodeId, OpRecordTombstone x => x.AuthorNodeId, _ => null };

    /// Apply one resolved op to a staged record. Echo suppression happens inside
    /// MergeField: identical (hlc, author, value) → changed=False → nothing queued.
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ApplyOpInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool ApplyOp(Record rec, object op, byte[] author)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return ApplyOpInternal(rec, op, author);
    }

internal bool ApplyOpInternal(Record rec, object op, byte[] author)
    {
        if (op is OpFieldWrite fw)
        {
            if (fw.FieldId == Model.LifecycleFieldId)
            {
                byte val = fw.Value.Length > 0 ? fw.Value[0] : Model.Alive;
                return Merge.ApplyLifecycle(rec, new Hlc(fw.Wall, fw.Logical), author, val);
            }
            var fs = new FieldState(fw.Value, new Hlc(fw.Wall, fw.Logical), author, fw.KeyEpoch);
            return Merge.ApplyFieldWrite(rec, fw.FieldId, fs);
        }
        if (op is OpFieldTomb ft)
        {
            var fs = new FieldState(Array.Empty<byte>(), new Hlc(ft.Wall, ft.Logical), author, ft.KeyEpoch);
            return Merge.ApplyFieldTomb(rec, ft.FieldId, fs);
        }
        if (op is OpRecordTombstone rt)
            return Merge.ApplyLifecycle(rec, new Hlc(rt.Wall, rt.Logical), author, Model.Dead);
        throw new SyncException("INTERNAL");
    }

    /// Receiver ACK advances our push watermark; GC of outbox up to min acked.
    /// SPEC CORRECTION (errata E-4): the ACK names the acknowledged AUTHOR STREAM.
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ProcessAckInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void ProcessAck(byte[] peerNode, int namespaceId, long ackedSeq, byte[]? authorStream = null)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) ProcessAckInternal(peerNode, namespaceId, ackedSeq, authorStream);
    }

internal void ProcessAckInternal(byte[] peerNode, int namespaceId, long ackedSeq, byte[]? authorStream = null)
    {
        if (!PeerStates.TryGetValue((peerNode, namespaceId), out var ps)) return;
        if (authorStream is not null)
            ps.StreamAcked[authorStream] = Math.Max(ps.StreamAcked.TryGetValue(authorStream, out var cur) ? cur : 0, ackedSeq);
        else
        {
            var entries = Outbox.TryGetValue(namespaceId, out var es) ? es : new List<OutboxEntry>();
            var heads = new Dictionary<byte[], long>(BytesEq.Instance);
            foreach (var e in entries)
                if (!heads.TryGetValue(e.AuthorNodeId, out var cur) || e.Seq < cur)
                    heads[e.AuthorNodeId] = e.Seq;
            foreach (var (author, firstSeq) in heads)
                if (firstSeq <= ackedSeq)
                    ps.StreamAcked[author] = Math.Max(ps.StreamAcked.TryGetValue(author, out var c2) ? c2 : 0, ackedSeq);
        }
        // legacy mirror: monotone scalar = min over known streams' watermarks.
        if (ps.StreamAcked.Count > 0)
            ps.LastAckedSeq = ps.StreamAcked.Values.Min();
    }

    /// Snapshot = all live records + tombstones + ghosts (§ summary 'снапшот').
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>BuildSnapshotInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public List<Batch> BuildSnapshot(int namespaceId)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return BuildSnapshotInternal(namespaceId);
    }

internal List<Batch> BuildSnapshotInternal(int namespaceId)
    {
        var batches = new List<Batch>();
        var nsStore = Ns(namespaceId);
        foreach (var (rid, rec) in nsStore.OrderBy(kv => kv.Key.ToString()))
        {
            var entries = new List<OutboxEntry>();
            foreach (var (fid, fs) in rec.State.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldWrite { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId, Value = fs.Value };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid.ToArray(), OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
            }
            foreach (var (fid, fs) in rec.FieldTombs.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldTomb { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid.ToArray(), OpKind = OpsConst.TagFieldTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
            }
            if (rec.LifecycleHlc is not null)
            {
                var lcAuthor = rec.LifecycleAuthor ?? NodeId;
                var op = new OpRecordTombstone { RecordId = rid.ToArray(), Wall = rec.LifecycleHlc.Wall, Logical = rec.LifecycleHlc.Logical, Origin = 1, AuthorNodeId = lcAuthor };
                if (rec.LifecycleValue == Model.Alive)
                {
                    // SPEC HARDENING: snapshots must carry an explicit REVIVE lifecycle op
                    // so the receiver un-ghosts historical fields (§7.4 У4).
                    var opw = new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = rec.LifecycleHlc.Wall, Logical = rec.LifecycleHlc.Logical, Origin = 1, AuthorNodeId = lcAuthor, Value = new[] { Model.Alive } };
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid.ToArray(), OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(opw), AuthorNodeId = opw.AuthorNodeId!, CreatedAt = 0 });
                }
                else
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid.ToArray(), OpKind = OpsConst.TagRecordTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = op.AuthorNodeId!, CreatedAt = 0 });
            }
            if (entries.Count > 0)
                batches.Add(new Batch(namespaceId, 0, 1, 0x02, 0, NodeId, Epoch, 0, 0,
                                      Guid.NewGuid().ToByteArray(), Zero128,
                                      ComputeRecordsRoot(entries), Zero128, entries));
        }
        foreach (var b in batches)
            b.BatchDigest = ComputeBatchDigest(b.BatchId, b.NamespaceId, b.IssuerNodeId,
                b.IssuerEpoch, b.CursorStart, b.CursorEnd, b.RecordsRoot, b.PrevBatchDigest);
        return batches;
    }

    /// Apply snapshot ops directly (not through cursor machinery).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ApplySnapshotInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int ApplySnapshot(Batch batch, byte[] senderPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return ApplySnapshotInternal(batch, senderPeer);
    }

internal int ApplySnapshotInternal(Batch batch, byte[] senderPeer)
    {
        int applied = 0;
        foreach (var e in batch.Entries)
        {
            var op = Ops.DecodeOp(e.Payload).Op;
            var author = GetAuthor(op)!;
            var nsStore = Ns(batch.NamespaceId);
            if (!nsStore.TryGetValue(e.RecordId, out var rec)) nsStore[e.RecordId] = rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
            if (ApplyOp(rec, op, author)) applied++;
        }
        return applied;
    }

}
