// Node engine: storage, outbox/inbox, alias maps, batches, applyBatch (§13.4),
// relay with alias translation (§13.6), snapshots (§12.18), echo suppression [E2],
// idempotency-before-chain-check [S7].
// Port of binsync/node.py — all SPEC HARDENING / errata notes preserved.

namespace BinSync;

public sealed partial class Node
{
    public static readonly byte[] Zero128 = new byte[16];

    // ---- production hardening (Шаг 2): thread safety -------------------------------
    /// <summary>
    /// Domain sub-gates (production). Acquisition order is fixed to avoid deadlocks:
    /// <c>Storage → Outbox → Meta</c>. Public entry points take only the domains they
    /// need; multi-domain ops nest in that order. Internal helpers assume the caller
    /// already holds the required locks (re-entrant Monitor).
    /// Deadlock prevention (§12.18): transport must NEVER hold gates of two nodes at once.
    /// </summary>
    private readonly object _lockStorage = new();
    private readonly object _lockOutbox = new();
    private readonly object _lockMeta = new();

    /// <summary>
    /// Acquire all domain locks (Storage → Outbox → Meta). Prefer this over the legacy
    /// <see cref="Gate"/> object when taking a point-in-time snapshot.
    /// </summary>
    public AllGateScope EnterAllGates() => new AllGateScope(_lockStorage, _lockOutbox, _lockMeta);

    /// <summary>
    /// Legacy coarse gate object (locks Meta only). Prefer <see cref="EnterAllGates"/> for
    /// full exclusion. Kept so existing <c>lock (node.Gate)</c> still compiles; hosts that
    /// need storage consistency should migrate to <see cref="EnterAllGates"/>.
    /// </summary>
    public object Gate => _lockMeta;


    public byte[] NodeId;
    public int Epoch;
    // BUGFIX (implementation finding): the HLC must start cold (§5.2) — see Python port note.
    public HlcClock Clock;
    // storage: namespaceId -> recordId -> Record
    // BUGFIX (implementation finding): the inner recordId maps MUST use content equality
    // (BytesEq). Test fixtures create a fresh byte[] key on every lookup (Fix.Rid(n) allocates),
    // so the default reference comparer made `node.Storage[ns][rid]` miss even when the
    // record existed. Ns() created BytesEq maps, but ApplyBatch replaced the namespace entry
    // with a default-comparer staging dictionary — reads after any applied batch threw
    // KeyNotFoundException / Assert.Contains failures. The setter below normalizes every
    // assignment to a BytesEq-keyed map; all read sites keep working unchanged.
    private readonly Dictionary<int, Dictionary<RecordId, Record>> _storage = new();

    public Dictionary<int, Dictionary<RecordId, Record>> Storage
    {
        get => _storage;
        set
        {
            _storage.Clear();
            foreach (var (ns, m) in value)
                _storage[ns] = new Dictionary<RecordId, Record>(m);
        }
    }
    // outbox: namespaceId -> list[OutboxEntry] (per-author seq counters inside)
    public Dictionary<int, List<OutboxEntry>> Outbox = new();
    internal readonly Dictionary<(byte[], int), long> _outboxSeq = new(BytesIntEq.Instance);   // (author, ns) -> seq
    // inbox idempotency: set of (issuerNodeId, ns, issuerEpoch, batchId)
    public readonly HashSet<(byte[], int, int, byte[])> Inbox = new(BytesIntIntBytesEqB.Instance);
    // batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
    public readonly Dictionary<(byte[], int, int), byte[]> LastBatchDigest = new(BytesIntIntEqB.Instance);
    // BUGFIX: per-STREAM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
    // -> last sent batchDigest (§12.17.2 hardening).
    public readonly Dictionary<((byte[], int, int), byte[]), byte[]> LastStreamDigest = new(BytesIntIntBytesKeyEq.Instance);
    // alias maps per session peer: peerNodeId -> {alias -> (nodeId, epoch)} and reverse
    public Dictionary<NodeId, Dictionary<int, (NodeId Id, int Epoch)>> AliasToNode = new();
    public Dictionary<NodeId, Dictionary<(NodeId Id, int Epoch), int>> NodeToAlias = new();   // peer → (durableNodeId, epoch) → alias
    // Errata E-9 (transport): DURABLE author knowledge — every (nodeId, epoch) ever
    // resolved through ANY ALIAS_MAP. Used ONLY for session-table construction, never
    // for resolution [А3].
    public Dictionary<NodeId, int> DurableAuthors = new();   // nodeId -> max known epoch
    public Dictionary<(byte[], int), PeerState> PeerStates = new(BytesIntEq.Instance);
    // Errata E-10 (scenario 23): provenance seen-set — (senderPeer, ns, changeId)
    // (E-13 follow-up 2: channel-scoped). Relay eligibility decided by FIRST sighting.
    public readonly HashSet<(byte[], int, byte[])> SeenChanges = new(SeenKeyComparer.Instance);
    // Errata E-11: per-(senderPeer, ns) cache of the durable author stream last accepted.
    public readonly Dictionary<(byte[], int), byte[]> LastStreamAuthor = new(BytesIntEq.Instance);
    // known_epochs: mixed keys — (peer, ns) session watermarks, (author, -1) durable
    // author epochs, (peer, 0) HELLO epoch. See register_session_epoch / learn_author_epoch.
    public Dictionary<(byte[], int), int> KnownEpochs = new(BytesIntEq.Instance);
    public readonly HashSet<NodeId> Quarantined = new();       // zombie nodes requiring resync
    public List<string> ErrorsOut = new();            // observable error log for tests
    public List<byte[]> AcksOut = new();              // ACKed batchIds
    public readonly HashSet<int> ForbiddenNamespaces = new();
    public Dictionary<int, int> SchemaVersions = new();  // namespaceId -> supported schemaVersion
    // Harness handle to peer Node objects (live connection handles in a real stack).
    public Dictionary<NodeId, Node>? PeersByNid = null;

    public Node(byte[]? nodeId = null, int epoch = 1, Func<long>? nowFn = null,
                long maxSkewUs = 60_000_000)
    {
        NodeId = nodeId ?? Guid.NewGuid().ToByteArray();
        if (NodeId.Length != 16) throw new InvalidOperationException("nodeId must be 16 bytes");
        Epoch = epoch;
        Clock = new HlcClock(nowFn, maxSkewUs);
    }

    // Alias / session epoch / durable id → Production/Node.Alias.cs (partial)

    /// §10.1: `seq` is monotonic per (authorNodeId, namespaceId).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>NextOutboxSeqInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public long NextOutboxSeq(byte[] author, int namespaceId)
    {
        lock (_lockOutbox) return NextOutboxSeqInternal(author, namespaceId);
    }

internal long NextOutboxSeqInternal(byte[] author, int namespaceId)
    {
        var key = (author, namespaceId);
        long s = (_outboxSeq.TryGetValue(key, out var v) ? v : 0) + 1;
        _outboxSeq[key] = s;
        return s;
    }

    /// Current durable (wall, logical, author, value|"TOMB") of one field, straight from storage.
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>SeenStateInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public (ulong Wall, uint Logical, byte[] Author, object Val)? SeenState(int ns, byte[] recordId, int fieldId)
    {
        lock (_lockStorage) return SeenStateInternal(ns, recordId, fieldId);
    }

internal (ulong Wall, uint Logical, byte[] Author, object Val)? SeenStateInternal(int ns, byte[] recordId, int fieldId)
    {
        if (!Storage.TryGetValue(ns, out var store)) return null;
        if (!store.TryGetValue(recordId, out var rec)) return null;
        FieldState? fs = rec.State.TryGetValue((ushort)fieldId, out var w) ? w :
                         (rec.FieldTombs.TryGetValue((ushort)fieldId, out var t) ? t : null);
        if (fs is null) return null;
        bool tomb = rec.FieldTombs.ContainsKey((ushort)fieldId);
        return (fs.Timestamp.Wall, fs.Timestamp.Logical, fs.AuthorNodeId, tomb ? "TOMB" : fs.Value);
    }

    /// Append a LOCAL write to outbox ONLY if merge reported changed (echo [E2]).
    /// Errata E-13 (scenario 37): restored-echo suppression — see Python port docstring.
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>PushOutboxInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public OutboxEntry? PushOutbox(int ns, byte[] recordId, object op, byte[] author, byte[]? peer = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return PushOutboxInternal(ns, recordId, op, author, peer);
    }

internal OutboxEntry? PushOutboxInternal(int ns, byte[] recordId, object op, byte[] author, byte[]? peer = null)
    {
        if (!author.AsSpan().SequenceEqual(NodeId))
            throw new InvalidOperationException("local push only; relay path enqueues raw payloads");
        // Errata E-13 follow-up 3: suppress ONLY when OUR OWN queued rival carried
        // byte-identical durable state to what this write re-established [Ш2].
        ushort? fid = op switch { OpFieldWrite fw => fw.FieldId, OpFieldTomb ft => ft.FieldId, _ => (ushort?)null };
        if (fid is not null && op is not OpRecordTombstone)
        {
            var st = SeenState(ns, recordId, fid.Value);
            if (st is not null)
            {
                var (wall, logical, sAuthor, val) = st.Value;
                if (sAuthor.AsSpan().SequenceEqual(NodeId))
                {
                    bool sameState = op switch
                    {
                        OpFieldWrite fw => val is byte[] vb && vb.AsSpan().SequenceEqual(fw.Value)
                                           && wall == fw.Wall && logical == fw.Logical,
                        OpFieldTomb ft => (val is string sv) && sv == "TOMB"
                                          && wall == ft.Wall && logical == ft.Logical,
                        _ => false,
                    };
                    if (sameState)
                    {
                        var list = Outbox.TryGetValue(ns, out var l) ? l : new List<OutboxEntry>();
                        for (int i = list.Count - 1; i >= 0; i--)
                        {
                            var q = list[i];
                            if (!q.RecordId.AsSpan().SequenceEqual(recordId) ||
                                !q.AuthorNodeId.AsSpan().SequenceEqual(NodeId)) continue;
                            object qop;
                            try { qop = Ops.DecodeOp(q.Payload).Op; }
                            catch { break; }
                            ushort? qfid = qop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                            if (qfid != fid) continue;
                            bool qSame = TagOf(qop) == TagOf(op) && WallOf(qop) == WallOf(op) &&
                                         LogicalOf(qop) == LogicalOf(op) &&
                                         ValueOf(qop).AsSpan().SequenceEqual(ValueOf(op));
                            if (qSame) return null;   // E-13: restored echo of a queued winner
                            break;                    // newest own queued op differs → push
                        }
                    }
                }
            }
        }
        SetOrigin(op, 0);   // neutral; CreateDeltaBatch rewrites it per target session
        var payload = Ops.EncodeOp(op, fullAuthor: false);
        var entry = new OutboxEntry
        {
            NamespaceId = ns,
            Seq = NextOutboxSeq(author, ns),
            ChangeId = Guid.NewGuid().ToByteArray(),
            RecordId = recordId,
            OpKind = TagOf(op),
            Payload = payload,
            AuthorNodeId = author,
            CreatedAt = (long)Clock.SendTick().Wall,
        };
        if (!Outbox.TryGetValue(ns, out var ol)) Outbox[ns] = ol = new();
        ol.Add(entry);
        // Errata E-13 follow-up: register our own changeId under our OWN channel key so
        // apply_batch's relay rule treats a direct echo as already-seen (see Python port).
        SeenChanges.Add((NodeId, ns, entry.ChangeId));
        return entry;
    }

    private static byte TagOf(object op) => op switch
    { OpFieldWrite => OpsConst.TagFieldWrite, OpFieldTomb => OpsConst.TagFieldTomb, OpRecordTombstone => OpsConst.TagRecordTomb, _ => throw new ArgumentException() };
    private static ulong WallOf(object op) => op switch
    { OpFieldWrite x => x.Wall, OpFieldTomb x => x.Wall, OpRecordTombstone x => x.Wall, _ => 0 };
    private static uint LogicalOf(object op) => op switch
    { OpFieldWrite x => x.Logical, OpFieldTomb x => x.Logical, OpRecordTombstone x => x.Logical, _ => 0 };
    private static byte[] ValueOf(object op) => op is OpFieldWrite fw ? fw.Value : Array.Empty<byte>();
    private static void SetOrigin(object op, ushort v)
    {
        switch (op)
        {
            case OpFieldWrite x: x.Origin = v; break;
            case OpFieldTomb x: x.Origin = v; break;
            case OpRecordTombstone x: x.Origin = v; break;
        }
    }

    /// Local FieldWrite. Returns true if state changed (and outbox grew).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>WriteFieldInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool WriteField(int namespaceId, byte[] recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return WriteFieldInternal(namespaceId, recordId, fieldId, value, hlc);
    }

internal bool WriteFieldInternal(int namespaceId, byte[] recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(value, h, NodeId);
        // BUGFIX (implementation finding): `origin` is overwritten per target session in
        // CreateDeltaBatch (§13.6.2 [R4]); neutral slot here, like TombField/DeleteRecord.
        var op = new OpFieldWrite { FieldId = (ushort)fieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 0, AuthorNodeId = NodeId, Value = value };
        bool changed = Merge.ApplyFieldWrite(rec, (ushort)fieldId, fs);
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>TombFieldInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool TombField(int namespaceId, byte[] recordId, int fieldId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return TombFieldInternal(namespaceId, recordId, fieldId, hlc);
    }

internal bool TombFieldInternal(int namespaceId, byte[] recordId, int fieldId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(Array.Empty<byte>(), h, NodeId);
        var op = new OpFieldTomb { FieldId = (ushort)fieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId };
        bool changed = Merge.ApplyFieldTomb(rec, (ushort)fieldId, fs);
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    /// Lifecycle → dead (delete-wins).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>DeleteRecordInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool DeleteRecord(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return DeleteRecordInternal(namespaceId, recordId, hlc);
    }

internal bool DeleteRecordInternal(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Dead);
        var op = new OpRecordTombstone { RecordId = recordId, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId };
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    /// Undelete only via explicit lifecycle write value=0x00 (§7.4).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>UndeleteRecordInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool UndeleteRecord(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return UndeleteRecordInternal(namespaceId, recordId, hlc);
    }

internal bool UndeleteRecordInternal(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Alive);
        // encode as FieldWrite on reserved field 0x0000 with value 0x00
        var op = new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId, Value = new[] { Model.Alive } };
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        else
            // SPEC HARDENING: propagate the no-op revive anyway ([П7]/anti-entropy; scenario 38 step 5).
            PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    // --------------------------------------------------------------------- batching
    /// BUGFIX (scenario 23): a relay must stamp ops of a THIRD-PARTY author with that
    /// author's own epoch, not the relay's self.epoch (see Python port docstring).
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AuthorEpochInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int AuthorEpoch(byte[] author)
    {
        lock (_lockMeta) return AuthorEpochInternal(author);
    }

internal int AuthorEpochInternal(byte[] author)
    {
        foreach (var m in AliasToNode.Values)
            foreach (var (nid, ep) in m.Values)
                if (nid == (NodeId)author) return ep;
        if (author.AsSpan().SequenceEqual(NodeId)) return Epoch;
        return KnownEpochs.TryGetValue((author, -1), out var ke) ? ke : Epoch;
    }

    /// Build DELTA_BATCH from outbox beyond peer's per-stream watermark.
    /// §13.6.1 [Р3]: NO coalescing across authors — one author per batch.
    /// §13.6.2 [R4]: translate each op's `origin` into the alias agreed for THIS session.
    /// SPEC HARDENING: unknown author ⇒ deterministic UNKNOWN_ALIAS rejection (never a
    /// silent skip that would hole the seq stream). Full rationale in the Python port.
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>CreateDeltaBatchInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Batch? CreateDeltaBatch(byte[] targetPeer, int namespaceId,
                                   byte[]? filterAuthorNodeId = null,
                                   int schemaId = 0, int schemaVersion = 1)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return CreateDeltaBatchInternal(targetPeer, namespaceId, filterAuthorNodeId, schemaId, schemaVersion);
    }

internal Batch? CreateDeltaBatchInternal(byte[] targetPeer, int namespaceId,
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
            int? alias = AliasOf(targetPeer, e.AuthorNodeId, Epoch);
            if (alias is null && e.AuthorNodeId.AsSpan().SequenceEqual(NodeId))
                alias = 1;   // BUGFIX: self-entry fallback keeps [А1] intact
            if (alias is null)
            {
                // Errata E-11 (scenario 23): try incremental ALIAS_MAP negotiation before failing closed.
                // NOTE: a resumed node (NodePersistence.Load) has registry KEYS but no live Node
                // handles; TryGetValue then yields a null value — treat it as "peer not attached".
                var peerNode = PeersByNid is not null && PeersByNid.TryGetValue(targetPeer, out var pn) ? pn : null;
                if (peerNode is not null && !e.AuthorNodeId.AsSpan().SequenceEqual(NodeId))
                {
                    int epTry = KnownAuthorEpoch(e.AuthorNodeId) ?? Epoch;
                    alias = EnsureAlias(peerNode, e.AuthorNodeId, epTry);
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
                NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = e.ChangeId,
                RecordId = e.RecordId, OpKind = e.OpKind, Payload = newPayload,
                AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt,
            });
        }
        if (translated.Count == 0) return null;
        var recordsRoot = ComputeRecordsRoot(translated);
        // BUGFIX (scenario 23): stamp with the epoch LEARNED for the author in the target
        // session (ALIAS_MAP entry / per-session watermark), not self.epoch.
        // BUGFIX (errata E-6 follow-up): stream key is `stream`, not a stale loop variable.
        int ep = Epoch;
        byte[] author = stream;
        if (!author.AsSpan().SequenceEqual(NodeId))
        {
            var m = AliasToNode.TryGetValue(targetPeer, out var am) ? am : new Dictionary<int, (NodeId Id, int Epoch)>();
            bool hit = false;
            foreach (var (alias_, (nid2, ep2)) in m)
                if (nid2 == (NodeId)author) { ep = ep2; hit = true; break; }
            if (!hit)
                foreach (var kv in KnownEpochs)
                    if (kv.Key.Item2 >= 0 && kv.Key.Item1.AsSpan().SequenceEqual(targetPeer))
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
    public Batch SignBatchFor(Batch batch, byte[] targetPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return SignBatchForInternal(batch, targetPeer);
    }

internal Batch SignBatchForInternal(Batch batch, byte[] targetPeer)
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
    public void AdvanceStream(byte[] targetPeer, int namespaceId, Batch batch)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) AdvanceStreamInternal(targetPeer, namespaceId, batch);
    }

internal void AdvanceStreamInternal(byte[] targetPeer, int namespaceId, Batch batch)
    {
        LastStreamDigest[((NodeId, namespaceId, batch.IssuerEpoch), targetPeer)] = batch.BatchDigest;
    }
    // EncodeBatchPayload / DecodeBatchPayload → Production/Node.BatchCodec.cs

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
        byte[] streamAuthor;
        try
        {
            object? op0 = batch.Entries.Count > 0 ? Ops.DecodeOp(batch.Entries[0].Payload).Op : null;
            streamAuthor = op0 is not null ? ResolveAuthor(op0) : batch.IssuerNodeId;
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
        // STEP 4: atomic application [T1] — all-or-nothing on a copy-on-write staging
        var nsStore = Ns(batch.NamespaceId);
        var staged = new Dictionary<RecordId, Record>();
        foreach (var (rid, rec) in nsStore) staged[rid.ToArray()] = Clone(rec);
        int applied = 0;
        var changedIdx = new List<int>();   // indices of entries that actually changed state
        for (int idx = 0; idx < batch.Entries.Count; idx++)
        {
            var e = batch.Entries[idx];
            var op = Ops.DecodeOp(e.Payload).Op;
            var opAuthor = ResolveAuthor(op);   // [А2]; unknown → raise
            // clock skew clamp on incoming HLC wall (§5.3)
            long now = Clock.Now();
            if ((long)WallOf(op) > now + Clock.MaxSkewUs)
                throw new SyncException("CLOCK_DRIFT", batch.BatchId);
            byte[] recId = op is OpRecordTombstone rt ? rt.RecordId : e.RecordId;
            if (!staged.TryGetValue(recId, out var rec)) staged[recId] = rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
            bool changed = ApplyOp(rec, op, opAuthor);
            if (changed) { applied++; changedIdx.Add(idx); }
        }
        // commit atomically (rollback path: exceptions above leave storage untouched [Т1])
        Storage[batch.NamespaceId] = staged;
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
            var batchOps = batch.Ops();
            for (int idx = 0; idx < batch.Entries.Count; idx++)
            {
                var e = batch.Entries[idx];
                byte[] cid = e.ChangeId.AsSpan().SequenceEqual(Zero128) ? Guid.NewGuid().ToByteArray() : e.ChangeId;
                var ck = (senderPeer, batch.NamespaceId, cid);
                bool known = SeenChanges.Contains(ck);
                SeenChanges.Add(ck);
                if (!known && !changedIdx.Contains(idx))
                {
                    bool relayOk = true;
                    var rop = batchOps[idx];
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
                    if (relayOk) changedIdx.Add(idx);     // new provenance ⇒ relay even as loser
                }
            }
            foreach (var idx in changedIdx)
            {
                var e = batch.Entries[idx];
                var op = batchOps[idx];
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
                            EnsureAlias(otherPeer, resolved.ToArray(), epm.Value);
                    }
                    var senderNode = PeersByNid is not null && PeersByNid.TryGetValue(senderPeer, out var sn) ? sn : null;
                    if (senderNode is not null && !(resolved == (NodeId)NodeId))
                        EnsureAlias(senderNode, resolved.ToArray(), epm.Value);
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

    // ---------------------------------------------------------------------- helpers
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>RecordDigestOfInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public byte[]? RecordDigestOf(int namespaceId, byte[] recordId)
    {
        lock (_lockStorage) return RecordDigestOfInternal(namespaceId, recordId);
    }

internal byte[]? RecordDigestOfInternal(int namespaceId, byte[] recordId)
    { return Storage.TryGetValue(namespaceId, out var s) && s.TryGetValue(recordId, out var rec) ? Digests.RecordDigest(rec) : null; }

/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AllRecordDigestsInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Dictionary<byte[], byte[]> AllRecordDigests(int namespaceId)
    {
        lock (_lockStorage) return AllRecordDigestsInternal(namespaceId);
    }

internal Dictionary<byte[], byte[]> AllRecordDigestsInternal(int namespaceId)
    {
        var d = new Dictionary<byte[], byte[]>(BytesEq.Instance);
        if (Storage.TryGetValue(namespaceId, out var s))
            foreach (var (rid, rec) in s) d[rid.ToArray()] = Digests.RecordDigest(rec);
        return d;
    }

/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>OutboxSizeInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int OutboxSize(int namespaceId)
    {
        lock (_lockOutbox) return OutboxSizeInternal(namespaceId);
    }

internal int OutboxSizeInternal(int namespaceId)
    { return Outbox.TryGetValue(namespaceId, out var l) ? l.Count : 0; }

    internal static Record Clone(Record rec)
    {
        var r = new Record(rec.RecordId, rec.NamespaceId)
        {
            State = new(rec.State),
            LifecycleHlc = rec.LifecycleHlc,
            LifecycleAuthor = rec.LifecycleAuthor,
            LifecycleValue = rec.LifecycleValue,
            FieldTombs = new(rec.FieldTombs),
        };
        return r;
    }

    /// Canonical records array: count varint, then per entry:
    /// recordId b128 | opKind u8 | payloadLen varint | payload.
    /// <summary>Write canonical records array into an existing Writer (no intermediate buffer).</summary>
    public static void WriteRecordsArray(Writer w, List<OutboxEntry> entries)
    {
        w.Varint((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            w.B128(e.RecordId).U8(e.OpKind).Varint((ulong)e.Payload.Length).Raw(e.Payload);
        }
    }

    public static byte[] EncodeRecordsArray(List<OutboxEntry> entries)
    {
        int est = 8;
        for (int i = 0; i < entries.Count; i++)
            est += 24 + entries[i].Payload.Length;
        var w = new Writer(est);
        WriteRecordsArray(w, entries);
        return w.Value();
    }

    public static List<(byte[] Rid, byte Kind, byte[] Payload)> DecodeRecordsArray(byte[] data)
    {
        var r = new Reader(data);
        int n = (int)r.Varint();
        var outp = new List<(byte[], byte, byte[])>();
        for (int i = 0; i < n; i++)
        {
            byte[] rid = r.B128();
            byte kind = r.U8();
            byte[] payload = r.Bytes();
            outp.Add((rid, kind, payload));
        }
        return outp;
    }

    /// §12.17.2 batchDigest = BLAKE3-128(domainTag || batchId || namespaceId || ...)
    public static byte[] ComputeBatchDigest(byte[] batchId, int namespaceId, byte[] issuerNode,
        int issuerEpoch, long cursorStart, long cursorEnd, byte[] recordsRoot, byte[] prevBatchDigest)
    {
        var w = new Writer();
        w.Raw(Domains.DomainBatch).B128(batchId).U16((ushort)namespaceId).B128(issuerNode);
        w.U32((uint)issuerEpoch).U64((ulong)cursorStart).U64((ulong)cursorEnd);
        w.B128(recordsRoot).B128(prevBatchDigest);
        return Digests.Blake3(w.Value(), 16);
    }

    /// recordsRoot = BLAKE3-128 over the canonical records array.
    public static byte[] ComputeRecordsRoot(List<OutboxEntry> entries) =>
        Digests.Blake3(Domains.Cat(Domains.DomainRecords, EncodeRecordsArray(entries)), 16);
}

public sealed class BatchResult
{
    public byte[] BatchId;
    public int Applied = 0;
    public bool Acked = false;
    public bool Duplicate = false;
    public BatchResult(byte[] batchId) { BatchId = batchId; }
}
