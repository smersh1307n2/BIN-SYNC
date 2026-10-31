// Outbox push + local field/record writes (echo [E2], E-13). Behavior unchanged.

namespace BinSync;

public sealed partial class Node
{
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
    public (ulong Wall, uint Logical, byte[] Author, object Val)? SeenState(int ns, RecordId recordId, int fieldId)
    {
        lock (_lockStorage) return SeenStateInternal(ns, recordId, fieldId);
    }

    internal (ulong Wall, uint Logical, byte[] Author, object Val)? SeenStateInternal(int ns, RecordId recordId, int fieldId)
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
    public OutboxEntry? PushOutbox(int ns, RecordId recordId, object op, byte[] author, byte[]? peer = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return PushOutboxInternal(ns, recordId, op, author, peer);
    }

    internal OutboxEntry? PushOutboxInternal(int ns, RecordId recordId, object op, byte[] author, byte[]? peer = null)
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
                            if (!q.RecordId.AsSpan().SequenceEqual(recordId.ToArray()) ||
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
            RecordId = recordId.ToArray(),
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
    public bool WriteField(int namespaceId, RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return WriteFieldInternal(namespaceId, recordId, fieldId, value, hlc);
    }

    internal bool WriteFieldInternal(int namespaceId, RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId.ToArray(), (ushort)namespaceId);
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
    public bool TombField(int namespaceId, RecordId recordId, int fieldId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return TombFieldInternal(namespaceId, recordId, fieldId, hlc);
    }

    internal bool TombFieldInternal(int namespaceId, RecordId recordId, int fieldId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId.ToArray(), (ushort)namespaceId);
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
    public bool DeleteRecord(int namespaceId, RecordId recordId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return DeleteRecordInternal(namespaceId, recordId, hlc);
    }

    internal bool DeleteRecordInternal(int namespaceId, RecordId recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId.ToArray(), (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Dead);
        var op = new OpRecordTombstone { RecordId = recordId.ToArray(), Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId };
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    /// Undelete only via explicit lifecycle write value=0x00 (§7.4).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>UndeleteRecordInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool UndeleteRecord(int namespaceId, RecordId recordId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return UndeleteRecordInternal(namespaceId, recordId, hlc);
    }

    internal bool UndeleteRecordInternal(int namespaceId, RecordId recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId.ToArray(), (ushort)namespaceId);
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

}
