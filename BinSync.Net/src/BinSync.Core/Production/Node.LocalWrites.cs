// Outbox push + local field/record writes (echo [E2], E-13). Behavior unchanged.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    private static BatchId NewChangeId()
    {
        Span<byte> g = stackalloc byte[16];
        Guid.NewGuid().TryWriteBytes(g);
        return BatchId.From(g);
    }

    // Test/internal diagnostics only. Sequence reservation is deliberately not public.
    internal long NextOutboxSeqInternal(NodeId author, int namespaceId)
    {
        lock (_lockOutbox)
        {
            if (_wal is not null && author == NodeId)
                throw new InvalidOperationException("Local Outbox sequence allocation is committed with the mutation while WAL is attached.");
            var key = (author, namespaceId);
            long current = _outboxSeq.TryGetValue(key, out var value) ? value : 0;
            if (current == long.MaxValue)
                throw new SyncException("INTERNAL");
            long next = current + 1;
            _outboxSeq[key] = next;
            return next;
        }
    }

    internal (ulong Wall, uint Logical, byte[] Author, object Val)? SeenStateInternal(int ns, RecordId recordId, int fieldId)
    {
        if (!Storage.TryGetValue(ns, out var store)) return null;
        if (!store.TryGetValue(recordId, out var rec)) return null;
        FieldState? fs = rec.State.TryGetValue((ushort)fieldId, out var w) ? w :
                         (rec.FieldTombs.TryGetValue((ushort)fieldId, out var t) ? t : null);
        if (fs is null) return null;
        bool tomb = rec.FieldTombs.ContainsKey((ushort)fieldId);
        return (fs.Timestamp.Wall, fs.Timestamp.Logical, fs.AuthorNodeId.ToArray(), tomb ? "TOMB" : fs.Value);
    }

    /// Append a LOCAL write to outbox ONLY if merge reported changed (echo [E2]).
    /// Errata E-13 (scenario 37): restored-echo suppression — see Python port docstring.
    /// <summary>
    /// Append a local operation to the durable Outbox. Prefer <see cref="WriteField"/>
    /// and the other typed mutation APIs for ordinary application writes.
    /// </summary>
    public OutboxEntry? PushOutbox(int ns, RecordId recordId, OpFieldWrite op, NodeId author, NodeId? peer = null) =>
        PushOutboxCore(ns, recordId, op, author, peer);

    /// <summary>Append a local field tombstone to the durable Outbox.</summary>
    public OutboxEntry? PushOutbox(int ns, RecordId recordId, OpFieldTomb op, NodeId author, NodeId? peer = null) =>
        PushOutboxCore(ns, recordId, op, author, peer);

    /// <summary>Append a local record tombstone to the durable Outbox.</summary>
    public OutboxEntry? PushOutbox(int ns, RecordId recordId, OpRecordTombstone op, NodeId author, NodeId? peer = null) =>
        PushOutboxCore(ns, recordId, op, author, peer);

    private OutboxEntry? PushOutboxCore(int ns, RecordId recordId, object op, NodeId author, NodeId? peer)
    {
        lock (_lockStorage) lock (_lockOutbox)
        {
            var entry = PushOutboxInternal(ns, recordId, op, author.ToArray(),
                peer is { } p ? p.ToArray() : null);
            return entry is null ? null : CloneOutboxEntryForPublicRead(entry);
        }
    }

    internal OutboxEntry? PushOutboxInternal(int ns, RecordId recordId, object op, byte[] author, byte[]? peer = null)
    {
        var entry = PrepareOutboxEntryInternal(ns, recordId, op, author, suppressRestoredEcho: true);
        if (entry is null) return null;
        CommitLocalMutationInternal(entry, stagedRecord: null, applyToStorage: false);
        return entry;
    }

    private OutboxEntry? PrepareOutboxEntryInternal(
        int ns, RecordId recordId, object op, byte[] author, bool suppressRestoredEcho)
    {
        if (!NodeId.EqualsBytes(author))
            throw new InvalidOperationException("local push only; relay path enqueues raw payloads");

        // Errata E-13: manual outbox pushes may be restored echoes. A local write
        // which has already passed its merge test is not suppressed here.
        ushort? fid = op switch { OpFieldWrite fw => fw.FieldId, OpFieldTomb ft => ft.FieldId, _ => (ushort?)null };
        if (suppressRestoredEcho && fid is not null && op is not OpRecordTombstone)
        {
            var st = SeenStateInternal(ns, recordId, fid.Value);
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
                            if (recordId != q.RecordId || q.AuthorNodeId != NodeId) continue;
                            object qop;
                            try { qop = Ops.DecodeOp(q.Payload).Op; }
                            catch (Exception)
                            {
                                // Corrupt queued payload — stop scanning this list; do not fail the write.
                                break;
                            }
                            ushort? qfid = qop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                            if (qfid != fid) continue;
                            bool qSame = TagOf(qop) == TagOf(op) && WallOf(qop) == WallOf(op) &&
                                         LogicalOf(qop) == LogicalOf(op) &&
                                         ValueOf(qop).AsSpan().SequenceEqual(ValueOf(op).AsSpan());
                            if (qSame) return null;
                            break;
                        }
                    }
                }
            }
        }

        SetOrigin(op, 1);
        var payload = Ops.EncodeOp(op, fullAuthor: false);
        Limits?.EnsureCanEnqueueOutbox(this, ns);
        Limits?.EnsureSeenChangesCapacity(this);
        var key = (NodeId, ns);
        long current = _outboxSeq.TryGetValue(key, out var value) ? value : 0;
        if (current == long.MaxValue) throw new SyncException("INTERNAL");
        return new OutboxEntry
        {
            NamespaceId = ns,
            Seq = current + 1,
            ChangeId = NewChangeId(),
            RecordId = recordId,
            OpKind = TagOf(op),
            Payload = payload,
            AuthorNodeId = NodeId,
            CreatedAt = (long)Clock.SendTick().Wall,
        };
    }

    /// <summary>
    /// Durably append the complete mutation before publishing storage, sequence,
    /// Outbox and provenance changes. Caller holds Storage → Outbox locks.
    /// </summary>
    private void CommitLocalMutationInternal(
        OutboxEntry entry, Record? stagedRecord, bool applyToStorage, bool preserveRecordIdentity = false)
    {
        // Stage every in-memory publication while the caller holds Storage → Outbox.
        // Other Node operations cannot observe these changes until the gates are released.
        // If the durable append fails, roll the staged changes back before releasing them.
        Dictionary<RecordId, Record>? store = null;
        bool storageNamespaceAdded = false;
        bool storageRecordChanged = false;
        Record? previousRecord = null;
        bool previousRecordExisted = false;
        Record? identityRecord = null;
        List<(ushort FieldId, bool Existed, FieldState? Previous)>? fieldUndo = null;

        List<OutboxEntry>? outboxList = null;
        bool outboxNamespaceAdded = false;
        bool outboxEntryAdded = false;
        bool sequenceChanged = false;
        bool sequenceExisted = false;
        long previousSequence = 0;
        bool seenAdded = false;

        try
        {
            if (applyToStorage && stagedRecord is not null)
            {
                if (!Storage.TryGetValue(entry.NamespaceId, out store))
                {
                    store = new Dictionary<RecordId, Record>();
                    Storage.Add(entry.NamespaceId, store);
                    storageNamespaceAdded = true;
                }

                if (preserveRecordIdentity && store.TryGetValue(entry.RecordId, out var current))
                {
                    identityRecord = current;
                    fieldUndo = new List<(ushort, bool, FieldState?)>();
                    foreach (var pair in stagedRecord.State)
                    {
                        bool existed = current.State.TryGetValue(pair.Key, out var previous);
                        if (existed && ReferenceEquals(previous, pair.Value)) continue;
                        // Record the inverse before changing the live map so even an
                        // allocation failure during insertion can be rolled back.
                        fieldUndo.Add((pair.Key, existed, previous));
                        current.State[pair.Key] = pair.Value;
                    }
                }
                else
                {
                    previousRecordExisted = store.TryGetValue(entry.RecordId, out previousRecord);
                    store[entry.RecordId] = stagedRecord;
                    storageRecordChanged = true;
                }
            }

            if (!Outbox.TryGetValue(entry.NamespaceId, out outboxList))
            {
                outboxList = new List<OutboxEntry>();
                Outbox.Add(entry.NamespaceId, outboxList);
                outboxNamespaceAdded = true;
            }
            outboxList.Add(entry);
            outboxEntryAdded = true;

            var sequenceKey = (NodeId, entry.NamespaceId);
            sequenceExisted = _outboxSeq.TryGetValue(sequenceKey, out previousSequence);
            _outboxSeq[sequenceKey] = entry.Seq;
            sequenceChanged = true;

            seenAdded = SeenChanges.Add((NodeId, entry.NamespaceId, entry.ChangeId));
            if (!seenAdded)
                throw new InvalidOperationException("Local Outbox ChangeId is already present in SeenChanges.");

            // The single WAL frame is the commit point. On failure the staged state
            // is invisible to other Node operations and is rolled back below.
            _wal?.AppendLocalMutation(entry, applyToStorage);
        }
        catch
        {
            if (seenAdded)
                SeenChanges.Remove((NodeId, entry.NamespaceId, entry.ChangeId));

            if (sequenceChanged)
            {
                var key = (NodeId, entry.NamespaceId);
                if (sequenceExisted) _outboxSeq[key] = previousSequence;
                else _outboxSeq.Remove(key);
            }

            if (outboxEntryAdded)
                outboxList!.RemoveAt(outboxList.Count - 1);
            if (outboxNamespaceAdded && outboxList!.Count == 0)
                Outbox.Remove(entry.NamespaceId);

            if (storageRecordChanged)
            {
                if (previousRecordExisted) store![entry.RecordId] = previousRecord!;
                else store!.Remove(entry.RecordId);
            }

            if (fieldUndo is not null && identityRecord is not null)
            {
                for (int i = fieldUndo.Count - 1; i >= 0; i--)
                {
                    var undo = fieldUndo[i];
                    if (undo.Existed) identityRecord.State[undo.FieldId] = undo.Previous!;
                    else identityRecord.State.Remove(undo.FieldId);
                }
            }

            if (storageNamespaceAdded && store!.Count == 0)
                Storage.Remove(entry.NamespaceId);
            throw;
        }
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

    /// <summary>
    /// Thread-safe bounded write. Resource preflight and mutation share the node gates;
    /// the public event is raised only after all gates are released.
    /// </summary>
    internal bool WriteFieldBounded(ResourceLimits limits, int namespaceId,
        RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        ArgumentNullException.ThrowIfNull(limits);
        ArgumentNullException.ThrowIfNull(value);
        bool changed;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            // MaxRecords limits creation only; an update to an existing record
            // must remain possible when storage is exactly at capacity.
            bool recordExists = Storage.TryGetValue(namespaceId, out var store) &&
                                store.ContainsKey(recordId);
            if (!recordExists) limits.EnsureStorageCapacity(this);
            limits.EnsureCanEnqueueOutbox(this, namespaceId);
            limits.EnsureSeenChangesCapacity(this);
            changed = WriteFieldInternal(namespaceId, recordId, fieldId, value, hlc);
        }
        if (changed) RaiseFieldWritten(namespaceId, recordId, fieldId);
        return changed;
    }

    /// <summary>Local FieldWrite. Returns true if state changed (and Outbox grew).</summary>
    public bool WriteField(int namespaceId, RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        bool changed;
        lock (_lockStorage) lock (_lockOutbox)
            changed = WriteFieldInternal(namespaceId, recordId, fieldId, value, hlc);
        if (changed) RaiseFieldWritten(namespaceId, recordId, fieldId);
        return changed;
    }

    internal bool WriteFieldInternal(int namespaceId, RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        ArgumentNullException.ThrowIfNull(value);
        var ownedValue = value.ToArray();
        Storage.TryGetValue(namespaceId, out var store);
        store ??= new Dictionary<RecordId, Record>();
        bool exists = store.TryGetValue(recordId, out var currentRecord);
        var rec = exists ? Clone(currentRecord!) : new Record(recordId, (ushort)namespaceId);

        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(ownedValue, h, NodeId);
        var (winner, changed) = Merge.MergeFieldWithPolicy(
            rec.State.TryGetValue((ushort)fieldId, out var current) ? current : null,
            fs,
            MergePolicy.LwwField);
        if (!changed)
            return false;

        // Validate limits before publishing the record. A rejected Outbox append
        // must not leave a local state mutation without a sync event.
        EnsureLocalWriteCapacity(namespaceId, isNewRecord: !exists);

        // Alias 1 matches the common two-node session and avoids a payload rewrite.
        var op = new OpFieldWrite
        {
            FieldId = (ushort)fieldId,
            KeyEpoch = 0,
            Wall = h.Wall,
            Logical = h.Logical,
            Origin = 1,
            AuthorNodeId = NodeId,
            Value = ownedValue
        };

        rec.State[(ushort)fieldId] = winner;
        var entry = PrepareOutboxEntryInternal(namespaceId, recordId, op, NodeId.ToArray(),
            suppressRestoredEcho: false) ?? throw new InvalidOperationException("A changed local write must produce an Outbox entry.");
        CommitLocalMutationInternal(entry, rec, applyToStorage: true, preserveRecordIdentity: true);
        return true;
    }

    private void EnsureLocalWriteCapacity(int namespaceId, bool isNewRecord)
    {
        // MaxRecords applies to record creation, not updates of an existing record.
        if (isNewRecord)
            Limits?.EnsureStorageCapacity(this);
        Limits?.EnsureCanEnqueueOutbox(this, namespaceId);
        Limits?.EnsureSeenChangesCapacity(this);
        if (_outboxSeq.TryGetValue((NodeId, namespaceId), out var seq) && seq == long.MaxValue)
            throw new SyncException("INTERNAL");
    }

    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>TombFieldInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public bool TombField(int namespaceId, RecordId recordId, int fieldId, Hlc? hlc = null)
    {
        lock (_lockStorage) lock (_lockOutbox) return TombFieldInternal(namespaceId, recordId, fieldId, hlc);
    }

    internal bool TombFieldInternal(int namespaceId, RecordId recordId, int fieldId, Hlc? hlc = null)
    {
        Storage.TryGetValue(namespaceId, out var store);
        store ??= new Dictionary<RecordId, Record>();
        bool exists = store.TryGetValue(recordId, out var current);
        var rec = exists ? Clone(current!) : new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(Array.Empty<byte>(), h, NodeId);
        var op = new OpFieldTomb
        {
            FieldId = (ushort)fieldId,
            KeyEpoch = 0,
            Wall = h.Wall,
            Logical = h.Logical,
            Origin = 1,
            AuthorNodeId = NodeId
        };

        bool changed = Merge.ApplyFieldTomb(rec, (ushort)fieldId, fs);
        if (!changed)
            return false;

        EnsureLocalWriteCapacity(namespaceId, isNewRecord: !exists);
        var entry = PrepareOutboxEntryInternal(namespaceId, recordId, op, NodeId.ToArray(),
            suppressRestoredEcho: false) ?? throw new InvalidOperationException("A changed local tombstone must produce an Outbox entry.");
        CommitLocalMutationInternal(entry, rec, applyToStorage: true);
        return true;
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
        Storage.TryGetValue(namespaceId, out var store);
        store ??= new Dictionary<RecordId, Record>();
        bool exists = store.TryGetValue(recordId, out var current);
        var rec = exists ? Clone(current!) : new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Dead);
        if (!changed)
            return false;

        EnsureLocalWriteCapacity(namespaceId, isNewRecord: !exists);
        var op = new OpRecordTombstone
        {
            RecordId = recordId,
            Wall = h.Wall,
            Logical = h.Logical,
            Origin = 1,
            AuthorNodeId = NodeId
        };
        var entry = PrepareOutboxEntryInternal(namespaceId, recordId, op, NodeId.ToArray(),
            suppressRestoredEcho: false) ?? throw new InvalidOperationException("A changed record deletion must produce an Outbox entry.");
        CommitLocalMutationInternal(entry, rec, applyToStorage: true);
        return true;
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
        Storage.TryGetValue(namespaceId, out var store);
        store ??= new Dictionary<RecordId, Record>();
        bool exists = store.TryGetValue(recordId, out var current);
        var rec = exists ? Clone(current!) : new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Alive);

        // Explicit revive is propagated even if it is locally a no-op.
        EnsureLocalWriteCapacity(namespaceId, isNewRecord: !exists);
        var op = new OpFieldWrite
        {
            FieldId = Model.LifecycleFieldId,
            KeyEpoch = 0,
            Wall = h.Wall,
            Logical = h.Logical,
            Origin = 1,
            AuthorNodeId = NodeId,
            Value = [Model.Alive]
        };
        var entry = PrepareOutboxEntryInternal(namespaceId, recordId, op, NodeId.ToArray(),
            suppressRestoredEcho: false) ?? throw new InvalidOperationException("An explicit undelete must produce an Outbox entry.");
        CommitLocalMutationInternal(entry, changed ? rec : null, applyToStorage: true);
        return changed;
    }

}
