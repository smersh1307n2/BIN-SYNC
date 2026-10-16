namespace BinSync.Core;

/// <summary>
/// Thread-safe CRDT sync node (spec §7.4 / §12.18 engine contract).
///
/// All public operations are serialized through a re-entrant gate (<see cref="_gate"/>):
/// the public surface only takes the lock and delegates to private *Impl methods that
/// contain the actual (non-thread-safe) engine logic. Re-entrancy lets internal helpers
/// (e.g. CreateDeltaBatch -> PushOutbox) run under one acquisition.
///
/// Deadlock prevention (§12.18): transport code must never hold the gates of two nodes at
/// the same time. See <see cref="AsyncTransport"/> — it copies out of one node, releases the
/// gate, and only then delivers into the other node.
/// </summary>
public sealed class Node
{
    /// <summary>Re-entrant gate for all state mutations/reads.</summary>
    private readonly object _gate = new();

    /// <summary>The synchronization gate. External collaborators (persistence) may take it to
    /// obtain a consistent point-in-time image of the node state.</summary>
    internal object Gate => _gate;

    // ---- state (guarded by _gate) ----------------------------------------------------
    private readonly Dictionary<string, Record> _records = new();
    private readonly Dictionary<string, string> _aliasMap = new();          // alias -> canonical id
    private readonly Dictionary<string, Queue<DeltaBatch>> _outbox = new(); // per-author pending batches
    private readonly Dictionary<string, long> _outboxSeq = new();           // per-author stream sequence
    private readonly Dictionary<string, long> _inboxCursors = new();        // last applied seq per peer
    private readonly HashSet<string> _appliedBatchIds = new();              // replay cache (batchId level)
    private readonly Dictionary<string, string> _replayDigestCache = new(); // batchId -> digest (checkpoint)
    private readonly List<(long Seq, string BatchId, string Digest)> _checkpoints = new(); // batch digests
    private readonly SortedSet<string> _knownAuthors = new(StringComparer.Ordinal); // authors seen in the replica set
    private readonly HlcClock _clock;

    public string Id { get; }

    public Node(string nodeId, long startUs = 0)
    {
        Id = nodeId;
        _clock = new HlcClock(nodeId, startUs);
        _knownAuthors.Add(nodeId);
    }

    // ---- public API (locked) ---------------------------------------------------------

    /// <summary>Write a field of a record (creating the record if needed) and queue it in the outbox.</summary>
    public FieldValue WriteField(string recId, string field, byte[] value)
    {
        lock (_gate) return WriteFieldImpl(recId, field, value);
    }

    /// <summary>Tombstone a record on behalf of <paramref name="author"/>.</summary>
    public TombOp DeleteRecord(string recId, string? author = null)
    {
        lock (_gate) return DeleteRecordImpl(recId, author ?? Id);
    }

    /// <summary>Queue a locally-built batch into the per-author outbox stream.</summary>
    public DeltaBatch PushOutbox(DeltaBatch batch)
    {
        lock (_gate) return PushOutboxImpl(batch);
    }

    /// <summary>Build a delta batch for a peer from everything queued since its inbox cursor.</summary>
    public DeltaBatch? CreateDeltaBatch(string peer)
    {
        lock (_gate) return CreateDeltaBatchImpl(peer);
    }

    /// <summary>Apply an incoming batch from <paramref name="peer"/>. Idempotent + gap-detecting.</summary>
    public ApplyResult ApplyBatch(DeltaBatch batch)
    {
        lock (_gate) return ApplyBatchImpl(batch);
    }

    /// <summary>Process a peer's ack (up to which seq it has durably applied): drops outbox prefix.</summary>
    public int ProcessAck(string peer, long ackSeq)
    {
        lock (_gate) return ProcessAckImpl(peer, ackSeq);
    }

    /// <summary>Register an alias (nickname) pointing at a canonical record id.</summary>
    public void SetAlias(string alias, string recId)
    {
        lock (_gate) SetAliasImpl(alias, recId);
    }

    /// <summary>Resolve an alias to the canonical id (identity if not aliased).</summary>
    public string ResolveAlias(string aliasOrId)
    {
        lock (_gate) return ResolveAliasImpl(aliasOrId);
    }

    /// <summary>Full-state snapshot for cold-start bootstrapping of a peer.</summary>
    public Snapshot TakeSnapshot()
    {
        lock (_gate) return TakeSnapshotImpl();
    }

    /// <summary>Merge a remote snapshot (record-level LWW merge; cursors are max-merged).</summary>
    public void ApplySnapshot(Snapshot snap)
    {
        lock (_gate) ApplySnapshotImpl(snap);
    }

    /// <summary>Peek the next deliverable batch for a peer without consuming anything.</summary>
    public DeltaBatch? PeekOutboxFor(string peer)
    {
        lock (_gate)
            return _outbox.TryGetValue(peer, out var q) && q.Count > 0 ? q.Peek() : null;
    }

    /// <summary>Last acknowledged/applied inbound seq from a peer.</summary>
    public long InboxCursorOf(string peer)
    {
        lock (_gate) return _inboxCursors.TryGetValue(peer, out var s) ? s : 0;
    }

    /// <summary>Current per-author outbound stream sequence (persisted across restarts).</summary>
    public long OutboxSeqOf(string author)
    {
        lock (_gate) return _outboxSeq.TryGetValue(author, out var s) ? s : 0;
    }

    public IReadOnlyList<(long Seq, string BatchId, string Digest)> Checkpoints
    {
        get { lock (_gate) return _checkpoints.ToList(); }
    }

    /// <summary>Copy of a record (or null when absent). Used by tests/GC inspection.</summary>
    public Record? GetRecord(string recId)
    {
        lock (_gate)
        {
            var id = ResolveAliasImpl(recId);
            return _records.TryGetValue(id, out var r) ? r.Clone() : null;
        }
    }

    public IEnumerable<string> RecordIds()
    {
        lock (_gate) return _records.Keys.ToList();
    }

    public int RecordCount()
    {
        lock (_gate) return _records.Count;
    }

    /// <summary>Current HLC stamp of this node (advances the clock).</summary>
    public (long ts, uint log, string node) NewStamp()
    {
        lock (_gate) return _clock.New();
    }

    /// <summary>Raw HLC reading without advancing (for persistence).</summary>
    internal (long phys, long log) ClockRaw()
    {
        lock (_gate) return (_clock.PhysicalUs, _clock.Logical);
    }

    /// <summary>True when the record is Dead and every known author tombstoned it.</summary>
    public bool IsFullyDead(string recId)
    {
        lock (_gate)
        {
            var id = ResolveAliasImpl(recId);
            return _records.TryGetValue(id, out var r) && r.IsFullyDead(_knownAuthors);
        }
    }

    internal HlcClock Clock => _clock;
    internal Dictionary<string, Record> RecordsRef => _records;
    internal Dictionary<string, string> AliasMapRef => _aliasMap;
    internal Dictionary<string, Queue<DeltaBatch>> OutboxRef => _outbox;
    internal Dictionary<string, long> OutboxSeqRef => _outboxSeq;
    internal Dictionary<string, long> InboxCursorsRef => _inboxCursors;
    internal HashSet<string> AppliedBatchIdsRef => _appliedBatchIds;
    internal Dictionary<string, string> ReplayDigestCacheRef => _replayDigestCache;
    internal List<(long Seq, string BatchId, string Digest)> CheckpointsRef => _checkpoints;
    internal SortedSet<string> KnownAuthorsRef => _knownAuthors;

    // ---- GC ("ghost records", §7.4 [У7]) ---------------------------------------------

    /// <summary>
    /// Collect garbage: removes ONLY records whose lifecycle status is <see cref="RecordStatus.Dead"/>
    /// AND whose every trace (all field versions AND all tombstones) is older than the horizon
    /// <c>nowUs - graceUs</c>. Fresh dead records, live records, and partially-dead records
    /// (missing tombstone evidence from some known author) are preserved.
    /// Returns the ids of collected records.
    /// </summary>
    public IReadOnlyList<string> CollectGarbage(long nowUs, long graceUs)
    {
        lock (_gate) return CollectGarbageImpl(nowUs, graceUs);
    }

    private IReadOnlyList<string> CollectGarbageImpl(long nowUs, long graceUs)
    {
        if (graceUs < 0) throw new ArgumentOutOfRangeException(nameof(graceUs));
        long horizon = nowUs - graceUs;
        var doomed = new List<string>();
        foreach (var (id, rec) in _records)
        {
            if (rec.Status != RecordStatus.Dead) continue;          // lifecycle must be Dead
            if (rec.Tombstones.Count == 0) continue;                // no death evidence at all
            if (!_knownAuthors.All(a => rec.Tombstones.ContainsKey(a))) continue; // not fully dead
            // every field version and every tombstone must be strictly older than the horizon
            bool allOld = true;
            foreach (var f in rec.Fields.Values)
                foreach (var v in f.AllVersions())
                    if (v.TsUs > horizon) { allOld = false; break; }
            if (allOld)
                foreach (var t in rec.Tombstones.Values)
                    if (t > horizon) { allOld = false; break; }
            if (allOld) doomed.Add(id);
        }
        foreach (var id in doomed)
        {
            _records.Remove(id);
            foreach (var kv in _aliasMap.Where(a => a.Value == id).ToList())
                _aliasMap.Remove(kv.Key);
        }
        return doomed;
    }

    // ---- private impl (assumes _gate held) --------------------------------------------

    private FieldValue WriteFieldImpl(string recId, string field, byte[] value)
    {
        var id = ResolveAliasImpl(recId);
        if (!_records.TryGetValue(id, out var rec))
            _records[id] = rec = new Record { Id = id };
        if (rec.Status == RecordStatus.Dead)
            throw new InvalidOperationException($"Record '{id}' is Dead; resurrect via snapshot merge only.");

        var (ts, log, node) = _clock.New();
        var fv = new FieldValue { Author = node, TsUs = ts, Value = (byte[])value.Clone() };
        if (!rec.Fields.TryGetValue(field, out var cell))
            rec.Fields[field] = cell = new FieldCell();
        cell.Merge(fv);

        // queue into our own outbox stream (to every known peer lazily via CreateDeltaBatch)
        var batch = new DeltaBatch
        {
            Peer = Id,
            Ops = new List<FieldOp> { new(id, field, fv) },
            HlcTs = ts, HlcLog = log, HlcNode = node,
        };
        PushOutboxLocal(batch);
        return fv;
    }

    private TombOp DeleteRecordImpl(string recId, string author)
    {
        var id = ResolveAliasImpl(recId);
        if (!_records.TryGetValue(id, out var rec))
            _records[id] = rec = new Record { Id = id };
        var (ts, log, node) = _clock.New();
        _knownAuthors.Add(author);
        rec.Tombstones[author] = Math.Max(ts, rec.Tombstones.GetValueOrDefault(author));
        // Record becomes Dead only when authored by its owner-tombstone and stripped of live intent.
        if (author == id || author == Id)
            rec.Status = RecordStatus.Dead;

        var batch = new DeltaBatch
        {
            Peer = Id,
            Tombs = new List<TombOp> { new(id, author, ts) },
            HlcTs = ts, HlcLog = log, HlcNode = node,
        };
        PushOutboxLocal(batch);
        return batch.Tombs[0];
    }

    private void PushOutboxLocal(DeltaBatch batch)
    {
        batch.Peer = Id;
        if (!_outboxSeq.TryGetValue(Id, out var seq)) _outboxSeq[Id] = seq = 0;
        batch.Seq = ++seq;
        _outboxSeq[Id] = seq;
        batch.DepSeq = _inboxCursors.Values.DefaultIfEmpty().Max();
        TrackAuthors(batch);
        batch.ComputeDigest();
        batch.BatchId = $"{Id}:{batch.Seq}:{batch.Digest[..16]}";
        if (!_outbox.TryGetValue(Id, out var q)) _outbox[Id] = q = new Queue<DeltaBatch>();
        q.Enqueue(batch);
    }

    private DeltaBatch PushOutboxImpl(DeltaBatch batch)
    {
        var author = batch.Peer.Length == 0 ? Id : batch.Peer;
        batch.Peer = author;
        if (!_outboxSeq.TryGetValue(author, out var seq)) _outboxSeq[author] = seq = 0;
        batch.Seq = ++seq;
        _outboxSeq[author] = seq;
        if (batch.DepSeq == 0)
            batch.DepSeq = _inboxCursors.Values.DefaultIfEmpty().Max();
        if (string.IsNullOrEmpty(batch.BatchId))
            batch.BatchId = $"{author}:{batch.Seq}:{Blake3.Hex256(batch.CanonicalBytes())[..16]}";
        batch.ComputeDigest();
        if (!_outbox.TryGetValue(author, out var q)) _outbox[author] = q = new Queue<DeltaBatch>();
        q.Enqueue(batch);
        return batch;
    }

    private void TrackAuthors(DeltaBatch batch)
    {
        foreach (var o in batch.Ops) _knownAuthors.Add(o.Value.Author);
        foreach (var t in batch.Tombs) _knownAuthors.Add(t.Author);
    }

    private DeltaBatch? CreateDeltaBatchImpl(string peer)
    {
        // Aggregate everything still in our outbox that the peer hasn't been given yet.
        if (!_outbox.TryGetValue(Id, out var q) || q.Count == 0) return null;
        // The delta aggregates every still-pending batch newer than the peer's inbox cursor.
        long since = _inboxCursors.TryGetValue(peer, out var ic0) ? ic0 : 0;
        var ops = new List<FieldOp>();
        var tombs = new List<TombOp>();
        var aliases = new List<(string, string)>();
        foreach (var b in q)
        {
            if (b.Seq <= since) continue;
            ops.AddRange(b.Ops);
            tombs.AddRange(b.Tombs);
            aliases.AddRange(b.AliasRenames);
        }
        if (ops.Count == 0 && tombs.Count == 0 && aliases.Count == 0) return null;
        var (ts, log, node) = _clock.New();
        var batch = new DeltaBatch
        {
            Peer = Id,
            DepSeq = _inboxCursors.TryGetValue(peer, out var ic) ? ic : 0,
            Ops = ops,
            Tombs = tombs,
            AliasRenames = aliases,
            HlcTs = ts, HlcLog = log, HlcNode = node,
        };
        return PushOutboxImpl(batch);
    }

    private ApplyResult ApplyBatchImpl(DeltaBatch batch)
    {
        var peer = batch.Peer;
        _clock.Observe(batch.HlcTs, batch.HlcLog);

        // Replay cache: exact batch already applied.
        if (_appliedBatchIds.Contains(batch.BatchId)) return ApplyResult.Duplicate;

        long cursor = _inboxCursors.TryGetValue(peer, out var c) ? c : 0;
        if (batch.Seq <= cursor) return ApplyResult.Duplicate;   // stale / already covered
        if (batch.Seq > cursor + 1) return ApplyResult.OutOfOrder; // gap → caller must resync

        // integrity check
        if (!string.IsNullOrEmpty(batch.Digest) && Blake3.Hex256(batch.CanonicalBytes()) != batch.Digest)
            throw new InvalidDataException($"Batch {batch.BatchId} digest mismatch — corrupt payload.");

        foreach (var (rid, alias) in batch.AliasRenames)
            if (!_aliasMap.ContainsKey(alias)) _aliasMap[alias] = rid;

        foreach (var op in batch.Ops)
        {
            _knownAuthors.Add(op.Value.Author);
            var id = ResolveAliasImpl(op.RecId);
            if (!_records.TryGetValue(id, out var rec))
                _records[id] = rec = new Record { Id = id };
            if (!rec.Fields.TryGetValue(op.Field, out var cell))
                rec.Fields[op.Field] = cell = new FieldCell();
            cell.Merge(op.Value);
        }

        foreach (var t in batch.Tombs)
        {
            var id = ResolveAliasImpl(t.RecId);
            if (!_records.TryGetValue(id, out var rec))
                _records[id] = rec = new Record { Id = id };
            _knownAuthors.Add(t.Author);
            rec.Tombstones[t.Author] = Math.Max(rec.Tombstones.GetValueOrDefault(t.Author), t.TsUs);
            // The record is Dead only when EVERY known author left a tombstone (§7.4).
            rec.Status = _knownAuthors.All(a => rec.Tombstones.ContainsKey(a))
                ? RecordStatus.Dead
                : RecordStatus.Live;
        }

        _inboxCursors[peer] = batch.Seq;
        _appliedBatchIds.Add(batch.BatchId);
        _replayDigestCache[batch.BatchId] = batch.Digest;
        _checkpoints.Add((batch.Seq, batch.BatchId, batch.Digest));
        return ApplyResult.Applied;
    }

    private int ProcessAckImpl(string peer, long ackSeq)
    {
        if (!_outbox.TryGetValue(peer, out var q))
            if (!_outbox.TryGetValue(Id, out q)) return 0;
        int dropped = 0;
        while (q.Count > 0 && q.Peek().Seq <= ackSeq) { q.Dequeue(); dropped++; }
        // keep the replay-cache bounded but retain digests for idempotency
        return dropped;
    }

    private void SetAliasImpl(string alias, string recId)
    {
        var id = ResolveAliasImpl(recId);
        _aliasMap[alias] = id;
        var batch = new DeltaBatch
        {
            Peer = Id,
            AliasRenames = new List<(string, string)> { (id, alias) },
        };
        var (ts, log, node) = _clock.New();
        batch.HlcTs = ts; batch.HlcLog = log; batch.HlcNode = node;
        PushOutboxLocal(batch);
    }

    private string ResolveAliasImpl(string aliasOrId) =>
        _aliasMap.TryGetValue(aliasOrId, out var id) ? id : aliasOrId;

    private Snapshot TakeSnapshotImpl()
    {
        var snap = new Snapshot
        {
            FromPeer = Id,
            HlcPhysical = _clock.PhysicalUs,
            HlcLogical = _clock.Logical,
        };
        foreach (var (k, v) in _records) snap.Records[k] = v.Clone();
        foreach (var (k, v) in _aliasMap) snap.AliasMap[k] = v;
        foreach (var (k, v) in _outboxSeq) snap.OutboxSeq[k] = v;
        foreach (var (k, v) in _inboxCursors) snap.InboxCursors[k] = v;
        snap.Digest = Blake3.Hex256(SnapshotCanonical(snap));
        return snap;
    }

    private void ApplySnapshotImpl(Snapshot snap)
    {
        var expected = Blake3.Hex256(SnapshotCanonical(snap));
        if (!string.IsNullOrEmpty(snap.Digest) && snap.Digest != expected)
            throw new InvalidDataException("Snapshot digest mismatch.");

        foreach (var (id, remote) in snap.Records)
        {
            if (!_records.TryGetValue(id, out var local))
                _records[id] = local = new Record { Id = id };
            // status: Dead wins only with tombstone evidence and newer max-ts
            if (remote.Status == RecordStatus.Dead && remote.MaxAnyTs() >= local.MaxAnyTs())
                local.Status = RecordStatus.Dead;
            else if (remote.Status == RecordStatus.Live && remote.MaxAnyTs() > local.MaxAnyTs())
                local.Status = RecordStatus.Live;
            foreach (var (a, t) in remote.Tombstones)
            {
                _knownAuthors.Add(a);
                local.Tombstones[a] = Math.Max(local.Tombstones.GetValueOrDefault(a), t);
            }
            foreach (var (f, cell) in remote.Fields)
            {
                if (cell.Current is null) continue;
                _knownAuthors.Add(cell.Current.Author);
                if (!local.Fields.TryGetValue(f, out var lc))
                    local.Fields[f] = lc = new FieldCell();
                lc.Merge(cell.Current.Clone());
            }
        }
        foreach (var (a, id) in snap.AliasMap)
            if (!_aliasMap.ContainsKey(a)) _aliasMap[a] = id;
        foreach (var (p, s) in snap.OutboxSeq)
            _outboxSeq[p] = Math.Max(_outboxSeq.GetValueOrDefault(p), s);
        foreach (var (p, s) in snap.InboxCursors)
            _inboxCursors[p] = Math.Max(_inboxCursors.GetValueOrDefault(p), s);
        _clock.Observe(snap.HlcPhysical, snap.HlcLogical);
    }

    internal static byte[] SnapshotCanonical(Snapshot snap)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        DeltaBatch.WriteStr(w, snap.FromPeer);
        w.Write(snap.HlcPhysical);
        w.Write(snap.HlcLogical);
        foreach (var (id, rec) in snap.Records.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            DeltaBatch.WriteStr(w, id);
            w.Write((byte)rec.Status);
            w.Write(rec.Tombstones.Count);
            foreach (var (a, t) in rec.Tombstones.OrderBy(k => k.Key, StringComparer.Ordinal))
            { DeltaBatch.WriteStr(w, a); w.Write(t); }
            w.Write(rec.Fields.Count);
            foreach (var (f, c) in rec.Fields.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                DeltaBatch.WriteStr(w, f);
                var v = c.Current;
                w.Write(v is not null);
                if (v is not null)
                {
                    DeltaBatch.WriteStr(w, v.Author); w.Write(v.TsUs);
                    w.Write(v.Value.Length); w.Write(v.Value);
                }
            }
        }
        w.Write(snap.AliasMap.Count);
        foreach (var (a, id) in snap.AliasMap.OrderBy(k => k.Key, StringComparer.Ordinal))
        { DeltaBatch.WriteStr(w, a); DeltaBatch.WriteStr(w, id); }
        w.Flush();
        return ms.ToArray();
    }
}
