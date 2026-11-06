// Node engine: storage, outbox/inbox, alias maps, batches, applyBatch (§13.4),
// relay with alias translation (§13.6), snapshots (§12.18), echo suppression [E2],
// idempotency-before-chain-check [S7].
// Port of binsync/node.py — all SPEC HARDENING / errata notes preserved.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node : IDisposable
{
    internal static void ValidateNamespaceId(int namespaceId)
    {
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId), "Namespace id must fit the 16-bit wire field.");
    }

    internal static void ValidateFieldId(int fieldId)
    {
        if ((uint)fieldId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(fieldId), "Field id must fit the 16-bit wire field.");
    }

    /// <summary>16 zero bytes used as a sentinel identity or empty digest prefix.</summary>
    private static readonly byte[] Zero128Storage = new byte[16];
    /// <summary>16 zero bytes. Returns a fresh copy so callers cannot corrupt a shared buffer.</summary>
    public static byte[] Zero128 => (byte[])Zero128Storage.Clone();

    /// <summary>Shared empty alias map to avoid per-call empty Dictionary allocations.</summary>
    private static readonly Dictionary<int, (NodeId Id, int Epoch)> EmptyAliasMap = new();
    private static readonly Dictionary<(NodeId Id, int Epoch), int> EmptyRevAlias = new();

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
    /// Acquire all domain locks (Storage → Outbox → Meta) for a point-in-time snapshot.
    /// </summary>
    public AllGateScope EnterAllGates()
    {
        var scope = new AllGateScope(_lockStorage, _lockOutbox, _lockMeta);
        try
        {
            ThrowIfDisposed();
            return scope;
        }
        catch
        {
            scope.Dispose();
            throw;
        }
    }

    internal void ThrowIfDisposed()
    {
        if (System.Threading.Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(Node));
        if (System.Threading.Volatile.Read(ref _replayFaulted) != 0)
            throw new InvalidOperationException(
                "Node is unusable after an incomplete WAL replay; discard it and reload from a valid snapshot.");
    }

    // A semantic WAL failure can occur only after earlier records have been replayed.
    // Fail closed so callers cannot accidentally use or persist a partially restored node.
    internal void MarkReplayFaulted() =>
        System.Threading.Volatile.Write(ref _replayFaulted, 1);

    /// <summary>Raised after a batch is applied or accepted as a duplicate (outside engine locks).</summary>
    public event Action<BatchResult, NodeId>? BatchApplied;

    /// <summary>Raised after a successful local field write (outside engine locks).</summary>
    public event Action<int, RecordId, int>? FieldWritten;
    /// <summary>Raised after a local field tombstone is committed (outside engine locks).</summary>
    public event Action<int, RecordId, int>? FieldTombstoned;
    /// <summary>Raised after a local record deletion is committed (outside engine locks).</summary>
    public event Action<int, RecordId>? RecordDeleted;
    /// <summary>Raised after an explicit local record revive is committed (outside engine locks).</summary>
    public event Action<int, RecordId>? RecordUndeleted;
    /// <summary>Raised after the typed low-level PushOutbox API commits an entry (outside node locks).</summary>
    public event Action<OutboxEntry>? OutboxEntryPushed;

    /// <summary>Raised after an ACK advances peer watermarks (outside engine locks).</summary>
    public event Action<NodeId, int, long, NodeId?>? AckProcessed;

    /// <summary>Raised after a MEMBERSHIP roster is merged (outside engine locks when possible).</summary>
    public event Action<MembershipMessage, int>? MembershipApplied;

    /// <summary>Raised after a KEY_ROTATION notice is accepted.</summary>
    public event Action<KeyRotationNotice>? KeyRotationApplied;
    
    /// <summary>Raised when a local schema is registered (outside node locks; control plane only).</summary>
    public event Action<int, int, int>? SchemaRegistered;
    /// <summary>Raised when a SCHEMA_MAP teaches at least one new schema version (outside node locks).</summary>
    public event Action<int>? SchemaMapLearned;
    /// <summary>Raised after a GC_NOTICE is merged (outside engine locks).</summary>
    public event Action<GcNoticeMessage, int>? GcNoticeApplied;
    /// <summary>Raised after tombstones are purged (outside engine locks).</summary>
    public event Action<int, int, ulong>? GarbageCollected;
    /// <summary>Raised after a peer trust pin is added or revoked (outside node locks; control plane only).</summary>
    public event Action<NodeId, bool>? TrustChanged;
    /// <summary>Raised after a complete snapshot is applied (outside engine locks).</summary>
    public event Action<int, int>? SnapshotApplied;

    private int _disposed;
    private int _replayFaulted;

    /// <summary>Stable node identity; set only in the constructor.</summary>
    public NodeId NodeId { get; }
    /// <summary>Session/epoch counter; mutated on load and epoch bump.</summary>
    public int Epoch { get; internal set; }
    /// <summary>HLC clock (starts cold §5.2). Replace is not allowed; mutate LastWall via load.</summary>
    internal HlcClock Clock { get; }
    /// <summary>Consistent read-only snapshot of the node's hybrid logical clock.</summary>
    public HlcClockSnapshot ClockState => Clock.Snapshot();

    /// <summary>
    /// Optional resource caps (outbox/storage/seen-set). When set, enqueue and apply
    /// paths enforce them; null means unlimited (tests / unconstrained hosts).
    /// </summary>
    private ResourceLimits? _limits;

    /// <summary>Optional resource caps; configuration access is serialized with Node metadata operations.</summary>
    public ResourceLimits? Limits
    {
        get { lock (_lockMeta) { ThrowIfDisposed(); return _limits; } }
        set { lock (_lockMeta) { ThrowIfDisposed(); System.Threading.Volatile.Write(ref _limits, value); } }
    }

    /// <summary>Look up a record in the live store.</summary>
    public bool TryGetRecord(int namespaceId, RecordId recordId, out Record? record)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockStorage)
        {
            ThrowIfDisposed();
            if (_storage.TryGetValue(namespaceId, out var map) && map.TryGetValue(recordId, out var rec))
            {
                record = CloneForPublicRead(rec);
                return true;
            }
            record = null;
            return false;
        }
    }

    /// <summary>Look up one field state inside a record.</summary>
    public bool TryGetField(int namespaceId, RecordId recordId, int fieldId, out FieldState? field)
    {
        ValidateNamespaceId(namespaceId);
        ValidateFieldId(fieldId);
        lock (_lockStorage)
        {
            ThrowIfDisposed();
            if (_storage.TryGetValue(namespaceId, out var map)
                && map.TryGetValue(recordId, out var rec)
                && rec.State.TryGetValue((ushort)fieldId, out var fs))
            {
                field = CloneFieldForPublicRead(fs);
                return true;
            }
            field = null;
            return false;
        }
    }

    /// <summary>Number of records currently stored in a namespace.</summary>
    public int GetRecordCount(int namespaceId)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockStorage)
        {
            ThrowIfDisposed();
            return _storage.TryGetValue(namespaceId, out var map) ? map.Count : 0;
        }
    }

    /// <summary>Number of pending outbox entries for a namespace.</summary>
    public int GetOutboxCount(int namespaceId) => OutboxSize(namespaceId);

    /// <summary>Register an in-process peer for alias negotiation / loopback helpers.</summary>
    public void RegisterLoopbackPeer(Node peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            PeersByNid ??= new Dictionary<NodeId, Node>();
            PeersByNid[peer.NodeId] = peer;
        }
    }

    // storage: namespaceId -> recordId -> Record (RecordId uses value equality).
    private readonly Dictionary<int, Dictionary<RecordId, Record>> _storage = new();

    /// <summary>Live record store. Mutate maps in place; wholesale replace is not supported.</summary>
    /// <summary>Live record store. Engine-internal; use TryGetRecord/TryGetField from outside.</summary>
    internal Dictionary<int, Dictionary<RecordId, Record>> Storage => _storage;

    /// <summary>Pending local mutations awaiting delta batch export.</summary>
    internal Dictionary<int, List<OutboxEntry>> Outbox { get; } = new();
    internal readonly Dictionary<(NodeId, int), long> _outboxSeq = new();   // (author, ns) -> seq
    // inbox: (issuerNodeId, ns, epoch, batchId) — epoch in key for [Э2] isolation
    /// <summary>Set of applied batch keys for idempotent replay detection.</summary>
    internal HashSet<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)> Inbox { get; } = new();
    /// <summary>Durable author stream for each accepted batch, retained for exact duplicate ACKs.</summary>
    internal Dictionary<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId), NodeId> InboxAuthors { get; } = new();
    // batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
    /// <summary>Per-(issuer, ns, epoch) chain checkpoint of the last accepted batch digest.</summary>
    internal Dictionary<(NodeId Issuer, int Ns, int Epoch), Digest16> LastBatchDigest { get; } = new();
    // per-stream prevBatchDigest: AM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
    // -> last sent batchDigest (§12.17.2 hardening).
    /// <summary>Per-stream prev-batch digest checkpoints on the issuer side.</summary>
    internal Dictionary<((NodeId Issuer, int Ns, int Epoch), NodeId Peer), Digest16> LastStreamDigest { get; } = new();
    // alias maps per session peer: peerNodeId -> {alias -> (nodeId, epoch)} and reverse
    /// <summary>Gets alias to node.</summary>
    internal Dictionary<NodeId, Dictionary<int, (NodeId Id, int Epoch)>> AliasToNode { get; } = new();
    /// <summary>Gets node to alias.</summary>
    internal Dictionary<NodeId, Dictionary<(NodeId Id, int Epoch), int>> NodeToAlias { get; } = new();
    // Errata E-9: durable author knowledge for session-table construction only [А3].
    /// <summary>Durable author knowledge for session-table construction (Errata E-9).</summary>
    internal Dictionary<NodeId, int> DurableAuthors { get; } = new();
    /// <summary>Per-peer session cursors and ACK watermarks keyed by (peer, namespace).</summary>
    internal Dictionary<(NodeId, int), PeerState> PeerStates { get; } = new();
    // Errata E-10 (scenario 23): provenance seen-set — (senderPeer, ns, changeId)
    /// <summary>Provenance seen-set of (sender, ns, changeId) for relay (Errata E-10).</summary>
    internal HashSet<(NodeId Peer, int Ns, BatchId ChangeId)> SeenChanges { get; } = new();
    // Errata E-11: per-(senderPeer, ns) cache of the durable author stream last accepted.
    /// <summary>Cache of the durable author stream last accepted per (sender, ns).</summary>
    internal Dictionary<(NodeId, int), NodeId> LastStreamAuthor { get; } = new();
    /// <summary>Highest known epoch per (author, namespace).</summary>
    internal Dictionary<(NodeId, int), int> KnownEpochs { get; } = new();
    /// <summary>Peers currently quarantined from sync.</summary>
    internal HashSet<NodeId> Quarantined { get; } = new();
    /// <summary>Collected error code names from the last operations (diagnostic).</summary>
    internal List<string> ErrorsOut { get; } = new();
    /// <summary>Batch ids ACKed during the last apply (diagnostic).</summary>
    internal List<BatchId> AcksOut { get; } = new();
    /// <summary>Namespaces that must not be applied (NAMESPACE_FORBIDDEN).</summary>
    internal HashSet<int> ForbiddenNamespaces { get; } = new();
    /// <summary>Highest supported schema version per namespace.</summary>
    internal Dictionary<int, int> SchemaVersions { get; } = new();
    /// <summary>Optional in-process peer map for loopback transport.</summary>
    /// <summary>In-process peer map for loopback. Prefer RegisterLoopbackPeer.</summary>
    internal Dictionary<NodeId, Node>? PeersByNid { get; set; }

    /// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
    /// <summary>Create a node with optional identity, epoch, clock and resource limits.</summary>
    public Node(NodeId? nodeId = null, int epoch = 1, Func<long>? nowFn = null,
                long maxSkewUs = 60_000_000, ResourceLimits? limits = null)
    {
        if (nodeId.HasValue && nodeId.Value.IsZero)
            throw new ArgumentException("Node identity must be non-zero.", nameof(nodeId));
        if (epoch < 0)
            throw new ArgumentOutOfRangeException(nameof(epoch));
        if (maxSkewUs < 0)
            throw new ArgumentOutOfRangeException(nameof(maxSkewUs));

        NodeId = nodeId ?? NodeId.From(Guid.NewGuid().ToByteArray());
        Epoch = epoch;
        Clock = new HlcClock(nowFn, maxSkewUs);
        Limits = limits;
    }

    /// <summary>Create a node from a structured options bag.</summary>
    public Node(NodeOptions options)
        : this(options.NodeId, options.Epoch, options.NowFn, options.MaxSkewUs, options.Limits)
    {
    }

    private static void LogObserverFailure(string eventName, Exception exception) =>
        System.Diagnostics.Trace.TraceError("BIN-SYNC observer for {0} failed: {1}", eventName, exception);

    private static void InvokeSafely<T1>(Action<T1>? handlers, string eventName, T1 arg1)
    {
        if (handlers is null) return;
        foreach (Action<T1> handler in handlers.GetInvocationList())
        {
            try { handler(arg1); }
            catch (Exception ex) { LogObserverFailure(eventName, ex); }
        }
    }

    private static void InvokeSafely<T1, T2>(Action<T1, T2>? handlers, string eventName, T1 arg1, T2 arg2)
    {
        if (handlers is null) return;
        foreach (Action<T1, T2> handler in handlers.GetInvocationList())
        {
            try { handler(arg1, arg2); }
            catch (Exception ex) { LogObserverFailure(eventName, ex); }
        }
    }

    private static void InvokeSafely<T1, T2, T3>(
        Action<T1, T2, T3>? handlers, string eventName, T1 arg1, T2 arg2, T3 arg3)
    {
        if (handlers is null) return;
        foreach (Action<T1, T2, T3> handler in handlers.GetInvocationList())
        {
            try { handler(arg1, arg2, arg3); }
            catch (Exception ex) { LogObserverFailure(eventName, ex); }
        }
    }

    private static void InvokeSafely<T1, T2, T3, T4>(
        Action<T1, T2, T3, T4>? handlers, string eventName, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
    {
        if (handlers is null) return;
        foreach (Action<T1, T2, T3, T4> handler in handlers.GetInvocationList())
        {
            try { handler(arg1, arg2, arg3, arg4); }
            catch (Exception ex) { LogObserverFailure(eventName, ex); }
        }
    }

    /// <summary>Raise <see cref="BatchApplied"/> after commit and outside node gates.</summary>
    internal void RaiseBatchApplied(BatchResult result, NodeId senderPeer) =>
        InvokeSafely(BatchApplied, nameof(BatchApplied), result, senderPeer);

    /// <summary>Raise <see cref="FieldWritten"/> after commit and outside node gates.</summary>
    internal void RaiseFieldWritten(int namespaceId, RecordId recordId, int fieldId) =>
        InvokeSafely(FieldWritten, nameof(FieldWritten), namespaceId, recordId, fieldId);

    internal void RaiseFieldTombstoned(int namespaceId, RecordId recordId, int fieldId) =>
        InvokeSafely(FieldTombstoned, nameof(FieldTombstoned), namespaceId, recordId, fieldId);

    internal void RaiseRecordDeleted(int namespaceId, RecordId recordId) =>
        InvokeSafely(RecordDeleted, nameof(RecordDeleted), namespaceId, recordId);

    internal void RaiseRecordUndeleted(int namespaceId, RecordId recordId) =>
        InvokeSafely(RecordUndeleted, nameof(RecordUndeleted), namespaceId, recordId);

    internal void RaiseOutboxEntryPushed(OutboxEntry entry)
    {
        var handlers = OutboxEntryPushed;
        if (handlers is null) return;
        foreach (Action<OutboxEntry> handler in handlers.GetInvocationList())
        {
            try { handler(CloneOutboxEntryForPublicRead(entry)); }
            catch (Exception ex) { LogObserverFailure(nameof(OutboxEntryPushed), ex); }
        }
    }

    internal void RaiseAckProcessed(NodeId peer, int namespaceId, long ackedSeq, NodeId? authorStream) =>
        InvokeSafely(AckProcessed, nameof(AckProcessed), peer, namespaceId, ackedSeq, authorStream);

    internal void RaiseMembershipApplied(MembershipMessage msg, int learned) =>
        InvokeSafely(MembershipApplied, nameof(MembershipApplied), msg, learned);

    internal void RaiseKeyRotationApplied(KeyRotationNotice notice) =>
        InvokeSafely(KeyRotationApplied, nameof(KeyRotationApplied), notice);

    internal void RaiseSchemaRegistered(int namespaceId, int schemaId, int version) =>
        InvokeSafely(SchemaRegistered, nameof(SchemaRegistered), namespaceId, schemaId, version);

    internal void RaiseSchemaMapLearned(int learned) =>
        InvokeSafely(SchemaMapLearned, nameof(SchemaMapLearned), learned);

    internal void RaiseGcNoticeApplied(GcNoticeMessage notice, int added) =>
        InvokeSafely(GcNoticeApplied, nameof(GcNoticeApplied), notice, added);

    internal void RaiseGarbageCollected(int namespaceId, int removed, ulong cutoffWall) =>
        InvokeSafely(GarbageCollected, nameof(GarbageCollected), namespaceId, removed, cutoffWall);

    internal void RaiseTrustChanged(NodeId nodeId, bool trusted) =>
        InvokeSafely(TrustChanged, nameof(TrustChanged), nodeId, trusted);

    internal void RaiseSnapshotApplied(int namespaceId, int applied) =>
        InvokeSafely(SnapshotApplied, nameof(SnapshotApplied), namespaceId, applied);

    /// <summary>Apply a verified KEY_ROTATION notice via <paramref name="store"/> and raise <see cref="KeyRotationApplied"/>.</summary>
    public bool ApplyKeyRotation(KeyRotationNotice notice, KeyEpochStore store)
    {
        ArgumentNullException.ThrowIfNull(notice);
        ArgumentNullException.ThrowIfNull(store);
        var ownedNotice = new KeyRotationNotice
        {
            NamespaceId = notice.NamespaceId,
            OldKeyEpoch = notice.OldKeyEpoch,
            NewKeyEpoch = notice.NewKeyEpoch,
            NodeId = notice.NodeId,
            Proof = notice.Proof?.ToArray() ?? Array.Empty<byte>(),
        };
        bool applied;
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            applied = store.TryApply(ownedNotice);
        }
        if (!applied) return false;
        RaiseKeyRotationApplied(ownedNotice);
        return true;
    }

    /// <summary>Release engine state and detach caller-owned services; safe to call multiple times. Stateful API calls after disposal throw <see cref="ObjectDisposedException"/>.</summary>
    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            _storage.Clear();
            Outbox.Clear();
            Inbox.Clear();
            InboxAuthors.Clear();
            PeerStates.Clear();
            AliasToNode.Clear();
            NodeToAlias.Clear();
            SeenChanges.Clear();
            LastBatchDigest.Clear();
            LastStreamDigest.Clear();
            DurableAuthors.Clear();
            KnownEpochs.Clear();
            LastStreamAuthor.Clear();
            Quarantined.Clear();
            SchemaVersions.Clear();
            ForbiddenNamespaces.Clear();
            _outboxSeq.Clear();
            AcksOut.Clear();
            ErrorsOut.Clear();
            _schemas = null;
            _causal = null;
            _e2eeKeys = null;
            _trust = null;
            _wal = null;
            System.Threading.Volatile.Write(ref _limits, null);
            PeersByNid = null;
            BatchApplied = null;
            FieldWritten = null;
            FieldTombstoned = null;
            RecordDeleted = null;
            RecordUndeleted = null;
            OutboxEntryPushed = null;
            AckProcessed = null;
            MembershipApplied = null;
            KeyRotationApplied = null;
            SchemaRegistered = null;
            SchemaMapLearned = null;
            GcNoticeApplied = null;
            GarbageCollected = null;
            TrustChanged = null;
            SnapshotApplied = null;
        }
        GC.SuppressFinalize(this);
    }

    // Alias / session epoch / durable id → Production/Node.Alias.cs (partial)

    // Local writes / outbox → Production/Node.LocalWrites.cs
    // CreateDeltaBatch / SignBatch → Production/Node.DeltaBatch.cs
    // ApplyBatch / Snapshot → Production/Node.ApplyBatch.cs
    // Helpers → Production/Node.Helpers.cs

}
