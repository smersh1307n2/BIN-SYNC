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


    public NodeId NodeId { get; set; }
    public int Epoch { get; set; }
    // BUGFIX (implementation finding): the HLC must start cold (§5.2) — see Python port note.
    public HlcClock Clock { get; set; } = null!;
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
            if (value is null) return;
            foreach (var (ns, inner) in value)
                _storage[ns] = new Dictionary<RecordId, Record>(inner);
        }
    }

    public Dictionary<int, List<OutboxEntry>> Outbox { get; set; } = new();
    internal readonly Dictionary<(NodeId, int), long> _outboxSeq = new();   // (author, ns) -> seq
    // inbox: (issuerNodeId, ns, epoch, batchId) — epoch in key for [Э2] isolation
    public HashSet<(byte[], int, int, byte[])> Inbox { get; } = new(BytesIntIntBytesEq.Instance);
    // batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
    public Dictionary<(byte[], int, int), byte[]> LastBatchDigest { get; } = new(BytesIntIntEq.Instance);
    // per-stream prevBatchDigest: AM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
    // -> last sent batchDigest (§12.17.2 hardening).
    public Dictionary<((byte[], int, int), byte[]), byte[]> LastStreamDigest { get; } = new(BytesIntIntBytesKeyEq.Instance);
    // alias maps per session peer: peerNodeId -> {alias -> (nodeId, epoch)} and reverse
    public Dictionary<NodeId, Dictionary<int, (NodeId Id, int Epoch)>> AliasToNode { get; set; } = new();
    public Dictionary<NodeId, Dictionary<(NodeId Id, int Epoch), int>> NodeToAlias { get; set; } = new();
    // Errata E-9 (transport): DURABLE author knowledge — every (nodeId, epoch) ever
    // resolved through ANY ALIAS_MAP. Used ONLY for session-table construction, never
    // for resolution [А3].
    public Dictionary<NodeId, int> DurableAuthors { get; set; } = new();
    public Dictionary<(NodeId, int), PeerState> PeerStates { get; set; } = new();
    // Errata E-10 (scenario 23): provenance seen-set — (senderPeer, ns, changeId)
    public HashSet<(byte[], int, byte[])> SeenChanges { get; } = new(SeenKeyComparer.Instance);
    // Errata E-11: per-(senderPeer, ns) cache of the durable author stream last accepted.
    public Dictionary<(NodeId, int), byte[]> LastStreamAuthor { get; } = new();
    public Dictionary<(NodeId, int), int> KnownEpochs { get; set; } = new();
    public HashSet<NodeId> Quarantined { get; } = new();
    public List<string> ErrorsOut { get; set; } = new();
    public List<byte[]> AcksOut { get; set; } = new();
    public HashSet<int> ForbiddenNamespaces { get; } = new();
    public Dictionary<int, int> SchemaVersions { get; set; } = new();
    public Dictionary<NodeId, Node>? PeersByNid { get; set; }

    public Node(byte[]? nodeId = null, int epoch = 1, Func<long>? nowFn = null,
                long maxSkewUs = 60_000_000)
    {
        var raw = nodeId ?? Guid.NewGuid().ToByteArray();
        if (raw.Length != 16) throw new InvalidOperationException("nodeId must be 16 bytes");
        NodeId = NodeId.From(raw);
        Epoch = epoch;
        Clock = new HlcClock(nowFn, maxSkewUs);
    }

    // Alias / session epoch / durable id → Production/Node.Alias.cs (partial)

    // Local writes / outbox → Production/Node.LocalWrites.cs
    // CreateDeltaBatch / SignBatch → Production/Node.DeltaBatch.cs
    // ApplyBatch / Snapshot → Production/Node.ApplyBatch.cs
    // Helpers → Production/Node.Helpers.cs

}
