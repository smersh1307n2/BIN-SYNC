// Node engine: storage, outbox/inbox, alias maps, batches, applyBatch (§13.4),
// relay with alias translation (§13.6), snapshots (§12.18), echo suppression [E2],
// idempotency-before-chain-check [S7].
// Port of binsync/node.py — all SPEC HARDENING / errata notes preserved.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>16 zero bytes used as a sentinel identity or empty digest prefix.</summary>
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
    /// Legacy coarse gate object (locks Meta only). Prefer <see cref="EnterAllGates"/>.
    /// </summary>
    [Obsolete("Use EnterAllGates() for full Storage/Outbox/Meta exclusion.")]
    public object Gate => _lockMeta;


    /// <summary>Stable node identity; set only in the constructor.</summary>
    public NodeId NodeId { get; }
    /// <summary>Session/epoch counter; mutated on load and epoch bump.</summary>
    public int Epoch { get; internal set; }
    /// <summary>HLC clock (starts cold §5.2). Replace is not allowed; mutate LastWall via load.</summary>
    public HlcClock Clock { get; }

    /// <summary>
    /// Optional resource caps (outbox/storage/seen-set). When set, enqueue and apply
    /// paths enforce them; null means unlimited (tests / unconstrained hosts).
    /// </summary>
    public ResourceLimits? Limits { get; set; }
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
    internal HashSet<(byte[], int, int, byte[])> Inbox { get; } = new(BytesIntIntBytesEq.Instance);
    // batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
    /// <summary>Per-(issuer, ns, epoch) chain checkpoint of the last accepted batch digest.</summary>
    internal Dictionary<(byte[], int, int), byte[]> LastBatchDigest { get; } = new(BytesIntIntEq.Instance);
    // per-stream prevBatchDigest: AM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
    // -> last sent batchDigest (§12.17.2 hardening).
    /// <summary>Per-stream prev-batch digest checkpoints on the issuer side.</summary>
    internal Dictionary<((byte[], int, int), byte[]), byte[]> LastStreamDigest { get; } = new(BytesIntIntBytesKeyEq.Instance);
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
    internal HashSet<(byte[], int, byte[])> SeenChanges { get; } = new(SeenKeyComparer.Instance);
    // Errata E-11: per-(senderPeer, ns) cache of the durable author stream last accepted.
    /// <summary>Cache of the durable author stream last accepted per (sender, ns).</summary>
    internal Dictionary<(NodeId, int), byte[]> LastStreamAuthor { get; } = new();
    /// <summary>Highest known epoch per (author, namespace).</summary>
    internal Dictionary<(NodeId, int), int> KnownEpochs { get; } = new();
    /// <summary>Peers currently quarantined from sync.</summary>
    internal HashSet<NodeId> Quarantined { get; } = new();
    /// <summary>Collected error code names from the last operations (diagnostic).</summary>
    internal List<string> ErrorsOut { get; } = new();
    /// <summary>Batch ids ACKed during the last apply (diagnostic).</summary>
    internal List<byte[]> AcksOut { get; } = new();
    /// <summary>Namespaces that must not be applied (NAMESPACE_FORBIDDEN).</summary>
    internal HashSet<int> ForbiddenNamespaces { get; } = new();
    /// <summary>Highest supported schema version per namespace.</summary>
    internal Dictionary<int, int> SchemaVersions { get; } = new();
    /// <summary>Optional in-process peer map for loopback transport.</summary>
    /// <summary>In-process peer map for loopback. Prefer RegisterLoopbackPeer.</summary>
    internal Dictionary<NodeId, Node>? PeersByNid { get; set; }

    /// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
    public Node(byte[]? nodeId = null, int epoch = 1, Func<long>? nowFn = null,
                long maxSkewUs = 60_000_000)
    {
        var raw = nodeId ?? Guid.NewGuid().ToByteArray();
        if (raw.Length != 16) throw new ArgumentException("nodeId must be 16 bytes", nameof(nodeId));
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
