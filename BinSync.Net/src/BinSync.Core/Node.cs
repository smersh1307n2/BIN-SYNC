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

    // Local writes / outbox → Production/Node.LocalWrites.cs
    // CreateDeltaBatch / SignBatch → Production/Node.DeltaBatch.cs
    // ApplyBatch / Snapshot → Production/Node.ApplyBatch.cs
    // Helpers → Production/Node.Helpers.cs

}
