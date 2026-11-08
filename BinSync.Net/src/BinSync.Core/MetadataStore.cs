namespace BinSync;

/// <summary>
/// Durable protocol metadata boundary. Payload streams, receipts, progress,
/// leases and pending-apply recovery are separate logical retention domains.
/// </summary>
public interface IMetadataStore : IDisposable
{
    MetadataBatchReceipt AcceptIncomingBatch(MetadataIncomingBatch batch);
    MetadataPendingApplyIntent BeginPendingApply(MetadataIncomingBatch batch, Guid applyToken);
    IReadOnlyList<MetadataPendingApplyIntent> ReadPendingApplyIntents(string scopeId, NodeId localReplica);
    MetadataBatchReceipt FinalizePendingApply(string scopeId, NodeId localReplica, MetadataProviderApplyReceipt providerReceipt);
    IMetadataStoreSnapshot OpenSnapshot(string scopeId, NodeId localReplica);

    IReadOnlyList<MetadataOutboundItem> FreezeOutbound(string scopeId, NodeId localReplica, int namespaceId,
        NodeId localAuthor, IReadOnlyList<MetadataFrozenOperation> operations);
    ulong AcquireOutboundLease(string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor, Guid consumerId);
    MetadataOutboundPage ReadOutbound(string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor,
        ulong leaseGeneration, ulong afterSequence, int limit);
    void AcknowledgeOutbound(string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor,
        ulong leaseGeneration, ulong safeThroughSequence);
    void CompactOutboundThrough(string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor, ulong safeThroughSequence);
    ulong GetOutboundHighWatermark(string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor);

    MetadataRelayPage ReadRelay(string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor,
        ulong afterSequence, int limit);
    void AcknowledgeRelay(string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor, ulong safeThroughSequence);
    void CompactRelayThrough(string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor, ulong safeThroughSequence);
    ulong GetReceiveCursor(string scopeId, NodeId localReplica, NodeId sender, uint senderEpoch, int namespaceId);
    ulong GetReceiveCursor(string scopeId, NodeId localReplica, NodeId sender, uint senderEpoch, int namespaceId, NodeId authorStream)
        => GetReceiveCursor(scopeId, localReplica, sender, senderEpoch, namespaceId);
    ulong GetRelayHighWatermark(string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor);
}

/// <summary>
/// Optional Metadata Store capability used by durable provider-feed replay.
/// It avoids requiring every third-party IMetadataStore implementation to expose
/// BIN-SYNC's local outbound ChangeId ledger.
/// </summary>
public interface IMetadataOutboundChangeIdentityStore
{
    /// <summary>Returns the durable fingerprint when the ChangeId is already frozen as a local outbound operation.</summary>
    bool TryGetOutboundChangeFingerprint(string scopeId, NodeId localReplica, int namespaceId,
        BatchId changeId, out ReadOnlyMemory<byte> fingerprint);
}

public interface IMetadataStoreSnapshot : IDisposable
{
    ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId);
    ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId, NodeId authorStream)
        => GetReceiveCursor(sender, senderEpoch, namespaceId);
    ulong GetRelayHighWatermark(int namespaceId, NodeId originalAuthor);
    ulong GetOutboundHighWatermark(int namespaceId, NodeId localAuthor);
    MetadataRelayPage ReadRelay(int namespaceId, NodeId originalAuthor, ulong afterSequence, int limit);
    MetadataOutboundPage ReadOutbound(int namespaceId, NodeId localAuthor, ulong afterSequence, int limit);
}

/// <summary>
/// Indicates that a PendingApply token already has a durable completed receipt.
/// Callers may treat this specific condition as an idempotent retry; other state
/// conflicts must remain failures.
/// </summary>
public sealed class MetadataPendingApplyAlreadyFinalizedException : InvalidOperationException
{
    public MetadataPendingApplyAlreadyFinalizedException()
        : base("Pending apply token has already been finalized.") { }
}

public sealed record MetadataProviderApplyReceipt(Guid ApplyToken, byte[] BatchDigest);
public sealed record MetadataPendingApplyIntent(Guid ApplyToken, MetadataIncomingBatch Batch)
{
    public bool IsDuplicate { get; init; }

    /// <summary>
    /// Per-operation apply decisions made by the Metadata Store's global ChangeId ledger.
    /// True means the operation is new and must be applied to Node data; false means the
    /// same fingerprint was already accepted and Node must not recreate a second identity ledger.
    /// </summary>
    public IReadOnlyList<bool> ApplyOperations { get; init; } = Array.Empty<bool>();
}

public sealed class MetadataOperation
{
    public NodeId AuthorNodeId { get; init; }
    public BatchId ChangeId { get; init; }
    public required byte[] Fingerprint { get; init; }
    public required byte[] Payload { get; init; }
    public bool RelayEligible { get; init; } = true;
    public uint OriginAuthorEpoch { get; init; }
    public ulong OriginSequence { get; init; }
    /// <summary>Canonical immutable origin tuple for this globally identified operation.</summary>
    public OperationOrigin Provenance => AuthorNodeId.IsZero || OriginAuthorEpoch == 0 || OriginSequence == 0
        ? default : new OperationOrigin(AuthorNodeId, OriginAuthorEpoch, OriginSequence);
}

public sealed class MetadataIncomingBatch
{
    public required string ScopeId { get; init; }
    public required NodeId LocalReplica { get; init; }
    public required NodeId SenderPeer { get; init; }
    /// <summary>Wire issuer/author epoch; distinct from the authenticated transport peer epoch.</summary>
    public required uint SenderEpoch { get; init; }
    /// <summary>Authenticated transport peer session epoch; zero only for legacy callers without session context.</summary>
    public uint TransportPeerEpoch { get; init; }
    /// <summary>Original author stream carried by this batch; cursors are independent per author.</summary>
    public NodeId AuthorStreamNodeId { get; init; }
    public required int NamespaceId { get; init; }
    public required BatchId BatchId { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] PreviousBatchDigest { get; init; }
    public required byte[] BatchDigest { get; init; }
    /// <summary>Canonical fingerprint of the full wire header, including fields outside BatchDigest.</summary>
    public byte[] HeaderFingerprint { get; init; } = Array.Empty<byte>();
    public required IReadOnlyList<MetadataOperation> Operations { get; init; }
    /// <summary>Exact accepted DELTA_BATCH payload, retained for Node replay during PendingApply recovery.</summary>
    public byte[] WirePayload { get; init; } = Array.Empty<byte>();
    /// <summary>
    /// True when this v3 intent carries an authenticated full-header fingerprint and
    /// the Metadata Store is authoritative for receive cursor, digest-chain and receipt
    /// decisions. False is reserved for direct conformance calls without wire-session context.
    /// </summary>
    public bool ReceiveStateOwnedByMetadataStore { get; init; }
}

public sealed record MetadataBatchReceipt(ulong CursorStart, ulong CursorEnd, byte[] BatchDigest,
    int NewRelayOperationCount, bool IsDuplicate)
{
    /// <summary>Original author resolved from the accepted batch; never trust a replay-supplied value.</summary>
    public NodeId DurableAuthor { get; init; }
    /// <summary>Canonical full-wire-header fingerprint committed with the receipt.</summary>
    public byte[] HeaderFingerprint { get; init; } = Array.Empty<byte>();
}
public sealed record MetadataRelayItem(ulong Sequence, NodeId OriginalAuthor, BatchId ChangeId, byte[] Payload)
{
    public uint OriginAuthorEpoch { get; init; }
    public ulong OriginSequence { get; init; }
    /// <summary>Immutable original author provenance; Sequence above is the local relay sequence.</summary>
    public OperationOrigin Provenance => OriginalAuthor.IsZero || OriginAuthorEpoch == 0 || OriginSequence == 0
        ? default : new OperationOrigin(OriginalAuthor, OriginAuthorEpoch, OriginSequence);
}
public sealed record MetadataRelayPage(IReadOnlyList<MetadataRelayItem> Items, ulong ScannedThroughSequence);
public sealed class MetadataFrozenOperation
{
    public required BatchId ChangeId { get; init; }
    public required byte[] Fingerprint { get; init; }
    public required byte[] Payload { get; init; }
    public uint OriginAuthorEpoch { get; init; }
    public ulong OriginSequence { get; init; }
}
public sealed record MetadataOutboundItem(ulong Sequence, NodeId AuthorNodeId, BatchId ChangeId, byte[] Payload)
{
    public uint OriginAuthorEpoch { get; init; }
    public ulong OriginSequence { get; init; }
    /// <summary>Immutable origin; Sequence above is the local outbound stream sequence.</summary>
    public OperationOrigin Provenance => AuthorNodeId.IsZero || OriginAuthorEpoch == 0 || OriginSequence == 0
        ? default : new OperationOrigin(AuthorNodeId, OriginAuthorEpoch, OriginSequence);
}
public sealed record MetadataOutboundPage(IReadOnlyList<MetadataOutboundItem> Items, ulong ScannedThroughSequence);
