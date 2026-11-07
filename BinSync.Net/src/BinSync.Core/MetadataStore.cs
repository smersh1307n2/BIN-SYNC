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
    ulong GetRelayHighWatermark(string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor);
}

public interface IMetadataStoreSnapshot : IDisposable
{
    ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId);
    ulong GetRelayHighWatermark(int namespaceId, NodeId originalAuthor);
    ulong GetOutboundHighWatermark(int namespaceId, NodeId localAuthor);
    MetadataRelayPage ReadRelay(int namespaceId, NodeId originalAuthor, ulong afterSequence, int limit);
    MetadataOutboundPage ReadOutbound(int namespaceId, NodeId localAuthor, ulong afterSequence, int limit);
}

public sealed record MetadataProviderApplyReceipt(Guid ApplyToken, byte[] BatchDigest);
public sealed record MetadataPendingApplyIntent(Guid ApplyToken, MetadataIncomingBatch Batch);

public sealed class MetadataOperation
{
    public NodeId AuthorNodeId { get; init; }
    public BatchId ChangeId { get; init; }
    public required byte[] Fingerprint { get; init; }
    public required byte[] Payload { get; init; }
    public bool RelayEligible { get; init; } = true;
}

public sealed class MetadataIncomingBatch
{
    public required string ScopeId { get; init; }
    public required NodeId LocalReplica { get; init; }
    public required NodeId SenderPeer { get; init; }
    public required uint SenderEpoch { get; init; }
    public required int NamespaceId { get; init; }
    public required BatchId BatchId { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] PreviousBatchDigest { get; init; }
    public required byte[] BatchDigest { get; init; }
    public required IReadOnlyList<MetadataOperation> Operations { get; init; }
}

public sealed record MetadataBatchReceipt(ulong CursorStart, ulong CursorEnd, byte[] BatchDigest,
    int NewRelayOperationCount, bool IsDuplicate);
public sealed record MetadataRelayItem(ulong Sequence, NodeId OriginalAuthor, BatchId ChangeId, byte[] Payload);
public sealed record MetadataRelayPage(IReadOnlyList<MetadataRelayItem> Items, ulong ScannedThroughSequence);
public sealed class MetadataFrozenOperation
{
    public required BatchId ChangeId { get; init; }
    public required byte[] Payload { get; init; }
}
public sealed record MetadataOutboundItem(ulong Sequence, NodeId AuthorNodeId, BatchId ChangeId, byte[] Payload);
public sealed record MetadataOutboundPage(IReadOnlyList<MetadataOutboundItem> Items, ulong ScannedThroughSequence);
