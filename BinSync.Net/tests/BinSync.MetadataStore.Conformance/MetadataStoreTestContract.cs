using BinSync;

namespace BinSync.MetadataStore.Conformance;

/// <summary>
/// Test-only adapter contract. This is not the production Metadata Store API.
/// A real store integration supplies a small adapter that maps these semantic
/// operations to its implementation, then derives from MetadataStoreConformanceSuite.
/// </summary>
public interface IMetadataStoreConformanceAdapter : IDisposable
{
    /// <summary>False for the in-memory reference adapter; durable adapters must add restart tests.</summary>
    bool SupportsRestartDurability { get; }

    MetadataBatchReceipt AcceptIncomingBatch(MetadataIncomingBatch batch);

    IReadOnlyList<MetadataOutboundItem> FreezeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations);

    ulong AcquireOutboundLease(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        Guid consumerId);

    MetadataOutboundPage ReadOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong afterSequence,
        int limit);

    void AcknowledgeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong safeThroughSequence);

    ulong GetOutboundHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor);

    MetadataRelayPage ReadRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong afterSequence,
        int limit);

    void AcknowledgeRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence);

    ulong GetReceiveCursor(
        string scopeId,
        NodeId localReplica,
        NodeId sender,
        uint senderEpoch,
        int namespaceId);

    ulong GetRelayHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor);
}

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

public sealed record MetadataBatchReceipt(
    ulong CursorStart,
    ulong CursorEnd,
    byte[] BatchDigest,
    int NewRelayOperationCount,
    bool IsDuplicate);

public sealed record MetadataRelayItem(
    ulong Sequence,
    NodeId OriginalAuthor,
    BatchId ChangeId,
    byte[] Payload);

public sealed record MetadataRelayPage(
    IReadOnlyList<MetadataRelayItem> Items,
    ulong ScannedThroughSequence);


public sealed class MetadataFrozenOperation
{
    public required BatchId ChangeId { get; init; }
    public required byte[] Payload { get; init; }
}

public sealed record MetadataOutboundItem(
    ulong Sequence,
    NodeId AuthorNodeId,
    BatchId ChangeId,
    byte[] Payload);

public sealed record MetadataOutboundPage(
    IReadOnlyList<MetadataOutboundItem> Items,
    ulong ScannedThroughSequence);
