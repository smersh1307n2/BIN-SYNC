namespace BinSync;

/// <summary>
/// Optional durable per-peer replication progress capability. Implementations keep
/// one shared outbound payload stream and only compact peer-specific cursors and
/// the single in-flight immutable batch reservation.
/// </summary>
public interface IMetadataPeerStateStore
{
    MetadataPeerStreamState GetPeerStreamState(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream);

    MetadataOutboundBatchReservation? GetPendingOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream);

    /// <summary>
    /// Durably reserves the exact encoded DELTA_BATCH payload before transport send.
    /// A retry for the same stream returns the existing reservation byte-for-byte;
    /// a conflicting reservation is rejected.
    /// </summary>
    MetadataOutboundBatchReservation ReserveOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        MetadataOutboundBatchReservation reservation);

    /// <summary>Records that the reserved payload was handed to the reliable transport.</summary>
    void MarkOutboundBatchPushed(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, BatchId batchId);

    /// <summary>
    /// Commits the matching peer ACK and digest checkpoint, then releases the in-flight
    /// reservation atomically. ACKs without a matching pushed reservation fail closed.
    /// </summary>
    void AcknowledgeOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        BatchId batchId, ulong cursorEnd, byte[] batchDigest);
}

/// <summary>Compact durable progress for one peer/epoch/namespace/author stream.</summary>
public sealed record MetadataPeerStreamState(
    ulong AckedCursor,
    ulong PushedCursor,
    ulong StreamOffset,
    byte[] LastBatchDigest,
    MetadataOutboundBatchReservation? PendingBatch);

/// <summary>An immutable encoded batch retained until its matching ACK is committed.</summary>
public sealed class MetadataOutboundBatchReservation
{
    public required BatchId BatchId { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] PreviousBatchDigest { get; init; }
    public required byte[] BatchDigest { get; init; }
    public required byte[] Payload { get; init; }
}
