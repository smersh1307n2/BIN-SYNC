namespace BinSync;

/// <summary>
/// Optional durable per-peer replication progress capability. Implementations keep
/// one shared outbound payload stream and only compact peer-specific cursors and
/// the single in-flight immutable batch reservation.
/// </summary>
public interface IMetadataPeerStateStore
{
    IReadOnlyList<MetadataPeerStreamSnapshot> ReadPeerStreamStates(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId);

    /// <summary>
    /// Registers a peer as requiring this author stream. Registration is durable and
    /// fail-closed: a newly registered peer starts at the supplied proven baseline,
    /// or at cursor zero when no snapshot baseline is supplied.
    /// </summary>
    void RegisterInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId);
    IReadOnlyList<MetadataInterestedPeer> ReadInterestedPeers(
        string scopeId, NodeId localReplica, int namespaceId);
    bool IsPeerInitialSyncComplete(
        string scopeId, NodeId localReplica, NodeId peer, uint peerEpoch, int namespaceId);
    void CompletePeerInitialSync(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId,
        IReadOnlyList<MetadataPeerStreamBaseline> streamBaselines);
    uint? GetRetiredPeerEpoch(string scopeId, NodeId localReplica, NodeId peer, int namespaceId);

    /// <summary>
    /// Retires an older peer incarnation after a newer epoch has completed session
    /// establishment. The retired epoch is fenced against future reads and writes.
    /// </summary>
    /// <summary>
    /// Fence an old transport epoch and remove its peer-stream/baseline state. Receipt
    /// rows remain durable until the separate signed-frontier + host-reference GC path
    /// proves they can be reclaimed.
    /// </summary>
    void RetireInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint retiredPeerEpoch, uint replacementPeerEpoch, int namespaceId);

    void RegisterInterestedPeerStream(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        ulong baselineAckedCursor, ulong streamOffset, uint issuerEpoch, byte[] lastBatchDigest);

    /// <summary>
    /// Returns the minimum ACK cursor across every durably registered interested peer.
    /// Returns zero when no peer is registered or any required progress is unavailable.
    /// </summary>
    IReadOnlyList<NodeId> ReadOutboundAuthors(string scopeId, NodeId localReplica, int namespaceId);
    IReadOnlyList<NodeId> ReadRelayAuthors(string scopeId, NodeId localReplica, int namespaceId);

    /// <summary>Reads the local-origin outbound page after this peer's durable local cursor.</summary>
    MetadataOutboundPage ReadOutboundForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit);

    /// <summary>Reads the relay page after this peer's durable local cursor.</summary>
    MetadataRelayPage ReadRelayForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit);

    ulong GetSafePeerStreamCompactionCursor(string scopeId, NodeId localReplica,
        int namespaceId, NodeId authorStream);

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
    void AcknowledgePeerBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        BatchId batchId, ulong cursorEnd, byte[] batchDigest);
}

/// <summary>Compact durable progress for one peer/epoch/namespace/author stream.</summary>
public sealed record MetadataPeerStreamSnapshot(NodeId AuthorStream, MetadataPeerStreamState State);

public sealed record MetadataInterestedPeer(NodeId Peer, uint PeerEpoch, int NamespaceId);

/// <summary>
/// Per-author feed watermark captured before initial reconciliation. Outbound streams
/// may skip this local sequence only after the peer's complete snapshot baseline is
/// confirmed; relay streams must use offset zero because snapshots do not replace relay history.
/// </summary>
public sealed record MetadataPeerStreamBaseline(NodeId AuthorStream, ulong StreamOffset, uint IssuerEpoch);

public sealed record MetadataPeerStreamState(
    ulong AckedCursor,
    ulong PushedCursor,
    ulong StreamOffset,
    uint IssuerEpoch,
    byte[] LastBatchDigest,
    MetadataOutboundBatchReservation? PendingBatch);

/// <summary>An immutable encoded batch retained until its matching ACK is committed.</summary>
public sealed class MetadataOutboundBatchReservation
{
    public required BatchId BatchId { get; init; }
    public required uint IssuerEpoch { get; init; }
    public ulong StreamOffset { get; init; }
    public required ulong CursorStart { get; init; }
    public required ulong CursorEnd { get; init; }
    public required byte[] PreviousBatchDigest { get; init; }
    public required byte[] BatchDigest { get; init; }
    public required byte[] Payload { get; init; }
}
