namespace BinSync;

/// <summary>
/// Production source-data boundary. A provider owns application records and
/// exposes durable local changes plus idempotent remote application.
/// </summary>
public interface IDataProvider : IDisposable
{
    RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey);
    ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey);

    void AppendLocalChange(ProviderMutation mutation);
    ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit);
    void AcknowledgeLocalChanges(ulong safeThroughSequence);
    ulong LocalChangeHighWatermark { get; }

    ProviderSnapshotHandle OpenSnapshot();
    ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit);
    void ReleaseSnapshot(Guid snapshotToken);

    /// <summary>
    /// Applies an immutable remote batch idempotently to the provider's current
    /// application state. This operation MUST NOT append to, rewrite, or acknowledge
    /// the provider's local unpublished-change feed. A remote LWW winner may replace
    /// the visible current value while an older local intent remains pending; that
    /// intent must stay independently readable until the publication coordinator
    /// freezes it and explicitly acknowledges its source-feed sequence.
    /// </summary>
    ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations);
    bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation);
}

/// <summary>
/// Optional durable capability for a provider-authoritative outbound cutover.
/// The stored watermark fences later direct Node writes after Metadata Store
/// becomes the sole allocator for new local outbound sequences.
/// </summary>
public interface IProviderPublicationMode
{
    bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark);
    void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark);
}

/// <summary>
/// Optional provider capability for an authenticated authoritative FULL_REPLACE.
/// The caller validates the roster/snapshot certificate, serializes the switch against
/// remote APPLY, and proves no unresolved PendingApply, relay reservation, provider
/// intent or reader pin depends on the old generation before invoking this API.
/// Staging is invisible until CommitFullReplace atomically switches the active
/// generation. Old generations are retained; this API does not authorize GC.
/// </summary>
public interface IProviderFullReplace
{
    void BeginFullReplace(Guid generation, ulong sourceChangeFeedWatermark,
        int expectedEntryCount, ReadOnlyMemory<byte> expectedDigest);
    void StageFullReplacePage(Guid generation, int startIndex,
        IReadOnlyList<ProviderMutation> entries);
    void CommitFullReplace(Guid generation);
    bool AbortFullReplace(Guid generation, int limit);
    bool TryGetFullReplaceProgress(Guid generation, out ProviderFullReplaceProgress? progress)
    {
        progress = null;
        return false;
    }
}

public sealed record ProviderFullReplaceProgress(
    Guid Generation, ulong SourceChangeFeedWatermark, int ExpectedEntryCount,
    int NextIndex, byte[] ExpectedDigest, bool IsCommitted);

public sealed class ProviderMutation
{
    public required RecordId RecordId { get; init; }
    public required ushort FieldId { get; init; }
    public bool IsTombstone { get; init; }
    /// <summary>True for a record lifecycle tombstone; FieldId must be the reserved lifecycle id.</summary>
    public bool IsRecordTombstone { get; init; }
    public ulong HlcWall { get; init; }
    public uint HlcLogical { get; init; }
    public required NodeId AuthorNodeId { get; init; }
    public ushort KeyEpoch { get; init; }
    public required byte[] Value { get; init; }
    /// <summary>Stable global identity of the immutable source operation; zero only for legacy materialized rows.</summary>
    public BatchId ChangeId { get; init; }
    /// <summary>Immutable origin author incarnation; zero only for legacy materialized rows.</summary>
    public uint OriginAuthorEpoch { get; init; }
    /// <summary>Immutable origin sequence; zero only for legacy materialized rows.</summary>
    public ulong OriginSequence { get; init; }
    public OperationOrigin Provenance
    {
        get => AuthorNodeId.IsZero || OriginAuthorEpoch == 0 || OriginSequence == 0
            ? default : new OperationOrigin(AuthorNodeId, OriginAuthorEpoch, OriginSequence);
        init
        {
            AuthorNodeId = value.AuthorNodeId;
            OriginAuthorEpoch = value.AuthorEpoch;
            OriginSequence = value.Sequence;
        }
    }
}

public sealed record ProviderChange(ulong Sequence, ProviderMutation Mutation)
{
    /// <summary>
    /// Stable 128-bit identity of this durable local-feed entry. It must remain
    /// unchanged when the same sequence is read again after interruption/reopen.
    /// Zero denotes a legacy provider feed that has not yet persisted source identities.
    /// </summary>
    public BatchId ChangeId { get; init; }
}
public sealed record ProviderChangePage(IReadOnlyList<ProviderChange> Changes, ulong ScannedThroughSequence);

/// <summary>One local feed entry paired with its immutable Metadata Store OperationOrigin.</summary>
public sealed record ProviderPublishedOrigin(ProviderChange Change, OperationOrigin Origin);

/// <summary>
/// Optional provider capability for atomically stamping the current-state projection
/// with the Origin assigned by immutable publication. Feed sequence is never OriginSequence.
/// </summary>
public interface IProviderPublishedOriginStore
{
    void CommitPublishedOrigins(IReadOnlyList<ProviderPublishedOrigin> published);
}

/// <summary>
/// Host-owned live-reference scan required before physical ChangeId/tombstone GC.
/// Implementations inspect WAL/FLD outbox, pending application work and other
/// durable references outside BIN-SYNC's own Storage database. Call only while
/// the caller's composed application commit lease is held; exceptions fail closed.
/// </summary>
public readonly record struct ApplicationDraftHorizon(
    ulong Count, ulong MaxHlcWall, uint MaxHlcLogical);

/// <summary>
/// Host-owned factory for one stable, partition-scoped reference snapshot. Call only
/// while the composed application commit lease is held. Implementations should scan
/// each durable host source once and index candidates; per-candidate rescans are
/// needlessly quadratic for bounded GC batches.
/// </summary>
public interface IApplicationGcReferenceScanner
{
    IApplicationGcReferenceScan OpenScan(
        string scopeId, NodeId localReplica, int namespaceId);
}

/// <summary>
/// Stable host-side view for one GC/frontier operation. Dispose releases any pinned
/// host snapshots or scan resources. Exceptions fail closed.
/// </summary>
public interface IApplicationGcReferenceScan : IDisposable
{
    ApplicationDraftHorizon UnpublishedLocalDraftHorizon { get; }

    bool HasExternalReceiptReference(
        NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId);

    bool HasExternalChangeIdReference(BatchId changeId);

    bool HasExternalRecordReference(RecordId recordId, BatchId tombstoneChangeId);
}

/// <summary>Durable provider capability for terminally closing one local author epoch.</summary>
public interface IProviderAuthorEpochClosureStore
{
    /// <summary>Closes the epoch only when no unpublished feed entries for it remain.</summary>
    bool TryCloseLocalAuthorEpoch(uint authorEpoch);
    void CloseLocalAuthorIdentity(uint finalAuthorEpoch);
    void ActivateLocalAuthorEpoch(uint authorEpoch);
}

/// <summary>Provider-wide exclusive lease used to serialize FULL_REPLACE commit and local GC.</summary>
public interface IProviderCommitGuard
{
    IDisposable AcquireCommitGuard();
}

public sealed record ProviderSnapshotHandle(Guid Token, ulong ChangeFeedWatermark);
public sealed record ProviderSnapshotPage(IReadOnlyList<ProviderMutation> Entries, int NextIndex, bool IsComplete);
public sealed record ProviderApplyReceipt(Guid ApplyToken, byte[] BatchFingerprint, bool IsDuplicate);

/// <summary>
/// Full, transport-neutral remote batch context delivered to providers that persist a
/// source-specific apply journal. Sender/transport epoch are distinct from the wire
/// issuer/author epoch; cursor and digest-chain values are the received immutable batch
/// coordinates. Current v3 DELTA_BATCH has no separate protocol-state-delta field, so
/// that payload is empty unless a future negotiated contract supplies one.
/// </summary>
public sealed class ProviderRemoteBatchCommit
{
    public Guid ApplyToken { get; }
    public NodeId SenderPeer { get; }
    public uint SenderTransportEpoch { get; }
    public int NamespaceId { get; }
    public NodeId IssuerNodeId { get; }
    public uint IssuerEpoch { get; }
    public BatchId BatchId { get; }
    public ulong CursorStart { get; }
    public ulong CursorEnd { get; }
    public byte[] PreviousBatchDigest { get; }
    public byte[] BatchDigest { get; }
    public byte[] RecordsRoot { get; }
    public byte[] ProtocolStateDelta { get; }
    public IReadOnlyList<ProviderMutation> Mutations { get; }

    public ProviderRemoteBatchCommit(
        Guid applyToken,
        NodeId senderPeer,
        uint senderTransportEpoch,
        int namespaceId,
        NodeId issuerNodeId,
        uint issuerEpoch,
        BatchId batchId,
        ulong cursorStart,
        ulong cursorEnd,
        ReadOnlySpan<byte> previousBatchDigest,
        ReadOnlySpan<byte> batchDigest,
        ReadOnlySpan<byte> recordsRoot,
        ReadOnlySpan<byte> protocolStateDelta,
        IReadOnlyList<ProviderMutation> mutations)
    {
        if (applyToken == Guid.Empty) throw new ArgumentException("Apply token must be non-empty.", nameof(applyToken));
        if (senderPeer.IsZero) throw new ArgumentException("Sender peer must be non-zero.", nameof(senderPeer));
        if (issuerNodeId.IsZero) throw new ArgumentException("Batch issuer must be non-zero.", nameof(issuerNodeId));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (issuerEpoch == 0) throw new ArgumentOutOfRangeException(nameof(issuerEpoch));
        if (batchId.IsZero) throw new ArgumentException("Batch identity must be non-zero.", nameof(batchId));
        if (cursorStart == 0 || cursorEnd < cursorStart)
            throw new ArgumentOutOfRangeException(nameof(cursorStart), "Remote batch cursor range is invalid.");
        if (previousBatchDigest.Length != 16 || batchDigest.Length != 16 || recordsRoot.Length != 16)
            throw new ArgumentException("v3 batch digests must be exactly 16 bytes.");
        if (protocolStateDelta.Length > 4 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(protocolStateDelta));
        ArgumentNullException.ThrowIfNull(mutations);
        if (mutations.Count == 0 || mutations.Count > 4096 ||
            checked(cursorEnd - cursorStart + 1) != (ulong)mutations.Count)
            throw new ArgumentException("Remote batch mutation count must match its bounded cursor range.", nameof(mutations));

        var owned = new ProviderMutation[mutations.Count];
        var seen = new HashSet<BatchId>();
        for (int i = 0; i < mutations.Count; i++)
        {
            ProviderMutation mutation = mutations[i]
                ?? throw new ArgumentException("Remote batch contains a null mutation.", nameof(mutations));
            ProviderMutationCodec.ValidatePublished(mutation);
            if (!seen.Add(mutation.ChangeId))
                throw new ArgumentException("Remote batch contains duplicate ChangeId values.", nameof(mutations));
            owned[i] = new ProviderMutation
            {
                RecordId = mutation.RecordId,
                FieldId = mutation.FieldId,
                IsTombstone = mutation.IsTombstone,
                IsRecordTombstone = mutation.IsRecordTombstone,
                HlcWall = mutation.HlcWall,
                HlcLogical = mutation.HlcLogical,
                AuthorNodeId = mutation.AuthorNodeId,
                KeyEpoch = mutation.KeyEpoch,
                Value = mutation.Value.ToArray(),
                ChangeId = mutation.ChangeId,
                OriginAuthorEpoch = mutation.OriginAuthorEpoch,
                OriginSequence = mutation.OriginSequence
            };
        }

        ApplyToken = applyToken;
        SenderPeer = senderPeer;
        SenderTransportEpoch = senderTransportEpoch;
        NamespaceId = namespaceId;
        IssuerNodeId = issuerNodeId;
        IssuerEpoch = issuerEpoch;
        BatchId = batchId;
        CursorStart = cursorStart;
        CursorEnd = cursorEnd;
        PreviousBatchDigest = previousBatchDigest.ToArray();
        BatchDigest = batchDigest.ToArray();
        RecordsRoot = recordsRoot.ToArray();
        ProtocolStateDelta = protocolStateDelta.ToArray();
        Mutations = Array.AsReadOnly(owned);
    }
}

/// <summary>
/// Optional provider capability for a full remote-apply journal. Adapters that
/// implement this interface receive the complete immutable batch header instead of
/// only the lossy (ApplyToken, mutations) projection. BIN-SYNC falls back to
/// IDataProvider.ApplyRemoteBatch for providers that do not need the wider context.
/// </summary>
public interface IProviderRemoteBatchCommitStore
{
    ProviderApplyReceipt ApplyRemoteBatch(ProviderRemoteBatchCommit batch);
}
