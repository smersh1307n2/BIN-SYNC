namespace BinSync;

/// <summary>
/// Asynchronous counterpart of IProviderSnapshotStateApply. Snapshot rows are
/// authenticated materialized state, not DELTA_BATCH operations.
/// </summary>
public interface IAsyncProviderSnapshotStateApply
{
    ValueTask<ProviderApplyReceipt> ApplySnapshotBatchAsync(
        Guid snapshotToken, IReadOnlyList<ProviderMutation> mutations,
        CancellationToken cancellationToken = default);
}

/// <summary>Asynchronous durable publication-cutover capability.</summary>
public interface IAsyncProviderPublicationMode
{
    ValueTask<(bool Exists, ulong NodeOutboxHighWatermark)> TryGetMetadataStoreOwnedPublicationCutoverAsync(
        CancellationToken cancellationToken = default);
    ValueTask EnableMetadataStoreOwnedPublicationAsync(
        ulong nodeOutboxHighWatermark, CancellationToken cancellationToken = default);
}

/// <summary>
/// Optional IPC receipt lifecycle for providers with durable local-operation receipts.
/// The caller must persist the returned sequence in its own durable commit before
/// acknowledging that receipt prefix. This ACK is independent of source-feed/Outbox ACK.
/// ClientSequence is contiguous per stable provider ClientId/receipt stream, not per
/// ChangeId. A receipt stream must be dedicated to one BIN-SYNC namespace/coordinator;
/// do not share one FLD ClientId across namespace coordinators because the Node WAL
/// currently scopes its compact receipt mapping by namespace. Reconnects reuse the
/// same ClientId so ChangeId-derived append retries recover the original sequence.
/// </summary>
public interface IAsyncProviderOperationReceiptStore
{
    /// <summary>Stable FLD ClientId whose ClientSequence namespace this provider exposes.</summary>
    Guid ReceiptStreamId { get; }

    ValueTask<ulong> AppendLocalChangeWithReceiptAsync(
        ProviderMutation mutation, CancellationToken cancellationToken = default);
    ValueTask AcknowledgeOperationReceiptsAsync(
        ulong throughSequence, CancellationToken cancellationToken = default);
}

/// <summary>Asynchronous immutable-origin stamping capability.</summary>
public interface IAsyncProviderPublishedOriginStore
{
    ValueTask CommitPublishedOriginsAsync(
        IReadOnlyList<ProviderPublishedOrigin> published,
        CancellationToken cancellationToken = default);
}

/// <summary>Asynchronous durable author-epoch lifecycle capability.</summary>
public interface IAsyncProviderAuthorEpochClosureStore
{
    ValueTask<bool> TryCloseLocalAuthorEpochAsync(
        uint authorEpoch, CancellationToken cancellationToken = default);
    ValueTask CloseLocalAuthorIdentityAsync(
        uint finalAuthorEpoch, CancellationToken cancellationToken = default);
    ValueTask ActivateLocalAuthorEpochAsync(
        uint authorEpoch, CancellationToken cancellationToken = default);
}

/// <summary>Asynchronous authenticated authoritative FULL_REPLACE capability.</summary>
public interface IAsyncProviderFullReplace
{
    ValueTask BeginFullReplaceAsync(
        Guid generation, ulong sourceChangeFeedWatermark, int expectedEntryCount,
        ReadOnlyMemory<byte> expectedDigest, CancellationToken cancellationToken = default);
    ValueTask StageFullReplacePageAsync(
        Guid generation, int startIndex, IReadOnlyList<ProviderMutation> entries,
        CancellationToken cancellationToken = default);
    ValueTask CommitFullReplaceAsync(
        Guid generation, CancellationToken cancellationToken = default);
    ValueTask<bool> AbortFullReplaceAsync(
        Guid generation, int limit, CancellationToken cancellationToken = default);
    ValueTask<(bool Exists, ProviderFullReplaceProgress? Progress)> TryGetFullReplaceProgressAsync(
        Guid generation, CancellationToken cancellationToken = default);
}

/// <summary>Asynchronous durable provider snapshot recovery and retention capability.</summary>
public interface IAsyncProviderDurableSnapshotStore
{
    ValueTask<bool> HasPersistedSnapshotAsync(
        Guid snapshotToken, CancellationToken cancellationToken = default);
    ValueTask<(bool Exists, ulong ChangeFeedWatermark)> TryGetPersistedSnapshotWatermarkAsync(
        Guid snapshotToken, CancellationToken cancellationToken = default);
    ValueTask PruneSnapshotsExceptAsync(
        IReadOnlySet<Guid> activeSnapshotTokens, CancellationToken cancellationToken = default);
}

/// <summary>
/// Async-disposable provider commit lease. Implementations must release on DisposeAsync
/// and must be safe to hold across awaited provider IPC calls.
/// </summary>
public interface IAsyncProviderCommitGuard
{
    ValueTask<IAsyncDisposable> AcquireCommitGuardAsync(
        CancellationToken cancellationToken = default);
}

/// <summary>Async counterpart of the snapshot-GC pin guard; null means a snapshot is active.</summary>
public interface IAsyncProviderSnapshotGcGuard
{
    ValueTask<IAsyncDisposable?> TryAcquireSnapshotGcGuardAsync(
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Asynchronous host-owned GC reference scanner. OpenScanAsync returns one stable,
/// partition-scoped snapshot; all reads must use that pinned view until disposal.
/// This capability does not authorize enabling physical GC.
/// </summary>
public interface IAsyncApplicationGcReferenceScanner
{
    ValueTask<IAsyncApplicationGcReferenceScan> OpenScanAsync(
        string scopeId, NodeId localReplica, int namespaceId,
        CancellationToken cancellationToken = default);
}

public interface IAsyncApplicationGcReferenceScan : IAsyncDisposable
{
    ValueTask<ApplicationDraftHorizon> GetUnpublishedLocalDraftHorizonAsync(
        CancellationToken cancellationToken = default);
    ValueTask<bool> HasExternalReceiptReferenceAsync(
        NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId,
        CancellationToken cancellationToken = default);
    ValueTask<bool> HasExternalChangeIdReferenceAsync(
        BatchId changeId, CancellationToken cancellationToken = default);
    ValueTask<bool> HasExternalRecordReferenceAsync(
        RecordId recordId, BatchId tombstoneChangeId,
        CancellationToken cancellationToken = default);
}
