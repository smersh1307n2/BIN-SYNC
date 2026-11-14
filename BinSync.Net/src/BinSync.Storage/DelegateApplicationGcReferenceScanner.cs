namespace BinSync.Storage;

/// <summary>
/// Adapts an application-owned immutable reference snapshot to BIN-SYNC's GC contract
/// without introducing a dependency on the application's database or domain types.
/// The caller must hold the composed application commit lease before opening a scan.
/// The factory opens one immutable host snapshot under that lease; the returned scan
/// owns only the snapshot and must not reacquire the host commit gate.
/// </summary>
public sealed class DelegateApplicationGcReferenceScanner : IApplicationGcReferenceScanner
{
    private readonly Func<string, NodeId, int, IApplicationGcReferenceScan> _openScan;

    public DelegateApplicationGcReferenceScanner(
        Func<string, NodeId, int, IApplicationGcReferenceScan> openScan)
    {
        _openScan = openScan ?? throw new ArgumentNullException(nameof(openScan));
    }

    public IApplicationGcReferenceScan OpenScan(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        return _openScan(scopeId, localReplica, namespaceId)
            ?? throw new InvalidOperationException("The host reference-snapshot factory returned null.");
    }
}

/// <summary>
/// Owns one immutable host snapshot while adapting its pre-indexed reference
/// queries to BIN-SYNC. The caller owns the composed application commit lease and
/// must keep it held until this scan has been disposed.
/// </summary>
public sealed class DelegateApplicationGcReferenceScan : IApplicationGcReferenceScan
{
    private IDisposable? _snapshot;
    private readonly Func<NodeId, uint, uint, BatchId, bool> _hasReceipt;
    private readonly Func<BatchId, bool> _hasChangeId;
    private readonly Func<RecordId, BatchId, bool> _hasRecord;

    public DelegateApplicationGcReferenceScan(
        IDisposable snapshot,
        ApplicationDraftHorizon unpublishedLocalDraftHorizon,
        Func<NodeId, uint, uint, BatchId, bool> hasReceipt,
        Func<BatchId, bool> hasChangeId,
        Func<RecordId, BatchId, bool> hasRecord)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(hasReceipt);
        ArgumentNullException.ThrowIfNull(hasChangeId);
        ArgumentNullException.ThrowIfNull(hasRecord);
        if (unpublishedLocalDraftHorizon.Count == 0 &&
            (unpublishedLocalDraftHorizon.MaxHlcWall != 0 ||
             unpublishedLocalDraftHorizon.MaxHlcLogical != 0))
            throw new ArgumentException("An empty draft horizon cannot have a non-zero HLC maximum.",
                nameof(unpublishedLocalDraftHorizon));

        _snapshot = snapshot;
        UnpublishedLocalDraftHorizon = unpublishedLocalDraftHorizon;
        _hasReceipt = hasReceipt;
        _hasChangeId = hasChangeId;
        _hasRecord = hasRecord;
    }

    public ApplicationDraftHorizon UnpublishedLocalDraftHorizon { get; }

    public bool HasExternalReceiptReference(
        NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId)
    {
        ThrowIfDisposed();
        return _hasReceipt(senderPeer, authorEpoch, transportPeerEpoch, batchId);
    }

    public bool HasExternalChangeIdReference(BatchId changeId)
    {
        ThrowIfDisposed();
        return _hasChangeId(changeId);
    }

    public bool HasExternalRecordReference(RecordId recordId, BatchId tombstoneChangeId)
    {
        ThrowIfDisposed();
        return _hasRecord(recordId, tombstoneChangeId);
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _snapshot, null)?.Dispose();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_snapshot is null, this);
    }
}
