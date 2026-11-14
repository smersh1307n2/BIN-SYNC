using BinSync;

namespace BinSync.Storage.Integration;

internal sealed class TestAsyncGcReferenceScanner(
    bool hasReceiptReference = false,
    bool hasChangeIdReference = false,
    bool hasRecordReference = false) : IAsyncApplicationGcReferenceScanner
{
    public ValueTask<IAsyncApplicationGcReferenceScan> OpenScanAsync(
        string scopeId, NodeId localReplica, int namespaceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero || namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IAsyncApplicationGcReferenceScan>(
            new Scan(hasReceiptReference, hasChangeIdReference, hasRecordReference));
    }

    private sealed class Scan(
        bool hasReceiptReference,
        bool hasChangeIdReference,
        bool hasRecordReference) : IAsyncApplicationGcReferenceScan
    {
        public ValueTask<ApplicationDraftHorizon> GetUnpublishedLocalDraftHorizonAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new ApplicationDraftHorizon(0, 0, 0));
        }

        public ValueTask<bool> HasExternalReceiptReferenceAsync(
            NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(hasReceiptReference);
        }

        public ValueTask<bool> HasExternalChangeIdReferenceAsync(
            BatchId changeId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(hasChangeIdReference);
        }

        public ValueTask<bool> HasExternalRecordReferenceAsync(
            RecordId recordId, BatchId tombstoneChangeId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(hasRecordReference);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
