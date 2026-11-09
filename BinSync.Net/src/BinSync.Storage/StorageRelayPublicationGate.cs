using System.Runtime.CompilerServices;

namespace BinSync.Storage;

/// <summary>
/// Host-facing serialization boundary for FLD relay publication. The host must route
/// every outbound batch construction/send for the coordinated namespace through this
/// gate; a source page is scanned and its RPM3 mapping committed before the publish
/// callback can observe the Outbox. The same gate serializes safe-ACK translation and
/// FLD ACK + map pruning against publication.
/// </summary>
public sealed class StorageRelayPublicationGate
{
    private static readonly ConditionalWeakTable<StorageSyncCoordinator, StorageRelayPublicationGate> Gates = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    private StorageRelayPublicationGate() { }

    /// <summary>Returns the one shared publication gate for this namespace coordinator.</summary>
    public static StorageRelayPublicationGate For(StorageSyncCoordinator coordinator)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        return Gates.GetValue(coordinator, static _ => new StorageRelayPublicationGate());
    }

    /// <summary>
    /// Serialize remote apply, source-page resolution/persistence and the subsequent
    /// outbound send as one publication boundary. The apply callback must durably apply
    /// one remote batch to the host database; no sender may observe its relay Outbox
    /// entries until the source mapping has committed and the publish callback runs.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishPageAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<(StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq)> readSourcePage,
        Func<NodeId, CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(applyRemoteBatch);
        ArgumentNullException.ThrowIfNull(readSourcePage);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await applyRemoteBatch(cancellationToken).ConfigureAwait(false);
            bool committed = coordinator.ReadAndAppendRelayProvenancePage(
                map, originalAuthor, acquireHostCommitLease, readSourcePage);
            if (!committed)
                return false;

            await publishReadyOutbox(originalAuthor, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Startup/recovery path: validate the persisted RPM3 cursor against the durable
    /// source JournalSeq high watermark, then reconcile and publish the next page while
    /// the namespace sender remains serialized behind this gate.
    /// </summary>
    public async ValueTask<bool> RecoverAndPublishPageAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<ulong> readSourceJournalHighWatermark,
        Func<(StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq)> readSourcePage,
        Func<NodeId, CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(readSourceJournalHighWatermark);
        ArgumentNullException.ThrowIfNull(readSourcePage);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            coordinator.ValidateRelayProvenanceRecovery(
                map, originalAuthor, acquireHostCommitLease, readSourceJournalHighWatermark);
            bool committed = coordinator.ReadAndAppendRelayProvenancePage(
                map, originalAuthor, acquireHostCommitLease, readSourcePage);
            if (!committed)
                return false;

            await publishReadyOutbox(originalAuthor, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Read one whole FLD relay page, resolve all ChangeIds, persist its RPM3 cursor,
    /// and only then allow the host to publish/send ready Outbox entries. If resolution
    /// fails because a source ChangeId is no longer retained, the callback is not run.
    /// </summary>
    public async ValueTask<bool> PublishPageAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<(StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq)> readSourcePage,
        Func<NodeId, CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(readSourcePage);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool committed = coordinator.ReadAndAppendRelayProvenancePage(
                map, originalAuthor, acquireHostCommitLease, readSourcePage);
            if (!committed)
                return false;

            await publishReadyOutbox(originalAuthor, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Compute the downstream-safe author Outbox watermark, durably ACK the corresponding
    /// FLD JournalSeq prefix, then prune RPM3 rows. The host callback must call
    /// FldDatabase.AcknowledgeRelayBatches and must be idempotent across crash/retry.
    /// </summary>
    public async ValueTask<ulong> AcknowledgeAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Action<ulong> acknowledgeSource,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(acknowledgeSource);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return coordinator.AcknowledgeRelayProvenance(
                map, originalAuthor, acquireHostCommitLease, acknowledgeSource);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Run any other outbound batch construction/send under the same host publication
    /// gate. Do not call this from PublishPageAsync/AcknowledgeAsync callbacks.
    /// </summary>
    public async ValueTask RunOutboundAsync(
        Func<CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publish);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await publish(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }
}
