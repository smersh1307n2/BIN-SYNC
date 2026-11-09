using System.Runtime.CompilerServices;

namespace BinSync.Storage;

/// <summary>
/// Host-facing serialization boundary for FLD relay publication. One instance is shared
/// per StorageSyncCoordinator. It drains every complete source page and commits all RPM3
/// mappings through the durable source high watermark before any sender callback runs.
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
    /// Serialize durable remote apply, every pending relay-page resolution/map commit,
    /// and downstream send. The read callback receives the current RPM3 cursor and must
    /// return one whole JournalSeq-bounded page plus the source journal high watermark
    /// observed under the same FLD commit lease.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
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
            bool committed = DrainAllPages(
                coordinator, map, originalAuthor, acquireHostCommitLease, readSourcePage);
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
    /// Startup/recovery path. Keep every sender disabled while the persisted RPM3 cursor
    /// is checked against the durable source high watermark and all remaining pages are
    /// resolved/committed. Only then invoke the sender callback.
    /// </summary>
    public async ValueTask<bool> RecoverAndPublishAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<ulong> readSourceJournalHighWatermark,
        Func<StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
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
            bool committed = DrainAllPages(
                coordinator, map, originalAuthor, acquireHostCommitLease, readSourcePage);
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
    /// Compute the downstream-safe author sequence, durably ACK the corresponding FLD
    /// JournalSeq prefix, then prune RPM3 rows. The host callback must call
    /// FldDatabase.AcknowledgeRelayBatches and be idempotent across crash/retry.
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
    /// gate. Do not call this from ApplyAndPublishAsync/RecoverAndPublishAsync callbacks.
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

    private static bool DrainAllPages(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage)
    {
        while (true)
        {
            using IDisposable commitGuard =
                coordinator.AcquireApplicationCommitGuard(acquireHostCommitLease);
            StorageRelayProvenanceCursor prior = map.ReadCursor(
                coordinator.NamespaceId, originalAuthor);
            var page = readSourcePage(prior);
            if (page.Changes is null)
                throw new InvalidDataException("Host relay page callback returned a null change array.");
            if (page.ScannedThroughJournalSeq < prior.ScannedThroughJournalSeq ||
                page.ScannedThroughJournalSeq > page.SourceHighWatermark)
                throw new InvalidDataException("Host relay page cursor is outside the durable source journal range.");

            bool progressed = page.ScannedThroughJournalSeq > prior.ScannedThroughJournalSeq;
            if (!progressed && page.SourceHighWatermark > prior.ScannedThroughJournalSeq)
                throw new InvalidDataException("Host relay page made no JournalSeq progress while source history remains.");

            bool committed = coordinator.AppendRelayProvenancePage(
                map, originalAuthor, page.Changes, page.ScannedThroughJournalSeq);
            if (!committed)
                return false;

            if (page.ScannedThroughJournalSeq == page.SourceHighWatermark)
                return true;
        }
    }
}
