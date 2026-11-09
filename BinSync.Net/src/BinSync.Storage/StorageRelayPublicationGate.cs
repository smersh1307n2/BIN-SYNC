using System.Runtime.CompilerServices;

namespace BinSync.Storage;

/// <summary>
/// Host-facing serialization boundary for FLD relay publication. One instance is shared
/// per StorageSyncCoordinator. It drains every pending source page for every relay
/// author before any namespace-wide sender callback can observe outbound payload.
/// </summary>
public sealed class StorageRelayPublicationGate
{
    private static readonly ConditionalWeakTable<StorageSyncCoordinator, StorageRelayPublicationGate> Gates = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _reconciliationRequired = true;

    private StorageRelayPublicationGate() { }

    /// <summary>Returns the one shared publication gate for this namespace coordinator.</summary>
    public static StorageRelayPublicationGate For(StorageSyncCoordinator coordinator)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        return Gates.GetValue(coordinator, static _ => new StorageRelayPublicationGate());
    }

    /// <summary>
    /// Serialize durable remote apply, all pending relay pages for all source authors,
    /// and namespace-wide downstream send. The host supplies the union of FLD's
    /// unacknowledged relay authors and the RPM3 map's existing author cursors.
    /// The page callback returns the source high watermark observed under the same
    /// FLD commit lease as the page read.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishAllAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<NodeId, StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
        Func<CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(coordinator, map, readRelayAuthors, acquireHostCommitLease, readSourcePage, publishReadyOutbox);
        ArgumentNullException.ThrowIfNull(applyRemoteBatch);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            try
            {
                await applyRemoteBatch(cancellationToken).ConfigureAwait(false);
                foreach (NodeId author in DiscoverAuthors(coordinator, acquireHostCommitLease, readRelayAuthors))
                {
                    if (!DrainAllPages(
                        coordinator, map, author, acquireHostCommitLease,
                        cursor => readSourcePage(author, cursor)))
                    {
                        _reconciliationRequired = true;
                        return false;
                    }
                }
            }
            catch
            {
                // APPLY may have become durable before a later callback/resolver failed.
                // Require a complete source rescan before any sender can run again.
                _reconciliationRequired = true;
                throw;
            }

            await publishReadyOutbox(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Startup/recovery path. Keep every namespace sender disabled while all author
    /// cursors are checked against the durable source high watermark and every remaining
    /// source page is resolved/committed. Only then invoke the namespace sender callback.
    /// </summary>
    public async ValueTask<bool> RecoverAndPublishAllAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<ulong> readSourceJournalHighWatermark,
        Func<NodeId, StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
        Func<CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(coordinator, map, readRelayAuthors, acquireHostCommitLease, readSourcePage, publishReadyOutbox);
        ArgumentNullException.ThrowIfNull(readSourceJournalHighWatermark);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                NodeId[] authors = DiscoverAuthors(coordinator, acquireHostCommitLease, readRelayAuthors);
                foreach (NodeId author in authors)
                {
                    coordinator.ValidateRelayProvenanceRecovery(
                        map, author, acquireHostCommitLease, readSourceJournalHighWatermark);
                    if (!DrainAllPages(
                        coordinator, map, author, acquireHostCommitLease,
                        cursor => readSourcePage(author, cursor)))
                    {
                        _reconciliationRequired = true;
                        return false;
                    }
                }
                _reconciliationRequired = false;
            }
            catch
            {
                _reconciliationRequired = true;
                throw;
            }

            await publishReadyOutbox(cancellationToken).ConfigureAwait(false);
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
            EnsureReconciled();
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
    /// gate. Do not call this from ApplyAndPublishAllAsync/RecoverAndPublishAllAsync callbacks.
    /// </summary>
    public async ValueTask RunOutboundAsync(
        Func<CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publish);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            await publish(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static void ValidateArguments(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<NodeId, StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
        Func<CancellationToken, ValueTask> publishReadyOutbox)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(readRelayAuthors);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(readSourcePage);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
    }

    private static NodeId[] DiscoverAuthors(
        StorageSyncCoordinator coordinator,
        Func<IDisposable?> acquireHostCommitLease,
        Func<IReadOnlyList<NodeId>> readRelayAuthors)
    {
        using IDisposable commitGuard =
            coordinator.AcquireApplicationCommitGuard(acquireHostCommitLease);
        IReadOnlyList<NodeId> discovered = readRelayAuthors()
            ?? throw new InvalidDataException("Host relay-author discovery returned null.");
        var seen = new HashSet<NodeId>();
        var authors = new List<NodeId>(discovered.Count);
        foreach (NodeId author in discovered)
        {
            if (author.IsZero || author == coordinator.LocalReplicaId)
                throw new InvalidDataException("Relay author discovery returned a zero or local author identity.");
            if (!seen.Add(author))
                throw new InvalidDataException("Relay author discovery returned a duplicate identity.");
            authors.Add(author);
        }
        authors.Sort(static (left, right) =>
            StringComparer.Ordinal.Compare(left.ToString(), right.ToString()));
        return authors.ToArray();
    }

    private void EnsureReconciled()
    {
        if (_reconciliationRequired)
            throw new InvalidOperationException(
                "Relay publication is closed until RecoverAndPublishAllAsync reconciles every durable FLD relay author and RPM3 cursor.");
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
