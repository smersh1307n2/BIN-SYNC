using System.Buffers.Binary;
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
    /// The sender callback receives a durable per-author sequence ceiling and must
    /// never expose an outbound item above that ceiling. This prevents a concurrent
    /// FLD local write from escaping before its provenance page is committed.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishAllAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<NodeId, StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>, CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(coordinator, map, readRelayAuthors, acquireHostCommitLease, readSourcePage, publishReadyOutbox);
        ArgumentNullException.ThrowIfNull(applyRemoteBatch);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            NodeId[] authors;
            try
            {
                await applyRemoteBatch(cancellationToken).ConfigureAwait(false);
                authors = DiscoverAuthors(coordinator, map, acquireHostCommitLease, readRelayAuthors);
                foreach (NodeId author in authors)
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

            StorageRelayAuthorPublicationWatermark[] publicationWatermarks = authors
                .Select(author => map.ReadPublicationWatermark(coordinator.NamespaceId, author))
                .ToArray();
            await publishReadyOutbox(publicationWatermarks, cancellationToken).ConfigureAwait(false);
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
    /// source page is resolved/committed. The sender receives the resulting per-author
    /// sequence ceilings and must cap all outbound reads at those values.
    /// </summary>
    public async ValueTask<bool> RecoverAndPublishAllAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<ulong> readSourceJournalHighWatermark,
        Func<NodeId, StorageRelayProvenanceCursor,
            (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq, ulong SourceHighWatermark)> readSourcePage,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>, CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(coordinator, map, readRelayAuthors, acquireHostCommitLease, readSourcePage, publishReadyOutbox);
        ArgumentNullException.ThrowIfNull(readSourceJournalHighWatermark);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            NodeId[] authors;
            try
            {
                authors = DiscoverAuthors(coordinator, map, acquireHostCommitLease, readRelayAuthors);
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

            StorageRelayAuthorPublicationWatermark[] publicationWatermarks = authors
                .Select(author => map.ReadPublicationWatermark(coordinator.NamespaceId, author))
                .ToArray();
            await publishReadyOutbox(publicationWatermarks, cancellationToken).ConfigureAwait(false);
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
    /// Translate and durably apply the safe source ACK for every author that has either
    /// unacknowledged FLD relay rows or a retained RPM3 cursor. A partial failure is
    /// retryable: already-pruned authors disappear from the map; the remaining authors
    /// are rediscovered on the next call.
    /// </summary>
    public async ValueTask<IReadOnlyList<(NodeId Author, ulong SafeJournalSeq)>> AcknowledgeAllAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Action<NodeId, ulong> acknowledgeSource,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(readRelayAuthors);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(acknowledgeSource);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            NodeId[] authors = DiscoverAuthors(coordinator, map, acquireHostCommitLease, readRelayAuthors);
            var acknowledgements = new List<(NodeId Author, ulong SafeJournalSeq)>(authors.Length);
            foreach (NodeId author in authors)
            {
                ulong safeJournal = coordinator.AcknowledgeRelayProvenance(
                    map, author, acquireHostCommitLease,
                    sequence => acknowledgeSource(author, sequence));
                acknowledgements.Add((author, safeJournal));
            }
            return acknowledgements;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Run another outbound send under the same host publication gate, passing the
    /// current durable per-author publication ceilings. Do not call this from the
    /// ApplyAndPublishAllAsync/RecoverAndPublishAllAsync callbacks.
    /// </summary>
    public async ValueTask RunOutboundAsync(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        Func<IReadOnlyList<NodeId>> readRelayAuthors,
        Func<IDisposable?> acquireHostCommitLease,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>, CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(readRelayAuthors);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(publish);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            NodeId[] authors = DiscoverAuthors(coordinator, map, acquireHostCommitLease, readRelayAuthors);
            StorageRelayAuthorPublicationWatermark[] publicationWatermarks = authors
                .Select(author => map.ReadPublicationWatermark(coordinator.NamespaceId, author))
                .ToArray();
            await publish(publicationWatermarks, cancellationToken).ConfigureAwait(false);
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
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>, CancellationToken, ValueTask> publishReadyOutbox)
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
        StorageRelayProvenanceMap map,
        Func<IDisposable?> acquireHostCommitLease,
        Func<IReadOnlyList<NodeId>> readRelayAuthors)
    {
        using IDisposable commitGuard =
            coordinator.AcquireApplicationCommitGuard(acquireHostCommitLease);
        IReadOnlyList<NodeId> discovered = readRelayAuthors()
            ?? throw new InvalidDataException("Host relay-author discovery returned null.");
        IReadOnlyList<NodeId> mappedAuthors = map.ReadAuthors(coordinator.NamespaceId);
        var seen = new HashSet<NodeId>();
        var authors = new List<NodeId>(discovered.Count + mappedAuthors.Count);
        AddAuthors(discovered);
        AddAuthors(mappedAuthors);

        void AddAuthors(IReadOnlyList<NodeId> source)
        {
            foreach (NodeId author in source)
            {
                if (author.IsZero || author == coordinator.LocalReplicaId)
                    throw new InvalidDataException("Relay author discovery returned a zero or local author identity.");
                if (seen.Add(author))
                    authors.Add(author);
            }
        }
        // Match NodeId.ToString()'s canonical byte/hex order without allocating
        // a 16-byte array and 32-character string for every comparison.
        authors.Sort(static (left, right) =>
        {
            int lo = BinaryPrimitives.ReverseEndianness(left.Lo)
                .CompareTo(BinaryPrimitives.ReverseEndianness(right.Lo));
            return lo != 0
                ? lo
                : BinaryPrimitives.ReverseEndianness(left.Hi)
                    .CompareTo(BinaryPrimitives.ReverseEndianness(right.Hi));
        });
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
