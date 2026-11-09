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
    private bool _metadataReconciliationRequired = true;

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
        coordinator.ValidateRelayProvenanceMap(map);
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

            StorageRelayAuthorPublicationWatermark[] publicationWatermarks =
                CapturePublicationWatermarks(
                    coordinator, map, authors, acquireHostCommitLease);
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
        coordinator.ValidateRelayProvenanceMap(map);
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

            StorageRelayAuthorPublicationWatermark[] publicationWatermarks =
                CapturePublicationWatermarks(
                    coordinator, map, authors, acquireHostCommitLease);
            await publishReadyOutbox(publicationWatermarks, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Startup path after Metadata Store becomes the publication authority. The durable
    /// relay rows and author sequences are already in Metadata Store; no FLD JournalSeq
    /// scan or RPM3 translation is performed. PendingApply recovery must complete before
    /// the caller invokes this method.
    /// </summary>
    public async ValueTask<bool> RecoverMetadataOwnedAndPublishAsync(
        StorageSyncCoordinator coordinator,
        Func<IDisposable?> acquireHostCommitLease,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
        if (!coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned recovery cannot run before provider-authoritative cutover.");

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            IReadOnlyList<StorageRelayAuthorPublicationWatermark> watermarks =
                coordinator.CaptureMetadataOwnedPublicationWatermarks(acquireHostCommitLease);
            _metadataReconciliationRequired = false;
            await publishReadyOutbox(watermarks, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            _metadataReconciliationRequired = true;
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Serialize one metadata-authoritative remote APPLY and the following send.
    /// Finalized PendingApply rows atomically create relay payload in Metadata Store;
    /// there is intentionally no second FLD relay journal or RPM3 translation here.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishMetadataOwnedAsync(
        StorageSyncCoordinator coordinator,
        Func<IDisposable?> acquireHostCommitLease,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(applyRemoteBatch);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
        if (!coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned APPLY requires provider-authoritative cutover.");

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                await applyRemoteBatch(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _metadataReconciliationRequired = true;
                throw;
            }

            IReadOnlyList<StorageRelayAuthorPublicationWatermark> watermarks;
            try
            {
                watermarks = coordinator.CaptureMetadataOwnedPublicationWatermarks(
                    acquireHostCommitLease);
                _metadataReconciliationRequired = false;
            }
            catch
            {
                _metadataReconciliationRequired = true;
                throw;
            }

            await publishReadyOutbox(watermarks, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Run a later metadata-owned outbound pass. Startup recovery is mandatory first;
    /// capture current local and relay high watermarks under the composed commit guard.
    /// </summary>
    public async ValueTask RunMetadataOwnedAsync(
        StorageSyncCoordinator coordinator,
        Func<IDisposable?> acquireHostCommitLease,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(publish);
        if (!coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned outbound requires provider-authoritative cutover.");

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_metadataReconciliationRequired)
                throw new InvalidOperationException(
                    "Metadata-owned outbound is closed until startup/pending-apply recovery completes.");
            IReadOnlyList<StorageRelayAuthorPublicationWatermark> watermarks =
                coordinator.CaptureMetadataOwnedPublicationWatermarks(acquireHostCommitLease);
            await publish(watermarks, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Compact relay payload only through the minimum durable ACK across all interested
    /// peers. Per-peer ACKs are committed by SyncSession; FLD ApplyJournal relay ACK and
    /// RPM3 are not involved in Metadata Store-owned publication.
    /// </summary>
    public async ValueTask<IReadOnlyList<(NodeId Author, ulong SafeThroughSequence)>>
        CompactMetadataOwnedAcknowledgedAsync(
            StorageSyncCoordinator coordinator,
            Func<IDisposable?> acquireHostCommitLease,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (!coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned relay compaction requires provider-authoritative cutover.");

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_metadataReconciliationRequired)
                throw new InvalidOperationException(
                    "Metadata-owned relay compaction is closed until startup recovery completes.");
            return coordinator.CompactAcknowledgedMetadataRelayPayloads(
                acquireHostCommitLease);
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
        coordinator.ValidateRelayProvenanceMap(map);

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
        coordinator.ValidateRelayProvenanceMap(map);

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
        coordinator.ValidateRelayProvenanceMap(map);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReconciled();
            NodeId[] authors = DiscoverAuthors(coordinator, map, acquireHostCommitLease, readRelayAuthors);
            StorageRelayAuthorPublicationWatermark[] publicationWatermarks =
                CapturePublicationWatermarks(
                    coordinator, map, authors, acquireHostCommitLease);
            await publish(publicationWatermarks, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static StorageRelayAuthorPublicationWatermark[] CapturePublicationWatermarks(
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        IReadOnlyList<NodeId> relayAuthors,
        Func<IDisposable?> acquireHostCommitLease)
    {
        // Capture local outbound and all relay ceilings under one application barrier.
        // A concurrent FLD apply/freeze can only publish above this captured ceiling,
        // so the sender cannot leak a not-yet-mapped operation.
        using IDisposable commitGuard =
            coordinator.AcquireApplicationCommitGuard(acquireHostCommitLease);
        bool metadataStoreOwnedPublication = coordinator.IsMetadataStoreOwnedPublication;
        var watermarks = new StorageRelayAuthorPublicationWatermark[relayAuthors.Count + 1];
        watermarks[0] = coordinator.ReadLocalPublicationWatermark(
            metadataStoreOwnedPublication);
        for (int i = 0; i < relayAuthors.Count; i++)
        {
            NodeId author = relayAuthors[i];
            if (author == coordinator.LocalReplicaId || author.IsZero)
                throw new InvalidDataException("Relay publication watermark contains a zero or local relay author.");
            StorageRelayAuthorPublicationWatermark relayWatermark =
                map.ReadPublicationWatermark(coordinator.NamespaceId, author);
            watermarks[i + 1] = relayWatermark with
            {
                IsMetadataStoreOwnedPublication = metadataStoreOwnedPublication
            };
        }
        return watermarks;
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
        authors.Sort(NodeIdCanonicalComparer.Instance);
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
