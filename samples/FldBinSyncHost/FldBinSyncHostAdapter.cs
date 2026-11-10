using BinSync;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;

namespace BinSync.FldHost;

/// <summary>
/// Concrete application-layer binding between one FLD database and the durable
/// BIN-SYNC relay coordinator. This sample is deliberately outside both database
/// runtimes: FLD remains protocol-neutral and the two FLD runtime assemblies stay
/// Fld1.Storage.dll + Fld1.Database.dll.
/// </summary>
public sealed class FldBinSyncHostAdapter
{
    private const int RelayBatchPageLimit = 256;
    private const int MaximumSourceChangesPerPage = 4096;

    private readonly FldDatabase _database;
    private readonly StorageSyncCoordinator _coordinator;
    private readonly StorageRelayProvenanceMap _map;
    private readonly StorageRelayPublicationGate _publicationGate;
    private readonly string _scopeId;

    public FldBinSyncHostAdapter(
        FldDatabase database,
        FldDatabaseDataProvider dataProvider,
        StorageMetadataStore metadataStore,
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        string scopeId,
        FldRemoteBatchApplyStore remoteBatchApplyStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        ArgumentNullException.ThrowIfNull(dataProvider);
        ArgumentNullException.ThrowIfNull(metadataStore);
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _map = map ?? throw new ArgumentNullException(nameof(map));
        ArgumentNullException.ThrowIfNull(remoteBatchApplyStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (coordinator.NamespaceId != checked((int)database.NamespaceId))
            throw new ArgumentException("The coordinator and FLD database must target the same namespace.", nameof(coordinator));
        if (!dataProvider.IsBoundTo(database))
            throw new ArgumentException("The FLD data provider must target this exact database instance.", nameof(dataProvider));
        if (!coordinator.UsesDataProvider(dataProvider))
            throw new ArgumentException("The StorageSyncCoordinator must be constructed with this exact FldDatabaseDataProvider.", nameof(coordinator));
        if (!dataProvider.IsBoundToMetadataStore(
                metadataStore, scopeId, NodeId.From(database.LocalAuthor), checked((int)database.NamespaceId)))
            throw new ArgumentException("The FLD provider publication-mode marker must use this exact Metadata Store, scope, local replica and namespace.", nameof(metadataStore));
        if (!coordinator.UsesMetadataPeerStateStore(metadataStore))
            throw new ArgumentException("The StorageSyncCoordinator must use this exact Metadata Store instance.", nameof(coordinator));
        if (!remoteBatchApplyStore.IsBoundTo(database))
            throw new ArgumentException(
                "The contextual remote-apply store must target this exact FLD database instance.",
                nameof(remoteBatchApplyStore));
        if (!coordinator.UsesRemoteBatchCommitStore(remoteBatchApplyStore))
            throw new ArgumentException(
                "The StorageSyncCoordinator must be constructed with this exact FldRemoteBatchApplyStore.",
                nameof(coordinator));

        _scopeId = scopeId;
        _publicationGate = StorageRelayPublicationGate.For(coordinator);
    }

    /// <summary>
    /// Acquire the FULL_REPLACE barrier across the coordinator, Node, FLD database,
    /// durable Metadata Store and provider. The Metadata Store lease rejects unresolved
    /// PendingApply and immutable peer reservations before the receiver may switch.
    /// </summary>
    public IFullReplaceCommitLease AcquireFullReplaceCommitGuard()
        => _coordinator.AcquireFullReplaceCommitGuard(_database.AcquireCommitGuard);

    /// <summary>
    /// Freeze the provider's currently pending local feed into the active outbound
    /// authority (Node Outbox before cutover, Metadata Store after cutover). Call
    /// before startup recovery/publication and before each later outbound gate run.
    /// </summary>
    public int PublishLocalChanges(Action<Node> persistNode, int limit = 1024)
        => _coordinator.PublishLocalChanges(
            persistNode ?? throw new ArgumentNullException(nameof(persistNode)), limit);

    /// <summary>
    /// Freeze local provider changes, reconcile FLD/RPM3 recovery state, then publish
    /// through the supplied ceiling-aware sender callback.
    /// </summary>
    public async ValueTask<bool> RecoverAndPublishAllAsync(
        Action<Node> persistNode,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        int limit = 1024,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_coordinator.IsMetadataStoreOwnedPublication)
        {
            using (IDisposable recoveryGuard =
                _coordinator.AcquireApplicationCommitGuard(_database.AcquireCommitGuard))
            {
                _ = _coordinator.RecoverPendingApplies(persistNode);
                PublishLocalChanges(persistNode, limit);
            }

            // Metadata Store already owns relay payload and per-author sequences.
            // Startup must reconcile that authority directly; the RPM3/FLD JournalSeq
            // recovery path is exclusively for legacy Node-owned publication.
            return await _publicationGate.RecoverMetadataOwnedAndPublishAsync(
                _coordinator,
                _database.AcquireCommitGuard,
                publishReadyOutbox,
                cancellationToken).ConfigureAwait(false);
        }

        PublishLocalChanges(persistNode, limit);
        return await RecoverAndPublishAllAsync(
            publishReadyOutbox, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reconcile FLD source authors and RPM3-only authors before the host enables
    /// any namespace sender. The callback must use PublishReadyOutboxAsync or another
    /// ceiling-enforcing sender path.
    /// </summary>
    public ValueTask<bool> RecoverAndPublishAllAsync(
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
        if (_coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned startup recovery requires the overload that supplies the Node checkpoint callback.");
        return _publicationGate.RecoverAndPublishAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            _database.GetRelayJournalHighWatermark,
            ReadRelayPage,
            publishReadyOutbox,
            cancellationToken);
    }

    /// <summary>
    /// Serialize a durable remote APPLY with the following namespace-wide send.
    /// After Metadata Store cutover, finalizing PendingApply publishes relay payload
    /// directly into Metadata Store; before cutover the legacy FLD/RPM3 mapping path is used.
    /// </summary>
    /// <summary>
    /// Freeze current local provider changes, durably apply one remote batch, then
    /// map and publish every resulting relay page through captured ceilings.
    /// </summary>
    public async ValueTask<bool> ApplyAndPublishAllAsync(
        Action<Node> persistNode,
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        int limit = 1024,
        CancellationToken cancellationToken = default)
    {
        PublishLocalChanges(persistNode, limit);
        return await ApplyAndPublishAllAsync(
            applyRemoteBatch, publishReadyOutbox, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<bool> ApplyAndPublishAllAsync(
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(applyRemoteBatch);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
        if (_coordinator.IsMetadataStoreOwnedPublication)
            return _publicationGate.ApplyAndPublishMetadataOwnedAsync(
                _coordinator,
                _database.AcquireCommitGuard,
                applyRemoteBatch,
                publishReadyOutbox,
                cancellationToken);
        return _publicationGate.ApplyAndPublishAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            applyRemoteBatch,
            ReadRelayPage,
            publishReadyOutbox,
            cancellationToken);
    }

    /// <summary>
    /// Receive one durable batch through PendingApply, apply it to Node and the
    /// contextual FLD ApplyJournal receipt store, then publish the durable relay rows
    /// owned by Metadata Store when cutover is active. FLD ApplyJournal does not receive
    /// a duplicate relay payload copy in this mode.
    /// </summary>
    public ValueTask<bool> ReceiveAndPublishAllAsync(
        SyncSession inboundSession,
        Action<Node> persistNode,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        int limit = 1024,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inboundSession);
        ArgumentNullException.ThrowIfNull(persistNode);
        ArgumentNullException.ThrowIfNull(publishReadyOutbox);
        return ApplyAndPublishAllAsync(
            persistNode,
            async token =>
            {
                _ = await _coordinator.ReceiveBatchBeforeAckAsync(
                    inboundSession, persistNode, token).ConfigureAwait(false);
            },
            publishReadyOutbox,
            limit,
            cancellationToken);
    }

    /// <summary>Run a later outbound send through the same reconciled namespace gate.</summary>
    /// <summary>Freeze current local changes, then run an outbound send through the shared gate.</summary>
    public async ValueTask RunOutboundAsync(
        Action<Node> persistNode,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publish,
        int limit = 1024,
        CancellationToken cancellationToken = default)
    {
        PublishLocalChanges(persistNode, limit);
        await RunOutboundAsync(publish, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask RunOutboundAsync(
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publish);
        if (_coordinator.IsMetadataStoreOwnedPublication)
            return _publicationGate.RunMetadataOwnedAsync(
                _coordinator,
                _database.AcquireCommitGuard,
                publish,
                cancellationToken);
        return _publicationGate.RunOutboundAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            publish,
            cancellationToken);
    }

    /// <summary>
    /// Drain one peer's active outbound streams using the exact ceilings
    /// captured by the publication gate. Invoke this from the gate's publish callback;
    /// it cannot independently discover or widen the eligible author set.
    /// </summary>
    public async ValueTask PublishReadyOutboxAsync(
        SyncSession session,
        Node sender,
        IMetadataPeerStateStore? peerStateStore,
        IReadOnlyList<StorageRelayAuthorPublicationWatermark> publicationWatermarks,
        int schemaVersion = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(publicationWatermarks);
        if (sender.NodeId != _coordinator.LocalReplicaId)
            throw new ArgumentException("Sender does not match the coordinated local replica.", nameof(sender));

        if (publicationWatermarks.Count == 0)
            throw new InvalidDataException("Publication gate returned no author ceilings.");
        bool metadataStoreOwnedPublication =
            publicationWatermarks[0].IsMetadataStoreOwnedPublication;
        var ceilings = new Dictionary<NodeId, ulong>(publicationWatermarks.Count);
        bool localAuthorPresent = false;
        foreach (StorageRelayAuthorPublicationWatermark watermark in publicationWatermarks)
        {
            if (watermark.IsMetadataStoreOwnedPublication != metadataStoreOwnedPublication)
                throw new InvalidDataException("Publication ceilings were captured from mixed authority modes.");
            if (watermark.Author.IsZero || !ceilings.TryAdd(
                    watermark.Author, watermark.PublishedThroughSequence))
                throw new InvalidDataException("Publication ceilings contain a zero or duplicate author.");
            if (watermark.Author == _coordinator.LocalReplicaId)
                localAuthorPresent = true;
        }
        if (!localAuthorPresent)
            throw new InvalidDataException("Publication gate omitted the local author's durable outbound ceiling.");

        if (metadataStoreOwnedPublication)
        {
            if (peerStateStore is null)
                throw new ArgumentNullException(nameof(peerStateStore),
                    "Metadata Store-owned publication requires the durable peer-state adapter.");
            if (!_coordinator.UsesMetadataPeerStateStore(peerStateStore))
                throw new InvalidOperationException(
                    "The outbound session must use the exact Metadata Store instance owned by this coordinator.");
            while (await session.PushBatchAsync(
                sender,
                _coordinator.NamespaceId,
                peerStateStore,
                _scopeId,
                ceilings,
                schemaVersion,
                cancellationToken).ConfigureAwait(false))
            {
            }
        }
        else
        {
            while (await session.PushBatchAsync(
                sender,
                _coordinator.NamespaceId,
                ceilings,
                schemaVersion,
                cancellationToken).ConfigureAwait(false))
            {
            }
        }
    }

    /// <summary>
    /// Legacy Node-owned path: translate the downstream-safe Outbox cursor to FLD
    /// JournalSeq, durably ACK ApplyJournal, then prune RPM3. Metadata-owned publication
    /// commits per-peer ACKs inside SyncSession and must use CompactAcknowledgedRelayPayloadsAsync.
    /// </summary>
    public ValueTask<IReadOnlyList<(NodeId Author, ulong SafeJournalSeq)>> AcknowledgeAllAsync(
        CancellationToken cancellationToken = default)
    {
        if (_coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned peer ACKs are already durable in the Metadata Store; use CompactAcknowledgedRelayPayloadsAsync.");
        return _publicationGate.AcknowledgeAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            (author, safeJournalSeq) =>
                _database.AcknowledgeRelayBatches(author.ToArray(), safeJournalSeq),
            cancellationToken);
    }

    /// <summary>
    /// Metadata-owned path: compact only relay payload below the minimum durable
    /// per-peer ACK already committed by SyncSession. No FLD JournalSeq or RPM3 row
    /// is written or acknowledged.
    /// </summary>
    public ValueTask<IReadOnlyList<(NodeId Author, ulong SafeThroughSequence)>>
        CompactAcknowledgedRelayPayloadsAsync(
            CancellationToken cancellationToken = default)
    {
        if (!_coordinator.IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata relay payload compaction requires provider-authoritative cutover.");
        return _publicationGate.CompactMetadataOwnedAcknowledgedAsync(
            _coordinator,
            _database.AcquireCommitGuard,
            cancellationToken);
    }

    /// <summary>
    /// Create the host reference scanner used by BIN-SYNC's ChangeId, receipt,
    /// record-tombstone and FULL_REPLACE proof paths. The caller holds the composed
    /// application commit lease; each scan owns only its immutable FLD reference index.
    /// </summary>
    public IApplicationGcReferenceScanner CreateReferenceScanner()
        => new DelegateApplicationGcReferenceScanner((scope, replica, namespaceId) =>
        {
            if (!StringComparer.Ordinal.Equals(scope, _scopeId) ||
                replica != _coordinator.LocalReplicaId ||
                namespaceId != _coordinator.NamespaceId)
                throw new InvalidOperationException(
                    "The FLD reference scanner is bound to a different scope, replica or namespace.");

            FldReferenceSnapshot snapshot = _database.OpenReferenceSnapshot();
            try
            {
                FldDraftHorizon drafts = snapshot.UnpublishedLocalDraftHorizon;
                return new DelegateApplicationGcReferenceScan(
                    snapshot,
                    new ApplicationDraftHorizon(
                        drafts.Count, drafts.MaxHlcWall, drafts.MaxHlcLogical),
                    (sender, authorEpoch, transportEpoch, batch) =>
                        snapshot.HasReceiptReference(
                            sender.ToArray(),
                            authorEpoch,
                            transportEpoch,
                            checked((ushort)namespaceId),
                            batch.ToArray()),
                    changeId => snapshot.HasChangeIdReference(changeId.ToArray()),
                    (recordId, tombstoneChangeId) =>
                        snapshot.HasRecordReference(
                            recordId.ToArray(), tombstoneChangeId.ToArray()));
            }
            catch
            {
                snapshot.Dispose();
                throw;
            }
        });

    private IReadOnlyList<NodeId> ReadRelayAuthors()
    {
        IReadOnlyList<byte[]> source = _database.ReadUnacknowledgedRelayAuthors();
        var authors = new NodeId[source.Count];
        for (int i = 0; i < source.Count; i++)
            authors[i] = NodeId.From(source[i]);
        return authors;
    }

    private (StorageRelaySourceChange[] Changes,
        ulong ScannedThroughJournalSeq, ulong SourceHighWatermark) ReadRelayPage(
        NodeId author, StorageRelayProvenanceCursor cursor)
    {
        RelayBatchPage page = _database.ReadRelayBatchPage(
            author.ToArray(), cursor.ScannedThroughJournalSeq, RelayBatchPageLimit);

        int changeCount = 0;
        foreach (RelayJournalBatch batch in page.Batches)
        {
            changeCount = checked(changeCount + batch.Batch.RelayWrites.Count);
            if (changeCount > MaximumSourceChangesPerPage)
                throw new InvalidDataException(
                    "One FLD relay page exceeds the bounded RPM3 source-change limit.");
        }

        var changes = new StorageRelaySourceChange[changeCount];
        int index = 0;
        foreach (RelayJournalBatch batch in page.Batches)
        {
            foreach (RemoteFieldWrite write in batch.Batch.RelayWrites)
            {
                changes[index++] = new StorageRelaySourceChange(
                    batch.JournalSeq, BatchId.From(write.ChangeId));
            }
        }

        return (changes, page.ScannedThroughJournalSeq,
            _database.GetRelayJournalHighWatermark());
    }
}
