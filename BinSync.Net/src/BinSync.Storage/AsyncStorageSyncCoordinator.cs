using System.IO;

namespace BinSync.Storage;

/// <summary>
/// Async-only receiver coordinator. It owns Node and Metadata durability without
/// requiring an IDataProvider or sync-over-async adapter. Provider APPLY is always
/// contextual and awaited before the transport ACK.
/// </summary>
public sealed class AsyncStorageSyncCoordinator
{
    private readonly Node _node;
    private readonly IMetadataStore _metadataStore;
    private readonly string _scopeId;
    private readonly int _namespaceId;
    private readonly SemaphoreSlim _asyncAdmissionGate = new(1, 1);
    private readonly object _metadataGate = new();

    public AsyncStorageSyncCoordinator(
        Node node, IMetadataStore metadataStore, string scopeId, int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(metadataStore);
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("A stable Metadata Store scope is required.", nameof(scopeId));
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        _node = node;
        _metadataStore = metadataStore;
        _scopeId = scopeId;
        _namespaceId = namespaceId;
    }

    /// <summary>
    /// Register a session peer and migrate only safe Node-only stream progress into
    /// durable Metadata peer state. Prior peer epochs remain active until reconciliation.
    /// </summary>
    public void RegisterInterestedPeer(SyncSession session, int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (_metadataStore is not IMetadataPeerStateStore peerStore)
            throw new InvalidOperationException("The configured Metadata Store does not support durable peer state.");
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        lock (_metadataGate)
        {
            NodeId peer = session.PeerNodeId;
            uint peerEpoch = checked((uint)session.PeerEpoch);
            var priorPeerEpochs = peerStore.ReadInterestedPeers(_scopeId, _node.NodeId, namespaceId)
                .Where(item => item.Peer == peer && item.PeerEpoch < peerEpoch)
                .Select(item => item.PeerEpoch).Distinct().ToArray();
            bool hasPriorIncarnation = priorPeerEpochs.Length != 0 ||
                peerStore.GetRetiredPeerEpoch(_scopeId, _node.NodeId, peer, namespaceId) is not null;
            peerStore.RegisterInterestedPeer(_scopeId, _node.NodeId, peer, peerEpoch, namespaceId);

            var known = peerStore.ReadPeerStreamStates(_scopeId, _node.NodeId, peer, peerEpoch, namespaceId)
                .ToDictionary(stream => stream.AuthorStream, stream => stream.State);
            var authors = peerStore.ReadOutboundAuthors(_scopeId, _node.NodeId, namespaceId)
                .Concat(peerStore.ReadRelayAuthors(_scopeId, _node.NodeId, namespaceId))
                .Distinct()
                .ToArray();
            foreach (NodeId author in authors)
            {
                if (author == peer) continue;
                var saved = known.TryGetValue(author, out var existing)
                    ? existing
                    : peerStore.GetPeerStreamState(_scopeId, _node.NodeId, peer, peerEpoch, namespaceId, author);
                bool pristine = saved.AckedCursor == 0 && saved.PushedCursor == 0 &&
                    saved.StreamOffset == 0 && saved.IssuerEpoch == 0 &&
                    saved.PendingBatch is null && saved.LastBatchDigest.All(value => value == 0);
                if (!pristine || hasPriorIncarnation) continue;
                var legacy = _node.ReadPeerStreamProgress(peer, namespaceId, author);
                if (legacy.PushedCursor > legacy.AckedCursor)
                    throw new InvalidDataException(
                        "Legacy Node peer state has an unacknowledged batch without a durable replay reservation.");
                peerStore.RegisterInterestedPeerStream(_scopeId, _node.NodeId, peer,
                    peerEpoch, namespaceId, author, legacy.AckedCursor,
                    legacy.StreamOffset, legacy.IssuerEpoch, legacy.LastBatchDigest);
            }
        }
    }

    /// <summary>Persist an incoming batch intent before Node mutation.</summary>
    public StorageRemoteApplyPlan BeginRemoteBatchBeforeNodeApply(
        Batch batch, byte[] wirePayload, NodeId senderPeer, uint transportPeerEpoch = 0)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(wirePayload);
        if (wirePayload.Length == 0 || wirePayload.Length > 16 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(wirePayload));
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));

        lock (_metadataGate)
        {
            MetadataIncomingBatch metadataBatch = CreateMetadataBatch(
                batch, wirePayload, senderPeer, transportPeerEpoch);
            Guid token = StorageSyncCoordinator.CreateBatchApplyToken(batch);
            bool finalizeMetadata = true;
            bool isDuplicate;
            IReadOnlyList<bool> applyOperations;
            try
            {
                MetadataPendingApplyIntent intent = _metadataStore.BeginPendingApply(metadataBatch, token);
                isDuplicate = intent.IsDuplicate;
                applyOperations = intent.ApplyOperations;
                if (applyOperations.Count != metadataBatch.Operations.Count)
                    throw new InvalidDataException("Metadata Store returned an incomplete operation decision mask.");
            }
            catch (MetadataPendingApplyAlreadyFinalizedException)
            {
                MetadataBatchReceipt prior = _metadataStore.AcceptIncomingBatch(metadataBatch);
                if (!prior.IsDuplicate) throw;
                finalizeMetadata = false;
                isDuplicate = true;
                applyOperations = Array.AsReadOnly(new bool[metadataBatch.Operations.Count]);
            }
            return new StorageRemoteApplyPlan(token, metadataBatch, finalizeMetadata, isDuplicate, applyOperations);
        }
    }

    /// <summary>
    /// Publish the finite local provider-feed prefix through the async provider boundary.
    /// The FLD operation receipt is mapped in the Node WAL before metadata freeze; source
    /// Outbox ACK and operation-receipt ACK remain separate, ordered durable transitions.
    /// </summary>
    public async Task<int> PublishLocalChangesAsync(
        IAsyncDataProvider asyncProvider,
        Action<Node> persistNode,
        int limit = 1024,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(asyncProvider);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (limit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(limit));
        if (asyncProvider is not IAsyncProviderOperationReceiptStore receiptStore ||
            asyncProvider is not IAsyncProviderPublishedOriginStore originStore ||
            asyncProvider is not IAsyncProviderPublicationMode publicationMode)
            throw new NotSupportedException(
                "Async local publication requires durable operation receipts, immutable-origin stamping and publication-mode capabilities.");
        if (_metadataStore is not IMetadataOutboundChangeIdentityStore identityStore)
            throw new NotSupportedException(
                "Async local publication requires the durable Metadata Store outbound ChangeId identity index.");

        await _asyncAdmissionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            (bool Exists, ulong NodeOutboxHighWatermark) cutover =
                await publicationMode.TryGetMetadataStoreOwnedPublicationCutoverAsync(ct)
                    .ConfigureAwait(false);
            if (!cutover.Exists)
                throw new InvalidOperationException(
                    "Async local publication requires the provider-authoritative Metadata Store cutover.");
            long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
            if (nodeHigh < 0 || checked((ulong)nodeHigh) != cutover.NodeOutboxHighWatermark)
                throw new InvalidOperationException(
                    "Node Outbox advanced after Metadata-Store-owned publication was enabled.");

            ulong providerHighWatermark =
                await asyncProvider.GetLocalChangeHighWatermarkAsync(ct).ConfigureAwait(false);
            var winners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>();
            Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>? nodeWinners = null;
            var recoveredPublishedOrigins = new List<ProviderPublishedOrigin>();
            var receiptMappings = new List<(BatchId ChangeId, ulong ClientSequence)>();
            var seenChangeIds = new HashSet<BatchId>();
            ulong afterSequence = 0;
            ulong scannedThroughSequence = 0;
            ulong lastFrozenFeedSequence = 0;
            int totalChanges = 0;

            // Scan exactly the source-feed ceiling captured above. Appends made while
            // IPC is in flight are deliberately deferred to the next publication pass.
            while (afterSequence < providerHighWatermark)
            {
                ProviderChangePage page = await asyncProvider.ReadLocalChangesAsync(
                    afterSequence, limit, ct).ConfigureAwait(false);
                if (page.Changes.Count > limit ||
                    page.ScannedThroughSequence < afterSequence ||
                    page.ScannedThroughSequence == afterSequence)
                    throw new InvalidDataException("Async provider returned an invalid or non-advancing local-feed page.");

                int pageChangeCount = 0;
                ulong previousChangeSequence = afterSequence;
                foreach (ProviderChange change in page.Changes)
                {
                    if (change.Sequence <= previousChangeSequence ||
                        change.Sequence > page.ScannedThroughSequence)
                        throw new InvalidDataException("Async provider local-feed changes are not ordered within the scanned prefix.");
                    previousChangeSequence = change.Sequence;
                    if (change.Sequence > providerHighWatermark)
                        continue;
                    pageChangeCount++;
                    if (change.ChangeId.IsZero || change.Mutation.ChangeId != change.ChangeId ||
                        change.Mutation.AuthorNodeId != _node.NodeId ||
                        change.Mutation.OriginAuthorEpoch == 0 || change.Mutation.OriginSequence != 0)
                        throw new InvalidDataException(
                            "Async local feed requires stable ChangeId, local author epoch and deferred OriginSequence.");
                    if (!seenChangeIds.Add(change.ChangeId))
                        throw new InvalidDataException("Async provider local feed repeated a ChangeId within one captured prefix.");
                    _node.ValidateProviderMutation(change.Mutation);
                    if (change.Mutation.KeyEpoch != 0)
                        throw new NotSupportedException("The current Node field-write API accepts key epoch zero only.");

                    // Append is idempotent by ChangeId-derived OperationId. If the Host
                    // committed but its response was lost, this exact retry recovers the
                    // original ClientSequence before the Node WAL records the mapping.
                    if (!_node.TryGetProviderOperationReceipt(
                            _namespaceId, change.ChangeId, out ulong clientSequence, out _))
                    {
                        clientSequence = await receiptStore.AppendLocalChangeWithReceiptAsync(
                            change.Mutation, ct).ConfigureAwait(false);
                        _node.RecordProviderOperationReceipt(
                            _namespaceId, change.ChangeId, clientSequence);
                    }
                    receiptMappings.Add((change.ChangeId, clientSequence));

                    bool sharedNodeAndOutboundWinners = false;
                    nodeWinners ??= winners;
                    sharedNodeAndOutboundWinners = ReferenceEquals(nodeWinners, winners);
                    if (sharedNodeAndOutboundWinners)
                        AddAsyncCoalescedPendingLocalChange(winners, change);
                    else
                        AddAsyncCoalescedPendingLocalChange(nodeWinners, change);

                    if (identityStore.TryGetOutboundChangeIdentity(
                            _scopeId, _node.NodeId, _namespaceId, change.ChangeId,
                            out ReadOnlyMemory<byte> frozenFingerprint, out OperationOrigin frozenOrigin))
                    {
                        Digest16 actualFingerprint = ChangeIdentity.Fingerprint(
                            _namespaceId, change.Mutation, _node.NodeId);
                        if (frozenFingerprint.Length != 16 ||
                            Digest16.From(frozenFingerprint.Span) != actualFingerprint)
                            throw new InvalidDataException(
                                "Async provider ChangeId conflicts with its previously frozen outbound operation.");
                        recoveredPublishedOrigins.Add(new ProviderPublishedOrigin(change, frozenOrigin));
                        lastFrozenFeedSequence = change.Sequence;
                        if (sharedNodeAndOutboundWinners)
                            nodeWinners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>(winners);
                        winners.Clear();
                        continue;
                    }
                    if (change.Sequence <= lastFrozenFeedSequence)
                        throw new InvalidDataException(
                            "Async provider local feed is not in strictly increasing source-sequence order.");
                    if (!sharedNodeAndOutboundWinners)
                        AddAsyncCoalescedPendingLocalChange(winners, change);
                }

                totalChanges = checked(totalChanges + pageChangeCount);
                // A page may include appends that arrived after the entry ceiling.
                // Clamp the durable ACK cursor and leave that suffix for the next pass.
                scannedThroughSequence = Math.Min(page.ScannedThroughSequence, providerHighWatermark);
                afterSequence = scannedThroughSequence;
            }

            ulong currentMetadataHigh = _metadataStore.GetOutboundHighWatermark(
                _scopeId, _node.NodeId, _namespaceId, _node.NodeId);
            if (currentMetadataHigh < cutover.NodeOutboxHighWatermark)
                throw new InvalidDataException(
                    "Metadata Store outbound watermark regressed below the provider cutover fence.");

            ProviderChange[] nodeChanges = (nodeWinners ?? winners).Values
                .OrderBy(change => change.Sequence).ToArray();
            if (nodeChanges.Length > totalChanges ||
                (nodeChanges.Length != 0 && nodeChanges[^1].Sequence > scannedThroughSequence))
                throw new InvalidDataException(
                    "Async provider cache winners exceed the captured local-feed prefix.");

            ProviderChange[] orderedWinners = winners.Values.OrderBy(change => change.Sequence).ToArray();
            var frozenOperations = new List<MetadataFrozenOperation>(orderedWinners.Length);
            foreach (ProviderChange change in orderedWinners)
                frozenOperations.Add(CreateAsyncMetadataFrozenOperation(change));

            IReadOnlyList<MetadataOutboundItem> published;
            lock (_metadataGate)
            {
                // Revalidate the immutable cutover fence immediately before freezing.
                ulong metadataHigh = _metadataStore.GetOutboundHighWatermark(
                    _scopeId, _node.NodeId, _namespaceId, _node.NodeId);
                if (metadataHigh < cutover.NodeOutboxHighWatermark)
                    throw new InvalidDataException("Metadata Store outbound watermark regressed during async publication.");
                published = frozenOperations.Count == 0
                    ? Array.Empty<MetadataOutboundItem>()
                    : _metadataStore.FreezeOutbound(
                        _scopeId, _node.NodeId, _namespaceId, _node.NodeId, frozenOperations);
            }
            if (published.Count != frozenOperations.Count)
                throw new InvalidDataException("Metadata Store freeze returned an unexpected operation count.");
            for (int i = 0; i < published.Count; i++)
            {
                if (published[i].ChangeId != frozenOperations[i].ChangeId ||
                    (i > 0 && published[i].Sequence <= published[i - 1].Sequence))
                    throw new InvalidDataException(
                        "Metadata Store returned an unexpected or unordered async outbound identity.");
            }

            var publishedOrigins = new List<ProviderPublishedOrigin>(
                checked(recoveredPublishedOrigins.Count + published.Count));
            publishedOrigins.AddRange(recoveredPublishedOrigins);
            for (int i = 0; i < published.Count; i++)
                publishedOrigins.Add(new ProviderPublishedOrigin(orderedWinners[i], published[i].Provenance));
            if (publishedOrigins.Count != 0)
                await originStore.CommitPublishedOriginsAsync(publishedOrigins, ct).ConfigureAwait(false);

            // Node remains an application-data cache. Persist it before marking the
            // receipt mappings terminal or acknowledging either provider retention domain.
            lock (_metadataGate)
            {
                _node.ApplyProviderChanges(_namespaceId, nodeChanges);
                persistNode(_node);
            }
            foreach ((BatchId changeId, ulong clientSequence) in receiptMappings)
                _node.MarkProviderOperationReceiptPublished(_namespaceId, changeId, clientSequence);

            // Source Outbox ACK must precede receipt ACK. If this call fails, the
            // Published WAL mappings remain and the same feed prefix is replayed.
            if (scannedThroughSequence > 0)
                await asyncProvider.AcknowledgeLocalChangesAsync(scannedThroughSequence, ct)
                    .ConfigureAwait(false);

            // This also recovers the crash window after source Outbox ACK but before
            // receipt ACK: the WAL still holds the exact contiguous ClientSequence prefix.
            ulong receiptPrefix = _node.GetPublishedProviderOperationReceiptPrefix(_namespaceId);
            if (receiptPrefix > 0)
            {
                await receiptStore.AcknowledgeOperationReceiptsAsync(receiptPrefix, ct)
                    .ConfigureAwait(false);
                _node.AcknowledgeProviderOperationReceipts(_namespaceId, receiptPrefix);
            }
            return totalChanges;
        }
        finally
        {
            _asyncAdmissionGate.Release();
        }
    }

    /// <summary>
    /// Receive one batch and emit its transport ACK only after Node checkpoint,
    /// contextual async provider APPLY and Metadata finalization all succeed.
    /// </summary>
    public async Task<BatchResult> ReceiveBatchWithAsyncProviderBeforeAckAsync(
        SyncSession session,
        IAsyncProviderRemoteBatchCommitStore asyncProvider,
        Action<Node> persistNode,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(asyncProvider);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (session.PeerEpoch <= 0)
            throw new InvalidDataException("A nonzero transport peer epoch is required for async receive.");
        RegisterInterestedPeer(session, _namespaceId);
        await _asyncAdmissionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            StorageRemoteApplyPlan? plan = null;
            BatchResult result = await session.ReceiveBatchAsync(
                _node,
                beforeApply: null,
                beforeAck: null,
                ct: ct,
                receiveStateOwnedByMetadataStore: true,
                beforeApplyWithOperationDecisions: (batch, payload, senderPeer) =>
                {
                    plan = BeginRemoteBatchBeforeNodeApply(
                        batch, payload.ToArray(), senderPeer, checked((uint)session.PeerEpoch));
                    return plan.ApplyOperations;
                },
                beforeAckAsync: async (batch, _) =>
                {
                    StorageRemoteApplyPlan accepted = plan
                        ?? throw new InvalidOperationException("Receive intent was not durably staged.");
                    await ApplyRemoteBatchAfterNodeApplyCoreAsync(
                        batch, accepted, asyncProvider, persistNode, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            if (plan?.IsDuplicate == true)
                result.Duplicate = true;
            return result;
        }
        finally
        {
            _asyncAdmissionGate.Release();
        }
    }

    /// <summary>Complete a separately staged receive through the async provider.</summary>
    public async Task<ProviderApplyReceipt> ApplyRemoteBatchAfterNodeApplyAsync(
        Batch batch,
        StorageRemoteApplyPlan plan,
        IAsyncProviderRemoteBatchCommitStore asyncProvider,
        Action<Node> persistNode,
        CancellationToken ct = default)
    {
        await _asyncAdmissionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await ApplyRemoteBatchAfterNodeApplyCoreAsync(
                batch, plan, asyncProvider, persistNode, ct).ConfigureAwait(false);
        }
        finally
        {
            _asyncAdmissionGate.Release();
        }
    }

    private async Task<ProviderApplyReceipt> ApplyRemoteBatchAfterNodeApplyCoreAsync(
        Batch batch,
        StorageRemoteApplyPlan plan,
        IAsyncProviderRemoteBatchCommitStore asyncProvider,
        Action<Node> persistNode,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(asyncProvider);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (batch.NamespaceId != _namespaceId ||
            plan.ApplyToken != StorageSyncCoordinator.CreateBatchApplyToken(batch) ||
            plan.Batch.NamespaceId != _namespaceId ||
            plan.Batch.BatchId != batch.BatchId ||
            !plan.Batch.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest.ToArray()) ||
            !plan.Batch.HeaderFingerprint.AsSpan().SequenceEqual(InboxReceiptIdentity.Compute(batch).ToArray()))
            throw new InvalidDataException("Pre-staged receive intent does not match the applied Node batch.");

        // Node has already applied the batch in SyncSession. Persist before IPC.
        persistNode(_node);
        List<ProviderMutation> mutations = StorageSyncCoordinator.CreateProviderMutations(plan.Batch.Operations);
        if (mutations.Count == 0)
            throw new InvalidDataException("Pre-staged receive intent contains no provider mutations.");

        ProviderRemoteBatchCommit context = CreateRemoteBatchCommit(batch, plan, mutations);
        ProviderApplyReceipt receipt = await asyncProvider.ApplyRemoteBatchAsync(context, ct)
            .ConfigureAwait(false);
        if (receipt.ApplyToken != plan.ApplyToken ||
            !receipt.BatchFingerprint.AsSpan().SequenceEqual(plan.Batch.BatchDigest))
            throw new InvalidDataException(
                "Async provider receipt does not match the pending ApplyToken and batch digest.");

        if (plan.FinalizeMetadata)
        {
            lock (_metadataGate)
            {
                MetadataBatchReceipt metadataReceipt = _metadataStore.FinalizePendingApply(
                    _scopeId, _node.NodeId,
                    new MetadataProviderApplyReceipt(plan.ApplyToken, plan.Batch.BatchDigest.ToArray()));
                if (metadataReceipt.DurableAuthor != batch.DurableAuthor)
                    throw new InvalidDataException("Finalized metadata receipt author does not match the applied Node batch.");
                if (!metadataReceipt.HeaderFingerprint.AsSpan()
                    .SequenceEqual(InboxReceiptIdentity.Compute(batch).ToArray()))
                    throw new InvalidDataException("Finalized metadata receipt header fingerprint does not match the applied Node batch.");
            }
        }
        return receipt;
    }

    /// <summary>
    /// Recover all metadata-authoritative PendingApply intents before accepting sessions.
    /// Exact wire payload and operation decisions are replayed, Node is checkpointed,
    /// then the same contextual ApplyToken is retried against the async provider.
    /// </summary>
    public async Task<int> RecoverPendingAppliesAsync(
        IAsyncProviderRemoteBatchCommitStore asyncProvider,
        Action<Node> persistNode,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(asyncProvider);
        ArgumentNullException.ThrowIfNull(persistNode);
        await _asyncAdmissionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            IReadOnlyList<MetadataPendingApplyIntent> intents;
            lock (_metadataGate)
                intents = _metadataStore.ReadPendingApplyIntents(_scopeId, _node.NodeId);

            int recovered = 0;
            foreach (MetadataPendingApplyIntent intent in intents)
            {
                ct.ThrowIfCancellationRequested();
                MetadataIncomingBatch pending = intent.Batch;
                if (pending.NamespaceId != _namespaceId) continue;
                if (!pending.ReceiveStateOwnedByMetadataStore ||
                    pending.WirePayload.Length == 0 ||
                    pending.HeaderFingerprint.Length != 16 ||
                    intent.ApplyOperations.Count != pending.Operations.Count)
                    throw new InvalidDataException(
                        "Async PendingApply recovery requires the exact metadata-authoritative wire payload, header fingerprint and operation mask.");

                Batch recoveredBatch;
                lock (_metadataGate)
                {
                    recoveredBatch = _node.ApplyBatchPayloadWithMetadataAuthority(
                        pending.WirePayload, pending.SenderPeer, intent.ApplyOperations).Batch;
                    persistNode(_node);
                }

                if (recoveredBatch.NamespaceId != _namespaceId ||
                    recoveredBatch.BatchId != pending.BatchId ||
                    checked((uint)recoveredBatch.IssuerEpoch) != pending.SenderEpoch ||
                    checked((ulong)recoveredBatch.CursorStart) != pending.CursorStart ||
                    checked((ulong)recoveredBatch.CursorEnd) != pending.CursorEnd ||
                    (!pending.AuthorStreamNodeId.IsZero &&
                     recoveredBatch.DurableAuthor != pending.AuthorStreamNodeId) ||
                    !recoveredBatch.BatchDigest.ToArray().AsSpan().SequenceEqual(pending.BatchDigest) ||
                    !recoveredBatch.PrevBatchDigest.ToArray().AsSpan().SequenceEqual(pending.PreviousBatchDigest) ||
                    !InboxReceiptIdentity.Compute(recoveredBatch).ToArray().AsSpan().SequenceEqual(pending.HeaderFingerprint) ||
                    StorageSyncCoordinator.CreateBatchApplyToken(recoveredBatch) != intent.ApplyToken)
                    throw new InvalidDataException("Recovered wire payload does not match its durable PendingApply identity.");

                List<ProviderMutation> mutations = StorageSyncCoordinator.CreateProviderMutations(pending.Operations);
                if (mutations.Count == 0)
                    throw new InvalidDataException("PendingApply intent contains no provider mutations.");
                ProviderRemoteBatchCommit context = CreateRemoteBatchCommit(recoveredBatch, intent, mutations);
                ProviderApplyReceipt receipt = await asyncProvider.ApplyRemoteBatchAsync(context, ct)
                    .ConfigureAwait(false);
                if (receipt.ApplyToken != intent.ApplyToken ||
                    !receipt.BatchFingerprint.AsSpan().SequenceEqual(pending.BatchDigest))
                    throw new InvalidDataException(
                        "Async provider recovery receipt does not match the pending ApplyToken and batch digest.");

                lock (_metadataGate)
                {
                    MetadataBatchReceipt metadataReceipt = _metadataStore.FinalizePendingApply(
                        _scopeId, _node.NodeId,
                        new MetadataProviderApplyReceipt(intent.ApplyToken, pending.BatchDigest.ToArray()));
                    NodeId expectedAuthor = !pending.AuthorStreamNodeId.IsZero
                        ? pending.AuthorStreamNodeId
                        : pending.Operations.Count == 0 ? default : pending.Operations[0].AuthorNodeId;
                    if (metadataReceipt.DurableAuthor != expectedAuthor)
                        throw new InvalidDataException("Recovered metadata receipt author does not match its PendingApply intent.");
                    if (!metadataReceipt.HeaderFingerprint.AsSpan().SequenceEqual(pending.HeaderFingerprint))
                        throw new InvalidDataException("Recovered metadata receipt header fingerprint does not match its PendingApply intent.");
                }
                recovered++;
            }
            return recovered;
        }
        finally
        {
            _asyncAdmissionGate.Release();
        }
    }

    private MetadataFrozenOperation CreateAsyncMetadataFrozenOperation(ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        if (change.ChangeId.IsZero || mutation.ChangeId != change.ChangeId ||
            mutation.OriginAuthorEpoch == 0 || mutation.OriginSequence != 0 ||
            mutation.AuthorNodeId != _node.NodeId)
            throw new InvalidDataException(
                "Async provider feed entry must retain a stable ChangeId/local author epoch and defer OriginSequence until freeze.");
        object operation;
        byte operationKind;
        if (mutation.IsRecordTombstone)
        {
            operation = new OpRecordTombstone
            {
                RecordId = mutation.RecordId,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            operationKind = OpsConst.TagRecordTomb;
        }
        else if (mutation.IsTombstone)
        {
            operation = new OpFieldTomb
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            operationKind = OpsConst.TagFieldTomb;
        }
        else
        {
            operation = new OpFieldWrite
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1,
                Value = mutation.Value.ToArray()
            };
            operationKind = OpsConst.TagFieldWrite;
        }

        byte[] encoded = Ops.EncodeOp(operation, fullAuthor: false);
        byte[] envelope = new byte[16 + encoded.Length];
        mutation.RecordId.WriteTo(envelope);
        encoded.CopyTo(envelope, 16);
        Digest16 fingerprint = ChangeIdentity.Fingerprint(
            _namespaceId, mutation.RecordId, operationKind, operation,
            _node.NodeId, Array.Empty<BatchId>());
        return new MetadataFrozenOperation
        {
            ChangeId = change.ChangeId,
            Fingerprint = fingerprint.ToArray(),
            Payload = envelope,
            OriginAuthorEpoch = mutation.OriginAuthorEpoch,
            OriginSequence = 0,
            HlcWall = mutation.HlcWall,
            HlcLogical = mutation.HlcLogical
        };
    }

    private static void AddAsyncCoalescedPendingLocalChange(
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange> winners,
        ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        var key = (mutation.RecordId, mutation.FieldId, mutation.IsRecordTombstone);
        if (!winners.TryGetValue(key, out ProviderChange? prior))
        {
            winners.Add(key, change);
            return;
        }

        ProviderMutation previous = prior.Mutation;
        int comparison = HlcOps.CompareHlc(
            new Hlc(mutation.HlcWall, mutation.HlcLogical), mutation.AuthorNodeId,
            new Hlc(previous.HlcWall, previous.HlcLogical), previous.AuthorNodeId);
        if (comparison == 0)
        {
            if (!SameAsyncMutation(mutation, previous))
                throw new InvalidDataException(
                    "Async pending local feed contains conflicting mutations at the same HLC and author.");
            return;
        }
        if (comparison > 0)
            winners[key] = change;
    }

    private static bool SameAsyncMutation(ProviderMutation left, ProviderMutation right)
        => left.RecordId == right.RecordId
            && left.FieldId == right.FieldId
            && left.IsTombstone == right.IsTombstone
            && left.IsRecordTombstone == right.IsRecordTombstone
            && left.HlcWall == right.HlcWall
            && left.HlcLogical == right.HlcLogical
            && left.AuthorNodeId == right.AuthorNodeId
            && left.KeyEpoch == right.KeyEpoch
            && left.Value.AsSpan().SequenceEqual(right.Value);

    private ProviderRemoteBatchCommit CreateRemoteBatchCommit(
        Batch batch, StorageRemoteApplyPlan plan, IReadOnlyList<ProviderMutation> mutations) =>
        new(
            plan.ApplyToken,
            plan.Batch.SenderPeer,
            plan.Batch.TransportPeerEpoch,
            plan.Batch.NamespaceId,
            batch.IssuerNodeId,
            checked((uint)batch.IssuerEpoch),
            batch.BatchId,
            checked((ulong)batch.CursorStart),
            checked((ulong)batch.CursorEnd),
            batch.PrevBatchDigest.ToArray(),
            batch.BatchDigest.ToArray(),
            batch.RecordsRoot.ToArray(),
            ReadOnlySpan<byte>.Empty,
            mutations);

    private ProviderRemoteBatchCommit CreateRemoteBatchCommit(
        Batch batch, MetadataPendingApplyIntent intent, IReadOnlyList<ProviderMutation> mutations) =>
        new(
            intent.ApplyToken,
            intent.Batch.SenderPeer,
            intent.Batch.TransportPeerEpoch,
            intent.Batch.NamespaceId,
            batch.IssuerNodeId,
            checked((uint)batch.IssuerEpoch),
            batch.BatchId,
            checked((ulong)batch.CursorStart),
            checked((ulong)batch.CursorEnd),
            batch.PrevBatchDigest.ToArray(),
            batch.BatchDigest.ToArray(),
            batch.RecordsRoot.ToArray(),
            ReadOnlySpan<byte>.Empty,
            mutations);

    private MetadataIncomingBatch CreateMetadataBatch(
        Batch batch, byte[] wirePayload, NodeId senderPeer, uint transportPeerEpoch)
    {
        var operations = new List<MetadataOperation>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
        {
            object op = Ops.DecodeOp(entry.Payload).Op;
            Digest16 fingerprint = ChangeIdentity.Fingerprint(
                batch.NamespaceId, entry.RecordId, entry.OpKind, op,
                batch.DurableAuthor, entry.CausalDeps);
            byte[] envelope = new byte[16 + entry.Payload.Length];
            entry.RecordId.WriteTo(envelope);
            entry.Payload.Span.CopyTo(envelope.AsSpan(16));
            operations.Add(new MetadataOperation
            {
                AuthorNodeId = batch.DurableAuthor,
                ChangeId = entry.ChangeId,
                Fingerprint = fingerprint.ToArray(),
                OriginAuthorEpoch = entry.OriginAuthorEpoch,
                OriginSequence = entry.OriginSequence,
                Payload = envelope,
                RelayEligible = true
            });
        }

        return new MetadataIncomingBatch
        {
            ScopeId = _scopeId,
            LocalReplica = _node.NodeId,
            SenderPeer = senderPeer,
            SenderEpoch = checked((uint)batch.IssuerEpoch),
            TransportPeerEpoch = transportPeerEpoch,
            AuthorStreamNodeId = batch.DurableAuthor,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = checked((ulong)batch.CursorStart),
            CursorEnd = checked((ulong)batch.CursorEnd),
            PreviousBatchDigest = batch.PrevBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            HeaderFingerprint = InboxReceiptIdentity.Compute(batch).ToArray(),
            WirePayload = wirePayload.ToArray(),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = operations
        };
    }
}
