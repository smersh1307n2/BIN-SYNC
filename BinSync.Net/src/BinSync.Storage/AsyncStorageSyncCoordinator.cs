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
