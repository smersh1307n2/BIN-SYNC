using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BinSync.Storage;

public sealed record StorageRemoteApplyPlan(
    Guid ApplyToken, MetadataIncomingBatch Batch, bool FinalizeMetadata, bool IsDuplicate = false);

/// <summary>
/// Coordinates a BIN-SYNC Node with an application-owned durable Data Provider.
/// The caller supplies the durable Node checkpoint action so the source-feed ACK
/// is advanced only after the Node's local Outbox has been persisted.
/// </summary>
public sealed class StorageSyncCoordinator
{
    private readonly Node _node;
    private readonly IDataProvider _provider;
    private readonly int _namespaceId;
    private readonly IMetadataStore? _metadataStore;
    private readonly string? _scopeId;
    private readonly object _gate = new();

    public StorageSyncCoordinator(Node node, IDataProvider provider, int namespaceId,
        IMetadataStore? metadataStore = null, string? scopeId = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (metadataStore is not null && string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("A stable metadata scope is required when a Metadata Store is attached.", nameof(scopeId));
        if (metadataStore is null && scopeId is not null)
            throw new ArgumentException("A metadata scope cannot be supplied without a Metadata Store.", nameof(scopeId));
        _namespaceId = namespaceId;
        _metadataStore = metadataStore;
        _scopeId = scopeId;
    }

    /// <summary>
    /// Imports local provider changes into the Node. The provider feed is acknowledged
    /// only after persistNode has durably saved the Node's Outbox and state.
    /// Replaying an unacknowledged page is idempotent because the original HLC and
    /// local author are passed through to Node.WriteField/TombField.
    /// </summary>
    public int PublishLocalChanges(Action<Node> persistNode, int limit = 1024)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
        lock (_gate)
            return PublishLocalChangesCore(persistNode, limit, installProviderSnapshot: true);
    }

    private int PublishLocalChangesCore(
        Action<Node> persistNode, int limit, bool installProviderSnapshot)
    {
        var winners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>();
        ulong afterSequence = 0;
        ulong scannedThroughSequence = 0;
        int totalChanges = 0;
        bool sawStableChangeId = false;
        bool sawLegacyChangeId = false;

        // Drain the complete currently-pending provider feed, not just one page.
        // The winner map is bounded by distinct record/field identities rather than
        // the number of historical edits, so repeated pre-sync writes across page
        // boundaries collapse without creating intermediate Outbox records.
        while (true)
        {
            var page = _provider.ReadLocalChanges(afterSequence, limit);
            if (page.Changes.Count == 0)
            {
                scannedThroughSequence = Math.Max(scannedThroughSequence, page.ScannedThroughSequence);
                break;
            }
            if (page.ScannedThroughSequence <= afterSequence)
                throw new InvalidDataException("Provider local feed cursor did not advance.");

            foreach (var change in page.Changes)
            {
                if (change.Mutation.AuthorNodeId != _node.NodeId)
                    throw new InvalidOperationException("A local provider change must use the attached Node's author identity.");
                if (change.Mutation.KeyEpoch != 0)
                    throw new NotSupportedException("The current Node field-write API accepts key epoch zero only.");
                sawStableChangeId |= !change.ChangeId.IsZero;
                sawLegacyChangeId |= change.ChangeId.IsZero;
                AddCoalescedPendingLocalChange(winners, change);
            }

            totalChanges = checked(totalChanges + page.Changes.Count);
            scannedThroughSequence = page.ScannedThroughSequence;
            afterSequence = page.ScannedThroughSequence;
            if (page.Changes.Count < limit)
                break;
        }

        if (totalChanges == 0)
            return 0;

        if (_metadataStore is not null && sawStableChangeId)
        {
            if (sawLegacyChangeId)
                throw new InvalidDataException("A durable provider feed page cannot mix stable and zero ChangeIds.");
            return PublishLocalChangesToMetadataStore(
                winners, scannedThroughSequence, totalChanges, persistNode, installProviderSnapshot);
        }

        // Legacy/volatile providers without durable source identities retain the
        // transition path through Node Outbox. Durable StorageDataProvider feeds use
        // the Metadata-Store-owned path above and do not allocate duplicate Node rows.
        foreach (var change in winners.Values.OrderBy(change => change.Sequence))
        {
            ProviderMutation mutation = change.Mutation;
            var hlc = new Hlc(mutation.HlcWall, mutation.HlcLogical);
            if (mutation.IsRecordTombstone)
                _node.DeleteRecord(_namespaceId, mutation.RecordId, hlc);
            else if (mutation.IsTombstone)
                _node.TombField(_namespaceId, mutation.RecordId, mutation.FieldId, hlc);
            else
                _node.WriteField(_namespaceId, mutation.RecordId, mutation.FieldId, mutation.Value, hlc);
        }

        // Do not ACK if persistence or metadata freeze fails. The provider feed
        // remains the durable PendingLocal journal and is replayed/coalesced on restart.
        persistNode(_node);
        FreezeLocalOutboxToMetadata();
        if (scannedThroughSequence > 0)
            _provider.AcknowledgeLocalChanges(scannedThroughSequence);
        return totalChanges;
    }

    private int PublishLocalChangesToMetadataStore(
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange> winners,
        ulong scannedThroughSequence,
        int totalChanges,
        Action<Node> persistNode,
        bool installProviderSnapshot)
    {
        if (_metadataStore is null || string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("Stable provider ChangeIds require a durable Metadata Store scope.");

        // Finish any pre-cutover Node Outbox suffix before allocating Metadata-Store
        // sequences directly from the provider feed. Once Metadata Store is ahead of
        // the Node counter, the Node mirror is no longer an allocator.
        ulong metadataHigh = _metadataStore.GetOutboundHighWatermark(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
        long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
        if (metadataHigh <= (ulong)nodeHigh)
            FreezeLocalOutboxToMetadata();

        var frozenOperations = new List<MetadataFrozenOperation>(winners.Count);
        foreach (var change in winners.Values.OrderBy(change => change.Sequence))
        {
            if (change.ChangeId.IsZero)
                throw new InvalidDataException("Metadata-Store-owned publication requires a stable provider ChangeId.");
            frozenOperations.Add(CreateMetadataFrozenOperation(change));
        }

        var published = _metadataStore.FreezeOutbound(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId, frozenOperations);
        if (published.Count != frozenOperations.Count)
            throw new InvalidDataException("Metadata Store freeze returned an unexpected operation count.");
        for (int i = 0; i < published.Count; i++)
        {
            if (published[i].ChangeId != frozenOperations[i].ChangeId)
                throw new InvalidDataException("Metadata Store returned a different provider ChangeId.");
            if (i > 0 && published[i].Sequence <= published[i - 1].Sequence)
                throw new InvalidDataException("Metadata Store returned outbound sequences out of order.");
        }

        // The provider's current state is authoritative. The local feed can be
        // acknowledged once immutable outbound payloads are durable; Node is only
        // rebuilt as an application-data cache and can be rehydrated after a crash.
        if (scannedThroughSequence > 0)
            _provider.AcknowledgeLocalChanges(scannedThroughSequence);
        if (installProviderSnapshot)
            InstallProviderSnapshot(persistNode);
        return totalChanges;
    }

    private MetadataFrozenOperation CreateMetadataFrozenOperation(ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        object op;
        byte opKind;
        if (mutation.IsRecordTombstone)
        {
            op = new OpRecordTombstone
            {
                RecordId = mutation.RecordId,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            opKind = OpsConst.TagRecordTomb;
        }
        else if (mutation.IsTombstone)
        {
            op = new OpFieldTomb
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            opKind = OpsConst.TagFieldTomb;
        }
        else
        {
            op = new OpFieldWrite
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1,
                Value = mutation.Value.ToArray()
            };
            opKind = OpsConst.TagFieldWrite;
        }

        byte[] encoded = Ops.EncodeOp(op, fullAuthor: false);
        byte[] envelope = new byte[16 + encoded.Length];
        mutation.RecordId.WriteTo(envelope);
        encoded.CopyTo(envelope, 16);
        Digest16 fingerprint = ChangeIdentity.Fingerprint(
            _namespaceId, mutation.RecordId, opKind, op, _node.NodeId, Array.Empty<CausalDependency>());
        return new MetadataFrozenOperation
        {
            ChangeId = change.ChangeId,
            Fingerprint = fingerprint.ToArray(),
            Payload = envelope
        };
    }

    private static void AddCoalescedPendingLocalChange(
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange> winners,
        ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        var key = (mutation.RecordId, mutation.FieldId, mutation.IsRecordTombstone);
        if (!winners.TryGetValue(key, out var prior))
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
            if (!SameMutation(mutation, previous))
                throw new InvalidDataException("Pending local feed contains conflicting mutations at the same HLC and author.");
            return;
        }
        if (comparison > 0)
            winners[key] = change;
    }

    /// <summary>Compares complete local mutations when equal HLC/author keys are encountered.</summary>
    private static bool SameMutation(ProviderMutation left, ProviderMutation right)
        => left.RecordId == right.RecordId
            && left.FieldId == right.FieldId
            && left.IsTombstone == right.IsTombstone
            && left.IsRecordTombstone == right.IsRecordTombstone
            && left.HlcWall == right.HlcWall
            && left.HlcLogical == right.HlcLogical
            && left.AuthorNodeId == right.AuthorNodeId
            && left.KeyEpoch == right.KeyEpoch
            && left.Value.AsSpan().SequenceEqual(right.Value);

    /// <summary>
    /// Freezes newly appended local Node Outbox entries into the attached durable
    /// Metadata Store. Node sequence numbers are preserved exactly; a gap between the
    /// metadata high-watermark and the first retained Node entry fails closed.
    /// </summary>
    private void FreezeLocalOutboxToMetadata()
    {
        if (_metadataStore is null)
            return;

        ulong high = _metadataStore.GetOutboundHighWatermark(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
        if (high > long.MaxValue)
            throw new InvalidDataException("Metadata outbound high-watermark exceeds Node Outbox sequence range.");

        long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
        if (high > (ulong)nodeHigh)
            throw new InvalidDataException("Metadata outbound high-watermark is ahead of the restored Node Outbox.");
        long after = (long)high;
        if (after == nodeHigh)
            return;

        const int pageSize = 1024;
        while (true)
        {
            var entries = _node.ReadOutboxEntries(_namespaceId, _node.NodeId, after, pageSize);
            if (entries.Count == 0)
                throw new InvalidDataException("Node Outbox entries required by the Metadata Store high-watermark have been compacted or lost.");

            var frozen = new List<MetadataFrozenOperation>(entries.Count);
            long expected = checked(after + 1);
            foreach (var entry in entries)
            {
                if (entry.Seq != expected)
                    throw new InvalidDataException("Node Outbox sequence gap prevents safe Metadata Store freeze.");

                object op = Ops.DecodeOp(entry.Payload).Op;
                Digest16 fingerprint = ChangeIdentity.Fingerprint(
                    _namespaceId, entry.RecordId, entry.OpKind, op,
                    entry.AuthorNodeId, entry.CausalDeps);

                byte[] envelope = new byte[16 + entry.Payload.Length];
                entry.RecordId.WriteTo(envelope);
                entry.Payload.Span.CopyTo(envelope.AsSpan(16));
                frozen.Add(new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = fingerprint.ToArray(),
                    Payload = envelope
                });
                expected = checked(expected + 1);
            }

            var published = _metadataStore.FreezeOutbound(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId, frozen);
            if (published.Count != entries.Count)
                throw new InvalidDataException("Metadata Store freeze returned an unexpected operation count.");
            for (int i = 0; i < published.Count; i++)
                if (published[i].Sequence != checked((ulong)entries[i].Seq) ||
                    published[i].ChangeId != entries[i].ChangeId)
                    throw new InvalidDataException("Metadata Store sequence diverged from the Node Outbox.");

            after = entries[^1].Seq;
            if (after == nodeHigh)
                return;
            if (entries.Count < pageSize)
                throw new InvalidDataException("Node Outbox page ended before its allocated high-watermark.");
        }
    }

    /// <summary>
    /// Materializes the Node's current LWW field states into the application provider.
    /// Remote materialization never enters the provider's local change feed.
    /// The deterministic apply token makes an interrupted retry idempotent.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteFields(IEnumerable<(RecordId RecordId, ushort FieldId)> fields)
        => ApplyRemoteFieldsCore(fields, applyToken: null);

    /// <summary>
    /// Materializes exact immutable FieldWrite, FieldTomb and RecordTombstone operations
    /// carried by an accepted BIN-SYNC batch. The apply token is derived from the
    /// immutable batch identity, not the receiver's current winners, so replay remains
    /// idempotent even if later batches have advanced one or more of the same fields.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteBatch(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));

        var mutations = new List<ProviderMutation>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
        {
            if (entry.NamespaceId != _namespaceId || entry.Payload.IsEmpty)
                throw new InvalidDataException("Batch entry has an invalid namespace or empty operation payload.");

            object op = Ops.DecodeOp(entry.Payload).Op;
            switch (op)
            {
                case OpFieldWrite write:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = entry.RecordId,
                        FieldId = write.FieldId,
                        HlcWall = write.Wall,
                        HlcLogical = write.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = write.KeyEpoch,
                        Value = write.Value.ToArray()
                    });
                    break;
                case OpFieldTomb tomb:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = entry.RecordId,
                        FieldId = tomb.FieldId,
                        IsTombstone = true,
                        HlcWall = tomb.Wall,
                        HlcLogical = tomb.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = tomb.KeyEpoch,
                        Value = Array.Empty<byte>()
                    });
                    break;
                case OpRecordTombstone recordTombstone:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordTombstone.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = recordTombstone.Wall,
                        HlcLogical = recordTombstone.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>()
                    });
                    break;
                default:
                    throw new InvalidDataException("Batch contains an unsupported operation type.");
            }
        }

        if (mutations.Count == 0) return null;
        // Preserve wire order in the fingerprint. Multiple immutable operations
        // for the same field can legitimately occur in one batch; Storage applies
        // their LWW order while retaining the complete batch receipt. Do not sort or
        // reconstruct payload from current Node winners.
        Guid token = CreateBatchApplyToken(batch);
        lock (_gate)
        {
            if (_metadataStore is null)
                return _provider.ApplyRemoteBatch(token, mutations);

            MetadataIncomingBatch metadataBatch = CreateMetadataBatch(batch);
            bool finalizeMetadata = true;
            try
            {
                _metadataStore.BeginPendingApply(metadataBatch, token);
            }
            catch (InvalidOperationException)
            {
                // A finalized token is resolved through the durable receive receipt.
                // If it is not an exact duplicate, fail closed.
                var prior = _metadataStore.AcceptIncomingBatch(metadataBatch);
                if (!prior.IsDuplicate || (!prior.DurableAuthor.IsZero && prior.DurableAuthor != batch.DurableAuthor))
                    throw new InvalidDataException("Completed metadata receipt does not match the replayed batch author.");
                finalizeMetadata = false;
            }

            var providerReceipt = _provider.ApplyRemoteBatch(token, mutations);
            if (finalizeMetadata)
            {
                _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(token, metadataBatch.BatchDigest.ToArray()));
            }
            return providerReceipt;
        }
    }

    /// <summary>
    /// Registers a session peer as interested in this namespace. Existing Node-only
    /// progress is migrated first; an unacknowledged legacy send without a durable
    /// reservation fails closed instead of being silently skipped.
    /// </summary>
    public void RegisterInterestedPeer(SyncSession session, int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (_metadataStore is not IMetadataPeerStateStore peerStore)
            throw new InvalidOperationException("The configured Metadata Store does not support durable peer state.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("A stable Metadata Store scope is required for peer registration.");
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        lock (_gate)
        {
            NodeId peer = session.PeerNodeId;
            uint peerEpoch = checked((uint)session.PeerEpoch);
            var priorPeerEpochs = peerStore.ReadInterestedPeers(_scopeId!, _node.NodeId, namespaceId)
                .Where(item => item.Peer == peer && item.PeerEpoch < peerEpoch)
                .Select(item => item.PeerEpoch).Distinct().ToArray();
            bool hasPriorIncarnation = priorPeerEpochs.Length != 0 ||
                peerStore.GetRetiredPeerEpoch(_scopeId!, _node.NodeId, peer, namespaceId) is not null;
            peerStore.RegisterInterestedPeer(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId);
            foreach (uint oldEpoch in priorPeerEpochs)
                peerStore.RetireInterestedPeer(_scopeId!, _node.NodeId, peer,
                    oldEpoch, peerEpoch, namespaceId);

            var known = peerStore.ReadPeerStreamStates(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId)
                .ToDictionary(stream => stream.AuthorStream, stream => stream.State);
            var authors = peerStore.ReadOutboundAuthors(_scopeId!, _node.NodeId, namespaceId)
                .Concat(peerStore.ReadRelayAuthors(_scopeId!, _node.NodeId, namespaceId))
                .Distinct()
                .ToArray();
            foreach (NodeId author in authors)
            {
                if (author == peer) continue;
                var saved = known.TryGetValue(author, out var existing)
                    ? existing
                    : peerStore.GetPeerStreamState(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId, author);
                bool pristine = saved.AckedCursor == 0 && saved.PushedCursor == 0 &&
                    saved.StreamOffset == 0 && saved.IssuerEpoch == 0 &&
                    saved.PendingBatch is null && saved.LastBatchDigest.All(value => value == 0);
                if (!pristine || hasPriorIncarnation) continue;
                var legacy = _node.ReadPeerStreamProgress(peer, namespaceId, author);
                if (legacy.PushedCursor > legacy.AckedCursor)
                    throw new InvalidDataException("Legacy Node peer state has an unacknowledged batch without a durable replay reservation.");
                peerStore.RegisterInterestedPeerStream(_scopeId!, _node.NodeId, peer,
                    peerEpoch, namespaceId, author, legacy.AckedCursor,
                    legacy.StreamOffset, legacy.IssuerEpoch, legacy.LastBatchDigest);
            }
        }
    }

    /// <summary>
    /// Rebuilds the Node's namespace data cache from the authoritative provider snapshot,
    /// then replays any durable PendingApply intents before the caller starts or resumes
    /// transport work. The caller must use this only when all local mutations for this
    /// namespace are captured by the provider feed; Node-originated writes that bypass
    /// the provider are intentionally not part of the restored data snapshot.
    /// </summary>
    /// <returns>The number of PendingApply intents recovered after installing the snapshot.</returns>
    public int RehydrateNodeFromProviderSnapshot(Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_metadataStore is null)
            throw new InvalidOperationException("Provider-authoritative rehydration requires a durable Metadata Store.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("Provider-authoritative rehydration requires a stable Metadata Store scope.");

        lock (_gate)
        {
            // Publish pending provider-feed changes without first rebuilding the cache;
            // the stable Metadata Store payloads are durable before the snapshot install.
            PublishLocalChangesCore(persistNode, 1024, installProviderSnapshot: false);
            return InstallProviderSnapshot(persistNode);
        }
    }

    private int InstallProviderSnapshot(Action<Node> persistNode)
    {
        // One Metadata Store scope may contain multiple namespaces. This coordinator
        // owns only its namespace; leave other coordinators' intents untouched.
        var intents = _metadataStore!.ReadPendingApplyIntents(_scopeId!, _node.NodeId)
            .Where(intent => intent.Batch.NamespaceId == _namespaceId)
            .ToArray();
        if (intents.Any(intent => intent.Batch.WirePayload.Length == 0))
            throw new InvalidOperationException(
                "Provider-authoritative rehydration requires replayable PendingApply wire payloads; recover legacy intents before replacing the Node cache.");

        BatchId[] pendingRemoteChangeIds = intents
            .SelectMany(intent => intent.Batch.Operations)
            .Select(operation => operation.ChangeId)
            .Where(changeId => !changeId.IsZero)
            .Distinct()
            .ToArray();

        ProviderSnapshotHandle snapshot = _provider.OpenSnapshot();
        try
        {
            var mutations = new List<ProviderMutation>();
            int afterIndex = 0;
            while (true)
            {
                ProviderSnapshotPage page = _provider.ReadSnapshot(snapshot.Token, afterIndex, 512);
                if (page.Entries.Count > 512)
                    throw new InvalidDataException("Provider snapshot exceeded the requested page size.");
                mutations.AddRange(page.Entries);

                if (page.IsComplete) break;
                if (page.NextIndex <= afterIndex)
                    throw new InvalidDataException("Provider snapshot page did not advance.");
                afterIndex = page.NextIndex;
            }

            _node.RestoreProviderSnapshot(_namespaceId, mutations, pendingRemoteChangeIds);
            // Persist the clean provider-derived base before replaying pending intents.
            // If recovery is interrupted, the same durable intents can safely replay.
            persistNode(_node);
        }
        finally
        {
            _provider.ReleaseSnapshot(snapshot.Token);
        }

        return RecoverPendingApplies(persistNode);
    }

    /// <summary>
    /// Runs bidirectional anti-entropy and commits the matching durable peer snapshot/feed
    /// baseline. The feed watermarks are captured before reconciliation: changes after
    /// that barrier remain eligible for DELTA_BATCH, even if a later snapshot happens
    /// to include them. Relay streams always retain offset zero because snapshots do not
    /// replace relay/event history.
    /// </summary>
    public async Task<(int Prefixes, int FieldsApplied)> AutoReconcileWithMetadataAsync(
        SyncSession session, Action<Node> persistNode, bool initiate,
        int maxPrefixes = 16, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_metadataStore is not IMetadataPeerStateStore peerStore)
            throw new InvalidOperationException("The configured Metadata Store does not support durable peer state.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("A stable Metadata Store scope is required for initial-sync baseline.");

        ProviderSnapshotHandle providerSnapshot = _provider.OpenSnapshot();
        try
        {
            RecoverPendingApplies(persistNode);
            PublishLocalChanges(persistNode);
            MergeProviderSnapshotIntoNode(providerSnapshot, persistNode);
        }
        finally
        {
            _provider.ReleaseSnapshot(providerSnapshot.Token);
        }

        RegisterInterestedPeer(session, _namespaceId);
        persistNode(_node);

        NodeId peer = session.PeerNodeId;
        uint peerEpoch = checked((uint)session.PeerEpoch);
        var relayAuthors = peerStore.ReadRelayAuthors(_scopeId!, _node.NodeId, _namespaceId).ToHashSet();
        var authors = peerStore.ReadOutboundAuthors(_scopeId!, _node.NodeId, _namespaceId)
            .Concat(relayAuthors)
            .Where(author => author != peer)
            .Distinct()
            .ToArray();
        var baselines = new List<MetadataPeerStreamBaseline>(authors.Length);
        foreach (NodeId author in authors)
        {
            ulong offset = relayAuthors.Contains(author)
                ? 0
                : _metadataStore.GetOutboundHighWatermark(_scopeId!, _node.NodeId, _namespaceId, author);
            uint issuerEpoch = _node.ReadPeerStreamProgress(peer, _namespaceId, author).IssuerEpoch;
            baselines.Add(new MetadataPeerStreamBaseline(author, offset, issuerEpoch));
        }

        var result = await session.AutoReconcileAsync(
            _node, _namespaceId, initiate, maxPrefixes, ct,
            afterSnapshotApplied: (chunks, _) =>
            {
                persistNode(_node);
                ApplyRemoteSnapshotToProvider(session.PeerNodeId, peerEpoch, chunks);
            }).ConfigureAwait(false);
        if (!_node.IsInitialSyncComplete(peer, _namespaceId))
            throw new SyncException("SNAPSHOT_REQUIRED");
        persistNode(_node);
        peerStore.CompletePeerInitialSync(
            _scopeId!, _node.NodeId, peer, peerEpoch, _namespaceId, baselines);
        return result;
    }

    private void MergeProviderSnapshotIntoNode(
        ProviderSnapshotHandle snapshot, Action<Node> persistNode)
    {
        var recordsById = new Dictionary<RecordId, SnapshotRecord>();
        int afterIndex = 0;
        while (true)
        {
            ProviderSnapshotPage page = _provider.ReadSnapshot(snapshot.Token, afterIndex, 512);
            foreach (ProviderMutation mutation in page.Entries)
            {
                if (!recordsById.TryGetValue(mutation.RecordId, out var record))
                {
                    record = new SnapshotRecord { RecordId = mutation.RecordId };
                    recordsById.Add(mutation.RecordId, record);
                }

                if (mutation.IsRecordTombstone)
                {
                    record.HasLifecycle = true;
                    record.LifecycleValue = Model.Dead;
                    record.LifecycleWall = mutation.HlcWall;
                    record.LifecycleLogical = mutation.HlcLogical;
                    record.LifecycleAuthor = mutation.AuthorNodeId;
                }
                else
                {
                    record.Fields.Add(new SnapshotField
                    {
                        FieldId = mutation.FieldId,
                        IsTombstone = mutation.IsTombstone,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        AuthorNodeId = mutation.AuthorNodeId,
                        KeyEpoch = mutation.KeyEpoch,
                        Value = mutation.Value.ToArray()
                    });
                }
            }

            if (page.IsComplete) break;
            if (page.NextIndex <= afterIndex)
                throw new InvalidDataException("Provider snapshot page did not advance.");
            afterIndex = page.NextIndex;
        }

        if (recordsById.Count == 0) return;
        var records = recordsById.Values.ToList();
        foreach (var record in records)
            record.Fields.Sort(static (left, right) => left.FieldId.CompareTo(right.FieldId));

        var partitions = new List<List<SnapshotRecord>>();
        var current = new List<SnapshotRecord>();
        void AddPartition(List<SnapshotRecord> candidate)
        {
            var probe = new SnapshotChunkMessage
            {
                NamespaceId = _namespaceId,
                ChunkIndex = 0,
                ChunkCount = 1,
                Records = candidate
            };
            if (Reconcile.EncodeSnapshotChunk(probe).Length <= Frames.MAX_PAYLOAD_SIZE)
            {
                partitions.Add(candidate);
                if (partitions.Count > 4096) throw new SyncException("PAYLOAD_TOO_LARGE");
                return;
            }
            if (candidate.Count <= 1) throw new SyncException("PAYLOAD_TOO_LARGE");
            int middle = candidate.Count / 2;
            AddPartition(candidate.GetRange(0, middle));
            AddPartition(candidate.GetRange(middle, candidate.Count - middle));
        }

        foreach (var record in records)
        {
            current.Add(record);
            if (current.Count == 64)
            {
                AddPartition(current);
                current = new List<SnapshotRecord>();
            }
        }
        if (current.Count > 0) AddPartition(current);

        uint chunkCount = checked((uint)partitions.Count);
        var chunks = new List<SnapshotChunkMessage>(partitions.Count);
        for (int i = 0; i < partitions.Count; i++)
        {
            chunks.Add(new SnapshotChunkMessage
            {
                NamespaceId = _namespaceId,
                ChunkIndex = checked((uint)i),
                ChunkCount = chunkCount,
                Records = partitions[i]
            });
        }

        Reconcile.ApplySnapshotChunks(_node, chunks);
        persistNode(_node);
    }

    private void ApplyRemoteSnapshotToProvider(
        NodeId senderPeer, uint senderEpoch, IReadOnlyList<SnapshotChunkMessage> chunks)
    {
        var mutations = new List<ProviderMutation>();
        foreach (var chunk in chunks)
        {
            foreach (var record in chunk.Records)
            {
                if (record.HasLifecycle && record.LifecycleValue == Model.Dead)
                {
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = record.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = record.LifecycleWall,
                        HlcLogical = record.LifecycleLogical,
                        AuthorNodeId = record.LifecycleAuthor,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>()
                    });
                }

                foreach (var field in record.Fields)
                {
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = record.RecordId,
                        FieldId = field.FieldId,
                        IsTombstone = field.IsTombstone,
                        HlcWall = field.Wall,
                        HlcLogical = field.Logical,
                        AuthorNodeId = field.AuthorNodeId,
                        KeyEpoch = field.KeyEpoch,
                        Value = field.Value.ToArray()
                    });
                }
            }
        }
        if (mutations.Count == 0) return;

        using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(
            System.Security.Cryptography.HashAlgorithmName.SHA256);
        hash.AppendData(System.Text.Encoding.UTF8.GetBytes(_scopeId!));
        hash.AppendData(senderPeer.ToArray());
        Span<byte> fixedPart = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart, senderEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(4), checked((uint)_namespaceId));
        hash.AppendData(fixedPart);
        Span<byte> length = stackalloc byte[4];
        foreach (var chunk in chunks)
        {
            byte[] encoded = Reconcile.EncodeSnapshotChunk(chunk);
            BinaryPrimitives.WriteInt32LittleEndian(length, encoded.Length);
            hash.AppendData(length);
            hash.AppendData(encoded);
        }
        byte[] digest = hash.GetHashAndReset();
        Guid applyToken = new(digest.AsSpan(0, 16));
        if (applyToken == Guid.Empty)
        {
            digest[0] = 1;
            applyToken = new Guid(digest.AsSpan(0, 16));
        }
        _provider.ApplyRemoteBatch(applyToken, mutations);
    }

    /// <summary>
    /// Receive through SyncSession with a durable PendingApply intent committed before
    /// Node mutation. The ACK is emitted only after Node checkpoint, provider apply and
    /// metadata receipt finalization all succeed.
    /// </summary>
    public async Task<BatchResult> ReceiveBatchBeforeAckAsync(
        SyncSession session, Action<Node> persistNode, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(persistNode);
        RegisterInterestedPeer(session, _namespaceId);
        StorageRemoteApplyPlan? plan = null;
        BatchResult result = await session.ReceiveBatchAsync(_node,
            beforeApply: (batch, payload, senderPeer) =>
                plan = BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), senderPeer),
            beforeAck: (batch, _) =>
            {
                var accepted = plan ?? throw new InvalidOperationException("Receive intent was not durably staged.");
                ApplyRemoteBatchAfterNodeApply(batch, accepted, persistNode);
            },
            ct: ct,
            receiveStateOwnedByMetadataStore: true).ConfigureAwait(false);
        if (plan?.IsDuplicate == true)
            result.Duplicate = true;
        return result;
    }

    /// <summary>
    /// Persist the exact incoming batch as a recoverable intent before mutating Node.
    /// If the process stops at any point after this call, recovery can replay the same
    /// wire payload into Node and the provider before the sender is acknowledged.
    /// </summary>
    public StorageRemoteApplyPlan BeginRemoteBatchBeforeNodeApply(Batch batch, byte[] wirePayload,
        NodeId senderPeer)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(wirePayload);
        if (wirePayload.Length == 0 || wirePayload.Length > 16 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(wirePayload));
        if (_metadataStore is null)
            throw new InvalidOperationException("A Metadata Store is required for durable receive staging.");
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));

        lock (_gate)
        {
            MetadataIncomingBatch metadataBatch = CreateMetadataBatch(batch, wirePayload, senderPeer);
            Guid token = CreateBatchApplyToken(batch);
            bool finalizeMetadata = true;
            bool isDuplicate;
            try
            {
                var intent = _metadataStore.BeginPendingApply(metadataBatch, token);
                isDuplicate = intent.IsDuplicate;
            }
            catch (MetadataPendingApplyAlreadyFinalizedException)
            {
                var prior = _metadataStore.AcceptIncomingBatch(metadataBatch);
                if (!prior.IsDuplicate)
                    throw;
                finalizeMetadata = false;
                isDuplicate = true;
            }
            return new StorageRemoteApplyPlan(token, metadataBatch, finalizeMetadata, isDuplicate);
        }
    }

    /// <summary>
    /// Complete a pre-staged receive after Node has applied the decoded batch:
    /// persist Node, apply the exact immutable provider mutations, then finalize receipt.
    /// </summary>
    public ProviderApplyReceipt ApplyRemoteBatchAfterNodeApply(
        Batch batch, StorageRemoteApplyPlan plan, Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_metadataStore is null)
            throw new InvalidOperationException("A Metadata Store is required for durable receive staging.");
        if (batch.NamespaceId != _namespaceId || plan.ApplyToken != CreateBatchApplyToken(batch) ||
            plan.Batch.BatchId != batch.BatchId ||
            !plan.Batch.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest.ToArray()))
            throw new InvalidDataException("Pre-staged receive intent does not match the applied Node batch.");

        lock (_gate)
        {
            persistNode(_node);
            var mutations = CreateProviderMutations(plan.Batch.Operations);
            if (mutations.Count == 0)
                throw new InvalidDataException("Pre-staged receive intent contains no provider mutations.");
            var receipt = _provider.ApplyRemoteBatch(plan.ApplyToken, mutations);
            if (plan.FinalizeMetadata)
            {
                var metadataReceipt = _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(plan.ApplyToken, plan.Batch.BatchDigest.ToArray()));
                if (metadataReceipt.DurableAuthor != batch.DurableAuthor)
                    throw new InvalidDataException("Finalized metadata receipt author does not match the applied Node batch.");
                byte[] expectedHeaderFingerprint = InboxReceiptIdentity.Compute(batch).ToArray();
                if (!metadataReceipt.HeaderFingerprint.AsSpan().SequenceEqual(expectedHeaderFingerprint))
                    throw new InvalidDataException("Finalized metadata receipt header fingerprint does not match the applied Node batch.");
            }
            return receipt;
        }
    }

    /// <summary>
    /// Complete the receiver-side durability sequence for a pre-ACK callback:
    /// checkpoint Node, commit provider state and finalize Metadata Store PendingApply.
    /// The caller may ACK the sender only after this method returns successfully.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteBatchBeforeAck(
        Batch batch, Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));
        lock (_gate)
        {
            persistNode(_node);
            return ApplyRemoteBatch(batch);
        }
    }

    /// <summary>Recover pending provider commits after restart before accepting new sync work.</summary>
    public int RecoverPendingApplies(Action<Node>? persistNode = null)
    {
        if (_metadataStore is null)
            throw new InvalidOperationException("No Metadata Store is attached to this coordinator.");

        lock (_gate)
        {
            var intents = _metadataStore.ReadPendingApplyIntents(_scopeId!, _node.NodeId);
            int recovered = 0;
            foreach (var intent in intents)
            {
                if (intent.Batch.NamespaceId != _namespaceId)
                    continue;

                if (intent.Batch.WirePayload.Length > 0)
                {
                    if (persistNode is null)
                        throw new InvalidOperationException("Recovering a pre-Node PendingApply intent requires a Node checkpoint callback.");
                    if (intent.Batch.ReceiveStateOwnedByMetadataStore)
                        _node.ApplyBatchPayloadWithMetadataAuthority(intent.Batch.WirePayload, intent.Batch.SenderPeer);
                    else
                        _node.ApplyBatchPayload(intent.Batch.WirePayload, intent.Batch.SenderPeer);
                    persistNode(_node);
                }

                var mutations = CreateProviderMutations(intent.Batch.Operations);
                if (mutations.Count == 0)
                    throw new InvalidDataException("Pending apply intent contains no provider mutations.");

                _provider.ApplyRemoteBatch(intent.ApplyToken, mutations);
                var metadataReceipt = _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(intent.ApplyToken, intent.Batch.BatchDigest.ToArray()));
                NodeId expectedAuthor = !intent.Batch.AuthorStreamNodeId.IsZero
                    ? intent.Batch.AuthorStreamNodeId
                    : intent.Batch.Operations.Count == 0 ? default : intent.Batch.Operations[0].AuthorNodeId;
                if (!metadataReceipt.DurableAuthor.IsZero && metadataReceipt.DurableAuthor != expectedAuthor)
                    throw new InvalidDataException("Recovered metadata receipt author does not match its PendingApply intent.");
                if (intent.Batch.ReceiveStateOwnedByMetadataStore &&
                    !metadataReceipt.HeaderFingerprint.AsSpan().SequenceEqual(intent.Batch.HeaderFingerprint))
                    throw new InvalidDataException("Recovered metadata receipt header fingerprint does not match its PendingApply intent.");
                recovered++;
            }

            return recovered;
        }
    }

    private MetadataIncomingBatch CreateMetadataBatch(Batch batch, byte[]? wirePayload = null,
        NodeId? senderPeer = null)
    {
        var operations = new List<MetadataOperation>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
        {
            object op = Ops.DecodeOp(entry.Payload).Op;
            Digest16 fingerprint = ChangeIdentity.Fingerprint(
                batch.NamespaceId, entry.RecordId, entry.OpKind, op,
                batch.DurableAuthor, entry.CausalDeps);

            // MetadataStore payloads are opaque relay records. Prefix the 16-byte
            // RecordId so relay/recovery can reconstruct the exact provider mutation.
            byte[] envelope = new byte[16 + entry.Payload.Length];
            entry.RecordId.WriteTo(envelope);
            entry.Payload.Span.CopyTo(envelope.AsSpan(16));
            operations.Add(new MetadataOperation
            {
                AuthorNodeId = batch.DurableAuthor,
                ChangeId = entry.ChangeId,
                Fingerprint = fingerprint.ToArray(),
                Payload = envelope,
                RelayEligible = true
            });
        }

        return new MetadataIncomingBatch
        {
            ScopeId = _scopeId!,
            LocalReplica = _node.NodeId,
            SenderPeer = senderPeer ?? batch.IssuerNodeId,
            SenderEpoch = checked((uint)batch.IssuerEpoch),
            AuthorStreamNodeId = batch.DurableAuthor,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = checked((ulong)batch.CursorStart),
            CursorEnd = checked((ulong)batch.CursorEnd),
            PreviousBatchDigest = batch.PrevBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            HeaderFingerprint = InboxReceiptIdentity.Compute(batch).ToArray(),
            WirePayload = wirePayload?.ToArray() ?? Array.Empty<byte>(),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = operations
        };
    }

    private static List<ProviderMutation> CreateProviderMutations(
        IReadOnlyList<MetadataOperation> operations)
    {
        var mutations = new List<ProviderMutation>(operations.Count);
        foreach (var operation in operations)
        {
            if (operation.Payload.Length <= 16)
                throw new InvalidDataException("Pending metadata operation has no RecordId envelope.");

            RecordId recordId = RecordId.From(operation.Payload.AsSpan(0, 16));
            object op = Ops.DecodeOp(operation.Payload.AsMemory(16)).Op;
            switch (op)
            {
                case OpFieldWrite write:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordId,
                        FieldId = write.FieldId,
                        HlcWall = write.Wall,
                        HlcLogical = write.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = write.KeyEpoch,
                        Value = write.Value.ToArray()
                    });
                    break;
                case OpFieldTomb tomb:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordId,
                        FieldId = tomb.FieldId,
                        IsTombstone = true,
                        HlcWall = tomb.Wall,
                        HlcLogical = tomb.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = tomb.KeyEpoch,
                        Value = Array.Empty<byte>()
                    });
                    break;
                case OpRecordTombstone recordTombstone:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordTombstone.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = recordTombstone.Wall,
                        HlcLogical = recordTombstone.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>()
                    });
                    break;
                default:
                    throw new InvalidDataException("Pending metadata operation has an unsupported operation type.");
            }
        }
        return mutations;
    }

    private ProviderApplyReceipt? ApplyRemoteFieldsCore(
        IEnumerable<(RecordId RecordId, ushort FieldId)> fields, Guid? applyToken)
    {
        ArgumentNullException.ThrowIfNull(fields);
        lock (_gate)
        {
            var mutations = new List<ProviderMutation>();
            foreach (var item in fields.Distinct())
            {
                if (!_node.TryGetRecord(_namespaceId, item.RecordId, out var record) || record is null)
                    continue;

                bool tombstone;
                FieldState? field;
                if (record.State.TryGetValue(item.FieldId, out field))
                    tombstone = false;
                else if (record.FieldTombs.TryGetValue(item.FieldId, out field))
                    tombstone = true;
                else
                    continue;
                if (field is null) continue;

                mutations.Add(new ProviderMutation
                {
                    RecordId = item.RecordId,
                    FieldId = item.FieldId,
                    IsTombstone = tombstone,
                    HlcWall = field.Timestamp.Wall,
                    HlcLogical = field.Timestamp.Logical,
                    AuthorNodeId = field.AuthorNodeId,
                    KeyEpoch = 0,
                    Value = tombstone ? Array.Empty<byte>() : field.Value.ToArray()
                });
            }

            if (mutations.Count == 0) return null;
            mutations.Sort(static (a, b) =>
            {
                int c = CompareRecordIds(a.RecordId, b.RecordId);
                return c != 0 ? c : a.FieldId.CompareTo(b.FieldId);
            });

            Guid token = applyToken ?? CreateApplyToken(_namespaceId, mutations);
            return _provider.ApplyRemoteBatch(token, mutations);
        }
    }

    private static Guid CreateBatchApplyToken(Batch batch)
    {
        Span<byte> identity = stackalloc byte[34];
        BinaryPrimitives.WriteUInt16LittleEndian(identity, checked((ushort)batch.NamespaceId));
        batch.IssuerNodeId.WriteTo(identity.Slice(2, 16));
        batch.BatchId.WriteTo(identity.Slice(18, 16));
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(identity, digest);
        return new Guid(digest.Slice(0, 16));
    }

    private static int CompareRecordIds(RecordId left, RecordId right)
    {
        ulong leftLo = left.Lo, rightLo = right.Lo;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftLo >> (i * 8))).CompareTo((byte)(rightLo >> (i * 8)));
            if (c != 0) return c;
        }

        ulong leftHi = left.Hi, rightHi = right.Hi;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftHi >> (i * 8))).CompareTo((byte)(rightHi >> (i * 8)));
            if (c != 0) return c;
        }

        return 0;
    }

    private static Guid CreateApplyToken(int namespaceId, IReadOnlyList<ProviderMutation> mutations)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> header = stackalloc byte[6];
        BinaryPrimitives.WriteUInt16LittleEndian(header, checked((ushort)namespaceId));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(2), mutations.Count);
        hash.AppendData(header);

        Span<byte> fixedPart = stackalloc byte[53];
        foreach (var mutation in mutations)
        {
            mutation.RecordId.WriteTo(fixedPart);
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(16), mutation.FieldId);
            fixedPart[18] = mutation.IsTombstone ? (byte)1 : (byte)0;
            BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(19), mutation.HlcWall);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(27), mutation.HlcLogical);
            mutation.AuthorNodeId.WriteTo(fixedPart.Slice(31));
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(47), mutation.KeyEpoch);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(49), checked((uint)mutation.Value.Length));
            hash.AppendData(fixedPart);
            hash.AppendData(mutation.Value);
        }

        byte[] digest = hash.GetHashAndReset();
        return new Guid(digest.AsSpan(0, 16));
    }
}
