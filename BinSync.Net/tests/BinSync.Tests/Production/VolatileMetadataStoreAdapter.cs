using System.IO;
using BinSync;
using BinSync.MetadataStore.Conformance;

namespace BinSync.Tests.MetadataStore;

/// <summary>
/// Volatile reference adapter for the semantic conformance suite. It is deliberately
/// test-only and makes no restart-durability claim.
/// </summary>
public sealed class VolatileMetadataStoreAdapter : IMetadataStoreConformanceAdapter, IMetadataOutboundChangeIdentityStore
{
    private readonly Dictionary<PartitionKey, PartitionState> _partitions = new();

    public bool SupportsRestartDurability => false;

    public MetadataBatchReceipt AcceptIncomingBatch(MetadataIncomingBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = _partitions.TryGetValue(partitionKey, out var stored)
            ? stored
            : new PartitionState();
        _ = BuildPendingApplyDecisions(current, batch, Guid.Empty);
        var next = current.Clone();
        var receipt = ApplyIncomingBatch(next, batch);
        _partitions[partitionKey] = next;
        return receipt;
    }

    public MetadataPendingApplyIntent BeginPendingApply(MetadataIncomingBatch batch, Guid applyToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);
        if (applyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(applyToken));

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = _partitions.TryGetValue(partitionKey, out var stored)
            ? stored
            : new PartitionState();
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        bool isDuplicate = current.Receipts.TryGetValue(receiptKey, out var priorReceipt);
        if (isDuplicate && !priorReceipt!.Matches(batch))
            throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");

        if (current.CompletedPendingApplies.TryGetValue(applyToken, out var completed))
        {
            if (!completed.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest))
                throw new InvalidDataException("Completed apply token was reused for different input.");
            throw new MetadataPendingApplyAlreadyFinalizedException();
        }

        if (current.PendingApplies.TryGetValue(applyToken, out var pending))
        {
            if (!SamePendingBatch(pending.Batch, batch))
                throw new InvalidDataException("Pending apply token was reused for different input.");
            return new MetadataPendingApplyIntent(applyToken, CloneBatch(pending.Batch))
            {
                IsDuplicate = isDuplicate,
                ApplyOperations = BuildPendingApplyDecisions(current, pending.Batch, applyToken)
            };
        }

        IReadOnlyList<bool> applyOperations = BuildPendingApplyDecisions(current, batch, applyToken);
        var next = current.Clone();
        next.PendingApplies.Add(applyToken, new PendingApplyValue(CloneBatch(batch)));
        _partitions[partitionKey] = next;
        return new MetadataPendingApplyIntent(applyToken, CloneBatch(batch))
        {
            IsDuplicate = isDuplicate,
            ApplyOperations = applyOperations
        };
    }

    public IReadOnlyList<MetadataPendingApplyIntent> ReadPendingApplyIntents(
        string scopeId,
        NodeId localReplica)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return Array.Empty<MetadataPendingApplyIntent>();

        return state.PendingApplies
            .Select(pair => new MetadataPendingApplyIntent(pair.Key, CloneBatch(pair.Value.Batch))
            {
                IsDuplicate = state.Receipts.ContainsKey(new ReceiptKey(
                    pair.Value.Batch.SenderPeer, pair.Value.Batch.SenderEpoch,
                    pair.Value.Batch.NamespaceId, pair.Value.Batch.BatchId)),
                ApplyOperations = BuildPendingApplyDecisions(state, pair.Value.Batch, pair.Key)
            })
            .ToArray();
    }

    public MetadataBatchReceipt FinalizePendingApply(
        string scopeId,
        NodeId localReplica,
        MetadataProviderApplyReceipt providerReceipt)
    {
        ArgumentNullException.ThrowIfNull(providerReceipt);
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (providerReceipt.ApplyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(providerReceipt));
        ValidateDigest(providerReceipt.BatchDigest, nameof(providerReceipt.BatchDigest));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!_partitions.TryGetValue(partitionKey, out var current))
            throw new InvalidOperationException("Pending apply intent does not exist.");

        if (current.CompletedPendingApplies.TryGetValue(providerReceipt.ApplyToken, out var completed))
        {
            if (!completed.BatchDigest.AsSpan().SequenceEqual(providerReceipt.BatchDigest))
                throw new InvalidDataException("Provider receipt conflicts with the completed apply token.");
            return CloneReceipt(completed.Receipt, isDuplicate: true);
        }

        if (!current.PendingApplies.TryGetValue(providerReceipt.ApplyToken, out var pending))
            throw new InvalidOperationException("Pending apply intent does not exist.");
        if (!pending.Batch.BatchDigest.AsSpan().SequenceEqual(providerReceipt.BatchDigest))
            throw new InvalidDataException("Provider receipt does not match the pending apply input.");

        // Apply incoming metadata and remove the raw intent in one state publication.
        // A durable adapter must implement this as one metadata transaction.
        var next = current.Clone();
        var batch = next.PendingApplies[providerReceipt.ApplyToken].Batch;
        var receipt = ApplyIncomingBatch(next, batch);
        next.PendingApplies.Remove(providerReceipt.ApplyToken);
        next.CompletedPendingApplies.Add(providerReceipt.ApplyToken,
            new CompletedPendingApplyValue(providerReceipt.BatchDigest.ToArray(), CloneReceipt(receipt, receipt.IsDuplicate)));
        _partitions[partitionKey] = next;
        return receipt;
    }

    public IMetadataStoreSnapshot OpenSnapshot(string scopeId, NodeId localReplica)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var state = _partitions.TryGetValue(partitionKey, out var stored)
            ? stored.Clone()
            : new PartitionState();
        return new VolatileSnapshot(state);
    }

    private static IReadOnlyList<bool> BuildPendingApplyDecisions(
        PartitionState state, MetadataIncomingBatch batch, Guid applyToken)
    {
        var decisions = new bool[batch.Operations.Count];
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        if (state.Receipts.ContainsKey(receiptKey))
            return Array.AsReadOnly(decisions);

        foreach (var otherPending in state.PendingApplies)
        {
            if (otherPending.Key == applyToken ||
                otherPending.Value.Batch.NamespaceId != batch.NamespaceId)
                continue;
            foreach (var pendingOperation in otherPending.Value.Batch.Operations)
            {
                for (int i = 0; i < batch.Operations.Count; i++)
                {
                    var operation = batch.Operations[i];
                    if (operation.ChangeId != pendingOperation.ChangeId)
                        continue;
                    if (!operation.Fingerprint.AsSpan().SequenceEqual(pendingOperation.Fingerprint))
                        throw new InvalidDataException("Pending ChangeId was reused with a different canonical fingerprint.");
                    throw new InvalidOperationException("ChangeId is reserved by another pending apply and must be retried after it finalizes.");
                }
            }
        }

        for (int i = 0; i < batch.Operations.Count; i++)
        {
            var operation = batch.Operations[i];
            var key = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (state.ChangeLedger.TryGetValue(key, out var priorFingerprint))
            {
                if (!priorFingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                decisions[i] = false;
            }
            else
            {
                decisions[i] = true;
            }
        }
        return Array.AsReadOnly(decisions);
    }

    private static MetadataBatchReceipt ApplyIncomingBatch(PartitionState next, MetadataIncomingBatch batch)
    {
        var receiptKey = new ReceiptKey(
            batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId);
        if (next.Receipts.TryGetValue(receiptKey, out var priorReceipt))
        {
            if (!priorReceipt.Matches(batch))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");

            return new MetadataBatchReceipt(
                priorReceipt.CursorStart,
                priorReceipt.CursorEnd,
                priorReceipt.BatchDigest.ToArray(),
                priorReceipt.NewRelayOperationCount,
                IsDuplicate: true)
            {
                DurableAuthor = priorReceipt.DurableAuthor,
                HeaderFingerprint = priorReceipt.HeaderFingerprint.ToArray()
            };
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.AuthorStreamNodeId);
        var receive = next.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0);
        var chainKey = new ReceiveChainKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId);
        var chain = next.ReceiveChains.TryGetValue(chainKey, out var priorChain)
            ? priorChain
            : new ReceiveChainState(new byte[16]);

        ulong expectedStart = checked(receive.CursorEnd + 1);
        if (batch.CursorStart != expectedStart ||
            !chain.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue the durable author cursor and sender digest chain.");

        int relayAdded = 0;
        foreach (var operation in batch.Operations)
        {
            var ledgerKey = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (next.ChangeLedger.TryGetValue(ledgerKey, out var priorFingerprint))
            {
                if (!priorFingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                continue;
            }

            next.ChangeLedger.Add(ledgerKey, operation.Fingerprint.ToArray());
            if (!operation.RelayEligible)
                continue;

            var relayKey = new RelayKey(batch.NamespaceId, operation.AuthorNodeId);
            if (!next.Relay.TryGetValue(relayKey, out var entries))
            {
                entries = new List<MetadataRelayItem>();
                next.Relay.Add(relayKey, entries);
            }

            ulong previousHighWatermark = next.RelayHighWatermarks.TryGetValue(relayKey, out var relayHighWatermark)
                ? relayHighWatermark
                : 0;
            ulong sequence = checked(previousHighWatermark + 1);
            entries.Add(new MetadataRelayItem(
                sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray())
            { OriginAuthorEpoch = operation.OriginAuthorEpoch, OriginSequence = operation.OriginSequence });
            next.RelayHighWatermarks[relayKey] = sequence;
            relayAdded++;
        }

        next.Receives[receiveKey] = new ReceiveState(batch.CursorEnd);
        next.ReceiveChains[chainKey] = new ReceiveChainState(batch.BatchDigest.ToArray());
        next.Receipts.Add(receiptKey, new ReceiptValue(
            batch.CursorStart,
            batch.CursorEnd,
            batch.PreviousBatchDigest.ToArray(),
            batch.BatchDigest.ToArray(),
            relayAdded,
            ResolveAuthorStream(batch),
            batch.HeaderFingerprint.ToArray()));

        return new MetadataBatchReceipt(
            batch.CursorStart,
            batch.CursorEnd,
            batch.BatchDigest.ToArray(),
            relayAdded,
            IsDuplicate: false)
        {
            DurableAuthor = ResolveAuthorStream(batch),
            HeaderFingerprint = batch.HeaderFingerprint.ToArray()
        };
    }

    private static bool SamePendingBatch(MetadataIncomingBatch left, MetadataIncomingBatch right)
        => left.ScopeId == right.ScopeId
            && left.LocalReplica == right.LocalReplica
            && left.SenderPeer == right.SenderPeer
            && left.SenderEpoch == right.SenderEpoch
            && left.AuthorStreamNodeId == right.AuthorStreamNodeId
            && left.NamespaceId == right.NamespaceId
            && left.BatchId == right.BatchId
            && left.CursorStart == right.CursorStart
            && left.CursorEnd == right.CursorEnd
            && left.BatchDigest.AsSpan().SequenceEqual(right.BatchDigest)
            && left.WirePayload.AsSpan().SequenceEqual(right.WirePayload)
            && left.HeaderFingerprint.AsSpan().SequenceEqual(right.HeaderFingerprint)
            && left.ReceiveStateOwnedByMetadataStore == right.ReceiveStateOwnedByMetadataStore;

    private static MetadataIncomingBatch CloneBatch(MetadataIncomingBatch batch)
        => new()
        {
            ScopeId = batch.ScopeId,
            LocalReplica = batch.LocalReplica,
            SenderPeer = batch.SenderPeer,
            SenderEpoch = batch.SenderEpoch,
            AuthorStreamNodeId = batch.AuthorStreamNodeId,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = batch.CursorStart,
            CursorEnd = batch.CursorEnd,
            PreviousBatchDigest = batch.PreviousBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            HeaderFingerprint = batch.HeaderFingerprint.ToArray(),
            WirePayload = batch.WirePayload.ToArray(),
            ReceiveStateOwnedByMetadataStore = batch.ReceiveStateOwnedByMetadataStore,
            Operations = batch.Operations.Select(operation => new MetadataOperation
            {
                AuthorNodeId = operation.AuthorNodeId,
                ChangeId = operation.ChangeId,
                Fingerprint = operation.Fingerprint.ToArray(),
                Payload = operation.Payload.ToArray(),
                RelayEligible = operation.RelayEligible,
                OriginAuthorEpoch = operation.OriginAuthorEpoch,
                OriginSequence = operation.OriginSequence
            }).ToArray()
        };

    private static MetadataBatchReceipt CloneReceipt(MetadataBatchReceipt receipt, bool isDuplicate)
        => new MetadataBatchReceipt(receipt.CursorStart, receipt.CursorEnd, receipt.BatchDigest.ToArray(),
            receipt.NewRelayOperationCount, isDuplicate)
        {
            DurableAuthor = receipt.DurableAuthor,
            HeaderFingerprint = receipt.HeaderFingerprint.ToArray()
        };

    private static NodeId ResolveAuthorStream(MetadataIncomingBatch batch)
        => batch.AuthorStreamNodeId;

    public IReadOnlyList<MetadataOutboundItem> FreezeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        ArgumentNullException.ThrowIfNull(operations);

        var seen = new HashSet<BatchId>();
        ulong previousOriginSequence = 0;
        foreach (var operation in operations)
        {
            if (operation.ChangeId.IsZero)
                throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(operations));
            if (operation.OriginAuthorEpoch == 0 || operation.OriginSequence == 0 ||
                operation.OriginSequence <= previousOriginSequence)
                throw new InvalidDataException("Published operation has invalid immutable Origin provenance.");
            previousOriginSequence = operation.OriginSequence;
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("Outbound ChangeId values must be unique within a freeze operation.", nameof(operations));
        }

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = _partitions.TryGetValue(partitionKey, out var stored)
            ? stored
            : new PartitionState();
        if (operations.Count == 0)
            return Array.Empty<MetadataOutboundItem>();

        var key = new OutboundKey(namespaceId, localAuthor);
        var existingByChangeId = new Dictionary<BatchId, MetadataOutboundItem>();
        var newOperations = new List<MetadataFrozenOperation>(operations.Count);
        bool sawNewOperation = false;
        ulong previousExistingSequence = 0;
        foreach (var operation in operations)
        {
            var changeKey = new OutboundChangeKey(namespaceId, operation.ChangeId);
            var ledgerKey = new LedgerKey(namespaceId, operation.ChangeId);
            bool alreadyFrozen = current.OutboundChangeIds.Contains(changeKey);
            if (current.ChangeLedger.TryGetValue(ledgerKey, out var priorFingerprint))
            {
                if (!priorFingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("Outbound ChangeId conflicts with the global ChangeId ledger.");
                if (!alreadyFrozen)
                    throw new InvalidDataException("ChangeId is already known from an incoming route and cannot be published as a new local operation.");
            }
            else if (alreadyFrozen)
            {
                throw new InvalidDataException("Frozen outbound ChangeId is missing its global fingerprint.");
            }

            if (!alreadyFrozen)
            {
                sawNewOperation = true;
                newOperations.Add(operation);
                continue;
            }
            if (sawNewOperation)
                throw new InvalidDataException("Previously frozen outbound operations must form a prefix of an incremental retry.");

            if (!current.Outbound.TryGetValue(key, out var priorEntries))
                throw new InvalidDataException("Outbound ChangeId is already frozen in another author stream.");

            var prior = priorEntries.FirstOrDefault(item => item.ChangeId == operation.ChangeId);
            if (prior is null || !prior.Payload.AsSpan().SequenceEqual(operation.Payload) ||
                prior.OriginAuthorEpoch != operation.OriginAuthorEpoch ||
                prior.OriginSequence != operation.OriginSequence)
                throw new InvalidDataException("Outbound ChangeId was retried with conflicting author, Origin or payload.");
            if (prior.Sequence <= previousExistingSequence)
                throw new InvalidDataException("Previously frozen outbound operations are not in author-sequence order.");
            previousExistingSequence = prior.Sequence;
            existingByChangeId.Add(operation.ChangeId, CloneOutboundItem(prior));
        }

        var next = current.Clone();
        var frozenByChangeId = new Dictionary<BatchId, MetadataOutboundItem>(operations.Count);
        foreach (var existing in existingByChangeId)
            frozenByChangeId.Add(existing.Key, existing.Value);

        if (newOperations.Count > 0)
        {
            if (!next.Outbound.TryGetValue(key, out var entries))
            {
                entries = new List<MetadataOutboundItem>();
                next.Outbound.Add(key, entries);
            }

            foreach (var operation in newOperations)
            {
                ulong previousHighWatermark = next.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark)
                    ? outboundHighWatermark
                    : 0;
                ulong sequence = checked(previousHighWatermark + 1);
                var item = new MetadataOutboundItem(sequence, localAuthor, operation.ChangeId, operation.Payload.ToArray())
                { OriginAuthorEpoch = operation.OriginAuthorEpoch, OriginSequence = operation.OriginSequence };
                entries.Add(item);
                next.OutboundChangeIds.Add(new OutboundChangeKey(namespaceId, operation.ChangeId));
                next.ChangeLedger.Add(new LedgerKey(namespaceId, operation.ChangeId), operation.Fingerprint.ToArray());
                next.OutboundHighWatermarks[key] = sequence;
                frozenByChangeId.Add(operation.ChangeId, item);
            }
        }

        _partitions[partitionKey] = next;
        var result = new MetadataOutboundItem[operations.Count];
        for (int i = 0; i < operations.Count; i++)
            result[i] = CloneOutboundItem(frozenByChangeId[operations[i].ChangeId]);
        return result;
    }

    public bool TryGetOutboundChangeFingerprint(
        string scopeId, NodeId localReplica, int namespaceId, BatchId changeId, out ReadOnlyMemory<byte> fingerprint)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
        if (changeId.IsZero)
            throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(changeId));
        fingerprint = ReadOnlyMemory<byte>.Empty;
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return false;

        var key = new OutboundChangeKey(namespaceId, changeId);
        var ledgerKey = new LedgerKey(namespaceId, changeId);
        if (!state.ChangeLedger.TryGetValue(ledgerKey, out var priorFingerprint))
        {
            if (state.OutboundChangeIds.Contains(key))
                throw new InvalidDataException("Frozen outbound ChangeId is missing its global fingerprint.");
            return false;
        }
        if (!state.OutboundChangeIds.Contains(key))
            throw new InvalidDataException("Provider ChangeId is already owned by an incoming route and cannot be published locally.");
        fingerprint = priorFingerprint;
        return true;
    }

    public ulong AcquireOutboundLease(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        Guid consumerId)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (consumerId == Guid.Empty)
            throw new ArgumentException("ConsumerId must be non-empty.", nameof(consumerId));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = _partitions.TryGetValue(partitionKey, out var stored)
            ? stored
            : new PartitionState();
        var key = new OutboundKey(namespaceId, localAuthor);
        ulong previous = current.OutboundLeases.TryGetValue(key, out var prior) ? prior.Generation : 0;
        ulong generation = checked(previous + 1);

        var next = current.Clone();
        next.OutboundLeases[key] = new LeaseState(generation, consumerId);
        _partitions[partitionKey] = next;
        return generation;
    }

    public MetadataOutboundPage ReadOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong afterSequence,
        int limit)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            throw new InvalidOperationException("Outbound lease is not active.");

        var key = new OutboundKey(namespaceId, localAuthor);
        ValidateOutboundLease(state, key, leaseGeneration);
        ulong acknowledged = state.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        if (!state.Outbound.TryGetValue(key, out var entries))
        {
            if (afterSequence != 0)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), acknowledged);
        }

        ulong highWatermark = state.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
        if (afterSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));

        ulong cursor = Math.Max(afterSequence, acknowledged);
        if (limit <= 0)
            return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), cursor);

        var page = entries
            .Where(entry => entry.Sequence > cursor)
            .Take(limit)
            .Select(CloneOutboundItem)
            .ToArray();
        ulong scannedThrough = page.Length == 0 ? cursor : page[^1].Sequence;
        return new MetadataOutboundPage(page, scannedThrough);
    }

    public void AcknowledgeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong safeThroughSequence)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!_partitions.TryGetValue(partitionKey, out var current))
            throw new InvalidOperationException("Outbound lease is not active.");

        var key = new OutboundKey(namespaceId, localAuthor);
        ValidateOutboundLease(current, key, leaseGeneration);
        ulong previous = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = current.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
        if (safeThroughSequence < previous || safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        next.OutboundAck[key] = safeThroughSequence;
        _partitions[partitionKey] = next;
    }

    public void CompactOutboundThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong safeThroughSequence)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!_partitions.TryGetValue(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new OutboundKey(namespaceId, localAuthor);
        ulong acknowledged = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        if (safeThroughSequence > acknowledged)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        if (next.Outbound.TryGetValue(key, out var entries))
            entries.RemoveAll(entry => entry.Sequence <= safeThroughSequence);
        _partitions[partitionKey] = next;
    }

    public ulong GetOutboundHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor)
    {
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.OutboundHighWatermarks.TryGetValue(new OutboundKey(namespaceId, localAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    }

    public MetadataRelayPage ReadRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong afterSequence,
        int limit)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
        {
            if (afterSequence != 0)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), 0);
        }

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong acknowledged = state.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        if (!state.Relay.TryGetValue(key, out var entries))
        {
            if (afterSequence != 0)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), acknowledged);
        }

        ulong highWatermark = state.RelayHighWatermarks.TryGetValue(key, out var relayHighWatermark) ? relayHighWatermark : 0;
        if (afterSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));

        ulong cursor = Math.Max(afterSequence, acknowledged);
        if (limit <= 0)
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), cursor);

        var page = entries
            .Where(entry => entry.Sequence > cursor)
            .Take(limit)
            .Select(CloneRelayItem)
            .ToArray();

        ulong scannedThrough = page.Length == 0 ? cursor : page[^1].Sequence;
        return new MetadataRelayPage(page, scannedThrough);
    }

    public void AcknowledgeRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!_partitions.TryGetValue(partitionKey, out var current))
        {
            if (safeThroughSequence == 0) return;
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
        }

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong previous = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = current.RelayHighWatermarks.TryGetValue(key, out var relayHighWatermark) ? relayHighWatermark : 0;

        if (safeThroughSequence < previous || safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        next.RelayAck[key] = safeThroughSequence;
        _partitions[partitionKey] = next;
    }

    public ulong GetReceiveCursor(
        string scopeId,
        NodeId localReplica,
        NodeId sender,
        uint senderEpoch,
        int namespaceId)
    {
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.Receives
            .Where(pair => pair.Key.Sender == sender && pair.Key.Epoch == senderEpoch &&
                pair.Key.NamespaceId == namespaceId)
            .Select(pair => pair.Value.CursorEnd)
            .DefaultIfEmpty(0)
            .Max();
    }

    public void CompactRelayThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!_partitions.TryGetValue(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong acknowledged = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        if (safeThroughSequence > acknowledged)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        if (next.Relay.TryGetValue(key, out var entries))
            entries.RemoveAll(entry => entry.Sequence <= safeThroughSequence);
        _partitions[partitionKey] = next;
    }

    public ulong GetRelayHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.RelayHighWatermarks.TryGetValue(new RelayKey(namespaceId, originalAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    }

    public void Dispose()
    {
    }

    private static void ValidateBatch(MetadataIncomingBatch batch)
    {
        if (string.IsNullOrWhiteSpace(batch.ScopeId))
            throw new ArgumentException("ScopeId is required.", nameof(batch));
        if (batch.LocalReplica.IsZero || batch.SenderPeer.IsZero || batch.BatchId.IsZero)
            throw new ArgumentException("Replica, sender and batch identities must be non-zero.", nameof(batch));
        if ((uint)batch.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(batch.NamespaceId));
        if (batch.CursorStart == 0 || batch.CursorEnd < batch.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(batch.CursorStart));
        ValidateDigest(batch.PreviousBatchDigest, nameof(batch.PreviousBatchDigest));
        ValidateDigest(batch.BatchDigest, nameof(batch.BatchDigest));
        if (batch.HeaderFingerprint.Length != 0)
            ValidateDigest(batch.HeaderFingerprint, nameof(batch.HeaderFingerprint));
        if (batch.ReceiveStateOwnedByMetadataStore && batch.HeaderFingerprint.Length != 16)
            throw new ArgumentException("Metadata-authoritative receive requires a full header fingerprint.", nameof(batch));
        ArgumentNullException.ThrowIfNull(batch.Operations);
        if (batch.AuthorStreamNodeId.IsZero)
            throw new InvalidDataException("v3 batch has no resolved author stream.");

        var seen = new HashSet<BatchId>();
        ulong previousOriginSequence = 0;
        foreach (var operation in batch.Operations)
        {
            if (operation.AuthorNodeId.IsZero || operation.AuthorNodeId != batch.AuthorStreamNodeId ||
                operation.ChangeId.IsZero || operation.OriginAuthorEpoch == 0 ||
                operation.OriginSequence == 0 || operation.OriginSequence <= previousOriginSequence)
                throw new InvalidDataException("v3 operation has invalid immutable Origin provenance.");
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("ChangeId values must be unique within one incoming batch.", nameof(batch));
            previousOriginSequence = operation.OriginSequence;
        }
    }

    private static void ValidateDigest(byte[]? value, string name)
    {
        ArgumentNullException.ThrowIfNull(value, name);
        if (value.Length != 16)
            throw new ArgumentException("Digest/fingerprint must be 16 bytes.", name);
    }

    private static void ValidatePartitionArguments(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero || originalAuthor.IsZero)
            throw new ArgumentException("Replica and author identities must be non-zero.");
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
    }

    private static MetadataRelayItem CloneRelayItem(MetadataRelayItem item)
        => new MetadataRelayItem(item.Sequence, item.OriginalAuthor, item.ChangeId, item.Payload.ToArray())
        { OriginAuthorEpoch = item.OriginAuthorEpoch, OriginSequence = item.OriginSequence };

    private static MetadataOutboundItem CloneOutboundItem(MetadataOutboundItem item)
        => new MetadataOutboundItem(item.Sequence, item.AuthorNodeId, item.ChangeId, item.Payload.ToArray())
        { OriginAuthorEpoch = item.OriginAuthorEpoch, OriginSequence = item.OriginSequence };

    private static void ValidateOutboundLease(PartitionState state, OutboundKey key, ulong generation)
    {
        if (!state.OutboundLeases.TryGetValue(key, out var lease) || lease.Generation != generation)
            throw new InvalidOperationException("Outbound lease was superseded by a newer consumer.");
    }

    private sealed class VolatileSnapshot : IMetadataStoreSnapshot
    {
        private PartitionState? _state;

        public VolatileSnapshot(PartitionState state) => _state = state;

        public ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId)
        {
            var state = GetState();
            return state.Receives
                .Where(pair => pair.Key.Sender == sender && pair.Key.Epoch == senderEpoch &&
                    pair.Key.NamespaceId == namespaceId)
                .Select(pair => pair.Value.CursorEnd)
                .DefaultIfEmpty(0)
                .Max();
        }

        public ulong GetRelayHighWatermark(int namespaceId, NodeId originalAuthor)
        {
            var state = GetState();
            return state.RelayHighWatermarks.TryGetValue(new RelayKey(namespaceId, originalAuthor), out var high)
                ? high
                : 0;
        }

        public ulong GetOutboundHighWatermark(int namespaceId, NodeId localAuthor)
        {
            var state = GetState();
            return state.OutboundHighWatermarks.TryGetValue(new OutboundKey(namespaceId, localAuthor), out var high)
                ? high
                : 0;
        }

        public MetadataRelayPage ReadRelay(int namespaceId, NodeId originalAuthor, ulong afterSequence, int limit)
        {
            var state = GetState();
            var key = new RelayKey(namespaceId, originalAuthor);
            ulong acknowledged = state.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
            ulong high = state.RelayHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (afterSequence > high)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            ulong cursor = Math.Max(afterSequence, acknowledged);
            if (limit <= 0 || !state.Relay.TryGetValue(key, out var entries))
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), cursor);
            var page = entries.Where(item => item.Sequence > cursor).Take(limit).Select(CloneRelayItem).ToArray();
            return new MetadataRelayPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }

        public MetadataOutboundPage ReadOutbound(int namespaceId, NodeId localAuthor, ulong afterSequence, int limit)
        {
            var state = GetState();
            var key = new OutboundKey(namespaceId, localAuthor);
            ulong acknowledged = state.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
            ulong high = state.OutboundHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (afterSequence > high)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            ulong cursor = Math.Max(afterSequence, acknowledged);
            if (limit <= 0 || !state.Outbound.TryGetValue(key, out var entries))
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), cursor);
            var page = entries.Where(item => item.Sequence > cursor).Take(limit).Select(CloneOutboundItem).ToArray();
            return new MetadataOutboundPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }

        public void Dispose() => _state = null;

        private PartitionState GetState()
            => _state ?? throw new ObjectDisposedException(nameof(VolatileSnapshot));
    }

    private readonly record struct PartitionKey(string ScopeId, NodeId LocalReplica);
    private readonly record struct ReceiptKey(NodeId Sender, uint Epoch, int NamespaceId, BatchId BatchId);
    private readonly record struct ReceiveKey(NodeId Sender, uint Epoch, int NamespaceId, NodeId AuthorStream);
    private readonly record struct ReceiveChainKey(NodeId Sender, uint Epoch, int NamespaceId);
    private readonly record struct LedgerKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct RelayKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundChangeKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct LeaseState(ulong Generation, Guid ConsumerId);
    private readonly record struct ReceiveState(ulong CursorEnd);
    private sealed record ReceiveChainState(byte[] LastBatchDigest);

    private sealed class ReceiptValue(
        ulong cursorStart,
        ulong cursorEnd,
        byte[] previousBatchDigest,
        byte[] batchDigest,
        int newRelayOperationCount,
        NodeId durableAuthor = default,
        byte[]? headerFingerprint = null)
    {
        public ulong CursorStart { get; } = cursorStart;
        public ulong CursorEnd { get; } = cursorEnd;
        public byte[] PreviousBatchDigest { get; } = previousBatchDigest;
        public byte[] BatchDigest { get; } = batchDigest;
        public int NewRelayOperationCount { get; } = newRelayOperationCount;
        public NodeId DurableAuthor { get; } = durableAuthor;
        public byte[] HeaderFingerprint { get; } = headerFingerprint ?? Array.Empty<byte>();

        public bool Matches(MetadataIncomingBatch batch)
            => CursorStart == batch.CursorStart
                && CursorEnd == batch.CursorEnd
                && PreviousBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest)
                && BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest)
                && HeaderFingerprint.AsSpan().SequenceEqual(batch.HeaderFingerprint);
    }

    private sealed record PendingApplyValue(MetadataIncomingBatch Batch);
    private sealed record CompletedPendingApplyValue(byte[] BatchDigest, MetadataBatchReceipt Receipt);

    private sealed class PartitionState
    {
        public Dictionary<ReceiptKey, ReceiptValue> Receipts { get; } = new();
        public Dictionary<ReceiveKey, ReceiveState> Receives { get; } = new();
        public Dictionary<ReceiveChainKey, ReceiveChainState> ReceiveChains { get; } = new();
        public Dictionary<LedgerKey, byte[]> ChangeLedger { get; } = new();
        public Dictionary<RelayKey, List<MetadataRelayItem>> Relay { get; } = new();
        public Dictionary<RelayKey, ulong> RelayAck { get; } = new();
        public Dictionary<RelayKey, ulong> RelayHighWatermarks { get; } = new();
        public Dictionary<OutboundKey, List<MetadataOutboundItem>> Outbound { get; } = new();
        public Dictionary<OutboundKey, ulong> OutboundHighWatermarks { get; } = new();
        public Dictionary<OutboundKey, ulong> OutboundAck { get; } = new();
        public Dictionary<OutboundKey, LeaseState> OutboundLeases { get; } = new();
        public HashSet<OutboundChangeKey> OutboundChangeIds { get; } = new();
        public Dictionary<Guid, PendingApplyValue> PendingApplies { get; } = new();
        public Dictionary<Guid, CompletedPendingApplyValue> CompletedPendingApplies { get; } = new();

        public PartitionState Clone()
        {
            var clone = new PartitionState();
            foreach (var pair in Receipts)
                clone.Receipts.Add(pair.Key, new ReceiptValue(
                    pair.Value.CursorStart,
                    pair.Value.CursorEnd,
                    pair.Value.PreviousBatchDigest.ToArray(),
                    pair.Value.BatchDigest.ToArray(),
                    pair.Value.NewRelayOperationCount,
                    pair.Value.DurableAuthor,
                    pair.Value.HeaderFingerprint.ToArray()));
            foreach (var pair in Receives)
                clone.Receives.Add(pair.Key, new ReceiveState(pair.Value.CursorEnd));
            foreach (var pair in ReceiveChains)
                clone.ReceiveChains.Add(pair.Key, new ReceiveChainState(pair.Value.LastBatchDigest.ToArray()));
            foreach (var pair in ChangeLedger)
                clone.ChangeLedger.Add(pair.Key, pair.Value.ToArray());
            foreach (var pair in Relay)
                clone.Relay.Add(pair.Key, pair.Value.Select(CloneRelayItem).ToList());
            foreach (var pair in RelayAck)
                clone.RelayAck.Add(pair.Key, pair.Value);
            foreach (var pair in RelayHighWatermarks)
                clone.RelayHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundHighWatermarks)
                clone.OutboundHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in Outbound)
                clone.Outbound.Add(pair.Key, pair.Value.Select(CloneOutboundItem).ToList());
            foreach (var pair in OutboundAck)
                clone.OutboundAck.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundLeases)
                clone.OutboundLeases.Add(pair.Key, pair.Value);
            foreach (var key in OutboundChangeIds)
                clone.OutboundChangeIds.Add(key);
            foreach (var pair in PendingApplies)
                clone.PendingApplies.Add(pair.Key, new PendingApplyValue(CloneBatch(pair.Value.Batch)));
            foreach (var pair in CompletedPendingApplies)
                clone.CompletedPendingApplies.Add(pair.Key, new CompletedPendingApplyValue(
                    pair.Value.BatchDigest.ToArray(), CloneReceipt(pair.Value.Receipt, pair.Value.Receipt.IsDuplicate)));
            return clone;
        }
    }
}
