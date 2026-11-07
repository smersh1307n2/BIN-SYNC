using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable BIN-SYNC Metadata Store backed by Fld1.Storage.
/// Each logical metadata entry is stored under its own key; transactions persist only diffs.
/// </summary>
public class StorageMetadataStore : IMetadataStore
{
    private readonly object _gate = new();
    private readonly Dictionary<PartitionKey, PartitionState> _partitions = new();
    private readonly StorageDatabase _storage;
    private readonly string? _directory;
    private readonly bool _deleteOnDispose;
    private readonly bool _ownsStorage;
    private bool _disposed;

    public StorageMetadataStore(string directory, ulong partitionId = 301, bool deleteOnDispose = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = directory;
        _deleteOnDispose = deleteOnDispose;
        _ownsStorage = true;
        _storage = StorageDatabase.Open(directory, partitionId);
    }

    /// <summary>Attach to a caller-owned shared Storage database.</summary>
    public StorageMetadataStore(StorageDatabase storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public bool SupportsRestartDurability => true;

    public MetadataBatchReceipt AcceptIncomingBatch(MetadataIncomingBatch batch)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = GetOrCreatePartition(partitionKey);
        var next = current.Clone();
        var receipt = ApplyIncomingBatch(next, batch);
        PersistPartition(partitionKey, current, next);
        return receipt;
    
        }
    }

    public MetadataPendingApplyIntent BeginPendingApply(MetadataIncomingBatch batch, Guid applyToken)
    {
        lock (_gate)
        {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);
        if (applyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(applyToken));

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = GetOrCreatePartition(partitionKey);

        if (current.CompletedPendingApplies.TryGetValue(applyToken, out var completed))
        {
            if (!completed.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest))
                throw new InvalidDataException("Completed apply token was reused for different input.");
            throw new InvalidOperationException("Pending apply token has already been finalized.");
        }

        if (current.PendingApplies.TryGetValue(applyToken, out var pending))
        {
            if (!SamePendingBatch(pending.Batch, batch))
                throw new InvalidDataException("Pending apply token was reused for different input.");
            return new MetadataPendingApplyIntent(applyToken, CloneBatch(pending.Batch));
        }

        var next = current.Clone();
        next.PendingApplies.Add(applyToken, new PendingApplyValue(CloneBatch(batch)));
        PersistPartition(partitionKey, current, next);
        return new MetadataPendingApplyIntent(applyToken, CloneBatch(batch));
    
        }
    }

    public IReadOnlyList<MetadataPendingApplyIntent> ReadPendingApplyIntents(
        string scopeId,
        NodeId localReplica)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return Array.Empty<MetadataPendingApplyIntent>();

        return state.PendingApplies
            .Select(pair => new MetadataPendingApplyIntent(pair.Key, CloneBatch(pair.Value.Batch)))
            .ToArray();
    
        }
    }

    public MetadataBatchReceipt FinalizePendingApply(
        string scopeId,
        NodeId localReplica,
        MetadataProviderApplyReceipt providerReceipt)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(providerReceipt);
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (providerReceipt.ApplyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(providerReceipt));
        ValidateDigest(providerReceipt.BatchDigest, nameof(providerReceipt.BatchDigest));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
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
        PersistPartition(partitionKey, current, next);
        return receipt;
    
        }
    }

    public IMetadataStoreSnapshot OpenSnapshot(string scopeId, NodeId localReplica)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var state = GetOrCreatePartition(partitionKey).Clone();
        return new VolatileSnapshot(state);
    
        }
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
                IsDuplicate: true);
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, ResolveAuthorStream(batch));
        var receive = next.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0, new byte[16]);

        ulong expectedStart = checked(receive.CursorEnd + 1);
        if (batch.CursorStart != expectedStart ||
            !receive.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue the durable receive cursor and digest chain.");

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
                sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray()));
            next.RelayHighWatermarks[relayKey] = sequence;
            relayAdded++;
        }

        next.Receives[receiveKey] = new ReceiveState(batch.CursorEnd, batch.BatchDigest.ToArray());
        next.Receipts.Add(receiptKey, new ReceiptValue(
            batch.CursorStart,
            batch.CursorEnd,
            batch.PreviousBatchDigest.ToArray(),
            batch.BatchDigest.ToArray(),
            relayAdded));

        return new MetadataBatchReceipt(
            batch.CursorStart,
            batch.CursorEnd,
            batch.BatchDigest.ToArray(),
            relayAdded,
            IsDuplicate: false);
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
            && left.BatchDigest.AsSpan().SequenceEqual(right.BatchDigest);

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
            Operations = batch.Operations.Select(operation => new MetadataOperation
            {
                AuthorNodeId = operation.AuthorNodeId,
                ChangeId = operation.ChangeId,
                Fingerprint = operation.Fingerprint.ToArray(),
                Payload = operation.Payload.ToArray(),
                RelayEligible = operation.RelayEligible
            }).ToArray()
        };

    private static MetadataBatchReceipt CloneReceipt(MetadataBatchReceipt receipt, bool isDuplicate)
        => new(receipt.CursorStart, receipt.CursorEnd, receipt.BatchDigest.ToArray(),
            receipt.NewRelayOperationCount, isDuplicate);

    public IReadOnlyList<MetadataOutboundItem> FreezeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations)
    {
        lock (_gate)
        {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        ArgumentNullException.ThrowIfNull(operations);

        var seen = new HashSet<BatchId>();
        foreach (var operation in operations)
        {
            if (operation.ChangeId.IsZero)
                throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(operations));
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("Outbound ChangeId values must be unique within a freeze operation.", nameof(operations));
        }

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = GetOrCreatePartition(partitionKey);
        if (operations.Count == 0)
            return Array.Empty<MetadataOutboundItem>();

        var key = new OutboundKey(namespaceId, localAuthor);
        var existing = new List<MetadataOutboundItem>(operations.Count);
        int existingCount = 0;
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

            if (!alreadyFrozen) continue;
            if (!current.Outbound.TryGetValue(key, out var priorEntries))
                throw new InvalidDataException("Outbound ChangeId is already frozen in another author stream.");

            var prior = priorEntries.FirstOrDefault(item => item.ChangeId == operation.ChangeId);
            if (prior is null || !prior.Payload.AsSpan().SequenceEqual(operation.Payload))
                throw new InvalidDataException("Outbound ChangeId was retried with conflicting author or payload.");
            existing.Add(CloneOutboundItem(prior));
            existingCount++;
        }

        // A freeze retry is idempotent only when the complete original batch is
        // present. A mixed old/new batch is ambiguous and must be retried as a whole.
        if (existingCount != 0)
        {
            if (existingCount != operations.Count)
                throw new InvalidDataException("Outbound freeze retry partially overlaps an earlier freeze.");
            return existing;
        }

        var next = current.Clone();
        if (!next.Outbound.TryGetValue(key, out var entries))
        {
            entries = new List<MetadataOutboundItem>();
            next.Outbound.Add(key, entries);
        }

        var frozen = new List<MetadataOutboundItem>(operations.Count);
        foreach (var operation in operations)
        {
            ulong previousHighWatermark = next.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
            ulong sequence = checked(previousHighWatermark + 1);
            var item = new MetadataOutboundItem(sequence, localAuthor, operation.ChangeId, operation.Payload.ToArray());
            entries.Add(item);
            frozen.Add(CloneOutboundItem(item));
            next.OutboundChangeIds.Add(new OutboundChangeKey(namespaceId, operation.ChangeId));
            next.ChangeLedger.Add(new LedgerKey(namespaceId, operation.ChangeId), operation.Fingerprint.ToArray());
            next.OutboundHighWatermarks[key] = sequence;
        }

        PersistPartition(partitionKey, current, next);
        return frozen;
    
        }
    }

    public ulong AcquireOutboundLease(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        Guid consumerId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (consumerId == Guid.Empty)
            throw new ArgumentException("ConsumerId must be non-empty.", nameof(consumerId));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = GetOrCreatePartition(partitionKey);
        var key = new OutboundKey(namespaceId, localAuthor);
        ulong previous = current.OutboundLeases.TryGetValue(key, out var prior) ? prior.Generation : 0;
        ulong generation = checked(previous + 1);

        var next = current.Clone();
        next.OutboundLeases[key] = new LeaseState(generation, consumerId);
        PersistPartition(partitionKey, current, next);
        return generation;
    
        }
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
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
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
    }

    public void AcknowledgeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new InvalidOperationException("Outbound lease is not active.");

        var key = new OutboundKey(namespaceId, localAuthor);
        ValidateOutboundLease(current, key, leaseGeneration);
        ulong previous = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = current.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
        if (safeThroughSequence < previous || safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        next.OutboundAck[key] = safeThroughSequence;
        PersistPartition(partitionKey, current, next);
    
        }
    }

    public void CompactOutboundThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new OutboundKey(namespaceId, localAuthor);
        ulong acknowledged = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        if (safeThroughSequence > acknowledged)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        if (next.Outbound.TryGetValue(key, out var entries))
            entries.RemoveAll(entry => entry.Sequence <= safeThroughSequence);
        PersistPartition(partitionKey, current, next);
    
        }
    }

    public ulong GetOutboundHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.OutboundHighWatermarks.TryGetValue(new OutboundKey(namespaceId, localAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    
        }
    }

    public MetadataRelayPage ReadRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong afterSequence,
        int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
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
    }

    public void AcknowledgeRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
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
        PersistPartition(partitionKey, current, next);
    
        }
    }

    public ulong GetReceiveCursor(
        string scopeId,
        NodeId localReplica,
        NodeId sender,
        uint senderEpoch,
        int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        ulong cursor = 0;
        foreach (var pair in state.Receives)
            if (pair.Key.Sender == sender && pair.Key.Epoch == senderEpoch &&
                pair.Key.NamespaceId == namespaceId && pair.Value.CursorEnd > cursor)
                cursor = pair.Value.CursorEnd;
        return cursor;
    
        }
    }

    public ulong GetReceiveCursor(
        string scopeId, NodeId localReplica, NodeId sender, uint senderEpoch,
        int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return 0;
            return state.Receives.TryGetValue(
                new ReceiveKey(sender, senderEpoch, namespaceId, authorStream), out var receive)
                ? receive.CursorEnd : 0;
        }
    }

    public void CompactRelayThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong acknowledged = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        if (safeThroughSequence > acknowledged)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var next = current.Clone();
        if (next.Relay.TryGetValue(key, out var entries))
            entries.RemoveAll(entry => entry.Sequence <= safeThroughSequence);
        PersistPartition(partitionKey, current, next);
    
        }
    }

    public ulong GetRelayHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.RelayHighWatermarks.TryGetValue(new RelayKey(namespaceId, originalAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
        if (_disposed) return;
        _disposed = true;
        if (_ownsStorage) _storage.Dispose();
        _partitions.Clear();
        if (_deleteOnDispose && _directory is { } directory)
        {
            try { Directory.Delete(directory, recursive: true); }
            catch { /* isolated test directory cleanup */ }
        }
    
        }
    }


    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private PartitionState GetOrCreatePartition(PartitionKey key)
    {
        if (TryGetPartition(key, out var state)) return state;
        state = new PartitionState { StorageCommitSeq = _storage.LastCommitSeq };
        _partitions.Add(key, state);
        return state;
    }

    private bool TryGetPartition(PartitionKey key, out PartitionState state)
    {
        ulong currentCommitSeq = _storage.LastCommitSeq;
        if (_partitions.TryGetValue(key, out state!) && state.StorageCommitSeq == currentCommitSeq)
            return true;
        byte[] root = PartitionRoot(key);
        using var snapshot = _storage.OpenSnapshot();
        var rows = snapshot.ScanPrefix(root, ReadOnlySpan<byte>.Empty, int.MaxValue);
        if (rows.Count == 0)
        {
            _partitions.Remove(key);
            state = null!;
            return false;
        }

        state = DecodePartition(key, root, rows, snapshot.CommitSeq);
        state.PersistedEntries = rows.ToDictionary(
            row => Convert.ToHexString(row.Key),
            row => (row.Key, row.Value),
            StringComparer.Ordinal);
        _partitions[key] = state;
        return true;
    }

    private void PersistPartition(PartitionKey key, PartitionState current, PartitionState next)
    {
        var oldEntries = current.IsPersisted
            ? current.PersistedEntries
            : EmptyEntries;
        var newEntries = StateEntries(key, next);
        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        bool changed = false;
        foreach (var old in oldEntries)
        {
            string hex = old.Key;
            if (!newEntries.ContainsKey(hex))
            {
                batch.Delete(old.Value.Key);
                changed = true;
            }
        }

        foreach (var entry in newEntries.Values)
        {
            string hex = Convert.ToHexString(entry.Key);
            if (!oldEntries.TryGetValue(hex, out var previous) ||
                !previous.Value.AsSpan().SequenceEqual(entry.Value))
            {
                batch.Put(entry.Key, entry.Value);
                changed = true;
            }
        }

        if (changed) batch.Commit();
        next.StorageCommitSeq = _storage.LastCommitSeq;
        next.IsPersisted = true;
        next.PersistedEntries = newEntries;
        _partitions[key] = next;
    }

    private static readonly Dictionary<string, (byte[] Key, byte[] Value)> EmptyEntries = new(StringComparer.Ordinal);

    private static byte[] PartitionRoot(PartitionKey key)
    {
        byte[] scope = Encoding.UTF8.GetBytes(key.ScopeId);
        byte[] hash = SHA256.HashData(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        hash.CopyTo(root, 1);
        key.LocalReplica.WriteTo(root.AsSpan(33));
        return root;
    }

    private static byte[] StorageKey(byte[] root, byte kind, ReadOnlySpan<byte> suffix)
    {
        byte[] key = new byte[root.Length + 1 + suffix.Length];
        root.CopyTo(key, 0);
        key[root.Length] = kind;
        suffix.CopyTo(key.AsSpan(root.Length + 1));
        return key;
    }

    private static Dictionary<string, (byte[] Key, byte[] Value)> StateEntries(
        PartitionKey partition,
        PartitionState state)
    {
        byte[] root = PartitionRoot(partition);
        var entries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        void Add(byte[] key, byte[] value) => entries.Add(Convert.ToHexString(key), (key, value));
        Add(StorageKey(root, 0, ReadOnlySpan<byte>.Empty), Encoding.UTF8.GetBytes(partition.ScopeId));

        foreach (var pair in state.Receipts)
        {
            byte[] suffix = new byte[38];
            pair.Key.Sender.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.Epoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.BatchId.WriteTo(suffix.AsSpan(22));
            byte[] value = new byte[52];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.CursorStart);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value.CursorEnd);
            pair.Value.PreviousBatchDigest.CopyTo(value, 16);
            pair.Value.BatchDigest.CopyTo(value, 32);
            BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(48), pair.Value.NewRelayOperationCount);
            Add(StorageKey(root, 1, suffix), value);
        }

        foreach (var pair in state.Receives)
        {
            byte[] suffix = new byte[38];
            pair.Key.Sender.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.Epoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            byte[] value = new byte[24];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.CursorEnd);
            pair.Value.LastBatchDigest.CopyTo(value, 8);
            Add(StorageKey(root, 2, suffix), value);
        }

        foreach (var pair in state.ChangeLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
            pair.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            Add(StorageKey(root, 3, suffix), pair.Value.ToArray());
        }

        foreach (var pair in state.Relay)
        {
            foreach (var item in pair.Value)
            {
                byte[] suffix = new byte[26];
                BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
                pair.Key.Author.WriteTo(suffix.AsSpan(2));
                BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Sequence);
                byte[] value = EncodePayloadItem(item.ChangeId, item.Payload);
                Add(StorageKey(root, 4, suffix), value);
            }
        }

        foreach (var pair in state.RelayAck)
            Add(StorageKey(root, 5, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), EncodeUInt64(pair.Value));
        foreach (var pair in state.RelayHighWatermarks)
            Add(StorageKey(root, 6, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), EncodeUInt64(pair.Value));

        foreach (var pair in state.Outbound)
        {
            foreach (var item in pair.Value)
            {
                byte[] suffix = new byte[26];
                BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
                pair.Key.Author.WriteTo(suffix.AsSpan(2));
                BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Sequence);
                Add(StorageKey(root, 7, suffix), EncodePayloadItem(item.ChangeId, item.Payload));
            }
        }

        foreach (var pair in state.OutboundHighWatermarks)
            Add(StorageKey(root, 8, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), EncodeUInt64(pair.Value));
        foreach (var pair in state.OutboundAck)
            Add(StorageKey(root, 9, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), EncodeUInt64(pair.Value));

        foreach (var pair in state.OutboundLeases)
        {
            byte[] value = new byte[24];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.Generation);
            pair.Value.ConsumerId.TryWriteBytes(value.AsSpan(8));
            Add(StorageKey(root, 10, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value);
        }

        foreach (var key in state.OutboundChangeIds)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
            key.ChangeId.WriteTo(suffix.AsSpan(2));
            Add(StorageKey(root, 11, suffix), Array.Empty<byte>());
        }

        foreach (var pair in state.PendingApplies)
            Add(StorageKey(root, 12, pair.Key.ToByteArray()), EncodeIncomingBatch(pair.Value.Batch));
        foreach (var pair in state.CompletedPendingApplies)
            Add(StorageKey(root, 13, pair.Key.ToByteArray()), EncodeCompleted(pair.Value));

        return entries;
    }

    private static PartitionState DecodePartition(
        PartitionKey partition,
        byte[] root,
        IReadOnlyList<StorageEntry> rows,
        ulong commitSeq)
    {
        var state = new PartitionState { StorageCommitSeq = commitSeq, IsPersisted = true };
        bool identitySeen = false;
        foreach (var row in rows)
        {
            ReadOnlySpan<byte> suffix = row.Key.AsSpan(root.Length);
            if (suffix.Length < 1) throw new InvalidDataException("Metadata storage key is truncated.");
            byte kind = suffix[0];
            ReadOnlySpan<byte> key = suffix.Slice(1);
            switch (kind)
            {
                case 0:
                    if (!Encoding.UTF8.GetString(row.Value).Equals(partition.ScopeId, StringComparison.Ordinal))
                        throw new InvalidDataException("Metadata partition scope hash collision.");
                    identitySeen = true;
                    break;
                case 1:
                {
                    RequireLength(key, 38);
                    RequireLength(row.Value, 52);
                    var receiptKey = new ReceiptKey(NodeId.From(key.Slice(0, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20)),
                        BatchId.From(key.Slice(22, 16)));
                    state.Receipts.Add(receiptKey, new ReceiptValue(
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value),
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8)),
                        row.Value.AsSpan(16, 16).ToArray(),
                        row.Value.AsSpan(32, 16).ToArray(),
                        BinaryPrimitives.ReadInt32LittleEndian(row.Value.AsSpan(48))));
                    break;
                }
                case 2:
                {
                    if (key.Length is not (22 or 38))
                        throw new InvalidDataException("Metadata receive key has an invalid length.");
                    RequireLength(row.Value, 24);
                    var receiveKey = new ReceiveKey(NodeId.From(key.Slice(0, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20)),
                        key.Length == 38 ? NodeId.From(key.Slice(22, 16)) : default);
                    state.Receives.Add(receiveKey, new ReceiveState(
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value),
                        row.Value.AsSpan(8, 16).ToArray()));
                    break;
                }
                case 3:
                {
                    RequireLength(key, 18);
                    RequireLength(row.Value, 16);
                    state.ChangeLedger.Add(new LedgerKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        BatchId.From(key.Slice(2, 16))), row.Value.ToArray());
                    break;
                }
                case 4:
                case 7:
                {
                    RequireLength(key, 26);
                    var relayKey = new RelayKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)));
                    ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(key.Slice(18, 8));
                    var item = DecodePayloadItem(sequence, relayKey.Author, row.Value);
                    if (kind == 4)
                    {
                        if (!state.Relay.TryGetValue(relayKey, out var relayItems))
                            state.Relay.Add(relayKey, relayItems = new List<MetadataRelayItem>());
                        relayItems.Add(new MetadataRelayItem(item.Sequence, item.AuthorNodeId, item.ChangeId, item.Payload));
                    }
                    else
                    {
                        var outboundKey = new OutboundKey(relayKey.NamespaceId, relayKey.Author);
                        if (!state.Outbound.TryGetValue(outboundKey, out var outboundItems))
                            state.Outbound.Add(outboundKey, outboundItems = new List<MetadataOutboundItem>());
                        outboundItems.Add(new MetadataOutboundItem(item.Sequence, item.AuthorNodeId, item.ChangeId, item.Payload));
                    }
                    break;
                }
                case 5:
                case 6:
                case 8:
                case 9:
                {
                    RequireLength(key, 18);
                    RequireLength(row.Value, 8);
                    var streamKey = new RelayKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    ulong value = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    if (kind == 5) state.RelayAck.Add(streamKey, value);
                    else if (kind == 6) state.RelayHighWatermarks.Add(streamKey, value);
                    else if (kind == 8) state.OutboundHighWatermarks.Add(new OutboundKey(streamKey.NamespaceId, streamKey.Author), value);
                    else state.OutboundAck.Add(new OutboundKey(streamKey.NamespaceId, streamKey.Author), value);
                    break;
                }
                case 10:
                {
                    RequireLength(key, 18);
                    RequireLength(row.Value, 24);
                    var outboundKey = new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    state.OutboundLeases.Add(outboundKey, new LeaseState(
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value),
                        new Guid(row.Value.AsSpan(8, 16))));
                    break;
                }
                case 11:
                {
                    RequireLength(key, 18);
                    state.OutboundChangeIds.Add(new OutboundChangeKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        BatchId.From(key.Slice(2, 16))));
                    break;
                }
                case 12:
                    RequireLength(key, 16);
                    state.PendingApplies.Add(new Guid(key), new PendingApplyValue(DecodeIncomingBatch(row.Value)));
                    break;
                case 13:
                    RequireLength(key, 16);
                    state.CompletedPendingApplies.Add(new Guid(key), DecodeCompleted(row.Value));
                    break;
                default:
                    throw new InvalidDataException($"Unknown metadata storage key kind {kind}.");
            }
        }

        if (!identitySeen)
            throw new InvalidDataException("Metadata partition is missing its scope identity record.");
        return state;
    }

    private static byte[] KeyNamespaceAuthor(int namespaceId, NodeId author)
    {
        byte[] key = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(key, checked((ushort)namespaceId));
        author.WriteTo(key.AsSpan(2));
        return key;
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] EncodePayloadItem(BatchId changeId, byte[] payload)
    {
        byte[] bytes = new byte[20 + payload.Length];
        changeId.WriteTo(bytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(16), payload.Length);
        payload.CopyTo(bytes, 20);
        return bytes;
    }

    private static MetadataOutboundItem DecodePayloadItem(ulong sequence, NodeId author, byte[] bytes)
    {
        if (bytes.Length < 20) throw new InvalidDataException("Stored stream payload is truncated.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(16));
        if (length < 0 || bytes.Length != 20 + length)
            throw new InvalidDataException("Stored stream payload length is invalid.");
        return new MetadataOutboundItem(sequence, author, BatchId.From(bytes.AsSpan(0, 16)), bytes.AsSpan(20).ToArray());
    }

    private static void RequireLength(ReadOnlySpan<byte> bytes, int length)
    {
        if (bytes.Length != length) throw new InvalidDataException("Metadata storage key/value has an invalid length.");
    }

    private static byte[] EncodeCompleted(CompletedPendingApplyValue completed)
    {
        byte[] bytes = new byte[53];
        completed.BatchDigest.CopyTo(bytes, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(16), completed.Receipt.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(24), completed.Receipt.CursorEnd);
        completed.Receipt.BatchDigest.CopyTo(bytes, 32);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(48), completed.Receipt.NewRelayOperationCount);
        bytes[52] = completed.Receipt.IsDuplicate ? (byte)1 : (byte)0;
        return bytes;
    }

    private static CompletedPendingApplyValue DecodeCompleted(byte[] bytes)
    {
        RequireLength(bytes, 53);
        if (bytes[52] > 1) throw new InvalidDataException("Completed apply receipt flags are invalid.");
        byte[] digest = bytes.AsSpan(0, 16).ToArray();
        var receipt = new MetadataBatchReceipt(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(16)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(24)),
            bytes.AsSpan(32, 16).ToArray(),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(48)),
            bytes[52] != 0);
        return new CompletedPendingApplyValue(digest, receipt);
    }

    private static byte[] EncodeIncomingBatch(MetadataIncomingBatch batch)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        byte[] scope = Encoding.UTF8.GetBytes(batch.ScopeId);
        writer.Write(-2); // version 2: author-stream-scoped receive cursor
        writer.Write(scope.Length); writer.Write(scope);
        writer.Write(batch.LocalReplica.ToArray());
        writer.Write(batch.SenderPeer.ToArray());
        writer.Write(batch.SenderEpoch);
        writer.Write(batch.AuthorStreamNodeId.ToArray());
        writer.Write(checked((ushort)batch.NamespaceId));
        writer.Write(batch.BatchId.ToArray());
        writer.Write(batch.CursorStart); writer.Write(batch.CursorEnd);
        writer.Write(batch.PreviousBatchDigest); writer.Write(batch.BatchDigest);
        writer.Write(batch.Operations.Count);
        foreach (var operation in batch.Operations)
        {
            writer.Write(operation.AuthorNodeId.ToArray());
            writer.Write(operation.ChangeId.ToArray());
            writer.Write(operation.Fingerprint);
            writer.Write(operation.RelayEligible);
            writer.Write(operation.Payload.Length);
            writer.Write(operation.Payload);
        }
        writer.Flush();
        return stream.ToArray();
    }

    private static MetadataIncomingBatch DecodeIncomingBatch(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);
        int versionOrScopeLength = reader.ReadInt32();
        bool hasAuthorStream = versionOrScopeLength == -2;
        int scopeLength = hasAuthorStream ? reader.ReadInt32() : versionOrScopeLength;
        string scope = Encoding.UTF8.GetString(ReadExactly(reader, scopeLength));
        NodeId local = NodeId.From(ReadExactly(reader, 16));
        NodeId sender = NodeId.From(ReadExactly(reader, 16));
        uint epoch = reader.ReadUInt32();
        NodeId authorStream = hasAuthorStream ? NodeId.From(ReadExactly(reader, 16)) : default;
        int ns = reader.ReadUInt16();
        BatchId batchId = BatchId.From(ReadExactly(reader, 16));
        ulong cursorStart = reader.ReadUInt64();
        ulong cursorEnd = reader.ReadUInt64();
        byte[] previous = ReadExactly(reader, 16);
        byte[] digest = ReadExactly(reader, 16);
        int count = reader.ReadInt32();
        if (count < 0 || count > 100_000) throw new InvalidDataException("Pending batch operation count is invalid.");
        var operations = new MetadataOperation[count];
        for (int i = 0; i < count; i++)
        {
            var author = NodeId.From(ReadExactly(reader, 16));
            var changeId = BatchId.From(ReadExactly(reader, 16));
            byte[] fingerprint = ReadExactly(reader, 16);
            bool relay = reader.ReadBoolean();
            int payloadLength = reader.ReadInt32();
            operations[i] = new MetadataOperation
            {
                AuthorNodeId = author,
                ChangeId = changeId,
                Fingerprint = fingerprint,
                RelayEligible = relay,
                Payload = ReadExactly(reader, payloadLength)
            };
        }
        if (stream.Position != stream.Length) throw new InvalidDataException("Pending batch has trailing bytes.");
        return new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = epoch,
            AuthorStreamNodeId = authorStream,
            NamespaceId = ns,
            BatchId = batchId,
            CursorStart = cursorStart,
            CursorEnd = cursorEnd,
            PreviousBatchDigest = previous,
            BatchDigest = digest,
            Operations = operations
        };
    }

    private static byte[] ReadExactly(BinaryReader reader, int length)
    {
        if (length < 0 || length > 16 * 1024 * 1024) throw new InvalidDataException("Metadata field length is invalid.");
        byte[] bytes = reader.ReadBytes(length);
        if (bytes.Length != length) throw new InvalidDataException("Metadata payload is truncated.");
        return bytes;
    }

    private static NodeId ResolveAuthorStream(MetadataIncomingBatch batch)
        => batch.AuthorStreamNodeId;

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
        ArgumentNullException.ThrowIfNull(batch.Operations);
        if (!batch.AuthorStreamNodeId.IsZero &&
            batch.Operations.Any(operation => operation.AuthorNodeId != batch.AuthorStreamNodeId))
            throw new ArgumentException("Incoming batch operations must belong to one author stream.", nameof(batch));

        var seen = new HashSet<BatchId>();
        foreach (var operation in batch.Operations)
        {
            if (operation.AuthorNodeId.IsZero || operation.ChangeId.IsZero)
                throw new ArgumentException("Operation author and ChangeId must be non-zero.", nameof(batch));
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("ChangeId values must be unique within one incoming batch.", nameof(batch));
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
        => new(item.Sequence, item.OriginalAuthor, item.ChangeId, item.Payload.ToArray());

    private static MetadataOutboundItem CloneOutboundItem(MetadataOutboundItem item)
        => new(item.Sequence, item.AuthorNodeId, item.ChangeId, item.Payload.ToArray());

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
            ulong cursor = 0;
            foreach (var pair in state.Receives)
                if (pair.Key.Sender == sender && pair.Key.Epoch == senderEpoch &&
                    pair.Key.NamespaceId == namespaceId && pair.Value.CursorEnd > cursor)
                    cursor = pair.Value.CursorEnd;
            return cursor;
        }

        public ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId, NodeId authorStream)
        {
            var state = GetState();
            return state.Receives.TryGetValue(
                new ReceiveKey(sender, senderEpoch, namespaceId, authorStream), out var receive)
                ? receive.CursorEnd : 0;
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
    private readonly record struct LedgerKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct RelayKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundChangeKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct LeaseState(ulong Generation, Guid ConsumerId);
    private readonly record struct ReceiveState(ulong CursorEnd, byte[] LastBatchDigest);

    private sealed class ReceiptValue(
        ulong cursorStart,
        ulong cursorEnd,
        byte[] previousBatchDigest,
        byte[] batchDigest,
        int newRelayOperationCount)
    {
        public ulong CursorStart { get; } = cursorStart;
        public ulong CursorEnd { get; } = cursorEnd;
        public byte[] PreviousBatchDigest { get; } = previousBatchDigest;
        public byte[] BatchDigest { get; } = batchDigest;
        public int NewRelayOperationCount { get; } = newRelayOperationCount;

        public bool Matches(MetadataIncomingBatch batch)
            => CursorStart == batch.CursorStart
                && CursorEnd == batch.CursorEnd
                && PreviousBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest)
                && BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest);
    }

    private sealed record PendingApplyValue(MetadataIncomingBatch Batch);
    private sealed record CompletedPendingApplyValue(byte[] BatchDigest, MetadataBatchReceipt Receipt);

    private sealed class PartitionState
    {
        public Dictionary<ReceiptKey, ReceiptValue> Receipts { get; } = new();
        public Dictionary<ReceiveKey, ReceiveState> Receives { get; } = new();
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

        public ulong StorageCommitSeq { get; set; }
        public bool IsPersisted { get; set; }
        public Dictionary<string, (byte[] Key, byte[] Value)> PersistedEntries { get; set; } = new(StringComparer.Ordinal);

        public PartitionState Clone()
        {
            // Stored values are immutable after insertion. Copy collection structure only;
            // copy payload arrays only when they cross the public API boundary. Relay and
            // outbound lists are the only nested mutable collections and need shallow copies.
            var clone = new PartitionState
            {
                StorageCommitSeq = StorageCommitSeq,
                IsPersisted = IsPersisted,
                PersistedEntries = PersistedEntries
            };
            foreach (var pair in Receipts) clone.Receipts.Add(pair.Key, pair.Value);
            foreach (var pair in Receives) clone.Receives.Add(pair.Key, pair.Value);
            foreach (var pair in ChangeLedger) clone.ChangeLedger.Add(pair.Key, pair.Value);
            foreach (var pair in Relay) clone.Relay.Add(pair.Key, new List<MetadataRelayItem>(pair.Value));
            foreach (var pair in RelayAck) clone.RelayAck.Add(pair.Key, pair.Value);
            foreach (var pair in RelayHighWatermarks) clone.RelayHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in Outbound) clone.Outbound.Add(pair.Key, new List<MetadataOutboundItem>(pair.Value));
            foreach (var pair in OutboundHighWatermarks) clone.OutboundHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundAck) clone.OutboundAck.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundLeases) clone.OutboundLeases.Add(pair.Key, pair.Value);
            foreach (var key in OutboundChangeIds) clone.OutboundChangeIds.Add(key);
            foreach (var pair in PendingApplies) clone.PendingApplies.Add(pair.Key, pair.Value);
            foreach (var pair in CompletedPendingApplies) clone.CompletedPendingApplies.Add(pair.Key, pair.Value);
            return clone;
        }
    }
}
