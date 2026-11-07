using System.IO;
using BinSync;
using BinSync.MetadataStore.Conformance;

namespace BinSync.Tests.Production;

/// <summary>
/// Volatile reference adapter for the semantic conformance suite. It is deliberately
/// test-only and makes no restart-durability claim.
/// </summary>
public sealed class VolatileMetadataStoreAdapter : IMetadataStoreConformanceAdapter
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

        var receiptKey = new ReceiptKey(
            batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId);
        if (current.Receipts.TryGetValue(receiptKey, out var priorReceipt))
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

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId);
        var receive = current.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0, new byte[16]);

        ulong expectedStart = checked(receive.CursorEnd + 1);
        if (batch.CursorStart != expectedStart ||
            !receive.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue the durable receive cursor and digest chain.");

        // Stage against a private copy. Any validation failure below leaves the
        // published partition, receive cursor, ledger and relay journal unchanged.
        var next = current.Clone();
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

            ulong sequence = entries.Count == 0 ? 1 : checked(entries[^1].Sequence + 1);
            entries.Add(new MetadataRelayItem(
                sequence,
                operation.AuthorNodeId,
                operation.ChangeId,
                operation.Payload.ToArray()));
            relayAdded++;
        }

        next.Receives[receiveKey] = new ReceiveState(batch.CursorEnd, batch.BatchDigest.ToArray());
        next.Receipts.Add(receiptKey, new ReceiptValue(
            batch.CursorStart,
            batch.CursorEnd,
            batch.PreviousBatchDigest.ToArray(),
            batch.BatchDigest.ToArray(),
            relayAdded));

        _partitions[partitionKey] = next;
        return new MetadataBatchReceipt(
            batch.CursorStart,
            batch.CursorEnd,
            batch.BatchDigest.ToArray(),
            relayAdded,
            IsDuplicate: false);
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
        if (limit <= 0)
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), afterSequence);

        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), afterSequence);

        var key = new RelayKey(namespaceId, originalAuthor);
        if (!state.Relay.TryGetValue(key, out var entries))
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), afterSequence);

        ulong acknowledged = state.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong cursor = Math.Max(afterSequence, acknowledged);
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
        ulong highWatermark = current.Relay.TryGetValue(key, out var entries) && entries.Count != 0
            ? entries[^1].Sequence
            : 0;

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

        return state.Receives.TryGetValue(new ReceiveKey(sender, senderEpoch, namespaceId), out var receive)
            ? receive.CursorEnd
            : 0;
    }

    public ulong GetRelayHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        if (!_partitions.TryGetValue(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.Relay.TryGetValue(new RelayKey(namespaceId, originalAuthor), out var entries) &&
               entries.Count != 0
            ? entries[^1].Sequence
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
        ArgumentNullException.ThrowIfNull(batch.Operations);

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

    private readonly record struct PartitionKey(string ScopeId, NodeId LocalReplica);
    private readonly record struct ReceiptKey(NodeId Sender, uint Epoch, int NamespaceId, BatchId BatchId);
    private readonly record struct ReceiveKey(NodeId Sender, uint Epoch, int NamespaceId);
    private readonly record struct LedgerKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct RelayKey(int NamespaceId, NodeId Author);
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

    private sealed class PartitionState
    {
        public Dictionary<ReceiptKey, ReceiptValue> Receipts { get; } = new();
        public Dictionary<ReceiveKey, ReceiveState> Receives { get; } = new();
        public Dictionary<LedgerKey, byte[]> ChangeLedger { get; } = new();
        public Dictionary<RelayKey, List<MetadataRelayItem>> Relay { get; } = new();
        public Dictionary<RelayKey, ulong> RelayAck { get; } = new();

        public PartitionState Clone()
        {
            var clone = new PartitionState();
            foreach (var pair in Receipts)
                clone.Receipts.Add(pair.Key, new ReceiptValue(
                    pair.Value.CursorStart,
                    pair.Value.CursorEnd,
                    pair.Value.PreviousBatchDigest.ToArray(),
                    pair.Value.BatchDigest.ToArray(),
                    pair.Value.NewRelayOperationCount));
            foreach (var pair in Receives)
                clone.Receives.Add(pair.Key, new ReceiveState(pair.Value.CursorEnd, pair.Value.LastBatchDigest.ToArray()));
            foreach (var pair in ChangeLedger)
                clone.ChangeLedger.Add(pair.Key, pair.Value.ToArray());
            foreach (var pair in Relay)
                clone.Relay.Add(pair.Key, pair.Value.Select(CloneRelayItem).ToList());
            foreach (var pair in RelayAck)
                clone.RelayAck.Add(pair.Key, pair.Value);
            return clone;
        }
    }
}
