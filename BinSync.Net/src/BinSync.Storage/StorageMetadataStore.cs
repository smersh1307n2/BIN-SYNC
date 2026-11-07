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
public class StorageMetadataStore : IMetadataStore, IMetadataPeerStateStore
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
        if (current.HasLegacyEncoding)
        {
            var migrated = current.Clone();
            var legacyReceipt = ApplyIncomingBatch(migrated, batch);
            PersistPartition(partitionKey, current, migrated);
            return legacyReceipt;
        }
        return AcceptIncomingBatchDelta(partitionKey, current, batch);
    
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
            throw new MetadataPendingApplyAlreadyFinalizedException();
        }

        if (current.PendingApplies.TryGetValue(applyToken, out var pending))
        {
            if (!SamePendingBatch(pending.Batch, batch))
                throw new InvalidDataException("Pending apply token was reused for different input.");
            return new MetadataPendingApplyIntent(applyToken, CloneBatch(pending.Batch));
        }

        ValidateIncomingBatchForStaging(current, batch, applyToken);

        var pendingValue = new PendingApplyValue(CloneBatch(batch));
        PersistPendingApply(partitionKey, current, applyToken, pendingValue);
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

        // Finalization is a hot durability boundary. Do not clone and re-encode the
        // entire partition here: a single incoming batch must write only its receipt,
        // cursor/digest, new ledger/relay rows and PendingApply transition.
        return FinalizePendingApplyDelta(partitionKey, current,
            providerReceipt.ApplyToken, pending.Batch, providerReceipt.BatchDigest);
    
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


    private MetadataBatchReceipt FinalizePendingApplyDelta(
        PartitionKey partitionKey, PartitionState current, Guid applyToken,
        MetadataIncomingBatch incoming, byte[] providerDigest)
    {
        var receiptKey = new ReceiptKey(incoming.SenderPeer, incoming.SenderEpoch,
            incoming.NamespaceId, incoming.BatchId);
        MetadataBatchReceipt receipt;
        bool isNewReceipt = !current.Receipts.TryGetValue(receiptKey, out var priorReceipt);
        var newLedger = new List<(LedgerKey Key, ChangeLedgerValue Value)>();
        var newRelay = new List<(RelayKey Key, MetadataRelayItem Item)>();
        var relayHigh = new Dictionary<RelayKey, ulong>();
        var newPeerStreams = new Dictionary<PeerStreamKey, PeerStreamStateValue>();
        var newRequiredStreams = new HashSet<PeerStreamKey>();

        if (!isNewReceipt)
        {
            if (!priorReceipt!.Matches(incoming))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            receipt = new MetadataBatchReceipt(priorReceipt.CursorStart, priorReceipt.CursorEnd,
                priorReceipt.BatchDigest.ToArray(), priorReceipt.NewRelayOperationCount, IsDuplicate: true)
            { DurableAuthor = priorReceipt.DurableAuthor };
        }
        else
        {
            var receiveKey = new ReceiveKey(incoming.SenderPeer, incoming.SenderEpoch,
                incoming.NamespaceId, ResolveAuthorStream(incoming));
            var receive = current.Receives.TryGetValue(receiveKey, out var priorReceive)
                ? priorReceive : new ReceiveState(0);
            var chainKey = new ReceiveChainKey(incoming.SenderPeer, incoming.SenderEpoch, incoming.NamespaceId);
            var chain = current.ReceiveChains.TryGetValue(chainKey, out var priorChain)
                ? priorChain : new ReceiveChainState(new byte[16]);
            if (incoming.CursorStart != checked(receive.CursorEnd + 1) ||
                !chain.LastBatchDigest.AsSpan().SequenceEqual(incoming.PreviousBatchDigest))
                throw new InvalidDataException("Incoming batch does not continue its author cursor and sender digest chain.");

            int relayAdded = 0;
            var interestedPeers = current.InterestedPeers
                .Where(peer => peer.NamespaceId == incoming.NamespaceId)
                .Select(peer => (peer.Peer, peer.PeerEpoch))
                .Concat(current.RequiredPeerStreams
                    .Where(peer => peer.NamespaceId == incoming.NamespaceId)
                    .Select(peer => (peer.Peer, peer.PeerEpoch)))
                .Distinct()
                .ToArray();
            foreach (var operation in incoming.Operations)
            {
                var ledgerKey = new LedgerKey(incoming.NamespaceId, operation.ChangeId);
                if (current.ChangeLedger.TryGetValue(ledgerKey, out var priorLedger))
                {
                    if (!priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                        throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                    continue;
                }
                newLedger.Add((ledgerKey,
                    new ChangeLedgerValue(operation.Fingerprint.ToArray(), IsLocalOutbound: false)));
                if (!operation.RelayEligible) continue;

                var relayKey = new RelayKey(incoming.NamespaceId, operation.AuthorNodeId);
                ulong high = relayHigh.TryGetValue(relayKey, out var stagedHigh)
                    ? stagedHigh
                    : current.RelayHighWatermarks.TryGetValue(relayKey, out var savedHigh) ? savedHigh : 0;
                ulong sequence = checked(high + 1);
                relayHigh[relayKey] = sequence;
                newRelay.Add((relayKey, new MetadataRelayItem(
                    sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray())));
                relayAdded++;

                foreach (var peer in interestedPeers)
                {
                    if (peer.Peer == operation.AuthorNodeId) continue;
                    var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch,
                        incoming.NamespaceId, operation.AuthorNodeId);
                    if (!current.PeerStreams.ContainsKey(streamKey) &&
                        !newPeerStreams.ContainsKey(streamKey))
                        newPeerStreams.Add(streamKey, new PeerStreamStateValue(
                            0, 0, 0, incoming.SenderEpoch, new byte[16], null));
                    if (!current.RequiredPeerStreams.Contains(streamKey))
                        newRequiredStreams.Add(streamKey);
                }
            }

            receipt = new MetadataBatchReceipt(incoming.CursorStart, incoming.CursorEnd,
                incoming.BatchDigest.ToArray(), relayAdded, IsDuplicate: false)
            { DurableAuthor = ResolveAuthorStream(incoming) };
        }

        byte[] root = PartitionRoot(partitionKey);
        byte[] pendingKey = StorageKey(root, 12, applyToken.ToByteArray());
        byte[] completedKey = StorageKey(root, 13, applyToken.ToByteArray());
        byte[] completedValue = EncodeCompleted(new CompletedPendingApplyValue(
            providerDigest.ToArray(), CloneReceipt(receipt, receipt.IsDuplicate)));
        byte[]? identityKey = current.IsPersisted ? null : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null ? null : Encoding.UTF8.GetBytes(partitionKey.ScopeId);

        var writes = new List<(byte[] Key, byte[] Value)>();
        var deletes = new List<byte[]> { pendingKey };
        if (identityKey is not null) writes.Add((identityKey, identityValue!));
        writes.Add((completedKey, completedValue));

        if (isNewReceipt)
        {
            byte[] receiptSuffix = new byte[38];
            incoming.SenderPeer.WriteTo(receiptSuffix);
            BinaryPrimitives.WriteUInt32LittleEndian(receiptSuffix.AsSpan(16), incoming.SenderEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(receiptSuffix.AsSpan(20), checked((ushort)incoming.NamespaceId));
            incoming.BatchId.WriteTo(receiptSuffix.AsSpan(22));
            byte[] receiptValue = new byte[68];
            BinaryPrimitives.WriteUInt64LittleEndian(receiptValue, incoming.CursorStart);
            BinaryPrimitives.WriteUInt64LittleEndian(receiptValue.AsSpan(8), incoming.CursorEnd);
            incoming.PreviousBatchDigest.CopyTo(receiptValue, 16);
            incoming.BatchDigest.CopyTo(receiptValue, 32);
            BinaryPrimitives.WriteInt32LittleEndian(receiptValue.AsSpan(48), receipt.NewRelayOperationCount);
            receipt.DurableAuthor.WriteTo(receiptValue.AsSpan(52));
            writes.Add((StorageKey(root, 1, receiptSuffix), receiptValue));

            byte[] receiveSuffix = new byte[38];
            incoming.SenderPeer.WriteTo(receiveSuffix);
            BinaryPrimitives.WriteUInt32LittleEndian(receiveSuffix.AsSpan(16), incoming.SenderEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(receiveSuffix.AsSpan(20), checked((ushort)incoming.NamespaceId));
            ResolveAuthorStream(incoming).WriteTo(receiveSuffix.AsSpan(22));
            byte[] receiveValue = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(receiveValue, incoming.CursorEnd);
            writes.Add((StorageKey(root, 2, receiveSuffix), receiveValue));

            byte[] chainSuffix = new byte[22];
            incoming.SenderPeer.WriteTo(chainSuffix);
            BinaryPrimitives.WriteUInt32LittleEndian(chainSuffix.AsSpan(16), incoming.SenderEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(chainSuffix.AsSpan(20), checked((ushort)incoming.NamespaceId));
            writes.Add((StorageKey(root, 14, chainSuffix), incoming.BatchDigest.ToArray()));
        }

        foreach (var item in newLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)item.Key.NamespaceId));
            item.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            byte[] value = new byte[17];
            item.Value.Fingerprint.CopyTo(value, 0);
            value[16] = item.Value.IsLocalOutbound ? (byte)1 : (byte)0;
            writes.Add((StorageKey(root, 3, suffix), value));
        }

        foreach (var item in newRelay)
        {
            byte[] suffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)item.Key.NamespaceId));
            item.Key.Author.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Item.Sequence);
            writes.Add((StorageKey(root, 4, suffix), EncodePayloadItem(item.Item.ChangeId, item.Item.Payload)));
        }

        foreach (var pair in relayHigh)
        {
            byte[] value = new byte[16];
            if (current.RelayAck.TryGetValue(pair.Key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value);
            writes.Add((StorageKey(root, 5, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value));
        }

        foreach (var pair in newPeerStreams)
        {
            byte[] suffix = new byte[38];
            pair.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(pair.Value)));
        }
        foreach (var key in newRequiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(suffix.AsSpan(2));
            key.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), key.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        using (var storageBatch = _storage.BeginWriteBatch())
        {
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != current.StorageCommitSeq)
                throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
            foreach (var write in writes) storageBatch.Put(write.Key, write.Value);
            foreach (var delete in deletes) storageBatch.Delete(delete);
            storageBatch.Commit();
        }

        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
        {
            current.IsPersisted = true;
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        }
        current.PendingApplies.Remove(applyToken);
        current.CompletedPendingApplies.Add(applyToken,
            new CompletedPendingApplyValue(providerDigest.ToArray(), CloneReceipt(receipt, receipt.IsDuplicate)));
        if (isNewReceipt)
        {
            current.Receipts.Add(receiptKey, new ReceiptValue(incoming.CursorStart, incoming.CursorEnd,
                incoming.PreviousBatchDigest.ToArray(), incoming.BatchDigest.ToArray(),
                receipt.NewRelayOperationCount, receipt.DurableAuthor));
            current.Receives[new ReceiveKey(incoming.SenderPeer, incoming.SenderEpoch,
                incoming.NamespaceId, ResolveAuthorStream(incoming))] = new ReceiveState(incoming.CursorEnd);
            current.ReceiveChains[new ReceiveChainKey(incoming.SenderPeer, incoming.SenderEpoch,
                incoming.NamespaceId)] = new ReceiveChainState(incoming.BatchDigest.ToArray());
        }
        foreach (var item in newLedger) current.ChangeLedger.Add(item.Key, item.Value);
        foreach (var item in newRelay)
        {
            if (!current.Relay.TryGetValue(item.Key, out var entries))
                current.Relay.Add(item.Key, entries = new List<MetadataRelayItem>());
            entries.Add(item.Item);
        }
        foreach (var pair in relayHigh) current.RelayHighWatermarks[pair.Key] = pair.Value;
        foreach (var pair in newPeerStreams) current.PeerStreams.Add(pair.Key, pair.Value);
        foreach (var key in newRequiredStreams) current.RequiredPeerStreams.Add(key);

        foreach (var write in writes)
            current.PersistedEntries[Convert.ToHexString(write.Key)] = (write.Key, write.Value);
        foreach (var delete in deletes)
            current.PersistedEntries.Remove(Convert.ToHexString(delete));
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        return receipt;
    }

    private static void ValidateIncomingBatchForStaging(
        PartitionState state, MetadataIncomingBatch batch, Guid applyToken)
    {
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        if (state.Receipts.TryGetValue(receiptKey, out var receipt))
        {
            if (!receipt.Matches(batch))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            return;
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, ResolveAuthorStream(batch));
        foreach (var pending in state.PendingApplies)
        {
            if (pending.Key == applyToken) continue;
            var other = pending.Value.Batch;
            if (other.SenderPeer == receiveKey.Sender &&
                other.SenderEpoch == receiveKey.Epoch &&
                other.NamespaceId == receiveKey.NamespaceId)
                throw new InvalidOperationException("An earlier batch for this sender session namespace is still pending apply.");
        }

        var receive = state.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0);
        var chainKey = new ReceiveChainKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId);
        var chain = state.ReceiveChains.TryGetValue(chainKey, out var priorChain)
            ? priorChain
            : new ReceiveChainState(new byte[16]);
        if (batch.CursorStart != checked(receive.CursorEnd + 1) ||
            !chain.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue its author cursor and sender digest chain.");

        // Index pending identities once. The durable identity key is
        // (namespace, ChangeId), not ChangeId alone; scanning every pending batch
        // for every incoming operation made staging O(pending × incoming).
        var pendingIdentities = new Dictionary<LedgerKey, byte[]>();
        foreach (var otherPending in state.PendingApplies.Values)
        {
            foreach (var pendingOperation in otherPending.Batch.Operations)
            {
                var pendingKey = new LedgerKey(otherPending.Batch.NamespaceId, pendingOperation.ChangeId);
                if (pendingIdentities.TryGetValue(pendingKey, out var knownFingerprint) &&
                    !knownFingerprint.AsSpan().SequenceEqual(pendingOperation.Fingerprint))
                    throw new InvalidDataException("Pending ChangeId was reused with a different canonical fingerprint.");
                pendingIdentities[pendingKey] = pendingOperation.Fingerprint;
            }
        }

        var batchIdentities = new Dictionary<LedgerKey, byte[]>();
        foreach (var operation in batch.Operations)
        {
            var key = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (batchIdentities.TryGetValue(key, out var inBatchFingerprint) &&
                !inBatchFingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint within one incoming batch.");
            batchIdentities[key] = operation.Fingerprint;

            if (state.ChangeLedger.TryGetValue(key, out var priorLedger) &&
                !priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");

            if (pendingIdentities.TryGetValue(key, out var pendingFingerprint) &&
                !pendingFingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                throw new InvalidDataException("Pending ChangeId was reused with a different canonical fingerprint.");
        }
    }

    private MetadataBatchReceipt AcceptIncomingBatchDelta(
        PartitionKey partitionKey, PartitionState current, MetadataIncomingBatch batch)
    {
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId);
        if (current.Receipts.TryGetValue(receiptKey, out var priorReceipt))
        {
            if (!priorReceipt.Matches(batch))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            return new MetadataBatchReceipt(priorReceipt.CursorStart, priorReceipt.CursorEnd,
                priorReceipt.BatchDigest.ToArray(), priorReceipt.NewRelayOperationCount, IsDuplicate: true)
            { DurableAuthor = priorReceipt.DurableAuthor };
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, ResolveAuthorStream(batch));
        var receive = current.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0);
        var chainKey = new ReceiveChainKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId);
        var chain = current.ReceiveChains.TryGetValue(chainKey, out var priorChain)
            ? priorChain
            : new ReceiveChainState(new byte[16]);
        if (batch.CursorStart != checked(receive.CursorEnd + 1) ||
            !chain.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue its author cursor and sender digest chain.");

        var newLedger = new Dictionary<LedgerKey, ChangeLedgerValue>();
        var newRelay = new Dictionary<RelayKey, List<MetadataRelayItem>>();
        var relayHigh = new Dictionary<RelayKey, ulong>();
        var peerStreamStates = new Dictionary<PeerStreamKey, PeerStreamStateValue>();
        var requiredStreams = new HashSet<PeerStreamKey>();
        var registeredAuthors = new HashSet<NodeId>();
        int relayAdded = 0;

        foreach (var operation in batch.Operations)
        {
            var ledgerKey = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (current.ChangeLedger.TryGetValue(ledgerKey, out var priorLedger))
            {
                if (!priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                continue;
            }
            if (newLedger.TryGetValue(ledgerKey, out var stagedLedger))
            {
                if (!stagedLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint within one incoming batch.");
                continue;
            }

            newLedger.Add(ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), IsLocalOutbound: false));
            if (!operation.RelayEligible)
                continue;

            var relayKey = new RelayKey(batch.NamespaceId, operation.AuthorNodeId);
            ulong previousHigh = relayHigh.TryGetValue(relayKey, out ulong stagedHigh)
                ? stagedHigh
                : current.RelayHighWatermarks.TryGetValue(relayKey, out ulong currentHigh) ? currentHigh : 0;
            ulong sequence = checked(previousHigh + 1);
            if (!newRelay.TryGetValue(relayKey, out var relayItems))
                newRelay.Add(relayKey, relayItems = new List<MetadataRelayItem>());
            relayItems.Add(new MetadataRelayItem(
                sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray()));
            relayHigh[relayKey] = sequence;
            relayAdded++;

            if (registeredAuthors.Add(operation.AuthorNodeId))
            {
                var peers = current.InterestedPeers.Where(peer => peer.NamespaceId == batch.NamespaceId)
                    .Select(peer => (peer.Peer, peer.PeerEpoch))
                    .Concat(current.RequiredPeerStreams.Where(peer => peer.NamespaceId == batch.NamespaceId)
                        .Select(peer => (peer.Peer, peer.PeerEpoch)))
                    .Distinct();
                foreach (var peer in peers.Where(peer => peer.Peer != operation.AuthorNodeId))
                {
                    var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch,
                        batch.NamespaceId, operation.AuthorNodeId);
                    if (!current.PeerStreams.ContainsKey(streamKey) && !peerStreamStates.ContainsKey(streamKey))
                        peerStreamStates.Add(streamKey,
                            EmptyPeerStreamValue() with { IssuerEpoch = batch.SenderEpoch });
                    if (!current.RequiredPeerStreams.Contains(streamKey))
                        requiredStreams.Add(streamKey);
                }
            }
        }

        var receipt = new MetadataBatchReceipt(batch.CursorStart, batch.CursorEnd,
            batch.BatchDigest.ToArray(), relayAdded, IsDuplicate: false)
        { DurableAuthor = ResolveAuthorStream(batch) };
        byte[] root = PartitionRoot(partitionKey);
        byte[] receiptSuffix = new byte[38];
        batch.SenderPeer.WriteTo(receiptSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(receiptSuffix.AsSpan(16), batch.SenderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(receiptSuffix.AsSpan(20), checked((ushort)batch.NamespaceId));
        batch.BatchId.WriteTo(receiptSuffix.AsSpan(22));
        byte[] receiptValue = new byte[68];
        BinaryPrimitives.WriteUInt64LittleEndian(receiptValue, batch.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(receiptValue.AsSpan(8), batch.CursorEnd);
        batch.PreviousBatchDigest.CopyTo(receiptValue, 16);
        batch.BatchDigest.CopyTo(receiptValue, 32);
        BinaryPrimitives.WriteInt32LittleEndian(receiptValue.AsSpan(48), relayAdded);
        receipt.DurableAuthor.WriteTo(receiptValue.AsSpan(52));

        byte[] receiveSuffix = new byte[38];
        batch.SenderPeer.WriteTo(receiveSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(receiveSuffix.AsSpan(16), batch.SenderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(receiveSuffix.AsSpan(20), checked((ushort)batch.NamespaceId));
        receiveKey.AuthorStream.WriteTo(receiveSuffix.AsSpan(22));
        byte[] receiveValue = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(receiveValue, batch.CursorEnd);

        byte[] chainSuffix = new byte[22];
        batch.SenderPeer.WriteTo(chainSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(chainSuffix.AsSpan(16), batch.SenderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(chainSuffix.AsSpan(20), checked((ushort)batch.NamespaceId));

        var writes = new List<(byte[] Key, byte[] Value)>(
            3 + newLedger.Count + newRelay.Values.Sum(items => items.Count) + relayHigh.Count + peerStreamStates.Count + requiredStreams.Count);
        writes.Add((StorageKey(root, 1, receiptSuffix), receiptValue));
        writes.Add((StorageKey(root, 2, receiveSuffix), receiveValue));
        writes.Add((StorageKey(root, 14, chainSuffix), batch.BatchDigest.ToArray()));

        foreach (var pair in newLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
            pair.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            byte[] value = new byte[17];
            pair.Value.Fingerprint.CopyTo(value, 0);
            value[16] = pair.Value.IsLocalOutbound ? (byte)1 : (byte)0;
            writes.Add((StorageKey(root, 3, suffix), value));
        }
        foreach (var pair in newRelay)
        {
            foreach (var item in pair.Value)
            {
                byte[] suffix = new byte[26];
                BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
                pair.Key.Author.WriteTo(suffix.AsSpan(2));
                BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Sequence);
                writes.Add((StorageKey(root, 4, suffix), EncodePayloadItem(item.ChangeId, item.Payload)));
            }
        }
        foreach (var pair in relayHigh)
        {
            byte[] value = new byte[16];
            if (current.RelayAck.TryGetValue(pair.Key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value);
            writes.Add((StorageKey(root, 5, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value));
        }
        foreach (var pair in peerStreamStates)
        {
            byte[] suffix = new byte[38];
            pair.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(pair.Value)));
        }
        foreach (var key in requiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(suffix.AsSpan(2));
            key.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), key.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : Encoding.UTF8.GetBytes(partitionKey.ScopeId);
        using var storageBatch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            storageBatch.Put(identityKey, identityValue!);
        foreach (var write in writes)
            storageBatch.Put(write.Key, write.Value);
        storageBatch.Commit();

        current.Receipts.Add(receiptKey, new ReceiptValue(batch.CursorStart, batch.CursorEnd,
            batch.PreviousBatchDigest.ToArray(), batch.BatchDigest.ToArray(), relayAdded, receipt.DurableAuthor));
        current.Receives[receiveKey] = new ReceiveState(batch.CursorEnd);
        current.ReceiveChains[chainKey] = new ReceiveChainState(batch.BatchDigest.ToArray());
        foreach (var pair in newLedger)
            current.ChangeLedger.Add(pair.Key, pair.Value);
        foreach (var pair in newRelay)
        {
            if (!current.Relay.TryGetValue(pair.Key, out var entries))
                current.Relay.Add(pair.Key, entries = new List<MetadataRelayItem>());
            entries.AddRange(pair.Value);
        }
        foreach (var pair in relayHigh)
            current.RelayHighWatermarks[pair.Key] = pair.Value;
        foreach (var pair in peerStreamStates)
            current.PeerStreams.Add(pair.Key, pair.Value);
        foreach (var key in requiredStreams)
            current.RequiredPeerStreams.Add(key);

        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        foreach (var write in writes)
            current.PersistedEntries[Convert.ToHexString(write.Key)] = (write.Key, write.Value);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.HasLegacyEncoding = false;
        return receipt;
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
            { DurableAuthor = priorReceipt.DurableAuthor };
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, ResolveAuthorStream(batch));
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
            throw new InvalidDataException("Incoming batch does not continue its author cursor and sender digest chain.");

        int relayAdded = 0;
        foreach (var operation in batch.Operations)
        {
            var ledgerKey = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (next.ChangeLedger.TryGetValue(ledgerKey, out var priorLedger))
            {
                if (!priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                continue;
            }

            next.ChangeLedger.Add(ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), IsLocalOutbound: false));
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
            EnsureRegisteredPeerStreams(next, batch.NamespaceId, operation.AuthorNodeId, batch.SenderEpoch);
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
            ResolveAuthorStream(batch)));

        return new MetadataBatchReceipt(
            batch.CursorStart,
            batch.CursorEnd,
            batch.BatchDigest.ToArray(),
            relayAdded,
            IsDuplicate: false)
        { DurableAuthor = ResolveAuthorStream(batch) };
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
            && left.PreviousBatchDigest.AsSpan().SequenceEqual(right.PreviousBatchDigest)
            && left.BatchDigest.AsSpan().SequenceEqual(right.BatchDigest)
            && left.WirePayload.AsSpan().SequenceEqual(right.WirePayload);

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
            WirePayload = batch.WirePayload.ToArray(),
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
        => new MetadataBatchReceipt(receipt.CursorStart, receipt.CursorEnd, receipt.BatchDigest.ToArray(),
            receipt.NewRelayOperationCount, isDuplicate) { DurableAuthor = receipt.DurableAuthor };

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
            var ledgerKey = new LedgerKey(namespaceId, operation.ChangeId);
            bool alreadyFrozen = current.ChangeLedger.TryGetValue(ledgerKey, out var priorLedger)
                && priorLedger.IsLocalOutbound;
            if (priorLedger is not null)
            {
                if (!priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint))
                    throw new InvalidDataException("Outbound ChangeId conflicts with the global ChangeId ledger.");
                if (!alreadyFrozen)
                    throw new InvalidDataException("ChangeId is already known from an incoming route and cannot be published as a new local operation.");
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

        ulong highWatermark = current.OutboundHighWatermarks.TryGetValue(key, out var priorHighWatermark)
            ? priorHighWatermark
            : 0;
        var frozen = new List<MetadataOutboundItem>(operations.Count);
        foreach (var operation in operations)
        {
            ulong sequence = checked(++highWatermark);
            frozen.Add(new MetadataOutboundItem(sequence, localAuthor, operation.ChangeId, operation.Payload.ToArray()));
        }

        PersistFrozenOutboundDelta(partitionKey, current, key, namespaceId, localAuthor,
            operations, frozen, highWatermark);
        return frozen.Select(CloneOutboundItem).ToArray();
    
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

        PersistOutboundLease(partitionKey, current, key, new LeaseState(generation, consumerId));
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

        PersistStreamWatermark(partitionKey, current, 8,
            namespaceId, localAuthor, safeThroughSequence, highWatermark);
    
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
        ulong peerSafe = GetSafePeerCursor(current, namespaceId, localAuthor);
        bool hasPeerRequirements = current.InterestedPeers.Any(peer => peer.NamespaceId == namespaceId && peer.Peer != localAuthor) ||
            current.RequiredPeerStreams.Any(peer => peer.NamespaceId == namespaceId && peer.AuthorStream == localAuthor && peer.Peer != localAuthor);
        bool hasLocalConsumer = current.OutboundLeases.ContainsKey(key);
        if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
            (hasPeerRequirements && safeThroughSequence > peerSafe) ||
            (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        CompactStreamPayload(partitionKey, current, 7,
            namespaceId, localAuthor, safeThroughSequence, relay: false);
    
        }
    }

    public IReadOnlyList<MetadataPeerStreamSnapshot> ReadPeerStreamStates(
        string scopeId, NodeId localReplica, NodeId peer, uint peerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
            if (localReplica.IsZero || peer.IsZero) throw new ArgumentException("Replica and peer identities must be non-zero.");
            if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<MetadataPeerStreamSnapshot>();
            return state.PeerStreams
                .Where(pair => pair.Key.Peer == peer && pair.Key.PeerEpoch == peerEpoch && pair.Key.NamespaceId == namespaceId)
                .OrderBy(pair => Convert.ToHexString(pair.Key.AuthorStream.ToArray()), StringComparer.Ordinal)
                .Select(pair => new MetadataPeerStreamSnapshot(pair.Key.AuthorStream, ToPublicPeerStreamState(pair.Value)))
                .ToArray();
        }
    }

    public IReadOnlyList<NodeId> ReadOutboundAuthors(string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<NodeId>();
            return state.OutboundHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId)
                .Select(key => key.Author).Distinct()
                .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                .ToArray();
        }
    }

    public IReadOnlyList<NodeId> ReadRelayAuthors(string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<NodeId>();
            return state.RelayHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId)
                .Select(key => key.Author).Distinct()
                .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                .ToArray();
        }
    }

    public MetadataOutboundPage ReadOutboundForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), 0);
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            if (!state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var streamKey = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var progress = state.PeerStreams.TryGetValue(streamKey, out var saved)
                ? saved : EmptyPeerStreamValue();
            ulong cursor = checked(progress.AckedCursor + progress.StreamOffset);
            var key = new OutboundKey(namespaceId, authorStream);
            ulong high = state.OutboundHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (cursor >= high || limit == 0)
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), Math.Min(cursor, high));
            if (!state.Outbound.TryGetValue(key, out var entries))
                throw new InvalidDataException("Outbound high-watermark has no recoverable payload rows.");
            var page = entries.Where(item => item.Sequence > cursor)
                .Take(limit).Select(CloneOutboundItem).ToArray();
            if (progress.AckedCursor == 0 && progress.StreamOffset == 0 &&
                page.Length > 0 && page[0].Sequence > 1)
                throw new SyncException("SNAPSHOT_REQUIRED");
            ValidatePeerPageContinuity(page.Select(item => item.Sequence), cursor, high, page.Length, limit, "outbound");
            return new MetadataOutboundPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }
    }

    public MetadataRelayPage ReadRelayForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), 0);
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            if (!state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var streamKey = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var progress = state.PeerStreams.TryGetValue(streamKey, out var saved)
                ? saved : EmptyPeerStreamValue();
            ulong cursor = checked(progress.AckedCursor + progress.StreamOffset);
            var key = new RelayKey(namespaceId, authorStream);
            ulong high = state.RelayHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (cursor >= high || limit == 0)
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), Math.Min(cursor, high));
            if (!state.Relay.TryGetValue(key, out var entries))
                throw new InvalidDataException("Relay high-watermark has no recoverable payload rows.");
            var page = entries.Where(item => item.Sequence > cursor)
                .Take(limit).Select(CloneRelayItem).ToArray();
            if (progress.AckedCursor == 0 && progress.StreamOffset == 0 &&
                page.Length > 0 && page[0].Sequence > 1)
                throw new SyncException("SNAPSHOT_REQUIRED");
            ValidatePeerPageContinuity(page.Select(item => item.Sequence), cursor, high, page.Length, limit, "relay");
            return new MetadataRelayPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }
    }

    private static void ValidatePeerPageContinuity(IEnumerable<ulong> sequences,
        ulong cursor, ulong highWatermark, int count, int limit, string streamName)
    {
        ulong expected = checked(cursor + 1);
        foreach (ulong sequence in sequences)
        {
            if (sequence != expected)
                throw new InvalidDataException($"Peer {streamName} page contains a gap after its durable cursor.");
            expected = checked(expected + 1);
        }
        ulong scanned = count == 0 ? cursor : expected - 1;
        if (count < limit && scanned < highWatermark)
            throw new InvalidDataException($"Peer {streamName} payload is missing below its durable high-watermark.");
    }

    public void RegisterInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            if (current.InterestedPeers.Contains(rosterKey)) return;

            if (current.HasLegacyEncoding)
            {
                var migrated = current.Clone();
                migrated.InterestedPeers.Add(rosterKey);
                foreach (var outbound in migrated.OutboundHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId && key.Author != peer))
                    EnsurePeerStream(migrated, new PeerStreamKey(peer, peerEpoch, namespaceId, outbound.Author), 0);
                foreach (var relay in migrated.RelayHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId && key.Author != peer))
                    EnsurePeerStream(migrated, new PeerStreamKey(peer, peerEpoch, namespaceId, relay.Author), 0);
                PersistPartition(partitionKey, current, migrated);
                return;
            }

            var streamKeys = current.OutboundHighWatermarks.Keys
                .Where(key => key.NamespaceId == namespaceId && key.Author != peer)
                .Select(key => new PeerStreamKey(peer, peerEpoch, namespaceId, key.Author))
                .Concat(current.RelayHighWatermarks.Keys
                    .Where(key => key.NamespaceId == namespaceId && key.Author != peer)
                    .Select(key => new PeerStreamKey(peer, peerEpoch, namespaceId, key.Author)))
                .Distinct()
                .Where(key => !current.PeerStreams.ContainsKey(key))
                .ToArray();
            var requiredKeys = streamKeys.Where(key => !current.RequiredPeerStreams.Contains(key)).ToArray();
            var writes = new List<(byte[] Key, byte[] Value)>(1 + streamKeys.Length + requiredKeys.Length);
            writes.Add((EncodeInterestedPeerKey(partitionKey, rosterKey), Array.Empty<byte>()));
            foreach (var key in streamKeys)
                writes.Add((EncodePeerStreamKey(partitionKey, key), EncodePeerStreamState(EmptyPeerStreamValue())));
            foreach (var key in requiredKeys)
                writes.Add((EncodeRequiredPeerStreamKey(partitionKey, key), Array.Empty<byte>()));

            CommitPeerMetadataDelta(partitionKey, current, writes, Array.Empty<byte[]>(), () =>
            {
                current.InterestedPeers.Add(rosterKey);
                foreach (var key in streamKeys)
                    current.PeerStreams.Add(key, EmptyPeerStreamValue());
                foreach (var key in requiredKeys)
                    current.RequiredPeerStreams.Add(key);
            });
        }
    }

    public IReadOnlyList<MetadataInterestedPeer> ReadInterestedPeers(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<MetadataInterestedPeer>();
            return state.InterestedPeers.Where(key => key.NamespaceId == namespaceId)
                .Select(key => new MetadataInterestedPeer(key.Peer, key.PeerEpoch, key.NamespaceId))
                .OrderBy(peer => Convert.ToHexString(peer.Peer.ToArray()), StringComparer.Ordinal)
                .ThenBy(peer => peer.PeerEpoch)
                .ToArray();
        }
    }

    public bool IsPeerInitialSyncComplete(
        string scopeId, NodeId localReplica, NodeId peer, uint peerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return false;
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            return state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch));
        }
    }

    public void CompletePeerInitialSync(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId,
        IReadOnlyList<MetadataPeerStreamBaseline> streamBaselines)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            ArgumentNullException.ThrowIfNull(streamBaselines);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            if (!current.InterestedPeers.Contains(rosterKey))
                throw new InvalidOperationException("Peer must be durably registered before initial sync completion.");

            var seen = new HashSet<NodeId>();
            foreach (var baseline in streamBaselines)
            {
                if (baseline is null || baseline.AuthorStream.IsZero ||
                    baseline.AuthorStream == peer || baseline.StreamOffset > long.MaxValue)
                    throw new ArgumentException("Initial-sync author baseline is invalid.", nameof(streamBaselines));
                if (!seen.Add(baseline.AuthorStream))
                    throw new ArgumentException("Initial-sync author baselines must be unique.", nameof(streamBaselines));

                var outboundKey = new OutboundKey(namespaceId, baseline.AuthorStream);
                ulong outboundHigh = current.OutboundHighWatermarks.TryGetValue(outboundKey, out var outHigh)
                    ? outHigh : 0;
                bool hasRelayHistory = current.RelayHighWatermarks.ContainsKey(
                    new RelayKey(namespaceId, baseline.AuthorStream));
                if (baseline.StreamOffset > outboundHigh || (hasRelayHistory && baseline.StreamOffset != 0))
                    throw new InvalidDataException("Snapshot baseline cannot skip relay history or exceed the outbound feed watermark.");
            }

            if (!current.CompletedPeerBaselines.Contains(rosterKey))
            {
                var requiredAuthors = current.PeerStreams.Keys
                    .Where(key => key.Peer == peer && key.PeerEpoch == peerEpoch && key.NamespaceId == namespaceId)
                    .Select(key => key.AuthorStream)
                    .Concat(current.RequiredPeerStreams
                        .Where(key => key.Peer == peer && key.PeerEpoch == peerEpoch && key.NamespaceId == namespaceId)
                        .Select(key => key.AuthorStream))
                    .Distinct();
                if (requiredAuthors.Any(author => !seen.Contains(author)))
                    throw new InvalidDataException("Initial-sync baseline must cover every currently registered peer author stream.");
            }

            if (current.CompletedPeerBaselines.Contains(rosterKey))
            {
                foreach (var baseline in streamBaselines)
                {
                    var key = new PeerStreamKey(peer, peerEpoch, namespaceId, baseline.AuthorStream);
                    if (!current.PeerStreams.TryGetValue(key, out var saved) ||
                        saved.StreamOffset != baseline.StreamOffset || saved.IssuerEpoch != baseline.IssuerEpoch)
                        throw new InvalidDataException("Repeated initial-sync completion conflicts with its durable feed baseline.");
                }
                return;
            }

            if (current.HasLegacyEncoding)
            {
                var migrated = current.Clone();
                foreach (var baseline in streamBaselines)
                {
                    var key = new PeerStreamKey(peer, peerEpoch, namespaceId, baseline.AuthorStream);
                    if (migrated.PeerStreams.TryGetValue(key, out var saved) &&
                        (saved.AckedCursor != 0 || saved.PushedCursor != 0 || saved.PendingBatch is not null))
                        throw new InvalidOperationException("Initial snapshot baseline cannot reset an active peer author stream.");
                    migrated.PeerStreams[key] = new PeerStreamStateValue(
                        0, 0, baseline.StreamOffset, baseline.IssuerEpoch, new byte[16], null);
                    migrated.RequiredPeerStreams.Add(key);
                }
                migrated.CompletedPeerBaselines.Add(rosterKey);
                PersistPartition(partitionKey, current, migrated);
                return;
            }

            var baselineWrites = new List<(byte[] Key, byte[] Value)>(streamBaselines.Count * 2 + 1);
            var baselineStates = new List<(PeerStreamKey Key, PeerStreamStateValue Value)>(streamBaselines.Count);
            var newRequired = new List<PeerStreamKey>(streamBaselines.Count);
            foreach (var baseline in streamBaselines)
            {
                var key = new PeerStreamKey(peer, peerEpoch, namespaceId, baseline.AuthorStream);
                if (current.PeerStreams.TryGetValue(key, out var saved) &&
                    (saved.AckedCursor != 0 || saved.PushedCursor != 0 || saved.PendingBatch is not null))
                    throw new InvalidOperationException("Initial snapshot baseline cannot reset an active peer author stream.");
                var value = new PeerStreamStateValue(
                    0, 0, baseline.StreamOffset, baseline.IssuerEpoch, new byte[16], null);
                baselineStates.Add((key, value));
                baselineWrites.Add((EncodePeerStreamKey(partitionKey, key), EncodePeerStreamState(value)));
                if (!current.RequiredPeerStreams.Contains(key))
                {
                    newRequired.Add(key);
                    baselineWrites.Add((EncodeRequiredPeerStreamKey(partitionKey, key), Array.Empty<byte>()));
                }
            }
            baselineWrites.Add((EncodeCompletedPeerBaselineKey(partitionKey, rosterKey), Array.Empty<byte>()));
            CommitPeerMetadataDelta(partitionKey, current, baselineWrites, Array.Empty<byte[]>(), () =>
            {
                foreach (var item in baselineStates)
                    current.PeerStreams[item.Key] = item.Value;
                foreach (var key in newRequired)
                    current.RequiredPeerStreams.Add(key);
                current.CompletedPeerBaselines.Add(rosterKey);
            });
        }
    }

    public uint? GetRetiredPeerEpoch(string scopeId, NodeId localReplica, NodeId peer, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return null;
            return state.RetiredPeerEpochs.TryGetValue((namespaceId, peer), out uint retiredThrough)
                ? retiredThrough : null;
        }
    }

    public void RetireInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint retiredPeerEpoch, uint replacementPeerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (replacementPeerEpoch <= retiredPeerEpoch)
                throw new ArgumentOutOfRangeException(nameof(replacementPeerEpoch),
                    "Replacement peer epoch must be newer than the retired incarnation.");

            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("The replacement peer epoch has not been registered.");

            var replacement = new InterestedPeerKey(namespaceId, peer, replacementPeerEpoch);
            if (!current.InterestedPeers.Contains(replacement))
                throw new InvalidOperationException("The replacement peer epoch must be durably registered before retirement.");
            uint latestActiveEpoch = current.InterestedPeers
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer)
                .Select(key => key.PeerEpoch).DefaultIfEmpty(0U).Max();
            if (latestActiveEpoch != replacementPeerEpoch)
                throw new InvalidOperationException("Only the newest registered peer epoch can retire an older incarnation.");

            var retiredKey = (NamespaceId: namespaceId, Peer: peer);
            bool alreadyRetired = current.RetiredPeerEpochs.TryGetValue(retiredKey, out uint retiredThrough) &&
                retiredThrough >= retiredPeerEpoch;
            var oldRosterKey = new InterestedPeerKey(namespaceId, peer, retiredPeerEpoch);
            bool hasOldRows = current.InterestedPeers.Contains(oldRosterKey) ||
                current.PeerStreams.Keys.Any(key => key.NamespaceId == namespaceId &&
                    key.Peer == peer && key.PeerEpoch == retiredPeerEpoch) ||
                current.RequiredPeerStreams.Any(key => key.NamespaceId == namespaceId &&
                    key.Peer == peer && key.PeerEpoch == retiredPeerEpoch) ||
                current.CompletedPeerBaselines.Contains(oldRosterKey);
            if (alreadyRetired && !hasOldRows)
                return;

            if (current.HasLegacyEncoding)
            {
                var migrated = current.Clone();
                migrated.InterestedPeers.Remove(oldRosterKey);
                migrated.CompletedPeerBaselines.Remove(oldRosterKey);
                foreach (var stream in migrated.PeerStreams.Keys
                    .Where(key => key.NamespaceId == namespaceId && key.Peer == peer &&
                        key.PeerEpoch == retiredPeerEpoch).ToArray())
                    migrated.PeerStreams.Remove(stream);
                migrated.RequiredPeerStreams.RemoveWhere(key =>
                    key.NamespaceId == namespaceId && key.Peer == peer && key.PeerEpoch == retiredPeerEpoch);
                migrated.RetiredPeerEpochs[retiredKey] = Math.Max(
                    migrated.RetiredPeerEpochs.TryGetValue(retiredKey, out uint prior) ? prior : 0,
                    retiredPeerEpoch);
                PersistPartition(partitionKey, current, migrated);
                return;
            }

            var oldStreams = current.PeerStreams.Keys
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer &&
                    key.PeerEpoch == retiredPeerEpoch).ToArray();
            var oldRequired = current.RequiredPeerStreams
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer &&
                    key.PeerEpoch == retiredPeerEpoch).ToArray();
            var deletes = new List<byte[]>(oldStreams.Length + oldRequired.Length + 2)
            {
                EncodeInterestedPeerKey(partitionKey, oldRosterKey),
                EncodeCompletedPeerBaselineKey(partitionKey, oldRosterKey)
            };
            deletes.AddRange(oldStreams.Select(key => EncodePeerStreamKey(partitionKey, key)));
            deletes.AddRange(oldRequired.Select(key => EncodeRequiredPeerStreamKey(partitionKey, key)));
            uint nextRetiredThrough = Math.Max(
                current.RetiredPeerEpochs.TryGetValue(retiredKey, out uint priorEpoch) ? priorEpoch : 0,
                retiredPeerEpoch);
            byte[] retiredValue = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(retiredValue, nextRetiredThrough);
            var writes = new[]
            {
                (EncodeRetiredPeerEpochKey(partitionKey, namespaceId, peer), retiredValue)
            };
            CommitPeerMetadataDelta(partitionKey, current, writes, deletes, () =>
            {
                current.InterestedPeers.Remove(oldRosterKey);
                current.CompletedPeerBaselines.Remove(oldRosterKey);
                foreach (var key in oldStreams)
                    current.PeerStreams.Remove(key);
                foreach (var key in oldRequired)
                    current.RequiredPeerStreams.Remove(key);
                current.RetiredPeerEpochs[retiredKey] = nextRetiredThrough;
            });
        }
    }

    public void RegisterInterestedPeerStream(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        ulong baselineAckedCursor, ulong streamOffset, uint issuerEpoch, byte[] lastBatchDigest)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (baselineAckedCursor > long.MaxValue || streamOffset > long.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(baselineAckedCursor));
            ValidateDigest(lastBatchDigest, nameof(lastBatchDigest));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            bool hasExisting = current.PeerStreams.TryGetValue(key, out var existing);
            if (hasExisting && IsPristinePeerStream(existing!))
            {
                bool sameBaseline = existing!.AckedCursor == baselineAckedCursor &&
                    existing.StreamOffset == streamOffset &&
                    existing.IssuerEpoch == issuerEpoch &&
                    existing.LastBatchDigest.AsSpan().SequenceEqual(lastBatchDigest);
                if (sameBaseline && current.RequiredPeerStreams.Contains(key))
                {
                    if (baselineAckedCursor > 0)
                        PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                    return;
                }
                PersistPeerStreamState(partitionKey, current, key,
                    new PeerStreamStateValue(baselineAckedCursor, baselineAckedCursor,
                        streamOffset, issuerEpoch, lastBatchDigest.ToArray(), null),
                    ensureRequired: true);
                if (baselineAckedCursor > 0)
                    PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                return;
            }
            if (hasExisting)
            {
                // A live or already-advanced stream is never reset by a repeated
                // membership/snapshot registration call.
                if (current.RequiredPeerStreams.Contains(key))
                {
                    if (baselineAckedCursor > 0)
                        PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                    return;
                }
                throw new InvalidDataException("Interested peer registration conflicts with existing stream progress.");
            }

            var baseline = new PeerStreamStateValue(baselineAckedCursor, baselineAckedCursor,
                streamOffset, issuerEpoch, lastBatchDigest.ToArray(), null);
            PersistPeerStreamState(partitionKey, current, key, baseline, ensureRequired: true);
            if (baselineAckedCursor > 0)
                PersistPeerBaselineMarker(partitionKey, current, rosterKey);
        }
    }

    public ulong GetSafePeerStreamCompactionCursor(
        string scopeId, NodeId localReplica, int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
            if (localReplica.IsZero || authorStream.IsZero) throw new ArgumentException("Replica and author identities must be non-zero.");
            if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return 0;
            return GetSafePeerCursor(state, namespaceId, authorStream);
        }
    }

    public MetadataPeerStreamState GetPeerStreamState(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state) ||
                !state.PeerStreams.TryGetValue(new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream), out var value))
                return EmptyPeerStreamState();
            return ToPublicPeerStreamState(value);
        }
    }

    public MetadataOutboundBatchReservation? GetPendingOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream)
        => GetPeerStreamState(scopeId, localReplica, peer, peerEpoch, namespaceId, authorStream).PendingBatch;

    public MetadataOutboundBatchReservation ReserveOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        MetadataOutboundBatchReservation reservation)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            ValidateReservation(reservation);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (current.PeerStreams.TryGetValue(key, out var existing) && existing.PendingBatch is not null)
                return CloneReservation(existing.PendingBatch);
            if (!current.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");

            if (!current.PeerStreams.TryGetValue(key, out existing))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var prior = existing;
            if (prior.StreamOffset != reservation.StreamOffset)
                throw new InvalidDataException("Outbound reservation changed the durable snapshot stream offset.");
            if (reservation.CursorStart != checked(prior.AckedCursor + 1))
                throw new InvalidDataException("Outbound batch reservation is not contiguous with the durable peer ACK cursor.");
            byte[] expectedPreviousDigest = prior.IssuerEpoch == reservation.IssuerEpoch
                ? prior.LastBatchDigest
                : new byte[16];
            if (!reservation.PreviousBatchDigest.AsSpan().SequenceEqual(expectedPreviousDigest))
                throw new InvalidDataException("Outbound batch reservation does not extend the durable peer digest chain.");

            PersistPeerStreamState(partitionKey, current, key, prior with
            {
                IssuerEpoch = reservation.IssuerEpoch,
                LastBatchDigest = expectedPreviousDigest.ToArray(),
                PendingBatch = CloneReservation(reservation)
            }, ensureRequired: true);
            return CloneReservation(reservation);
        }
    }

    public void MarkOutboundBatchPushed(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, BatchId batchId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("No durable outbound reservation exists.");
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (!current.PeerStreams.TryGetValue(key, out var prior) ||
                prior.PendingBatch is null || prior.PendingBatch.BatchId != batchId)
                throw new InvalidOperationException("Outbound batch is not the active durable reservation.");
            if (prior.PushedCursor >= prior.PendingBatch.CursorEnd) return;
            PersistPeerStreamState(partitionKey, current, key,
                prior with { PushedCursor = prior.PendingBatch.CursorEnd }, ensureRequired: true);
        }
    }

    public void AcknowledgeOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        BatchId batchId, ulong cursorEnd, byte[] batchDigest)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            ValidateDigest(batchDigest, nameof(batchDigest));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("No durable outbound reservation exists.");
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (!current.PeerStreams.TryGetValue(key, out var prior) || prior.PendingBatch is null)
                throw new InvalidOperationException("ACK has no matching durable outbound reservation.");
            var pending = prior.PendingBatch;
            if (pending.BatchId != batchId || pending.CursorEnd != cursorEnd ||
                !pending.BatchDigest.AsSpan().SequenceEqual(batchDigest) || prior.PushedCursor < cursorEnd)
                throw new InvalidDataException("ACK does not match the pushed durable outbound reservation.");

            PersistPeerStreamState(partitionKey, current, key, prior with
            {
                AckedCursor = cursorEnd,
                LastBatchDigest = batchDigest.ToArray(),
                PendingBatch = null
            }, ensureRequired: true);
        }
    }

    private static bool IsPristinePeerStream(PeerStreamStateValue value)
        => value.AckedCursor == 0 && value.PushedCursor == 0 && value.StreamOffset == 0 &&
           value.IssuerEpoch == 0 && value.PendingBatch is null &&
           value.LastBatchDigest.AsSpan().IndexOfAnyExcept((byte)0) < 0;

    private static ulong GetSafePeerCursor(PartitionState state, int namespaceId, NodeId authorStream)
    {
        var peers = state.InterestedPeers.Where(key => key.NamespaceId == namespaceId && key.Peer != authorStream)
            .Select(key => (key.Peer, key.PeerEpoch))
            .Concat(state.RequiredPeerStreams
                .Where(key => key.NamespaceId == namespaceId && key.AuthorStream == authorStream && key.Peer != authorStream)
                .Select(key => (key.Peer, key.PeerEpoch)))
            .Distinct()
            .ToArray();
        if (peers.Length == 0) return 0;
        ulong safe = ulong.MaxValue;
        foreach (var peer in peers)
        {
            var key = new PeerStreamKey(peer.Peer, peer.PeerEpoch, namespaceId, authorStream);
            if (!state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer.Peer, peer.PeerEpoch)) ||
                !state.PeerStreams.TryGetValue(key, out var progress)) return 0;
            safe = Math.Min(safe, checked(progress.AckedCursor + progress.StreamOffset));
        }
        return safe == ulong.MaxValue ? 0 : safe;
    }

    private static void ValidatePeerEpochForAccess(
        PartitionState state, int namespaceId, NodeId peer, uint peerEpoch)
    {
        if (state.RetiredPeerEpochs.TryGetValue((namespaceId, peer), out uint retiredThrough) &&
            peerEpoch <= retiredThrough)
            throw new InvalidDataException("Peer session epoch has been durably retired.");

        uint latestActiveEpoch = state.InterestedPeers
            .Where(key => key.NamespaceId == namespaceId && key.Peer == peer)
            .Select(key => key.PeerEpoch).DefaultIfEmpty(0U).Max();
        if (peerEpoch < latestActiveEpoch)
            throw new InvalidDataException("Peer session epoch is stale relative to the durable namespace roster.");
    }

    private static void EnsurePeerStream(PartitionState state, PeerStreamKey key, uint issuerEpoch)
    {
        if (!state.PeerStreams.ContainsKey(key))
            state.PeerStreams.Add(key, new PeerStreamStateValue(0, 0, 0, issuerEpoch, new byte[16], null));
        state.RequiredPeerStreams.Add(key);
    }

    private static void EnsureRegisteredPeerStreams(
        PartitionState state, int namespaceId, NodeId authorStream, uint issuerEpoch)
    {
        var peers = state.InterestedPeers.Where(key => key.NamespaceId == namespaceId)
            .Select(key => (key.Peer, key.PeerEpoch))
            .Concat(state.RequiredPeerStreams.Where(key => key.NamespaceId == namespaceId)
                .Select(key => (key.Peer, key.PeerEpoch)))
            .Distinct();
        foreach (var peer in peers.Where(peer => peer.Peer != authorStream))
            EnsurePeerStream(state,
                new PeerStreamKey(peer.Peer, peer.PeerEpoch, namespaceId, authorStream), issuerEpoch);
    }

    private static void ValidatePeerStreamArguments(string scopeId, NodeId localReplica,
        NodeId peer, int namespaceId, NodeId authorStream)
    {
        if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero || peer.IsZero || authorStream.IsZero)
            throw new ArgumentException("Replica, peer and author identities must be non-zero.");
        if (peer == localReplica || peer == authorStream)
            throw new ArgumentException("A replica cannot register itself or the author as a downstream interested peer.");
        if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
    }

    private static void ValidateReservation(MetadataOutboundBatchReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        if (reservation.BatchId.IsZero) throw new ArgumentException("BatchId must be non-zero.", nameof(reservation));
        if (reservation.CursorStart == 0 || reservation.CursorEnd < reservation.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(reservation.CursorStart));
        ValidateDigest(reservation.PreviousBatchDigest, nameof(reservation.PreviousBatchDigest));
        ValidateDigest(reservation.BatchDigest, nameof(reservation.BatchDigest));
        ArgumentNullException.ThrowIfNull(reservation.Payload);
        if (reservation.Payload.Length == 0) throw new ArgumentException("Reserved batch payload cannot be empty.", nameof(reservation));
    }

    private static MetadataPeerStreamState EmptyPeerStreamState()
        => new(0, 0, 0, 0, new byte[16], null);
    private static PeerStreamStateValue EmptyPeerStreamValue()
        => new(0, 0, 0, 0, new byte[16], null);
    private static MetadataPeerStreamState ToPublicPeerStreamState(PeerStreamStateValue value)
        => new(value.AckedCursor, value.PushedCursor, value.StreamOffset, value.IssuerEpoch,
            value.LastBatchDigest.ToArray(),
            value.PendingBatch is null ? null : CloneReservation(value.PendingBatch));
    private static MetadataOutboundBatchReservation CloneReservation(MetadataOutboundBatchReservation value)
        => new()
        {
            BatchId = value.BatchId, IssuerEpoch = value.IssuerEpoch, StreamOffset = value.StreamOffset,
            CursorStart = value.CursorStart, CursorEnd = value.CursorEnd,
            PreviousBatchDigest = value.PreviousBatchDigest.ToArray(),
            BatchDigest = value.BatchDigest.ToArray(), Payload = value.Payload.ToArray()
        };

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

        PersistStreamWatermark(partitionKey, current, 5,
            namespaceId, originalAuthor, safeThroughSequence, highWatermark);
    
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
        ulong peerSafe = GetSafePeerCursor(current, namespaceId, originalAuthor);
        bool hasPeerRequirements = current.InterestedPeers.Any(peer =>
            peer.NamespaceId == namespaceId && peer.Peer != originalAuthor) ||
            current.RequiredPeerStreams.Any(peer =>
                peer.NamespaceId == namespaceId && peer.AuthorStream == originalAuthor && peer.Peer != originalAuthor);
        bool hasLocalConsumer = current.RelayAck.ContainsKey(key);
        if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
            (hasPeerRequirements && safeThroughSequence > peerSafe) ||
            (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        CompactStreamPayload(partitionKey, current, 4,
            namespaceId, originalAuthor, safeThroughSequence, relay: true);
    
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


    private void PersistStreamWatermark(
        PartitionKey partitionKey, PartitionState current, byte storageKind,
        int namespaceId, NodeId author, ulong acknowledged, ulong highWatermark)
    {
        if (storageKind is not (5 or 8))
            throw new ArgumentOutOfRangeException(nameof(storageKind));
        byte[] root = PartitionRoot(partitionKey);
        byte[] key = StorageKey(root, storageKind, KeyNamespaceAuthor(namespaceId, author));
        byte[] value = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(value, acknowledged);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), highWatermark);
        string hexKey = Convert.ToHexString(key);
        if (current.PersistedEntries.TryGetValue(hexKey, out var existing) &&
            existing.Value.AsSpan().SequenceEqual(value))
            return;
        if (!current.PersistedEntries.ContainsKey(hexKey) && acknowledged == 0 && highWatermark == 0)
            return;

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        batch.Put(key, value);
        batch.Commit();

        if (storageKind == 5)
            current.RelayAck[new RelayKey(namespaceId, author)] = acknowledged;
        else
            current.OutboundAck[new OutboundKey(namespaceId, author)] = acknowledged;
        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        current.PersistedEntries[Convert.ToHexString(key)] = (key, value);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private void CompactStreamPayload(
        PartitionKey partitionKey, PartitionState current, byte storageKind,
        int namespaceId, NodeId author, ulong safeThroughSequence, bool relay)
    {
        if ((relay && storageKind != 4) || (!relay && storageKind != 7))
            throw new ArgumentOutOfRangeException(nameof(storageKind));
        byte[] root = PartitionRoot(partitionKey);
        List<ulong> removedSequences;
        if (relay)
        {
            if (!current.Relay.TryGetValue(new RelayKey(namespaceId, author), out var entries))
                return;
            removedSequences = entries.Where(item => item.Sequence <= safeThroughSequence)
                .Select(item => item.Sequence).ToList();
        }
        else
        {
            if (!current.Outbound.TryGetValue(new OutboundKey(namespaceId, author), out var entries))
                return;
            removedSequences = entries.Where(item => item.Sequence <= safeThroughSequence)
                .Select(item => item.Sequence).ToList();
        }
        if (removedSequences.Count == 0) return;

        var keys = new List<byte[]>(removedSequences.Count);
        foreach (ulong sequence in removedSequences)
        {
            byte[] suffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
            author.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), sequence);
            keys.Add(StorageKey(root, storageKind, suffix));
        }

        using (var batch = _storage.BeginWriteBatch())
        {
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != current.StorageCommitSeq)
                throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
            foreach (var key in keys) batch.Delete(key);
            batch.Commit();
        }

        if (relay)
            current.Relay[new RelayKey(namespaceId, author)].RemoveAll(item => item.Sequence <= safeThroughSequence);
        else
            current.Outbound[new OutboundKey(namespaceId, author)].RemoveAll(item => item.Sequence <= safeThroughSequence);
        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        foreach (var key in keys)
            current.PersistedEntries.Remove(Convert.ToHexString(key));
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
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
        next.HasLegacyEncoding = false;
        next.PersistedEntries = newEntries;
        _partitions[key] = next;
    }

    private void PersistFrozenOutboundDelta(
        PartitionKey partitionKey,
        PartitionState current,
        OutboundKey outboundKey,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations,
        IReadOnlyList<MetadataOutboundItem> frozen,
        ulong highWatermark)
    {
        byte[] root = PartitionRoot(partitionKey);
        bool requiresLegacyRewrite = current.HasLegacyEncoding;
        if (requiresLegacyRewrite)
        {
            var migrated = current.Clone();
            if (!migrated.Outbound.TryGetValue(outboundKey, out var migratedEntries))
            {
                migratedEntries = new List<MetadataOutboundItem>();
                migrated.Outbound.Add(outboundKey, migratedEntries);
            }
            migratedEntries.AddRange(frozen);
            foreach (var operation in operations)
                migrated.ChangeLedger.Add(new LedgerKey(namespaceId, operation.ChangeId),
                    new ChangeLedgerValue(operation.Fingerprint.ToArray(), IsLocalOutbound: true));
            migrated.OutboundHighWatermarks[outboundKey] = highWatermark;
            EnsureRegisteredPeerStreams(migrated, namespaceId, localAuthor, issuerEpoch: 0);
            PersistPartition(partitionKey, current, migrated);
            return;
        }

        var newPeerStreams = new List<(PeerStreamKey Key, PeerStreamStateValue Value)>();
        var newRequiredStreams = new List<PeerStreamKey>();
        var peers = current.InterestedPeers.Where(peer => peer.NamespaceId == namespaceId)
            .Select(peer => (peer.Peer, peer.PeerEpoch))
            .Concat(current.RequiredPeerStreams.Where(peer => peer.NamespaceId == namespaceId)
                .Select(peer => (peer.Peer, peer.PeerEpoch)))
            .Distinct();
        foreach (var peer in peers.Where(peer => peer.Peer != localAuthor))
        {
            var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch, namespaceId, localAuthor);
            if (!current.PeerStreams.ContainsKey(streamKey))
                newPeerStreams.Add((streamKey, EmptyPeerStreamValue() with { IssuerEpoch = 0 }));
            if (!current.RequiredPeerStreams.Contains(streamKey))
                newRequiredStreams.Add(streamKey);
        }

        var writes = new List<(byte[] Key, byte[] Value)>(operations.Count * 2 + newPeerStreams.Count + newRequiredStreams.Count + 2);
        for (int i = 0; i < operations.Count; i++)
        {
            var operation = operations[i];
            var item = frozen[i];
            byte[] outboundSuffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(outboundSuffix, checked((ushort)namespaceId));
            localAuthor.WriteTo(outboundSuffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(outboundSuffix.AsSpan(18), item.Sequence);
            writes.Add((StorageKey(root, 7, outboundSuffix), EncodePayloadItem(item.ChangeId, item.Payload)));

            byte[] ledgerSuffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(ledgerSuffix, checked((ushort)namespaceId));
            operation.ChangeId.WriteTo(ledgerSuffix.AsSpan(2));
            byte[] ledgerValue = new byte[17];
            operation.Fingerprint.CopyTo(ledgerValue, 0);
            ledgerValue[16] = 1;
            writes.Add((StorageKey(root, 3, ledgerSuffix), ledgerValue));
        }

        byte[] watermarkValue = new byte[16];
        if (current.OutboundAck.TryGetValue(outboundKey, out ulong acknowledged))
            BinaryPrimitives.WriteUInt64LittleEndian(watermarkValue, acknowledged);
        BinaryPrimitives.WriteUInt64LittleEndian(watermarkValue.AsSpan(8), highWatermark);
        writes.Add((StorageKey(root, 8, KeyNamespaceAuthor(namespaceId, localAuthor)), watermarkValue));

        foreach (var peerStream in newPeerStreams)
        {
            byte[] suffix = new byte[38];
            peerStream.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), peerStream.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)namespaceId));
            localAuthor.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(peerStream.Value)));
        }
        foreach (var required in newRequiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
            localAuthor.WriteTo(suffix.AsSpan(2));
            required.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), required.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : Encoding.UTF8.GetBytes(partitionKey.ScopeId);
        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        foreach (var write in writes)
            batch.Put(write.Key, write.Value);
        batch.Commit();

        if (!current.Outbound.TryGetValue(outboundKey, out var entries))
        {
            entries = new List<MetadataOutboundItem>();
            current.Outbound.Add(outboundKey, entries);
        }
        entries.AddRange(frozen);
        foreach (var operation in operations)
            current.ChangeLedger.Add(new LedgerKey(namespaceId, operation.ChangeId),
                new ChangeLedgerValue(operation.Fingerprint.ToArray(), IsLocalOutbound: true));
        current.OutboundHighWatermarks[outboundKey] = highWatermark;
        foreach (var peerStream in newPeerStreams)
            current.PeerStreams.Add(peerStream.Key, peerStream.Value);
        foreach (var required in newRequiredStreams)
            current.RequiredPeerStreams.Add(required);

        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        foreach (var write in writes)
            current.PersistedEntries[Convert.ToHexString(write.Key)] = (write.Key, write.Value);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.HasLegacyEncoding = false;
    }

    private void PersistOutboundLease(
        PartitionKey partitionKey, PartitionState current, OutboundKey key, LeaseState lease)
    {
        byte[] root = PartitionRoot(partitionKey);
        bool requiresLegacyRewrite = current.HasLegacyEncoding;
        if (requiresLegacyRewrite)
        {
            // Preserve all legacy row/key migrations on the first write to an old
            // partition. Modern partitions take the single-row fast path below.
            var migrated = current.Clone();
            migrated.OutboundLeases[key] = lease;
            PersistPartition(partitionKey, current, migrated);
            return;
        }

        byte[] storageKey = StorageKey(root, 10, KeyNamespaceAuthor(key.NamespaceId, key.Author));
        byte[] value = new byte[24];
        BinaryPrimitives.WriteUInt64LittleEndian(value, lease.Generation);
        lease.ConsumerId.TryWriteBytes(value.AsSpan(8));
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : Encoding.UTF8.GetBytes(partitionKey.ScopeId);

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(storageKey, value);
        batch.Commit();

        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        current.PersistedEntries[Convert.ToHexString(storageKey)] = (storageKey, value);
        current.OutboundLeases[key] = lease;
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.HasLegacyEncoding = false;
    }

    private void PersistPeerBaselineMarker(
        PartitionKey partitionKey, PartitionState current, InterestedPeerKey peer)
    {
        if (current.CompletedPeerBaselines.Contains(peer)) return;
        byte[] root = PartitionRoot(partitionKey);
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        byte[] key = StorageKey(root, 19, suffix);
        byte[] value = Array.Empty<byte>();

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        batch.Put(key, value);
        batch.Commit();

        current.CompletedPeerBaselines.Add(peer);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.PersistedEntries[Convert.ToHexString(key)] = (key, value);
    }

    private void PersistPendingApply(
        PartitionKey partitionKey, PartitionState current, Guid applyToken, PendingApplyValue value)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] key = StorageKey(root, 12, applyToken.ToByteArray());
        byte[] encoded = EncodeIncomingBatch(value.Batch);
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : Encoding.UTF8.GetBytes(partitionKey.ScopeId);

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(key, encoded);
        batch.Commit();

        current.PendingApplies.Add(applyToken, value);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.PersistedEntries[Convert.ToHexString(key)] = (key, encoded);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
    }

    private void CommitPeerMetadataDelta(
        PartitionKey partitionKey,
        PartitionState current,
        IReadOnlyList<(byte[] Key, byte[] Value)> writes,
        IReadOnlyList<byte[]> deletes,
        Action publish)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : Encoding.UTF8.GetBytes(partitionKey.ScopeId);

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        foreach (var key in deletes)
            batch.Delete(key);
        foreach (var write in writes)
            batch.Put(write.Key, write.Value);
        batch.Commit();

        publish();
        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        foreach (var key in deletes)
            current.PersistedEntries.Remove(Convert.ToHexString(key));
        foreach (var write in writes)
            current.PersistedEntries[Convert.ToHexString(write.Key)] = (write.Key, write.Value);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        current.HasLegacyEncoding = false;
    }

    private static byte[] EncodeInterestedPeerKey(PartitionKey partitionKey, InterestedPeerKey peer)
    {
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 17, suffix);
    }

    private static byte[] EncodeCompletedPeerBaselineKey(PartitionKey partitionKey, InterestedPeerKey peer)
    {
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 19, suffix);
    }

    private static byte[] EncodePeerStreamKey(PartitionKey partitionKey, PeerStreamKey peer)
    {
        byte[] suffix = new byte[38];
        peer.Peer.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), peer.PeerEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)peer.NamespaceId));
        peer.AuthorStream.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 15, suffix);
    }

    private static byte[] EncodeRequiredPeerStreamKey(PartitionKey partitionKey, PeerStreamKey peer)
    {
        byte[] suffix = new byte[38];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.AuthorStream.WriteTo(suffix.AsSpan(2));
        peer.Peer.WriteTo(suffix.AsSpan(18));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 16, suffix);
    }

    private static byte[] EncodeRetiredPeerEpochKey(PartitionKey partitionKey, int namespaceId, NodeId peer)
    {
        byte[] suffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        peer.WriteTo(suffix.AsSpan(2));
        return StorageKey(PartitionRoot(partitionKey), 18, suffix);
    }

    private void PersistPeerStreamState(
        PartitionKey partitionKey, PartitionState current, PeerStreamKey key,
        PeerStreamStateValue value, bool ensureRequired)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] streamSuffix = new byte[38];
        key.Peer.WriteTo(streamSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(streamSuffix.AsSpan(16), key.PeerEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(streamSuffix.AsSpan(20), checked((ushort)key.NamespaceId));
        key.AuthorStream.WriteTo(streamSuffix.AsSpan(22));
        byte[] streamKey = StorageKey(root, 15, streamSuffix);
        byte[] streamValue = EncodePeerStreamState(value);

        bool addRequired = ensureRequired && !current.RequiredPeerStreams.Contains(key);
        var rosterKey = new InterestedPeerKey(key.NamespaceId, key.Peer, key.PeerEpoch);
        bool addRoster = ensureRequired && !current.InterestedPeers.Contains(rosterKey);
        byte[]? requiredKey = null;
        byte[]? rosterRowKey = null;
        if (addRequired)
        {
            byte[] requiredSuffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(requiredSuffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(requiredSuffix.AsSpan(2));
            key.Peer.WriteTo(requiredSuffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(requiredSuffix.AsSpan(34), key.PeerEpoch);
            requiredKey = StorageKey(root, 16, requiredSuffix);
        }
        if (addRoster)
        {
            byte[] rosterSuffix = new byte[22];
            BinaryPrimitives.WriteUInt16LittleEndian(rosterSuffix, checked((ushort)key.NamespaceId));
            key.Peer.WriteTo(rosterSuffix.AsSpan(2));
            BinaryPrimitives.WriteUInt32LittleEndian(rosterSuffix.AsSpan(18), key.PeerEpoch);
            rosterRowKey = StorageKey(root, 17, rosterSuffix);
        }

        bool addPartitionIdentity = !current.IsPersisted;
        byte[]? identityKey = addPartitionIdentity
            ? StorageKey(root, 0, ReadOnlySpan<byte>.Empty)
            : null;
        byte[]? identityValue = addPartitionIdentity
            ? Encoding.UTF8.GetBytes(partitionKey.ScopeId)
            : null;

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(streamKey, streamValue);
        if (requiredKey is not null)
            batch.Put(requiredKey, Array.Empty<byte>());
        if (rosterRowKey is not null)
            batch.Put(rosterRowKey, Array.Empty<byte>());
        batch.Commit();

        current.PeerStreams[key] = value;
        if (addRequired) current.RequiredPeerStreams.Add(key);
        if (addRoster) current.InterestedPeers.Add(rosterKey);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        if (ReferenceEquals(current.PersistedEntries, EmptyEntries))
            current.PersistedEntries = new Dictionary<string, (byte[] Key, byte[] Value)>(StringComparer.Ordinal);
        if (identityKey is not null)
            current.PersistedEntries[Convert.ToHexString(identityKey)] = (identityKey, identityValue!);
        current.PersistedEntries[Convert.ToHexString(streamKey)] = (streamKey, streamValue);
        if (requiredKey is not null)
            current.PersistedEntries[Convert.ToHexString(requiredKey)] = (requiredKey, Array.Empty<byte>());
        if (rosterRowKey is not null)
            current.PersistedEntries[Convert.ToHexString(rosterRowKey)] = (rosterRowKey, Array.Empty<byte>());
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
            // Protocol receipt identity is (sender, epoch, namespace, batchId).
            // The resolved author stream belongs to receive progress, not the
            // idempotency key; keeping it here would permit BatchId equivocation.
            byte[] suffix = new byte[38];
            pair.Key.Sender.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.Epoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.BatchId.WriteTo(suffix.AsSpan(22));
            byte[] value = new byte[68];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.CursorStart);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value.CursorEnd);
            pair.Value.PreviousBatchDigest.CopyTo(value, 16);
            pair.Value.BatchDigest.CopyTo(value, 32);
            BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(48), pair.Value.NewRelayOperationCount);
            pair.Value.DurableAuthor.WriteTo(value.AsSpan(52));
            Add(StorageKey(root, 1, suffix), value);
        }

        foreach (var pair in state.Receives)
        {
            byte[] suffix = new byte[38];
            pair.Key.Sender.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.Epoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            byte[] value = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.CursorEnd);
            Add(StorageKey(root, 2, suffix), value);
        }

        foreach (var pair in state.ReceiveChains)
        {
            byte[] suffix = new byte[22];
            pair.Key.Sender.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.Epoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            Add(StorageKey(root, 14, suffix), pair.Value.LastBatchDigest.ToArray());
        }

        foreach (var pair in state.ChangeLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
            pair.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            byte[] value = new byte[17];
            pair.Value.Fingerprint.CopyTo(value, 0);
            value[16] = pair.Value.IsLocalOutbound ? (byte)1 : (byte)0;
            Add(StorageKey(root, 3, suffix), value);
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

        var relayWatermarkKeys = new HashSet<RelayKey>(state.RelayAck.Keys);
        relayWatermarkKeys.UnionWith(state.RelayHighWatermarks.Keys);
        foreach (var key in relayWatermarkKeys)
        {
            byte[] value = new byte[16];
            if (state.RelayAck.TryGetValue(key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            if (state.RelayHighWatermarks.TryGetValue(key, out ulong high))
                BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), high);
            Add(StorageKey(root, 5, KeyNamespaceAuthor(key.NamespaceId, key.Author)), value);
        }

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

        var outboundWatermarkKeys = new HashSet<OutboundKey>(state.OutboundHighWatermarks.Keys);
        outboundWatermarkKeys.UnionWith(state.OutboundAck.Keys);
        foreach (var key in outboundWatermarkKeys)
        {
            byte[] value = new byte[16];
            if (state.OutboundAck.TryGetValue(key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            if (state.OutboundHighWatermarks.TryGetValue(key, out ulong high))
                BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), high);
            Add(StorageKey(root, 8, KeyNamespaceAuthor(key.NamespaceId, key.Author)), value);
        }

        foreach (var pair in state.OutboundLeases)
        {
            byte[] value = new byte[24];
            BinaryPrimitives.WriteUInt64LittleEndian(value, pair.Value.Generation);
            pair.Value.ConsumerId.TryWriteBytes(value.AsSpan(8));
            Add(StorageKey(root, 10, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value);
        }

        foreach (var peer in state.RequiredPeerStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
            peer.AuthorStream.WriteTo(suffix.AsSpan(2));
            peer.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), peer.PeerEpoch);
            Add(StorageKey(root, 16, suffix), Array.Empty<byte>());
        }

        foreach (var peer in state.InterestedPeers)
        {
            byte[] suffix = new byte[22];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
            peer.Peer.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
            Add(StorageKey(root, 17, suffix), Array.Empty<byte>());
        }

        foreach (var pair in state.RetiredPeerEpochs)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
            pair.Key.Peer.WriteTo(suffix.AsSpan(2));
            byte[] value = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(value, pair.Value);
            Add(StorageKey(root, 18, suffix), value);
        }

        foreach (var peer in state.CompletedPeerBaselines)
        {
            byte[] suffix = new byte[22];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
            peer.Peer.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
            Add(StorageKey(root, 19, suffix), Array.Empty<byte>());
        }

        foreach (var pair in state.PeerStreams)
        {
            byte[] suffix = new byte[38];
            pair.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            Add(StorageKey(root, 15, suffix), EncodePeerStreamState(pair.Value));
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
            if (kind is 6 or 9 or 11 ||
                (kind == 3 && row.Value.Length == 16) ||
                ((kind is 5 or 8) && row.Value.Length == 8))
                state.HasLegacyEncoding = true;
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
                    if (key.Length is not (38 or 54))
                        throw new InvalidDataException("Metadata receipt key has an invalid length.");
                    if (row.Value.Length is not (52 or 68))
                        throw new InvalidDataException("Stored inbox receipt value has an invalid length.");
                    var receiptKey = new ReceiptKey(NodeId.From(key.Slice(0, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20)),
                        BatchId.From(key.Slice(22, 16)));
                    state.Receipts.Add(receiptKey, new ReceiptValue(
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value),
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8)),
                        row.Value.AsSpan(16, 16).ToArray(),
                        row.Value.AsSpan(32, 16).ToArray(),
                        BinaryPrimitives.ReadInt32LittleEndian(row.Value.AsSpan(48)),
                        row.Value.Length == 68 ? NodeId.From(row.Value.AsSpan(52, 16)) : default));
                    break;
                }
                case 2:
                {
                    var sender = NodeId.From(key.Slice(0, 16));
                    uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16));
                    int namespaceId = BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20));
                    if (key.Length == 22 && row.Value.Length == 24)
                    {
                        // Legacy v1 combined receive cursor + digest-chain checkpoint.
                        state.Receives.Add(new ReceiveKey(sender, epoch, namespaceId, default),
                            new ReceiveState(BinaryPrimitives.ReadUInt64LittleEndian(row.Value)));
                        state.ReceiveChains.Add(new ReceiveChainKey(sender, epoch, namespaceId),
                            new ReceiveChainState(row.Value.AsSpan(8, 16).ToArray()));
                    }
                    else
                    {
                        RequireLength(key, 38);
                        RequireLength(row.Value, 8);
                        state.Receives.Add(new ReceiveKey(sender, epoch, namespaceId,
                            NodeId.From(key.Slice(22, 16))),
                            new ReceiveState(BinaryPrimitives.ReadUInt64LittleEndian(row.Value)));
                    }
                    break;
                }
                case 14:
                {
                    RequireLength(key, 22);
                    RequireLength(row.Value, 16);
                    var chainKey = new ReceiveChainKey(NodeId.From(key.Slice(0, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20)));
                    state.ReceiveChains.Add(chainKey, new ReceiveChainState(row.Value.ToArray()));
                    break;
                }
                case 3:
                {
                    RequireLength(key, 18);
                    if (row.Value.Length is not (16 or 17) ||
                        (row.Value.Length == 17 && row.Value[16] > 1))
                        throw new InvalidDataException("Stored ChangeId ledger value has an invalid length or origin flag.");
                    state.ChangeLedger.Add(new LedgerKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        BatchId.From(key.Slice(2, 16))),
                        new ChangeLedgerValue(row.Value.AsSpan(0, 16).ToArray(),
                            row.Value.Length == 17 && row.Value[16] == 1));
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
                    var streamKey = new RelayKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    if (kind is 5 or 6)
                    {
                        if (kind == 5 && row.Value.Length == 16)
                        {
                            state.RelayAck.Add(streamKey, BinaryPrimitives.ReadUInt64LittleEndian(row.Value));
                            state.RelayHighWatermarks.Add(streamKey, BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8)));
                        }
                        else
                        {
                            RequireLength(row.Value, 8);
                            ulong value = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                            if (kind == 5) state.RelayAck.Add(streamKey, value);
                            else state.RelayHighWatermarks.Add(streamKey, value);
                        }
                    }
                    else
                    {
                        var outboundKey = new OutboundKey(streamKey.NamespaceId, streamKey.Author);
                        if (kind == 8 && row.Value.Length == 16)
                        {
                            state.OutboundAck.Add(outboundKey, BinaryPrimitives.ReadUInt64LittleEndian(row.Value));
                            state.OutboundHighWatermarks.Add(outboundKey, BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8)));
                        }
                        else
                        {
                            RequireLength(row.Value, 8);
                            ulong value = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                            if (kind == 8) state.OutboundHighWatermarks.Add(outboundKey, value);
                            else state.OutboundAck.Add(outboundKey, value);
                        }
                    }
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
                    // Legacy local-origin marker. vNext folds this bit into the
                    // ChangeId ledger value and omits the redundant marker key.
                    RequireLength(key, 18);
                    RequireLength(row.Value, 0);
                    var ledgerKey = new LedgerKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        BatchId.From(key.Slice(2, 16)));
                    if (!state.ChangeLedger.TryGetValue(ledgerKey, out var ledger))
                        throw new InvalidDataException("Legacy outbound-origin marker has no ChangeId ledger entry.");
                    state.ChangeLedger[ledgerKey] = ledger with { IsLocalOutbound = true };
                    break;
                }
                case 18:
                {
                    RequireLength(key, 18);
                    RequireLength(row.Value, 4);
                    state.RetiredPeerEpochs.Add(
                        (BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16))),
                        BinaryPrimitives.ReadUInt32LittleEndian(row.Value));
                    break;
                }
                case 19:
                {
                    RequireLength(key, 22);
                    RequireLength(row.Value, 0);
                    state.CompletedPeerBaselines.Add(new InterestedPeerKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(18))));
                    break;
                }
                case 17:
                {
                    RequireLength(key, 22);
                    RequireLength(row.Value, 0);
                    state.InterestedPeers.Add(new InterestedPeerKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(18))));
                    break;
                }
                case 16:
                {
                    RequireLength(key, 38);
                    RequireLength(row.Value, 0);
                    var requiredKey = new PeerStreamKey(
                        NodeId.From(key.Slice(18, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(34)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)));
                    state.RequiredPeerStreams.Add(requiredKey);
                    break;
                }
                case 15:
                {
                    RequireLength(key, 38);
                    var peerKey = new PeerStreamKey(NodeId.From(key.Slice(0, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16)),
                        BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20)),
                        NodeId.From(key.Slice(22, 16)));
                    state.PeerStreams.Add(peerKey, DecodePeerStreamState(row.Value));
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

    private static byte[] EncodePeerStreamState(PeerStreamStateValue state)
    {
        int payloadLength = state.PendingBatch?.Payload.Length ?? 0;
        byte[] bytes = new byte[46 + (state.PendingBatch is null ? 0 : 80 + payloadLength)];
        bytes[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(1), state.AckedCursor);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(9), state.PushedCursor);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(17), state.StreamOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(25), state.IssuerEpoch);
        state.LastBatchDigest.CopyTo(bytes, 29);
        bytes[45] = state.PendingBatch is null ? (byte)0 : (byte)1;
        if (state.PendingBatch is not null)
        {
            var pending = state.PendingBatch;
            int offset = 46;
            pending.BatchId.WriteTo(bytes.AsSpan(offset)); offset += 16;
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset), pending.IssuerEpoch); offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.StreamOffset); offset += 8;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.CursorStart); offset += 8;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.CursorEnd); offset += 8;
            pending.PreviousBatchDigest.CopyTo(bytes, offset); offset += 16;
            pending.BatchDigest.CopyTo(bytes, offset); offset += 16;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), payloadLength); offset += 4;
            pending.Payload.CopyTo(bytes, offset);
        }
        return bytes;
    }

    private static PeerStreamStateValue DecodePeerStreamState(byte[] bytes)
    {
        if (bytes.Length < 46 || bytes[0] != 1 || bytes[45] > 1)
            throw new InvalidDataException("Stored peer stream state has an invalid header.");
        ulong acked = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(1));
        ulong pushed = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(9));
        ulong streamOffset = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(17));
        uint stateIssuerEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(25));
        byte[] digest = bytes.AsSpan(29, 16).ToArray();
        if (pushed < acked) throw new InvalidDataException("Stored peer pushed cursor is behind its ACK cursor.");
        if (bytes[45] == 0)
        {
            RequireLength(bytes, 46);
            return new PeerStreamStateValue(acked, pushed, streamOffset, stateIssuerEpoch, digest, null);
        }
        if (bytes.Length < 126) throw new InvalidDataException("Stored peer batch reservation is truncated.");
        int offset = 46;
        var batchId = BatchId.From(bytes.AsSpan(offset, 16)); offset += 16;
        uint reservationIssuerEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        ulong reservationStreamOffset = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong cursorStart = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong cursorEnd = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        byte[] previous = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
        byte[] batchDigest = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        if (payloadLength <= 0 || bytes.Length != offset + payloadLength ||
            batchId.IsZero || cursorStart == 0 || cursorEnd < cursorStart ||
            cursorStart != checked(acked + 1) || reservationIssuerEpoch != stateIssuerEpoch || reservationStreamOffset != streamOffset ||
            !previous.AsSpan().SequenceEqual(digest))
            throw new InvalidDataException("Stored peer batch reservation is inconsistent.");
        var pending = new MetadataOutboundBatchReservation
        {
            BatchId = batchId, IssuerEpoch = reservationIssuerEpoch, StreamOffset = reservationStreamOffset, CursorStart = cursorStart, CursorEnd = cursorEnd,
            PreviousBatchDigest = previous, BatchDigest = batchDigest,
            Payload = bytes.AsSpan(offset, payloadLength).ToArray()
        };
        return new PeerStreamStateValue(acked, pushed, streamOffset, stateIssuerEpoch, digest, pending);
    }

    private static byte[] EncodeCompleted(CompletedPendingApplyValue completed)
    {
        byte[] bytes = new byte[69];
        completed.BatchDigest.CopyTo(bytes, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(16), completed.Receipt.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(24), completed.Receipt.CursorEnd);
        completed.Receipt.BatchDigest.CopyTo(bytes, 32);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(48), completed.Receipt.NewRelayOperationCount);
        bytes[52] = completed.Receipt.IsDuplicate ? (byte)1 : (byte)0;
        completed.Receipt.DurableAuthor.WriteTo(bytes.AsSpan(53));
        return bytes;
    }

    private static CompletedPendingApplyValue DecodeCompleted(byte[] bytes)
    {
        if (bytes.Length is not (53 or 69))
            throw new InvalidDataException("Completed apply receipt has an invalid length.");
        if (bytes[52] > 1) throw new InvalidDataException("Completed apply receipt flags are invalid.");
        byte[] digest = bytes.AsSpan(0, 16).ToArray();
        var receipt = new MetadataBatchReceipt(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(16)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(24)),
            bytes.AsSpan(32, 16).ToArray(),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(48)),
            bytes[52] != 0)
        { DurableAuthor = bytes.Length == 69 ? NodeId.From(bytes.AsSpan(53, 16)) : default };
        return new CompletedPendingApplyValue(digest, receipt);
    }

    private static byte[] EncodeIncomingBatch(MetadataIncomingBatch batch)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        byte[] scope = Encoding.UTF8.GetBytes(batch.ScopeId);
        writer.Write(-3); // version 3: preserve raw DELTA_BATCH for Node recovery
        writer.Write(scope.Length); writer.Write(scope);
        writer.Write(batch.LocalReplica.ToArray());
        writer.Write(batch.SenderPeer.ToArray());
        writer.Write(batch.SenderEpoch);
        writer.Write(batch.AuthorStreamNodeId.ToArray());
        writer.Write(checked((ushort)batch.NamespaceId));
        writer.Write(batch.BatchId.ToArray());
        writer.Write(batch.CursorStart); writer.Write(batch.CursorEnd);
        writer.Write(batch.PreviousBatchDigest); writer.Write(batch.BatchDigest);
        writer.Write(batch.WirePayload.Length); writer.Write(batch.WirePayload);
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
        bool hasAuthorStream = versionOrScopeLength == -2 || versionOrScopeLength == -3;
        bool hasWirePayload = versionOrScopeLength == -3;
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
        byte[] wirePayload = hasWirePayload ? ReadExactly(reader, reader.ReadInt32()) : Array.Empty<byte>();
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
            WirePayload = wirePayload,
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
    private readonly record struct ReceiveChainKey(NodeId Sender, uint Epoch, int NamespaceId);
    private readonly record struct LedgerKey(int NamespaceId, BatchId ChangeId);
    private readonly record struct RelayKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundKey(int NamespaceId, NodeId Author);
    private readonly record struct LeaseState(ulong Generation, Guid ConsumerId);
    private readonly record struct PeerStreamKey(NodeId Peer, uint PeerEpoch, int NamespaceId, NodeId AuthorStream);
    private readonly record struct InterestedPeerKey(int NamespaceId, NodeId Peer, uint PeerEpoch);
    private sealed record PeerStreamStateValue(ulong AckedCursor, ulong PushedCursor, ulong StreamOffset,
        uint IssuerEpoch, byte[] LastBatchDigest, MetadataOutboundBatchReservation? PendingBatch);
    private readonly record struct ReceiveState(ulong CursorEnd);
    private readonly record struct ReceiveChainState(byte[] LastBatchDigest);

    private sealed class ReceiptValue(
        ulong cursorStart,
        ulong cursorEnd,
        byte[] previousBatchDigest,
        byte[] batchDigest,
        int newRelayOperationCount,
        NodeId durableAuthor = default)
    {
        public ulong CursorStart { get; } = cursorStart;
        public ulong CursorEnd { get; } = cursorEnd;
        public byte[] PreviousBatchDigest { get; } = previousBatchDigest;
        public byte[] BatchDigest { get; } = batchDigest;
        public int NewRelayOperationCount { get; } = newRelayOperationCount;
        public NodeId DurableAuthor { get; } = durableAuthor;

        public bool Matches(MetadataIncomingBatch batch)
            => CursorStart == batch.CursorStart
                && CursorEnd == batch.CursorEnd
                && PreviousBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest)
                && BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest);
    }

    private sealed record PendingApplyValue(MetadataIncomingBatch Batch);
    private sealed record CompletedPendingApplyValue(byte[] BatchDigest, MetadataBatchReceipt Receipt);

    private sealed record ChangeLedgerValue(byte[] Fingerprint, bool IsLocalOutbound);

    private sealed class PartitionState
    {
        public Dictionary<ReceiptKey, ReceiptValue> Receipts { get; } = new();
        public Dictionary<ReceiveKey, ReceiveState> Receives { get; } = new();
        public Dictionary<ReceiveChainKey, ReceiveChainState> ReceiveChains { get; } = new();
        public Dictionary<LedgerKey, ChangeLedgerValue> ChangeLedger { get; } = new();
        public Dictionary<RelayKey, List<MetadataRelayItem>> Relay { get; } = new();
        public Dictionary<RelayKey, ulong> RelayAck { get; } = new();
        public Dictionary<RelayKey, ulong> RelayHighWatermarks { get; } = new();
        public Dictionary<OutboundKey, List<MetadataOutboundItem>> Outbound { get; } = new();
        public Dictionary<OutboundKey, ulong> OutboundHighWatermarks { get; } = new();
        public Dictionary<OutboundKey, ulong> OutboundAck { get; } = new();
        public Dictionary<OutboundKey, LeaseState> OutboundLeases { get; } = new();
        public Dictionary<Guid, PendingApplyValue> PendingApplies { get; } = new();
        public Dictionary<Guid, CompletedPendingApplyValue> CompletedPendingApplies { get; } = new();
        public Dictionary<PeerStreamKey, PeerStreamStateValue> PeerStreams { get; } = new();
        public HashSet<PeerStreamKey> RequiredPeerStreams { get; } = new();
        public HashSet<InterestedPeerKey> InterestedPeers { get; } = new();
        public HashSet<InterestedPeerKey> CompletedPeerBaselines { get; } = new();
        public Dictionary<(int NamespaceId, NodeId Peer), uint> RetiredPeerEpochs { get; } = new();

        public ulong StorageCommitSeq { get; set; }
        public bool IsPersisted { get; set; }
        public bool HasLegacyEncoding { get; set; }
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
                HasLegacyEncoding = HasLegacyEncoding,
                PersistedEntries = PersistedEntries
            };
            foreach (var pair in Receipts) clone.Receipts.Add(pair.Key, pair.Value);
            foreach (var pair in Receives) clone.Receives.Add(pair.Key, pair.Value);
            foreach (var pair in ReceiveChains) clone.ReceiveChains.Add(pair.Key, pair.Value);
            foreach (var pair in ChangeLedger) clone.ChangeLedger.Add(pair.Key, pair.Value);
            foreach (var pair in Relay) clone.Relay.Add(pair.Key, new List<MetadataRelayItem>(pair.Value));
            foreach (var pair in RelayAck) clone.RelayAck.Add(pair.Key, pair.Value);
            foreach (var pair in RelayHighWatermarks) clone.RelayHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in Outbound) clone.Outbound.Add(pair.Key, new List<MetadataOutboundItem>(pair.Value));
            foreach (var pair in OutboundHighWatermarks) clone.OutboundHighWatermarks.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundAck) clone.OutboundAck.Add(pair.Key, pair.Value);
            foreach (var pair in OutboundLeases) clone.OutboundLeases.Add(pair.Key, pair.Value);
            foreach (var pair in PendingApplies) clone.PendingApplies.Add(pair.Key, pair.Value);
            foreach (var pair in CompletedPendingApplies) clone.CompletedPendingApplies.Add(pair.Key, pair.Value);
            foreach (var pair in PeerStreams) clone.PeerStreams.Add(pair.Key, pair.Value);
            foreach (var peer in RequiredPeerStreams) clone.RequiredPeerStreams.Add(peer);
            foreach (var peer in InterestedPeers) clone.InterestedPeers.Add(peer);
            foreach (var peer in CompletedPeerBaselines) clone.CompletedPeerBaselines.Add(peer);
            foreach (var pair in RetiredPeerEpochs) clone.RetiredPeerEpochs.Add(pair.Key, pair.Value);
            return clone;
        }
    }
}
