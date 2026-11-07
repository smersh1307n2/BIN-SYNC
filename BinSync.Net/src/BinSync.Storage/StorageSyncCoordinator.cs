using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BinSync.Storage;

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
        {
            var winners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>();
            ulong afterSequence = 0;
            ulong scannedThroughSequence = 0;
            int totalChanges = 0;

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

            // Publish only final winners after every source page has been validated.
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
                if (!prior.IsDuplicate)
                    throw;
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
    public int RecoverPendingApplies()
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
                    throw new InvalidDataException("Pending apply intent belongs to a different namespace.");

                var mutations = CreateProviderMutations(intent.Batch.Operations);
                if (mutations.Count == 0)
                    throw new InvalidDataException("Pending apply intent contains no provider mutations.");

                _provider.ApplyRemoteBatch(intent.ApplyToken, mutations);
                _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(intent.ApplyToken, intent.Batch.BatchDigest.ToArray()));
                recovered++;
            }

            return recovered;
        }
    }

    private MetadataIncomingBatch CreateMetadataBatch(Batch batch)
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
            SenderPeer = batch.IssuerNodeId,
            SenderEpoch = checked((uint)batch.IssuerEpoch),
            AuthorStreamNodeId = batch.DurableAuthor,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = checked((ulong)batch.CursorStart),
            CursorEnd = checked((ulong)batch.CursorEnd),
            PreviousBatchDigest = batch.PrevBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
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
