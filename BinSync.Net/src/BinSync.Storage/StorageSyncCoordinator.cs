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
            var page = _provider.ReadLocalChanges(afterSequence: 0, limit);
            if (page.Changes.Count == 0)
                return 0;

            foreach (var change in page.Changes)
            {
                ProviderMutation mutation = change.Mutation;
                if (mutation.AuthorNodeId != _node.NodeId)
                    throw new InvalidOperationException("A local provider change must use the attached Node's author identity.");
                if (mutation.KeyEpoch != 0)
                    throw new NotSupportedException("The current Node field-write API accepts key epoch zero only.");

                var hlc = new Hlc(mutation.HlcWall, mutation.HlcLogical);
                if (mutation.IsTombstone)
                    _node.TombField(_namespaceId, mutation.RecordId, mutation.FieldId, hlc);
                else
                    _node.WriteField(_namespaceId, mutation.RecordId, mutation.FieldId, mutation.Value, hlc);
            }

            // Do not ACK if persistence fails. A restart can replay the same HLCs
            // into the restored Node without creating a second logical operation.
            persistNode(_node);
            if (page.ScannedThroughSequence > 0)
                _provider.AcknowledgeLocalChanges(page.ScannedThroughSequence);
            return page.Changes.Count;
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
    /// Materializes only the field keys carried by an accepted BIN-SYNC batch.
    /// The apply token is derived from the immutable batch identity, not the receiver's
    /// current winners, so replay remains idempotent even if later batches have advanced
    /// one or more of the same fields.
    /// Whole-record tombstones are rejected until the provider contract has a durable
    /// record-level tombstone representation; they must never be silently treated as fields.
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
                case OpRecordTombstone:
                    throw new NotSupportedException("StorageDataProvider does not yet persist record-level tombstones.");
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
            return _provider.ApplyRemoteBatch(token, mutations);
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
