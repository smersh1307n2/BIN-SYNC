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
    private readonly object _gate = new();

    public StorageSyncCoordinator(Node node, IDataProvider provider, int namespaceId)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        _namespaceId = namespaceId;
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
                int c = a.RecordId.ToArray().AsSpan().SequenceCompareTo(b.RecordId.ToArray());
                return c != 0 ? c : a.FieldId.CompareTo(b.FieldId);
            });

            Guid token = CreateApplyToken(_namespaceId, mutations);
            return _provider.ApplyRemoteBatch(token, mutations);
        }
    }

    private static Guid CreateApplyToken(int namespaceId, IReadOnlyList<ProviderMutation> mutations)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> header = stackalloc byte[6];
        BinaryPrimitives.WriteUInt16LittleEndian(header, checked((ushort)namespaceId));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(2), mutations.Count);
        hash.AppendData(header);

        foreach (var mutation in mutations)
        {
            Span<byte> fixedPart = stackalloc byte[51];
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
