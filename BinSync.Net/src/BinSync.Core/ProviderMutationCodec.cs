using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BinSync;

public static class ProviderMutationCodec
{
    public const int FixedEncodedLength = 81;

    public static byte[] Encode(ProviderMutation mutation)
    {
        Validate(mutation);
        byte[] result = new byte[checked(FixedEncodedLength + mutation.Value.Length)];
        mutation.RecordId.WriteTo(result);
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(16), mutation.FieldId);
        result[18] = (byte)((mutation.IsTombstone ? 1 : 0) | (mutation.IsRecordTombstone ? 2 : 0));
        BinaryPrimitives.WriteUInt64LittleEndian(result.AsSpan(19), mutation.HlcWall);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(27), mutation.HlcLogical);
        mutation.AuthorNodeId.WriteTo(result.AsSpan(31));
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(47), mutation.KeyEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(49), checked((uint)mutation.Value.Length));
        mutation.ChangeId.WriteTo(result.AsSpan(53, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(69), mutation.OriginAuthorEpoch);
        BinaryPrimitives.WriteUInt64LittleEndian(result.AsSpan(73), mutation.OriginSequence);
        mutation.Value.CopyTo(result.AsSpan(FixedEncodedLength));
        return result;
    }

    public static ProviderMutation Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < FixedEncodedLength)
            throw new InvalidDataException("Stored provider mutation is truncated.");
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(49));
        if (valueLength > int.MaxValue - FixedEncodedLength ||
            bytes.Length != FixedEncodedLength + (int)valueLength ||
            bytes[18] > 3 || bytes[18] == 3)
            throw new InvalidDataException("Stored provider mutation length or flags are invalid.");
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(bytes.Slice(0, 16)),
            FieldId = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(16)),
            IsTombstone = (bytes[18] & 1) != 0,
            IsRecordTombstone = (bytes[18] & 2) != 0,
            HlcWall = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(19)),
            HlcLogical = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(27)),
            AuthorNodeId = NodeId.From(bytes.Slice(31, 16)),
            KeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(47)),
            ChangeId = BatchId.From(bytes.Slice(53, 16)),
            OriginAuthorEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(69)),
            OriginSequence = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(73)),
            Value = bytes.Slice(FixedEncodedLength).ToArray()
        };
        try { Validate(mutation); }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Stored provider mutation violates its canonical contract.", exception);
        }
        return mutation;
    }

    public static void Validate(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        if (mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero)
            throw new ArgumentException("RecordId and original AuthorNodeId must be non-zero.", nameof(mutation));
        ArgumentNullException.ThrowIfNull(mutation.Value);
        bool hasChangeId = !mutation.ChangeId.IsZero;
        bool hasOriginEpoch = mutation.OriginAuthorEpoch != 0;
        bool hasOriginSequence = mutation.OriginSequence != 0;
        if (hasChangeId != hasOriginEpoch || hasChangeId != hasOriginSequence)
            throw new ArgumentException("ChangeId, origin epoch and origin sequence must be present together.", nameof(mutation));
        if (mutation.IsRecordTombstone)
        {
            if (mutation.IsTombstone || mutation.FieldId != Model.LifecycleFieldId ||
                mutation.KeyEpoch != 0 || mutation.Value.Length != 0)
                throw new ArgumentException("Record tombstone must use the lifecycle field, key epoch zero and no value.", nameof(mutation));
        }
        else if (mutation.IsTombstone && mutation.Value.Length != 0)
        {
            throw new ArgumentException("Field tombstone mutation must not carry a value payload.", nameof(mutation));
        }
    }

    public static int CompareCanonical(ProviderMutation left, ProviderMutation right)
    {
        Span<byte> leftId = stackalloc byte[16];
        Span<byte> rightId = stackalloc byte[16];
        left.RecordId.WriteTo(leftId);
        right.RecordId.WriteTo(rightId);
        int order = leftId.SequenceCompareTo(rightId);
        if (order != 0) return order;
        if (left.IsRecordTombstone != right.IsRecordTombstone)
            return left.IsRecordTombstone ? -1 : 1;
        return left.FieldId.CompareTo(right.FieldId);
    }

    public static void WriteSortKey(ProviderMutation mutation, Span<byte> destination)
    {
        if (destination.Length < 19) throw new ArgumentException("Sort-key destination must be at least 19 bytes.", nameof(destination));
        mutation.RecordId.WriteTo(destination);
        destination[16] = mutation.IsRecordTombstone ? (byte)0 : (byte)1;
        BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(17, 2),
            mutation.IsRecordTombstone ? (ushort)0 : mutation.FieldId);
    }
}

public sealed class ProviderFullReplaceDigestBuilder : IDisposable
{
    public const int MaxPageEntries = 4096;
    private readonly IncrementalHash _hash;
    private readonly byte[] _lastSortKey = new byte[19];
    private int _entryCount;
    private bool _completed;
    private bool _faulted;
    private bool _disposed;

    public ProviderFullReplaceDigestBuilder(Guid generation, ulong sourceChangeFeedWatermark)
    {
        if (generation == Guid.Empty)
            throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
        _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        ProviderFullReplaceDigest.AppendHeader(_hash, generation, sourceChangeFeedWatermark);
    }

    public int EntryCount => _entryCount;

    public void AppendPage(IReadOnlyList<ProviderMutation> entries)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count > MaxPageEntries)
            throw new ArgumentOutOfRangeException(nameof(entries), "Digest page exceeds the bounded provider page size.");
        try
        {
            Span<byte> sortKey = stackalloc byte[19];
            foreach (var mutation in entries)
            {
                ArgumentNullException.ThrowIfNull(mutation);
                ProviderMutationCodec.WriteSortKey(mutation, sortKey);
                if (_entryCount > 0 && _lastSortKey.AsSpan().SequenceCompareTo(sortKey) >= 0)
                    throw new InvalidDataException("Full-replace digest pages must be strictly ordered and contain no duplicate identities.");
                _hash.AppendData(ProviderMutationCodec.Encode(mutation));
                sortKey.CopyTo(_lastSortKey);
                _entryCount = checked(_entryCount + 1);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    public byte[] Complete()
    {
        EnsureOpen();
        ProviderFullReplaceDigest.AppendEntryCount(_hash, _entryCount);
        _completed = true;
        return _hash.GetHashAndReset();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _hash.Dispose();
    }

    private void EnsureOpen()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("Full-replace digest builder is already completed.");
        if (_faulted) throw new InvalidOperationException("Full-replace digest builder is faulted.");
    }
}

public static class ProviderFullReplaceDigest
{
    public static void AppendHeader(IncrementalHash hash, Guid generation, ulong sourceChangeFeedWatermark)
    {
        ArgumentNullException.ThrowIfNull(hash);
        if (generation == Guid.Empty)
            throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
        hash.AppendData("BIN-SYNC/FULL-REPLACE/v2"u8);
        Span<byte> header = stackalloc byte[24];
        generation.TryWriteBytes(header[..16], true, out _);
        BinaryPrimitives.WriteUInt64LittleEndian(header[16..24], sourceChangeFeedWatermark);
        hash.AppendData(header);
    }

    public static void AppendEntryCount(IncrementalHash hash, int count)
    {
        ArgumentNullException.ThrowIfNull(hash);
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, count);
        hash.AppendData(bytes);
    }

    public static byte[] Compute(Guid generation, ulong sourceChangeFeedWatermark,
        IEnumerable<ProviderMutation> mutations)
    {
        if (generation == Guid.Empty)
            throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
        ArgumentNullException.ThrowIfNull(mutations);
        ProviderMutation[] ordered = mutations.ToArray();
        foreach (var mutation in ordered) ArgumentNullException.ThrowIfNull(mutation);
        Array.Sort(ordered, ProviderMutationCodec.CompareCanonical);
        using var builder = new ProviderFullReplaceDigestBuilder(generation, sourceChangeFeedWatermark);
        for (int offset = 0; offset < ordered.Length; offset += ProviderFullReplaceDigestBuilder.MaxPageEntries)
        {
            int count = Math.Min(ProviderFullReplaceDigestBuilder.MaxPageEntries, ordered.Length - offset);
            builder.AppendPage(new ArraySegment<ProviderMutation>(ordered, offset, count));
        }
        return builder.Complete();
    }
}
