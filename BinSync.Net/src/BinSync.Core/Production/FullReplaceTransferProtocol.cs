using System.Buffers.Binary;

namespace BinSync;

/// <summary>Canonical v3.1 FULL_REPLACE transfer envelopes. These codecs do not authorize a provider switch.</summary>
public static class FullReplaceTransferProtocol
{
    public const int DigestSize = 32;
    public const int MaxChunkEntries = ProviderFullReplaceDigestBuilder.MaxPageEntries;
    public const int MaxChunkBytes = 16 * 1024 * 1024;
    private const int BeginLength = 2 + 16 + 8 + 32 + 8 + 4 + 32;
    private const int TransferIdLength = 2 + 16;

    public sealed record Begin(
        int NamespaceId,
        Guid Generation,
        ulong RosterGeneration,
        byte[] RosterDigest,
        ulong SourceChangeFeedWatermark,
        int ExpectedEntryCount,
        byte[] ExpectedDigest);

    public sealed record Chunk(
        int NamespaceId,
        Guid Generation,
        int StartIndex,
        IReadOnlyList<ProviderMutation> Entries);

    public sealed record TransferId(int NamespaceId, Guid Generation);

    public static byte[] EncodeBegin(Begin value)
    {
        ValidateBegin(value);
        byte[] bytes = new byte[BeginLength];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, checked((ushort)value.NamespaceId));
        value.Generation.TryWriteBytes(bytes.AsSpan(2, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(18), value.RosterGeneration);
        value.RosterDigest.CopyTo(bytes, 26);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(58), value.SourceChangeFeedWatermark);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(66), value.ExpectedEntryCount);
        value.ExpectedDigest.CopyTo(bytes, 70);
        return bytes;
    }

    public static Begin DecodeBegin(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != BeginLength)
            throw new InvalidDataException("FULL_REPLACE_BEGIN length is invalid.");
        var value = new Begin(
            BinaryPrimitives.ReadUInt16LittleEndian(bytes),
            new Guid(bytes.Slice(2, 16), bigEndian: true),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(18)),
            bytes.Slice(26, DigestSize).ToArray(),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(58)),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(66)),
            bytes.Slice(70, DigestSize).ToArray());
        try { ValidateBegin(value); }
        catch (ArgumentException ex) { throw new InvalidDataException("FULL_REPLACE_BEGIN is invalid.", ex); }
        return value;
    }

    public static byte[] EncodeChunk(Chunk value)
    {
        ValidateTransferId(value.NamespaceId, value.Generation);
        ArgumentNullException.ThrowIfNull(value.Entries);
        if (value.StartIndex < 0 || value.Entries.Count is < 1 or > MaxChunkEntries)
            throw new ArgumentOutOfRangeException(nameof(value));
        int total = 2 + 16 + 4 + 2;
        var encoded = new byte[value.Entries.Count][];
        for (int i = 0; i < value.Entries.Count; i++)
        {
            encoded[i] = ProviderMutationCodec.Encode(value.Entries[i]);
            total = checked(total + 4 + encoded[i].Length);
            if (total > MaxChunkBytes)
                throw new ArgumentOutOfRangeException(nameof(value), "FULL_REPLACE_CHUNK exceeds the bounded wire size.");
        }

        byte[] bytes = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, checked((ushort)value.NamespaceId));
        value.Generation.TryWriteBytes(bytes.AsSpan(2, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(18), value.StartIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(22), checked((ushort)encoded.Length));
        int offset = 24;
        foreach (byte[] entry in encoded)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset), checked((uint)entry.Length));
            offset += 4;
            entry.CopyTo(bytes, offset);
            offset += entry.Length;
        }
        return bytes;
    }

    public static Chunk DecodeChunk(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 24 || bytes.Length > MaxChunkBytes)
            throw new InvalidDataException("FULL_REPLACE_CHUNK size is invalid.");
        int namespaceId = BinaryPrimitives.ReadUInt16LittleEndian(bytes);
        Guid generation = new(bytes.Slice(2, 16), bigEndian: true);
        int startIndex = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(18));
        int count = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(22));
        try { ValidateTransferId(namespaceId, generation); }
        catch (ArgumentException ex) { throw new InvalidDataException("FULL_REPLACE_CHUNK identity is invalid.", ex); }
        if (startIndex < 0 || count is < 1 or > MaxChunkEntries)
            throw new InvalidDataException("FULL_REPLACE_CHUNK range is invalid.");

        var entries = new ProviderMutation[count];
        int offset = 24;
        for (int i = 0; i < count; i++)
        {
            if (bytes.Length - offset < 4)
                throw new InvalidDataException("FULL_REPLACE_CHUNK entry length is truncated.");
            uint length = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(offset));
            offset += 4;
            if (length < ProviderMutationCodec.FixedEncodedLength ||
                length > int.MaxValue || length > bytes.Length - offset)
                throw new InvalidDataException("FULL_REPLACE_CHUNK entry length is invalid.");
            entries[i] = ProviderMutationCodec.Decode(bytes.Slice(offset, (int)length));
            offset += (int)length;
        }
        if (offset != bytes.Length)
            throw new InvalidDataException("FULL_REPLACE_CHUNK contains trailing bytes.");
        return new Chunk(namespaceId, generation, startIndex, entries);
    }

    public static byte[] EncodeTransferId(TransferId value)
    {
        ArgumentNullException.ThrowIfNull(value);
        ValidateTransferId(value.NamespaceId, value.Generation);
        byte[] bytes = new byte[TransferIdLength];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, checked((ushort)value.NamespaceId));
        value.Generation.TryWriteBytes(bytes.AsSpan(2, 16), bigEndian: true, out _);
        return bytes;
    }

    public static TransferId DecodeTransferId(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != TransferIdLength)
            throw new InvalidDataException("FULL_REPLACE control frame length is invalid.");
        int namespaceId = BinaryPrimitives.ReadUInt16LittleEndian(bytes);
        Guid generation = new(bytes.Slice(2, 16), bigEndian: true);
        try { ValidateTransferId(namespaceId, generation); }
        catch (ArgumentException ex) { throw new InvalidDataException("FULL_REPLACE control frame identity is invalid.", ex); }
        return new TransferId(namespaceId, generation);
    }

    private static void ValidateBegin(Begin value)
    {
        ArgumentNullException.ThrowIfNull(value);
        ValidateTransferId(value.NamespaceId, value.Generation);
        if (value.RosterGeneration == 0 ||
            value.RosterDigest is null || value.RosterDigest.Length != DigestSize ||
            value.ExpectedEntryCount < 0 ||
            value.ExpectedDigest is null || value.ExpectedDigest.Length != DigestSize)
            throw new ArgumentException("FULL_REPLACE_BEGIN metadata is invalid.", nameof(value));
    }

    private static void ValidateTransferId(int namespaceId, Guid generation)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue || generation == Guid.Empty)
            throw new ArgumentException("FULL_REPLACE namespace and generation must be non-zero.");
    }
}
