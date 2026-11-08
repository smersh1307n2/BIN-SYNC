using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace BinSync;

/// <summary>Canonical v3.1 FULL_REPLACE transfer envelopes. These codecs do not authorize a provider switch.</summary>
public static class FullReplaceTransferProtocol
{
    public const int DigestSize = 32;
    public const int MaxChunkEntries = ProviderFullReplaceDigestBuilder.MaxPageEntries;
    public const int MaxChunkBytes = Frames.MAX_PAYLOAD_SIZE;
    private const int BeginPayloadLength = 2 + 16 + 8 + 32 + 8 + 4 + 32;
    private const int BeginLength = BeginPayloadLength + BatchSignature.SignatureSize;
    public const string ManifestSignDomain = "BIN-SYNC/FULL-REPLACE-MANIFEST/v1";
    private const int TransferIdLength = 2 + 16;

    public sealed record Begin(
        int NamespaceId,
        Guid Generation,
        ulong RosterGeneration,
        byte[] RosterDigest,
        ulong SourceChangeFeedWatermark,
        int ExpectedEntryCount,
        byte[] ExpectedDigest,
        byte[] AuthoritySignature);

    public sealed record Chunk(
        int NamespaceId,
        Guid Generation,
        int StartIndex,
        IReadOnlyList<ProviderMutation> Entries);

    public sealed record TransferId(int NamespaceId, Guid Generation);

    public sealed record Progress(
        int NamespaceId, Guid Generation, int ExpectedEntryCount, int NextIndex,
        byte[] ExpectedDigest, bool IsCommitted, ulong NextFrameSequence);

    public static Begin SignBegin(Begin value, ReadOnlySpan<byte> authoritySeed)
    {
        ValidateBegin(value);
        byte[] payload = EncodeBeginPayload(value);
        byte[] signature = BatchSignature.Sign(authoritySeed, BuildManifestSigningPayload(payload));
        return value with { AuthoritySignature = signature };
    }

    public static void VerifyBegin(
        Begin value, NamespaceRosterCertificate acceptedRoster, TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        ArgumentNullException.ThrowIfNull(trustStore);
        ValidateBegin(value);
        NamespaceRosterProtocol.VerifyOrThrow(acceptedRoster, trustStore);
        if (value.AuthoritySignature is null ||
            value.AuthoritySignature.Length != BatchSignature.SignatureSize)
            throw new CryptographicException("FULL_REPLACE manifest signature is missing.");
        byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(acceptedRoster);
        if (value.NamespaceId != acceptedRoster.NamespaceId ||
            value.RosterGeneration != acceptedRoster.RosterGeneration ||
            !value.RosterDigest.AsSpan().SequenceEqual(rosterDigest))
            throw new InvalidDataException("FULL_REPLACE manifest is not bound to the accepted namespace roster.");
        if (!trustStore.TryGet(acceptedRoster.AuthorityNodeId, out TrustedPeer? trusted) ||
            trusted is null || trusted.Revoked || trusted.Mode != TrustMode.PreShared ||
            trusted.PublicKey.Length != BatchSignature.PublicKeySize)
            throw new CryptographicException("FULL_REPLACE authority is not pinned with active PreShared trust.");
        byte[] payload = EncodeBeginPayload(value);
        if (!BatchSignature.Verify(trusted.PublicKey, BuildManifestSigningPayload(payload), value.AuthoritySignature))
            throw new CryptographicException("FULL_REPLACE authority signature is invalid.");
    }

    public static byte[] EncodeBegin(Begin value)
    {
        ValidateBegin(value);
        if (value.AuthoritySignature is null ||
            value.AuthoritySignature.Length != BatchSignature.SignatureSize)
            throw new ArgumentException("FULL_REPLACE_BEGIN requires an authority signature.", nameof(value));
        byte[] payload = EncodeBeginPayload(value);
        byte[] bytes = new byte[BeginLength];
        payload.CopyTo(bytes, 0);
        value.AuthoritySignature.CopyTo(bytes, BeginPayloadLength);
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
            bytes.Slice(70, DigestSize).ToArray(),
            bytes.Slice(BeginPayloadLength, BatchSignature.SignatureSize).ToArray());
        try { ValidateBegin(value); }
        catch (ArgumentException ex) { throw new InvalidDataException("FULL_REPLACE_BEGIN is invalid.", ex); }
        return value;
    }

    private static byte[] EncodeBeginPayload(Begin value)
    {
        ValidateBegin(value);
        byte[] bytes = new byte[BeginPayloadLength];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, checked((ushort)value.NamespaceId));
        value.Generation.TryWriteBytes(bytes.AsSpan(2, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(18), value.RosterGeneration);
        value.RosterDigest.CopyTo(bytes, 26);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(58), value.SourceChangeFeedWatermark);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(66), value.ExpectedEntryCount);
        value.ExpectedDigest.CopyTo(bytes, 70);
        return bytes;
    }

    private static byte[] BuildManifestSigningPayload(ReadOnlySpan<byte> payload)
    {
        byte[] domain = Encoding.UTF8.GetBytes(ManifestSignDomain);
        byte[] result = new byte[domain.Length + payload.Length];
        domain.CopyTo(result, 0);
        payload.CopyTo(result.AsSpan(domain.Length));
        return result;
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

    public static byte[] EncodeProgress(Progress value)
    {
        ValidateProgress(value);
        byte[] bytes = new byte[67];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, checked((ushort)value.NamespaceId));
        value.Generation.TryWriteBytes(bytes.AsSpan(2, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(18), value.ExpectedEntryCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(22), value.NextIndex);
        value.ExpectedDigest.CopyTo(bytes, 26);
        bytes[58] = value.IsCommitted ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(59), value.NextFrameSequence);
        return bytes;
    }

    public static Progress DecodeProgress(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 67)
            throw new InvalidDataException("FULL_REPLACE_READY length is invalid.");
        var value = new Progress(
            BinaryPrimitives.ReadUInt16LittleEndian(bytes),
            new Guid(bytes.Slice(2, 16), bigEndian: true),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(18)),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(22)),
            bytes.Slice(26, DigestSize).ToArray(),
            bytes[58] switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidDataException("FULL_REPLACE_READY committed flag is invalid.")
            },
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(59)));
        try { ValidateProgress(value); }
        catch (ArgumentException ex) { throw new InvalidDataException("FULL_REPLACE_READY is invalid.", ex); }
        return value;
    }

    public static ulong GetNextFrameSequence(int nextIndex)
    {
        if (nextIndex < 0) throw new ArgumentOutOfRangeException(nameof(nextIndex));
        int pages = nextIndex / MaxChunkEntries + (nextIndex % MaxChunkEntries == 0 ? 0 : 1);
        return checked(2UL + (ulong)pages);
    }

    private static void ValidateProgress(Progress value)
    {
        ArgumentNullException.ThrowIfNull(value);
        ValidateTransferId(value.NamespaceId, value.Generation);
        if (value.ExpectedEntryCount < 0 || value.NextIndex < 0 ||
            value.NextIndex > value.ExpectedEntryCount ||
            value.ExpectedDigest is null || value.ExpectedDigest.Length != DigestSize ||
            value.NextFrameSequence == 0 ||
            value.NextFrameSequence != GetNextFrameSequence(value.NextIndex) ||
            (value.NextIndex != value.ExpectedEntryCount &&
             value.NextIndex % MaxChunkEntries != 0) ||
            (value.IsCommitted && value.NextIndex != value.ExpectedEntryCount))
            throw new ArgumentException("FULL_REPLACE_READY progress is inconsistent.", nameof(value));
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
            value.ExpectedDigest is null || value.ExpectedDigest.Length != DigestSize ||
            value.AuthoritySignature is null || (value.AuthoritySignature.Length != 0 &&
            value.AuthoritySignature.Length != BatchSignature.SignatureSize))
            throw new ArgumentException("FULL_REPLACE_BEGIN metadata is invalid.", nameof(value));
    }

    private static void ValidateTransferId(int namespaceId, Guid generation)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue || generation == Guid.Empty)
            throw new ArgumentException("FULL_REPLACE namespace and generation must be non-zero.");
    }
}
