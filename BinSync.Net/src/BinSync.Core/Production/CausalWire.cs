// Optional on-wire causal deps trailer (§16.19) appended after the standard batch payload.
// The footer length makes trailer location unambiguous; payload bytes are never scanned for magic.
namespace BinSync;

/// <summary>
/// Wire format after the standard DELTA_BATCH body:
/// magic u32 LE | entryCount varint | per entry: changeId b128 | depCount varint | sorted dep b128*
/// | trailerLength u32 LE (length from magic through the final dependency; excludes this footer).
/// </summary>
public static class CausalWire
{
    /// <summary>Trailer magic number.</summary>
    public const uint Magic = 0x31444343; // "CCD1" LE
    /// <summary>Batch.Flags bit: payload is followed by a causal trailer.</summary>
    public const int FlagHasCausalTrailer = 0x20;

    private const int MaxEntries = 100_000;
    private const int MaxDepsPerEntry = 4_096;
    private const int MaxTotalDeps = 250_000;

    /// <summary>True when any entry declares causal dependencies.</summary>
    public static bool HasDeps(IReadOnlyList<OutboxEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        for (int i = 0; i < entries.Count; i++)
            if (entries[i].CausalDepsOrNull is not null) return true;
        return false;
    }

    /// <summary>Encode a canonical trailer including its length footer.</summary>
    public static byte[] EncodeTrailer(IReadOnlyList<OutboxEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count > MaxEntries)
            throw new ArgumentOutOfRangeException(nameof(entries), "Too many causal trailer entries.");

        var w = new Writer();
        w.U32(Magic).Varint((ulong)entries.Count);
        int totalDeps = 0;
        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            var changeId = entry.ChangeId;
            if (changeId.IsZero)
                throw new ArgumentException("Causal trailer changeId cannot be zero.", nameof(entries));

            var sourceDeps = entry.CausalDepsOrNull ?? Array.Empty<BatchId>();
            if (sourceDeps.Count > MaxDepsPerEntry ||
                (totalDeps = checked(totalDeps + sourceDeps.Count)) > MaxTotalDeps)
                throw new ArgumentOutOfRangeException(nameof(entries), "Causal dependency count exceeds the configured limit.");

            var deps = sourceDeps.ToArray();
            Array.Sort(deps, CompareBatchIds);
            for (int i = 0; i < deps.Length; i++)
            {
                if (deps[i].IsZero)
                    throw new ArgumentException("Causal dependency ID cannot be zero.", nameof(entries));
                if (i > 0 && CompareBatchIds(deps[i - 1], deps[i]) == 0)
                    throw new ArgumentException("Causal dependency IDs must be unique.", nameof(entries));
            }

            w.B128(changeId).Varint((ulong)deps.Length);
            for (int i = 0; i < deps.Length; i++)
                w.B128(deps[i]);
        }

        var body = w.Value();
        var trailer = new byte[checked(body.Length + 4)];
        body.CopyTo(trailer, 0);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
            trailer.AsSpan(body.Length), (uint)body.Length);
        return trailer;
    }

    /// <summary>Validate a standard batch payload and its optional causal trailer without materializing public DTOs.</summary>
    internal static void ValidatePayload(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length < 8)
            throw new InvalidDataException("Truncated DELTA_BATCH header.");
        bool hasTrailer = (payload[7] & FlagHasCausalTrailer) != 0;
        if (!hasTrailer)
        {
            _ = Node.DecodeBatchPayloadInternal(payload);
            return;
        }

        int bodyLength = GetBodyLength(payload);
        var batch = Node.DecodeBatchPayloadInternal(payload.AsMemory(0, bodyLength));
        ApplyTrailerStrict(payload, bodyLength, batch.Entries);
    }

    /// <summary>Return the standard batch body length using the fixed trailer footer.</summary>
    public static int GetBodyLength(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length < 9)
            throw new InvalidDataException("Causal trailer footer is missing.");

        uint trailerLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
            payload.AsSpan(payload.Length - 4));
        if (trailerLength < 5 || trailerLength > (uint)(payload.Length - 4))
            throw new InvalidDataException("Invalid causal trailer length.");

        int bodyLength = payload.Length - 4 - (int)trailerLength;
        uint magic = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(bodyLength));
        if (magic != Magic)
            throw new InvalidDataException("Causal trailer magic does not match the footer.");

        return bodyLength;
    }

    /// <summary>
    /// Try to apply a valid trailer atomically. Returns payload.Length when no valid trailer exists.
    /// Entries must already be decoded from the standard body and their ChangeIds must match.
    /// </summary>
    public static int TryApplyTrailer(byte[] payload, List<OutboxEntry> entries)
    {
        try
        {
            int bodyLength = GetBodyLength(payload);
            ApplyTrailerStrict(payload, bodyLength, entries);
            return bodyLength;
        }
        catch (InvalidDataException) { return payload.Length; }
        catch (IOException) { return payload.Length; }
        catch (ArgumentException) { return payload.Length; }
        catch (OverflowException) { return payload.Length; }
    }

    internal static void ApplyTrailerStrict(byte[] payload, int bodyLength, List<OutboxEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(entries);
        if (GetBodyLength(payload) != bodyLength)
            throw new InvalidDataException("Causal trailer body length changed.");

        int trailerEnd = payload.Length - 4;
        var r = new Reader(payload, bodyLength + 4);
        ulong entryCount = r.Varint();
        if (entryCount > MaxEntries || entryCount != (ulong)entries.Count)
            throw new InvalidDataException("Causal trailer entry count does not match the batch.");

        var decoded = new List<(BatchId ChangeId, List<BatchId> Deps)>(entries.Count);
        int totalDeps = 0;
        for (int i = 0; i < entries.Count; i++)
        {
            var changeId = BatchId.From(r.B128());
            var expectedChangeId = entries[i].ChangeId;
            if (changeId.IsZero || changeId != expectedChangeId)
                throw new InvalidDataException("Causal trailer changeId does not match its batch entry.");

            ulong depCount = r.Varint();
            if (depCount > MaxDepsPerEntry ||
                depCount > (ulong)(r.Remaining / 16) ||
                (totalDeps = checked(totalDeps + (int)depCount)) > MaxTotalDeps)
                throw new InvalidDataException("Causal trailer dependency count exceeds the available payload or configured limit.");

            var deps = new List<BatchId>((int)depCount);
            byte[]? previous = null;
            for (int j = 0; j < (int)depCount; j++)
            {
                var bytes = r.B128();
                if (previous is not null && previous.AsSpan().SequenceCompareTo(bytes) >= 0)
                    throw new InvalidDataException("Causal dependency IDs must be strictly ordered and unique.");
                var dep = BatchId.From(bytes);
                if (dep.IsZero)
                    throw new InvalidDataException("Causal dependency ID cannot be zero.");
                deps.Add(dep);
                previous = bytes;
            }
            decoded.Add((changeId, deps));
        }

        if (r.Pos != trailerEnd)
            throw new InvalidDataException("Trailing bytes inside causal trailer.");

        // Publish only after the entire trailer has been validated.
        for (int i = 0; i < entries.Count; i++)
            entries[i].CausalDeps = decoded[i].Deps;
    }

    /// <summary>Standard batch payload + optional causal trailer.</summary>
    public static byte[] EncodeWithTrailer(byte[] standardPayload, IReadOnlyList<OutboxEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(standardPayload);
        if (!HasDeps(entries)) return standardPayload;
        var trailer = EncodeTrailer(entries);
        var combined = new byte[checked(standardPayload.Length + trailer.Length)];
        standardPayload.CopyTo(combined, 0);
        trailer.CopyTo(combined, standardPayload.Length);
        return combined;
    }

    private static int CompareBatchIds(BatchId left, BatchId right)
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        left.WriteTo(a);
        right.WriteTo(b);
        return a.SequenceCompareTo(b);
    }
}
