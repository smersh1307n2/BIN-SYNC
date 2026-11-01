// Batch wire encode/decode (payload path). Partial of Node.

using System.Buffers;

namespace BinSync;

public sealed partial class Node
{
    /// <summary>Upper bound on encoded DELTA_BATCH payload size for <paramref name="batch"/>.</summary>
    public static int EstimateBatchPayloadSize(Batch batch)
    {
        var entries = batch.Entries;
        int est = 108 + 2 * Codec.VarintLength((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
            est += 17 + Codec.VarintLength((ulong)entries[i].Payload.Length) + entries[i].Payload.Length;
        return est;
    }

    /// <summary>Thread-safe entry point. See <see cref="EnterAllGates"/>.</summary>
    public byte[] EncodeBatchPayload(Batch batch)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return EncodeBatchPayloadInternal(batch);
    }

    /// <summary>Encode into <paramref name="destination"/>; returns bytes written.
    /// Destination must be at least <see cref="EstimateBatchPayloadSize"/>.</summary>
    public int EncodeBatchPayload(Batch batch, Span<byte> destination)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
            return EncodeBatchPayloadInternal(batch, destination);
    }

    internal byte[] EncodeBatchPayloadInternal(Batch batch)
    {
        int est = EstimateBatchPayloadSize(batch);
        var dest = new byte[est];
        var w = new Writer(dest);
        WriteBatchPayload(w, batch);
        if (w.Length == dest.Length) return dest;
        return dest.AsSpan(0, w.Length).ToArray();
    }

    internal int EncodeBatchPayloadInternal(Batch batch, Span<byte> destination)
    {
        int est = EstimateBatchPayloadSize(batch);
        if (destination.Length < est)
            throw new ArgumentException($"destination too small (need ~{est}, got {destination.Length})", nameof(destination));

        // Write into a fixed ArrayPool buffer, then copy into the (possibly un-array) Span.
        var rented = ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented);
            WriteBatchPayload(w, batch);
            int n = w.Length;
            w.WrittenSpan.CopyTo(destination);
            return n;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void WriteBatchPayload(Writer w, Batch batch)
    {
        var entries = batch.Entries;
        w.U16((ushort)batch.NamespaceId).U16((ushort)batch.SchemaId).U16((ushort)batch.SchemaVersion);
        w.U8((byte)batch.MergePolicy).U8((byte)batch.Flags);
        w.B128(batch.IssuerNodeId).U32((uint)batch.IssuerEpoch);
        w.U64((ulong)batch.CursorStart).U64((ulong)batch.CursorEnd);
        w.Varint((ulong)entries.Count);
        w.B128(batch.BatchId).B128(batch.PrevBatchDigest);
        w.B128(batch.RecordsRoot).B128(batch.BatchDigest);
        WriteRecordsArray(w, entries);
    }

    /// <summary>Thread-safe entry point. See <see cref="EnterAllGates"/>.</summary>
    /// <remarks>
    /// Returns a batch with <b>owned</b> entry buffers (safe to keep after the frame is recycled).
    /// For decode+apply in one step without the copy, use <see cref="ApplyBatchPayload"/>.
    /// </remarks>
    public Batch DecodeBatchPayload(byte[] data)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var batch = DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)data);
            batch.MaterializeEntries();
            return batch;
        }
    }

    /// <summary>Decode from memory; entries are owned copies (safe after <paramref name="data"/> is dropped).</summary>
    public Batch DecodeBatchPayload(ReadOnlyMemory<byte> data)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var batch = DecodeBatchPayloadInternal(data);
            batch.MaterializeEntries();
            return batch;
        }
    }

    /// <summary>
    /// Decode (zero-copy into <paramref name="data"/>) and apply in one call.
    /// The batch never escapes; safe even if <paramref name="data"/> is pooled and returned after the call.
    /// </summary>
    public BatchResult ApplyBatchPayload(byte[] data, NodeId senderPeer)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var batch = DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)data);
            return ApplyBatchInternal(batch, senderPeer);
        }
    }

    internal Batch DecodeBatchPayloadInternal(byte[] data)
        => DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)data);

    /// <summary>Sliced decode — entry Memory views into <paramref name="data"/>. Do not return this batch to callers.</summary>
    internal Batch DecodeBatchPayloadInternal(ReadOnlyMemory<byte> data)
    {
        var r = new Reader(data);
        int ns = r.U16();
        int schemaId = r.U16();
        int schemaVer = r.U16();
        byte policy = r.U8();
        byte flags = r.U8();
        byte[] issuer = r.B128();
        int epoch = (int)r.U32();
        long cstart = (long)r.U64();
        long cend = (long)r.U64();
        int count = (int)r.Varint();
        byte[] batchId = r.B128();
        byte[] prev = r.B128();
        byte[] root = r.B128();
        byte[] digest = r.B128();
        int recCount = (int)r.Varint();
        if (recCount != count) throw new SyncException("BATCH_TRANSACTION_FAILED");
        var issuerMem = (ReadOnlyMemory<byte>)issuer;
        var entries = new List<OutboxEntry>(recCount);
        for (int i = 0; i < recCount; i++)
        {
            entries.Add(new OutboxEntry
            {
                NamespaceId = ns,
                Seq = 0,
                ChangeId = Zero128,
                RecordId = r.B128Memory(),
                OpKind = r.U8(),
                Payload = r.BytesMemory(),
                AuthorNodeId = issuerMem,
                CreatedAt = 0,
            });
        }
        return new Batch(ns, schemaId, schemaVer, policy, flags, issuer, epoch,
                         cstart, cend, batchId, prev, root, digest, entries);
    }
}
