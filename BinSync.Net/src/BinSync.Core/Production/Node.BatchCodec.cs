// Batch wire encode/decode (payload path). Partial of Node.

using System.Buffers;

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    // Compact transport-only record representation. It intentionally has no mutable
    // public DTO members and carries no per-entry CausalDeps collection.
    internal static Digest16 ComputeTransportRecordsRoot(TransportDeltaEntry[] entries, int count)
    {
        int size = checked(Domains.DomainRecordsBytes.Length +
            Codec.VarintLength((ulong)count));
        for (int i = 0; i < count; i++)
            size = checked(size + 33 +
                Codec.VarintLength((ulong)entries[i].Payload.Length) + entries[i].Payload.Length);

        var rented = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var writer = new Writer(rented);
            writer.Raw(Domains.DomainRecordsBytes);
            WriteTransportRecordsArray(writer, entries, count);
            return Digests.Blake3Digest16(writer.WrittenSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    internal static void WriteTransportRecordsArray(Writer writer, TransportDeltaEntry[] entries, int count)
    {
        writer.Varint((ulong)count);
        for (int i = 0; i < count; i++)
        {
            ref readonly var entry = ref entries[i];
            if (entry.ChangeId.IsZero)
                throw new SyncException("BATCH_TRANSACTION_FAILED");
            writer.B128(entry.RecordId).B128(entry.ChangeId).U8(entry.OpKind)
                .Varint((ulong)entry.Payload.Length).Raw(entry.Payload.Span);
        }
    }


    /// <summary>Upper bound on encoded DELTA_BATCH payload size for <paramref name="batch"/>.</summary>
    public static int EstimateBatchPayloadSize(Batch batch)
    {
        ValidateBatchForWire(batch);
        var entries = batch.Entries;
        int est = 108 + 2 * Codec.VarintLength((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            var payload = entries[i].Payload;
            est = checked(est + 33 + Codec.VarintLength((ulong)payload.Length) + payload.Length);
        }
        return est;
    }

    private static void ValidateBatchForWire(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateNamespaceId(batch.NamespaceId);
        if ((uint)batch.SchemaId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(batch.SchemaId));
        if ((uint)batch.SchemaVersion > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(batch.SchemaVersion));
        if ((uint)batch.MergePolicy > byte.MaxValue) throw new ArgumentOutOfRangeException(nameof(batch.MergePolicy));
        if ((uint)batch.Flags > byte.MaxValue) throw new ArgumentOutOfRangeException(nameof(batch.Flags));
        if (batch.IssuerEpoch < 0) throw new ArgumentOutOfRangeException(nameof(batch.IssuerEpoch));
        if (batch.CursorStart < 0 || batch.CursorEnd < batch.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(batch.CursorStart), "Batch cursors must be non-negative and ordered.");
        ArgumentNullException.ThrowIfNull(batch.Entries);

    }

    // The canonical records array carries both an envelope recordId/opKind and an
    // operation payload. They must identify the same operation before any state is staged.
    // In particular, RecordTombstone repeats recordId inside its payload.
    private static void ValidateEntryOperation(OutboxEntry entry, object op, BatchId batchId)
    {
        byte expectedKind = op switch
        {
            OpFieldWrite => OpsConst.TagFieldWrite,
            OpFieldTomb => OpsConst.TagFieldTomb,
            OpRecordTombstone => OpsConst.TagRecordTomb,
            _ => throw new SyncException("BATCH_TRANSACTION_FAILED", batchId),
        };

        if (entry.OpKind != expectedKind ||
            (op is OpRecordTombstone tombstone && entry.RecordId != tombstone.RecordId))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batchId);

        if (op is OpFieldWrite lifecycleWrite &&
            lifecycleWrite.FieldId == Model.LifecycleFieldId &&
            (lifecycleWrite.KeyEpoch != 0 || lifecycleWrite.Value.Length != 1 ||
             lifecycleWrite.Value[0] is not Model.Alive and not Model.Dead))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batchId);
        if (op is OpFieldTomb lifecycleTomb &&
            lifecycleTomb.FieldId == Model.LifecycleFieldId)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batchId);
    }

    /// <summary>Thread-safe entry point. See <see cref="EnterAllGates"/>.</summary>
    /// <summary>Encode batch to wire bytes. Lock-free: operates only on the given <see cref="Batch"/> snapshot.</summary>
    public byte[] EncodeBatchPayload(Batch batch) => EncodeBatchPayloadInternal(batch);

    /// <summary>Encode into <paramref name="destination"/>; returns bytes written.
    /// Destination must be at least <see cref="EstimateBatchPayloadSize"/>. Lock-free.</summary>
    public int EncodeBatchPayload(Batch batch, Span<byte> destination) =>
        EncodeBatchPayloadInternal(batch, destination);

    internal byte[] EncodeBatchPayloadInternal(Batch batch)
    {
        // Single allocation sized to estimate; shrink in-place when overshot (no second LOH copy).
        int est = EstimateBatchPayloadSize(batch);
        var dest = new byte[est];
        var w = new Writer(dest);
        WriteBatchPayload(w, batch);
        if (w.Length == dest.Length) return dest;
        Array.Resize(ref dest, w.Length);
        return dest;
    }

    internal int EncodeBatchPayloadInternal(Batch batch, Span<byte> destination)
    {
        int est = EstimateBatchPayloadSize(batch);
        if (destination.Length < est)
            throw new ArgumentException($"destination too small (need ~{est}, got {destination.Length})", nameof(destination));

        // Pooled scratch → one memcpy into caller Span (no LOH exact-sized intermediate).
        var w = Writer.Rent(est);
        try
        {
            WriteBatchPayload(w, batch);
            int n = w.Length;
            w.WrittenSpan.CopyTo(destination);
            return n;
        }
        finally
        {
            w.ReturnPool();
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
    /// <summary>Decode batch payload. Lock-free; materializes owned entry buffers.</summary>
    public Batch DecodeBatchPayload(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        // One contiguous ownership copy lets every decoded entry reference its own
        // immutable slice. Materializing every payload separately creates N arrays
        // and copies the same bytes again.
        var owned = data.ToArray();
        return DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)owned);
    }

    /// <summary>Decode from memory; returned entry slices are backed by one owned copy. Lock-free.</summary>
    public Batch DecodeBatchPayload(ReadOnlyMemory<byte> data)
    {
        var owned = data.ToArray();
        return DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)owned);
    }

    /// <summary>
    /// Decode (zero-copy into <paramref name="data"/>) and apply in one call.
    /// The batch never escapes; safe even if <paramref name="data"/> is pooled and returned after the call.
    /// </summary>
    public BatchResult ApplyBatchPayload(byte[] data, NodeId senderPeer)
        => ApplyBatchPayloadWithBatch(data, senderPeer).Result;

    /// <summary>
    /// Decode and apply a batch, returning the resolved batch alongside its result.
    /// The returned entries are detached from caller-owned data only for the lifetime
    /// of the supplied payload; callers retaining metadata must copy the payload slices.
    /// </summary>
    internal (Batch Batch, BatchResult Result) ApplyBatchPayloadWithBatch(byte[] data, NodeId senderPeer)
    {
        Batch batch;
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            batch = DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)data);
            result = ApplyBatchInternal(batch, senderPeer, durablePayload: data);
        }
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return (batch, result);
    }

    /// <summary>
    /// Apply a batch whose receive cursor, digest chain and inbox receipt were
    /// accepted by the durable Metadata Store before this method is called.
    /// Node still validates the payload and applies record state, but does not
    /// publish a second receive-state authority or relay mirror.
    /// </summary>
    internal (Batch Batch, BatchResult Result) ApplyBatchPayloadWithMetadataAuthority(
        byte[] data, NodeId senderPeer, IReadOnlyList<bool> applyOperations)
    {
        ArgumentNullException.ThrowIfNull(applyOperations);
        Batch batch = DecodeBatchPayloadForMetadataAuthority(data, senderPeer);
        if (applyOperations.Count != batch.Entries.Count)
            throw new InvalidDataException("Metadata apply-decision count does not match the incoming batch.");
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
            result = ApplyBatchInternal(batch, senderPeer, durablePayload: data,
                receiveStateOwnedByMetadataStore: true, metadataApplyOperations: applyOperations);
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return (batch, result);
    }

    internal Batch DecodeBatchPayloadInternal(byte[] data)
        => DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)data);

    /// <summary>Sliced decode — entry Memory views into <paramref name="data"/>. Do not return this batch to callers.</summary>
    internal static Batch DecodeBatchPayloadInternal(ReadOnlyMemory<byte> data)
    {
        var r = new Reader(data);
        int ns = r.U16();
        int schemaId = r.U16();
        int schemaVer = r.U16();
        byte policy = r.U8();
        byte flags = r.U8();
        var issuer = r.ReadNodeId();
        uint epochWire = r.U32();
        ulong cstartWire = r.U64();
        ulong cendWire = r.U64();
        ulong countWire = r.Varint();
        if (epochWire > int.MaxValue || cstartWire > long.MaxValue || cendWire > long.MaxValue || countWire > int.MaxValue)
            throw new SyncException("BATCH_TRANSACTION_FAILED");
        int epoch = (int)epochWire;
        long cstart = (long)cstartWire;
        long cend = (long)cendWire;
        int count = (int)countWire;
        if (cend < cstart) throw new SyncException("BATCH_TRANSACTION_FAILED");
        var batchId = r.ReadBatchId();
        var prev = r.ReadDigest16();
        var root = r.ReadDigest16();
        var digest = r.ReadDigest16();
        ulong recCountWire = r.Varint();
        if (recCountWire > int.MaxValue || (int)recCountWire != count)
            throw new SyncException("BATCH_TRANSACTION_FAILED");
        int recCount = (int)recCountWire;
        var entries = new List<OutboxEntry>(recCount);
        for (int i = 0; i < recCount; i++)
        {
            // Read fixed-width identities directly into value types; B128() would
            // allocate an intermediate 16-byte array for each identity.
            var recordId = r.ReadRecordId();
            var changeId = r.ReadBatchId();
            if (changeId.IsZero)
                throw new SyncException("BATCH_TRANSACTION_FAILED");
            byte opKind = r.U8();
            var payload = r.BytesMemory();
            entries.Add(new OutboxEntry
            {
                NamespaceId = ns,
                Seq = 0,
                ChangeId = changeId,
                RecordId = recordId,
                OpKind = opKind,
                Payload = payload,
                AuthorNodeId = issuer,
                CreatedAt = 0,
            });
        }
        if (!r.Eof()) throw new SyncException("BATCH_TRANSACTION_FAILED");
        return new Batch(ns, schemaId, schemaVer, policy, flags, issuer, epoch,
                         cstart, cend, batchId, prev, root, digest, entries);
    }
}

/// <summary>Compact immutable record used only while building an outbound transport packet.</summary>
internal readonly struct TransportDeltaEntry
{
    internal RecordId RecordId { get; }
    internal BatchId ChangeId { get; }
    internal byte OpKind { get; }
    internal ReadOnlyMemory<byte> Payload { get; }

    internal TransportDeltaEntry(RecordId recordId, BatchId changeId, byte opKind,
        ReadOnlyMemory<byte> payload)
    {
        RecordId = recordId;
        ChangeId = changeId;
        OpKind = opKind;
        Payload = payload;
    }
}
