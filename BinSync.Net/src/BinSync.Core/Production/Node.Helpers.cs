// Record digests, records array codec helpers. Behavior unchanged.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    // ---------------------------------------------------------------------- helpers
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>RecordDigestOfInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public byte[]? RecordDigestOf(int namespaceId, RecordId recordId)
    {
        lock (_lockStorage) return RecordDigestOfInternal(namespaceId, recordId);
    }

    internal byte[]? RecordDigestOfInternal(int namespaceId, RecordId recordId)
    { return Storage.TryGetValue(namespaceId, out var s) && s.TryGetValue(recordId, out var rec) ? Digests.RecordDigest(rec) : null; }

    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AllRecordDigestsInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Dictionary<RecordId, byte[]> AllRecordDigests(int namespaceId)
    {
        lock (_lockStorage) return AllRecordDigestsInternal(namespaceId);
    }

    internal Dictionary<RecordId, byte[]> AllRecordDigestsInternal(int namespaceId)
    {
        var d = new Dictionary<RecordId, byte[]>();
        if (Storage.TryGetValue(namespaceId, out var s))
            foreach (var (rid, rec) in s) d[rid] = Digests.RecordDigest(rec);
        return d;
    }

    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>OutboxSizeInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int OutboxSize(int namespaceId)
    {
        lock (_lockOutbox) return OutboxSizeInternal(namespaceId);
    }

    internal int OutboxSizeInternal(int namespaceId)
    { return Outbox.TryGetValue(namespaceId, out var l) ? l.Count : 0; }

    internal static Batch CloneBatchForPublicRead(Batch batch)
    {
        var copy = new Batch(
            batch.NamespaceId,
            batch.SchemaId,
            batch.SchemaVersion,
            batch.MergePolicy,
            batch.Flags,
            batch.IssuerNodeId,
            batch.IssuerEpoch,
            batch.CursorStart,
            batch.CursorEnd,
            batch.BatchId,
            batch.PrevBatchDigest,
            batch.RecordsRoot,
            batch.BatchDigest,
            batch.Entries.Select(CloneOutboxEntryForPublicRead).ToList())
        {
            AuthorStreamNodeId = batch.AuthorStreamNodeId,
        };
        return copy;
    }

    internal static OutboxEntry CloneOutboxEntryForPublicRead(OutboxEntry entry) => new()
    {
        NamespaceId = entry.NamespaceId,
        Seq = entry.Seq,
        ChangeId = entry.ChangeId,
        RecordId = entry.RecordId,
        OpKind = entry.OpKind,
        Payload = entry.Payload.ToArray(),
        AuthorNodeId = entry.AuthorNodeId,
        CreatedAt = entry.CreatedAt,
        CausalDeps = new List<BatchId>(entry.CausalDeps),
    };

    // Public read APIs return detached object graphs. Record/FieldState are mutable
    // DTOs and FieldState.Value is a byte[]; shallow copies would let callers mutate
    // live storage without the Node gates, WAL, or Outbox.
    internal static FieldState CloneFieldForPublicRead(FieldState field) =>
        field with { Value = field.Value.ToArray() };

    internal static Record CloneForPublicRead(Record rec)
    {
        var copy = Clone(rec);
        foreach (var key in copy.State.Keys.ToArray())
            copy.State[key] = CloneFieldForPublicRead(copy.State[key]);
        if (copy.HasFieldTombs)
            foreach (var key in copy.FieldTombs.Keys.ToArray())
                copy.FieldTombs[key] = CloneFieldForPublicRead(copy.FieldTombs[key]);
        return copy;
    }

    internal static Record Clone(Record rec)
    {
        // Shallow-copy field maps; do NOT touch FieldTombs getter (it would allocate empty).
        var r = new Record(rec.RecordId, rec.NamespaceId, fieldCapacity: Math.Max(1, rec.State.Count))
        {
            State = new Dictionary<ushort, FieldState>(rec.State),
            LifecycleHlc = rec.LifecycleHlc,
            LifecycleAuthor = rec.LifecycleAuthor,
            LifecycleValue = rec.LifecycleValue,
        };
        if (rec.HasFieldTombs)
            r.FieldTombs = new Dictionary<ushort, FieldState>(rec.FieldTombs);
        return r;
    }

    /// Canonical records array: count varint, then per entry:
    /// recordId b128 | changeId b128 | opKind u8 | payloadLen varint | payload.
    /// <summary>Write canonical records array into an existing Writer (no intermediate buffer).</summary>
    public static void WriteRecordsArray(Writer w, List<OutboxEntry> entries)
    {
        w.Varint((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            if (e.ChangeId.IsZero)
                throw new SyncException("BATCH_TRANSACTION_FAILED");
            w.B128(e.RecordId).B128(e.ChangeId).U8(e.OpKind).Varint((ulong)e.Payload.Length).Raw(e.Payload.Span);
        }
    }

    /// <summary>Encode records array into canonical wire bytes.</summary>
    public static byte[] EncodeRecordsArray(List<OutboxEntry> entries)
    {
        int est = 8;
        for (int i = 0; i < entries.Count; i++)
            est += 40 + entries[i].Payload.Length;
        var dest = new byte[Math.Max(est, 16)];
        var w = new Writer(dest);
        WriteRecordsArray(w, entries);
        if (w.Length == dest.Length) return dest;
        return dest.AsSpan(0, w.Length).ToArray();
    }

    /// <summary>Decode records array from canonical wire bytes.</summary>
    public static List<(byte[] Rid, byte[] ChangeId, byte Kind, byte[] Payload)> DecodeRecordsArray(byte[] data)
    {
        var r = new Reader(data);
        int n = (int)r.Varint();
        var outp = new List<(byte[], byte[], byte, byte[])>();
        for (int i = 0; i < n; i++)
        {
            byte[] rid = r.B128();
            byte[] changeId = r.B128();
            if (changeId.AsSpan().IndexOfAnyExcept((byte)0) < 0)
                throw new SyncException("BATCH_TRANSACTION_FAILED");
            byte kind = r.U8();
            byte[] payload = r.Bytes();
            outp.Add((rid, changeId, kind, payload));
        }
        return outp;
    }

    /// <summary>BLAKE3 digest binding batch id, cursors, roots, and prev digest.</summary>
    public static Digest16 ComputeBatchDigest(BatchId batchId, int namespaceId, NodeId issuerNode,
        int issuerEpoch, long cursorStart, long cursorEnd, Digest16 recordsRoot, Digest16 prevBatchDigest)
    {
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(192);
        try
        {
            var w = new Writer(buf);
            w.Raw(Domains.DomainBatchBytes).B128(batchId).U16((ushort)namespaceId).B128(issuerNode);
            w.U32((uint)issuerEpoch).U64((ulong)cursorStart).U64((ulong)cursorEnd);
            w.B128(recordsRoot).B128(prevBatchDigest);
            return Digests.Blake3Digest16(w.WrittenSpan);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <summary>BLAKE3 root over the ordered records array (incremental).</summary>
    public static Digest16 ComputeRecordsRoot(List<OutboxEntry> entries)
    {
        int est = Domains.DomainRecordsBytes.Length + Codec.VarintLength((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
            est += 33 + Codec.VarintLength((ulong)entries[i].Payload.Length) + entries[i].Payload.Length;
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(buf);
            w.Raw(Domains.DomainRecordsBytes);
            WriteRecordsArray(w, entries);
            return Digests.Blake3Digest16(w.WrittenSpan);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }
}

