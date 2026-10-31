// Record digests, records array codec helpers. Behavior unchanged.

namespace BinSync;

public sealed partial class Node
{
    // ---------------------------------------------------------------------- helpers
/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>RecordDigestOfInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public byte[]? RecordDigestOf(int namespaceId, byte[] recordId)
    {
        lock (_lockStorage) return RecordDigestOfInternal(namespaceId, recordId);
    }

internal byte[]? RecordDigestOfInternal(int namespaceId, byte[] recordId)
    { return Storage.TryGetValue(namespaceId, out var s) && s.TryGetValue(recordId, out var rec) ? Digests.RecordDigest(rec) : null; }

/// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AllRecordDigestsInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Dictionary<byte[], byte[]> AllRecordDigests(int namespaceId)
    {
        lock (_lockStorage) return AllRecordDigestsInternal(namespaceId);
    }

internal Dictionary<byte[], byte[]> AllRecordDigestsInternal(int namespaceId)
    {
        var d = new Dictionary<byte[], byte[]>(BytesEq.Instance);
        if (Storage.TryGetValue(namespaceId, out var s))
            foreach (var (rid, rec) in s) d[rid.ToArray()] = Digests.RecordDigest(rec);
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
    /// recordId b128 | opKind u8 | payloadLen varint | payload.
    /// <summary>Write canonical records array into an existing Writer (no intermediate buffer).</summary>
    public static void WriteRecordsArray(Writer w, List<OutboxEntry> entries)
    {
        w.Varint((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            w.B128(e.RecordId).U8(e.OpKind).Varint((ulong)e.Payload.Length).Raw(e.Payload);
        }
    }

    public static byte[] EncodeRecordsArray(List<OutboxEntry> entries)
    {
        int est = 8;
        for (int i = 0; i < entries.Count; i++)
            est += 24 + entries[i].Payload.Length;
        var w = new Writer(est);
        WriteRecordsArray(w, entries);
        return w.Value();
    }

    public static List<(byte[] Rid, byte Kind, byte[] Payload)> DecodeRecordsArray(byte[] data)
    {
        var r = new Reader(data);
        int n = (int)r.Varint();
        var outp = new List<(byte[], byte, byte[])>();
        for (int i = 0; i < n; i++)
        {
            byte[] rid = r.B128();
            byte kind = r.U8();
            byte[] payload = r.Bytes();
            outp.Add((rid, kind, payload));
        }
        return outp;
    }

    /// §12.17.2 batchDigest = BLAKE3-128(domainTag || batchId || namespaceId || ...)
    public static byte[] ComputeBatchDigest(byte[] batchId, int namespaceId, byte[] issuerNode,
        int issuerEpoch, long cursorStart, long cursorEnd, byte[] recordsRoot, byte[] prevBatchDigest)
    {
        // Fixed layout upper bound: domain + 5×b128 + u16 + u32 + 2×u64 ≈ 160B.
        var w = new Writer(192);
        w.Raw(Domains.DomainBatch).B128(batchId).U16((ushort)namespaceId).B128(issuerNode);
        w.U32((uint)issuerEpoch).U64((ulong)cursorStart).U64((ulong)cursorEnd);
        w.B128(recordsRoot).B128(prevBatchDigest);
        return Digests.Blake3(w.Value(), 16);
    }

    /// recordsRoot = BLAKE3-128 over domainTag || canonical records array (§12.17).
    public static byte[] ComputeRecordsRoot(List<OutboxEntry> entries)
    {
        int est = Domains.DomainRecords.Length + 8;
        for (int i = 0; i < entries.Count; i++)
            est += 24 + entries[i].Payload.Length;
        var w = new Writer(est);
        w.Raw(Domains.DomainRecords);
        WriteRecordsArray(w, entries);
        return Digests.Blake3(w.Value(), 16);
    }
}

