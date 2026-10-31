// Batch wire encode/decode (payload path). Partial of Node — behavior unchanged.

namespace BinSync;

public sealed partial class Node
{

    /// DELTA_BATCH wire layout (§12.17).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>EncodeBatchPayloadInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public byte[] EncodeBatchPayload(Batch batch)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return EncodeBatchPayloadInternal(batch);
    }

    internal byte[] EncodeBatchPayloadInternal(Batch batch)
    {
        // Single-pass encode: no intermediate records[] buffer.
        // Capacity ≈ fixed header (~100B) + per-entry (16+1+varint+payload).
        int est = 128;
        var entries = batch.Entries;
        for (int i = 0; i < entries.Count; i++)
            est += 24 + entries[i].Payload.Length;

        var w = new Writer(est);
        w.U16((ushort)batch.NamespaceId).U16((ushort)batch.SchemaId).U16((ushort)batch.SchemaVersion);
        w.U8((byte)batch.MergePolicy).U8((byte)batch.Flags);
        w.B128(batch.IssuerNodeId).U32((uint)batch.IssuerEpoch);
        w.U64((ulong)batch.CursorStart).U64((ulong)batch.CursorEnd);
        w.Varint((ulong)entries.Count);
        w.B128(batch.BatchId).B128(batch.PrevBatchDigest);
        w.B128(batch.RecordsRoot).B128(batch.BatchDigest);
        WriteRecordsArray(w, entries);
        return w.Value();
    }

    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>DecodeBatchPayloadInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public Batch DecodeBatchPayload(byte[] data)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return DecodeBatchPayloadInternal(data);
    }

    internal Batch DecodeBatchPayloadInternal(byte[] data)
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
        byte[] recordsData = r.Raw(data.Length - r.Pos);
        var rawEntries = DecodeRecordsArray(recordsData);
        if (rawEntries.Count != count) throw new SyncException("BATCH_TRANSACTION_FAILED");
        var entries = new List<OutboxEntry>();
        foreach (var (rid, kind, payload) in rawEntries)
            entries.Add(new OutboxEntry
            {
                NamespaceId = ns,
                Seq = 0,
                ChangeId = Zero128,
                RecordId = rid,
                OpKind = kind,
                Payload = payload,
                AuthorNodeId = issuer,
                CreatedAt = 0,
            });
        return new Batch(ns, schemaId, schemaVer, policy, flags, issuer, epoch,
                         cstart, cend, batchId, prev, root, digest, entries);
    }

}
