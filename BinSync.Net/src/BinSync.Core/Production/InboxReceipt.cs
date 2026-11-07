// Compact, durable idempotency receipt for an accepted DELTA_BATCH.
namespace BinSync;

/// <summary>One accepted-batch receipt; legacy receipts may lack a comparable fingerprint.</summary>
internal readonly struct InboxReceipt
{
    public NodeId DurableAuthor { get; }
    public Digest16 Fingerprint { get; }
    public bool HasFingerprint { get; }
    /// <summary>HLC wall time in Unix microseconds when the batch was accepted.</summary>
    public ulong AppliedAt { get; }
    /// <summary>Unix microsecond expiry; zero means no expiry has been scheduled.</summary>
    public ulong ExpiresAt { get; }

    public InboxReceipt(NodeId durableAuthor, Digest16 fingerprint, bool hasFingerprint,
        ulong appliedAt, ulong expiresAt)
    {
        DurableAuthor = durableAuthor;
        Fingerprint = fingerprint;
        HasFingerprint = hasFingerprint;
        AppliedAt = appliedAt;
        ExpiresAt = expiresAt;
    }
}

/// <summary>Canonical commitment to a DELTA_BATCH receipt, including fields not bound by BatchDigest.</summary>
internal static class InboxReceiptIdentity
{
    private static readonly byte[] Domain =
        System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/INBOX-RECEIPT/v1");

    internal static Digest16 Compute(Batch batch)
    {
        var w = new Writer(128);
        w.Raw(Domain)
            .U16((ushort)batch.NamespaceId)
            .B128(batch.IssuerNodeId)
            .U32((uint)batch.IssuerEpoch)
            .B128(batch.BatchId)
            .U64((ulong)batch.CursorStart)
            .U64((ulong)batch.CursorEnd)
            .B128(batch.PrevBatchDigest)
            .B128(batch.RecordsRoot)
            .B128(batch.BatchDigest)
            .U16((ushort)batch.SchemaId)
            .U16((ushort)batch.SchemaVersion)
            .U8((byte)batch.MergePolicy)
            .U8((byte)batch.Flags);
        return Digests.Blake3Digest16(w.Value());
    }

    /// <summary>Validate the actual records and batch digest before trusting a duplicate receipt.</summary>
    internal static Digest16 ValidateAndCompute(Batch batch)
    {
        var recordsRoot = Node.ComputeRecordsRoot(batch.Entries);
        if (recordsRoot != batch.RecordsRoot)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);

        var batchDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            batch.RecordsRoot, batch.PrevBatchDigest);
        if (batchDigest != batch.BatchDigest)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);

        return Compute(batch);
    }
}
