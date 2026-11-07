// Compact, durable idempotency receipt for an accepted DELTA_BATCH.
namespace BinSync;

/// <summary>One accepted-batch receipt; legacy receipts may lack cursor and header commitments.</summary>
internal readonly struct InboxReceipt
{
    public NodeId DurableAuthor { get; }
    public long CursorEnd { get; }
    public bool HasCursorEnd { get; }
    public Digest16 Fingerprint { get; }
    public bool HasFingerprint { get; }
    /// <summary>HLC wall time in Unix microseconds when the batch was accepted.</summary>
    public ulong AppliedAt { get; }
    /// <summary>Unix microsecond expiry; zero means no expiry has been scheduled.</summary>
    public ulong ExpiresAt { get; }

    public InboxReceipt(NodeId durableAuthor, long cursorEnd, bool hasCursorEnd,
        Digest16 fingerprint, bool hasFingerprint, ulong appliedAt, ulong expiresAt)
    {
        DurableAuthor = durableAuthor;
        CursorEnd = cursorEnd;
        HasCursorEnd = hasCursorEnd;
        Fingerprint = fingerprint;
        HasFingerprint = hasFingerprint;
        AppliedAt = appliedAt;
        ExpiresAt = expiresAt;
    }
}

/// <summary>
/// Canonical commitment to a DELTA_BATCH header. It deliberately does not inspect
/// entry payloads or causal trailers: a duplicate BatchId is an idempotency hit and
/// must not re-apply or trust replay-supplied ChangeIds/dependencies.
/// </summary>
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
            // The causal-trailer-present transport bit is framing, not batch semantics.
            .U8((byte)(batch.Flags & ~CausalWire.FlagHasCausalTrailer));
        return Digests.Blake3Digest16(w.Value());
    }
}
