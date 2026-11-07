// Core wire/engine DTOs — public surface uses properties (1.x/2.0 hygiene).

namespace BinSync;

/// <summary>Protocol/sync failure with a wire error code name (see <see cref="Frames.ErrorCodes"/>).</summary>
public class SyncException : Exception
{
    /// <summary>Symbolic error code (e.g. SEQ_GAP, UNKNOWN_ALIAS).</summary>
    public string CodeName { get; }

    /// <summary>Typed wire error code (preferred over string matching).</summary>
    public SyncErrorCode ErrorCode { get; }

    /// <summary>Numeric wire code when known; otherwise 0.</summary>
    public ushort Code => ErrorCode != SyncErrorCode.Unknown
        ? (ushort)ErrorCode
        : (Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0);

    private readonly BatchId? _batchId;

    /// <summary>Optional typed batch id associated with the error.</summary>
    public BatchId? BatchId => _batchId;

    /// <summary>Create with code name used as the exception message.</summary>
    public SyncException(string codeName, BatchId? batchId = null)
        : base(codeName)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId;
    }

    /// <summary>Create with typed error code (message = symbolic name when known).</summary>
    public SyncException(SyncErrorCode errorCode, BatchId? batchId = null)
        : base(SyncErrorCodes.GetName(errorCode) ?? errorCode.ToString())
    {
        ErrorCode = errorCode;
        CodeName = SyncErrorCodes.GetName(errorCode) ?? errorCode.ToString();
        _batchId = batchId;
    }

    /// <summary>Create with a distinct human-readable message.</summary>
    public SyncException(string codeName, string message, BatchId? batchId = null)
        : base(message)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId;
    }

    /// <summary>Create with message and inner exception.</summary>
    public SyncException(string codeName, string message, Exception innerException, BatchId? batchId = null)
        : base(message, innerException)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId;
    }
}

/// <summary>One pending or decoded mutation in the outbox / batch records array.</summary>
public sealed class OutboxEntry
{
    private List<BatchId>? _causalDeps;
    private bool _causalDepsDeferred;

    /// <summary>Creates an entry with an immediately available dependency list.</summary>
    public OutboxEntry() => _causalDeps = new List<BatchId>();

    // Delta batches normally carry no causal trailer. Defer the empty list allocation
    // until a caller actually accesses CausalDeps, while preserving a distinct,
    // mutable list for every entry.
    internal OutboxEntry(bool deferCausalDeps)
    {
        _causalDepsDeferred = deferCausalDeps;
        if (!deferCausalDeps) _causalDeps = new List<BatchId>();
    }

    /// <summary>Namespace id for this entry.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Monotonic per (authorNodeId, namespaceId) [П1].</summary>
    public long Seq { get; set; }
    /// <summary>Idempotency key (16-byte) for this change.</summary>
    public BatchId ChangeId { get; set; }
    /// <summary>Record id (b128 / 16-byte).</summary>
    public RecordId RecordId { get; set; }
    /// <summary>Op tag (FieldWrite / FieldTomb / RecordTombstone).</summary>
    public byte OpKind { get; set; }
    /// <summary>Canonical encoded op (compact mode). May be a slice of a frame buffer.</summary>
    public ReadOnlyMemory<byte> Payload { get; set; } = Array.Empty<byte>();
    /// <summary>Durable author node id when full-author mode is used.</summary>
    public NodeId AuthorNodeId { get; set; }
    /// <summary>Local creation time (HLC wall) if recorded.</summary>
    public long CreatedAt { get; set; }
    /// <summary>Change ids this entry causally depends on (optional trailer).</summary>
    public List<BatchId> CausalDeps
    {
        get
        {
            if (_causalDepsDeferred)
            {
                _causalDeps = new List<BatchId>();
                _causalDepsDeferred = false;
            }
            return _causalDeps!;
        }
        set
        {
            _causalDeps = value;
            _causalDepsDeferred = false;
        }
    }

    // Avoid materializing deferred empty dependency lists on the fingerprint hot path.
    internal IReadOnlyList<BatchId>? CausalDepsOrNull =>
        _causalDepsDeferred || _causalDeps is null || _causalDeps.Count == 0 ? null : _causalDeps;
    internal bool HasCausalDepsStorage => _causalDepsDeferred || _causalDeps is not null;

    // True only while a public delta batch still references an engine-owned outbox buffer.
    // Cleared as soon as the payload is detached into caller-owned storage.
    internal bool PayloadNeedsDetachment;
}

/// <summary>DELTA_BATCH logical unit: headers, digests, and <see cref="OutboxEntry"/> list.</summary>
public sealed partial class Batch
{
    /// <summary>Logical namespace this entity belongs to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Schema identifier for the namespace.</summary>
    public int SchemaId { get; set; }
    /// <summary>Schema version carried on the batch.</summary>
    public int SchemaVersion { get; set; }
    /// <summary>Merge policy byte applied when resolving concurrent field writes.</summary>
    public int MergePolicy { get; set; }
    /// <summary>Batch flag bits from the wire header.</summary>
    public int Flags { get; set; }
    /// <summary>Node that issued this batch.</summary>
    public NodeId IssuerNodeId { get; set; }
    /// <summary>Epoch of the issuer at batch creation.</summary>
    public int IssuerEpoch { get; set; }
    /// <summary>Inclusive start of the issuer sequence range covered by this batch.</summary>
    public long CursorStart { get; set; }
    /// <summary>Batch cursor end, copied for ACK construction.</summary>
    public long CursorEnd { get; set; }
    /// <summary>128-bit batch identity with value equality.</summary>
    public BatchId BatchId { get; set; }
    /// <summary>Digest of the previous batch in the issuer chain (or zero for the first).</summary>
    public Digest16 PrevBatchDigest { get; set; }
    /// <summary>BLAKE3 root over the ordered records array.</summary>
    public Digest16 RecordsRoot { get; set; }
    /// <summary>BLAKE3 digest binding batch id, cursors, roots, and prev digest.</summary>
    public Digest16 BatchDigest { get; set; }
    /// <summary>Decoded ops kept for retransmission/ACK.</summary>
    public List<OutboxEntry> Entries { get; set; } = new();
    /// <summary>Errata E-4: durable author of this batch, resolved on the receiver at apply time.</summary>
    public NodeId? AuthorStreamNodeId { get; set; }

    /// <summary>DELTA_BATCH logical unit: headers, digests, and list of OutboxEntry records.</summary>
    public Batch() { }

    /// <summary>DELTA_BATCH logical unit: headers, digests, and list of OutboxEntry records.</summary>
    public Batch(int ns, int schemaId, int schemaVer, int policy, int flags,
                 NodeId issuer, int issuerEpoch, long cstart, long cend,
                 BatchId batchId, Digest16 prev, Digest16 root, Digest16 digest,
                 List<OutboxEntry> entries)
    {
        NamespaceId = ns;
        SchemaId = schemaId;
        SchemaVersion = schemaVer;
        MergePolicy = policy;
        Flags = flags;
        IssuerNodeId = issuer;
        IssuerEpoch = issuerEpoch;
        CursorStart = cstart;
        CursorEnd = cend;
        BatchId = batchId;
        PrevBatchDigest = prev;
        RecordsRoot = root;
        BatchDigest = digest;
        Entries = entries;
    }


    /// <summary>Encode records into canonical wire bytes.</summary>
    public byte[] EncodeRecords() => Node.EncodeRecordsArray(Entries);

    /// <summary>Encode and decode FieldWrite / FieldTomb / RecordTombstone operations.</summary>
    public List<object> Ops()
    {
        var outp = new List<object>();
        foreach (var e in Entries)
            outp.Add(BinSync.Ops.DecodeOp(e.Payload).Op);  // ReadOnlyMemory
        return outp;
    }

    /// <summary>§13.6.1 [Р3]: a batch carries exactly one author — the stream key.</summary>
    public byte[] AuthorStream() =>
        Entries.Count > 0 ? Entries[0].AuthorNodeId.ToArray() : Node.Zero128;

    /// <summary>Author stream as NodeId (no alloc when AuthorNodeId is already 16 bytes).</summary>
    public NodeId AuthorStreamId() =>
        Entries.Count > 0 ? Entries[0].AuthorNodeId : default;

    /// <summary>Errata E-4: durable author on receiver; sender uses per-author stream key.</summary>
    public NodeId DurableAuthor => AuthorStreamNodeId ?? AuthorStreamId();

    /// <summary>Wire form of durable author (allocates).</summary>
    public byte[] DurableAuthorBytes => DurableAuthor.ToArray();

    /// <summary>
    /// Copy all entry buffers into fresh arrays so the batch is detached from its source.
    /// Each call creates new arrays, including when the input already spans a full array.
    /// </summary>
    public void MaterializeEntries()
    {
        for (int i = 0; i < Entries.Count; i++)
        {
            var e = Entries[i];
            // ChangeId/RecordId/AuthorNodeId are value types — no materialize
            e.Payload = e.Payload.ToArray();
        }
    }
}

/// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
internal struct PeerAuthorProgress
{
    internal const byte AckedFlag = 1;
    internal const byte PushedFlag = 2;
    internal const byte OffsetFlag = 4;

    internal long Acked;
    internal long Pushed;
    internal long Offset;
    internal byte Flags;

    internal bool HasAcked => (Flags & AckedFlag) != 0;
    internal bool HasPushed => (Flags & PushedFlag) != 0;
    internal bool HasOffset => (Flags & OffsetFlag) != 0;
}

/// <summary>Per-peer session cursors and compact per-author stream progress for one namespace.</summary>
internal sealed partial class PeerState
{
    public NodeId PeerNodeId { get; set; }
    public int NamespaceId { get; set; }
    public long LastPushedSeq { get; set; }
    public long LastReceivedSeq { get; set; }
    public long LastAckedSeq { get; set; }

    /// <summary>Receive cursors keyed by sender/epoch/author; namespace is the owning PeerState key.</summary>
    public Dictionary<(NodeId Peer, int Epoch, NodeId Author), long> ReceivedStreams { get; } = new();
    /// <summary>Combined ACK/pushed/offset watermarks; one author key per stream.</summary>
    internal Dictionary<NodeId, PeerAuthorProgress> AuthorProgress { get; } = new();

    public bool InitialSyncComplete { get; set; }
    public int MembershipEpoch { get; set; }
    public long LastSeen { get; set; }
    internal Dictionary<(NodeId Author, long Start, long End), (BatchId Id, Digest16 Prev)> Ids { get; } = new();

    public PeerState(NodeId peer, int ns)
    {
        PeerNodeId = peer;
        NamespaceId = ns;
    }

    public bool TryGetStreamAcked(NodeId author, out long value)
    {
        if (AuthorProgress.TryGetValue(author, out var p) && p.HasAcked)
        { value = p.Acked; return true; }
        value = 0; return false;
    }

    public bool TryGetStreamPushed(NodeId author, out long value)
    {
        if (AuthorProgress.TryGetValue(author, out var p) && p.HasPushed)
        { value = p.Pushed; return true; }
        value = 0; return false;
    }

    public bool TryGetStreamOffset(NodeId author, out long value)
    {
        if (AuthorProgress.TryGetValue(author, out var p) && p.HasOffset)
        { value = p.Offset; return true; }
        value = 0; return false;
    }

    public bool HasStreamAcked(NodeId author) => TryGetStreamAcked(author, out _);

    public void SetStreamAcked(NodeId author, long value)
    {
        AuthorProgress.TryGetValue(author, out var p);
        p.Acked = value;
        p.Flags |= PeerAuthorProgress.AckedFlag;
        AuthorProgress[author] = p;
    }

    public void SetStreamPushed(NodeId author, long value)
    {
        AuthorProgress.TryGetValue(author, out var p);
        p.Pushed = value;
        p.Flags |= PeerAuthorProgress.PushedFlag;
        AuthorProgress[author] = p;
    }

    public void SetStreamOffset(NodeId author, long value)
    {
        AuthorProgress.TryGetValue(author, out var p);
        p.Offset = value;
        p.Flags |= PeerAuthorProgress.OffsetFlag;
        AuthorProgress[author] = p;
    }

    public long AckedFor(NodeId author) =>
        TryGetStreamAcked(author, out var value) ? value : 0;

    public int StreamAckedCount
    {
        get
        {
            int count = 0;
            foreach (var progress in AuthorProgress.Values)
                if (progress.HasAcked) count++;
            return count;
        }
    }

    public long MinStreamAcked
    {
        get
        {
            long min = long.MaxValue;
            foreach (var progress in AuthorProgress.Values)
                if (progress.HasAcked && progress.Acked < min) min = progress.Acked;
            return min;
        }
    }
}

/// <summary>
/// Outcome of <see cref="Node.ApplyBatch"/> / <see cref="Node.ApplyBatchPayload"/>.
/// Value type so idempotent replay does not allocate on the heap.
/// </summary>
public struct BatchResult
{
    /// <summary>128-bit batch identity with value equality.</summary>
    public BatchId BatchId { get; set; }
    /// <summary>Number of operations newly applied (0 on pure duplicate).</summary>
    public int Applied { get; set; }
    /// <summary>True when the batch should be ACKed to the sender.</summary>
    public bool Acked { get; set; }
    /// <summary>True when the batch was already in the inbox (idempotent replay).</summary>
    public bool Duplicate { get; set; }
    /// <summary>Batch cursor end (for ACK). Set by ApplyBatch.</summary>
    public long CursorEnd { get; set; }
    /// <summary>Namespace of the applied batch (for ACK stream id).</summary>
    public int NamespaceId { get; set; }
    /// <summary>Durable author stream for ACK (Errata E-4).</summary>
    public NodeId DurableAuthor { get; set; }

    /// <summary>Outcome of ApplyBatch / ApplyBatchPayload including ACK metadata.</summary>
    public BatchResult(BatchId batchId) => BatchId = batchId;
}
