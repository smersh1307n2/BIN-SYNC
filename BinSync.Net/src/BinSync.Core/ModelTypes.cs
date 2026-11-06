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
    public SyncException(string codeName, byte[]? batchId = null)
        : base(codeName)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId is null ? null : BinSync.BatchId.From(batchId);
    }

    /// <summary>Create with typed error code (message = symbolic name when known).</summary>
    public SyncException(SyncErrorCode errorCode, byte[]? batchId = null)
        : base(SyncErrorCodes.GetName(errorCode) ?? errorCode.ToString())
    {
        ErrorCode = errorCode;
        CodeName = SyncErrorCodes.GetName(errorCode) ?? errorCode.ToString();
        _batchId = batchId is null ? null : BinSync.BatchId.From(batchId);
    }

    /// <summary>Create with code name and typed batch id.</summary>
    public SyncException(string codeName, BatchId batchId)
        : base(codeName)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId;
    }

    /// <summary>Create with a distinct human-readable message.</summary>
    public SyncException(string codeName, string message, byte[]? batchId = null)
        : base(message)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId is null ? null : BinSync.BatchId.From(batchId);
    }

    /// <summary>Create with message and inner exception.</summary>
    public SyncException(string codeName, string message, Exception innerException, byte[]? batchId = null)
        : base(message, innerException)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        ErrorCode = SyncErrorCodes.Parse(CodeName);
        _batchId = batchId is null ? null : BinSync.BatchId.From(batchId);
    }
}

/// <summary>One pending or decoded mutation in the outbox / batch records array.</summary>
public sealed class OutboxEntry
{
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
    public List<BatchId> CausalDeps { get; set; } = new();
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
public sealed partial class PeerState
{
    /// <summary>Remote peer node id this state is tracked for.</summary>
    public NodeId PeerNodeId { get; set; }
    /// <summary>Logical namespace this entity belongs to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Highest sequence the local node has pushed to this peer stream.</summary>
    public long LastPushedSeq { get; set; }
    /// <summary>Gets or sets last received seq.</summary>
    public long LastReceivedSeq { get; set; }
    /// <summary>Highest sequence the peer has ACKed on this stream.</summary>
    public long LastAckedSeq { get; set; }
    /// <summary>Per-author receive cursors: last contiguous seq accepted.</summary>
    public Dictionary<(NodeId Peer, int Epoch, int Ns, NodeId Author), long> ReceivedStreams { get; } = new();
    /// <summary>Per-author ACK watermarks used when building the next delta.</summary>
    public Dictionary<NodeId, long> StreamAcked { get; } = new();
    /// <summary>Highest sequence successfully handed to this peer for each author stream.</summary>
    public Dictionary<NodeId, long> StreamPushed { get; } = new();
    /// <summary>Local Outbox sequence minus this peer's wire cursor for each author stream.</summary>
    public Dictionary<NodeId, long> StreamOffsets { get; } = new();
    /// <summary>Gets or sets initial sync complete.</summary>
    public bool InitialSyncComplete { get; set; }
    /// <summary>Gets or sets membership epoch.</summary>
    public int MembershipEpoch { get; set; }
    /// <summary>Last time this peer or stream was observed.</summary>
    public long LastSeen { get; set; }
    /// <summary>Deterministic batch-id cache keyed by author stream + cursor range.</summary>
    internal Dictionary<(NodeId Author, long Start, long End), (BatchId Id, Digest16 Prev)> Ids { get; } = new();

    /// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
    public PeerState(NodeId peer, int ns)
    {
        PeerNodeId = peer;
        NamespaceId = ns;
    }

    /// <summary>Return the ACK watermark for a given author stream.</summary>
    public long AckedFor(NodeId author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
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
