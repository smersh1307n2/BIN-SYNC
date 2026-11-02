// Core wire/engine DTOs — public surface uses properties (1.x/2.0 hygiene).

namespace BinSync;

/// <summary>Protocol/sync failure with a wire error code name (see <see cref="Frames.ErrorCodes"/>).</summary>
public class SyncException : Exception
{
    /// <summary>Symbolic error code (e.g. SEQ_GAP, UNKNOWN_ALIAS).</summary>
    public string CodeName { get; }

    /// <summary>Numeric wire code when known; otherwise 0.</summary>
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;

    /// <summary>Optional batch id (wire b128) associated with the error.</summary>
    public byte[]? BatchId { get; }

    /// <summary>Create with code name used as the exception message.</summary>
    public SyncException(string codeName, byte[]? batchId = null)
        : base(codeName)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        BatchId = batchId;
    }

    /// <summary>Create with a distinct human-readable message.</summary>
    public SyncException(string codeName, string message, byte[]? batchId = null)
        : base(message)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        BatchId = batchId;
    }

    /// <summary>Create with message and inner exception.</summary>
    public SyncException(string codeName, string message, Exception innerException, byte[]? batchId = null)
        : base(message, innerException)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        BatchId = batchId;
    }
}

/// <summary>One pending or decoded mutation in the outbox / batch records array.</summary>
public sealed class OutboxEntry
{
    /// <summary>Namespace id for this entry.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Monotonic per (authorNodeId, namespaceId) [П1].</summary>
    public long Seq { get; set; }
    /// <summary>b128 idempotency key.</summary>
    public ReadOnlyMemory<byte> ChangeId { get; set; } = Array.Empty<byte>();
    /// <summary>Record id (b128 / 16-byte).</summary>
    public ReadOnlyMemory<byte> RecordId { get; set; } = Array.Empty<byte>();
    /// <summary>Op tag (FieldWrite / FieldTomb / RecordTombstone).</summary>
    public byte OpKind { get; set; }
    /// <summary>Canonical encoded op (compact mode). May be a slice of a frame buffer.</summary>
    public ReadOnlyMemory<byte> Payload { get; set; } = Array.Empty<byte>();
    /// <summary>Durable author node id when full-author mode is used.</summary>
    public ReadOnlyMemory<byte> AuthorNodeId { get; set; } = Array.Empty<byte>();
    /// <summary>Local creation time (HLC wall) if recorded.</summary>
    public long CreatedAt { get; set; }
    /// <summary>Gets or sets the causal deps.</summary>
    public List<byte[]> CausalDeps { get; set; } = new();
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
    public byte[] IssuerNodeId { get; set; } = Array.Empty<byte>();
    /// <summary>Epoch of the issuer at batch creation.</summary>
    public int IssuerEpoch { get; set; }
    /// <summary>Inclusive start of the issuer sequence range covered by this batch.</summary>
    public long CursorStart { get; set; }
    /// <summary>Batch cursor end, copied for ACK construction.</summary>
    public long CursorEnd { get; set; }
    /// <summary>128-bit batch identity with value equality.</summary>
    public byte[] BatchId { get; set; } = Node.Zero128;
    /// <summary>Digest of the previous batch in the issuer chain (or zero for the first).</summary>
    public byte[] PrevBatchDigest { get; set; } = Node.Zero128;
    /// <summary>BLAKE3 root over the ordered records array.</summary>
    public byte[] RecordsRoot { get; set; } = Node.Zero128;
    /// <summary>BLAKE3 digest binding batch id, cursors, roots, and prev digest.</summary>
    public byte[] BatchDigest { get; set; } = Node.Zero128;
    /// <summary>Decoded ops kept for retransmission/ACK.</summary>
    public List<OutboxEntry> Entries { get; set; } = new();
    /// <summary>Errata E-4: durable author of this batch, resolved on the receiver at apply time.</summary>
    public byte[]? AuthorStreamNodeId { get; set; }

    /// <summary>DELTA_BATCH logical unit: headers, digests, and list of OutboxEntry records.</summary>
    public Batch() { }

    /// <summary>DELTA_BATCH logical unit: headers, digests, and list of OutboxEntry records.</summary>
    public Batch(int ns, int schemaId, int schemaVer, int policy, int flags,
                 byte[] issuer, int issuerEpoch, long cstart, long cend,
                 byte[] batchId, byte[] prev, byte[] root, byte[] digest,
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

    /// <summary>EncodeRecords: encode records.</summary>
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

    /// <summary>Errata E-4: durable author on receiver; sender uses per-author stream key.</summary>
    public byte[] DurableAuthor => AuthorStreamNodeId ?? AuthorStream();

    /// <summary>
    /// Copy all entry buffers to owned arrays so the batch outlives the decode frame.
    /// Safe to call more than once (already-owned arrays are kept).
    /// </summary>
    public void MaterializeEntries()
    {
        for (int i = 0; i < Entries.Count; i++)
        {
            var e = Entries[i];
            e.ChangeId = Mem.ToArray(e.ChangeId);
            e.RecordId = Mem.ToArray(e.RecordId);
            e.Payload = Mem.ToArray(e.Payload);
            e.AuthorNodeId = Mem.ToArray(e.AuthorNodeId);
        }
    }
}

/// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
public sealed partial class PeerState
{
    /// <summary>Gets or sets the peer node id.</summary>
    public byte[] PeerNodeId { get; set; } = Array.Empty<byte>();
    /// <summary>Logical namespace this entity belongs to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Gets or sets the last pushed seq.</summary>
    public long LastPushedSeq { get; set; }
    /// <summary>Gets or sets the last received seq.</summary>
    public long LastReceivedSeq { get; set; }
    /// <summary>Gets or sets the last acked seq.</summary>
    public long LastAckedSeq { get; set; }
    /// <summary>Gets the received streams.</summary>
    public Dictionary<(byte[], int, int, byte[]), long> ReceivedStreams { get; } =
        new(BytesIntIntBytesEq.Instance);
    /// <summary>Gets the stream acked.</summary>
    public Dictionary<NodeId, long> StreamAcked { get; } = new();
    /// <summary>Gets or sets the initial sync complete.</summary>
    public bool InitialSyncComplete { get; set; }
    /// <summary>Gets or sets the membership epoch.</summary>
    public int MembershipEpoch { get; set; }
    /// <summary>Gets or sets the last seen.</summary>
    public long LastSeen { get; set; }
    internal Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> Ids { get; } =
        new(BytesLongLongEq.Instance);

    /// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
    public PeerState(NodeId peer, int ns)
    {
        PeerNodeId = peer.ToArray();
        NamespaceId = ns;
    }

    /// <summary>Per-peer session cursors and stream ACK watermarks for one namespace.</summary>
    public PeerState(byte[] peer, int ns) : this((NodeId)peer, ns) { }

    /// <summary>AckedFor: acked for.</summary>
    public long AckedFor(byte[] author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
    /// <summary>AckedFor: acked for.</summary>
    public long AckedFor(ReadOnlyMemory<byte> author) => StreamAcked.TryGetValue((NodeId)author, out var v) ? v : 0;
    /// <summary>AckedFor: acked for.</summary>
    public long AckedFor(NodeId author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
}

/// <summary>Outcome of <see cref="Node.ApplyBatch"/> / <see cref="Node.ApplyBatchPayload"/>.</summary>
public sealed class BatchResult
{
    /// <summary>128-bit batch identity with value equality.</summary>
    public byte[] BatchId { get; set; }
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
    public byte[] DurableAuthor { get; set; } = Array.Empty<byte>();

    /// <summary>Outcome of ApplyBatch / ApplyBatchPayload including ACK metadata.</summary>
    public BatchResult(byte[] batchId) => BatchId = batchId;
}
