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
    public List<byte[]> CausalDeps { get; set; } = new();
}

/// <summary>DELTA_BATCH logical unit: headers, digests, and <see cref="OutboxEntry"/> list.</summary>
public sealed partial class Batch
{
    public int NamespaceId { get; set; }
    public int SchemaId { get; set; }
    public int SchemaVersion { get; set; }
    public int MergePolicy { get; set; }
    public int Flags { get; set; }
    public byte[] IssuerNodeId { get; set; } = Array.Empty<byte>();
    public int IssuerEpoch { get; set; }
    public long CursorStart { get; set; }
    public long CursorEnd { get; set; }
    public byte[] BatchId { get; set; } = Node.Zero128;
    public byte[] PrevBatchDigest { get; set; } = Node.Zero128;
    public byte[] RecordsRoot { get; set; } = Node.Zero128;
    public byte[] BatchDigest { get; set; } = Node.Zero128;
    /// <summary>Decoded ops kept for retransmission/ACK.</summary>
    public List<OutboxEntry> Entries { get; set; } = new();
    /// <summary>Errata E-4: durable author of this batch, resolved on the receiver at apply time.</summary>
    public byte[]? AuthorStreamNodeId { get; set; }

    public Batch() { }

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

    public byte[] EncodeRecords() => Node.EncodeRecordsArray(Entries);

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
    public byte[] PeerNodeId { get; set; } = Array.Empty<byte>();
    public int NamespaceId { get; set; }
    public long LastPushedSeq { get; set; }
    public long LastReceivedSeq { get; set; }
    public long LastAckedSeq { get; set; }
    public Dictionary<(byte[], int, int, byte[]), long> ReceivedStreams { get; } =
        new(BytesIntIntBytesEq.Instance);
    public Dictionary<NodeId, long> StreamAcked { get; } = new();
    public bool InitialSyncComplete { get; set; }
    public int MembershipEpoch { get; set; }
    public long LastSeen { get; set; }
    internal Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> Ids { get; } =
        new(BytesLongLongEq.Instance);

    public PeerState(NodeId peer, int ns)
    {
        PeerNodeId = peer.ToArray();
        NamespaceId = ns;
    }

    public PeerState(byte[] peer, int ns) : this((NodeId)peer, ns) { }

    public long AckedFor(byte[] author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
    public long AckedFor(ReadOnlyMemory<byte> author) => StreamAcked.TryGetValue((NodeId)author, out var v) ? v : 0;
    public long AckedFor(NodeId author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
}

/// <summary>Outcome of <see cref="Node.ApplyBatch"/> / <see cref="Node.ApplyBatchPayload"/>.</summary>
public sealed class BatchResult
{
    public byte[] BatchId { get; set; }
    public int Applied { get; set; }
    public bool Acked { get; set; }
    public bool Duplicate { get; set; }
    /// <summary>Batch cursor end (for ACK). Set by ApplyBatch.</summary>
    public long CursorEnd { get; set; }
    /// <summary>Namespace of the applied batch (for ACK stream id).</summary>
    public int NamespaceId { get; set; }
    /// <summary>Durable author stream for ACK (Errata E-4).</summary>
    public byte[] DurableAuthor { get; set; } = Array.Empty<byte>();

    public BatchResult(byte[] batchId) => BatchId = batchId;
}
