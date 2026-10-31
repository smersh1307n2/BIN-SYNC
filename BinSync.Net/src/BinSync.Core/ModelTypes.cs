// Core wire/engine DTOs — public surface uses properties (1.x/2.0 hygiene).

namespace BinSync;

public class SyncException : Exception
{
    public string CodeName { get; }
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;
    /// <summary>Optional batch id (wire b128) associated with the error.</summary>
    public byte[]? BatchId { get; }
    public SyncException(string codeName, byte[]? batchId = null) : base(codeName)
    {
        CodeName = codeName;
        BatchId = batchId;
    }
}

public sealed class OutboxEntry
{
    public int NamespaceId { get; set; }
    /// <summary>Monotonic per (authorNodeId, namespaceId) [П1].</summary>
    public long Seq { get; set; }
    /// <summary>b128 idempotency key.</summary>
    public byte[] ChangeId { get; set; } = Array.Empty<byte>();
    public byte[] RecordId { get; set; } = Array.Empty<byte>();
    public byte OpKind { get; set; }
    /// <summary>Canonical encoded op (compact mode).</summary>
    public byte[] Payload { get; set; } = Array.Empty<byte>();
    public byte[] AuthorNodeId { get; set; } = Array.Empty<byte>();
    public long CreatedAt { get; set; }
    public List<byte[]> CausalDeps { get; set; } = new();
}

public sealed class Batch
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
            outp.Add(BinSync.Ops.DecodeOp(e.Payload).Op);
        return outp;
    }

    /// <summary>§13.6.1 [Р3]: a batch carries exactly one author — the stream key.</summary>
    public byte[] AuthorStream() =>
        Entries.Count > 0 ? Entries[0].AuthorNodeId : Node.Zero128;

    /// <summary>Errata E-4: durable author on receiver; sender uses per-author stream key.</summary>
    public byte[] DurableAuthor => AuthorStreamNodeId ?? AuthorStream();
}

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
    public long AckedFor(NodeId author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
}

public sealed class BatchResult
{
    public byte[] BatchId { get; set; }
    public int Applied { get; set; }
    public bool Acked { get; set; }
    public bool Duplicate { get; set; }

    public BatchResult(byte[] batchId) => BatchId = batchId;
}
