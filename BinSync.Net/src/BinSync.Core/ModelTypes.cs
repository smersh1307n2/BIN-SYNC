// Core wire/engine DTOs extracted from Node.cs (structure only; behavior unchanged).

namespace BinSync;

public class SyncException : Exception
{
    public string CodeName { get; }
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;
    public byte[]? BatchId { get; }
    public SyncException(string codeName, byte[]? batchId = null) : base(codeName)
    { CodeName = codeName; BatchId = batchId; }
}

public sealed class OutboxEntry
{
    public int NamespaceId;
    public long Seq;               // monotonic per (authorNodeId, namespaceId) [П1]
    public byte[] ChangeId = Array.Empty<byte>();   // b128 idempotency key
    public byte[] RecordId = Array.Empty<byte>();
    public byte OpKind;
    public byte[] Payload = Array.Empty<byte>();    // canonical encoded op (compact mode)
    public byte[] AuthorNodeId = Array.Empty<byte>();
    public long CreatedAt;
    public List<byte[]> CausalDeps = new();
}

public sealed class Batch
{
    public int NamespaceId, SchemaId, SchemaVersion;
    public int MergePolicy, Flags;
    public byte[] IssuerNodeId = Array.Empty<byte>();
    public int IssuerEpoch;
    public long CursorStart, CursorEnd;
    public byte[] BatchId = Node.Zero128, PrevBatchDigest = Node.Zero128;
    public byte[] RecordsRoot = Node.Zero128, BatchDigest = Node.Zero128;
    public List<OutboxEntry> Entries = new();      // decoded ops kept for retransmission/ACK
    /// Errata E-4: durable author of this batch, resolved on the receiver at apply time.
    public byte[]? AuthorStreamNodeId = null;

    public Batch() { }
    public Batch(int ns, int schemaId, int schemaVer, int policy, int flags,
                 byte[] issuer, int issuerEpoch, long cstart, long cend,
                 byte[] batchId, byte[] prev, byte[] root, byte[] digest,
                 List<OutboxEntry> entries)
    {
        NamespaceId = ns; SchemaId = schemaId; SchemaVersion = schemaVer;
        MergePolicy = policy; Flags = flags; IssuerNodeId = issuer; IssuerEpoch = issuerEpoch;
        CursorStart = cstart; CursorEnd = cend; BatchId = batchId; PrevBatchDigest = prev;
        RecordsRoot = root; BatchDigest = digest; Entries = entries;
    }

    public byte[] EncodeRecords() => Node.EncodeRecordsArray(Entries);

    public List<object> Ops()
    {
        var outp = new List<object>();
        foreach (var e in Entries) outp.Add(BinSync.Ops.DecodeOp(e.Payload).Op);
        return outp;
    }

    /// §13.6.1 [Р3]: a batch carries exactly one author — the stream key.
    public byte[] AuthorStream() =>
        Entries.Count > 0 ? Entries[0].AuthorNodeId : Node.Zero128;

    /// Errata E-4: on the RECEIVER entries carry the ISSUER's nodeId as placeholder
    /// author; the durable author is resolved from op.origin at apply time and cached
    /// in AuthorStreamNodeId. On the SENDER side the per-author stream key is authoritative.
    public byte[] DurableAuthor => AuthorStreamNodeId ?? AuthorStream();
}

public sealed partial class PeerState
{
    public byte[] PeerNodeId;
    public int NamespaceId;
    public long LastPushedSeq = 0;
    public long LastReceivedSeq = 0;
    // SPEC CORRECTION (errata E-3): receive cursor keyed per SESSION stream
    // (senderPeer, senderEpoch, ns, authorStream) — see Python port for full rationale.
    public long LastAckedSeq = 0;
    public readonly Dictionary<(byte[], int, int, byte[]), long> ReceivedStreams = new(BytesIntIntBytesEq.Instance);
    // SPEC CORRECTION (errata E-4): push watermark keyed by the AUTHOR STREAM.
    public readonly Dictionary<NodeId, long> StreamAcked = new();
    public bool InitialSyncComplete = false;
    public int MembershipEpoch = 0;
    public long LastSeen = 0;
    // Deterministic batchId cache keyed by (stream, cursorStart, cursorEnd) (§10.2.2 replay).
    internal readonly Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> Ids = new(BytesLongLongEq.Instance);

    public PeerState(byte[] peer, int ns) { PeerNodeId = peer; NamespaceId = ns; }
    public long AckedFor(byte[] author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
}

