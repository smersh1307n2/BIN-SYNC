// BinSync.Core — deterministic delta-sync protocol (C# port of the BinSync reference).
// Batch lifecycle invariant: create -> deliver -> checkpoint chain -> ack.
using System.Security.Cryptography;
using System.Text;

namespace BinSync.Core;

public sealed record NodeId(string Value);

public sealed record Rid(long Value)
{
    public override string ToString() => $"Rid({Value})";
}

public static class Digest
{
    // Deterministic, order-sensitive digest over a sequence of records.
    public static string Of(IEnumerable<Rid> rids)
    {
        var joined = string.Join(",", rids.Select(r => r.Value));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(joined));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    public static string Empty => Of(Array.Empty<Rid>());
}

public sealed record Batch(
    long SeqFrom,
    long SeqTo,
    IReadOnlyList<Rid> Records,
    string PrevBatchDigest,
    string BatchDigest);

public sealed class ReplicatedLog
{
    private readonly List<Rid> _entries = new();
    public IReadOnlyList<Rid> Entries => _entries;
    public long AppliedCount => _entries.Count;
    public void Append(Rid rid) => _entries.Add(rid);
    public void Apply(Batch b) { foreach (var r in b.Records) Append(r); }
}

// Issuer-side per-stream state machine.
public sealed class StreamState
{
    public long NextSeq = 1;               // next seq to assign
    public long CheckpointedWatermark = 0; // last chain-checkpointed seq (persisted on disk)
    public long DeliveredWatermark = 0;    // last seq delivered on the wire (not yet checkpointed)
    public long AckedWatermark = 0;        // last seq acknowledged by the receiver
    public long LastAckedSeq = 0;          // alias used by tests
    public string ChainTip = Digest.Empty; // prevBatchDigest for the next batch
    public readonly List<Rid> Pending = new(); // staged records not yet checkpointed
    public readonly HashSet<long> AckedSeqs = new();
}

public sealed class DeltaSyncEngine
{
    private readonly Dictionary<string, StreamState> _streams = new();

    public StreamState StateOf(NodeId peer, string stream)
    {
        var key = $"{peer.Value}|{stream}";
        if (!_streams.TryGetValue(key, out var s)) { s = new StreamState(); _streams[key] = s; }
        return s;
    }

    public void Stage(NodeId peer, string stream, IEnumerable<Rid> rids)
    {
        var st = StateOf(peer, stream);
        foreach (var r in rids)
        {
            st.Pending.Add(r);
            st.NextSeq++;
        }
    }

    // Coalesces everything between the chain-checkpointed watermark and the newest staged seq.
    public Batch CreateDeltaBatch(NodeId peer, string stream, int maxRecords = int.MaxValue)
    {
        var st = StateOf(peer, stream);
        var from = st.CheckpointedWatermark + 1;
        var to = st.NextSeq - 1;
        if (from > to) throw new InvalidOperationException("BATCH_EMPTY");
        var take = Math.Min(maxRecords, to - from + 1);
        var actualTo = from + take - 1;
        var recs = st.Pending.Skip((int)(from - 1)).Take((int)take).ToList();
        var prev = st.ChainTip;
        var digest = Digest.Of(recs);
        return new Batch(from, actualTo, recs, prev, digest);
    }

    public Batch SignBatchFor(Batch b, NodeId peer) => b with { };

    // Receiver-side validation before applying an inbound batch.
    public void ApplyBatch(ReplicatedLog target, Batch b, StreamState issuerState)
    {
        if (b.SeqFrom != target.AppliedCount + 1)
            throw new InvalidOperationException("SEQ_GAP");
        if (b.PrevBatchDigest != issuerState.ChainTip)
            throw new InvalidOperationException("BATCH_CHAIN_BROKEN");
        target.Apply(b);
    }

    // Out-of-order inbound batch: the receiver cannot apply it yet because a
    // predecessor is missing from its log — it buffers the batch and rejects
    // delivery with SEQ_GAP / BATCH_CHAIN_BROKEN (as Transport does on the wire).
    public void DeliverInbound(ReplicatedLog target, Batch b, StreamState issuerState)
    {
        if (b.SeqFrom > target.AppliedCount + 1)
            throw new InvalidOperationException("SEQ_GAP");
        ApplyBatch(target, b, issuerState);
    }

    // Successful wire delivery (as performed by Transport.DeliverBatch): marks the
    // batch delivered but does NOT move the chain tip / checkpoint watermark.
    public void MarkDelivered(StreamState issuerState, Batch b)
    {
        issuerState.DeliveredWatermark = Math.Max(issuerState.DeliveredWatermark, b.SeqTo);
    }

    // Persisting the chain checkpoint advances the watermark and moves the chain tip.
    public void AdvanceStream(StreamState issuerState, Batch b)
    {
        issuerState.CheckpointedWatermark = Math.Max(issuerState.CheckpointedWatermark, b.SeqTo);
        issuerState.ChainTip = b.BatchDigest;
    }

    // Receiver acknowledges a successfully applied batch.
    public void ProcessAck(StreamState issuerState, long ackedSeq)
    {
        issuerState.AckedWatermark = Math.Max(issuerState.AckedWatermark, ackedSeq);
        issuerState.LastAckedSeq = issuerState.AckedWatermark;
        issuerState.AckedSeqs.Add(ackedSeq);
    }

    // Transport-level happy path: create -> deliver -> checkpoint -> ack.
    public Batch DeliverBatch(ReplicatedLog target, NodeId peer, string stream, int maxRecords = int.MaxValue)
    {
        var st = StateOf(peer, stream);
        var b = CreateDeltaBatch(peer, stream, maxRecords);
        ApplyBatch(target, b, st);
        MarkDelivered(st, b);
        AdvanceStream(st, b);
        ProcessAck(st, b.SeqTo);
        return b;
    }
}
