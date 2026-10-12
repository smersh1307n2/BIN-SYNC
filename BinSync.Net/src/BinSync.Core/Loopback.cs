// In-memory transport (§13.2): frame routing, per-stream flow control, session lifecycle.
namespace BinSync;

public class SessionException : Exception { }
public class NoSessionException : Exception { public NoSessionException() : base("no session") { } }
public class PeerRejectedException : Exception { }

public class LoopbackTransport
{
    public Dictionary<byte[], Node>? Nodes = null;
    private readonly Dictionary<(byte[], byte[]), (FlowControl Send, FlowControl Recv)> _sessions = new(new TT());
    private int _streamCounter = 0;
    public Dictionary<(byte[], byte[]), int> Streams = new(new TT());

    public void Register(Node node) => (Nodes ??= new())[node.NodeId] = node;

    public void Connect(byte[] a, byte[] b)
    {
        if (Nodes is null || !Nodes.TryGetValue(a, out var na) || !Nodes.TryGetValue(b, out var nb))
            throw new KeyNotFoundException("register both nodes first");
        // BUGFIX: sessions are bidirectional — register the cross-node registry on BOTH ends.
        na.PeersByNid = Nodes;
        nb.PeersByNid = Nodes;
        _sessions[(a, b)] = (new FlowControl(), new FlowControl());
        _sessions[(b, a)] = _sessions[(a, b)];   // shared pair [УП4]
        Streams[(a, b)] = ++_streamCounter;
        Streams[(b, a)] = _streamCounter;
    }

    public void Close(byte[] a, byte[] b)
    {
        _sessions.Remove((a, b));
        _sessions.Remove((b, a));
        Streams.Remove((a, b));
        Streams.Remove((b, a));
    }

    public static List<(Node From, Frame F)> ExtractFrames(List<byte[]> raws)
    {
        var outp = new List<(Node, Frame)>();
        foreach (var (n, f) in raws) outp.Add((n, f));
        return outp;
    }

    public List<(Node From, Frame F)> Exchange(params (Node N, Frame F)[] frames)
    {
        var outp = new List<(Node, Frame)>();
        foreach (var (n, f) in frames)
        {
            if (!Nodes!.TryGetValue(n.NodeId, out var src)) throw new PeerRejectedException("unknown peer");
            byte[] dst;
            if (f.FrameType == 0x0D)
            {
                // ERROR frame: streamId carries the offending peer nodeId prefix lookup
                dst = LastStreamAuthorPeer(src);
            }
            else
            {
                dst = ResolveTarget(src, f);
            }
            var sess = _sessions.TryGetValue((src.NodeId, dst), out var s_) ? s_ : throw new NoSessionException();
            var fcSend = sess.Send;
            if (!fcSend.TryConsume(f.StreamId, f.Payload.Length))
            {
                var errPayload = new Writer().U16(Frames.ErrorCodes["FLOW_CONTROL_VIOLATION"])
                    .Bytes(System.Text.Encoding.UTF8.GetBytes("window exhausted")).Value();
                outp.Add((src, new Frame(0x0D, 0, f.StreamId, 0, errPayload)));
                continue;   // frame NOT delivered
            }
            var target = Nodes![dst];
            try
            {
                Deliver(target, f);
            }
            catch (FrameException e) when (e.SilentDrop)
            {
                continue;   // [Ц2]
            }
            outp.Add((target, f));
            // receiver consumed credit too (symmetric accounting [УП4])
            sess.Recv.TryConsume(f.StreamId, f.Payload.Length);
        }
        return outp;
    }

    private byte[] ResolveTarget(Node src, Frame f)
    {
        // harness convention: streamId encodes the destination index among registered peers
        foreach (var ((a, b), sid) in Streams)
            if (a.AsSpan().SequenceEqual(src.NodeId) && sid == f.StreamId && !b.AsSpan().SequenceEqual(src.NodeId))
                return b;
        throw new NoSessionException();
    }

    private byte[] LastStreamAuthorPeer(Node src)
    {
        foreach (var ((a, b), _) in Streams)
            if (a.AsSpan().SequenceEqual(src.NodeId)) return b;
        throw new NoSessionException();
    }

    private static void Deliver(Node target, Frame f)
    {
        switch (f.FrameType)
        {
            case 0x09:  // DELTA_BATCH
                var batch = target.DecodeBatchPayload(f.Payload);
                var sender = SenderOf(target, f);
                target.ApplyBatch(batch, sender);
                break;
            case 0x0B:  // ACK
                break;
            default:
                break;
        }
    }

    private byte[] SenderOf(Node target, Frame f)
    {
        foreach (var ((a, b), sid) in Streams)
            if (b.AsSpan().SequenceEqual(target.NodeId) && sid == f.StreamId) return a;
        throw new NoSessionException();
    }

    internal sealed class TT : IEqualityComparer<(byte[], byte[])>
    {
        static readonly ByteArrEq B = new();
        public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) => B.Equals(x.Item1, y.Item1) && B.Equals(x.Item2, y.Item2);
        public int GetHashCode((byte[], byte[]) t) => HashCode.Combine(B.GetHashCode(t.Item1), B.GetHashCode(t.Item2));
    }
}
