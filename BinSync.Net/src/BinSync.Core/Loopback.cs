// In-memory transport (§13.2): frame routing, per-stream flow control, session lifecycle.
namespace BinSync;

/// <summary>Base exception for session/transport setup failures.</summary>
public class SessionException : Exception { }
/// <summary>Thrown when an operation requires a session that is not open.</summary>
public class NoSessionException : Exception { public NoSessionException() : base("no session") { } }
/// <summary>Thrown when a peer is rejected during session setup.</summary>
public class PeerRejectedException : Exception { public PeerRejectedException(string msg) : base(msg) { } }

/// <summary>In-process transport delivering frames between local nodes.</summary>
public class LoopbackTransport
{
    /// <summary>Shared node map keyed by NodeId for in-process delivery.</summary>
    public Dictionary<NodeId, Node>? Nodes = null;
    private readonly Dictionary<(NodeId, NodeId), (FlowControl Send, FlowControl Recv)> _sessions = new();
    private int _streamCounter = 0;
    /// <summary>Per-pair stream sequence counters keyed by (local, remote) node ids.</summary>
    public Dictionary<(NodeId, NodeId), int> Streams = new();

    /// <summary>Register a node in the loopback peer map for local delivery.</summary>
    public void Register(Node node) => (Nodes ??= new())[node.NodeId] = node;

    /// <summary>Wire two nodes for loopback delivery.</summary>
    public void Connect(NodeId a, NodeId b)
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

    /// <summary>Tear down the loopback path between two node ids.</summary>
    public void Close(NodeId a, NodeId b)
    {
        _sessions.Remove((a, b));
        _sessions.Remove((b, a));
        Streams.Remove((a, b));
        Streams.Remove((b, a));
    }

    // BUGFIX (port): the previous signature was a broken stub — frames are captured
    // per-node by SyncRound, so no extraction from raw bytes is needed here.
    /// <summary>Extract delivered frames from a recorded exchange sequence.</summary>
    public static List<(Node From, Frame F)> ExtractFrames(IEnumerable<(Node From, Frame F)> frames)
        => frames.ToList();

    /// <summary>Run a bidirectional loopback exchange of pending frames.</summary>
    public List<(Node From, Frame F)> Exchange(params (Node N, Frame F)[] frames)
    {
        var outp = new List<(Node, Frame)>();
        foreach (var (n, f) in frames)
        {
            if (!Nodes!.TryGetValue(n.NodeId, out var src)) throw new PeerRejectedException("unknown peer");
            NodeId dst;
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

    private NodeId ResolveTarget(Node src, Frame f)
    {
        // harness convention: streamId encodes the destination index among registered peers
        foreach (var ((a, b), sid) in Streams)
            if (a == src.NodeId && sid == f.StreamId && b != src.NodeId)
                return b;
        throw new NoSessionException();
    }

    private NodeId LastStreamAuthorPeer(Node src)
    {
        foreach (var ((a, b), _) in Streams)
            if (a == src.NodeId) return b;
        throw new NoSessionException();
    }

    private void Deliver(Node target, Frame f)
    {
        switch (f.FrameType)
        {
            case 0x09:  // DELTA_BATCH
                var sender = SenderOf(target, f);
                target.ApplyBatchPayload(f.Payload, sender);
                break;
            case 0x0B:  // ACK
                break;
            default:
                break;
        }
    }

    private NodeId SenderOf(Node target, Frame f)
    {
        foreach (var ((a, b), sid) in Streams)
            if (b == target.NodeId && sid == f.StreamId) return a;
        throw new NoSessionException();
    }
}
