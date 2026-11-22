namespace BinSync;

/// <summary>Fluent construction for <see cref="Node"/>.</summary>
public sealed class NodeBuilder
{
    private NodeId? _nodeId;
    private int _epoch = 1;
    private Func<long>? _nowFn;
    private long _maxSkewUs = 60_000_000;
    private ResourceLimits? _limits;

    /// <summary>Set the 16-byte node identity.</summary>
    public NodeBuilder WithNodeId(NodeId nodeId)
    {
        _nodeId = nodeId;
        return this;
    }

    /// <summary>Set session epoch (default 1).</summary>
    public NodeBuilder WithEpoch(int epoch)
    {
        _epoch = epoch;
        return this;
    }

    /// <summary>Inject wall-clock (microseconds) — primarily for tests.</summary>
    public NodeBuilder WithClock(Func<long> nowFn)
    {
        _nowFn = nowFn;
        return this;
    }

    /// <summary>Maximum accepted HLC skew in microseconds.</summary>
    public NodeBuilder WithMaxSkewUs(long maxSkewUs)
    {
        _maxSkewUs = maxSkewUs;
        return this;
    }

    /// <summary>Attach resource ceilings at construction.</summary>
    public NodeBuilder WithLimits(ResourceLimits limits)
    {
        _limits = limits;
        return this;
    }

    /// <summary>Build the node.</summary>
    public Node Build() => new(new NodeOptions
    {
        NodeId = _nodeId,
        Epoch = _epoch,
        NowFn = _nowFn,
        MaxSkewUs = _maxSkewUs,
        Limits = _limits,
    });
}
