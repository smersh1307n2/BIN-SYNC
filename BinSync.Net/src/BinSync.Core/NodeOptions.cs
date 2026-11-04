namespace BinSync;

/// <summary>Construction options for <see cref="Node"/>.</summary>
public sealed class NodeOptions
{
    /// <summary>16-byte node identity; random when null.</summary>
    public byte[]? NodeId { get; init; }

    /// <summary>Session/epoch counter (default 1).</summary>
    public int Epoch { get; init; } = 1;

    /// <summary>Optional wall-clock source (microseconds); tests inject a fixed clock.</summary>
    public Func<long>? NowFn { get; init; }

    /// <summary>Maximum accepted HLC skew in microseconds (default 60s).</summary>
    public long MaxSkewUs { get; init; } = 60_000_000;

    /// <summary>Optional resource ceilings applied from construction.</summary>
    public ResourceLimits? Limits { get; init; }
}
