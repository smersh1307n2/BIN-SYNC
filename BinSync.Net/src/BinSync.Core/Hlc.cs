// HLC (§5): wall µs + logical + origin (wire only). Tick rules, skew clamp, tie-break.
namespace BinSync;

/// <summary>class: class.</summary>
public sealed record class Hlc(ulong Wall, uint Logical, ushort Origin = 0)
{
    /// <summary>Tuple key (wall, logical) for ordering HLC timestamps.</summary>
    public (ulong, uint) Key() => (Wall, Logical);
}

/// <summary>Static helpers for HLC comparison and ordering.</summary>
public static class HlcOps
{
    // §5.4 full order: wall, then logical, then durable authorNodeId (never alias).
    // BUGFIX (implementation finding): unknown (null) author sorts BEFORE every known
    // author — deterministic total order independent of arrival order.
    /// <summary>Total-order compare of two HLC timestamps with optional durable-author tiebreak.</summary>
    public static int CompareHlc(Hlc a, byte[]? aAuthor, Hlc b, byte[]? bAuthor)
    {
        // Unknown (null) author sorts as empty bytes — before every non-empty author.
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        return BytesCompare(aAuthor, bAuthor);
    }

    /// <summary>Total-order compare using NodeId authors (default = unknown/empty).</summary>
    public static int CompareHlc(Hlc a, NodeId aAuthor, Hlc b, NodeId bAuthor)
    {
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        if (aAuthor.Lo != bAuthor.Lo) return aAuthor.Lo < bAuthor.Lo ? -1 : 1;
        if (aAuthor.Hi != bAuthor.Hi) return aAuthor.Hi < bAuthor.Hi ? -1 : 1;
        return 0;
    }

    /// <summary>Total-order compare with optional NodeId authors (null = default/unknown).</summary>
    public static int CompareHlc(Hlc a, NodeId? aAuthor, Hlc b, NodeId? bAuthor) =>
        CompareHlc(a, aAuthor ?? default, b, bAuthor ?? default);

    /// <summary>Lexicographic compare of two byte arrays (null-safe).</summary>
    public static int BytesCompare(byte[]? x, byte[]? y) =>
        x.AsSpan().SequenceCompareTo(y);
}

/// <summary>Thrown when remote HLC violates local max-skew policy.</summary>
public class ClockDriftException : Exception
{
    /// <summary>ClockDriftException for codec/protocol errors.</summary>
    public ClockDriftException(string msg) : base(msg) { }
}

/// <summary>Mutable HLC state with skew protection for send and receive paths.</summary>
public class HlcClock
{
    /// <summary>Maximum logical counter value (uint.MaxValue).</summary>
    public const uint LogicalMax = uint.MaxValue;
    /// <summary>Optional wall-clock source (microseconds); null uses system time.</summary>
    public Func<long>? NowFn;
    /// <summary>Maximum allowed clock skew in microseconds before protection kicks in.</summary>
    public long MaxSkewUs;
    /// <summary>Last observed wall component (cold-starts at 0).</summary>
    public ulong LastWall = 0;   // BUGFIX: never tick at construction (§5.2 cold boot)
    /// <summary>Last observed logical component.</summary>
    public uint LastLogical = 0;
    // Production hardening: the clock is shared mutable state — concurrent writers inside
    // one node (and background clocks) must not interleave read-modify-write on Last*.
    private readonly object _tickGate = new();

    /// <summary>Mutable HLC state with skew protection for send and receive paths.</summary>
    public HlcClock(Func<long>? nowFn = null, long maxSkewUs = 60_000_000)
    {
        NowFn = nowFn ?? UnixTimeMicroseconds;
        MaxSkewUs = maxSkewUs;
    }

    /// <summary>Wall clock in microseconds since Unix epoch (§5). Not milliseconds scaled by 1000.</summary>
    public static long UnixTimeMicroseconds() =>
        (DateTimeOffset.UtcNow.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) / 10;

    /// <summary>Read the configured wall-clock source in microseconds.</summary>
    public long Now() => NowFn!();

    /// <summary>Advance the clock for a local send and return the new timestamp.</summary>
    public Hlc SendTick()
    {
        lock (_tickGate)
        {
            long now = Now();
            ulong wall; uint logical;
            if (now > (long)LastWall) { wall = (ulong)now; logical = 0; }
            else if (now == (long)LastWall) { wall = LastWall; logical = LastLogical + 1; }
            else { wall = LastWall; logical = LastLogical + 1; }  // clock backwards: bump logical
            if (logical >= LogicalMax) throw new InvalidOperationException("Logical clock overflow");
            LastWall = wall; LastLogical = logical;
            return new Hlc(wall, logical);
        }
    }

    /// <summary>Merge a remote timestamp into the local clock and return the result.</summary>
    public Hlc ReceiveTick(Hlc remote)
    {
        lock (_tickGate)
        {
            long now = Now();
            if (remote.Wall > (ulong)(now + MaxSkewUs))
                throw new ClockDriftException($"remote.wall {remote.Wall} beyond now+{MaxSkewUs}");
            ulong wall = Math.Max((ulong)now, Math.Max(LastWall, remote.Wall));
            uint logical;
            if (wall == (ulong)now && wall == LastWall && wall == remote.Wall)
                logical = Math.Max(LastLogical, remote.Logical) + 1;
            else if (wall == LastWall) logical = LastLogical + 1;
            else if (wall == remote.Wall) logical = remote.Logical + 1;
            else logical = 0;
            if (logical >= LogicalMax) throw new InvalidOperationException("Logical clock overflow");
            LastWall = wall; LastLogical = logical;
            return new Hlc(wall, logical);
        }
    }
}
