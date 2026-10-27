// HLC (§5): wall µs + logical + origin (wire only). Tick rules, skew clamp, tie-break.
namespace BinSync;

public sealed record class Hlc(ulong Wall, uint Logical, ushort Origin = 0)
{
    public (ulong, uint) Key() => (Wall, Logical);
}

public static class HlcOps
{
    // §5.4 full order: wall, then logical, then durable authorNodeId (never alias).
    // BUGFIX (implementation finding): unknown (null) author sorts BEFORE every known
    // author — deterministic total order independent of arrival order.
    public static int CompareHlc(Hlc a, byte[]? aAuthor, Hlc b, byte[]? bAuthor)
    {
        // Unknown (null) author sorts as empty bytes — before every non-empty author,
        // and equal to another unknown. Total order does not depend on arrival.
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        return BytesCompare(aAuthor, bAuthor);
    }

    public static int BytesCompare(byte[]? x, byte[]? y) =>
        x.AsSpan().SequenceCompareTo(y);
}

public class ClockDriftException : Exception
{
    public ClockDriftException(string msg) : base(msg) { }
}

public class HlcClock
{
    public const uint LogicalMax = uint.MaxValue;
    public Func<long>? NowFn;
    public long MaxSkewUs;
    public ulong LastWall = 0;   // BUGFIX: never tick at construction (§5.2 cold boot)
    public uint LastLogical = 0;
    // Production hardening: the clock is shared mutable state — concurrent writers inside
    // one node (and background clocks) must not interleave read-modify-write on Last*.
    private readonly object _tickGate = new();

    public HlcClock(Func<long>? nowFn = null, long maxSkewUs = 60_000_000)
    {
        NowFn = nowFn ?? UnixTimeMicroseconds;
        MaxSkewUs = maxSkewUs;
    }

    /// <summary>Wall clock in microseconds since Unix epoch (§5). Not milliseconds scaled by 1000.</summary>
    public static long UnixTimeMicroseconds() =>
        (DateTimeOffset.UtcNow.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) / 10;

    public long Now() => NowFn!();

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
