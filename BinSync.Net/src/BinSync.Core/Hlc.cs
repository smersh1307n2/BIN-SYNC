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
        if (aAuthor is null && bAuthor is not null) aAuthor = Array.Empty<byte>();
        else if (bAuthor is null && aAuthor is not null) bAuthor = Array.Empty<byte>();
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        int c = BytesCompare(aAuthor!, bAuthor!);
        return c;
    }

    public static int BytesCompare(byte[] x, byte[] y)
    {
        int n = Math.Min(x.Length, y.Length);
        for (int i = 0; i < n; i++)
        {
            if (x[i] != y[i]) return x[i] < y[i] ? -1 : 1;
        }
        if (x.Length == y.Length) return 0;
        return x.Length < y.Length ? -1 : 1;
    }
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

    public HlcClock(Func<long>? nowFn = null, long maxSkewUs = 60_000_000)
    {
        NowFn = nowFn ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000);
        MaxSkewUs = maxSkewUs;
    }

    public long Now() => NowFn!();

    public Hlc SendTick()
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

    public Hlc ReceiveTick(Hlc remote)
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
