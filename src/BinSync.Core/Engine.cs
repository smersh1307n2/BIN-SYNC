using System.Buffers.Binary;

namespace BinSync.Core;

/// <summary>Lifecycle status of a record (spec §7.4).</summary>
public enum RecordStatus : byte
{
    Live = 0,
    /// <summary>Tombstoned by its author: the authoritative death marker.</summary>
    Dead = 1,
}

/// <summary>A logical entity: every field value is tagged with the author who wrote it last.</summary>
public sealed class FieldValue
{
    public required string Author { get; init; }
    public required long TsUs { get; init; }
    public required byte[] Value { get; init; }

    public FieldValue Clone() => new() { Author = Author, TsUs = TsUs, Value = (byte[])Value.Clone() };
}

/// <summary>An LWW register for one field of one record.</summary>
public sealed class FieldCell
{
    public FieldValue? Current { get; private set; }

    /// <summary>Merge a remote candidate; wins on strictly newer timestamp (ties broken by author id).</summary>
    public bool Merge(FieldValue candidate)
    {
        if (Current is null || candidate.TsUs > Current.TsUs ||
            (candidate.TsUs == Current.TsUs && string.CompareOrdinal(candidate.Author, Current.Author) > 0))
        {
            Current = candidate;
            return true;
        }
        return false;
    }

    public IEnumerable<FieldValue> AllVersions() => Current is null ? Array.Empty<FieldValue>() : new[] { Current };

    public FieldCell Clone() => new() { Current = Current?.Clone() };
}

/// <summary>A synced record: fields + per-author tombstones + lifecycle status (§7.4).</summary>
public sealed class Record
{
    public required string Id { get; init; }
    public RecordStatus Status { get; set; } = RecordStatus.Live;
    /// <summary>Tombstone timestamps per author (multi-author delete evidence).</summary>
    public Dictionary<string, long> Tombstones { get; } = new();
    public Dictionary<string, FieldCell> Fields { get; } = new();

    /// <summary>Max timestamp over all field versions and tombstones (age used by GC).</summary>
    public long MaxAnyTs()
    {
        long m = 0;
        foreach (var f in Fields.Values)
            foreach (var v in f.AllVersions())
                m = Math.Max(m, v.TsUs);
        foreach (var t in Tombstones.Values)
            m = Math.Max(m, t);
        return m;
    }

    /// <summary>A record is fully dead only when its lifecycle status is Dead and every
    /// author that ever touched it left a tombstone (no surviving field evidence).</summary>
    public bool IsFullyDead(IReadOnlyCollection<string> knownAuthors) =>
        Status == RecordStatus.Dead &&
        Tombstones.Count > 0 &&
        knownAuthors.All(a => Tombstones.ContainsKey(a));

    public Record Clone()
    {
        var r = new Record { Id = Id, Status = Status };
        foreach (var (a, t) in Tombstones) r.Tombstones[a] = t;
        foreach (var (k, v) in Fields) r.Fields[k] = v.Clone();
        return r;
    }
}

/// <summary>Hybrid Logical Clock (Kulkarni-style): physical µs + logical counter + node id tie-break.</summary>
public sealed class HlcClock
{
    public long PhysicalUs { get; internal set; }
    public uint Logical { get; internal set; }
    public string NodeId { get; }

    public HlcClock(string nodeId, long startUs = 0)
    {
        NodeId = nodeId;
        PhysicalUs = startUs;
    }

    /// <summary>Generate a new local timestamp (monotonic across calls).</summary>
    public (long ts, uint log, string node) New()
    {
        long wall = StopwatchUs.Now();
        if (wall > PhysicalUs) { PhysicalUs = wall; Logical = 0; }
        else Logical++;
        return (PhysicalUs, Logical, NodeId);
    }

    /// <summary>Observe a remote timestamp and merge it into the clock.</summary>
    public void Observe(long ts, uint log)
    {
        long wall = StopwatchUs.Now();
        long newPhys = Math.Max(Math.Max(wall, PhysicalUs), ts);
        uint newLog;
        if (newPhys == PhysicalUs && newPhys == ts) newLog = Math.Max(Logical, log) + 1;
        else if (newPhys == PhysicalUs) newLog = Logical + 1;
        else if (newPhys == ts) newLog = log + 1;
        else newLog = 0;
        PhysicalUs = newPhys;
        Logical = newLog;
    }

    /// <summary>Compare two HLC stamps: (physical, logical, node-id ordinal).</summary>
    public static int Compare(long tsA, uint logA, string nodeA, long tsB, uint logB, string nodeB)
    {
        int c = tsA.CompareTo(tsB);
        if (c != 0) return c;
        c = logA.CompareTo(logB);
        if (c != 0) return c;
        return string.CompareOrdinal(nodeA, nodeB);
    }
}

internal static class StopwatchUs
{
    private static readonly long Base = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
    private static readonly System.Diagnostics.Stopwatch Sw = Start();
    private static System.Diagnostics.Stopwatch Start() { var s = System.Diagnostics.Stopwatch.StartNew(); return s; }
    public static long Now() => Base + Sw.Elapsed.Ticks / 10; // ticks are 100ns → µs
}

/// <summary>A single field mutation inside a batch.</summary>
public sealed record FieldOp(string RecId, string Field, FieldValue Value);

/// <summary>A single tombstone inside a batch.</summary>
public sealed record TombOp(string RecId, string Author, long TsUs);

/// <summary>Wire-level delta batch (§12.18 engine contract).</summary>
public sealed class DeltaBatch
{
    public string BatchId = "";
    public string Peer = "";
    public long Seq;                       // per-author stream sequence number
    public long DepSeq;                    // highest seq of `Peer` this batch depends on
    public List<FieldOp> Ops = new();
    public List<TombOp> Tombs = new();
    public List<(string RecId, string Alias)> AliasRenames = new();
    public long HlcTs;
    public uint HlcLog;
    public string HlcNode = "";
    public string Digest = "";             // content digest (hex, BLAKE3-256)

    public byte[] CanonicalBytes()
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        WriteStr(w, Peer);
        w.Write(Seq);
        w.Write(DepSeq);
        w.Write(Ops.Count);
        foreach (var o in Ops)
        {
            WriteStr(w, o.RecId); WriteStr(w, o.Field);
            WriteStr(w, o.Value.Author); w.Write(o.Value.TsUs);
            w.Write(o.Value.Value.Length); w.Write(o.Value.Value);
        }
        w.Write(Tombs.Count);
        foreach (var t in Tombs) { WriteStr(w, t.RecId); WriteStr(w, t.Author); w.Write(t.TsUs); }
        w.Write(AliasRenames.Count);
        foreach (var (r, a) in AliasRenames) { WriteStr(w, r); WriteStr(w, a); }
        w.Flush();
        return ms.ToArray();
    }

    public void ComputeDigest() => Digest = Blake3.Hex256(CanonicalBytes());

    internal static void WriteStr(BinaryWriter w, string s)
    {
        var b = System.Text.Encoding.UTF8.GetBytes(s);
        w.Write(b.Length);
        w.Write(b);
    }

    internal static string ReadStr(BinaryReader r)
    {
        int len = r.ReadInt32();
        if (len < 0 || len > r.BaseStream.Length - r.BaseStream.Position)
            throw new InvalidDataException("String length out of bounds.");
        return System.Text.Encoding.UTF8.GetString(r.ReadBytes(len));
    }
}

/// <summary>Wire-level full-state snapshot.</summary>
public sealed class Snapshot
{
    public string FromPeer = "";
    public Dictionary<string, Record> Records = new();
    public Dictionary<string, string> AliasMap = new();
    public Dictionary<string, long> OutboxSeq = new();
    public Dictionary<string, long> InboxCursors = new();
    public long HlcPhysical;
    public uint HlcLogical;
    public string Digest = "";
}

/// <summary>Result of applying an incoming batch.</summary>
public enum ApplyResult { Applied, Duplicate, OutOfOrder }
