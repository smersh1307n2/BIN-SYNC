// CRDT-style merge payloads for COUNTER_G / COUNTER_PN / SET_OR (§8.4–8.6).
namespace BinSync;

/// <summary>Per-author G-counter component (grow-only).</summary>
public static class CounterG
{
    /// <summary>
    /// Wire: count varint | (author b128 | value u64 LE)* sorted by author hex.
    /// </summary>
    public static byte[] Encode(IReadOnlyDictionary<byte[], ulong> byAuthor)
    {
        var items = byAuthor
            .Select(kv => (Author: Codec.CanonicalB128(kv.Key), Value: kv.Value))
            .OrderBy(x => Convert.ToHexString(x.Author), StringComparer.Ordinal)
            .ToList();
        var w = new Writer();
        w.Varint((ulong)items.Count);
        foreach (var (a, v) in items)
            w.B128(a).U64(v);
        return w.Value();
    }

    public static Dictionary<byte[], ulong> Decode(byte[] data)
    {
        var r = new Reader(data);
        int n = (int)r.Varint();
        var map = new Dictionary<byte[], ulong>(BA.Instance);
        for (int i = 0; i < n; i++)
        {
            var a = r.B128();
            ulong v = r.U64();
            map[a] = v;
        }
        return map;
    }

    /// <summary>§8.4: per-author max(local, remote).</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var a = local is null || local.Length == 0
            ? new Dictionary<byte[], ulong>(BA.Instance)
            : Decode(local);
        var b = Decode(remote);
        foreach (var (author, val) in b)
        {
            if (!a.TryGetValue(author, out var cur) || val > cur)
                a[author] = val;
        }
        return Encode(a);
    }

    public static ulong Total(byte[] data)
    {
        ulong sum = 0;
        foreach (var v in Decode(data).Values) sum += v;
        return sum;
    }
}

/// <summary>PN-counter: positive and negative G-maps; value = Σpos − Σneg (§8.5).</summary>
public static class CounterPn
{
    /// <summary>
    /// Wire: posCount | (author|u64)* | negCount | (author|u64)*  (each side sorted).
    /// </summary>
    public static byte[] Encode(
        IReadOnlyDictionary<byte[], ulong> pos,
        IReadOnlyDictionary<byte[], ulong> neg)
    {
        var w = new Writer();
        WriteMap(w, pos);
        WriteMap(w, neg);
        return w.Value();
    }

    static void WriteMap(Writer w, IReadOnlyDictionary<byte[], ulong> map)
    {
        var items = map
            .Select(kv => (Author: Codec.CanonicalB128(kv.Key), Value: kv.Value))
            .OrderBy(x => Convert.ToHexString(x.Author), StringComparer.Ordinal)
            .ToList();
        w.Varint((ulong)items.Count);
        foreach (var (a, v) in items)
            w.B128(a).U64(v);
    }

    public static (Dictionary<byte[], ulong> Pos, Dictionary<byte[], ulong> Neg) Decode(byte[] data)
    {
        var r = new Reader(data);
        var pos = ReadMap(r);
        var neg = ReadMap(r);
        return (pos, neg);
    }

    static Dictionary<byte[], ulong> ReadMap(Reader r)
    {
        int n = (int)r.Varint();
        var map = new Dictionary<byte[], ulong>(BA.Instance);
        for (int i = 0; i < n; i++)
            map[r.B128()] = r.U64();
        return map;
    }

    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var (lp, ln) = local is null || local.Length == 0
            ? (new Dictionary<byte[], ulong>(BA.Instance), new Dictionary<byte[], ulong>(BA.Instance))
            : Decode(local);
        var (rp, rn) = Decode(remote);
        MergeMax(lp, rp);
        MergeMax(ln, rn);
        return Encode(lp, ln);
    }

    static void MergeMax(Dictionary<byte[], ulong> a, Dictionary<byte[], ulong> b)
    {
        foreach (var (author, val) in b)
        {
            if (!a.TryGetValue(author, out var cur) || val > cur)
                a[author] = val;
        }
    }

    public static long Value(byte[] data)
    {
        var (p, n) = Decode(data);
        long sum = 0;
        foreach (var v in p.Values) sum += (long)v;
        foreach (var v in n.Values) sum -= (long)v;
        return sum;
    }
}

/// <summary>Observed-Remove set (§8.6): adds are (elem, actor, seq); removes are (actor, seq) dots.</summary>
public static class SetOr
{
    public sealed class Dot
    {
        public byte[] Actor = Array.Empty<byte>();
        public ulong Seq;
    }

    public sealed class Add
    {
        public byte[] Element = Array.Empty<byte>();
        public byte[] Actor = Array.Empty<byte>();
        public ulong Seq;
    }

    /// <summary>
    /// Wire: addCount | (elemLen varint | elem | actor b128 | seq u64)* |
    ///        remCount | (actor b128 | seq u64)*
    /// Adds sorted by (elem hex, actor hex, seq); removes by (actor hex, seq).
    /// </summary>
    public static byte[] Encode(IReadOnlyList<Add> adds, IReadOnlyList<Dot> removes)
    {
        var addSorted = adds
            .Select(a => new Add
            {
                Element = a.Element,
                Actor = Codec.CanonicalB128(a.Actor),
                Seq = a.Seq,
            })
            .OrderBy(a => Convert.ToHexString(a.Element), StringComparer.Ordinal)
            .ThenBy(a => Convert.ToHexString(a.Actor), StringComparer.Ordinal)
            .ThenBy(a => a.Seq)
            .ToList();
        var remSorted = removes
            .Select(d => new Dot { Actor = Codec.CanonicalB128(d.Actor), Seq = d.Seq })
            .OrderBy(d => Convert.ToHexString(d.Actor), StringComparer.Ordinal)
            .ThenBy(d => d.Seq)
            .ToList();

        var w = new Writer();
        w.Varint((ulong)addSorted.Count);
        foreach (var a in addSorted)
            w.Bytes(a.Element).B128(a.Actor).U64(a.Seq);
        w.Varint((ulong)remSorted.Count);
        foreach (var d in remSorted)
            w.B128(d.Actor).U64(d.Seq);
        return w.Value();
    }

    public static (List<Add> Adds, List<Dot> Removes) Decode(byte[] data)
    {
        var r = new Reader(data);
        int na = (int)r.Varint();
        var adds = new List<Add>(na);
        for (int i = 0; i < na; i++)
            adds.Add(new Add { Element = r.Bytes(), Actor = r.B128(), Seq = r.U64() });
        int nr = (int)r.Varint();
        var rems = new List<Dot>(nr);
        for (int i = 0; i < nr; i++)
            rems.Add(new Dot { Actor = r.B128(), Seq = r.U64() });
        return (adds, rems);
    }

    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var (la, lr) = local is null || local.Length == 0
            ? (new List<Add>(), new List<Dot>())
            : Decode(local);
        var (ra, rr) = Decode(remote);

        // Union of adds (dedupe by actor+seq+elem) and removes (by actor+seq)
        var addKey = new HashSet<string>(StringComparer.Ordinal);
        var mergedAdds = new List<Add>();
        void AddOne(Add a)
        {
            var k = Convert.ToHexString(a.Actor) + ":" + a.Seq + ":" + Convert.ToHexString(a.Element);
            if (addKey.Add(k)) mergedAdds.Add(a);
        }
        foreach (var a in la) AddOne(a);
        foreach (var a in ra) AddOne(a);

        var remKey = new HashSet<string>(StringComparer.Ordinal);
        var mergedRem = new List<Dot>();
        void RemOne(Dot d)
        {
            var k = Convert.ToHexString(d.Actor) + ":" + d.Seq;
            if (remKey.Add(k)) mergedRem.Add(d);
        }
        foreach (var d in lr) RemOne(d);
        foreach (var d in rr) RemOne(d);

        return Encode(mergedAdds, mergedRem);
    }

    /// <summary>Elements whose add-dot is not covered by any remove-dot.</summary>
    public static List<byte[]> Visible(byte[] data)
    {
        var (adds, rems) = Decode(data);
        var removed = new HashSet<string>(
            rems.Select(d => Convert.ToHexString(d.Actor) + ":" + d.Seq),
            StringComparer.Ordinal);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<byte[]>();
        foreach (var a in adds)
        {
            var dot = Convert.ToHexString(a.Actor) + ":" + a.Seq;
            if (removed.Contains(dot)) continue;
            var eh = Convert.ToHexString(a.Element);
            if (seen.Add(eh))
                result.Add(a.Element);
        }
        return result;
    }
}
