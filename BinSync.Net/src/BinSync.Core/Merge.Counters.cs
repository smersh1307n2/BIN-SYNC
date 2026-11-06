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
            .OrderBy(x => x.Author, ByteArrayComparer.Instance)
            .ToList();
        for (int i = 1; i < items.Count; i++)
            if (ByteArrayComparer.Instance.Compare(items[i - 1].Author, items[i].Author) == 0)
                throw new ArgumentException("Duplicate canonical COUNTER_G author.", nameof(byAuthor));
        var w = new Writer();
        w.Varint((ulong)items.Count);
        foreach (var (a, v) in items)
            w.B128(a).U64(v);
        return w.Value();
    }

    internal static int ReadMapCount(Reader r, string name)
    {
        ulong count = r.Varint();
        // Each map entry is exactly b128 + u64 (24 bytes). Validate before narrowing/allocation.
        if (count > (ulong)(r.Remaining / 24))
            throw new InvalidDataException($"Invalid {name} entry count.");
        return (int)count;
    }

    /// <summary>Decode from canonical wire bytes.</summary>
    public static Dictionary<byte[], ulong> Decode(byte[] data)
    {
        var r = new Reader(data);
        int n = ReadMapCount(r, "COUNTER_G");
        var map = new Dictionary<byte[], ulong>(n, BytesEq.Instance);
        byte[]? previous = null;
        for (int i = 0; i < n; i++)
        {
            var author = r.B128();
            if (previous is not null && ByteArrayComparer.Instance.Compare(previous, author) >= 0)
                throw new InvalidDataException("COUNTER_G authors must be strictly ordered and unique.");
            map.Add(author, r.U64());
            previous = author;
        }
        if (!r.Eof()) throw new InvalidDataException("Trailing bytes after COUNTER_G payload.");
        return map;
    }

    /// <summary>§8.4: per-author max(local, remote).</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var a = local is null || local.Length == 0
            ? new Dictionary<byte[], ulong>(BytesEq.Instance)
            : Decode(local);
        var b = Decode(remote);
        foreach (var (author, val) in b)
        {
            if (!a.TryGetValue(author, out var cur) || val > cur)
                a[author] = val;
        }
        return Encode(a);
    }

    /// <summary>Sum all counter values encoded in the CRDT payload.</summary>
    public static ulong Total(byte[] data)
    {
        ulong sum = 0;
        foreach (var v in Decode(data).Values) sum = checked(sum + v);
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
            .OrderBy(x => x.Author, ByteArrayComparer.Instance)
            .ToList();
        for (int i = 1; i < items.Count; i++)
            if (ByteArrayComparer.Instance.Compare(items[i - 1].Author, items[i].Author) == 0)
                throw new ArgumentException("Duplicate canonical COUNTER_PN author.", nameof(map));
        w.Varint((ulong)items.Count);
        foreach (var (a, v) in items)
            w.B128(a).U64(v);
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static (Dictionary<byte[], ulong> Pos, Dictionary<byte[], ulong> Neg) Decode(byte[] data)
    {
        var r = new Reader(data);
        var pos = ReadMap(r);
        var neg = ReadMap(r);
        if (!r.Eof()) throw new InvalidDataException("Trailing bytes after COUNTER_PN payload.");
        return (pos, neg);
    }

    static Dictionary<byte[], ulong> ReadMap(Reader r)
    {
        int n = CounterG.ReadMapCount(r, "COUNTER_PN");
        var map = new Dictionary<byte[], ulong>(n, BytesEq.Instance);
        byte[]? previous = null;
        for (int i = 0; i < n; i++)
        {
            var author = r.B128();
            if (previous is not null && ByteArrayComparer.Instance.Compare(previous, author) >= 0)
                throw new InvalidDataException("COUNTER_PN authors must be strictly ordered and unique.");
            map.Add(author, r.U64());
            previous = author;
        }
        return map;
    }

    /// <summary>LWW and CRDT field merge helpers.</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var (lp, ln) = local is null || local.Length == 0
            ? (new Dictionary<byte[], ulong>(BytesEq.Instance), new Dictionary<byte[], ulong>(BytesEq.Instance))
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

    /// <summary>Return a copy of the written buffer as a new byte array.</summary>
    public static long Value(byte[] data)
    {
        var (p, n) = Decode(data);

        // PN components are unsigned and the mathematical result is Σpos − Σneg.
        // Accumulate before narrowing so large positive and negative totals may
        // cancel; only the final signed value must fit Int64.
        Int128 positive = 0;
        Int128 negative = 0;
        foreach (var v in p.Values) positive = checked(positive + (Int128)v);
        foreach (var v in n.Values) negative = checked(negative + (Int128)v);
        return checked((long)(positive - negative));
    }
}

/// <summary>Observed-Remove set (§8.6): adds are (elem, actor, seq); removes are (actor, seq) dots.</summary>
public static class SetOr
{
    /// <summary>Actor/sequence pair identifying a causal counter dot.</summary>
    public sealed class Dot
    {
        /// <summary>Actor node id that issued this counter/seq element.</summary>
        public byte[] Actor = Array.Empty<byte>();
        /// <summary>Monotonic sequence number for the actor.</summary>
        public ulong Seq;
    }

    /// <summary>OR-set/counter add element with actor and sequence.</summary>
    public sealed class Add
    {
        /// <summary>Element payload bytes for the OR-set/counter entry.</summary>
        public byte[] Element = Array.Empty<byte>();
        /// <summary>Actor node id that issued this counter/seq element.</summary>
        public byte[] Actor = Array.Empty<byte>();
        /// <summary>Monotonic sequence number for the actor.</summary>
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
                Element = a.Element ?? Array.Empty<byte>(),
                Actor = Codec.CanonicalB128(a.Actor),
                Seq = a.Seq,
            })
            .OrderBy(a => a.Element, ByteArrayComparer.Instance)
            .ThenBy(a => a.Actor, ByteArrayComparer.Instance)
            .ThenBy(a => a.Seq)
            .ToList();
        if (addSorted.Count > 1)
        {
            var addDots = new HashSet<(byte[], ulong)>(ActorSeqEq.Instance);
            foreach (var add in addSorted)
                if (!addDots.Add((add.Actor, add.Seq)))
                    throw new ArgumentException("SET_OR add dots must be unique.", nameof(adds));
        }

        var remSorted = removes
            .Select(d => new Dot { Actor = Codec.CanonicalB128(d.Actor), Seq = d.Seq })
            .OrderBy(d => d.Actor, ByteArrayComparer.Instance)
            .ThenBy(d => d.Seq)
            .ToList();
        for (int i = 1; i < remSorted.Count; i++)
            if (ByteArrayComparer.Instance.Compare(remSorted[i - 1].Actor, remSorted[i].Actor) == 0 &&
                remSorted[i - 1].Seq == remSorted[i].Seq)
                throw new ArgumentException("SET_OR remove dots must be unique.", nameof(removes));

        var w = new Writer();
        w.Varint((ulong)addSorted.Count);
        foreach (var a in addSorted)
            w.Bytes(a.Element).B128(a.Actor).U64(a.Seq);
        w.Varint((ulong)remSorted.Count);
        foreach (var d in remSorted)
            w.B128(d.Actor).U64(d.Seq);
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static (List<Add> Adds, List<Dot> Removes) Decode(byte[] data)
    {
        var r = new Reader(data);
        ulong addCount = r.Varint();
        // Minimum encoded add: one-byte zero length + actor b128 + sequence u64.
        if (addCount > (ulong)(r.Remaining / 25))
            throw new InvalidDataException("Invalid SET_OR add count.");
        var adds = new List<Add>((int)addCount);
        HashSet<(byte[], ulong)>? addDots = addCount > 1
            ? new HashSet<(byte[], ulong)>(ActorSeqEq.Instance)
            : null;
        Add? previousAdd = null;
        for (int i = 0; i < (int)addCount; i++)
        {
            var add = new Add { Element = r.Bytes(), Actor = r.B128(), Seq = r.U64() };
            if (previousAdd is not null)
            {
                int order = ByteArrayComparer.Instance.Compare(previousAdd.Element, add.Element);
                if (order == 0) order = ByteArrayComparer.Instance.Compare(previousAdd.Actor, add.Actor);
                if (order == 0) order = previousAdd.Seq.CompareTo(add.Seq);
                if (order >= 0)
                    throw new InvalidDataException("SET_OR adds must be strictly ordered and unique.");
            }
            if (addDots is not null && !addDots.Add((add.Actor, add.Seq)))
                throw new InvalidDataException("SET_OR add dots must be unique.");
            adds.Add(add);
            previousAdd = add;
        }

        ulong removeCount = r.Varint();
        if (removeCount > (ulong)(r.Remaining / 24))
            throw new InvalidDataException("Invalid SET_OR remove count.");
        var rems = new List<Dot>((int)removeCount);
        Dot? previousRemove = null;
        for (int i = 0; i < (int)removeCount; i++)
        {
            var remove = new Dot { Actor = r.B128(), Seq = r.U64() };
            if (previousRemove is not null)
            {
                int order = ByteArrayComparer.Instance.Compare(previousRemove.Actor, remove.Actor);
                if (order == 0) order = previousRemove.Seq.CompareTo(remove.Seq);
                if (order >= 0)
                    throw new InvalidDataException("SET_OR remove dots must be strictly ordered and unique.");
            }
            rems.Add(remove);
            previousRemove = remove;
        }
        if (!r.Eof()) throw new InvalidDataException("Trailing bytes after SET_OR payload.");
        return (adds, rems);
    }

    /// <summary>LWW and CRDT field merge helpers.</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        var (la, lr) = local is null || local.Length == 0
            ? (new List<Add>(), new List<Dot>())
            : Decode(local);
        var (ra, rr) = Decode(remote);

        // Union of adds (dedupe by actor+seq+elem) and removes (by actor+seq)
        var addKey = new HashSet<(byte[], ulong, byte[])>(ActorSeqElemEq.Instance);
        var mergedAdds = new List<Add>();
        void AddOne(Add a)
        {
            if (addKey.Add((a.Actor, a.Seq, a.Element))) mergedAdds.Add(a);
        }
        // A dot identifies exactly one logical add. Two replicas presenting the
        // same (actor, seq) for different elements is a protocol conflict, not
        // two independent adds. Reject it as malformed remote state before canonical encoding.
        var dotElements = new Dictionary<(byte[] Actor, ulong Seq), byte[]>(ActorSeqEq.Instance);
        void AddValidated(Add add)
        {
            var dot = (add.Actor, add.Seq);
            if (dotElements.TryGetValue(dot, out var existing))
            {
                if (!existing.AsSpan().SequenceEqual(add.Element))
                    throw new InvalidDataException("SET_OR add dot identifies conflicting elements.");
                return;
            }
            dotElements.Add(dot, add.Element);
            AddOne(add);
        }
        foreach (var a in la) AddValidated(a);
        foreach (var a in ra) AddValidated(a);

        var remKey = new HashSet<(byte[], ulong)>(ActorSeqEq.Instance);
        var mergedRem = new List<Dot>();
        void RemOne(Dot d)
        {
            if (remKey.Add((d.Actor, d.Seq))) mergedRem.Add(d);
        }
        foreach (var d in lr) RemOne(d);
        foreach (var d in rr) RemOne(d);

        return Encode(mergedAdds, mergedRem);
    }

    /// <summary>Elements whose add-dot is not covered by any remove-dot.</summary>
    public static List<byte[]> Visible(byte[] data)
    {
        var (adds, rems) = Decode(data);
        var removed = new HashSet<(byte[], ulong)>(
            rems.Select(d => (d.Actor, d.Seq)), ActorSeqEq.Instance);
        var seen = new HashSet<byte[]>(BytesEq.Instance);
        var result = new List<byte[]>();
        foreach (var a in adds)
        {
            if (removed.Contains((a.Actor, a.Seq))) continue;
            if (seen.Add(a.Element))
                result.Add(a.Element);
        }
        return result;
    }
}
