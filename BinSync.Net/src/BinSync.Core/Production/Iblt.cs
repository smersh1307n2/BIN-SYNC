// IBLT (§13.2 set-diff under a prefix): peelable table of recordId keys.
// Wire frame type 0x09 (between NEED_KEYS 0x08 and SNAPSHOT 0x0A).
namespace BinSync;

public static partial class ReconcileFrames
{
    public const byte Iblt = 0x09;
}

/// <summary>One IBLT cell: count + XOR of keys + XOR of key hashes.</summary>
public struct IbltCell
{
    public int Count;
    public byte[] KeyXor;   // length KeyBytes (padded)
    public ulong HashXor;
}

/// <summary>
/// Invertible Bloom Lookup Table over fixed-width keys (recordId canonical 16 bytes).
/// Insert positive for local-only keys; subtract remote table to recover symmetric difference.
/// </summary>
public sealed class Iblt
{
    public const int KeyBytes = 16;
    public const int HashCount = 3;

    public int CellCount { get; }
    public IbltCell[] Cells { get; }

    public Iblt(int cellCount)
    {
        if (cellCount < 4) throw new ArgumentOutOfRangeException(nameof(cellCount));
        CellCount = cellCount;
        Cells = new IbltCell[cellCount];
        for (int i = 0; i < cellCount; i++)
            Cells[i] = new IbltCell { KeyXor = new byte[KeyBytes], HashXor = 0, Count = 0 };
    }

    public static byte[] NormalizeKey(byte[] key)
    {
        var k = Codec.CanonicalB128(key);
        if (k.Length == KeyBytes) return k;
        var r = new byte[KeyBytes];
        Array.Copy(k, 0, r, 0, Math.Min(KeyBytes, k.Length));
        return r;
    }

    public static ulong KeyHash(ReadOnlySpan<byte> key)
    {
        ulong h = 14695981039346656037UL;
        for (int i = 0; i < key.Length; i++)
        {
            h ^= key[i];
            h *= 1099511628211UL;
        }
        return h;
    }

    private void Indices(ReadOnlySpan<byte> key, Span<int> idx)
    {
        ulong h0 = KeyHash(key);
        ulong h1 = h0 * 0x9E3779B97F4A7C15UL + 1;
        ulong h2 = h1 * 0x9E3779B97F4A7C15UL + 1;
        idx[0] = (int)(h0 % (uint)CellCount);
        idx[1] = (int)(h1 % (uint)CellCount);
        idx[2] = (int)(h2 % (uint)CellCount);
        if (idx[1] == idx[0]) idx[1] = (idx[1] + 1) % CellCount;
        if (idx[2] == idx[0] || idx[2] == idx[1]) idx[2] = (idx[2] + 1) % CellCount;
        if (idx[2] == idx[0] || idx[2] == idx[1]) idx[2] = (idx[2] + 2) % CellCount;
    }

    public void Insert(byte[] key) => Add(NormalizeKey(key), +1);
    public void Delete(byte[] key) => Add(NormalizeKey(key), -1);

    private void Add(byte[] key, int delta)
    {
        Span<int> idx = stackalloc int[HashCount];
        Indices(key, idx);
        ulong hh = KeyHash(key);
        for (int t = 0; t < HashCount; t++)
        {
            ref var c = ref Cells[idx[t]];
            c.Count += delta;
            XorInPlace(c.KeyXor, key);
            c.HashXor ^= hh;
        }
    }

    /// <summary>Subtract remote cells from this table (in place): local − remote.</summary>
    public void Subtract(Iblt remote)
    {
        if (remote.CellCount != CellCount)
            throw new ArgumentException("IBLT size mismatch");
        for (int i = 0; i < CellCount; i++)
        {
            Cells[i].Count -= remote.Cells[i].Count;
            XorInPlace(Cells[i].KeyXor, remote.Cells[i].KeyXor);
            Cells[i].HashXor ^= remote.Cells[i].HashXor;
        }
    }

    /// <summary>
    /// Peel pure cells. Returns (onlyInThis, onlyInRemote).
    /// </summary>
    public (List<byte[]> OnlyLocal, List<byte[]> OnlyRemote) Peel(int maxIter = 10_000)
    {
        var onlyLocal = new List<byte[]>();
        var onlyRemote = new List<byte[]>();
        var queue = new Queue<int>();
        for (int i = 0; i < CellCount; i++)
            if (IsPure(Cells[i])) queue.Enqueue(i);

        int iter = 0;
        while (queue.Count > 0 && iter++ < maxIter)
        {
            int i = queue.Dequeue();
            var c = Cells[i];
            if (!IsPure(c)) continue;
            var key = (byte[])c.KeyXor.Clone();
            int sign = c.Count;
            if (sign > 0) onlyLocal.Add(key);
            else onlyRemote.Add(key);
            Span<int> idx = stackalloc int[HashCount];
            Indices(key, idx);
            ulong hh = KeyHash(key);
            for (int t = 0; t < HashCount; t++)
            {
                int j = idx[t];
                Cells[j].Count -= sign;
                XorInPlace(Cells[j].KeyXor, key);
                Cells[j].HashXor ^= hh;
                if (IsPure(Cells[j])) queue.Enqueue(j);
            }
        }
        return (onlyLocal, onlyRemote);
    }

    private static bool IsPure(IbltCell c)
    {
        if (c.Count != 1 && c.Count != -1) return false;
        return KeyHash(c.KeyXor) == c.HashXor;
    }

    private static void XorInPlace(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++) a[i] ^= b[i];
    }

    /// <summary>Build IBLT from local recordIds under optional prefix.</summary>
    public static Iblt BuildFromNode(Node node, int namespaceId, byte[]? prefix = null, int cellCount = 64)
    {
        var t = new Iblt(cellCount);
        if (!node.Storage.TryGetValue(namespaceId, out var map)) return t;
        foreach (var rid in map.Keys)
        {
            if (prefix is { Length: > 0 })
            {
                var key = BucketAccumulator.BucketKey(rid);
                if (prefix.Length > key.Length || !key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                    continue;
            }
            t.Insert(rid);
        }
        return t;
    }

    public static byte[] Encode(Iblt t)
    {
        var w = new Writer();
        w.U32((uint)t.CellCount);
        for (int i = 0; i < t.CellCount; i++)
        {
            w.U32(unchecked((uint)t.Cells[i].Count));
            w.Raw(t.Cells[i].KeyXor.Length == KeyBytes ? t.Cells[i].KeyXor : PadKey(t.Cells[i].KeyXor));
            w.U64(t.Cells[i].HashXor);
        }
        return w.Value();
    }

    public static Iblt Decode(byte[] payload)
    {
        var r = new Reader(payload);
        int n = (int)r.U32();
        var t = new Iblt(n);
        for (int i = 0; i < n; i++)
        {
            t.Cells[i].Count = unchecked((int)r.U32());
            t.Cells[i].KeyXor = r.Raw(KeyBytes);
            t.Cells[i].HashXor = r.U64();
        }
        return t;
    }

    private static byte[] PadKey(byte[] k)
    {
        var r = new byte[KeyBytes];
        Array.Copy(k, r, Math.Min(KeyBytes, k.Length));
        return r;
    }
}
