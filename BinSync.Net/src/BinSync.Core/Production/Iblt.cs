// IBLT (§13.2 set-diff under a prefix): peelable table of recordId keys.
// Wire frame type 0x09 (between NEED_KEYS 0x08 and SNAPSHOT 0x0A).
namespace BinSync;

/// <summary>Wire frame type byte for IBLT reconciliation messages.</summary>
public static partial class ReconcileFrames
{
    /// <summary>Frame type constant identifying an IBLT payload.</summary>
    public const byte Iblt = 0x09;
}

/// <summary>One IBLT cell: count + XOR of keys + XOR of key hashes.</summary>
public struct IbltCell
{
    /// <summary>Signed multiplicity of digests folded into this cell.</summary>
    public int Count;
    /// <summary>Running XOR of cell hashes used while peeling the IBLT.</summary>
    public ulong HashXor;
    /// <summary>Low 8 bytes of the XORed key (little-endian).</summary>
    public ulong KeyLo;
    /// <summary>High 8 bytes of the XORed key (little-endian).</summary>
    public ulong KeyHi;

    /// <summary>Write the 16-byte key XOR into <paramref name="dest"/>.</summary>
    public void WriteKey(Span<byte> dest)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest, KeyLo);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), KeyHi);
    }

    /// <summary>Materialize key XOR as a new array (peel / wire).</summary>
    public byte[] KeyToArray()
    {
        var a = new byte[16];
        WriteKey(a);
        return a;
    }

    /// <summary>Load key XOR from a 16-byte span.</summary>
    public void SetKey(ReadOnlySpan<byte> key)
    {
        KeyLo = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(key);
        KeyHi = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(key.Slice(8));
    }

    /// <summary>XOR this cell's key with <paramref name="key"/>.</summary>
    public void XorKey(ReadOnlySpan<byte> key)
    {
        KeyLo ^= System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(key);
        KeyHi ^= System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(key.Slice(8));
    }
}

/// <summary>
/// Invertible Bloom Lookup Table over fixed-width keys (recordId canonical 16 bytes).
/// Insert positive for local-only keys; subtract remote table to recover symmetric difference.
/// </summary>
public sealed class Iblt
{
    /// <summary>Fixed key width in bytes for IBLT cells.</summary>
    public const int KeyBytes = 16;
    /// <summary>Number of hash functions used to place each key into cells.</summary>
    public const int HashCount = 3;

    /// <summary>Number of cells in this IBLT.</summary>
    public int CellCount { get; }
    /// <summary>Backing cell array (count / key XOR / hash XOR).</summary>
    public IbltCell[] Cells { get; }

    /// <summary>Invertible Bloom Lookup Table for set-reconciliation of record ids.</summary>
    public Iblt(int cellCount)
    {
        if (cellCount < 4) throw new ArgumentOutOfRangeException(nameof(cellCount));
        CellCount = cellCount;
        Cells = new IbltCell[cellCount];
    }

    /// <summary>Normalize a key to the fixed IBLT key width (pad or hash as required).</summary>
    public static byte[] NormalizeKey(byte[] key)
    {
        var k = Codec.CanonicalB128(key);
        if (k.Length == KeyBytes) return k;
        var r = new byte[KeyBytes];
        Array.Copy(k, 0, r, 0, Math.Min(KeyBytes, k.Length));
        return r;
    }

    /// <summary>Hash a key into the cell-selection space for IBLT placement.</summary>
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

    /// <summary>Insert a key into the IBLT (+1 count).</summary>
    public void Insert(byte[] key) => Add(NormalizeKey(key), +1);
    /// <summary>Delete a key from the IBLT (−1 count).</summary>
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
            c.XorKey(key);
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
            ref var a = ref Cells[i];
            ref var b = ref remote.Cells[i];
            a.Count -= b.Count;
            a.KeyLo ^= b.KeyLo;
            a.KeyHi ^= b.KeyHi;
            a.HashXor ^= b.HashXor;
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
        // CA2014: stackalloc once outside the peel loop (HashCount is fixed).
        Span<int> idx = stackalloc int[HashCount];
        while (queue.Count > 0 && iter++ < maxIter)
        {
            int i = queue.Dequeue();
            var c = Cells[i];
            if (!IsPure(c)) continue;
            var key = c.KeyToArray();
            int sign = c.Count;
            if (sign > 0) onlyLocal.Add(key);
            else onlyRemote.Add(key);
            Indices(key, idx);
            ulong hh = KeyHash(key);
            for (int ht = 0; ht < HashCount; ht++)
            {
                int j = idx[ht];
                Cells[j].Count -= sign;
                Cells[j].XorKey(key);
                Cells[j].HashXor ^= hh;
                if (IsPure(Cells[j])) queue.Enqueue(j);
            }
        }
        return (onlyLocal, onlyRemote);
    }

    private static bool IsPure(IbltCell c)
    {
        if (c.Count != 1 && c.Count != -1) return false;
        Span<byte> key = stackalloc byte[KeyBytes];
        c.WriteKey(key);
        return KeyHash(key) == c.HashXor;
    }

    /// <summary>Build IBLT from local recordIds under optional prefix.</summary>
    public static Iblt BuildFromNode(Node node, int namespaceId, byte[]? prefix = null, int cellCount = 64)
    {
        var t = new Iblt(cellCount);
        if (!node.Storage.TryGetValue(namespaceId, out var map)) return t;
        foreach (var rid in map.Keys)
        {
            byte[] ridBytes = rid.ToArray();
            if (prefix is { Length: > 0 })
            {
                var key = BucketAccumulator.BucketKey(rid);
                if (prefix.Length > key.Length || !key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                    continue;
            }
            t.Insert(ridBytes);
        }
        return t;
    }

    /// <summary>Encode  into canonical wire bytes.</summary>
    public static byte[] Encode(Iblt t)
    {
        var w = new Writer();
        w.U32((uint)t.CellCount);
        Span<byte> key = stackalloc byte[KeyBytes];
        for (int i = 0; i < t.CellCount; i++)
        {
            w.U32(unchecked((uint)t.Cells[i].Count));
            t.Cells[i].WriteKey(key);
            w.Raw(key);
            w.U64(t.Cells[i].HashXor);
        }
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static Iblt Decode(byte[] payload)
    {
        var r = new Reader(payload);
        int n = (int)r.U32();
        var t = new Iblt(n);
        for (int i = 0; i < n; i++)
        {
            t.Cells[i].Count = unchecked((int)r.U32());
            t.Cells[i].SetKey(r.TakeSpan(KeyBytes));
            t.Cells[i].HashXor = r.U64();
        }
        return t;
    }

}
