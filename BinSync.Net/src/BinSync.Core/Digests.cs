// Digests (§9): record digest, field/lifecycle payloads, bucket accumulators, StateRoot.
// BLAKE3 via the in-tree dependency-free implementation (Production/Blake3.cs —
// production hardening: zero third-party packages); domain tags per [К8].
using System.Numerics;

namespace BinSync;

public static class Domains
{
    public static readonly byte[] DomainRecord = Ascii("BIN-SYNC/RECORD/v1");
    public static readonly byte[] DomainField = Ascii("BIN-SYNC/FIELD/v1");
    public static readonly byte[] DomainBucket = Ascii("BIN-SYNC/BUCKET/v1");
    public static readonly byte[] DomainState = Ascii("BIN-SYNC/STATE/v1");
    public static readonly byte[] DomainBatch = Ascii("BIN-SYNC/BATCH/v1");
    public static readonly byte[] DomainRecords = Ascii("BIN-SYNC/RECORDS/v1");
    public static readonly byte[] DomainAuthId = Ascii("BIN-SYNC/AUTHID/v1");

    private static byte[] Ascii(string s) => System.Text.Encoding.ASCII.GetBytes(s);
    public static byte[] Cat(params byte[][] parts)
    {
        int n = 0;
        for (int i = 0; i < parts.Length; i++) n += parts[i].Length;
        var r = new byte[n];
        int o = 0;
        for (int i = 0; i < parts.Length; i++)
        {
            var p = parts[i];
            p.AsSpan().CopyTo(r.AsSpan(o));
            o += p.Length;
        }
        return r;
    }
}

public static class Digests
{
    /// <summary>BLAKE3-256 truncated to <paramref name="len"/> bytes (in-tree implementation).</summary>
    public static byte[] Blake3(byte[] data, int len = 32) => Blake3((ReadOnlySpan<byte>)data, len);

    /// <summary>BLAKE3-256 truncated to <paramref name="len"/> bytes; zero intermediate byte[] for input.</summary>
    public static byte[] Blake3(ReadOnlySpan<byte> data, int len = 32)
    {
        Span<byte> full = stackalloc byte[32];
        Blake3Core.Hash256(data, full);
        if (len >= 32) return full.ToArray();
        var r = new byte[len];
        full[..len].CopyTo(r);
        return r;
    }

    /// <summary>Domain-tagged BLAKE3-256 (keyed-hash mode), truncated to `len` bytes.</summary>
    public static byte[] TaggedBlake3(string tag, byte[] data, int len = 32)
    {
        var full = Blake3Core.TaggedHash(tag, data);
        if (len >= full.Length) return full;
        var r = new byte[len];
        Array.Copy(full, r, len);
        return r;
    }

    /// §9.1.2 exact formula: fieldId u16 | keyEpoch u16 | wall u64 | logical u32 |
    /// authorNodeId b128 (DURABLE, never origin!) | valLen varint | valueBytes
    public static byte[] FieldDigestPayload(ushort fieldId, ushort keyEpoch, ulong wall,
        uint logical, byte[] authorNodeId, byte[] value)
    {
        var w = new Writer();
        // B128 normalizes only when length != 16 (no double CanonicalB128).
        w.U16(fieldId).U16(keyEpoch).U64(wall).U32(logical).B128(authorNodeId);
        w.Varint((ulong)value.Length).Raw(value);
        return w.Value();
    }

    /// §9.1.3 — fixed layout with fieldId=0, keyEpoch=0, valLen=1, value=[lifecycleValue].
    public static byte[] LifecycleDigestPayload(ulong wall, uint logical, byte[] lifecycleAuthor, byte value)
    {
        Span<byte> one = stackalloc byte[1];
        one[0] = value;
        var w = new Writer(48);
        w.U16(0).U16(0).U64(wall).U32(logical).B128(lifecycleAuthor);
        w.Varint(1).Raw(one);
        return w.Value();
    }

    /// recordDigest = BLAKE3-N(recordId || sorted FieldDigestPayload* || LifecyclePayload) (§9.1).
    public static byte[] RecordDigest(Record rec)
    {
        var parts = new List<byte[]>();
        var allFields = new List<(ushort, FieldState)>(rec.State.Select(kv => (kv.Key, kv.Value)));
        foreach (var t in rec.FieldTombs) allFields.Add((t.Key, t.Value));
        allFields.Sort((x, y) => x.Item1.CompareTo(y.Item1));
        foreach (var (fid, fs) in allFields)
            parts.Add(FieldDigestPayload(fid, fs.KeyEpoch, fs.Timestamp.Wall,
                                         fs.Timestamp.Logical, fs.AuthorNodeId, fs.Value));
        var lc = LifecycleDigestPayload(
            rec.LifecycleHlc?.Wall ?? 0, rec.LifecycleHlc?.Logical ?? 0,
            rec.LifecycleAuthor ?? new byte[16], rec.LifecycleValue);
        int est = Domains.DomainRecord.Length + 16 + lc.Length;
        for (int i = 0; i < parts.Count; i++) est += parts[i].Length;
        var w = new Writer(est);
        w.Raw(Domains.DomainRecord).Raw(rec.RecordId);
        for (int i = 0; i < parts.Count; i++) w.Raw(parts[i]);
        w.Raw(lc);
        return Blake3(w.Value(), 32);
    }
}

/// <summary>
/// §9.2 / A.9: count + xor + sum over record digests keyed by BLAKE3(recordId) prefix.
/// Commutative and incremental [Д3]. Sum/xor are 128-bit LE numbers (UInt128).
/// </summary>
public class BucketAccumulator
{
    public byte[] Prefix;
    public long Count = 0;
    public UInt128 Xor = 0;
    public UInt128 Sum = 0;   // wraps mod 2^128 like Python's masking at encode time

    public BucketAccumulator(byte[]? prefix = null) => Prefix = prefix ?? Array.Empty<byte>();

    public static byte[] BucketKey(byte[] recordId) => Digests.Blake3(recordId, 32);
    public static byte[] BucketKey(RecordId recordId)
    {
        Span<byte> id = stackalloc byte[16];
        recordId.WriteTo(id);
        return Digests.Blake3(id, 32);
    }

    private static UInt128 ToNum(byte[] digest)
    {
        // first 16 bytes as LE integer
        UInt128 v = 0;
        for (int i = 15; i >= 0; i--) v = (v << 8) | digest[i];
        return v;
    }

    public void Add(byte[] digest) { var n = ToNum(digest); Count += 1; Xor ^= n; Sum += n; }
    public void Remove(byte[] digest) { var n = ToNum(digest); Count -= 1; Xor ^= n; Sum -= n; }

    private static byte[] U128Le(UInt128 v)
    {
        var b = new byte[16];
        for (int i = 0; i < 16; i++) { b[i] = (byte)(v & 0xFF); v >>= 8; }
        return b;
    }

    public byte[] CanonicalEncoding()
    {
        var w = new Writer();
        w.Raw(Prefix);
        w.U32(unchecked((uint)Count));
        // Python: negative xor → 0xff*16; sum masked mod 2^128 (UInt128 wraps naturally)
        w.Raw(Count >= 0 || Xor != 0 ? U128Le(Xor) : new byte[16]);
        if (Xor == 0 && Count < 0) w.Raw(new byte[16]);
        w.Raw(U128Le(Sum));
        return w.Value();
    }

    public byte[] Rollup() => Digests.Blake3(Domains.Cat(Domains.DomainBucket, CanonicalEncoding()), 32);
}

public static class StateHash
{
    /// §9.1.4 StateRootDigest = BLAKE3-256(domainTag || nsId || outboxCursor || recordCount || buckets…)
    public static byte[] StateRootDigest(ushort namespaceId, ulong outboxCursor, List<BucketAccumulator> buckets)
    {
        var sorted = buckets.OrderBy(b => Convert.ToHexString(b.CanonicalEncoding())).ToList();
        long total = buckets.Sum(b => b.Count);
        var w = new Writer();
        w.Raw(Domains.DomainState).U16(namespaceId).U64(outboxCursor).Varint((ulong)total);
        foreach (var b in sorted) w.Bytes(b.CanonicalEncoding());
        return Digests.Blake3(w.Value(), 32);
    }

    public static List<BucketAccumulator> BuildBuckets(Dictionary<RecordId, Record> records, int radixBits = 8)
    {
        var buckets = new Dictionary<string, BucketAccumulator>();
        foreach (var (rid, rec) in records)
        {
            var key = BucketAccumulator.BucketKey(rid);
            var prefix = key[..Math.Max(1, radixBits / 8)];
            var pk = Convert.ToHexString(prefix);
            if (!buckets.TryGetValue(pk, out var acc))
                buckets[pk] = acc = new BucketAccumulator(prefix);
            acc.Add(Digests.RecordDigest(rec));
        }
        return buckets.Values.ToList();
    }
}
