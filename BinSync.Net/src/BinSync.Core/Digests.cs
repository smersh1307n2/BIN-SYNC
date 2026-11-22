// Digests (§9): record digest, field/lifecycle payloads, bucket accumulators, StateRoot.
// BLAKE3 via the in-tree dependency-free implementation (Production/Blake3.cs —
// production hardening: zero third-party packages); domain tags per [К8].
using System.Numerics;

namespace BinSync;

/// <summary>BLAKE3 domain-separation labels for digests and roots.</summary>
public static class Domains
{
    /// <summary>BLAKE3 domain separation label for per-record digests.</summary>
    private static readonly byte[] DomainRecordStorage = Ascii("BIN-SYNC/RECORD/v2");
    private static readonly byte[] DomainFieldStorage = Ascii("BIN-SYNC/FIELD/v1");
    private static readonly byte[] DomainBucketStorage = Ascii("BIN-SYNC/BUCKET/v1");
    private static readonly byte[] DomainStateStorage = Ascii("BIN-SYNC/STATE/v2");
    private static readonly byte[] DomainBatchStorage = Ascii("BIN-SYNC/BATCH/v1");
    private static readonly byte[] DomainRecordsStorage = Ascii("BIN-SYNC/RECORDS/v1");
    private static readonly byte[] DomainAuthIdStorage = Ascii("BIN-SYNC/AUTHID/v1");

    /// <summary>BLAKE3 domain separation label for per-record digests. Returns an owned copy.</summary>
    public static byte[] DomainRecord => DomainRecordStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for per-field digests. Returns an owned copy.</summary>
    public static byte[] DomainField => DomainFieldStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for bucket rollups. Returns an owned copy.</summary>
    public static byte[] DomainBucket => DomainBucketStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for state digests. Returns an owned copy.</summary>
    public static byte[] DomainState => DomainStateStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for batch digests. Returns an owned copy.</summary>
    public static byte[] DomainBatch => DomainBatchStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for the records-root hash. Returns an owned copy.</summary>
    public static byte[] DomainRecords => DomainRecordsStorage.ToArray();
    /// <summary>BLAKE3 domain separation label for auth-id material. Returns an owned copy.</summary>
    public static byte[] DomainAuthId => DomainAuthIdStorage.ToArray();

    internal static byte[] DomainRecordBytes => DomainRecordStorage;
    internal static byte[] DomainBucketBytes => DomainBucketStorage;
    internal static byte[] DomainStateBytes => DomainStateStorage;
    internal static byte[] DomainBatchBytes => DomainBatchStorage;
    internal static byte[] DomainRecordsBytes => DomainRecordsStorage;

    private static byte[] Ascii(string s) => System.Text.Encoding.ASCII.GetBytes(s);
    /// <summary>Concatenate byte arrays into one buffer (domain label helpers).</summary>
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

/// <summary>BLAKE3 helpers for record digests, batch digests, and roots.</summary>
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

    /// <summary>BLAKE3 truncated to 16 bytes as <see cref="Digest16"/> (no intermediate array).</summary>
    public static Digest16 Blake3Digest16(ReadOnlySpan<byte> data)
    {
        Span<byte> full = stackalloc byte[32];
        Blake3Core.Hash256(data, full);
        return Digest16.From(full[..16]);
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

    /// <summary>Build the canonical byte payload hashed for a single field digest.</summary>
    public static byte[] FieldDigestPayload(ushort fieldId, ushort keyEpoch, ulong wall,
        uint logical, NodeId authorNodeId, ReadOnlyMemory<byte> value) =>
        FieldDigestPayload(fieldId, keyEpoch, wall, logical, authorNodeId, value, isTombstone: false);

    /// <summary>Build a field digest payload with an explicit value/tombstone discriminator.</summary>
    public static byte[] FieldDigestPayload(ushort fieldId, ushort keyEpoch, ulong wall,
        uint logical, NodeId authorNodeId, ReadOnlyMemory<byte> value, bool isTombstone)
    {
        var w = new Writer();
        w.U16(fieldId).U8(isTombstone ? (byte)1 : (byte)0)
            .U16(keyEpoch).U64(wall).U32(logical).B128(authorNodeId);
        w.Varint((ulong)value.Length).Raw(value.Span);
        return w.Value();
    }

    /// <summary>Build the canonical byte payload hashed for a record lifecycle digest.</summary>
    public static byte[] LifecycleDigestPayload(ulong wall, uint logical, NodeId lifecycleAuthor, byte value)
    {
        Span<byte> one = stackalloc byte[1];
        one[0] = value;
        var w = new Writer(48);
        w.U16(0).U16(0).U64(wall).U32(logical).B128(lifecycleAuthor);
        w.Varint(1).Raw(one);
        return w.Value();
    }

    /// <summary>Compute the canonical digest of a record state.</summary>
    public static byte[] RecordDigest(Record rec)
    {
        var parts = new List<byte[]>();
        var allFields = new List<(ushort FieldId, byte Kind, FieldState State)>(rec.State.Count);
        foreach (var field in rec.State)
            allFields.Add((field.Key, 0, field.Value));

        // Most records have no field tombstones. Do not invoke the lazy getter on
        // the digest hot path unless the record actually owns tomb entries.
        if (rec.HasFieldTombs)
            foreach (var tomb in rec.FieldTombs)
                allFields.Add((tomb.Key, 1, tomb.Value));

        allFields.Sort((x, y) =>
        {
            int byField = x.FieldId.CompareTo(y.FieldId);
            return byField != 0 ? byField : x.Kind.CompareTo(y.Kind);
        });
        foreach (var (fid, kind, fs) in allFields)
            parts.Add(FieldDigestPayload(fid, fs.KeyEpoch, fs.Timestamp.Wall,
                fs.Timestamp.Logical, fs.AuthorNodeId, fs.Value, isTombstone: kind == 1));
        var lc = LifecycleDigestPayload(
            rec.LifecycleHlc?.Wall ?? 0, rec.LifecycleHlc?.Logical ?? 0,
            rec.LifecycleAuthor ?? default, rec.LifecycleValue);
        int est = Domains.DomainRecordBytes.Length + 16 + lc.Length;
        for (int i = 0; i < parts.Count; i++) est += parts[i].Length;
        var w = new Writer(est);
        Span<byte> ridBuf = stackalloc byte[16];
        rec.RecordId.WriteTo(ridBuf);
        w.Raw(Domains.DomainRecordBytes).Raw(ridBuf);
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
    /// <summary>Bucket prefix bytes for the IBLT cell.</summary>
    public byte[] Prefix;
    /// <summary>IBLT cell signed count.</summary>
    public long Count = 0;
    /// <summary>IBLT cell key XOR accumulator.</summary>
    public UInt128 Xor = 0;
    /// <summary>IBLT cell sum accumulator (mod 2^128).</summary>
    public UInt128 Sum = 0;   // wraps mod 2^128 like Python's masking at encode time

    /// <summary>Running XOR/sum/count accumulator used to build bucket digests.</summary>
    public BucketAccumulator(byte[]? prefix = null) => Prefix = prefix ?? Array.Empty<byte>();

    /// <summary>Derive the bucket key for a record id (BLAKE3 over the id).</summary>
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

    /// <summary>Fold a record digest into this accumulator (insert).</summary>
    public void Add(byte[] digest) { var n = ToNum(digest); Count += 1; Xor ^= n; Sum += n; }
    /// <summary>Fold a record digest out of this accumulator (delete).</summary>
    public void Remove(byte[] digest) { var n = ToNum(digest); Count -= 1; Xor ^= n; Sum -= n; }

    private static byte[] U128Le(UInt128 v)
    {
        var b = new byte[16];
        for (int i = 0; i < 16; i++) { b[i] = (byte)(v & 0xFF); v >>= 8; }
        return b;
    }

    /// <summary>Serialize the accumulator cells into the canonical byte encoding.</summary>
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

    /// <summary>BLAKE3 rollup of this accumulator under the bucket domain label.</summary>
    public byte[] Rollup() => Digests.Blake3(Domains.Cat(Domains.DomainBucketBytes, CanonicalEncoding()), 32);
}

/// <summary>Helpers to compute namespace state roots and bucket digests.</summary>
public static class StateHash
{
    /// <summary>Compute the state-root digest for replicated namespace data.</summary>
    public static byte[] StateRootDigest(ushort namespaceId, List<BucketAccumulator> buckets)
    {
        var sorted = buckets.OrderBy(b => Convert.ToHexString(b.CanonicalEncoding())).ToList();
        long total = buckets.Sum(b => b.Count);
        var w = new Writer();
        w.Raw(Domains.DomainStateBytes).U16(namespaceId).Varint((ulong)total);
        foreach (var b in sorted) w.Bytes(b.CanonicalEncoding());
        return Digests.Blake3(w.Value(), 32);
    }

    /// <summary>Compatibility overload. Outbox cursors are not part of replicated state.</summary>
    public static byte[] StateRootDigest(ushort namespaceId, ulong outboxCursor, List<BucketAccumulator> buckets) =>
        StateRootDigest(namespaceId, buckets);

    /// <summary>Partition record digests into bucket accumulators for anti-entropy.</summary>
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
