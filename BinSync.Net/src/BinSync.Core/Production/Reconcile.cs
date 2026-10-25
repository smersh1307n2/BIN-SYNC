// Reconcile ladder (§13.1–13.3): STATE_DIGEST / NEED_RANGE / NEED_KEYS (first slice).
// Uses existing Digests / BucketAccumulator / StateHash (§9). SNAPSHOT in Snapshot.cs.
namespace BinSync;

/// <summary>Wire frame type bytes for the reconcile ladder (§12.6).</summary>
public static partial class ReconcileFrames
{
    public const byte StateDigest = 0x06;
    public const byte NeedRange = 0x07;
    public const byte NeedKeys = 0x08;
    public const byte SnapshotChunk = 0x0A;
}

/// <summary>One bucket row carried in STATE_DIGEST.</summary>
public sealed class DigestBucket
{
    public byte[] Prefix = Array.Empty<byte>();
    public long Count;
    public byte[] Xor16 = new byte[16];
    public byte[] Sum16 = new byte[16];
    public byte[] Rollup32 = new byte[32];
}

/// <summary>Decoded STATE_DIGEST payload (§9.1.4 + §13.1).</summary>
public sealed class StateDigestMessage
{
    public int NamespaceId;
    public ulong OutboxCursor;
    public int RadixBits = 8;
    public byte[] RootDigest = new byte[32];
    public List<DigestBucket> Buckets = new();
}

/// <summary>NEED_RANGE: ask peer for records under a BLAKE3(recordId) prefix.</summary>
public sealed class NeedRangeMessage
{
    public int NamespaceId;
    public byte[] Prefix = Array.Empty<byte>();
}

/// <summary>NEED_KEYS: ask peer for specific recordIds.</summary>
public sealed class NeedKeysMessage
{
    public int NamespaceId;
    public List<byte[]> RecordIds = new();
}

/// <summary>
/// Build / encode / compare state digests. Diff result drives NEED_RANGE requests.
/// </summary>
public static partial class Reconcile
{
    public const int DefaultRadixBits = 8;

    /// <summary>Compute a STATE_DIGEST view of <paramref name="node"/> for one namespace.</summary>
    public static StateDigestMessage BuildStateDigest(Node node, int namespaceId, int radixBits = DefaultRadixBits)
    {
        if (!node.Storage.TryGetValue(namespaceId, out var records))
            records = new Dictionary<byte[], Record>(ByteArrayComparer.Instance);

        var dict = new Dictionary<byte[], Record>(ByteArrayComparer.Instance);
        foreach (var (rid, rec) in records)
            dict[rid] = rec;

        var buckets = StateHash.BuildBuckets(dict, radixBits);
        ulong cursor = OutboxHighWater(node, namespaceId);
        var root = StateHash.StateRootDigest((ushort)namespaceId, cursor, buckets);

        var msg = new StateDigestMessage
        {
            NamespaceId = namespaceId,
            OutboxCursor = cursor,
            RadixBits = radixBits,
            RootDigest = root,
        };
        foreach (var b in buckets.OrderBy(x => Convert.ToHexString(x.Prefix), StringComparer.Ordinal))
        {
            var enc = b.CanonicalEncoding();
            var xor = new byte[16];
            var sum = new byte[16];
            int off = b.Prefix.Length;
            if (enc.Length >= off + 4 + 16 + 16)
            {
                Array.Copy(enc, off + 4, xor, 0, 16);
                Array.Copy(enc, off + 4 + 16, sum, 0, 16);
            }
            msg.Buckets.Add(new DigestBucket
            {
                Prefix = b.Prefix,
                Count = b.Count,
                Xor16 = xor,
                Sum16 = sum,
                Rollup32 = b.Rollup(),
            });
        }
        return msg;
    }

    public static byte[] EncodeStateDigest(StateDigestMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U64(m.OutboxCursor);
        w.U8((byte)m.RadixBits);
        w.Raw(m.RootDigest.Length == 32 ? m.RootDigest : Pad32(m.RootDigest));
        w.Varint((ulong)m.Buckets.Count);
        foreach (var b in m.Buckets)
        {
            w.U8((byte)b.Prefix.Length);
            w.Raw(b.Prefix);
            w.U32(unchecked((uint)b.Count));
            w.Raw(Pad16(b.Xor16));
            w.Raw(Pad16(b.Sum16));
            w.Raw(Pad32(b.Rollup32));
        }
        return w.Value();
    }

    public static StateDigestMessage DecodeStateDigest(byte[] payload)
    {
        var r = new Reader(payload);
        var m = new StateDigestMessage
        {
            NamespaceId = r.U16(),
            OutboxCursor = r.U64(),
            RadixBits = r.U8(),
            RootDigest = r.Raw(32),
        };
        int n = (int)r.Varint();
        for (int i = 0; i < n; i++)
        {
            int plen = r.U8();
            var bucket = new DigestBucket
            {
                Prefix = r.Raw(plen),
                Count = r.U32(),
                Xor16 = r.Raw(16),
                Sum16 = r.Raw(16),
                Rollup32 = r.Raw(32),
            };
            m.Buckets.Add(bucket);
        }
        return m;
    }

    public static byte[] EncodeNeedRange(NeedRangeMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U8((byte)m.Prefix.Length);
        w.Raw(m.Prefix);
        return w.Value();
    }

    public static NeedRangeMessage DecodeNeedRange(byte[] payload)
    {
        var r = new Reader(payload);
        int ns = r.U16();
        int plen = r.U8();
        return new NeedRangeMessage { NamespaceId = ns, Prefix = r.Raw(plen) };
    }

    public static byte[] EncodeNeedKeys(NeedKeysMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.Varint((ulong)m.RecordIds.Count);
        foreach (var id in m.RecordIds)
            w.B128(Codec.CanonicalB128(id));
        return w.Value();
    }

    public static NeedKeysMessage DecodeNeedKeys(byte[] payload)
    {
        var r = new Reader(payload);
        var m = new NeedKeysMessage { NamespaceId = r.U16() };
        int n = (int)r.Varint();
        for (int i = 0; i < n; i++)
            m.RecordIds.Add(r.B128());
        return m;
    }

    public static List<byte[]> DivergentPrefixes(StateDigestMessage local, StateDigestMessage remote)
    {
        var result = new List<byte[]>();
        if (local.RootDigest.AsSpan().SequenceEqual(remote.RootDigest) &&
            local.NamespaceId == remote.NamespaceId)
            return result;

        var remoteBy = remote.Buckets.ToDictionary(
            b => Convert.ToHexString(b.Prefix), b => b, StringComparer.Ordinal);
        var localKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var lb in local.Buckets)
        {
            var key = Convert.ToHexString(lb.Prefix);
            localKeys.Add(key);
            if (!remoteBy.TryGetValue(key, out var rb) || !BucketEqual(lb, rb))
                result.Add(lb.Prefix);
        }
        foreach (var rb in remote.Buckets)
        {
            var key = Convert.ToHexString(rb.Prefix);
            if (!localKeys.Contains(key))
                result.Add(rb.Prefix);
        }
        return result;
    }

    public static List<byte[]> LocalKeysUnderPrefix(Node node, int namespaceId, byte[] prefix)
    {
        var list = new List<byte[]>();
        if (!node.Storage.TryGetValue(namespaceId, out var records)) return list;
        foreach (var rid in records.Keys)
        {
            var key = BucketAccumulator.BucketKey(rid);
            if (key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                list.Add(rid);
        }
        return list;
    }

    private static bool BucketEqual(DigestBucket a, DigestBucket b) =>
        a.Count == b.Count &&
        a.Xor16.AsSpan().SequenceEqual(b.Xor16) &&
        a.Sum16.AsSpan().SequenceEqual(b.Sum16) &&
        a.Rollup32.AsSpan().SequenceEqual(b.Rollup32);

    private static ulong OutboxHighWater(Node node, int namespaceId)
    {
        long max = 0;
        foreach (var ((author, ns), seq) in node.OutboxSeqRef)
        {
            if (ns == namespaceId && seq > max) max = seq;
        }
        return unchecked((ulong)max);
    }

    private static byte[] Pad16(byte[] b)
    {
        if (b.Length == 16) return b;
        var r = new byte[16];
        Array.Copy(b, r, Math.Min(16, b.Length));
        return r;
    }

    private static byte[] Pad32(byte[] b)
    {
        if (b.Length == 32) return b;
        var r = new byte[32];
        Array.Copy(b, r, Math.Min(32, b.Length));
        return r;
    }
}

// Content-equality for byte[] keys: see BinSync/ByteKeys.cs (ByteArrayComparer).
