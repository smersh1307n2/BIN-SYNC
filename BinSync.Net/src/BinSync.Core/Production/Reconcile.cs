// Reconcile ladder (§13.1–13.3): STATE_DIGEST / NEED_RANGE / NEED_KEYS (first slice).
// Uses existing Digests / BucketAccumulator / StateHash (§9). SNAPSHOT in Snapshot.cs.
namespace BinSync;

/// <summary>Wire frame type bytes for the reconcile ladder (§12.6).</summary>
public static partial class ReconcileFrames
{
    /// <summary>Digest of the full namespace state (all record digests).</summary>
    public const byte StateDigest = 0x06;
    /// <summary>Constant need range.</summary>
    public const byte NeedRange = 0x07;
    /// <summary>Constant need keys.</summary>
    public const byte NeedKeys = 0x08;
    /// <summary>Constant snapshot chunk.</summary>
    public const byte SnapshotChunk = 0x0A;
}

/// <summary>One bucket row carried in STATE_DIGEST.</summary>
public sealed class DigestBucket
{
    /// <summary>Optional bucket prefix bytes bound into the accumulator.</summary>
    public byte[] Prefix = Array.Empty<byte>();
    /// <summary>Signed multiplicity of digests folded into this cell.</summary>
    public long Count;
    /// <summary>16-byte XOR accumulator for a digest bucket.</summary>
    public byte[] Xor16 = new byte[16];
    /// <summary>16-byte sum accumulator for a digest bucket.</summary>
    public byte[] Sum16 = new byte[16];
    /// <summary>32-byte BLAKE3 rollup of a digest bucket.</summary>
    public byte[] Rollup32 = new byte[32];
}

/// <summary>Decoded STATE_DIGEST payload (§9.1.4 + §13.1).</summary>
public sealed class StateDigestMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId;
    /// <summary>Reserved legacy wire field. Canonical STATE_DIGEST messages set it to zero.</summary>
    public ulong OutboxCursor;
    /// <summary>Number of prefix bits used when partitioning buckets.</summary>
    public int RadixBits = 8;
    /// <summary>Root digest summarizing the state-digest message.</summary>
    public byte[] RootDigest = new byte[32];
    /// <summary>Digest buckets included in a state-digest reconcile message.</summary>
    public List<DigestBucket> Buckets = new();
}

/// <summary>NEED_RANGE: ask peer for records under a BLAKE3(recordId) prefix.</summary>
public sealed class NeedRangeMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId;
    /// <summary>Optional bucket prefix bytes bound into the accumulator.</summary>
    public byte[] Prefix = Array.Empty<byte>();
}

/// <summary>NEED_KEYS: ask peer for specific recordIds.</summary>
public sealed class NeedKeysMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId;
    /// <summary>Explicit record ids listed by a NEED_KEYS reconcile message.</summary>
    public List<byte[]> RecordIds = new();
}

/// <summary>
/// Build / encode / compare state digests. Diff result drives NEED_RANGE requests.
/// </summary>
public static partial class Reconcile
{
    /// <summary>Constant default radix bits.</summary>
    public const int DefaultRadixBits = 8;
    /// <summary>Hard upper bound for bucket rows accepted in one STATE_DIGEST.</summary>
    public const int MaxStateDigestBuckets = 100_000;

    /// <summary>Hard upper bound for record IDs accepted in one NEED_KEYS message.</summary>
    public const int MaxNeedKeysRecordIds = 100_000;

    private static int PrefixLengthForRadix(int radixBits)
    {
        if (radixBits < 8 || radixBits > 256 || (radixBits & 7) != 0)
            throw new FormatException("STATE_DIGEST RadixBits must be byte-aligned from 8 through 256");
        return radixBits / 8;
    }

    private static int ComparePrefix(byte[] left, byte[] right)
    {
        int length = Math.Min(left.Length, right.Length);
        for (int i = 0; i < length; i++)
        {
            int diff = left[i].CompareTo(right[i]);
            if (diff != 0) return diff;
        }
        return left.Length.CompareTo(right.Length);
    }

    private static void ValidateStateDigest(StateDigestMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        int prefixLength = PrefixLengthForRadix(message.RadixBits);
        if (message.NamespaceId < 0 || message.NamespaceId > ushort.MaxValue)
            throw new FormatException("STATE_DIGEST namespace is outside u16 range");
        if (message.RootDigest is null || message.RootDigest.Length != 32)
            throw new FormatException("STATE_DIGEST root digest must be 32 bytes");
        if (message.Buckets is null || message.Buckets.Count > MaxStateDigestBuckets)
            throw new FormatException("STATE_DIGEST bucket count exceeds limit");

        byte[]? previous = null;
        foreach (var bucket in message.Buckets)
        {
            if (bucket is null || bucket.Prefix is null || bucket.Prefix.Length != prefixLength)
                throw new FormatException("STATE_DIGEST bucket prefix length does not match RadixBits");
            if (bucket.Count <= 0)
                throw new FormatException("STATE_DIGEST bucket count must be positive");
            if (bucket.Xor16 is null || bucket.Xor16.Length != 16 ||
                bucket.Sum16 is null || bucket.Sum16.Length != 16 ||
                bucket.Rollup32 is null || bucket.Rollup32.Length != 32)
                throw new FormatException("STATE_DIGEST bucket accumulator has an invalid length");
            if (previous is not null && ComparePrefix(previous, bucket.Prefix) >= 0)
                throw new FormatException("STATE_DIGEST bucket prefixes must be strictly increasing");
            previous = bucket.Prefix;
        }
    }

    /// <summary>Compute a STATE_DIGEST view of <paramref name="node"/> for one namespace.</summary>
    public static StateDigestMessage BuildStateDigest(Node node, int namespaceId, int radixBits = DefaultRadixBits)
    {
        _ = PrefixLengthForRadix(radixBits);
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        // Build the bucket digest from one atomic storage view. The legacy cursor field
        // is reserved and is deliberately excluded from replicated state.
        using var gates = node.EnterAllGates();
        if (!node.Storage.TryGetValue(namespaceId, out var records))
            records = new Dictionary<RecordId, Record>();

        var buckets = StateHash.BuildBucketsFromView(records, radixBits);
        // Outbox sequence numbers are independent per author stream. A scalar maximum
        // is neither a globally comparable cursor nor replicated state; delta/ACK
        // synchronization tracks each author stream separately.
        const ulong cursor = 0;
        var root = StateHash.StateRootDigest((ushort)namespaceId, buckets);

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

    /// <summary>Encode state digest into canonical wire bytes.</summary>
    public static byte[] EncodeStateDigest(StateDigestMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U64(0); // Reserved legacy field; never serialize a misleading aggregate cursor.
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

    /// <summary>Decode state digest from canonical wire bytes.</summary>
    public static StateDigestMessage DecodeStateDigest(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        int namespaceId = r.U16();
        _ = r.U64(); // Consume the reserved legacy cursor field.
        int radixBits = r.U8();
        int expectedPrefixLength = PrefixLengthForRadix(radixBits);
        var rootDigest = r.Raw(32);
        ulong bucketCount = r.Varint();
        if (bucketCount > MaxStateDigestBuckets)
            throw new FormatException("STATE_DIGEST bucket count exceeds limit");

        // Each row has a one-byte prefix length, prefix bytes, and 68 fixed bytes.
        int minimumRowLength = 1 + expectedPrefixLength + 4 + 16 + 16 + 32;
        if (bucketCount > (ulong)(r.Remaining / minimumRowLength))
            throw new FormatException("STATE_DIGEST bucket count exceeds remaining payload");

        var m = new StateDigestMessage
        {
            NamespaceId = namespaceId,
            OutboxCursor = 0,
            RadixBits = radixBits,
            RootDigest = rootDigest,
        };
        byte[]? previousPrefix = null;
        for (ulong i = 0; i < bucketCount; i++)
        {
            int prefixLength = r.U8();
            if (prefixLength != expectedPrefixLength)
                throw new FormatException("STATE_DIGEST bucket prefix length does not match RadixBits");
            var prefix = r.Raw(prefixLength);
            if (previousPrefix is not null && ComparePrefix(previousPrefix, prefix) >= 0)
                throw new FormatException("STATE_DIGEST bucket prefixes must be strictly increasing");
            uint count = r.U32();
            if (count == 0)
                throw new FormatException("STATE_DIGEST bucket count must be positive");
            m.Buckets.Add(new DigestBucket
            {
                Prefix = prefix,
                Count = count,
                Xor16 = r.Raw(16),
                Sum16 = r.Raw(16),
                Rollup32 = r.Raw(32),
            });
            previousPrefix = prefix;
        }
        if (!r.Eof())
            throw new FormatException("STATE_DIGEST contains trailing bytes");
        return m;
    }

    /// <summary>Encode need range into canonical wire bytes.</summary>
    public static byte[] EncodeNeedRange(NeedRangeMessage m)
    {
        ArgumentNullException.ThrowIfNull(m);
        if (m.NamespaceId < 0 || m.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(m.NamespaceId));
        if (m.Prefix is null || m.Prefix.Length > 32)
            throw new FormatException("NEED_RANGE prefix must contain at most 32 bytes");
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U8((byte)m.Prefix.Length);
        w.Raw(m.Prefix);
        return w.Value();
    }

    /// <summary>Decode need range from canonical wire bytes.</summary>
    public static NeedRangeMessage DecodeNeedRange(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        int ns = r.U16();
        int plen = r.U8();
        if (plen > 32)
            throw new FormatException("NEED_RANGE prefix must contain at most 32 bytes");
        var prefix = r.Raw(plen);
        if (!r.Eof())
            throw new FormatException("NEED_RANGE contains trailing bytes");
        return new NeedRangeMessage { NamespaceId = ns, Prefix = prefix };
    }

    /// <summary>Encode need keys into canonical wire bytes.</summary>
    public static byte[] EncodeNeedKeys(NeedKeysMessage m)
    {
        ArgumentNullException.ThrowIfNull(m);
        if (m.NamespaceId < 0 || m.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(m.NamespaceId));
        if (m.RecordIds is null || m.RecordIds.Count > MaxNeedKeysRecordIds)
            throw new FormatException("NEED_KEYS record count exceeds limit");
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.Varint((ulong)m.RecordIds.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var id in m.RecordIds)
        {
            if (id is null)
                throw new FormatException("NEED_KEYS record ID cannot be null");
            var canonical = Codec.CanonicalB128(id);
            if (!seen.Add(Convert.ToHexString(canonical)))
                throw new FormatException("NEED_KEYS contains a duplicate record ID");
            w.B128(canonical);
        }
        return w.Value();
    }

    /// <summary>Decode need keys from canonical wire bytes.</summary>
    public static NeedKeysMessage DecodeNeedKeys(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        var m = new NeedKeysMessage { NamespaceId = r.U16() };
        ulong count = r.Varint();
        if (count > MaxNeedKeysRecordIds)
            throw new FormatException("NEED_KEYS record count exceeds limit");
        if (count > (ulong)(r.Remaining / 16))
            throw new FormatException("NEED_KEYS record count exceeds remaining payload");
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (ulong i = 0; i < count; i++)
        {
            var id = r.B128();
            if (!seen.Add(Convert.ToHexString(id)))
                throw new FormatException("NEED_KEYS contains a duplicate record ID");
            m.RecordIds.Add(id);
        }
        if (!r.Eof())
            throw new FormatException("NEED_KEYS contains trailing bytes");
        return m;
    }

    /// <summary>Compute bucket prefixes that differ between local and remote state digests.</summary>
    public static List<byte[]> DivergentPrefixes(StateDigestMessage local, StateDigestMessage remote)
    {
        ValidateStateDigest(local);
        ValidateStateDigest(remote);
        if (local.NamespaceId != remote.NamespaceId || local.RadixBits != remote.RadixBits)
            throw new FormatException("STATE_DIGEST namespace or RadixBits mismatch");
        var result = new List<byte[]>();
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

    /// <summary>List local record ids whose bucket key starts with the given prefix.</summary>
    public static List<byte[]> LocalKeysUnderPrefix(Node node, int namespaceId, byte[] prefix)
    {
        var list = new List<byte[]>();
        if (!node.Storage.TryGetValue(namespaceId, out var records)) return list;
        foreach (var rid in records.Keys)
        {
            var key = BucketAccumulator.BucketKey(rid);
            if (key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                list.Add(rid.ToArray());
        }
        return list;
    }

    private static bool BucketEqual(DigestBucket a, DigestBucket b) =>
        a.Count == b.Count &&
        a.Xor16.AsSpan().SequenceEqual(b.Xor16) &&
        a.Sum16.AsSpan().SequenceEqual(b.Sum16) &&
        a.Rollup32.AsSpan().SequenceEqual(b.Rollup32);

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
