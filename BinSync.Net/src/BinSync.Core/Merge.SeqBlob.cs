// SEQ_FRACTIONAL (§8.7) and BLOB_REF (§8.8) payload codecs + merge.
namespace BinSync;

/// <summary>
/// Fractional-index sequence item: total order by (parentId, orderKey, author, seq).
/// Wire: parent b128 | orderKey UTF-8 string | author b128 | seq u64 | value bytes.
/// </summary>
public static class SeqFractional
{
    public sealed class Item
    {
        public byte[] ParentId = Node.Zero128;
        public string OrderKey = "";
        public byte[] AuthorNodeId = Node.Zero128;
        public ulong Seq;
        public byte[] Value = Array.Empty<byte>();
    }

    public static byte[] Encode(Item item)
    {
        var w = new Writer();
        w.B128(Codec.CanonicalB128(item.ParentId))
            .StringUtf8(item.OrderKey ?? "")
            .B128(Codec.CanonicalB128(item.AuthorNodeId))
            .U64(item.Seq)
            .Bytes(item.Value ?? Array.Empty<byte>());
        return w.Value();
    }

    public static Item Decode(byte[] data)
    {
        var r = new Reader(data);
        return new Item
        {
            ParentId = r.B128(),
            OrderKey = r.StringUtf8(),
            AuthorNodeId = r.B128(),
            Seq = r.U64(),
            Value = r.Bytes(),
        };
    }

    /// <summary>
    /// Compare two items by (parentId bytes lexicographic, orderKey ordinal, author bytes
    /// lexicographic, seq). Byte-lexicographic comparison is exactly what Python's
    /// <c>bytes.hex()</c> ordinal comparison encodes (fixed-width lowercase hex preserves
    /// byte order and length), but without allocating hex strings on the hot path.
    /// Returns &gt;0 if a ranks after b (a is "greater" in total order).
    /// </summary>
    public static int Compare(Item a, Item b)
    {
        int c = CompareBytesLex(a.ParentId, b.ParentId);
        if (c != 0) return c;
        c = string.CompareOrdinal(a.OrderKey ?? "", b.OrderKey ?? "");
        if (c != 0) return c;
        c = CompareBytesLex(a.AuthorNodeId, b.AuthorNodeId);
        if (c != 0) return c;
        return a.Seq.CompareTo(b.Seq);
    }

    /// <summary>Python-compatible byte ordering: memcmp first, then length.</summary>
    internal static int CompareBytesLex(byte[]? x, byte[]? y)
    {
        ReadOnlySpan<byte> a = x ?? Array.Empty<byte>(), b = y ?? Array.Empty<byte>();
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
        }
        return a.Length.CompareTo(b.Length);
    }

    /// <summary>Winner is the item that ranks higher in the total order (or equal → keep either).</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        if (local is null || local.Length == 0) return remote;
        var a = Decode(local);
        var b = Decode(remote);
        return Compare(b, a) > 0 ? remote : local;
    }
}

/// <summary>
/// Immutable blob reference (§8.8). Changing content = new blobId.
/// Wire: blobId b128 | size u64 | contentType UTF-8 | hash b256 (optional zero).
/// Merge: if blobIds differ, pick by LWW at field layer; payload itself is immutable.
/// Here we treat equal blobId as identity; divergent blobId → remote wins only if
/// caller already chose via HLC (this merge keeps the payload with the "greater" blobId hex for determinism).
/// </summary>
public static class BlobRef
{
    public sealed class Descriptor
    {
        public byte[] BlobId = Node.Zero128;
        public ulong Size;
        public string ContentType = "";
        public byte[] Hash = new byte[32];
    }

    public static byte[] Encode(Descriptor d)
    {
        var w = new Writer();
        w.B128(Codec.CanonicalB128(d.BlobId))
            .U64(d.Size)
            .StringUtf8(d.ContentType ?? "")
            .B256(d.Hash is { Length: 32 } ? d.Hash : Codec.CanonicalB256(d.Hash ?? Array.Empty<byte>()));
        return w.Value();
    }

    public static Descriptor Decode(byte[] data)
    {
        var r = new Reader(data);
        return new Descriptor
        {
            BlobId = r.B128(),
            Size = r.U64(),
            ContentType = r.StringUtf8(),
            Hash = r.B256(),
        };
    }

    /// <summary>
    /// Immutable: same blobId → keep local encoding; different → lexicographically greater blobId wins
    /// (deterministic; field-level HLC still applied by MergeCrdtPayload metadata path).
    /// </summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        if (local is null || local.Length == 0) return remote;
        var a = Decode(local);
        var b = Decode(remote);
        int cmp = SeqFractional.CompareBytesLex(a.BlobId, b.BlobId);
        if (cmp == 0)
            return local; // identical identity — no rewrite
        return cmp < 0 ? remote : local;
    }
}
