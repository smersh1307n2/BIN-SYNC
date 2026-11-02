// SEQ_FRACTIONAL (§8.7) and BLOB_REF (§8.8) payload codecs + merge.
namespace BinSync;

/// <summary>
/// Fractional-index sequence item: total order by (parentId, orderKey, author, seq).
/// Wire: parent b128 | orderKey UTF-8 string | author b128 | seq u64 | value bytes.
/// </summary>
public static class SeqFractional
{
    /// <summary>Item type used by the BIN-SYNC engine.</summary>
    public sealed class Item
    {
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] ParentId = Node.Zero128;
        /// <summary>OrderKey: order key.</summary>
        public string OrderKey = "";
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] AuthorNodeId = Node.Zero128;
        /// <summary>Seq: seq.</summary>
        public ulong Seq;
        /// <summary>Return a copy of the written buffer as a new byte array.</summary>
        public byte[] Value = Array.Empty<byte>();
    }

    /// <summary>Encode: encode.</summary>
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

    /// <summary>Decode: decode.</summary>
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
    /// Compare two items by (parentId hex, orderKey, author hex, seq).
    /// Returns &gt;0 if a ranks after b (a is "greater" in total order).
    /// </summary>
    public static int Compare(Item a, Item b)
    {
        int c = string.CompareOrdinal(
            Convert.ToHexString(a.ParentId), Convert.ToHexString(b.ParentId));
        if (c != 0) return c;
        c = string.CompareOrdinal(a.OrderKey ?? "", b.OrderKey ?? "");
        if (c != 0) return c;
        c = string.CompareOrdinal(
            Convert.ToHexString(a.AuthorNodeId), Convert.ToHexString(b.AuthorNodeId));
        if (c != 0) return c;
        return a.Seq.CompareTo(b.Seq);
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
    /// <summary>Descriptor type used by the BIN-SYNC engine.</summary>
    public sealed class Descriptor
    {
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] BlobId = Node.Zero128;
        /// <summary>Size: size.</summary>
        public ulong Size;
        /// <summary>ContentType: content type.</summary>
        public string ContentType = "";
        /// <summary>Hash: hash.</summary>
        public byte[] Hash = new byte[32];
    }

    /// <summary>Encode: encode.</summary>
    public static byte[] Encode(Descriptor d)
    {
        var w = new Writer();
        w.B128(Codec.CanonicalB128(d.BlobId))
            .U64(d.Size)
            .StringUtf8(d.ContentType ?? "")
            .B256(d.Hash is { Length: 32 } ? d.Hash : Codec.CanonicalB256(d.Hash ?? Array.Empty<byte>()));
        return w.Value();
    }

    /// <summary>Decode: decode.</summary>
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
        int cmp = string.CompareOrdinal(
            Convert.ToHexString(a.BlobId), Convert.ToHexString(b.BlobId));
        if (cmp == 0)
            return local; // identical identity — no rewrite
        return cmp < 0 ? remote : local;
    }
}
