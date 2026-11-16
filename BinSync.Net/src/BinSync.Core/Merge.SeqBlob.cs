// SEQ_FRACTIONAL (§8.7) and BLOB_REF (§8.8) payload codecs + merge.
using System.Buffers;
using System.Text;

namespace BinSync;

/// <summary>
/// Fractional-index sequence item: total order by (parentId, orderKey, author, seq).
/// Wire: parent b128 | orderKey UTF-8 string | author b128 | seq u64 | value bytes.
/// </summary>
public static class SeqFractional
{
    /// <summary>Sequential-blob item descriptor entry.</summary>
    public sealed class Item
    {
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] ParentId = Node.Zero128;
        /// <summary>Sort key used for sequential blob ordering.</summary>
        public string OrderKey = "";
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] AuthorNodeId = Node.Zero128;
        /// <summary>Monotonic sequence number for the actor.</summary>
        public ulong Seq;
        /// <summary>Return a copy of the written buffer as a new byte array.</summary>
        public byte[] Value = Array.Empty<byte>();
    }

    /// <summary>Encode  into canonical wire bytes.</summary>
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

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static Item Decode(byte[] data)
    {
        var r = new Reader(data);
        var item = new Item
        {
            ParentId = r.B128(),
            OrderKey = r.StringUtf8(),
            AuthorNodeId = r.B128(),
            Seq = r.U64(),
            Value = r.Bytes(),
        };
        if (!r.Eof()) throw new InvalidDataException("Trailing bytes after SEQ_FRACTIONAL payload.");
        return item;
    }

    /// <summary>
    /// Compare two items by (parentId hex, orderKey, author hex, seq).
    /// Returns &gt;0 if a ranks after b (a is "greater" in total order).
    /// </summary>
    public static int Compare(Item a, Item b)
    {
        int c = a.ParentId.AsSpan().SequenceCompareTo(b.ParentId);
        if (c != 0) return c;
        c = CompareOrderKeyUtf8(a.OrderKey ?? "", b.OrderKey ?? "");
        if (c != 0) return c;
        c = a.AuthorNodeId.AsSpan().SequenceCompareTo(b.AuthorNodeId);
        if (c != 0) return c;
        return a.Seq.CompareTo(b.Seq);
    }

    // UTF-8 lexicographic order is Unicode scalar-value order. Comparing UTF-16
    // code units with String.CompareOrdinal differs for supplementary characters,
    // which would make replicas implemented in other languages choose another winner.
    private static int CompareOrderKeyUtf8(string left, string right)
    {
        var a = left.AsSpan();
        var b = right.AsSpan();
        while (!a.IsEmpty && !b.IsEmpty)
        {
            if (Rune.DecodeFromUtf16(a, out var ar, out int aConsumed) != OperationStatus.Done ||
                Rune.DecodeFromUtf16(b, out var br, out int bConsumed) != OperationStatus.Done)
                throw new ArgumentException("SEQ_FRACTIONAL orderKey must contain valid Unicode scalar values.");

            int c = ar.Value.CompareTo(br.Value);
            if (c != 0) return c;
            a = a[aConsumed..];
            b = b[bConsumed..];
        }
        return a.IsEmpty ? (b.IsEmpty ? 0 : -1) : 1;
    }

    /// <summary>Winner is the item that ranks higher in the total order (or equal → keep either).</summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        if (local is null || local.Length == 0)
        {
            _ = Decode(remote); // Validate even when the merge has no local operand.
            return remote;
        }
        var a = Decode(local);
        var b = Decode(remote);
        int order = Compare(b, a);
        if (order == 0 && !a.Value.AsSpan().SequenceEqual(b.Value))
            throw new InvalidDataException("Conflicting SEQ_FRACTIONAL values share the same item identity.");
        return order > 0 ? remote : local;
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
    /// <summary>Sequential-blob descriptor (order key, size, content type).</summary>
    public sealed class Descriptor
    {
        /// <summary>16 zero bytes used as a sentinel identity/digest.</summary>
        public byte[] BlobId = Node.Zero128;
        /// <summary>Declared size of the sequential blob content.</summary>
        public ulong Size;
        /// <summary>MIME-like content type of the sequential blob.</summary>
        public string ContentType = "";
        /// <summary>True when h.</summary>
        public byte[] Hash = new byte[32];
    }

    /// <summary>Encode  into canonical wire bytes.</summary>
    public static byte[] Encode(Descriptor d)
    {
        var w = new Writer();
        w.B128(Codec.CanonicalB128(d.BlobId))
            .U64(d.Size)
            .StringUtf8(d.ContentType ?? "")
            .B256(d.Hash is { Length: 32 } ? d.Hash : Codec.CanonicalB256(d.Hash ?? Array.Empty<byte>()));
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static Descriptor Decode(byte[] data)
    {
        var r = new Reader(data);
        var descriptor = new Descriptor
        {
            BlobId = r.B128(),
            Size = r.U64(),
            ContentType = r.StringUtf8(),
            Hash = r.B256(),
        };
        if (!r.Eof()) throw new InvalidDataException("Trailing bytes after BLOB_REF payload.");
        return descriptor;
    }

    /// <summary>
    /// Immutable: same blobId → keep local encoding; different → lexicographically greater blobId wins
    /// (deterministic; field-level HLC still applied by MergeCrdtPayload metadata path).
    /// </summary>
    public static byte[] Merge(byte[]? local, byte[] remote)
    {
        if (local is null || local.Length == 0)
        {
            _ = Decode(remote); // Validate even when the merge has no local operand.
            return remote;
        }
        var a = Decode(local);
        var b = Decode(remote);
        int cmp = a.BlobId.AsSpan().SequenceCompareTo(b.BlobId);
        if (cmp == 0)
        {
            if (a.Size != b.Size ||
                !string.Equals(a.ContentType, b.ContentType, StringComparison.Ordinal) ||
                !a.Hash.AsSpan().SequenceEqual(b.Hash))
                throw new InvalidDataException("Conflicting BLOB_REF descriptors share the same blobId.");
            return local; // identical immutable descriptor — no rewrite
        }
        return cmp < 0 ? remote : local;
    }
}
