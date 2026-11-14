using System.Buffers.Binary;

namespace BinSync.Storage;

/// <summary>
/// Allocation-free ordering equivalent to ordinal comparison of NodeId.ToString().
/// NodeId.ToString emits the two UInt64 limbs in little-endian byte order, so reversing
/// each limb's endianness gives the exact unsigned comparison order without materializing
/// byte arrays or hexadecimal strings.
/// </summary>
internal sealed class NodeIdCanonicalComparer : IComparer<NodeId>
{
    public static NodeIdCanonicalComparer Instance { get; } = new();

    private NodeIdCanonicalComparer() { }

    public int Compare(NodeId left, NodeId right)
    {
        int lo = BinaryPrimitives.ReverseEndianness(left.Lo)
            .CompareTo(BinaryPrimitives.ReverseEndianness(right.Lo));
        return lo != 0
            ? lo
            : BinaryPrimitives.ReverseEndianness(left.Hi)
                .CompareTo(BinaryPrimitives.ReverseEndianness(right.Hi));
    }
}
