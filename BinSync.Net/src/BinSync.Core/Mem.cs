
using System.Runtime.InteropServices;

namespace BinSync;

/// <summary>Helpers for ReadOnlyMemory&lt;byte&gt; on the wire/outbox path.</summary>
public static class Mem
{
    /// <summary>Return underlying array when Memory is a full array; otherwise copy.</summary>
    public static byte[] ToArray(ReadOnlyMemory<byte> m)
    {
        if (m.IsEmpty) return Array.Empty<byte>();
        if (MemoryMarshal.TryGetArray(m, out ArraySegment<byte> seg)
            && seg.Array is not null && seg.Offset == 0 && seg.Count == seg.Array.Length)
            return seg.Array;
        return m.ToArray();
    }
}
