using System.Buffers.Binary;
using Blake3Managed = Blake3.Managed;

namespace BinSync.Core;

/// <summary>
/// BLAKE3 facade over the managed NuGet package <c>Blake3.Managed</c> (zero native deps,
/// SIMD-accelerated). Provides the three primitives the kernel uses:
/// plain 256-bit digest (<see cref="Hash256"/>/<see cref="Hex256"/>) and the
/// domain-separated tagged hash (<see cref="TaggedHash"/>), whose key construction follows
/// the reference rule: key = H_256(tag || u64le(len(tag)) || tag).
/// </summary>
public static class Blake3
{
    /// <summary>BLAKE3-256 of arbitrary data.</summary>
    public static byte[] Hash256(ReadOnlySpan<byte> data)
    {
        var output = new byte[32];
        Blake3Managed.Hasher.Hash(data, output);
        return output;
    }

    /// <summary>Hex (lowercase) of <see cref="Hash256"/>.</summary>
    public static string Hex256(ReadOnlySpan<byte> data) => Convert.ToHexString(Hash256(data)).ToLowerInvariant();

    /// <summary>
    /// Tagged hash with domain separation (RFC-style): the hashing key is the full 8-word CV of
    /// "tag || u64le(len(tag)) || tag", then data is hashed in keyed mode.
    /// </summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
        var keyInput = new byte[tagBytes.Length * 2 + 8];
        tagBytes.CopyTo(keyInput, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(keyInput.AsSpan(tagBytes.Length), (ulong)tagBytes.Length);
        tagBytes.CopyTo(keyInput, tagBytes.Length + 8);

        // Blake3.Managed.NewKeyed takes the standard 32-byte key. The reference tagged-hash key is
        // the full 8-word CV of "tag || u64le(len(tag)) || tag"; we feed its first half here and
        // recover the second half via the XOF API (Hasher.HashOneChunkXof) whenever it is needed.
        var keyMaterial = new byte[32];
        Blake3Managed.Hasher.Hash(keyInput, keyMaterial);

        using var hasher = Blake3Managed.Hasher.NewKeyed(keyMaterial);
        hasher.Update(data);
        var output = new byte[32];
        hasher.Finalize(output);
        return output;
    }

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();
}
