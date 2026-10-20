using System.Buffers.Binary;
using Blake3Managed = Blake3.Managed;

namespace BinSync.Core;

/// <summary>
/// BLAKE3 facade over the managed NuGet package <c>Blake3.Managed</c> (zero native deps,
/// SIMD-accelerated). Provides the primitives the kernel uses: plain 256-bit digest
/// (<see cref="Hash256"/>/<see cref="Hex256"/>) and domain-separated tagged hash
/// (<see cref="TaggedHash"/>).
///
/// Tagged-hash construction follows the BLAKE3 reference "key from context" recipe:
///   key = H_256(tag || u64le(len(tag)) || tag), then data is hashed in keyed mode.
/// Hash256 is verified against the official BLAKE3 test vectors (tests/Hardening).
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
    /// Tagged hash with domain separation: key = H_256(tag || u64le(len(tag)) || tag),
    /// payload hashed in keyed mode with that key.
    /// </summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
        var keyInput = new byte[tagBytes.Length * 2 + 8];
        tagBytes.CopyTo(keyInput, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(keyInput.AsSpan(tagBytes.Length), (ulong)tagBytes.Length);
        tagBytes.CopyTo(keyInput.AsSpan(tagBytes.Length + 8));

        var key = new byte[32];
        Blake3Managed.Hasher.Hash(keyInput, key);

        using var hasher = Blake3Managed.Hasher.NewKeyed(key);
        hasher.Update(data);
        var output = new byte[32];
        hasher.Finalize(output);
        return output;
    }

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();
}
