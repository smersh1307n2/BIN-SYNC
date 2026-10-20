
namespace BinSync;

/// <summary>
/// BLAKE3-256 facade over the `Blake3.Managed` NuGet package. The previous hand-written
/// in-tree implementation was broken (it failed the official BLAKE3 test vectors, e.g.
/// the empty input), so it was replaced by this wrapper. The public API surface
/// (<see cref="Hash256"/>, <see cref="TaggedHash"/>, <see cref="Hex256"/>,
/// <see cref="TaggedHex"/>) is unchanged, so <see cref="Digests"/>, <see cref="Codec"/>
/// and <see cref="NodePersistence"/> keep working as before. Tagged hashing uses BLAKE3
/// derive-key mode: key = H_256(tag || u64le(len(tag)) || tag), then keyed hash.
/// Verified against the official BLAKE3 test vectors — see ProductionHardening tests.
/// </summary>
public static class Blake3Core
{
    /// <summary>BLAKE3-256 of arbitrary data.</summary>
    public static byte[] Hash256(ReadOnlySpan<byte> data)
        => Blake3.Managed.Hasher.Hash(data).AsSpan().ToArray();

    /// <summary>Tagged hash with domain separation (BLAKE3 derive-key mode).</summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        Span<byte> outBytes = stackalloc byte[32];
        using var hasher = Blake3.Managed.Hasher.NewDeriveKey(tag);
        hasher.Update(data);
        hasher.Finalize(outBytes);
        return outBytes.ToArray();
    }

    /// <summary>Hex (lowercase) of <see cref="Hash256"/>.</summary>
    public static string Hex256(ReadOnlySpan<byte> data) => Convert.ToHexString(Hash256(data)).ToLowerInvariant();

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();
}
