namespace BinSync;

/// <summary>
/// BLAKE3-256 facade over the fully managed <c>Blake3.Managed</c> NuGet package
/// (pure C#, zero native interop; SIMD/NEON dispatch stays entirely in managed code).
/// This replaces the previous in-tree hand-written implementation, which was a broken
/// reimplementation of the reference core (it threw ArgumentException from OutputBlockWords
/// and produced digests that matched none of the official test vectors).
/// Used by <see cref="Digests"/> for record/batch/state hashes and by
/// <see cref="NodePersistence"/> for the tagged node-state digest.
/// Verified against the official BLAKE3 test vectors (input pattern byte i = i % 251)
/// and against the reference Python `blake3` package — see ProductionHardening tests.
/// </summary>
public static class Blake3Core
{
    /// <summary>BLAKE3-256 of arbitrary data.</summary>
    public static byte[] Hash256(ReadOnlySpan<byte> data)
        => Blake3.Managed.Hasher.Hash(data).AsSpan().ToArray();

    /// <summary>
    /// Tagged hash with domain separation (derive-key mode): key = H_256(tag || u64le(len(tag)) || tag),
    /// then keyed-hash the input — exactly the official BLAKE3 "tagged hash" construction.
    /// </summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        var h = Blake3.Managed.Hasher.NewDeriveKey(System.Text.Encoding.UTF8.GetBytes(tag));
        try
        {
            h.Update(data);
            return h.Finalize().AsSpan().ToArray();
        }
        finally
        {
            h.Dispose();
        }
    }

    /// <summary>Hex (lowercase) of <see cref="Hash256"/>.</summary>
    public static string Hex256(ReadOnlySpan<byte> data) => Convert.ToHexString(Hash256(data)).ToLowerInvariant();

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();
}
