using System.Buffers.Binary;

namespace BinSync.Core;

/// <summary>
/// Dependency-free BLAKE3-256 implementation following the Rust reference core
/// (chunk tree, 7-round compression function). Used for record/batch content hashes
/// and for the tagged node-state digest in <see cref="NodePersistence"/>.
/// Verified against the official BLAKE3 test vectors.
/// </summary>
public static class Blake3
{
    private const uint ChunkStart = 1 << 0;
    private const uint ChunkEnd   = 1 << 1;
    private const uint Parent     = 1 << 2;
    private const uint Root       = 1 << 3;
    private const int KeyWords = 8;
    private const ulong OutLenWords = 8; // 256-bit output

    private static readonly uint[] Iv =
    {
        0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
        0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19
    };

    /// <summary>BLAKE3-256 of arbitrary data.</summary>
    public static byte[] Hash256(ReadOnlySpan<byte> data) => Hash(data, key: null);

    /// <summary>Tagged hash: BLAKE3 with key = H("BIN-SYNC..." || len(tag) || tag) style domain separation.</summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
        var keyInput = new byte[tagBytes.Length + 8];
        tagBytes.CopyTo(keyInput, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(keyInput.AsSpan(tagBytes.Length), (ulong)tagBytes.Length);
        return Hash(data, key: ToWordsLE(Hash(keyInput, key: null)));
    }

    /// <summary>Hex (lowercase) of <see cref="Hash256"/>.</summary>
    public static string Hex256(ReadOnlySpan<byte> data) => Convert.ToHexString(Hash256(data)).ToLowerInvariant();

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();

    // ------------------------------------------------------------------ core

    private static byte[] Hash(ReadOnlySpan<byte> input, uint[]? key)
    {
        var k = key ?? Iv;
        if (input.Length <= 1024)
            return CvToBytes(ChunkCv(input, k, chunkCounter: 0, root: true))[..32];

        int mid = LargestPow2LessThan(input.Length);
        var cvL = SubtreeCv(input.Slice(0, mid), k);
        var cvR = SubtreeCv(input.Slice(mid), k);

        var block = new byte[64];
        CvToBytes(cvL).CopyTo(block, 0);
        CvToBytes(cvR).CopyTo(block, 32);
        return CvToBytes(Compress(k, block, counter: 0, flags: Parent | Root, outLen: OutLenWords))[..32];
    }

    /// <summary>CV of a subtree of length &gt; 1024 (reference Rust: parent nodes always use the IV).</summary>
    private static uint[] SubtreeCv(ReadOnlySpan<byte> input, uint[] key)
    {
        DebugAssert(input.Length > 1024);
        int mid = LargestPow2LessThan(input.Length);
        var cvL = InputToRootCvOrCv(input.Slice(0, mid), key, hasRightChild: true);
        var cvR = InputToRootCvOrCv(input.Slice(mid), key, hasRightChild: false);

        var block = new byte[64];
        CvToBytes(cvL).CopyTo(block, 0);
        CvToBytes(cvR).CopyTo(block, 32);
        return Compress(Iv, block, counter: 0, flags: Parent, outLen: OutLenWords);
    }

    /// <summary>Reference Rust: input_to_root_cv_or_cv — chunks ≤1024 get KEY from the caller's context.</summary>
    private static uint[] InputToRootCvOrCv(ReadOnlySpan<byte> input, uint[] key, bool hasRightChild)
    {
        if (input.Length <= 1024)
            return ChunkCv(input, key, chunkCounter: 0, root: !hasRightChild);
        return SubtreeCv(input, key);
    }

    private static uint[] ChunkCv(ReadOnlySpan<byte> chunk, uint[] key, ulong chunkCounter, bool root)
    {
        var cv = Arrays.CopyOf(key, KeyWords);
        int blocks = Math.Max(1, (chunk.Length + 63) / 64);
        for (int i = 0; i < blocks; i++)
        {
            var block = new byte[64];
            int off = i * 64;
            int len = Math.Min(64, Math.Max(0, chunk.Length - off));
            if (len > 0) chunk.Slice(off, len).CopyTo(block);
            uint f = ChunkStart | ChunkEnd;
            if (i != 0) f &= ~ChunkStart;
            if (i != blocks - 1) f &= ~ChunkEnd;
            if (root && i == blocks - 1) f |= Root;
            cv = Compress(cv, block, chunkCounter, f, outLen: OutLenWords);
        }
        return cv;
    }

    internal static void DebugAssert(bool cond)
    {
        if (!cond) throw new InvalidOperationException("blake3 invariant violated");
    }

    private static int LargestPow2LessThan(int len)
    {
        int p = 1;
        while (p * 2 < len) p *= 2;
        return p;
    }

    private static uint[] Compress(uint[] cv, byte[] block, ulong counter, uint flags, ulong outLen)
    {
        var m = ToWordsLE(block);
        var st = new uint[16];
        Array.Copy(cv, st, KeyWords);
        Array.Copy(Iv, 0, st, 8, 4);
        st[12] = unchecked((uint)counter);
        st[13] = unchecked((uint)(counter >> 32));
        st[14] = flags;
        st[15] = unchecked((uint)outLen);

        // Reference BLAKE3: round r uses word order perm^r(i), i.e. mm[i] = m[PERM^r[i]].
        var order = new int[16];
        for (int i = 0; i < 16; i++) order[i] = i;
        for (int r = 0; r < 7; r++)
        {
            MixRound(st, m, order);
            var next = new int[16];
            for (int i = 0; i < 16; i++) next[i] = MsgPerm[order[i]];
            order = next;
        }

        var result = new uint[KeyWords];
        for (int i = 0; i < KeyWords; i++) result[i] = st[i] ^ st[i + KeyWords];
        return result;
    }

    private static readonly int[] MsgPerm = { 2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8 };

    private static void MixRound(uint[] s, uint[] m, int[] order)
    {
        G(s, 0, 4, 8, 12, m[order[0]], m[order[1]]);
        G(s, 1, 5, 9, 13, m[order[2]], m[order[3]]);
        G(s, 2, 6, 10, 14, m[order[4]], m[order[5]]);
        G(s, 3, 7, 11, 15, m[order[6]], m[order[7]]);
        G(s, 0, 5, 10, 15, m[order[8]], m[order[9]]);
        G(s, 1, 6, 11, 12, m[order[10]], m[order[11]]);
        G(s, 2, 7, 8, 13, m[order[12]], m[order[13]]);
        G(s, 3, 4, 9, 14, m[order[14]], m[order[15]]);
    }

    private static void G(uint[] s, int a, int b, int c, int d, uint mx, uint my)
    {
        s[a] = WrapAdd(WrapAdd(s[a], s[b]), mx);
        s[d] = RoR(s[d] ^ s[a], 16);
        s[c] = WrapAdd(s[c], s[d]);
        s[b] = RoR(s[b] ^ s[c], 12);
        s[a] = WrapAdd(WrapAdd(s[a], s[b]), my);
        s[d] = RoR(s[d] ^ s[a], 8);
        s[c] = WrapAdd(s[c], s[d]);
        s[b] = RoR(s[b] ^ s[c], 7);
    }

    private static uint RoR(uint x, int n) => (x >> n) | (x << (32 - n));
    private static uint WrapAdd(uint a, uint b) => unchecked(a + b);

    private static uint[] ToWordsLE(byte[] bytes)
    {
        var w = new uint[16];
        for (int i = 0; i < 16 && i * 4 + 4 <= bytes.Length; i++)
            w[i] = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(i * 4));
        return w;
    }

    private static byte[] CvToBytes(uint[] words)
    {
        var b = new byte[words.Length * 4];
        for (int i = 0; i < words.Length; i++)
            BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(i * 4), words[i]);
        return b;
    }
}

internal static class Arrays
{
    public static uint[] CopyOf(uint[] src, int len)
    {
        var dst = new uint[len];
        Array.Copy(src, dst, Math.Min(len, src.Length));
        return dst;
    }
}
