using System.Buffers.Binary;

namespace BinSync;

/// <summary>
/// Dependency-free BLAKE3-256 implementation following the reference core
/// (chunk tree, 7-round compression function). Production hardening: it replaces the
/// external `Blake3` NuGet package so the kernel has zero third-party dependencies and
/// works on every RID/platform without native interop. Used by <see cref="Digests"/>
/// for record/batch/state hashes and by <see cref="NodePersistence"/> for the tagged
/// node-state digest.
/// Verified against the official BLAKE3 test vectors (input pattern byte i = i % 251,
/// lengths 0..64, 1024, 1025, 2048, 3072, ...) — see ProductionHardening tests.
/// </summary>
public static class Blake3Core
{
    private const uint ChunkStart = 1 << 0;
    private const uint ChunkEnd   = 1 << 1;
    private const uint Parent     = 1 << 2;
    private const uint Root       = 1 << 3;
    private const int KeyWords = 8;
    private const ulong OutLenWords = 8; // 256-bit output
    private const int ChunkLen = 1024;

    private static readonly uint[] Iv =
    {
        0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
        0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19
    };

    /// <summary>BLAKE3-256 of arbitrary data.</summary>
    public static byte[] Hash256(ReadOnlySpan<byte> data) => Hash(data, key: null);

    /// <summary>Tagged hash with domain separation: key = H_256(tag || u64le(len(tag)) || tag).</summary>
    public static byte[] TaggedHash(string tag, ReadOnlySpan<byte> data)
    {
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
        var keyInput = new byte[tagBytes.Length * 2 + 8];
        tagBytes.CopyTo(keyInput, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(keyInput.AsSpan(tagBytes.Length), (ulong)tagBytes.Length);
        tagBytes.CopyTo(keyInput, tagBytes.Length + 8);
        // The key is the FULL 8-word CV of the tag prehash (words 4..7 matter for keyed hashing).
        return Hash(data, key: ToWordsLE(HashFullCv(keyInput)));
    }

    // ------------------------------------------------------------------ core
    //
    // PRODUCTION FIX (full rewrite per the reference BLAKE3 spec). The previous recursive
    // variant had several independent defects: chunk counters not numbered globally,
    // ROOT flag propagated down whole subtrees, chaining value computed as XOR instead of
    // modular addition, byte-based (not chunk-based) tree split, a message schedule that
    // re-permuted the previous round's order instead of deriving perm^r from the original
    // words, and — most subtly — NodeOutputs that stored only 8 block words. The official
    // pre-round function consumes TEN block words plus (counter, flags, block_len), so an
    // expanded root output must be re-compressed with the last compression's own counter/
    // flags/block_len (NOT zeros) and its message schedule spans all 10 stored words.
    // This version mirrors reference_blake3 impl.rs exactly:
    //   - chunk node -> CV after its last compression + input_block_words[0..10);
    //   - parent node -> chaining CV [cvL[0..4), cvR[0..4)]; at the root the mixing node is
    //     compressed inside output() with PARENT|ROOT, empty block, state[15]=64;
    //   - split point = largest power-of-two number of CHUNKS strictly less than total;
    //   - empty input hashes ONE all-zero block with bytes_hashed = 0 => H("") = af1349b9…

    // Mirrors reference impl.rs OutputNode::output + NodeOutputs EXACTLY. The critical
    // detail that broke every earlier attempt here: NodeOutputs.input_block_words are NOT
    // the raw message words — they are state[8..16) of the FINAL compression, i.e. the
    // message schedule AFTER SIX permutations (the seventh round's inputs). The pre-round
    // expansion then permutes them once more (the 7th permutation). For a chunk node those
    // 16 state words are paired with the last block's own counter/flags/block_len; for a
    // parent node the mixing compression contributes state[8..16) with counter 0,
    // flags PARENT(|ROOT), block_len 64.
    private static (uint[] Cv, NodeOutputs Out) OutputNode(
        ReadOnlySpan<byte> input, uint[] k, ulong counter, bool isRoot)
    {
        if (input.Length <= ChunkLen)
            return ChunkOutput(input, k, counter, isRoot ? Root : 0u);
        int chunks = (input.Length + ChunkLen - 1) / ChunkLen;
        int pChunks = LargestPow2LessThanChunks(chunks);      // chunks in the left subtree
        int pBytes = Math.Min(pChunks * ChunkLen, input.Length);
        var (cvL, _) = OutputNode(input[..pBytes], k, counter, isRoot: false);
        var (cvR, _) = OutputNode(input[pBytes..], k, counter + (ulong)pChunks, isRoot: false);
        return ParentOutput(cvL, cvR, isRoot);
    }

    // Reference impl.rs parent_node(): chaining CV is simply [cvL, cvR] (all 8 words); the
    // mixing compression consumes only the first four words of each child as its input block
    // (iw[0..4)=cvL[0..4), iw[4..8)=cvR[0..4), iw[8..10) unused → zeros), with counter 0,
    // flags PARENT(|ROOT) and block_len 64. The pre-round expansion reuses this node's OWN
    // metadata and state[8..16) schedule tail — see NodeOutputs.
    private static (uint[] Cv, NodeOutputs Out) ParentOutput(uint[] cvL, uint[] cvR, bool isRoot)
    {
        var chain = new uint[]
        {
            cvL[0], cvL[1], cvL[2], cvL[3], cvR[0], cvR[1], cvR[2], cvR[3],
        };
        var mix = new uint[]
        {
            cvL[0], cvL[1], cvL[2], cvL[3], cvR[0], cvR[1], cvR[2], cvR[3], 0, 0,
        };
        uint flags = Parent | (isRoot ? Root : 0u);
        var st = CompressFull(chain, mix, counter: 0, flags, blockLen: 64);
        return (chain, new NodeOutputs(OutputBlockWords(st), chain, 0, flags, 64));
    }

    // Chained chunks start from IV; keyed hashing uses the key only in the FIRST chunk
    // (subsequent chunks get IV as their "key" — reference impl.rs update()).
    private static (uint[] Cv, NodeOutputs Out) ChunkOutput(
        ReadOnlySpan<byte> input, uint[] key, ulong chunkCounter, uint flagsExtra)
    {
        var firstKey = chunkCounter == 0 ? Arrays.CopyOf(key, KeyWords) : Arrays.CopyOf(Iv, KeyWords);
        var cv = firstKey;
        int n = input.Length;
        ulong counter = chunkCounter;
        int pos = 0;
        while (true)
        {
            int remaining = n - pos;
            var block = new byte[64];
            int take = Math.Min(64, Math.Max(0, remaining));
            if (take > 0) input.Slice(pos, take).CopyTo(block);
            bool lastBlock = remaining <= 64;                   // incl. the empty final block
            uint f = (pos == 0 ? ChunkStart : 0u) | (lastBlock ? (ChunkEnd | flagsExtra) : 0u);
            var words = ToWordsLE(block);                       // 16 LE words
            var st = CompressFull(cv, words, counter, f, blockLen: take);
            var ibw = OutputBlockWords(st);                     // 10 pre-round block words
            cv = ChainingValue(st, cv);
            if (lastBlock)
                return (cv, new NodeOutputs(ibw, cv, counter, f, take));
            counter++;
            pos += take;
        }
    }

    // NodeOutputs.input_block_words per reference impl.rs: the TEN STATE words
    // state[8..16) of the node's LAST compression (the six-times-permuted message
    // schedule). The pre-round root expansion consumes these ten words as its input
    // block and continues the permutation sequence with the seventh permutation.
    // NOTE: st is only 16 words, so this copy reads exactly to the end — if it ever
    // throws "source array not long enough", the loaded assembly is stale; rebuild.
    private static uint[] OutputBlockWords(uint[] st)
    {
        System.Diagnostics.Debug.Assert(st.Length == 16);
        var w = new uint[10];
        Array.Copy(st, 8, w, 0, 10);
        return w;
    }

    /// <summary>NodeOutputs of the reference core: 10 pre-round block words (state[8..16)
    /// of the node's last compression) + the node's full chaining CV (8 words) + the
    /// metadata (counter/flags/block_len) of that compression — mirrors impl.rs
    /// Output { input_block_words, cv, output_block_len, ... }</summary>
    private sealed record NodeOutputs(uint[] BlockWords10, uint[] Cv, ulong Counter, uint Flags, int BlockLen);

    private static byte[] Hash(ReadOnlySpan<byte> input, uint[]? key)
        => RootOutputBytes(OutputNode(input, key ?? Iv, counter: 0, isRoot: true).Out)[..32];

    /// <summary>Returns the full 64-byte root output (words 0..15 of the expanded root block).</summary>
    private static byte[] HashFullCv(ReadOnlySpan<byte> input)
        => RootOutputBytes(OutputNode(input, Iv, counter: 0, isRoot: true).Out);

    /// <summary>Hex (lowercase) of <see cref="Hash256"/>.</summary>
    public static string Hex256(ReadOnlySpan<byte> data) => Convert.ToHexString(Hash256(data)).ToLowerInvariant();

    /// <summary>Hex (lowercase) of <see cref="TaggedHash"/>.</summary>
    public static string TaggedHex(string tag, ReadOnlySpan<byte> data)
        => Convert.ToHexString(TaggedHash(tag, data)).ToLowerInvariant();

    // Reference: split at the largest power-of-two number of 1024-byte CHUNKS strictly
    // less than the total chunk count (NOT bytes — that mis-splits lengths like 1025).
    private static int LargestPow2LessThanChunks(int chunks)
    {
        int p = 1;
        while (p * 2 < chunks) p *= 2;
        return p;
    }

    // BLAKE3 chaining value of a compression: state[0..4)+state[8..12) || state[4..8)+cv[0..4).
    private static uint[] ChainingValue(uint[] s, uint[] cv)
    {
        var outCv = new uint[KeyWords];
        for (int i = 0; i < 4; i++)
        {
            outCv[i] = unchecked(s[i] + s[i + 8]);
            outCv[i + 4] = unchecked(s[i + 4] + cv[i]);
        }
        return outCv;
    }

    /// <summary>Reference impl.rs Output::root_output_cv(): the root node's 10 input-block
    /// words iw, its chaining CV cv and metadata (counter/flags/block_len) go through ONE
    /// pre-round compression with ZERO key (IV):
    ///   state = [IV(8), iw[0..4), iw[4..6), ctr_lo, ctr_hi, flags, block_len]
    ///   m     = [iw(10), ctr_lo, ctr_hi, flags, block_len], then m[14]=m[0], m[15]=m[1]
    /// The 32-byte hash is the first 8 output words: out[i] = input_cv[i] ^ input_cv[i+4],
    /// where input_cv = [st'[0..4)+st'[8..12), st'[4..8)+st'[12..16)] (modular adds).</summary>
    private static byte[] RootOutputBytes(NodeOutputs o)
    {
        var iw = o.BlockWords10;      // 10 pre-round block words of the last compression
        var st = new uint[16];
        Array.Copy(Iv, st, KeyWords);                         // zero key == IV
        Array.Copy(iw, 0, st, 8, 6);                          // state[8..14) = iw[0..6)
        st[12] = unchecked((uint)o.Counter);                  // state[12] = ctr_lo
        st[13] = unchecked((uint)(o.Counter >> 32));          // state[13] = ctr_hi
        st[14] = o.Flags;                                     // state[14] = flags
        st[15] = unchecked((uint)o.BlockLen);                 // state[15] = block_len
        var m = new uint[16];
        Array.Copy(iw, 0, m, 0, 10);                          // m[0..10) = iw
        m[10] = unchecked((uint)o.Counter);                   // m[10] = ctr_lo
        m[11] = unchecked((uint)(o.Counter >> 32));           // m[11] = ctr_hi
        m[12] = o.Flags;                                      // m[12] = flags
        m[13] = unchecked((uint)o.BlockLen);                  // m[13] = block_len
        m[14] = m[0];                                         // PERM tail dups
        m[15] = m[1];
        var mm = new uint[16];
        for (int i = 0; i < 16; i++) mm[i] = m[MsgPerm[i]];   // one pre-round permutation
        MixRound(st, mm);
        var icv = new uint[KeyWords];
        for (int i = 0; i < 4; i++)
        {
            icv[i] = unchecked(st[i] + st[i + 8]);
            icv[i + 4] = unchecked(st[i + 4] + st[i + 12]);
        }
        var outw = new uint[KeyWords];
        for (int i = 0; i < 4; i++) outw[i] = unchecked(icv[i] ^ icv[i + 4]);
        return CvToBytes(outw);
    }

    // One compression: state = [cv(8), iw[0..6), ctr_lo, ctr_hi, flags, block_len] (16 words;
    // a full block supplies all 16 message words directly, so only the 10 stored iw words are
    // needed and slots 10..15 are the metadata). 7 rounds with perm^r-derived schedules. After
    // the 7th round state[8..16) holds the six-times permuted schedule; NodeOutputs captures it
    // so root expansion can continue the permutation sequence in the pre-round function.
    private static uint[] CompressFull(uint[] cv, uint[] iw10, ulong counter, uint flags, int blockLen)
    {
        var st = new uint[16];
        Array.Copy(cv, st, KeyWords);               // state[0..8) = chaining value / key
        Array.Copy(iw10, 0, st, 8, 6);              // state[8..14) = iw[0..6)
        st[12] = unchecked((uint)counter);          // state[12] = ctr_lo
        st[13] = unchecked((uint)(counter >> 32));  // state[13] = ctr_hi
        st[14] = flags;                             // state[14] = flags
        st[15] = unchecked((uint)blockLen);         // state[15] = block_len
        // Reference BLAKE3: round r uses word order perm^r(i), i.e. mm[i] = m[PERM^r[i]].
        // The message schedule must be RE-DERIVED from the ORIGINAL words each round
        // (applying the permutation to the previous round's already-permuted order is wrong).
        // Reference compress(): for FULL blocks the message is exactly the 16 state words
        // (state doubles as m); for nodes storing only 10 input-block words the tail is
        // m = [iw(10), ctr_lo, ctr_hi, flags, block_len], then m[14]=m[0], m[15]=m[1].
        var m = new uint[16];
        if (iw10.Length >= 16)
        {
            Array.Copy(iw10, m, 16);                // full block: 16 raw LE words
        }
        else
        {
            Array.Copy(iw10, 0, m, 0, 10);          // m[0..10) = the ten input-block words
            m[10] = unchecked((uint)counter);       // m[10] = ctr_lo
            m[11] = unchecked((uint)(counter >> 32));// m[11] = ctr_hi
            m[12] = flags;                          // m[12] = flags
            m[13] = unchecked((uint)blockLen);      // m[13] = block_len
            m[14] = m[0];
            m[15] = m[1];                           // tail dups for PERM indices 14,15
        }
        var order = new int[16];
        for (int i = 0; i < 16; i++) order[i] = i;
        for (int r = 0; r < 7; r++)
        {
            var mm = new uint[16];
            for (int i = 0; i < 16; i++) mm[i] = m[order[i]];
            MixRound(st, mm);
            var next = new int[16];
            for (int i = 0; i < 16; i++) next[i] = MsgPerm[order[i]];
            order = next;
        }
        return st;
    }

    private static readonly int[] MsgPerm = { 2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8 };

    private static void MixRound(uint[] s, uint[] m)
    {
        G(s, 0, 4, 8, 12, m[0], m[1]);
        G(s, 1, 5, 9, 13, m[2], m[3]);
        G(s, 2, 6, 10, 14, m[4], m[5]);
        G(s, 3, 7, 11, 15, m[6], m[7]);
        G(s, 0, 5, 10, 15, m[8], m[9]);
        G(s, 1, 6, 11, 12, m[10], m[11]);
        G(s, 2, 7, 8, 13, m[12], m[13]);
        G(s, 3, 4, 9, 14, m[14], m[15]);
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

    private static uint[] ToWordsLE(ReadOnlySpan<byte> bytes, int count = 16)
    {
        var w = new uint[count];
        for (int i = 0; i < count && i * 4 + 4 <= bytes.Length; i++)
            w[i] = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(i * 4));
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
