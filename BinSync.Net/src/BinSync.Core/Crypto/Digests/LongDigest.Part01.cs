#nullable disable
using System;

using BinSync.Crypto.Base;
using BinSync.Crypto.Utilities;

namespace BinSync.Crypto.Digests
{
    public abstract partial class LongDigest
    {
        internal void ProcessBlock()
        {
            AdjustByteCounts();

            for (int ti = 16; ti <= 79; ++ti)
            {
                W[ti] = Sigma1(W[ti - 2]) + W[ti - 7] + Sigma0(W[ti - 15]) + W[ti - 16];
            }

            ulong a = H1;
            ulong b = H2;
            ulong c = H3;
            ulong d = H4;
            ulong e = H5;
            ulong f = H6;
            ulong g = H7;
            ulong h = H8;

            int t = 0;
            for (int i = 0; i < 10; i++)
            {
                h += Sum1(e) + Ch(e, f, g) + K[t] + W[t++];
                d += h;
                h += Sum0(a) + Maj(a, b, c);

                g += Sum1(d) + Ch(d, e, f) + K[t] + W[t++];
                c += g;
                g += Sum0(h) + Maj(h, a, b);

                f += Sum1(c) + Ch(c, d, e) + K[t] + W[t++];
                b += f;
                f += Sum0(g) + Maj(g, h, a);

                e += Sum1(b) + Ch(b, c, d) + K[t] + W[t++];
                a += e;
                e += Sum0(f) + Maj(f, g, h);

                d += Sum1(a) + Ch(a, b, c) + K[t] + W[t++];
                h += d;
                d += Sum0(e) + Maj(e, f, g);

                c += Sum1(h) + Ch(h, a, b) + K[t] + W[t++];
                g += c;
                c += Sum0(d) + Maj(d, e, f);

                b += Sum1(g) + Ch(g, h, a) + K[t] + W[t++];
                f += b;
                b += Sum0(c) + Maj(c, d, e);

                a += Sum1(f) + Ch(f, g, h) + K[t] + W[t++];
                e += a;
                a += Sum0(b) + Maj(b, c, d);
            }

            H1 += a;
            H2 += b;
            H3 += c;
            H4 += d;
            H5 += e;
            H6 += f;
            H7 += g;
            H8 += h;

            wOff = 0;
            Array.Clear(W, 0, 16);
        }

        private static ulong Ch(ulong x, ulong y, ulong z) => (x & y) ^ (~x & z);
        private static ulong Maj(ulong x, ulong y, ulong z) => (x & y) ^ (x & z) ^ (y & z);
        private static ulong Sum0(ulong x) => ((x << 36) | (x >> 28)) ^ ((x << 30) | (x >> 34)) ^ ((x << 25) | (x >> 39));
        private static ulong Sum1(ulong x) => ((x << 50) | (x >> 14)) ^ ((x << 46) | (x >> 18)) ^ ((x << 23) | (x >> 41));
        private static ulong Sigma0(ulong x) => ((x << 63) | (x >> 1)) ^ ((x << 56) | (x >> 8)) ^ (x >> 7);
        private static ulong Sigma1(ulong x) => ((x << 45) | (x >> 19)) ^ ((x << 3) | (x >> 61)) ^ (x >> 6);

        internal static readonly ulong[] K =
        {
            0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
            0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
            0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
            0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
            0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
            0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
            0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
            0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
            0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
            0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
            0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
            0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
            0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
            0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
            0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
            0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
            0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
            0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
            0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
            0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817
        };
    }
}
