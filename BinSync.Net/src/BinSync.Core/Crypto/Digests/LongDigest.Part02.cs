#nullable disable
using System;

using BinSync.Crypto.Base;
using BinSync.Crypto.Utilities;

namespace BinSync.Crypto.Digests
{
    public abstract partial class LongDigest
    {

        public int GetByteLength()
        {
            return MyByteLength;
        }

        public abstract string AlgorithmName { get; }
        public abstract int GetDigestSize();
        public abstract int DoFinal(byte[] output, int outOff);
#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        public abstract int DoFinal(Span<byte> output);
#endif
        public abstract IMemoable Copy();
        public abstract void Reset(IMemoable t);
    }
}
