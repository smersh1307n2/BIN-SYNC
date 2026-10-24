#nullable disable
using System;

namespace BinSync.Crypto.Base
{
    /// <remarks>Base interface for a message digest.</remarks>
    public interface IDigest
    {
        string AlgorithmName { get; }
        int GetDigestSize();
        int GetByteLength();
        void Update(byte input);
        void BlockUpdate(byte[] input, int inOff, int inLen);
#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        void BlockUpdate(ReadOnlySpan<byte> input);
#endif
        int DoFinal(byte[] output, int outOff);
#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        int DoFinal(Span<byte> output);
#endif
        void Reset();
    }
}
