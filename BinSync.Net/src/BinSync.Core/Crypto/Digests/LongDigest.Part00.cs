#nullable disable
using System;

using BinSync.Crypto.Base;
using BinSync.Crypto.Utilities;

namespace BinSync.Crypto.Digests
{
    public abstract partial class LongDigest
        : IDigest, IMemoable
    {
        private int MyByteLength = 128;

        private byte[] xBuf;
        private int xBufOff;

        private long byteCount1;
        private long byteCount2;

        internal ulong H1, H2, H3, H4, H5, H6, H7, H8;

        private ulong[] W = new ulong[80];
        private int wOff;

        internal LongDigest()
        {
            xBuf = new byte[8];
            Reset();
        }

        internal LongDigest(LongDigest t)
        {
            xBuf = new byte[t.xBuf.Length];
            CopyIn(t);
        }

        protected void CopyIn(LongDigest t)
        {
            Array.Copy(t.xBuf, 0, xBuf, 0, t.xBuf.Length);
            xBufOff = t.xBufOff;
            byteCount1 = t.byteCount1;
            byteCount2 = t.byteCount2;
            H1 = t.H1; H2 = t.H2; H3 = t.H3; H4 = t.H4;
            H5 = t.H5; H6 = t.H6; H7 = t.H7; H8 = t.H8;
            Array.Copy(t.W, 0, W, 0, t.W.Length);
            wOff = t.wOff;
        }

        public void Update(byte input)
        {
            xBuf[xBufOff++] = input;
            if (xBufOff == xBuf.Length)
            {
                ProcessWord(xBuf, 0);
                xBufOff = 0;
            }
            byteCount1++;
        }

        public void BlockUpdate(byte[] input, int inOff, int length)
        {
            while ((xBufOff != 0) && (length > 0))
            {
                Update(input[inOff]);
                inOff++; length--;
            }
            while (length >= xBuf.Length)
            {
                ProcessWord(input, inOff);
                inOff += xBuf.Length;
                length -= xBuf.Length;
                byteCount1 += xBuf.Length;
            }
            while (length > 0)
            {
                Update(input[inOff]);
                inOff++; length--;
            }
        }

#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        public void BlockUpdate(ReadOnlySpan<byte> input)
        {
            int inOff = 0;
            int length = input.Length;
            while ((xBufOff != 0) && (length > 0))
            {
                Update(input[inOff]);
                inOff++; length--;
            }
            while (length >= xBuf.Length)
            {
                ProcessWord(input.Slice(inOff, xBuf.Length));
                inOff += xBuf.Length;
                length -= xBuf.Length;
                byteCount1 += xBuf.Length;
            }
            while (length > 0)
            {
                Update(input[inOff]);
                inOff++; length--;
            }
        }
#endif

        public void Finish()
        {
            AdjustByteCounts();
            long lowBitLength = byteCount1 << 3;
            long hiBitLength = byteCount2;
            Update((byte)128);
            while (xBufOff != 0)
                Update((byte)0);
            ProcessLength(lowBitLength, hiBitLength);
            ProcessBlock();
        }

        public virtual void Reset()
        {
            byteCount1 = 0;
            byteCount2 = 0;
            xBufOff = 0;
            for (int i = 0; i < xBuf.Length; i++)
                xBuf[i] = 0;
            wOff = 0;
            Array.Clear(W, 0, W.Length);
        }

        internal void ProcessWord(byte[] input, int inOff)
        {
            W[wOff] = Pack.BE_To_UInt64(input, inOff);
            if (++wOff == 16)
                ProcessBlock();
        }

#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        internal void ProcessWord(ReadOnlySpan<byte> word)
        {
            W[wOff] = Pack.BE_To_UInt64(word);
            if (++wOff == 16)
                ProcessBlock();
        }
#endif

        private void AdjustByteCounts()
        {
            if (byteCount1 > 0x1fffffffffffffffL)
            {
                byteCount2 += (long)((ulong)byteCount1 >> 61);
                byteCount1 &= 0x1fffffffffffffffL;
            }
        }

        internal void ProcessLength(long lowW, long hiW)
        {
            if (wOff > 14)
                ProcessBlock();
            W[14] = (ulong)hiW;
            W[15] = (ulong)lowW;
        }
    }
}
