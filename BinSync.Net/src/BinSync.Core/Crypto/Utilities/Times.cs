#nullable disable
using System;

namespace BinSync.Crypto.Utilities
{
    public static class Times
    {
        public static long NanoTime()
        {
            return System.Diagnostics.Stopwatch.GetTimestamp() * 1000000000L / System.Diagnostics.Stopwatch.Frequency;
        }
    }
}
