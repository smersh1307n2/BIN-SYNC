using System;
using System.Diagnostics;
using Xunit;

namespace BinSync.Tests;

/// <summary>
/// Guards against accidental allocation regressions on ApplyBatch replay / local write paths.
/// Absolute GC numbers vary by runtime; we assert stability across a warm loop.
/// </summary>
public class AllocationHotPathTests
{
    static byte[] Rid(int n)
    {
        var b = new byte[16];
        BitConverter.TryWriteBytes(b.AsSpan(0, 4), n);
        return b;
    }

    [Fact]
    public void ApplyBatch_replay_duplicate_is_near_allocation_free()
    {
        var a = new Node();
        var b = new Node();
        Transport.MakeSession(a, b);
        a.WriteField(1, Rid(1), 0x100, new byte[] { 1, 2, 3 });
        var batch = a.CreateDeltaBatch(b.NodeId, 1)!;
        Assert.NotNull(batch);
        // First apply
        var r1 = b.ApplyBatch(batch, a.NodeId);
        Assert.False(r1.Duplicate);
        // Warm-up replay
        for (int i = 0; i < 20; i++)
            b.ApplyBatch(batch, a.NodeId);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 200; i++)
        {
            var r = b.ApplyBatch(batch, a.NodeId);
            Assert.True(r.Duplicate);
        }
        long after = GC.GetAllocatedBytesForCurrentThread();
        long per = (after - before) / 200;
        // Duplicate path should stay in the low KB range (dict lookups, result object).
        // Regression guard: fail if average exceeds 8 KB per call.
        Assert.True(per < 8_000, $"ApplyBatch replay allocated ~{per} B/call (budget 8000)");
    }

    [Fact]
    public void WriteField_steady_state_budget()
    {
        var n = new Node();
        // Warm
        for (int i = 0; i < 50; i++)
            n.WriteField(1, Rid(i), 0x100, new byte[] { 9 });

        long before = GC.GetAllocatedBytesForCurrentThread();
        const int N = 200;
        for (int i = 0; i < N; i++)
            n.WriteField(1, Rid(10_000 + i), 0x100, new byte[] { 1, 2, 3, 4 });
        long after = GC.GetAllocatedBytesForCurrentThread();
        long per = (after - before) / N;
        // New record + outbox entry + op encode: allow up to 4 KB average.
        Assert.True(per < 4_000, $"WriteField allocated ~{per} B/call (budget 4000)");
    }
}
