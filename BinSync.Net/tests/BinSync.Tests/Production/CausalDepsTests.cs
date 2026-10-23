// §16.19 causal deps — KЗ1–KЗ5 unit coverage.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class CausalDepsTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Ready_when_no_deps()
    {
        var t = new CausalTracker();
        var o = t.Evaluate(Id(1), null, out _);
        Assert.Equal(CausalOutcome.Ready, o);
        t.NoteApplied(Id(1));
        Assert.True(t.IsKnown(Id(1)));
    }

    [Fact]
    public void Ready_when_all_parents_known()
    {
        var t = new CausalTracker();
        t.NoteApplied(Id(1));
        t.NoteApplied(Id(2));
        var o = t.Evaluate(Id(3), new[] { Id(1), Id(2) }, out _);
        Assert.Equal(CausalOutcome.Ready, o);
    }

    [Fact]
    public void Pending_when_parent_missing_KZ3_shape()
    {
        var t = new CausalTracker();
        var o = t.Evaluate(Id(2), new[] { Id(1) }, out var detail);
        Assert.Equal(CausalOutcome.Pending, o);
        Assert.Contains(CausalTracker.Key(Id(1)), detail!);
        t.EnqueuePending(Id(2), new[] { Id(1) }, payload: "op2");
        Assert.Equal(1, t.PendingCount);
    }

    [Fact]
    public void Drain_releases_when_parent_arrives()
    {
        var t = new CausalTracker();
        t.EnqueuePending(Id(2), new[] { Id(1) }, payload: 42);
        Assert.Empty(t.Drain()); // still waiting

        t.NoteApplied(Id(1));
        var released = t.Drain();
        Assert.Single(released);
        Assert.Equal(CausalOutcome.Ready, released[0].Outcome);
        Assert.Equal(42, released[0].Op.Payload);
        Assert.Equal(0, t.PendingCount);
        Assert.True(t.IsKnown(Id(2)));
    }

    [Fact]
    public void Timeout_drops_pending_KZ1()
    {
        long now = 1_000_000;
        var t = new CausalTracker
        {
            DepTimeout = TimeSpan.FromMilliseconds(100),
            UtcNowMs = () => now,
        };
        t.EnqueuePending(Id(2), new[] { Id(1) });
        now += 500; // past timeout
        var released = t.Drain();
        Assert.Single(released);
        Assert.Equal(CausalOutcome.TimedOut, released[0].Outcome);
        Assert.Equal(0, t.PendingCount);
        Assert.False(t.IsKnown(Id(2))); // timed-out must not enter known set
    }

    [Fact]
    public void Cycle_detected_KZ2()
    {
        var t = new CausalTracker();
        // A waits on B (pending edge A→B)
        t.EnqueuePending(Id(1), new[] { Id(2) });
        // B claims dep on A → cycle
        var o = t.Evaluate(Id(2), new[] { Id(1) }, out _);
        Assert.Equal(CausalOutcome.Cycle, o);
    }

    [Fact]
    public void Self_dep_is_cycle()
    {
        var t = new CausalTracker();
        var o = t.Evaluate(Id(1), new[] { Id(1) }, out _);
        Assert.Equal(CausalOutcome.Cycle, o);
    }

    [Fact]
    public void MarkLost_drops_dependents_KZ3()
    {
        var t = new CausalTracker();
        t.EnqueuePending(Id(2), new[] { Id(1) });
        t.EnqueuePending(Id(3), new[] { Id(1) });
        t.EnqueuePending(Id(4), new[] { Id(9) }); // unrelated
        var dropped = t.MarkLost(Id(1));
        Assert.Equal(2, dropped.Count);
        Assert.Equal(1, t.PendingCount);
    }

    [Fact]
    public void Batch_all_or_nothing_KZ5()
    {
        var t = new CausalTracker();
        t.NoteApplied(Id(1));
        var members = new List<(byte[], IReadOnlyList<byte[]>)>
        {
            (Id(2), new[] { Id(1) }),           // ready
            (Id(3), new[] { Id(99) }),          // pending
        };
        var o = t.EvaluateBatch(members!, out _);
        Assert.Equal(CausalOutcome.Pending, o);

        t.NoteApplied(Id(99));
        o = t.EvaluateBatch(members!, out _);
        Assert.Equal(CausalOutcome.Ready, o);
    }

    [Fact]
    public void Error_code_table_matches_spec()
    {
        Assert.Equal(0x0012, CausalError.CausalDepMissing);
    }
}
