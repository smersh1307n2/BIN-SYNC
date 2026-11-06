// §16.19 causal deps — KЗ1–KЗ5 unit coverage.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class CausalDepsTests
{
    static BatchId Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return BatchId.From(a);
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
        Assert.False(t.IsKnown(Id(2))); // Drain releases; successful apply confirms known state.
        t.NoteApplied(Id(2));
        Assert.True(t.IsKnown(Id(2)));
    }

    [Fact]
    public void Requeue_after_failed_apply_does_not_mark_change_known()
    {
        var tracker = new CausalTracker();
        tracker.NoteApplied(Id(1));
        tracker.EnqueuePending(Id(2), new[] { Id(1) }, payload: "batch");

        var released = Assert.Single(tracker.Drain());
        Assert.Equal(CausalOutcome.Ready, released.Outcome);
        Assert.False(tracker.IsKnown(Id(2)));

        tracker.RequeuePending(released.Op);
        Assert.Equal(1, tracker.PendingCount);
        Assert.False(tracker.IsKnown(Id(2)));
        Assert.Single(tracker.Drain());
    }

    [Fact]
    public void NoteApplied_removes_an_existing_pending_entry()
    {
        var tracker = new CausalTracker();
        tracker.EnqueuePending(Id(2), new[] { Id(1) });
        tracker.NoteApplied(Id(2));

        Assert.Equal(0, tracker.PendingCount);
        Assert.True(tracker.IsKnown(Id(2)));
        Assert.Empty(tracker.Drain());
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
    public void Causal_policy_rejects_invalid_configuration()
    {
        var tracker = new CausalTracker();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            tracker.DepTimeout = TimeSpan.FromMilliseconds(-1));
        Assert.Throws<ArgumentNullException>(() =>
            tracker.UtcNowMs = null!);

        tracker.DepTimeout = TimeSpan.Zero;
        long now = 123;
        tracker.UtcNowMs = () => now++;
        tracker.EnqueuePending(Id(2), new[] { Id(1) });
        Assert.Single(tracker.Drain());
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
    public void Lost_dependency_is_remembered_for_future_evaluations()
    {
        var tracker = new CausalTracker();
        tracker.MarkLost(Id(1));

        Assert.Equal(CausalOutcome.LostDep,
            tracker.Evaluate(Id(2), new[] { Id(1) }, out _));
        Assert.Throws<InvalidOperationException>(() => tracker.NoteApplied(Id(1)));
    }

    [Fact]
    public void MarkLost_propagates_through_transitive_pending_dependents()
    {
        var tracker = new CausalTracker();
        tracker.EnqueuePending(Id(2), new[] { Id(1) });
        tracker.EnqueuePending(Id(3), new[] { Id(2) });
        tracker.EnqueuePending(Id(4), new[] { Id(3) });

        var dropped = tracker.MarkLost(Id(1));

        Assert.Equal(3, dropped.Count);
        Assert.Equal(0, tracker.PendingCount);
        Assert.Equal(CausalOutcome.LostDep,
            tracker.Evaluate(Id(5), new[] { Id(4) }, out _));
    }

    [Fact]
    public void Drain_propagates_timeout_as_lost_dependency_to_descendants()
    {
        long now = 1_000;
        var tracker = new CausalTracker
        {
            DepTimeout = TimeSpan.FromMilliseconds(100),
            UtcNowMs = () => now,
        };
        tracker.EnqueuePending(Id(2), new[] { Id(1) });
        tracker.EnqueuePending(Id(3), new[] { Id(2) });
        now = 1_500;

        var released = tracker.Drain();

        Assert.Contains(released, x => x.Op.ChangeId == BatchId.From(Id(2)) &&
            x.Outcome == CausalOutcome.TimedOut);
        Assert.Contains(released, x => x.Op.ChangeId == BatchId.From(Id(3)) &&
            x.Outcome == CausalOutcome.LostDep);
        Assert.Equal(0, tracker.PendingCount);
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
    public void MarkLost_does_not_override_an_already_applied_change()
    {
        var tracker = new CausalTracker();
        tracker.NoteApplied(Id(1));

        Assert.Empty(tracker.MarkLost(Id(1)));
        Assert.True(tracker.IsKnown(Id(1)));
        Assert.Equal(CausalOutcome.Ready, tracker.Evaluate(Id(1), new[] { Id(2) }, out _));
    }

    [Fact]
    public void Lost_change_id_cannot_be_reapplied_or_reintroduced_in_a_batch()
    {
        var tracker = new CausalTracker();
        tracker.MarkLost(Id(1));

        Assert.Equal(CausalOutcome.LostDep,
            tracker.Evaluate(Id(1), Array.Empty<BatchId>(), out _));
        var members = new List<(BatchId, IReadOnlyList<BatchId>?)>
        {
            (Id(1), Array.Empty<BatchId>()),
        };
        Assert.Equal(CausalOutcome.LostDep, tracker.EvaluateBatch(members, out _));
    }

    [Fact]
    public void Known_parent_satisfies_dependency_even_if_it_appears_later_in_batch()
    {
        var tracker = new CausalTracker();
        tracker.NoteApplied(Id(2));
        var members = new List<(BatchId, IReadOnlyList<BatchId>?)>
        {
            (Id(1), new[] { Id(2) }),
            (Id(2), Array.Empty<BatchId>()),
        };
        Assert.Equal(CausalOutcome.Ready, tracker.EvaluateBatch(members, out _));
    }

    [Fact]
    public void Batch_gate_accepts_dependencies_on_earlier_members_atomically()
    {
        var tracker = new CausalTracker();
        var members = new List<(BatchId, IReadOnlyList<BatchId>?)>
        {
            (Id(1), Array.Empty<BatchId>()),
            (Id(2), new[] { Id(1) }),
        };
        Assert.Equal(CausalOutcome.Ready, tracker.EvaluateBatch(members, out _));
    }

    [Fact]
    public void Batch_gate_rejects_forward_dependencies_and_duplicate_change_ids()
    {
        var tracker = new CausalTracker();
        var forward = new List<(BatchId, IReadOnlyList<BatchId>?)>
        {
            (Id(1), new[] { Id(2) }),
            (Id(2), Array.Empty<BatchId>()),
        };
        Assert.Equal(CausalOutcome.Cycle, tracker.EvaluateBatch(forward, out _));

        var duplicate = new List<(BatchId, IReadOnlyList<BatchId>?)>
        {
            (Id(1), Array.Empty<BatchId>()),
            (Id(1), Array.Empty<BatchId>()),
        };
        Assert.Throws<InvalidDataException>(() => tracker.EvaluateBatch(duplicate, out _));
    }

    [Fact]
    public void Batch_all_or_nothing_KZ5()
    {
        var t = new CausalTracker();
        t.NoteApplied(Id(1));
        var members = new List<(BatchId, IReadOnlyList<BatchId>)>
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
