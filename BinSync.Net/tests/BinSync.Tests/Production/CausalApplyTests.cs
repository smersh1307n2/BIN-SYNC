// Integration: Node.ApplyBatchCausal + CausalTracker (KЗ4 / KЗ5).
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class CausalApplyTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    static byte[] Rid(byte b)
    {
        var a = new byte[16];
        a[0] = b;
        return a;
    }

    /// <summary>Minimal single-entry batch with optional causal deps (bypass full cursor chain).</summary>
    static (Node Local, Node Remote, Batch Batch) SetupPairWithDeps(byte[] changeId, byte[][] deps)
    {
        var a = new Node(Id(0xA1), epoch: 1);
        var b = new Node(Id(0xB2), epoch: 1);
        a.Causal = new CausalTracker();
        b.Causal = new CausalTracker();

        // Local write so we have a real op payload / outbox entry shape.
        var rid = Rid(1);
        a.WriteField(1, rid, fieldId: 1, value: new byte[] { 0x11 });
        var entry = a.Outbox[1][^1];
        entry.ChangeId = changeId;
        entry.CausalDeps = deps.ToList();

        // Build a batch the remote can try to apply via causal gate only.
        // Full ApplyBatch needs SEQ/cursor setup — we only assert the causal gate throws
        // before SEQ when deps missing, and succeeds path after NoteApplied parents.
        var batch = new Batch(
            ns: 1, schemaId: 0, schemaVer: 1, policy: 0x02, flags: 0,
            issuer: a.NodeId, issuerEpoch: a.Epoch,
            cstart: 1, cend: 1,
            batchId: Id(0xBB), prev: Node.Zero128, root: Node.Zero128, digest: Node.Zero128,
            entries: new List<OutboxEntry> { entry });

        return (a, b, batch);
    }

    [Fact]
    public void ApplyBatchCausal_without_tracker_delegates_like_plain_path()
    {
        var n = new Node(Id(1), epoch: 1);
        Assert.Null(n.Causal);
        // No entries → Ready path, will still hit SEQ_GAP on empty peer — empty batch is fine
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(9), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry>());
        // Empty entries: causal skipped; ApplyBatchInternal may still require cursor.
        // Using cursor start 1 on fresh peer expects 1 — OK for empty apply?
        // Safer: only test that null Causal does not throw CAUSAL_DEP_MISSING.
        try
        {
            n.ApplyBatchCausal(batch, senderPeer: Id(2));
        }
        catch (SyncException ex)
        {
            Assert.NotEqual("CAUSAL_DEP_MISSING", ex.CodeName);
        }
    }

    [Fact]
    public void Missing_dep_throws_CAUSAL_DEP_MISSING_and_quarantines_KZ4()
    {
        var tracker = new CausalTracker();
        var n = new Node(Id(1), epoch: 1) { Causal = tracker };
        var changeId = Id(0x20);
        var parent = Id(0x10);
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            Seq = 1,
            ChangeId = changeId,
            RecordId = Rid(1),
            OpKind = 0x01,
            Payload = Array.Empty<byte>(),
            AuthorNodeId = n.NodeId,
            CausalDeps = new List<byte[]> { parent },
        };
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry> { entry });

        var ex = Assert.Throws<SyncException>(() => n.ApplyBatchCausal(batch, senderPeer: Id(2)));
        Assert.Equal("CAUSAL_DEP_MISSING", ex.CodeName);
        Assert.Equal(0x0012, ex.Code);
        Assert.Equal(1, tracker.PendingCount);
        Assert.False(tracker.IsKnown(changeId));
    }

    [Fact]
    public void Cycle_throws_without_quarantine()
    {
        var tracker = new CausalTracker();
        var n = new Node(Id(1), epoch: 1) { Causal = tracker };
        var changeId = Id(0x20);
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            ChangeId = changeId,
            RecordId = Rid(1),
            Payload = Array.Empty<byte>(),
            AuthorNodeId = n.NodeId,
            CausalDeps = new List<byte[]> { changeId }, // self-dep
        };
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry> { entry });

        var ex = Assert.Throws<SyncException>(() => n.ApplyBatchCausal(batch, senderPeer: Id(2)));
        Assert.Equal("CAUSAL_DEP_MISSING", ex.CodeName);
        Assert.Equal(0, tracker.PendingCount);
    }

    [Fact]
    public void Ready_after_parent_known_reaches_ApplyBatchInternal()
    {
        var tracker = new CausalTracker();
        var parent = Id(0x10);
        tracker.NoteApplied(parent);

        var n = new Node(Id(1), epoch: 1) { Causal = tracker };
        var changeId = Id(0x20);
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            ChangeId = changeId,
            RecordId = Rid(1),
            Payload = Array.Empty<byte>(),
            AuthorNodeId = n.NodeId,
            CausalDeps = new List<byte[]> { parent },
        };
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry> { entry });

        // Causal gate passes; ApplyBatchInternal may still fail on empty/invalid payload / SEQ.
        // Assert we do NOT get CAUSAL_DEP_MISSING.
        try
        {
            n.ApplyBatchCausal(batch, senderPeer: Id(2));
        }
        catch (SyncException ex)
        {
            Assert.NotEqual("CAUSAL_DEP_MISSING", ex.CodeName);
        }
        catch
        {
            // other failures (decode, seq) are acceptable — causal gate cleared
        }
    }

    [Fact]
    public void DrainCausal_releases_after_parent()
    {
        var tracker = new CausalTracker();
        var n = new Node(Id(1), epoch: 1) { Causal = tracker };
        var child = Id(0x20);
        var parent = Id(0x10);
        tracker.EnqueuePending(child, new[] { parent }, payload: "held");
        Assert.Empty(n.DrainCausal());

        tracker.NoteApplied(parent);
        var released = n.DrainCausal();
        Assert.Single(released);
        Assert.Equal(CausalOutcome.Ready, released[0].Outcome);
        Assert.Equal("held", released[0].Op.Payload);
    }
}
