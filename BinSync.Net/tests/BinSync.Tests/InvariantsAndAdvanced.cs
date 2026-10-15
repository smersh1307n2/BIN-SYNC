using BinSync.Core;
using Xunit;

namespace BinSync.Tests;

// Invariants and advanced protocol scenarios for the BinSync delta-sync engine.
public class InvariantsAndAdvanced
{
    private const string NS = "default";

    private static Rid Rid(long v) => new(v);

    // ---------------------------------------------------------------------
    // Out-of-order batch delivery must be rejected (SEQ_GAP / BATCH_CHAIN_BROKEN)
    // and then fully healed once the missing predecessor is applied.
    //
    // NOTE: this test drives CreateDeltaBatch directly (bypassing
    // Transport.DeliverBatch), so it must reproduce the full batch lifecycle
    // invariant manually: create -> deliver -> checkpoint chain -> ack.
    // ---------------------------------------------------------------------
    [Fact]
    public void OutOfOrderBatchDeliveryRejectedThenHealed()
    {
        var a = new DeltaSyncEngine();            // issuer node
        var b = new DeltaSyncEngine();            // receiver node
        var aNode = new NodeId("A");
        var bNode = new NodeId("B");
        var log = new ReplicatedLog();            // b's replicated log

        // Stage two records on the A->B stream.
        a.Stage(bNode, NS, new[] { Rid(1) });
        var b1 = a.CreateDeltaBatch(bNode, NS);   // range [1..1]
        b1 = a.SignBatchFor(b1, bNode);

        // --- Fix #1 -------------------------------------------------------
        // Emulate the successful wire delivery of b1 exactly as
        // Transport.DeliverBatch would: mark it delivered and persist the
        // chain checkpoint BEFORE building the next batch. Without this the
        // watermark stays at 0 and the second CreateDeltaBatch coalesces the
        // whole unacked range [1..2] into one batch instead of [2..2].
        a.MarkDelivered(a.StateOf(bNode, NS), b1);
        a.AdvanceStream(a.StateOf(bNode, NS), b1);
        // ------------------------------------------------------------------

        a.Stage(bNode, NS, new[] { Rid(2) });
        var b2 = a.CreateDeltaBatch(bNode, NS);   // range [2..2], prev = b1 digest
        b2 = a.SignBatchFor(b2, bNode);

        Assert.Equal(2, b2.SeqFrom);
        Assert.Equal(2, b2.SeqTo);

        // Deliver b2 before b1 was ever applied on the receiver: rejected.
        var ex = Assert.Throws<InvalidOperationException>(
            () => b.DeliverInbound(log, b2, a.StateOf(bNode, NS)));
        Assert.True(ex.Message == "SEQ_GAP" || ex.Message == "BATCH_CHAIN_BROKEN",
            $"expected SEQ_GAP/BATCH_CHAIN_BROKEN, got: {ex.Message}");

        // Heal the chain: retransmit the first batch (range [1..1], prev = genesis).
        var d1 = new Batch(1, 1, new[] { Rid(1) }, Digest.Empty, b1.BatchDigest);
        b.ApplyBatch(log, d1, a.StateOf(bNode, NS)); // first batch heals the chain

        // --- Fix #2 -------------------------------------------------------
        // The receiver ACKs the already-accepted batch back to the issuer;
        // without it the retry of b2 would hit a spurious SEQ_GAP because the
        // issuer never learned that b1 was accepted.
        a.ProcessAck(a.StateOf(bNode, NS), d1.SeqTo);
        // ------------------------------------------------------------------

        b.DeliverInbound(log, b2, a.StateOf(bNode, NS)); // clean application now

        Assert.Equal(new[] { Rid(1), Rid(2) }, log.Entries);
    }

    // ---------------------------------------------------------------------
    // Outbox streams are delivered strictly in seq order and the sync cursor
    // advances monotonically.
    // ---------------------------------------------------------------------
    [Fact]
    public void OutboxStreamOrderingAndCursorMonotonicity()
    {
        var a = new DeltaSyncEngine();
        var b = new DeltaSyncEngine();
        var aNode = new NodeId("A");
        var bNode = new NodeId("B");
        var log = new ReplicatedLog();

        // Stage 12 records (Rid(1)..Rid(12)) and deliver them in batches of 3.
        for (int i = 0; i < 12; i++)
            a.Stage(bNode, NS, new[] { Rid(i + 1) });

        long lastCursor = 0;
        while (log.AppliedCount < 12)
        {
            var batch = a.DeliverBatch(log, bNode, NS, maxRecords: 3);
            var cursor = a.StateOf(bNode, NS).LastAckedSeq;
            Assert.True(cursor > lastCursor, "cursor must advance monotonically");
            lastCursor = cursor;
            Assert.Equal(batch.SeqTo, cursor);
        }

        Assert.Equal(12, lastCursor);

        // A trailing single-record batch is staged but intentionally NOT
        // delivered (it only exists in the outbox at this point).
        a.Stage(bNode, NS, new[] { Rid(99) });
        var pending = a.CreateDeltaBatch(bNode, NS);
        Assert.Equal(13, pending.SeqFrom);
        Assert.Equal(new[] { Rid(99) }, pending.Records);

        // The synced prefix is exactly Rid(1)..Rid(12) — everything actually
        // delivered to b. (Previously this check had an off-by-one: it compared
        // against Rid(0)..Rid(11) plus the never-delivered Rid(99).)
        var expected = Enumerable.Range(1, 12).Select(i => Rid(i)).ToList();
        Assert.Equal(expected, log.Entries);

        // Cursor remains monotonic and unchanged while the extra batch sits undelivered.
        Assert.Equal(12, a.StateOf(bNode, NS).LastAckedSeq);
    }
}
