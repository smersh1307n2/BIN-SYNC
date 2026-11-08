// §16.19 causal wire trailer + DrainAndRetry integration.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class CausalWireTests
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

    [Fact]
    public void Trailer_roundtrip_restores_deps()
    {
        var entries = new List<OutboxEntry>
        {
            new()
            {
                ChangeId = Id(0x20),
                RecordId = Rid(1),
                CausalDeps = new List<BatchId> { Id(0x10), Id(0x11) },
                Payload = new byte[] { 0x01 },
            },
            new()
            {
                ChangeId = Id(0x21),
                RecordId = Rid(2),
                CausalDeps = new List<BatchId>(),
                Payload = new byte[] { 0x02 },
            },
        };
        var trailer = CausalWire.EncodeTrailer(entries);
        Assert.True(trailer.Length > 4);

        // Simulate body + trailer
        var body = new byte[] { 0xAA, 0xBB };
        var combined = new byte[body.Length + trailer.Length];
        body.CopyTo(combined, 0);
        trailer.CopyTo(combined, body.Length);

        var target = new List<OutboxEntry>
        {
            new() { ChangeId = Id(0x20), RecordId = Rid(1), Payload = new byte[] { 0x01 } },
            new() { ChangeId = Id(0x21), RecordId = Rid(2), Payload = new byte[] { 0x02 } },
        };
        int bodyLen = CausalWire.TryApplyTrailer(combined, target);
        Assert.Equal(body.Length, bodyLen);
        Assert.Equal(Id(0x20), target[0].ChangeId);
        Assert.Equal(2, target[0].CausalDeps.Count);
        Assert.Equal(Id(0x10), target[0].CausalDeps[0]);
        Assert.Equal(Id(0x21), target[1].ChangeId);
        Assert.Empty(target[1].CausalDeps);
    }

    [Fact]
    public void EncodeDecodeBatchPayloadCausal_preserves_deps()
    {
        var n = new Node(Id(1), epoch: 1);
        var parent = Id(0x10);
        var child = Id(0x20);
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            Seq = 1,
            ChangeId = child,
            OriginAuthorEpoch = 1, OriginSequence = 1,
            RecordId = Rid(1),
            OpKind = 0x01,
            Payload = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, // minimal-ish
            AuthorNodeId = n.NodeId.ToArray(),
            CausalDeps = new List<BatchId> { parent },
        };
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry> { entry });

        // Use causal encode even if standard op payload is synthetic
        var wire = n.EncodeBatchPayloadCausal(batch);
        Assert.Equal(0, batch.Flags & CausalWire.FlagHasCausalTrailer);
        Assert.NotEqual(0, wire[7] & CausalWire.FlagHasCausalTrailer);

        var back = n.DecodeBatchPayloadCausal(wire);
        Assert.Single(back.Entries);
        Assert.Equal(child, back.Entries[0].ChangeId);
        Assert.Single(back.Entries[0].CausalDeps);
        Assert.Equal(parent, back.Entries[0].CausalDeps[0]);
    }

    [Fact]
    public void Trailer_parser_is_atomic_when_a_later_entry_is_truncated()
    {
        var first = Id(0x20);
        var second = Id(0x21);
        var w = new Writer().U32(CausalWire.Magic).Varint(2)
            .B128(first).Varint(1).B128(Id(0x10))
            .B128(second).Varint(1); // second dependency bytes intentionally missing
        var trailerBody = w.Value();
        var trailer = new byte[trailerBody.Length + 4];
        trailerBody.CopyTo(trailer, 0);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
            trailer.AsSpan(trailerBody.Length), (uint)trailerBody.Length);
        var combined = new byte[2 + trailer.Length];
        combined[0] = 0xAA;
        combined[1] = 0xBB;
        trailer.CopyTo(combined, 2);

        var target = new List<OutboxEntry>
        {
            new() { ChangeId = first, CausalDeps = new List<BatchId>() },
            new() { ChangeId = second, CausalDeps = new List<BatchId>() },
        };
        Assert.Equal(combined.Length, CausalWire.TryApplyTrailer(combined, target));
        Assert.Empty(target[0].CausalDeps);
        Assert.Empty(target[1].CausalDeps);
    }

    [Fact]
    public void Causal_batch_decoder_does_not_scan_magic_inside_record_payload()
    {
        var n = new Node(Id(1), epoch: 1);
        var parent = Id(0x10);
        var child = Id(0x20);
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            Seq = 1,
            ChangeId = child,
            RecordId = Rid(1),
            OpKind = 0x01,
            Payload = new byte[] { 0x11, 0x43, 0x43, 0x44, 0x31, 0x22 },
            AuthorNodeId = n.NodeId.ToArray(),
            CausalDeps = new List<BatchId> { parent },
        };
        var batch = new Batch(1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128, new List<OutboxEntry> { entry });

        var wire = n.EncodeBatchPayloadCausal(batch);
        Assert.Equal(0, batch.Flags & CausalWire.FlagHasCausalTrailer);
        Assert.NotEqual(0, wire[7] & CausalWire.FlagHasCausalTrailer);
        var decoded = n.DecodeBatchPayloadCausal(wire);

        Assert.Equal(new byte[] { 0x11, 0x43, 0x43, 0x44, 0x31, 0x22 }, decoded.Entries[0].Payload.ToArray());
        Assert.Equal(parent, decoded.Entries[0].CausalDeps[0]);
    }

    [Fact]
    public void Causal_trailer_rejects_change_id_mismatch()
    {
        var entries = new List<OutboxEntry>
        {
            new() { ChangeId = Id(0x20), CausalDeps = new List<BatchId>() },
        };
        var trailer = CausalWire.EncodeTrailer(new[]
        {
            new OutboxEntry { ChangeId = Id(0x21), OriginAuthorEpoch = 1, OriginSequence = 1, CausalDeps = new List<BatchId>() },
        });
        var combined = new byte[1 + trailer.Length];
        combined[0] = 0xAA;
        trailer.CopyTo(combined, 1);

        Assert.Equal(combined.Length, CausalWire.TryApplyTrailer(combined, entries));
        Assert.Equal(Id(0x20), entries[0].ChangeId);
        Assert.Empty(entries[0].CausalDeps);
    }

    [Fact]
    public void DrainAndRetry_applies_when_parent_known()
    {
        var tracker = new CausalTracker();
        var n = new Node(Id(1), epoch: 1) { Causal = tracker };
        var parent = Id(0x10);
        var child = Id(0x20);

        // Minimal entry — ApplyBatchInternal may still fail on SEQ/payload;
        // we only assert quarantine + drain path invokes without CAUSAL_DEP_MISSING after parent.
        var entry = new OutboxEntry
        {
            NamespaceId = 1,
            ChangeId = child,
            OriginAuthorEpoch = 1, OriginSequence = 1,
            RecordId = Rid(1),
            Payload = Array.Empty<byte>(),
            AuthorNodeId = n.NodeId.ToArray(),
            CausalDeps = new List<BatchId> { parent },
        };
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Id(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry> { entry });

        Assert.Throws<SyncException>(() => n.ApplyBatchCausal(batch, senderPeer: Id(2)));
        Assert.Equal(1, tracker.PendingCount);

        n.NoteCausalApplied(parent);
        // Drain may or may not fully apply (SEQ gaps); must not leave pending forever after parent
        var released = n.DrainCausal();
        Assert.Contains(released, r => r.Outcome == CausalOutcome.Ready);
    }

    [Fact]
    public void NoteCausalApplied_marks_known()
    {
        var n = new Node(Id(1), epoch: 1) { Causal = new CausalTracker() };
        var cid = Id(0x55);
        Assert.False(n.Causal!.IsKnown(cid));
        n.NoteCausalApplied(cid);
        Assert.True(n.Causal.IsKnown(cid));
    }
}
