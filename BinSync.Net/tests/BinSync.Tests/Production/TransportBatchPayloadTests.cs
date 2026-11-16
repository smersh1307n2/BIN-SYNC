using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TransportBatchPayloadTests
{
    [Fact]
    public void Delta_entries_keep_independent_mutable_causal_dependency_lists()
    {
        var sender = Fix.MakeNode(215);
        var receiver = Fix.MakeNode(216);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(9, Fix.Rid(0x73), 0x100, new byte[] { 1 }));
        Assert.True(sender.WriteField(9, Fix.Rid(0x73), 0x101, new byte[] { 2 }));

        var batch = sender.CreateDeltaBatch(receiver.NodeId, 9)!;
        Assert.Equal(2, batch.Entries.Count);

        batch.Entries[0].CausalDeps.Add(BatchId.From(Guid.NewGuid().ToByteArray()));
        Assert.Single(batch.Entries[0].CausalDeps);
        Assert.Empty(batch.Entries[1].CausalDeps);

        var ordinaryEntry = new OutboxEntry();
        Assert.NotNull(ordinaryEntry.CausalDeps);
        ordinaryEntry.CausalDeps.Add(BatchId.From(Guid.NewGuid().ToByteArray()));
        Assert.Single(ordinaryEntry.CausalDeps);
    }

    [Fact]
    public void Transport_payload_matches_public_batch_wire_bytes_and_ack_metadata()
    {
        var sender = Fix.MakeNode(211);
        var receiver = Fix.MakeNode(212);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(7, Fix.Rid(0x71), 0x100, new byte[] { 0x10, 0x20, 0x30 }));
        Assert.True(sender.WriteField(7, Fix.Rid(0x71), 0x101, new byte[] { 0x40, 0x50 }));

        // Establish the public API's canonical encoding first. The transport path
        // must produce exactly the same bytes for the same stream/cursor span.
        var publicBatch = sender.CreateDeltaBatch(receiver.NodeId, 7)!;
        var expectedPayload = sender.EncodeBatchPayload(publicBatch);

        var outbound = sender.CreateDeltaBatchPayloadForTransport(receiver.NodeId, 7);
        Assert.NotNull(outbound);
        Assert.Equal(expectedPayload, outbound!.Value.Payload);
        Assert.Equal(publicBatch.BatchId, outbound.Value.BatchId);
        Assert.Equal(publicBatch.AuthorStreamId(), outbound.Value.AuthorStreamId);
        Assert.Equal(publicBatch.DurableAuthor, outbound.Value.DurableAuthor);
        Assert.Equal(publicBatch.NamespaceId, outbound.Value.NamespaceId);
        Assert.Equal(publicBatch.IssuerEpoch, outbound.Value.IssuerEpoch);
        Assert.Equal(publicBatch.CursorStart, outbound.Value.CursorStart);
        Assert.Equal(publicBatch.CursorEnd, outbound.Value.CursorEnd);
        Assert.Equal(publicBatch.BatchDigest, outbound.Value.BatchDigest);

        // The transport-only metadata must drive the exact same pushed watermark
        // and ACK compaction behavior as the public Batch path.
        sender.AdvanceStreamForTransport(receiver.NodeId, 7, outbound.Value);
        sender.ProcessAck(receiver.NodeId, 7, outbound.Value.CursorEnd,
            authorStream: outbound.Value.DurableAuthor);

        Assert.Equal(0, sender.OutboxSize(7));
        Assert.True(sender.PeerStates[(receiver.NodeId, 7)].TryGetStreamPushed(outbound.Value.AuthorStreamId, out var pushed));
        Assert.Equal(outbound.Value.CursorEnd, pushed);
        Assert.Equal(outbound.Value.CursorEnd,
            sender.PeerStates[(receiver.NodeId, 7)].AckedFor(outbound.Value.AuthorStreamId));
    }

    [Fact]
    public void Transport_payload_is_independent_of_outbox_after_encoding()
    {
        var sender = Fix.MakeNode(213);
        var receiver = Fix.MakeNode(214);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(8, Fix.Rid(0x72), 0x100, new byte[] { 1, 2, 3, 4 }));
        var outbound = sender.CreateDeltaBatchPayloadForTransport(receiver.NodeId, 8);
        Assert.NotNull(outbound);

        var expected = outbound!.Value.Payload.ToArray();
        sender.Outbox[8][0].Payload = new byte[] { 9, 9, 9 };

        Assert.Equal(expected, outbound.Value.Payload);
    }
    [Fact]
    public void Apply_batch_payload_detaches_relayed_payload_from_caller_buffer()
    {
        var sender = Fix.MakeNode(217);
        var receiver = Fix.MakeNode(218);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(9, Fix.Rid(0x73), 0x100, new byte[] { 1, 2, 3 }));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 9)!;
        var payload = sender.EncodeBatchPayload(batch);

        var result = receiver.ApplyBatchPayload(payload, sender.NodeId);
        Assert.Equal(1, result.Applied);
        var relayed = Assert.Single(receiver.Outbox[9]);
        var expected = relayed.Payload.ToArray();

        Array.Fill(payload, (byte)0xEE);

        Assert.Equal(expected, relayed.Payload.ToArray());
        Assert.IsType<OpFieldWrite>(Ops.DecodeOp(relayed.Payload).Op);
    }

}
