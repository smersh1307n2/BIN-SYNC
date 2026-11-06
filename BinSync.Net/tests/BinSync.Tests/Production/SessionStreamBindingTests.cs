using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class SessionStreamBindingTests
{
    [Fact]
    public async Task Membership_rejects_namespace_stream()
    {
        using var a = Fix.MakeNode(301);
        using var b = Fix.MakeNode(302);
        b.DurableAuthors[Fix.Nid(303)] = 2;
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var payload = Membership.Encode(Membership.Build(b, 1));
        var receive = client.ReceiveMembershipAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(MembershipFrames.Membership,
            0x0001_0001u, 14, payload));

        await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.DoesNotContain(a.DurableAuthors.Keys, id => id == Fix.Nid(303));
    }

    [Fact]
    public async Task Gc_notice_rejects_namespace_stream()
    {
        using var a = Fix.MakeNode(304);
        using var b = Fix.MakeNode(305);
        b.Quarantined.Add(Fix.Nid(306));
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var payload = GcNotice.Encode(GcNotice.Build(b, 1));
        var receive = client.ReceiveGcNoticeAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(ReconcileFrames.GcNotice,
            0x0001_0001u, 16, payload));

        await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.DoesNotContain(a.Quarantined, id => id == Fix.Nid(306));
    }

    [Fact]
    public async Task Schema_map_rejects_namespace_stream()
    {
        using var a = Fix.MakeNode(307);
        using var b = Fix.MakeNode(308);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var receive = client.ReceiveSchemaMapAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(SchemaMap.FrameType,
            0x0001_0001u, 18, b.ExportSchemaMap()));

        await Assert.ThrowsAsync<FrameException>(() => receive);
    }

    [Fact]
    public async Task Delta_batch_rejects_wrong_namespace_stream_before_apply()
    {
        using var a = Fix.MakeNode(309);
        using var b = Fix.MakeNode(310);
        var recordId = Fix.Rid(311);
        Assert.True(b.WriteField(1, recordId, 0x100, new byte[] { 0xA5 }));
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var batch = b.CreateDeltaBatch(a.NodeId, 1)!;
        var payload = b.EncodeBatchPayload(batch);
        var receive = client.ReceiveBatchAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(0x09,
            0x0001_0002u, 1, payload));

        await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.False(a.TryGetRecord(1, recordId, out _));
    }

    [Fact]
    public async Task Push_batch_rejects_ack_on_wrong_namespace_stream()
    {
        using var a = Fix.MakeNode(312);
        using var b = Fix.MakeNode(313);
        Assert.True(a.WriteField(1, Fix.Rid(314), 0x100, new byte[] { 0x5A }));
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var push = client.PushBatchAsync(a, 1);
        var rawBatch = await bPipe.ReceiveAsync(CancellationToken.None);
        Assert.NotNull(rawBatch);
        var (batchFrame, _) = FrameCodec.ParseFrame(rawBatch!);
        var batch = b.DecodeBatchPayload(batchFrame.Payload);
        var ack = Transport.EncodeAck(batch.BatchId, batch.CursorEnd, batch.DurableAuthor);
        await bPipe.SendAsync(FrameCodec.BuildFrame(0x0B,
            0x0002_0001u, 2, ack));

        await Assert.ThrowsAsync<FrameException>(() => push);
        Assert.Equal(0, a.PeerStates[(b.NodeId, 1)].AckedFor(batch.AuthorStreamId()));
    }

    [Fact]
    public async Task Iblt_exchange_rejects_response_from_another_namespace_stream()
    {
        using var a = Fix.MakeNode(315);
        using var b = Fix.MakeNode(316);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var exchange = client.ExchangeIbltAsync(a, 1, initiate: true, cellCount: 16);
        var request = await bPipe.ReceiveAsync(CancellationToken.None);
        Assert.NotNull(request);
        var (requestFrame, _) = FrameCodec.ParseFrame(request!);
        Assert.Equal(ReconcileFrames.IbltRequest, requestFrame.FrameType);

        await bPipe.SendAsync(FrameCodec.BuildFrame(ReconcileFrames.IbltResponse,
            0x0001_0002u, 15, Iblt.Encode(new Iblt(16))));

        await Assert.ThrowsAsync<FrameException>(() => exchange);
    }
}
