// P0 production foundation: session wire, pipe transport, frame stream, bounds.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TransportFoundationTests
{
    [Fact]
    public void SessionWire_Hello_Roundtrips()
    {
        var id = Fix.Nid(7);
        var frame = SessionWire.EncodeHello(new HelloPayload(id, 3, SessionCapabilities.FlowControl));
        var (f, _) = FrameCodec.ParseFrame(frame);
        Assert.Equal(SessionWire.TypeHello, f.FrameType);
        Assert.Equal((uint)0, f.StreamId);
        var h = SessionWire.DecodeHello(frame);
        Assert.True(Fix.Eq(id, h.NodeId));
        Assert.Equal(3, h.Epoch);
        Assert.Equal(SessionCapabilities.FlowControl, h.Capabilities);
    }

    [Fact]
    public void SessionWire_AliasMap_Roundtrips()
    {
        var entries = new List<AliasMapEntry>
        {
            new(1, Fix.Nid(1), 1),
            new(2, Fix.Nid(2), 5),
        };
        var frame = SessionWire.EncodeAliasMap(entries);
        var got = SessionWire.DecodeAliasMap(frame);
        Assert.Equal(2, got.Count);
        Assert.Equal((ushort)1, got[0].Alias);
        Assert.True(Fix.Eq(Fix.Nid(2), got[1].NodeId));
        Assert.Equal(5, got[1].Epoch);
    }


    [Fact]
    public void SessionWire_Hello_decoders_reject_wrong_type_stream_flags_and_trailing_bytes()
    {
        var hello = SessionWire.EncodeHello(new HelloPayload(Fix.Nid(11), 2, SessionCapabilities.None));
        var ack = SessionWire.EncodeHelloAck(new HelloPayload(Fix.Nid(12), 3, SessionCapabilities.None));
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(ack));
        Assert.Throws<FrameException>(() => SessionWire.DecodeHelloAck(hello));

        var parsed = FrameCodec.ParseFrame(hello).Frame;
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(
            FrameCodec.BuildFrame(SessionWire.TypeHello, 7, parsed.FrameSeq, parsed.Payload)));
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(
            FrameCodec.BuildFrame(SessionWire.TypeHello, 0, parsed.FrameSeq, parsed.Payload, flags: Frames.FLAG_SIGNED)));
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(
            FrameCodec.BuildFrame(SessionWire.TypeHello, 0, parsed.FrameSeq, parsed.Payload.Concat(new byte[] { 0 }).ToArray())));
    }

    [Fact]
    public void SessionWire_AliasMap_rejects_duplicate_alias_node_and_trailing_bytes()
    {
        var id1 = Fix.Nid(21);
        var id2 = Fix.Nid(22);
        var duplicateAlias = new List<AliasMapEntry> { new(1, id1, 1), new(1, id2, 1) };
        var duplicateNode = new List<AliasMapEntry> { new(1, id1, 1), new(2, id1, 2) };

        Assert.Throws<FrameException>(() => SessionWire.DecodeAliasMap(SessionWire.EncodeAliasMap(duplicateAlias)));
        Assert.Throws<FrameException>(() => SessionWire.DecodeAliasMap(SessionWire.EncodeAliasMap(duplicateNode)));

        var valid = SessionWire.EncodeAliasMap(new List<AliasMapEntry> { new(1, id1, 1) });
        var parsed = FrameCodec.ParseFrame(valid).Frame;
        Assert.Throws<FrameException>(() => SessionWire.DecodeAliasMap(
            FrameCodec.BuildFrame(SessionWire.TypeAliasMap, 0, parsed.FrameSeq,
                parsed.Payload.Concat(new byte[] { 0 }).ToArray())));
        Assert.Throws<FrameException>(() => SessionWire.DecodeAliasMap(
            FrameCodec.BuildFrame(SessionWire.TypeAliasMap, 1, parsed.FrameSeq, parsed.Payload)));
    }


    [Fact]
    public void MergeAliasMap_rejects_endpoint_epoch_mismatch_from_peer()
    {
        using var local = new Node(Fix.Nid(31));
        var peerId = Fix.Nid(32);
        const int peerEpoch = 5;

        var staleLocal = new List<AliasMapEntry>
        {
            new(1, peerId, peerEpoch),
            new(2, local.NodeId, local.Epoch + 1),
        };
        var stalePeer = new List<AliasMapEntry>
        {
            new(1, peerId, peerEpoch - 1),
            new(2, local.NodeId, local.Epoch),
        };

        Assert.Throws<FrameException>(() =>
            SyncSession.MergeAliasMap(local, peerId, peerEpoch, staleLocal));
        Assert.Throws<FrameException>(() =>
            SyncSession.MergeAliasMap(local, peerId, peerEpoch, stalePeer));
    }

    [Fact]
    public async Task GenericSyncSession_UsesPipeTransport_WithoutTcpHost()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        var (ta, tb) = PipeTransport.CreatePair();

        var acceptTask = SyncSession.AcceptAsync(b, tb);
        var connectTask = SyncSession.ConnectAsync(a, ta);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        Assert.Equal(b.NodeId, initiator.PeerNodeId);
        Assert.Equal(a.NodeId, acceptor.PeerNodeId);
        Assert.NotNull(a.SessionEpoch(b.NodeId, 0));
        Assert.NotNull(b.SessionEpoch(a.NodeId, 0));
        Assert.Equal(1, a.AliasOf(b.NodeId, a.NodeId));

        a.WriteField(1, Fix.Rid(10), 0x100, new byte[] { 1, 2, 3 });
        var receiveTask = acceptor.ReceiveBatchAsync(b);
        Assert.True(await initiator.PushBatchAsync(a, 1));
        var received = await receiveTask;
        Assert.Equal(1, received.Applied);
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(10)));
    }


    [Fact]
    public async Task Same_peer_sessions_are_serialized_until_the_active_session_is_disposed()
    {
        using var low = new Node(Fix.Nid(1));
        using var high = new Node(Fix.Nid(2));
        var (low1, high1) = PipeTransport.CreatePair();
        var accept1Task = SyncSession.AcceptAsync(high, high1);
        var connect1Task = SyncSession.ConnectAsync(low, low1);
        var accept1 = await accept1Task;
        var connect1 = await connect1Task;

        var (low2, high2) = PipeTransport.CreatePair();
        var accept2Task = SyncSession.AcceptAsync(high, high2);
        var connect2Task = SyncSession.ConnectAsync(low, low2);

        await Task.Delay(100);
        Assert.False(accept2Task.IsCompleted);
        Assert.False(connect2Task.IsCompleted);

        await connect1.DisposeAsync();
        await accept1.DisposeAsync();

        var accept2 = await accept2Task.WaitAsync(TimeSpan.FromSeconds(3));
        var connect2 = await connect2Task.WaitAsync(TimeSpan.FromSeconds(3));
        await connect2.DisposeAsync();
        await accept2.DisposeAsync();
    }

    [Fact]
    public async Task Handshake_roles_are_deterministic_from_node_id_order()
    {
        using var low = new Node(Fix.Nid(1));
        using var high = new Node(Fix.Nid(2));
        var (outbound, inbound) = PipeTransport.CreatePair();

        var wrongAcceptor = SyncSession.AcceptAsync(low, inbound);
        var wrongInitiator = SyncSession.ConnectAsync(high, outbound);

        await Assert.ThrowsAsync<FrameException>(() => wrongAcceptor);
        await Assert.ThrowsAnyAsync<Exception>(() => wrongInitiator);
    }

    [Fact]
    public async Task FrameStream_MemoryStream_Roundtrip()
    {
        var payload = new Writer().U32(0xAABBCCDDu).Value();
        var frame = FrameCodec.BuildFrame(0x09, 0x0001_0001, 42, payload);
        using var ms = new MemoryStream();
        await FrameStream.WriteFrameAsync(ms, frame);
        ms.Position = 0;
        var got = await FrameStream.ReadFrameAsync(ms);
        Assert.NotNull(got);
        Assert.Equal(frame, got);
        var eof = await FrameStream.ReadFrameAsync(ms);
        Assert.Null(eof);
    }

    [Fact]
    public void ResourceLimits_Blocks_When_Outbox_Full()
    {
        var n = Fix.MakeNode(3);
        var lim = new ResourceLimits { MaxOutboxEntriesPerNamespace = 2 };
        Assert.True(BoundedNode.WriteField(n, lim, 1, Fix.Rid(1), 0x100, new byte[] { 1 }));
        Assert.True(BoundedNode.WriteField(n, lim, 1, Fix.Rid(2), 0x100, new byte[] { 2 }));
        Assert.Equal(2, n.OutboxSize(1));
        Assert.Throws<SyncException>(() =>
            BoundedNode.WriteField(n, lim, 1, Fix.Rid(3), 0x100, new byte[] { 3 }));
    }

    [Fact]
    public async Task StreamSyncTransport_Over_MemoryStream_Roundtrip()
    {
        using var ms = new MemoryStream();
        await using var t = new StreamSyncTransport(ms, ownsStream: false);
        var frame = SessionWire.EncodeHello(new HelloPayload(Fix.Nid(9), 1, SessionCapabilities.None));
        await t.SendAsync(frame);
        ms.Position = 0;
        await using var t2 = new StreamSyncTransport(ms, ownsStream: false);
        var got = await t2.ReceiveAsync();
        Assert.NotNull(got);
        var h = SessionWire.DecodeHello(got!);
        Assert.True(Fix.Eq(Fix.Nid(9), h.NodeId));
    }
}
