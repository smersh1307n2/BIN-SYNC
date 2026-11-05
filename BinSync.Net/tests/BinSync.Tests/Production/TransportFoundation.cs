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
    public async Task Full_duplex_batch_exchange_dispatches_data_and_ack_to_the_correct_operations()
    {
        using var low = Fix.MakeNode(1);
        using var high = Fix.MakeNode(2);
        var (toLow, toHigh) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(high, toHigh);
        var connectTask = SyncSession.ConnectAsync(low, toLow);
        await using var lowSession = await connectTask;
        await using var highSession = await acceptTask;

        low.WriteField(1, Fix.Rid(101), 0x100, new byte[] { 1, 2, 3 });
        high.WriteField(1, Fix.Rid(202), 0x100, new byte[] { 4, 5, 6 });

        var lowPush = lowSession.PushBatchAsync(low, 1);
        var lowReceive = lowSession.ReceiveBatchAsync(low);
        var highPush = highSession.PushBatchAsync(high, 1);
        var highReceive = highSession.ReceiveBatchAsync(high);
        await Task.WhenAll(lowPush, lowReceive, highPush, highReceive).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(await lowPush);
        Assert.True(await highPush);
        Assert.True(high.Storage[1].ContainsKey(Fix.Rid(101)));
        Assert.True(low.Storage[1].ContainsKey(Fix.Rid(202)));
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
    public async Task Symmetric_establish_chooses_protocol_roles_independently_of_transport_side()
    {
        using var low = new Node(Fix.Nid(1));
        using var high = new Node(Fix.Nid(2));
        var (left, right) = PipeTransport.CreatePair();

        var leftTask = SyncSession.EstablishAsync(low, left);
        var rightTask = SyncSession.EstablishAsync(high, right);
        var leftSession = await leftTask.WaitAsync(TimeSpan.FromSeconds(3));
        var rightSession = await rightTask.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(high.NodeId, leftSession.PeerNodeId);
        Assert.Equal(low.NodeId, rightSession.PeerNodeId);
        await leftSession.DisposeAsync();
        await rightSession.DisposeAsync();
    }

    [Fact]
    public async Task Crossed_transport_connections_do_not_deadlock_or_publish_mixed_sessions()
    {
        using var low = new Node(Fix.Nid(1));
        using var high = new Node(Fix.Nid(2));
        var (low1, high1) = PipeTransport.CreatePair();
        var (low2, high2) = PipeTransport.CreatePair();

        var pair1 = Task.WhenAll(
            SyncSession.EstablishAsync(low, low1),
            SyncSession.EstablishAsync(high, high1));
        var pair2 = Task.WhenAll(
            SyncSession.EstablishAsync(low, low2),
            SyncSession.EstablishAsync(high, high2));

        var first = await Task.WhenAny(pair1, pair2).WaitAsync(TimeSpan.FromSeconds(3));
        var firstSessions = await first;
        var second = ReferenceEquals(first, pair1) ? pair2 : pair1;
        Assert.False(second.IsCompleted);

        await firstSessions[0].DisposeAsync();
        await firstSessions[1].DisposeAsync();

        var secondSessions = await second.WaitAsync(TimeSpan.FromSeconds(3));
        await secondSessions[0].DisposeAsync();
        await secondSessions[1].DisposeAsync();
    }

    [Fact]
    public async Task Tcp_adapter_uses_symmetric_role_negotiation_when_higher_node_opens_socket()
    {
        using var low = new Node(Fix.Nid(1));
        using var high = new Node(Fix.Nid(2));
        await using var listener = TcpSyncListener.Start();
        var acceptTask = listener.AcceptAsync(low);
        var connectTask = TcpSync.ConnectAsync(high, "127.0.0.1", listener.Port);

        var accepted = await acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
        var connected = await connectTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(high.NodeId, accepted.PeerNodeId);
        Assert.Equal(low.NodeId, connected.PeerNodeId);
        await accepted.DisposeAsync();
        await connected.DisposeAsync();
    }


    [Fact]
    public async Task In_process_handshake_negotiates_the_union_of_both_durable_author_registries()
    {
        using var low = Fix.MakeNode(1);
        using var high = Fix.MakeNode(2);
        var lowOnly = Fix.Nid(30);
        var highOnly = Fix.Nid(40);
        low.RegisterAlias(high.NodeId, 3, lowOnly, 1);
        high.RegisterAlias(low.NodeId, 3, highOnly, 2);
        var (toLow, toHigh) = PipeTransport.CreatePair();

        await SessionWire.HandshakeAsync(low, toLow, high, toHigh);

        Assert.NotNull(low.AliasOf(high.NodeId, highOnly));
        Assert.NotNull(high.AliasOf(low.NodeId, lowOnly));
        Assert.Equal(low.AliasOf(high.NodeId, lowOnly), high.AliasOf(low.NodeId, lowOnly));
        Assert.Equal(low.AliasOf(high.NodeId, highOnly), high.AliasOf(low.NodeId, highOnly));
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
