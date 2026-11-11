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
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var unannouncedAuthor = NodeId.From(Fix.Nid(303));
        var payload = Membership.Encode(new MembershipMessage
        {
            NamespaceId = 1,
            Members = new List<MembershipEntry>
            {
                new() { NodeId = unannouncedAuthor, Epoch = 2, Flags = Membership.FlagActive }
            }
        });
        var receive = client.ReceiveMembershipAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(MembershipFrames.Membership,
            0x0001_0001u, 14, payload));

        await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.DoesNotContain(unannouncedAuthor, a.DurableAuthors.Keys);
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
    public async Task Lost_transport_ack_replays_same_batch_after_reconnect_without_reapplying()
    {
        using var sender = Fix.MakeNode(331);
        using var receiver = Fix.MakeNode(332);
        var record = Fix.Rid(333);
        byte[] value = [0x31, 0x32, 0x33];
        Assert.True(sender.WriteField(1, record, 0x100, value));

        // First connection: the receiver applies the batch, but its first ACK is
        // deliberately lost by closing the transport before the packet is delivered.
        var (firstSenderPipe, firstReceiverPipe) = PipeTransport.CreatePair();
        var droppingReceiver = new DropFirstAckTransport(firstReceiverPipe);
        var firstAccept = SyncSession.AcceptAsync(receiver, droppingReceiver);
        await using (var firstClient = await SyncSession.ConnectAsync(sender, firstSenderPipe))
        await using (var firstServer = await firstAccept)
        {
            Task<bool> push = firstClient.PushBatchAsync(sender, 1);
            Task<BatchResult> receive = firstServer.ReceiveBatchAsync(receiver);
            await Assert.ThrowsAnyAsync<IOException>(async () => await receive);
            await Assert.ThrowsAnyAsync<Exception>(async () => await push);
        }

        Assert.True(receiver.TryGetField(1, record, 0x100, out var afterFirstApply));
        Assert.Equal(value, afterFirstApply!.Value);
        Assert.Equal(0L, sender.PeerStates[(receiver.NodeId, 1)].AckedFor(sender.NodeId));

        // Reconnect with the same durable Nodes. Sender has no ACK watermark, so it
        // must replay the immutable batch. Receiver recognizes the already-applied
        // operation, returns an ACK, and does not apply it a second time.
        var (retrySenderPipe, retryReceiverPipe) = PipeTransport.CreatePair();
        var retryAccept = SyncSession.AcceptAsync(receiver, retryReceiverPipe);
        await using var retryClient = await SyncSession.ConnectAsync(sender, retrySenderPipe);
        await using var retryServer = await retryAccept;
        Task<bool> retryPush = retryClient.PushBatchAsync(sender, 1);
        Task<BatchResult> retryReceive = retryServer.ReceiveBatchAsync(receiver);
        Assert.True(await retryPush.WaitAsync(TimeSpan.FromSeconds(5)));
        BatchResult duplicate = await retryReceive.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, duplicate.Applied);
        Assert.True(receiver.TryGetField(1, record, 0x100, out var afterRetry));
        Assert.Equal(value, afterRetry!.Value);
        Assert.Equal(1L, sender.PeerStates[(receiver.NodeId, 1)].AckedFor(sender.NodeId));
    }

    private sealed class DropFirstAckTransport : IPeerAwareSyncTransport
    {
        private readonly ISyncTransport _inner;
        private int _drop = 1;
        private NodeId _peerNodeId;

        public DropFirstAckTransport(ISyncTransport inner) => _inner = inner;

        public NodeId PeerNodeId
        {
            get => _peerNodeId;
            set
            {
                _peerNodeId = value;
                if (_inner is IPeerAwareSyncTransport aware)
                    aware.PeerNodeId = value;
            }
        }

        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => _inner.MaxPacketSize;

        public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
        {
            var (frame, _) = FrameCodec.ParseFrame(packet.ToArray());
            if (frame.FrameType == 0x0B && Interlocked.Exchange(ref _drop, 0) == 1)
            {
                await _inner.DisposeAsync().ConfigureAwait(false);
                throw new IOException("Injected transport ACK loss.");
            }
            await _inner.SendAsync(packet, ct).ConfigureAwait(false);
        }

        public ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default) =>
            _inner.ReceiveAsync(ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    [Fact]
    public async Task Need_keys_rejects_frame_on_wrong_namespace_stream()
    {
        using var a = Fix.MakeNode(317);
        using var b = Fix.MakeNode(318);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var payload = Reconcile.EncodeNeedKeys(new NeedKeysMessage
        {
            NamespaceId = 1,
            RecordIds = new List<byte[]> { Fix.Rid(319) }
        });
        var receive = client.ReceiveNeedKeysAsync();
        await bPipe.SendAsync(FrameCodec.BuildFrame(ReconcileFrames.NeedKeys,
            0x0001_0002u, 12, payload));

        await Assert.ThrowsAsync<FrameException>(() => receive);
    }

    [Fact]
    public async Task Snapshot_rejects_chunk_on_wrong_namespace_stream_before_apply()
    {
        using var a = Fix.MakeNode(320);
        using var b = Fix.MakeNode(321);
        var recordId = Fix.Rid(322);
        Assert.True(b.WriteField(1, recordId, 0x100, new byte[] { 0x7A }));
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var chunk = Assert.Single(Reconcile.BuildSnapshotChunks(b, 1));
        var receive = client.ReceiveSnapshotAsync(a);
        await bPipe.SendAsync(FrameCodec.BuildFrame(ReconcileFrames.SnapshotChunk,
            0x0001_0002u, 13, Reconcile.EncodeSnapshotChunk(chunk)));

        await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.False(a.TryGetRecord(1, recordId, out _));
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
