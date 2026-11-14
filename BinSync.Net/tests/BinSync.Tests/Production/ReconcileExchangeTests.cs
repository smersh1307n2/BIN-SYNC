// TCP loopback: STATE_DIGEST exchange + NEED_RANGE after divergence.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ReconcileExchangeTests
{

    [Fact]
    public async Task ExchangeStateDigest_rejects_frame_on_wrong_namespace_stream()
    {
        var a = Fix.MakeNode(71);
        var b = Fix.MakeNode(72);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var capabilities = SessionCapabilities.FlowControl;
        var acceptTask = SyncSession.AcceptAsync(b, bPipe, capabilities);
        await using var client = await SyncSession.ConnectAsync(a, aPipe, capabilities);
        await using var server = await acceptTask;

        var peerDigest = Reconcile.EncodeStateDigest(Reconcile.BuildStateDigest(b, 1));
        var wrongStreamFrame = FrameCodec.BuildFrame(
            ReconcileFrames.StateDigest, 0x0001_0002u, 10ul, peerDigest);
        await bPipe.SendAsync(wrongStreamFrame);

        await Assert.ThrowsAsync<FrameException>(async () =>
            await client.ExchangeStateDigestAsync(a, 1, initiate: false));
    }

    [Fact]
    public async Task ReceiveNeedRange_rejects_frame_on_wrong_namespace_stream()
    {
        var a = Fix.MakeNode(73);
        var b = Fix.MakeNode(74);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var payload = Reconcile.EncodeNeedRange(new NeedRangeMessage
        {
            NamespaceId = 1,
            Prefix = new byte[] { 0x12 }
        });
        await bPipe.SendAsync(FrameCodec.BuildFrame(
            ReconcileFrames.NeedRange, 0x0001_0002u, 11ul, payload));

        await Assert.ThrowsAsync<FrameException>(async () =>
            await client.ReceiveNeedRangeAsync());
    }

    [Fact]
    public async Task NeedKeys_roundtrip_preserves_namespace_and_keys()
    {
        var a = Fix.MakeNode(75);
        var b = Fix.MakeNode(76);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var expected = new NeedKeysMessage
        {
            NamespaceId = 7,
            RecordIds = new List<byte[]> { Fix.Rid(750), Fix.Rid(751) }
        };
        var receive = server.ReceiveNeedKeysAsync();
        await client.SendNeedKeysAsync(expected);
        var actual = await receive;

        Assert.Equal(expected.NamespaceId, actual.NamespaceId);
        Assert.Equal(expected.RecordIds.Count, actual.RecordIds.Count);
        for (int i = 0; i < expected.RecordIds.Count; i++)
            Assert.Equal(expected.RecordIds[i], actual.RecordIds[i]);
    }

    [Fact]
    public async Task NeedKeys_exchange_can_run_during_auto_reconcile()
    {
        var a = Fix.MakeNode(77);
        var b = Fix.MakeNode(78);
        a.WriteField(1, Fix.Rid(770), 0x100, new byte[] { 1 });
        b.WriteField(1, Fix.Rid(780), 0x100, new byte[] { 2 });

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var autoA = client.AutoReconcileAsync(a, 1, initiate: true);
        var autoB = server.AutoReconcileAsync(b, 1, initiate: false);

        var expected = new NeedKeysMessage
        {
            NamespaceId = 1,
            RecordIds = new List<byte[]> { Fix.Rid(771) }
        };
        var receiveKeys = server.ReceiveNeedKeysAsync();
        await client.SendNeedKeysAsync(expected);
        var actual = await receiveKeys;

        Assert.Equal(expected.NamespaceId, actual.NamespaceId);
        Assert.Equal(expected.RecordIds.Count, actual.RecordIds.Count);
        Assert.Equal(expected.RecordIds[0], actual.RecordIds[0]);

        await Task.WhenAll(autoA, autoB);
    }

    [Fact]
    public async Task Reconciliation_operations_cannot_escape_scope()
    {
        var a = Fix.MakeNode(79);
        var b = Fix.MakeNode(80);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        SyncSession.ReconciliationOperations? escaped = null;
        await client.RunReconciliationAsync((ops, _) =>
        {
            escaped = ops;
            return Task.FromResult(true);
        });

        Assert.NotNull(escaped);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await escaped!.ReceiveNeedKeysAsync());
    }

    [Fact]
    public async Task RunReconciliation_releases_all_gates_when_callback_is_cancelled()
    {
        var a = Fix.MakeNode(85);
        var b = Fix.MakeNode(86);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var operation = client.RunReconciliationAsync(async (_, token) =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.Infinite, token);
            return true;
        }, cts.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);

        var expected = new NeedKeysMessage
        {
            NamespaceId = 1,
            RecordIds = new List<byte[]> { Fix.Rid(850) }
        };
        var receive = server.ReceiveNeedKeysAsync();
        await client.SendNeedKeysAsync(expected);
        var actual = await receive.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expected.RecordIds[0], actual.RecordIds[0]);
    }

    [Fact]
    public async Task AutoReconcile_runs_concurrently_with_full_duplex_batch_exchange()
    {
        using var a = Fix.MakeNode(81);
        using var b = Fix.MakeNode(82);
        a.WriteField(1, Fix.Rid(810), 0x100, new byte[] { 1 });
        b.WriteField(1, Fix.Rid(820), 0x100, new byte[] { 2 });
        a.WriteField(2, Fix.Rid(811), 0x100, new byte[] { 3 });
        b.WriteField(2, Fix.Rid(821), 0x100, new byte[] { 4 });

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var autoA = client.AutoReconcileAsync(a, 1, initiate: true);
        var autoB = server.AutoReconcileAsync(b, 1, initiate: false);

        var pushA = client.PushBatchAsync(a, 2);
        var receiveA = client.ReceiveBatchAsync(a);
        var pushB = server.PushBatchAsync(b, 2);
        var receiveB = server.ReceiveBatchAsync(b);

        await Task.WhenAll(autoA, autoB, pushA, receiveA, pushB, receiveB)
            .WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(await pushA);
        Assert.True(await pushB);
        Assert.True(b.TryGetRecord(2, Fix.Rid(811), out _));
        Assert.True(a.TryGetRecord(2, Fix.Rid(821), out _));
        Assert.True(a.TryGetRecord(1, Fix.Rid(820), out _));
        Assert.True(b.TryGetRecord(1, Fix.Rid(810), out _));
    }

    [Fact]
    public async Task RunReconciliation_releases_all_gates_when_callback_throws()
    {
        var a = Fix.MakeNode(83);
        var b = Fix.MakeNode(84);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.RunReconciliationAsync<int>((_, _) =>
                Task.FromException<int>(new InvalidOperationException("Injected callback failure"))));

        var expected = new NeedKeysMessage
        {
            NamespaceId = 1,
            RecordIds = new List<byte[]> { Fix.Rid(830) }
        };
        var receive = server.ReceiveNeedKeysAsync();
        await client.SendNeedKeysAsync(expected);
        var actual = await receive.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expected.RecordIds[0], actual.RecordIds[0]);
    }

    [Fact]
    public async Task Tcp_Exchange_StateDigest_converged()
    {
        var a = Fix.MakeNode(31);
        var b = Fix.MakeNode(32);
        a.WriteField(1, Fix.Rid(1), 0x100, new byte[] { 5 });
        Transport.MakeSession(a, b);
        Transport.SyncRound(a, b, 1);

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var tServer = server.ExchangeStateDigestAsync(b, 1, initiate: false);
        var (la, ra, divA) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        var (lb, rb, divB) = await tServer;

        Assert.True(la.RootDigest.AsSpan().SequenceEqual(ra.RootDigest));
        Assert.Empty(divA);
        Assert.Empty(divB);
    }

    [Fact]
    public async Task Tcp_Exchange_StateDigest_divergent_then_NeedRange()
    {
        var a = Fix.MakeNode(33);
        var b = Fix.MakeNode(34);
        a.WriteField(1, Fix.Rid(7), 0x100, new byte[] { 1 });
        b.WriteField(1, Fix.Rid(7), 0x100, new byte[] { 2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var tServer = server.ExchangeStateDigestAsync(b, 1, initiate: false);
        var (la, ra, divA) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        var (_, _, divB) = await tServer;

        Assert.NotEmpty(divA);
        Assert.NotEmpty(divB);

        var recvNeed = server.ReceiveNeedRangeAsync();
        await client.SendNeedRangeAsync(1, divA[0]);
        var need = await recvNeed;
        Assert.Equal(1, need.NamespaceId);
        Assert.Equal(divA[0], need.Prefix);
    }

    [Fact]
    public async Task Concurrent_AutoReconcile_calls_are_serialized_per_session()
    {
        var a = Fix.MakeNode(45);
        var b = Fix.MakeNode(46);
        a.WriteField(1, Fix.Rid(450), 0x100, new byte[] { 1 });
        b.WriteField(1, Fix.Rid(460), 0x100, new byte[] { 2 });

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        // Invoke in the same order on both peers. Each session must keep the two
        // directional rounds of one operation contiguous before starting the next.
        var a1 = client.AutoReconcileAsync(a, 1, initiate: true);
        var a2 = client.AutoReconcileAsync(a, 1, initiate: true);
        var b1 = server.AutoReconcileAsync(b, 1, initiate: false);
        var b2 = server.AutoReconcileAsync(b, 1, initiate: false);

        await Task.WhenAll(a1, a2, b1, b2);
        Assert.True(a.PeerStates[(b.NodeId, 1)].InitialSyncComplete);
        Assert.True(b.PeerStates[(a.NodeId, 1)].InitialSyncComplete);
    }

    [Fact]
    public async Task AutoReconcile_does_not_commit_baseline_when_second_direction_fails()
    {
        var a = Fix.MakeNode(41);
        var b = Fix.MakeNode(42);
        var aRecord = Fix.Rid(410);
        var bRecord = Fix.Rid(420);
        a.WriteField(1, aRecord, 0x100, new byte[] { 1 });
        b.WriteField(1, bRecord, 0x100, new byte[] { 2 });

        // Seed explicit incomplete peer states so the test verifies they are not
        // accidentally promoted after only the first directional round.
        var aPeerState = new PeerState(b.NodeId, 1);
        var bPeerState = new PeerState(a.NodeId, 1);
        a.PeerStates[(b.NodeId, 1)] = aPeerState;
        b.PeerStates[(a.NodeId, 1)] = bPeerState;

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, new FailOnSecondStateDigestTransport(bPipe));
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        // Round one completes. The second outgoing STATE_DIGEST from B is
        // deliberately failed, i.e. the reverse reconciliation round cannot start.
        var aTask = client.AutoReconcileAsync(a, 1, initiate: true);
        var bTask = server.AutoReconcileAsync(b, 1, initiate: false);
        await Assert.ThrowsAnyAsync<IOException>(async () => await Task.WhenAll(aTask, bTask));

        Assert.False(aPeerState.InitialSyncComplete);
        Assert.False(bPeerState.InitialSyncComplete);
        Assert.Same(aPeerState, a.PeerStates[(b.NodeId, 1)]);
        Assert.Same(bPeerState, b.PeerStates[(a.NodeId, 1)]);
    }

    [Fact]
    public async Task AutoReconcile_commits_baseline_when_divergence_exactly_matches_limit()
    {
        var a = Fix.MakeNode(51);
        var b = Fix.MakeNode(52);
        var sharedRecord = Fix.Rid(510);
        a.WriteField(1, sharedRecord, 0x100, new byte[] { 1 });
        b.WriteField(1, sharedRecord, 0x100, new byte[] { 2 });

        var aPeerState = new PeerState(b.NodeId, 1);
        var bPeerState = new PeerState(a.NodeId, 1);
        a.PeerStates[(b.NodeId, 1)] = aPeerState;
        b.PeerStates[(a.NodeId, 1)] = bPeerState;

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var aTask = client.AutoReconcileAsync(a, 1, initiate: true, maxPrefixes: 1);
        var bTask = server.AutoReconcileAsync(b, 1, initiate: false, maxPrefixes: 1);
        await Task.WhenAll(aTask, bTask);

        Assert.True(aPeerState.InitialSyncComplete);
        Assert.True(bPeerState.InitialSyncComplete);
        Assert.Same(aPeerState, a.PeerStates[(b.NodeId, 1)]);
        Assert.Same(bPeerState, b.PeerStates[(a.NodeId, 1)]);
    }

    [Fact]
    public async Task AutoReconcile_rejects_duplicate_need_range_prefix()
    {
        var a = Fix.MakeNode(61);
        var b = Fix.MakeNode(62);
        var rid = Fix.Rid(610);
        a.WriteField(1, rid, 0x100, new byte[] { 1 });
        b.WriteField(1, rid, 0x100, new byte[] { 2 });

        var bPeerState = new PeerState(a.NodeId, 1);
        b.PeerStates[(a.NodeId, 1)] = bPeerState;

        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await acceptTask;

        var serverTask = server.AutoReconcileAsync(b, 1, initiate: false, maxPrefixes: 2);
        var (_, _, divergent) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        Assert.Single(divergent);

        // Fulfill the one legitimate request, then repeat it instead of requesting
        // a different divergent prefix. The responder must reject the duplicate.
        await client.SendNeedRangeAsync(1, divergent[0]);
        await client.ReceiveSnapshotAsync(a);
        await client.SendNeedRangeAsync(1, divergent[0]);

        await Assert.ThrowsAsync<FormatException>(async () => await serverTask);
        Assert.False(bPeerState.InitialSyncComplete);
    }

    private sealed class FailOnSecondStateDigestTransport : ISyncTransport
    {
        private readonly PipeTransport _inner;
        private int _stateDigestSends;

        public FailOnSecondStateDigestTransport(PipeTransport inner) => _inner = inner;

        public NodeId PeerNodeId => _inner.PeerNodeId;
        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => null;

        public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
        {
            var frame = FrameCodec.ParseFrame(packet.ToArray()).Frame;
            if (frame.FrameType == ReconcileFrames.StateDigest &&
                Interlocked.Increment(ref _stateDigestSends) == 2)
            {
                await _inner.DisposeAsync();
                throw new IOException("Injected disconnect before second reconciliation direction");
            }

            await _inner.SendAsync(packet, ct);
        }

        public ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default) =>
            _inner.ReceiveAsync(ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

}
