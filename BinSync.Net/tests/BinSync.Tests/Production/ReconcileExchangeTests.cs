// TCP loopback: STATE_DIGEST exchange + NEED_RANGE after divergence.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ReconcileExchangeTests
{
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

    private sealed class FailOnSecondStateDigestTransport : ISyncTransport
    {
        private readonly PipeTransport _inner;
        private int _stateDigestSends;

        public FailOnSecondStateDigestTransport(PipeTransport inner) => _inner = inner;

        public NodeId PeerNodeId => _inner.PeerNodeId;
        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => _inner.MaxPacketSize;

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
