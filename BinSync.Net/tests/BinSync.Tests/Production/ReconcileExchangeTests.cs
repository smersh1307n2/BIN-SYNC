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
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
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
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
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
}
