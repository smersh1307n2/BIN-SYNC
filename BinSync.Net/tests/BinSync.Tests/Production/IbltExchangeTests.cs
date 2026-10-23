// TCP IBLT exchange + ReconcileViaIblt end-to-end.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class IbltExchangeTests
{
    [Fact]
    public async Task Tcp_ExchangeIblt_finds_symmetric_diff()
    {
        var a = Fix.MakeNode(80);
        var b = Fix.MakeNode(81);
        var ridA = Fix.Rid(100);
        var ridB = Fix.Rid(101);
        a.WriteField(1, ridA, 0x100, new byte[] { 1 });
        b.WriteField(1, ridB, 0x100, new byte[] { 2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.ExchangeIbltAsync(b, 1, initiate: false, cellCount: 32);
        var (onlyLocal, onlyRemote) = await client.ExchangeIbltAsync(a, 1, initiate: true, cellCount: 32);
        await serverTask;

        Assert.Contains(onlyLocal, x => Fix.Eq(Iblt.NormalizeKey(ridA), Iblt.NormalizeKey(x)));
        Assert.Contains(onlyRemote, x => Fix.Eq(Iblt.NormalizeKey(ridB), Iblt.NormalizeKey(x)));
    }

    [Fact]
    public async Task Tcp_ReconcileViaIblt_fills_missing_record()
    {
        var a = Fix.MakeNode(82);
        var b = Fix.MakeNode(83);
        var ridB = Fix.Rid(102);
        b.WriteField(1, ridB, 0x100, new byte[] { 9 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.ReconcileViaIbltAsync(b, 1, initiate: false, cellCount: 32);
        var (onlyL, onlyR, fields) = await client.ReconcileViaIbltAsync(a, 1, initiate: true, cellCount: 32);
        await serverTask;

        Assert.True(onlyR >= 1);
        Assert.True(fields >= 1);
        Assert.True(a.Storage.ContainsKey(1) && a.Storage[1].ContainsKey(ridB));
        Assert.Equal(new byte[] { 9 }, a.Storage[1][ridB].State[0x100].Value);
    }
}
