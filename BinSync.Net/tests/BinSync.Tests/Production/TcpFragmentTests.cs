// FLAG_FRAGMENT over TCP session.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TcpFragmentTests
{
    [Fact]
    public async Task Tcp_fragment_roundtrip()
    {
        var a = Fix.MakeNode(920);
        var b = Fix.MakeNode(921);
        var body = new byte[400];
        for (int i = 0; i < body.Length; i++) body[i] = (byte)(i * 3);

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var send = client.SendFragmentedAsync(0x09, 1, 1, body, maxBody: 80);
        var got = await server.ReceiveReassembledAsync();
        await send;
        Assert.Equal(body, got);
    }
}
