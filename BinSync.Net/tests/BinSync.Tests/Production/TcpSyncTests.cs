// TCP loopback: handshake + one DELTA_BATCH over a real socket.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TcpSyncTests
{
    [Fact]
    public async Task Loopback_Handshake_Then_One_Delta()
    {
        var a = Fix.MakeNode(11);
        var b = Fix.MakeNode(12);
        // Third party known only to the acceptor must survive the merge.
        b.DurableAuthors[Fix.Nid(99)] = 4;

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        Assert.True(Fix.Eq(b.NodeId, client.PeerNodeId));
        Assert.True(Fix.Eq(a.NodeId, server.PeerNodeId));
        Assert.Equal(1, a.AliasOf(b.NodeId, a.NodeId));
        Assert.Equal(2, a.AliasOf(b.NodeId, b.NodeId));
        Assert.Equal(1, b.AliasOf(a.NodeId, a.NodeId));
        Assert.Equal(2, b.AliasOf(a.NodeId, b.NodeId));
        Assert.NotNull(a.AliasOf(b.NodeId, Fix.Nid(99)));
        Assert.Equal(a.AliasOf(b.NodeId, Fix.Nid(99)), b.AliasOf(a.NodeId, Fix.Nid(99)));

        a.WriteField(1, Fix.Rid(10), 0x100, new byte[] { 9, 9 });
        var recv = server.ReceiveBatchAsync(b);
        Assert.True(await client.PushBatchAsync(a, 1));
        var result = await recv;
        Assert.True(result.Applied >= 1);
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(10)));
    }
}
