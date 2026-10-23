// NEED_RANGE fulfill → DELTA_BATCH → LWW convergence over TCP.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ReconcileFulfillTests
{
    [Fact]
    public async Task NeedRange_fulfill_pushes_and_receiver_applies()
    {
        // Divergent offline writes for the same record; B has the later wall clock.
        var a = Fix.MakeNode(41);
        var b = Fix.MakeNode(42);
        var rid = Fix.Rid(50);
        a.WriteField(1, rid, 0x100, new byte[] { 1 });
        b.WriteField(1, rid, 0x100, new byte[] { 2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // Discover divergence
        var tEx = server.ExchangeStateDigestAsync(b, 1, initiate: false);
        var (_, _, divA) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        await tEx;
        Assert.NotEmpty(divA);

        // A asks for the divergent prefix; B republishes and pushes; A receives
        var fulfill = Task.Run(async () =>
        {
            var need = await server.ReceiveNeedRangeAsync();
            return await server.FulfillNeedRangeAsync(b, need);
        });
        await client.SendNeedRangeAsync(1, divA[0]);
        var recv = client.ReceiveBatchAsync(a);
        var pushed = await fulfill;
        Assert.True(pushed >= 1);
        await recv;

        // LWW: B's republished value (new HLC on B) should win on A
        Assert.True(a.Storage.ContainsKey(1));
        Assert.True(a.Storage[1].ContainsKey(rid));
        var val = a.Storage[1][rid].State[0x100].Value;
        Assert.Equal(new byte[] { 2 }, val);
    }

    [Fact]
    public async Task NeedRange_reply_NeedKeys_lists_local_records()
    {
        var a = Fix.MakeNode(43);
        var b = Fix.MakeNode(44);
        var rid = Fix.Rid(60);
        b.WriteField(1, rid, 0x100, new byte[] { 9 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var key = BucketAccumulator.BucketKey(rid);
        var prefix = key[..1];

        var reply = Task.Run(async () =>
        {
            var need = await server.ReceiveNeedRangeAsync();
            var ids = Reconcile.LocalKeysUnderPrefix(b, need.NamespaceId, need.Prefix);
            await server.SendNeedKeysAsync(new NeedKeysMessage
            {
                NamespaceId = need.NamespaceId,
                RecordIds = ids,
            });
            return ids;
        });
        await client.SendNeedRangeAsync(1, prefix);
        var keys = await client.ReceiveNeedKeysAsync();
        var listed = await reply;
        Assert.Contains(keys.RecordIds, id => Fix.Eq(id, rid));
        Assert.NotEmpty(listed);
    }
}
