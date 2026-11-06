// GC_NOTICE codec + quarantine merge + TCP exchange.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class GcNoticeTests
{
    [Fact]
    public void GcNotice_encode_decode_roundtrip()
    {
        var a = Fix.MakeNode(90);
        a.Quarantined.Add(Fix.Nid(50));
        a.DurableAuthors[Fix.Nid(50)] = 4;
        var msg = GcNotice.Build(a, 1, tombstoneCutoffWall: 1000);
        Assert.Single(msg.Quarantined);
        var payload = GcNotice.Encode(msg);
        var back = GcNotice.Decode(payload);
        Assert.Throws<InvalidDataException>(() => GcNotice.Decode(payload.Append((byte)0).ToArray()));
        Assert.Equal(1, back.NamespaceId);
        Assert.Equal(1000u, back.TombstoneCutoffWall);
        Assert.Single(back.Quarantined);
        Assert.Equal(NodeId.From(Fix.Nid(50)), back.Quarantined[0].NodeId);
        Assert.Equal(4, back.Quarantined[0].Epoch);
    }

    [Fact]
    public void GcNotice_Apply_adds_quarantine()
    {
        var a = Fix.MakeNode(91);
        var b = Fix.MakeNode(92);
        b.Quarantined.Add(Fix.Nid(51));
        var msg = GcNotice.Build(b, 1);
        int n = GcNotice.Apply(a, msg);
        Assert.True(n >= 1);
        Assert.Contains(a.Quarantined, k => Fix.Eq(k, Fix.Nid(51)));
    }

    [Fact]
    public void PurgeBelowCutoff_removes_old_fields()
    {
        var a = Fix.MakeNode(93);
        var rid = Fix.Rid(200);
        a.ApplyRemoteField(1, rid, 0x100, new byte[] { 1 }, new Hlc(100, 0), a.NodeId);
        a.ApplyRemoteField(1, rid, 0x101, new byte[] { 2 }, new Hlc(5000, 0), a.NodeId);
        int removed = GcNotice.PurgeBelowCutoff(a, 1, cutoffWall: 1000);
        Assert.Equal(1, removed);
        Assert.False(a.Storage[1][rid].State.ContainsKey(0x100));
        Assert.True(a.Storage[1][rid].State.ContainsKey(0x101));
    }

    [Fact]
    public void Gc_events_run_after_node_gates_are_released()
    {
        using var node = Fix.MakeNode(96);
        var peer = Fix.Nid(53);
        var msg = new GcNoticeMessage
        {
            NamespaceId = 1,
            Quarantined = new List<GcQuarantineEntry> { new() { NodeId = NodeId.From(peer), Epoch = 2 } },
        };
        int noticeEvents = 0, gcEvents = 0;
        void AssertCanWrite()
        {
            var write = Task.Run(() => node.WriteField(1, Fix.Rid(96 + Interlocked.Increment(ref gcEvents)), 0x120, new byte[] { 1 }));
            Assert.True(write.Wait(TimeSpan.FromSeconds(3)), "GC event ran while node gates were held.");
            Assert.True(write.Result);
        }
        node.GcNoticeApplied += (_, added) => { Assert.Equal(1, added); AssertCanWrite(); noticeEvents++; };
        node.GarbageCollected += (_, removed, _) => { Assert.Equal(1, removed); AssertCanWrite(); gcEvents++; };

        Assert.Equal(1, GcNotice.Apply(node, msg));
        var rid = Fix.Rid(200);
        node.ApplyRemoteField(1, rid, 0x100, new byte[] { 1 }, new Hlc(100, 0), node.NodeId);
        Assert.Equal(1, GcNotice.PurgeBelowCutoff(node, 1, 1000));
        Assert.Equal(1, noticeEvents);
        Assert.Equal(2, gcEvents);
    }

    [Fact]
    public async Task Tcp_ExchangeGcNotice_propagates_quarantine()
    {
        var a = Fix.MakeNode(94);
        var b = Fix.MakeNode(95);
        b.Quarantined.Add(Fix.Nid(52));

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.ExchangeGcNoticeAsync(b, 1, initiate: false);
        var (_, learned) = await client.ExchangeGcNoticeAsync(a, 1, initiate: true);
        await serverTask;

        Assert.True(learned >= 1 || a.Quarantined.Any(k => Fix.Eq(k, Fix.Nid(52))));
        Assert.Contains(a.Quarantined, k => Fix.Eq(k, Fix.Nid(52)));
    }
}
