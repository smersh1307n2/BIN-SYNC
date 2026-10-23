// MEMBERSHIP codec + bilateral AutoReconcile (unique records on both sides).
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class MembershipTests
{
    [Fact]
    public void Membership_encode_decode_roundtrip()
    {
        var a = Fix.MakeNode(61);
        a.DurableAuthors[Fix.Nid(99)] = 3;
        var msg = Membership.Build(a, 1, membershipEpoch: 7);
        Assert.True(msg.Members.Count >= 2);
        var back = Membership.Decode(Membership.Encode(msg));
        Assert.Equal(1, back.NamespaceId);
        Assert.Equal(7u, back.MembershipEpoch);
        Assert.Equal(msg.Members.Count, back.Members.Count);
    }

    [Fact]
    public void Membership_Apply_learns_durable_author()
    {
        var a = Fix.MakeNode(62);
        var b = Fix.MakeNode(63);
        b.DurableAuthors[Fix.Nid(100)] = 5;
        var msg = Membership.Build(b, 1);
        int learned = Membership.Apply(a, msg);
        Assert.True(learned >= 1);
        Assert.True(a.DurableAuthors.ContainsKey(Fix.Nid(100)) ||
                    a.DurableAuthors.Keys.Any(k => Fix.Eq(k, Fix.Nid(100))));
    }

    [Fact]
    public async Task Tcp_ExchangeMembership_learns_peer()
    {
        var a = Fix.MakeNode(64);
        var b = Fix.MakeNode(65);
        b.DurableAuthors[Fix.Nid(101)] = 2;

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var tServer = server.ExchangeMembershipAsync(b, 1, initiate: false);
        var (_, learned) = await client.ExchangeMembershipAsync(a, 1, initiate: true);
        await tServer;

        Assert.True(learned >= 1 || a.DurableAuthors.Count >= 1);
        Assert.True(a.DurableAuthors.Keys.Any(k => Fix.Eq(k, Fix.Nid(101))) ||
                    a.DurableAuthors.Keys.Any(k => Fix.Eq(k, b.NodeId)));
    }

    [Fact]
    public async Task Tcp_BilateralAutoReconcile_merges_unique_records()
    {
        var a = Fix.MakeNode(66);
        var b = Fix.MakeNode(67);
        var ridA = Fix.Rid(80);
        var ridB = Fix.Rid(81);
        a.WriteField(1, ridA, 0x100, new byte[] { 10 });
        b.WriteField(1, ridB, 0x100, new byte[] { 20 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.BilateralAutoReconcileAsync(b, 1, initiate: false);
        await client.BilateralAutoReconcileAsync(a, 1, initiate: true);
        await serverTask;

        Assert.True(a.Storage.ContainsKey(1) && a.Storage[1].ContainsKey(ridB),
            "A should receive B's unique record");
        Assert.True(b.Storage.ContainsKey(1) && b.Storage[1].ContainsKey(ridA),
            "B should receive A's unique record");
        Assert.Equal(new byte[] { 20 }, a.Storage[1][ridB].State[0x100].Value);
        Assert.Equal(new byte[] { 10 }, b.Storage[1][ridA].State[0x100].Value);
    }
}
