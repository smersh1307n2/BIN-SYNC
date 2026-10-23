// SNAPSHOT_CHUNK round-trip + AutoReconcile over TCP.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class SnapshotTests
{
    [Fact]
    public void Snapshot_encode_decode_roundtrip()
    {
        var a = Fix.MakeNode(51);
        var rid = Fix.Rid(70);
        a.WriteField(1, rid, 0x100, new byte[] { 3, 4 });
        var snap = Reconcile.BuildSnapshot(a, 1);
        Assert.NotEmpty(snap.Records);
        var back = Reconcile.DecodeSnapshotChunk(Reconcile.EncodeSnapshotChunk(snap));
        Assert.Equal(snap.NamespaceId, back.NamespaceId);
        Assert.Equal(snap.Records.Count, back.Records.Count);
        Assert.True(Fix.Eq(rid, back.Records[0].RecordId));
        Assert.Equal(new byte[] { 3, 4 }, back.Records[0].Fields[0].Value);
    }

    [Fact]
    public void ApplySnapshot_merges_into_empty_node()
    {
        var a = Fix.MakeNode(52);
        var b = Fix.MakeNode(53);
        var rid = Fix.Rid(71);
        a.WriteField(1, rid, 0x100, new byte[] { 9 });
        var snap = Reconcile.BuildSnapshot(a, 1);
        int n = Reconcile.ApplySnapshot(b, snap);
        Assert.True(n >= 1);
        Assert.Equal(new byte[] { 9 }, b.Storage[1][rid].State[0x100].Value);
    }

    [Fact]
    public async Task Tcp_Snapshot_transfer_fills_missing_record()
    {
        var a = Fix.MakeNode(54);
        var b = Fix.MakeNode(55);
        var rid = Fix.Rid(72);
        b.WriteField(1, rid, 0x100, new byte[] { 7 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var send = server.SendSnapshotAsync(b, 1);
        var applied = await client.ReceiveSnapshotAsync(a);
        await send;
        Assert.True(applied >= 1);
        Assert.Equal(new byte[] { 7 }, a.Storage[1][rid].State[0x100].Value);
    }

    [Fact]
    public async Task Tcp_AutoReconcile_converges_divergent_field()
    {
        var a = Fix.MakeNode(56);
        var b = Fix.MakeNode(57);
        var rid = Fix.Rid(73);
        a.WriteField(1, rid, 0x100, new byte[] { 1 });
        b.WriteField(1, rid, 0x100, new byte[] { 2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.AutoReconcileAsync(b, 1, initiate: false);
        var (prefixes, fields) = await client.AutoReconcileAsync(a, 1, initiate: true);
        await serverTask;

        Assert.True(prefixes >= 1 || fields >= 0);
        Assert.True(a.Storage.ContainsKey(1) && a.Storage[1].ContainsKey(rid));
        var val = a.Storage[1][rid].State[0x100].Value;
        Assert.Equal(new byte[] { 2 }, val);
    }
}
