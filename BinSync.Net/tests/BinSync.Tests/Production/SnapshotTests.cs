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
        Assert.Equal(snap.Records[0].HasLifecycle, back.Records[0].HasLifecycle);
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
    public void Snapshot_roundtrip_and_apply_preserve_field_and_record_tombstones()
    {
        var source = Fix.MakeNode(58);
        var target = Fix.MakeNode(59);
        var rid = Fix.Rid(74);
        source.WriteField(1, rid, 0x100, new byte[] { 1 });
        source.TombField(1, rid, 0x100);
        source.DeleteRecord(1, rid);

        var encoded = Reconcile.EncodeSnapshotChunk(Reconcile.BuildSnapshot(source, 1));
        var decoded = Reconcile.DecodeSnapshotChunk(encoded);
        Assert.Contains(decoded.Records[0].Fields, f => f.IsTombstone && f.FieldId == 0x100);
        Assert.True(decoded.Records[0].HasLifecycle);
        Reconcile.ApplySnapshot(target, decoded);

        Assert.True(target.Storage[1][rid].HasFieldTombs);
        Assert.Equal(Model.Dead, target.Storage[1][rid].LifecycleValue);
    }

    [Fact]
    public void Snapshot_chunks_cover_the_complete_record_set()
    {
        var source = Fix.MakeNode(60);
        for (int i = 0; i < 5; i++)
            source.WriteField(1, Fix.Rid(100 + i), 0x100, new byte[] { (byte)i });

        var chunks = Reconcile.BuildSnapshotChunks(source, 1, maxRecords: 2);
        Assert.Equal(3, chunks.Count);
        Assert.Equal(new uint[] { 0, 1, 2 }, chunks.Select(c => c.ChunkIndex));
        Assert.All(chunks, c => Assert.Equal(3u, c.ChunkCount));
        Assert.Equal(5, chunks.Sum(c => c.Records.Count));
        var target = Fix.MakeNode(61);
        Assert.Throws<InvalidOperationException>(() => Reconcile.ApplySnapshot(target, chunks[0]));
        Assert.Empty(target.Storage);
        Assert.True(Reconcile.ApplySnapshotChunks(target, chunks) > 0);
        Assert.Equal(5, target.GetRecordCount(1));
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
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
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
        var onlyA = Fix.Rid(75);
        var onlyB = Fix.Rid(76);
        a.WriteField(1, onlyA, 0x100, new byte[] { 0xA1 });
        b.WriteField(1, onlyB, 0x100, new byte[] { 0xB1 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.AutoReconcileAsync(b, 1, initiate: false);
        var (prefixes, fields) = await client.AutoReconcileAsync(a, 1, initiate: true);
        await serverTask;

        Assert.True(prefixes >= 1);
        Assert.True(fields > 0);
        Assert.True(a.Storage[1].ContainsKey(onlyB));
        Assert.True(b.Storage[1].ContainsKey(onlyA));
        Assert.True(a.PeerStates[(b.NodeId, 1)].InitialSyncComplete);
        Assert.True(b.PeerStates[(a.NodeId, 1)].InitialSyncComplete);
    }

    [Fact]
    public async Task AutoReconcile_does_not_commit_baseline_after_only_one_direction()
    {
        var a = Fix.MakeNode(62);
        var b = Fix.MakeNode(63);
        a.WriteField(1, Fix.Rid(201), 0x100, new byte[] { 0xA1 });
        b.WriteField(1, Fix.Rid(202), 0x100, new byte[] { 0xB2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // Complete only the first direction: A requests B's divergent snapshots.
        var serverDigestTask = server.ExchangeStateDigestAsync(b, 1, initiate: false);
        var (_, _, diverged) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        await serverDigestTask;

        foreach (var prefix in diverged)
        {
            var needTask = server.ReceiveNeedRangeAsync();
            await client.SendNeedRangeAsync(1, prefix);
            var need = await needTask;
            var sendTask = server.FulfillNeedRangeWithSnapshotAsync(b, need);
            await client.ReceiveSnapshotAsync(a);
            await sendTask;
        }

        await client.SendNeedRangeAsync(1, Array.Empty<byte>());
        var terminator = await server.ReceiveNeedRangeAsync();
        Assert.Empty(terminator.Prefix);

        Assert.False(a.PeerStates.TryGetValue((b.NodeId, 1), out var aState) && aState.InitialSyncComplete);
        Assert.False(b.PeerStates.TryGetValue((a.NodeId, 1), out var bState) && bState.InitialSyncComplete);

        // Only the full two-direction operation may commit the baseline.
        var serverReconcile = server.AutoReconcileAsync(b, 1, initiate: false);
        await client.AutoReconcileAsync(a, 1, initiate: true);
        await serverReconcile;

        Assert.True(a.PeerStates[(b.NodeId, 1)].InitialSyncComplete);
        Assert.True(b.PeerStates[(a.NodeId, 1)].InitialSyncComplete);
    }

}
