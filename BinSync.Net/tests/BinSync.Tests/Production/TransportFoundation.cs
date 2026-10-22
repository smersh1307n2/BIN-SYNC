// P0 production foundation: session wire, pipe transport, frame stream, bounds.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TransportFoundationTests
{
    [Fact]
    public void SessionWire_Hello_Roundtrips()
    {
        var id = Fix.Nid(7);
        var frame = SessionWire.EncodeHello(new HelloPayload(id, 3, SessionCapabilities.FlowControl));
        var (f, _) = FrameCodec.ParseFrame(frame);
        Assert.Equal(SessionWire.TypeHello, f.FrameType);
        Assert.Equal((uint)0, f.StreamId);
        var h = SessionWire.DecodeHello(frame);
        Assert.True(Fix.Eq(id, h.NodeId));
        Assert.Equal(3, h.Epoch);
        Assert.Equal(SessionCapabilities.FlowControl, h.Capabilities);
    }

    [Fact]
    public void SessionWire_AliasMap_Roundtrips()
    {
        var entries = new List<AliasMapEntry>
        {
            new(1, Fix.Nid(1), 1),
            new(2, Fix.Nid(2), 5),
        };
        var frame = SessionWire.EncodeAliasMap(entries);
        var got = SessionWire.DecodeAliasMap(frame);
        Assert.Equal(2, got.Count);
        Assert.Equal((ushort)1, got[0].Alias);
        Assert.True(Fix.Eq(Fix.Nid(2), got[1].NodeId));
        Assert.Equal(5, got[1].Epoch);
    }

    [Fact]
    public async Task PipeTransport_Handshake_Then_SyncRound()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        var (ta, tb) = PipeTransport.CreatePair();
        ta.PeerNodeId = b.NodeId;
        tb.PeerNodeId = a.NodeId;

        await SessionWire.HandshakeAsync(a, ta, b, tb);

        Assert.NotNull(a.SessionEpoch(b.NodeId, 0));
        Assert.NotNull(b.SessionEpoch(a.NodeId, 0));
        Assert.Equal(1, a.AliasOf(b.NodeId, a.NodeId));
        a.WriteField(1, Fix.Rid(10), 0x100, new byte[] { 1, 2, 3 });
        int n = Transport.SyncRound(a, b, 1);
        Assert.True(n >= 1);
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(10)));
    }

    [Fact]
    public async Task FrameStream_MemoryStream_Roundtrip()
    {
        var payload = new Writer().U32(0xAABBCCDDu).Value();
        var frame = FrameCodec.BuildFrame(0x09, 0x0001_0001, 42, payload);
        using var ms = new MemoryStream();
        await FrameStream.WriteFrameAsync(ms, frame);
        ms.Position = 0;
        var got = await FrameStream.ReadFrameAsync(ms);
        Assert.NotNull(got);
        Assert.Equal(frame, got);
        var eof = await FrameStream.ReadFrameAsync(ms);
        Assert.Null(eof);
    }

    [Fact]
    public void ResourceLimits_Blocks_When_Outbox_Full()
    {
        var n = Fix.MakeNode(3);
        var lim = new ResourceLimits { MaxOutboxEntriesPerNamespace = 2 };
        Assert.True(BoundedNode.WriteField(n, lim, 1, Fix.Rid(1), 0x100, new byte[] { 1 }));
        Assert.True(BoundedNode.WriteField(n, lim, 1, Fix.Rid(2), 0x100, new byte[] { 2 }));
        Assert.Equal(2, n.OutboxSize(1));
        Assert.Throws<SyncException>(() =>
            BoundedNode.WriteField(n, lim, 1, Fix.Rid(3), 0x100, new byte[] { 3 }));
    }

    [Fact]
    public async Task StreamSyncTransport_Over_MemoryStream_Roundtrip()
    {
        using var ms = new MemoryStream();
        await using var t = new StreamSyncTransport(ms, ownsStream: false);
        var frame = SessionWire.EncodeHello(new HelloPayload(Fix.Nid(9), 1, SessionCapabilities.None));
        await t.SendAsync(frame);
        ms.Position = 0;
        await using var t2 = new StreamSyncTransport(ms, ownsStream: false);
        var got = await t2.ReceiveAsync();
        Assert.NotNull(got);
        var h = SessionWire.DecodeHello(got!);
        Assert.True(Fix.Eq(Fix.Nid(9), h.NodeId));
    }
}
