// FLAG_FRAGMENT over the generic SyncSession.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TcpFragmentTests
{
    [Fact]
    public async Task Tcp_fragment_roundtrip_preserves_frame_metadata()
    {
        using var a = Fix.MakeNode(920);
        using var b = Fix.MakeNode(921);
        var body = new byte[400];
        for (int i = 0; i < body.Length; i++) body[i] = (byte)(i * 3);

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var send = client.SendFragmentedAsync(0x09, 1, 1, body, maxBody: 80);
        var got = await server.ReceiveReassembledAsync();
        await send;

        Assert.Equal(0x09, got.FrameType);
        Assert.Equal(1u, got.StreamId);
        Assert.Equal(1UL, got.FrameSeq);
        Assert.Equal(body, got.Payload);
    }

    [Fact]
    public async Task Raw_reassembly_keeps_incomplete_message_when_another_frame_interleaves()
    {
        using var a = Fix.MakeNode(922);
        using var b = Fix.MakeNode(923);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var messageId = Guid.NewGuid().ToByteArray();
        var first = FragmentCodec.EncodeOne(messageId, 0, 2, new byte[] { 1, 2 });
        var second = FragmentCodec.EncodeOne(messageId, 1, 2, new byte[] { 3, 4 });
        var firstReceive = server.ReceiveReassembledAsync();
        await aPipe.SendAsync(FrameCodec.BuildFrame(0x09, 0x0001_0001, 10, first, Frames.FLAG_FRAGMENT));
        await aPipe.SendAsync(FrameCodec.BuildFrame(0x06, 0x0001_0001, 20, new byte[] { 0xAA }));

        var interleaved = await firstReceive;
        Assert.Equal(0x06, interleaved.FrameType);
        Assert.Equal(new byte[] { 0xAA }, interleaved.Payload);

        var secondReceive = server.ReceiveReassembledAsync();
        await aPipe.SendAsync(FrameCodec.BuildFrame(0x09, 0x0001_0001, 11, second, Frames.FLAG_FRAGMENT));
        var complete = await secondReceive;

        Assert.Equal(0x09, complete.FrameType);
        Assert.Equal(0x0001_0001u, complete.StreamId);
        Assert.Equal(10UL, complete.FrameSeq);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, complete.Payload);
    }

    [Fact]
    public async Task Raw_reassembly_rejects_one_message_spanning_streams()
    {
        using var a = Fix.MakeNode(924);
        using var b = Fix.MakeNode(925);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe);
        await using var server = await accept;

        var messageId = Guid.NewGuid().ToByteArray();
        var first = FragmentCodec.EncodeOne(messageId, 0, 2, new byte[] { 1 });
        var second = FragmentCodec.EncodeOne(messageId, 1, 2, new byte[] { 2 });
        var receive = server.ReceiveReassembledAsync();
        await aPipe.SendAsync(FrameCodec.BuildFrame(0x09, 0x0001_0001, 1, first, Frames.FLAG_FRAGMENT));
        await aPipe.SendAsync(FrameCodec.BuildFrame(0x09, 0x0001_0002, 2, second, Frames.FLAG_FRAGMENT));

        var ex = await Assert.ThrowsAsync<FrameException>(() => receive);
        Assert.True(ex.Fatal);
    }

    [Fact]
    public void Fragment_decoder_rejects_trailing_bytes()
    {
        var payload = FragmentCodec.EncodeOne(Guid.NewGuid().ToByteArray(), 0, 1, new byte[] { 1 });
        var malformed = new byte[payload.Length + 1];
        payload.CopyTo(malformed, 0);
        malformed[^1] = 0xFF;

        Assert.Throws<FrameException>(() => FragmentCodec.DecodeOne(malformed));
    }

    [Fact]
    public void Reassembler_rejects_conflicting_duplicate_fragment_and_releases_slot()
    {
        var reassembler = new FragmentReassembler();
        var messageId = Guid.NewGuid().ToByteArray();
        Assert.Null(reassembler.Accept(new FrameFragment(messageId, 0, 2, new byte[] { 1 })));

        Assert.Throws<FrameException>(() =>
            reassembler.Accept(new FrameFragment(messageId, 0, 2, new byte[] { 2 })));
        Assert.Equal(0, reassembler.PendingMessages);
    }
}
