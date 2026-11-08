using Xunit;

namespace BinSync.Tests;

public sealed class SessionProtocolMajorTests
{
    [Fact]
    public void Hello_and_ack_round_trip_with_protocol_major_three()
    {
        var hello = new HelloPayload(NodeId.From(Fix.Rid(1)), 7, SessionCapabilities.FlowControl);
        Assert.Equal((ushort)3, SessionWire.ProtocolMajor);
        Assert.Equal(hello, SessionWire.DecodeHello(SessionWire.EncodeHello(hello)));
        Assert.Equal(hello, SessionWire.DecodeHelloAck(SessionWire.EncodeHelloAck(hello)));
    }

    [Theory]
    [InlineData((ushort)2)]
    [InlineData((ushort)4)]
    public void Hello_and_ack_reject_other_protocol_majors(ushort major)
    {
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(BuildControlFrame(
            SessionWire.TypeHello, 1, major, includeMajor: true)));
        Assert.Throws<FrameException>(() => SessionWire.DecodeHelloAck(BuildControlFrame(
            SessionWire.TypeHelloAck, 2, major, includeMajor: true)));
    }

    [Fact]
    public void V2_hello_payload_without_major_is_rejected()
    {
        Assert.Throws<FrameException>(() => SessionWire.DecodeHello(BuildControlFrame(
            SessionWire.TypeHello, 1, 2, includeMajor: false)));
    }

    private static byte[] BuildControlFrame(byte type, ulong sequence, ushort major, bool includeMajor)
    {
        var writer = new Writer(includeMajor ? 26 : 24);
        writer.B128(NodeId.From(Fix.Rid(1))).U32(7);
        if (includeMajor)
            writer.U16(major);
        writer.U32((uint)SessionCapabilities.FlowControl);
        return FrameCodec.BuildFrame(type, SessionWire.ControlStreamId, sequence, writer.WrittenSpan);
    }
}
