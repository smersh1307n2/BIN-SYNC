using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class CodecIdentityNormalizationTests
{
    [Fact]
    public void Long_identity_input_normalizes_to_exactly_sixteen_bytes()
    {
        var input = Enumerable.Range(0, 32).Select(i => (byte)(i + 1)).ToArray();

        var canonical = Codec.CanonicalB128(input);
        Assert.Equal(16, canonical.Length);

        var writer = new Writer();
        writer.B128(input);
        Assert.Equal(canonical, writer.Value());

        Assert.Equal(NodeId.From(canonical), NodeId.From(input));
        Assert.Equal(RecordId.From(canonical), RecordId.From(input));
        Assert.Equal(BatchId.From(canonical), BatchId.From(input));
    }

    [Fact]
    public void Short_identity_input_is_zero_padded_to_sixteen_bytes()
    {
        var canonical = Codec.CanonicalB128(new byte[] { 0x12, 0x34 });

        Assert.Equal(16, canonical.Length);
        Assert.Equal(new byte[] { 0x12, 0x34 }, canonical[..2]);
        Assert.All(canonical[2..], value => Assert.Equal((byte)0, value));
    }
}
