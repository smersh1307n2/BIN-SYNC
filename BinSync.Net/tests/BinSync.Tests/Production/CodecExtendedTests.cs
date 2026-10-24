// §6 codec extensions: TLV, strings, floats, signed, field canonicalization.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class CodecExtendedTests
{
    [Fact]
    public void Varint_roundtrip_common()
    {
        foreach (ulong v in new ulong[] { 0, 1, 127, 128, 300, 16384, ulong.MaxValue })
        {
            var enc = Codec.EncodeVarint(v);
            var (dec, pos) = Codec.DecodeVarint(enc);
            Assert.Equal(v, dec);
            Assert.Equal(enc.Length, pos);
        }
    }

    [Fact]
    public void Overlong_varint_rejected()
    {
        // \x80\x00 is overlong encoding of 0
        Assert.ThrowsAny<InvalidVarintException>(() =>
            Codec.DecodeVarint(new byte[] { 0x80, 0x00 }));
    }

    [Fact]
    public void Tlv_roundtrip()
    {
        var payload = new byte[] { 1, 2, 3, 4 };
        var enc = Codec.EncodeTlv(0x2A, payload);
        var tlv = Codec.DecodeTlv(enc, 0, out int next);
        Assert.Equal(0x2A, tlv.Tag);
        Assert.Equal(payload, tlv.Value);
        Assert.Equal(enc.Length, next);
    }

    [Fact]
    public void Writer_Reader_Tlv()
    {
        var w = new Writer();
        w.Tlv(0x01, new byte[] { 0xAA }).Tlv(0x02, new byte[] { 0xBB, 0xCC });
        var r = new Reader(w.Value());
        var a = r.Tlv();
        var b = r.Tlv();
        Assert.Equal(0x01, a.Tag);
        Assert.Equal(new byte[] { 0xAA }, a.Value);
        Assert.Equal(0x02, b.Tag);
        Assert.Equal(new byte[] { 0xBB, 0xCC }, b.Value);
        Assert.True(r.Eof());
    }

    [Fact]
    public void String_utf8_roundtrip()
    {
        var s = "привет BIN-SYNC";
        var enc = Codec.EncodeString(s);
        var dec = Codec.DecodeString(enc, 0, out _);
        Assert.Equal(s, dec);
    }

    [Fact]
    public void String_strips_bom_if_present()
    {
        // Encoder never emits BOM; decoder tolerates and strips.
        var bom = new byte[] { 0xEF, 0xBB, 0xBF };
        var body = System.Text.Encoding.UTF8.GetBytes("x");
        var w = new Writer();
        w.Varint((ulong)(bom.Length + body.Length)).Raw(bom).Raw(body);
        var r = new Reader(w.Value());
        Assert.Equal("x", r.StringUtf8());
    }

    [Fact]
    public void F64_roundtrip_and_neg_zero()
    {
        Assert.Equal(1.5, Codec.DecodeF64(Codec.EncodeF64(1.5)));
        var negZero = Codec.EncodeF64(-0.0);
        var posZero = Codec.EncodeF64(0.0);
        Assert.Equal(posZero, negZero); // −0.0 → +0.0
        Assert.Equal(0.0, Codec.DecodeF64(negZero));
    }

    [Fact]
    public void F64_nan_rejected()
    {
        Assert.ThrowsAny<InvalidFloatException>(() => Codec.EncodeF64(double.NaN));
        // Craft a NaN bit pattern and decode
        var nanBits = BitConverter.GetBytes(double.NaN);
        if (!BitConverter.IsLittleEndian) Array.Reverse(nanBits);
        Assert.ThrowsAny<InvalidFloatException>(() => Codec.DecodeF64(nanBits));
    }

    [Fact]
    public void Signed_i64_roundtrip()
    {
        foreach (long v in new long[] { 0, -1, long.MinValue, long.MaxValue, 42 })
        {
            var enc = Codec.EncodeI64(v);
            Assert.Equal(v, Codec.DecodeI64(enc));
        }
    }

    [Fact]
    public void B256_canonical_pad()
    {
        var short_ = new byte[] { 1, 2, 3 };
        var c = Codec.CanonicalB256(short_);
        Assert.Equal(32, c.Length);
        Assert.Equal(1, c[0]);
        Assert.Equal(0, c[31]);
    }

    [Fact]
    public void CanonicalizeFields_sorts_and_rejects_dup()
    {
        var fields = new[]
        {
            new KeyValuePair<ushort, byte[]>(0x200, new byte[] { 2 }),
            new KeyValuePair<ushort, byte[]>(0x100, new byte[] { 1 }),
        };
        var sorted = Codec.CanonicalizeFields(fields);
        Assert.Equal(0x100, sorted[0].Key);
        Assert.Equal(0x200, sorted[1].Key);

        Assert.ThrowsAny<ArgumentException>(() =>
            Codec.CanonicalizeFields(new[]
            {
                new KeyValuePair<ushort, byte[]>(1, new byte[] { 1 }),
                new KeyValuePair<ushort, byte[]>(1, new byte[] { 2 }),
            }));
    }

    [Fact]
    public void Determinism_C1_same_input_same_bytes()
    {
        var w1 = new Writer().U16(7).StringUtf8("ab").Tlv(3, new byte[] { 9 }).F64(2.5);
        var w2 = new Writer().U16(7).StringUtf8("ab").Tlv(3, new byte[] { 9 }).F64(2.5);
        Assert.Equal(w1.Value(), w2.Value());
    }
}
