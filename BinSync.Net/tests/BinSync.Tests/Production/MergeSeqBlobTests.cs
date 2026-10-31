// §8.7 SEQ_FRACTIONAL + §8.8 BLOB_REF.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class MergeSeqBlobTests
{
    static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void SeqFractional_encode_decode()
    {
        var item = new SeqFractional.Item
        {
            ParentId = Author(1),
            OrderKey = "a0",
            AuthorNodeId = Author(2),
            Seq = 7,
            Value = new byte[] { 1, 2, 3 },
        };
        var back = SeqFractional.Decode(SeqFractional.Encode(item));
        Assert.Equal("a0", back.OrderKey);
        Assert.Equal(7UL, back.Seq);
        Assert.Equal(new byte[] { 1, 2, 3 }, back.Value);
    }

    [Fact]
    public void SeqFractional_orderKey_decides_winner()
    {
        var low = SeqFractional.Encode(new SeqFractional.Item
        {
            ParentId = Author(0),
            OrderKey = "a",
            AuthorNodeId = Author(1),
            Seq = 1,
            Value = new byte[] { 1 },
        });
        var high = SeqFractional.Encode(new SeqFractional.Item
        {
            ParentId = Author(0),
            OrderKey = "b",
            AuthorNodeId = Author(1),
            Seq = 1,
            Value = new byte[] { 2 },
        });
        var merged = SeqFractional.Merge(low, high);
        Assert.Equal(new byte[] { 2 }, SeqFractional.Decode(merged).Value);
    }

    [Fact]
    public void SeqFractional_policy_via_MergeField()
    {
        var local = new FieldState(
            SeqFractional.Encode(new SeqFractional.Item
            {
                ParentId = Author(0),
                OrderKey = "m",
                AuthorNodeId = Author(1),
                Seq = 1,
                Value = new byte[] { 9 },
            }),
            new Hlc(1, 0), Author(1));
        var remote = new FieldState(
            SeqFractional.Encode(new SeqFractional.Item
            {
                ParentId = Author(0),
                OrderKey = "z",
                AuthorNodeId = Author(2),
                Seq = 1,
                Value = new byte[] { 8 },
            }),
            new Hlc(2, 0), Author(2));
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.SeqFractional);
        Assert.True(changed);
        Assert.Equal(new byte[] { 8 }, SeqFractional.Decode(w.Value).Value);
    }

    [Fact]
    public void BlobRef_encode_decode()
    {
        var d = new BlobRef.Descriptor
        {
            BlobId = Author(5),
            Size = 1024,
            ContentType = "image/png",
            Hash = Codec.CanonicalB256(new byte[] { 0xAB }),
        };
        var back = BlobRef.Decode(BlobRef.Encode(d));
        Assert.Equal(1024UL, back.Size);
        Assert.Equal("image/png", back.ContentType);
        Assert.True(Fix.Eq(Author(5), back.BlobId));
    }

    [Fact]
    public void BlobRef_same_id_no_rewrite()
    {
        var payload = BlobRef.Encode(new BlobRef.Descriptor
        {
            BlobId = Author(1),
            Size = 10,
            ContentType = "a",
            Hash = new byte[32],
        });
        var merged = BlobRef.Merge(payload, payload);
        Assert.Equal(payload, merged);
    }

    [Fact]
    public void BlobRef_different_id_deterministic()
    {
        var a = BlobRef.Encode(new BlobRef.Descriptor
        {
            BlobId = Author(1),
            Size = 1,
            ContentType = "x",
            Hash = new byte[32],
        });
        var b = BlobRef.Encode(new BlobRef.Descriptor
        {
            BlobId = Author(2),
            Size = 2,
            ContentType = "y",
            Hash = new byte[32],
        });
        var m1 = BlobRef.Merge(a, b);
        var m2 = BlobRef.Merge(b, a);
        Assert.Equal(m1, m2); // commutative
        Assert.Equal(2UL, BlobRef.Decode(m1).Size);
    }

    [Fact]
    public void Spec_wire_codes()
    {
        Assert.Equal(0x06, (byte)MergePolicy.SeqFractional);
        Assert.Equal(0x08, (byte)MergePolicy.BlobRef);
    }
}
