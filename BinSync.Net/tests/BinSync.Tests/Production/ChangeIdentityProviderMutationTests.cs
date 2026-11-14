using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class ChangeIdentityProviderMutationTests
{
    private static readonly NodeId Author = NodeId.From(Identity(1));

    [Fact]
    public void Provider_field_write_fingerprint_matches_canonical_operation_fingerprint()
    {
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(Identity(2)),
            FieldId = 0x0101,
            KeyEpoch = 7,
            HlcWall = 123_456_789,
            HlcLogical = 19,
            AuthorNodeId = Author,
            Value = new byte[] { 1, 2, 3, 4, 5 }
        };
        var operation = new OpFieldWrite
        {
            FieldId = mutation.FieldId,
            KeyEpoch = mutation.KeyEpoch,
            Wall = mutation.HlcWall,
            Logical = mutation.HlcLogical,
            Origin = 1,
            Value = mutation.Value
        };

        Assert.Equal(
            ChangeIdentity.Fingerprint(3, mutation.RecordId, OpsConst.TagFieldWrite,
                operation, Author, Array.Empty<BatchId>()),
            ChangeIdentity.Fingerprint(3, mutation, Author));
    }

    [Fact]
    public void Provider_field_tombstone_fingerprint_matches_canonical_operation_fingerprint()
    {
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(Identity(3)),
            FieldId = 0x0102,
            KeyEpoch = 2,
            HlcWall = 223_456_789,
            HlcLogical = 4,
            AuthorNodeId = Author,
            IsTombstone = true,
            Value = Array.Empty<byte>()
        };
        var operation = new OpFieldTomb
        {
            FieldId = mutation.FieldId,
            KeyEpoch = mutation.KeyEpoch,
            Wall = mutation.HlcWall,
            Logical = mutation.HlcLogical,
            Origin = 1
        };

        Assert.Equal(
            ChangeIdentity.Fingerprint(3, mutation.RecordId, OpsConst.TagFieldTomb,
                operation, Author, Array.Empty<BatchId>()),
            ChangeIdentity.Fingerprint(3, mutation, Author));
    }

    [Fact]
    public void Provider_record_tombstone_fingerprint_matches_canonical_operation_fingerprint()
    {
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(Identity(4)),
            FieldId = Model.LifecycleFieldId,
            HlcWall = 323_456_789,
            HlcLogical = 8,
            AuthorNodeId = Author,
            IsRecordTombstone = true,
            Value = Array.Empty<byte>()
        };
        var operation = new OpRecordTombstone
        {
            RecordId = mutation.RecordId,
            Wall = mutation.HlcWall,
            Logical = mutation.HlcLogical,
            Origin = 1
        };

        Assert.Equal(
            ChangeIdentity.Fingerprint(3, mutation.RecordId, OpsConst.TagRecordTomb,
                operation, Author, Array.Empty<BatchId>()),
            ChangeIdentity.Fingerprint(3, mutation, Author));
    }

    [Fact]
    public void Provider_mutation_codec_roundtrips_write_and_tombstone_variants()
    {
        ProviderMutation[] mutations =
        {
            new()
            {
                RecordId = RecordId.From(Identity(10)), FieldId = 0x0101, KeyEpoch = 7,
                HlcWall = 123, HlcLogical = 4, AuthorNodeId = Author,
                Value = new byte[] { 1, 2, 3 }
            },
            new()
            {
                RecordId = RecordId.From(Identity(11)), FieldId = 0x0102, KeyEpoch = 0,
                HlcWall = 124, HlcLogical = 5, AuthorNodeId = Author,
                IsTombstone = true, Value = Array.Empty<byte>()
            },
            new()
            {
                RecordId = RecordId.From(Identity(12)), FieldId = Model.LifecycleFieldId,
                HlcWall = 125, HlcLogical = 6, AuthorNodeId = Author,
                IsRecordTombstone = true, Value = Array.Empty<byte>()
            }
        };

        foreach (var mutation in mutations)
        {
            ProviderMutation decoded = ProviderMutationCodec.Decode(ProviderMutationCodec.Encode(mutation));
            Assert.Equal(mutation.RecordId, decoded.RecordId);
            Assert.Equal(mutation.FieldId, decoded.FieldId);
            Assert.Equal(mutation.IsTombstone, decoded.IsTombstone);
            Assert.Equal(mutation.IsRecordTombstone, decoded.IsRecordTombstone);
            Assert.Equal(mutation.HlcWall, decoded.HlcWall);
            Assert.Equal(mutation.HlcLogical, decoded.HlcLogical);
            Assert.Equal(mutation.AuthorNodeId, decoded.AuthorNodeId);
            Assert.Equal(mutation.KeyEpoch, decoded.KeyEpoch);
            Assert.Equal(mutation.Value, decoded.Value);
        }
    }

    [Fact]
    public void Provider_mutation_codec_rejects_invalid_flags_and_trailing_bytes()
    {
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(Identity(13)), FieldId = 1, AuthorNodeId = Author,
            Value = new byte[] { 1 }
        };
        byte[] encoded = ProviderMutationCodec.Encode(mutation);

        byte[] invalidFlags = encoded.ToArray();
        invalidFlags[18] = 3;
        Assert.Throws<InvalidDataException>(() => ProviderMutationCodec.Decode(invalidFlags));

        byte[] trailing = encoded.Concat(new byte[] { 0 }).ToArray();
        Assert.Throws<InvalidDataException>(() => ProviderMutationCodec.Decode(trailing));
    }

    [Fact]
    public void Provider_mutation_codec_orders_record_tombstones_before_fields()
    {
        RecordId record = RecordId.From(Identity(14));
        var recordTombstone = new ProviderMutation
        {
            RecordId = record, FieldId = Model.LifecycleFieldId, AuthorNodeId = Author,
            IsRecordTombstone = true, Value = Array.Empty<byte>()
        };
        var field = new ProviderMutation
        {
            RecordId = record, FieldId = 1, AuthorNodeId = Author,
            Value = new byte[] { 1 }
        };
        Assert.True(ProviderMutationCodec.CompareCanonical(recordTombstone, field) < 0);
    }

    private static byte[] Identity(byte value)
    {
        var bytes = new byte[16];
        bytes[0] = value;
        bytes[15] = (byte)(value ^ 0xA5);
        return bytes;
    }
}
