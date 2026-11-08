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

    private static byte[] Identity(byte value)
    {
        var bytes = new byte[16];
        bytes[0] = value;
        bytes[15] = (byte)(value ^ 0xA5);
        return bytes;
    }
}
