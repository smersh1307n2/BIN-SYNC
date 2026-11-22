using Xunit;

namespace BinSync.Tests;

public sealed class OperationOriginTests
{
    [Fact]
    public void Immutable_origin_requires_a_complete_author_tuple()
    {
        var author = Fix.Nid(10);
        var origin = new OperationOrigin(author, 3, 41);

        Assert.True(origin.IsValid);
        Assert.Equal(author, origin.AuthorNodeId);
        Assert.Equal(3u, origin.AuthorEpoch);
        Assert.Equal(41UL, origin.Sequence);
        Assert.False(default(OperationOrigin).IsValid);
        Assert.Throws<ArgumentException>(() => new OperationOrigin(default, 3, 41));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OperationOrigin(author, 0, 41));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OperationOrigin(author, 3, 0));
        Assert.Throws<InvalidDataException>(() => default(OperationOrigin).EnsureValid());
    }

    [Fact]
    public void Outbox_and_metadata_relay_views_share_the_same_origin_value()
    {
        var author = Fix.Nid(11);
        var origin = new OperationOrigin(author, 7, 123);

        var entry = new OutboxEntry { Provenance = origin, Seq = 900 };
        Assert.Equal(origin, entry.Provenance);
        Assert.Equal(900, entry.Seq); // local stream coordinate is not Origin.Sequence

        var operation = new MetadataOperation
        {
            AuthorNodeId = author,
            OriginAuthorEpoch = origin.AuthorEpoch,
            OriginSequence = origin.Sequence,
            ChangeId = BatchId.From(Fix.Rid(12)),
            Fingerprint = new byte[16],
            Payload = Array.Empty<byte>()
        };
        Assert.Equal(origin, operation.Provenance);

        var relay = new MetadataRelayItem(900, author, operation.ChangeId, Array.Empty<byte>())
        {
            OriginAuthorEpoch = origin.AuthorEpoch,
            OriginSequence = origin.Sequence
        };
        Assert.Equal(origin, relay.Provenance);
        Assert.Equal(900UL, relay.Sequence);
    }
}
