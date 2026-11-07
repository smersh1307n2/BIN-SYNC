using System.IO;
using BinSync;
using Xunit;

namespace BinSync.DataProvider.Conformance;

/// <summary>
/// Provider-independent tests for stable source mapping, change capture,
/// snapshot/feed barriers and idempotent remote application.
/// </summary>
public abstract class DataProviderConformanceSuite
{
    protected abstract IDataProviderConformanceAdapter CreateProvider();

    [Fact]
    public void Source_item_and_field_mappings_are_stable_and_distinct()
    {
        using var provider = CreateProvider();
        byte[] itemA = { 1, 2, 3 };
        byte[] itemB = { 1, 2, 4 };
        byte[] fieldA = { 10, 11 };
        byte[] fieldB = { 10, 12 };

        RecordId recordA = provider.ResolveRecordId(itemA);
        RecordId recordB = provider.ResolveRecordId(itemB);
        ushort idA = provider.ResolveFieldId(fieldA);
        ushort idB = provider.ResolveFieldId(fieldB);

        Assert.False(recordA.IsZero);
        Assert.False(recordB.IsZero);
        Assert.NotEqual(recordA, recordB);
        Assert.Equal(recordA, provider.ResolveRecordId(itemA));
        Assert.NotEqual((ushort)0, idA);
        Assert.NotEqual(idA, idB);
        Assert.Equal(idA, provider.ResolveFieldId(fieldA));
    }

    [Fact]
    public void Local_change_feed_is_bounded_resumable_and_acknowledgement_gated()
    {
        using var provider = CreateProvider();
        provider.AppendLocalChange(Mutation(1, 1, 10, 1));
        provider.AppendLocalChange(Mutation(1, 2, 20, 1));
        provider.AppendLocalChange(Mutation(2, 1, 30, 1));

        var first = provider.ReadLocalChanges(0, 2);
        Assert.Equal(new ulong[] { 1, 2 }, first.Changes.Select(x => x.Sequence));
        Assert.Equal(2UL, first.ScannedThroughSequence);

        first.Changes[0].Mutation.Value[0] = 99;
        var retry = provider.ReadLocalChanges(0, 2);
        Assert.Equal(10, retry.Changes[0].Mutation.Value[0]);

        Assert.Throws<ArgumentOutOfRangeException>(() => provider.AcknowledgeLocalChanges(3));
        provider.AcknowledgeLocalChanges(2);

        var second = provider.ReadLocalChanges(2, 2);
        Assert.Equal(new ulong[] { 3 }, second.Changes.Select(x => x.Sequence));
        Assert.Equal(3UL, second.ScannedThroughSequence);
        provider.AcknowledgeLocalChanges(3);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(3UL, provider.LocalChangeHighWatermark);
    }

    [Fact]
    public void Source_snapshot_and_feed_watermark_form_one_gap_free_boundary()
    {
        using var provider = CreateProvider();
        provider.AppendLocalChange(Mutation(1, 1, 10, 1));

        var snapshot = provider.OpenSnapshot();
        provider.AppendLocalChange(Mutation(1, 1, 20, 1));

        var snapshotPage = provider.ReadSnapshot(snapshot.Token, 0, 10);
        Assert.True(snapshotPage.IsComplete);
        Assert.Single(snapshotPage.Entries);
        Assert.Equal(10, snapshotPage.Entries[0].Value[0]);

        var tail = provider.ReadLocalChanges(snapshot.ChangeFeedWatermark, 10);
        Assert.Single(tail.Changes);
        Assert.Equal(2UL, tail.Changes[0].Sequence);
        Assert.Equal(20, tail.Changes[0].Mutation.Value[0]);
        provider.ReleaseSnapshot(snapshot.Token);
    }

    [Fact]
    public void Snapshot_view_remains_stable_across_local_and_remote_mutations_until_release()
    {
        using var provider = CreateProvider();
        provider.AppendLocalChange(Mutation(1, 1, 10, 1));
        var snapshot = provider.OpenSnapshot();

        provider.AppendLocalChange(Mutation(1, 1, 20, 1));
        provider.ApplyRemoteBatch(Guid.Parse("00000000-0000-0000-0000-000000000201"),
            new[] { Mutation(2, 1, 30, 2) });

        var page = provider.ReadSnapshot(snapshot.Token, 0, 10);
        Assert.Single(page.Entries);
        Assert.Equal(10, page.Entries[0].Value[0]);

        Assert.True(provider.TryReadCurrent(Record(1), 1, out var localCurrent));
        Assert.Equal(20, localCurrent!.Value[0]);
        Assert.True(provider.TryReadCurrent(Record(2), 1, out var remoteCurrent));
        Assert.Equal(30, remoteCurrent!.Value[0]);

        provider.ReleaseSnapshot(snapshot.Token);
        Assert.Throws<ObjectDisposedException>(() => provider.ReadSnapshot(snapshot.Token, 0, 10));
    }

    [Fact]
    public void Remote_apply_is_idempotent_and_does_not_reenter_the_local_change_feed()
    {
        using var provider = CreateProvider();
        var token = Guid.Parse("00000000-0000-0000-0000-000000000202");
        var mutations = new[]
        {
            Mutation(1, 1, 10, 2),
            Mutation(2, 1, 20, 2)
        };

        var first = provider.ApplyRemoteBatch(token, mutations);
        var retry = provider.ApplyRemoteBatch(token, mutations);

        Assert.False(first.IsDuplicate);
        Assert.True(retry.IsDuplicate);
        Assert.Equal(first.BatchFingerprint, retry.BatchFingerprint);
        Assert.True(provider.TryReadCurrent(Record(1), 1, out var one));
        Assert.True(provider.TryReadCurrent(Record(2), 1, out var two));
        Assert.Equal(10, one!.Value[0]);
        Assert.Equal(20, two!.Value[0]);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }

    [Fact]
    public void Remote_apply_token_equivocation_and_invalid_batches_fail_without_partial_mutation()
    {
        using var provider = CreateProvider();
        var token = Guid.Parse("00000000-0000-0000-0000-000000000203");
        provider.ApplyRemoteBatch(token, new[] { Mutation(1, 1, 10, 2) });

        Assert.Throws<InvalidDataException>(() =>
            provider.ApplyRemoteBatch(token, new[] { Mutation(1, 1, 99, 2) }));
        Assert.True(provider.TryReadCurrent(Record(1), 1, out var existing));
        Assert.Equal(10, existing!.Value[0]);

        var invalidBatch = new[]
        {
            Mutation(2, 1, 20, 2),
            Mutation(3, 1, 30, 2) with { AuthorNodeId = default }
        };
        Assert.Throws<ArgumentException>(() =>
            provider.ApplyRemoteBatch(Guid.Parse("00000000-0000-0000-0000-000000000204"), invalidBatch));
        Assert.False(provider.TryReadCurrent(Record(2), 1, out _));
        Assert.False(provider.TryReadCurrent(Record(3), 1, out _));
    }

    protected static ProviderMutation Mutation(byte record, ushort field, byte value, byte author)
        => new()
        {
            RecordId = Record(record),
            FieldId = field,
            AuthorNodeId = Node(author),
            HlcWall = 100 + value,
            HlcLogical = 0,
            KeyEpoch = 0,
            Value = new[] { value }
        };

    protected static RecordId Record(byte seed) => RecordId.From(Bytes(seed));
    protected static NodeId Node(byte seed) => NodeId.From(Bytes(seed));

    private static byte[] Bytes(byte seed)
    {
        var bytes = new byte[16];
        bytes[15] = seed;
        return bytes;
    }
}
