using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class ResourceLimitsAtomicityTests
{
    private static RecordId Rid(byte value)
    {
        Span<byte> id = stackalloc byte[16];
        id.Clear();
        id[0] = value;
        return RecordId.From(id);
    }

    [Fact]
    public void Full_outbox_rejects_write_without_mutating_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.Throws<SyncException>(() => node.WriteField(3, Rid(1), 0x101, [2]));

        Assert.False(node.TryGetField(3, Rid(1), 0x101, out _));
        Assert.Equal(1, node.GetOutboxCount(3));
        Assert.Equal(1, node.GetRecordCount(3));
    }

    [Fact]
    public void Rejected_direct_outbox_push_does_not_consume_author_sequence()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));

        var op = new OpFieldWrite
        {
            FieldId = 0x101,
            KeyEpoch = 0,
            Wall = 2,
            Logical = 0,
            Origin = 1,
            AuthorNodeId = node.NodeId.ToArray(),
            Value = [2]
        };

        Assert.Throws<SyncException>(() =>
            node.PushOutbox(3, Rid(1), op, node.NodeId));

        // The next allocation must be exactly the next sequence after the
        // existing queued entry; the rejected push must not leave a hole.
        Assert.Equal(2, node.NextOutboxSeq(node.NodeId, 3));
        Assert.Equal(1, node.GetOutboxCount(3));
    }

    [Fact]
    public void Max_records_does_not_block_updates_to_existing_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxRecords = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.True(node.WriteField(3, Rid(1), 0x101, [2]));

        Assert.True(node.TryGetField(3, Rid(1), 0x101, out var field));
        Assert.Equal(new byte[] { 2 }, field!.Value);
        Assert.Equal(1, node.GetRecordCount(3));
    }

    [Fact]
    public void Max_records_rejects_new_record_without_creating_empty_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxRecords = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.Throws<SyncException>(() => node.WriteField(3, Rid(2), 0x100, [2]));

        Assert.False(node.TryGetRecord(3, Rid(2), out _));
        Assert.Equal(1, node.GetRecordCount(3));
    }
}
