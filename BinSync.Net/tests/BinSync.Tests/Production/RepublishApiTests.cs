using Xunit;

namespace BinSync.Tests;

public sealed class RepublishApiTests
{
    [Fact]
    public void Republish_methods_validate_public_arguments()
    {
        using var node = new Node(new NodeId(1, 2));

        Assert.Throws<ArgumentOutOfRangeException>(() => node.RepublishUnderPrefix(-1, Array.Empty<byte>()));
        Assert.Throws<ArgumentNullException>(() => node.RepublishUnderPrefix(1, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => node.RepublishUnderPrefix(1, new byte[33]));
        Assert.Throws<ArgumentOutOfRangeException>(() => node.RepublishRecords(-1, Array.Empty<RecordId>()));
        Assert.Throws<ArgumentNullException>(() => node.RepublishRecords(1, null!));
    }

    [Fact]
    public void RepublishRecords_materializes_caller_enumerable_before_taking_node_locks()
    {
        using var node = new Node(new NodeId(3, 4));
        var selected = new RecordId(5, 6);
        Assert.True(node.WriteField(1, selected, 0x100, [1]));

        IEnumerable<RecordId> SelectRecords()
        {
            var concurrentWrite = Task.Run(() =>
                node.WriteField(1, new RecordId(7, 8), 0x100, [2]));
            Assert.True(concurrentWrite.Wait(TimeSpan.FromSeconds(3)),
                "Caller enumerable was evaluated while Node locks were held.");
            Assert.True(concurrentWrite.Result);
            yield return selected;
        }

        Assert.Equal(1, node.RepublishRecords(1, SelectRecords()));
        Assert.Equal(3, node.GetOutboxCount(1));
    }
    [Fact]
    public void RepublishRecords_does_not_enumerate_after_node_disposal()
    {
        var node = new Node(new NodeId(9, 10));
        node.Dispose();
        bool enumerated = false;

        IEnumerable<RecordId> SelectRecords()
        {
            enumerated = true;
            yield return new RecordId(1, 2);
        }

        Assert.Throws<ObjectDisposedException>(() => node.RepublishRecords(1, SelectRecords()));
        Assert.False(enumerated);
    }

}
