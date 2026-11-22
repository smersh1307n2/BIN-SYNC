using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class EventIsolationTests
{
    [Fact]
    public void BatchApplied_handler_failure_does_not_turn_accepted_batch_into_failure()
    {
        var sender = Fix.MakeNode(941);
        using var receiver = Fix.MakeNode(942);
        Transport.MakeSession(sender, receiver);
        Assert.True(sender.WriteField(3, Fix.Rid(941), 0x100, new byte[] { 1 }));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        int laterHandlers = 0;
        receiver.BatchApplied += (_, _) => throw new InvalidOperationException("observer failure");
        receiver.BatchApplied += (_, _) => laterHandlers++;

        var result = receiver.ApplyBatch(batch, sender.NodeId);

        Assert.Equal(1, result.Applied);
        Assert.Equal(1, laterHandlers);
        Assert.True(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(941)), out _));
        Assert.Single(receiver.Inbox);
    }

    [Fact]
    public void FieldWritten_handler_failure_does_not_change_local_write_result()
    {
        using var node = Fix.MakeNode(943);
        node.FieldWritten += (_, _, _) => throw new InvalidOperationException("observer failure");

        Assert.True(node.WriteField(3, Fix.Rid(943), 0x100, new byte[] { 2 }));
        Assert.True(node.TryGetField(3, Fix.Rid(943), 0x100, out var field));
        Assert.Equal(new byte[] { 2 }, field!.Value);
    }

    [Fact]
    public void SnapshotApplied_handler_failure_does_not_change_snapshot_commit_result()
    {
        using var source = Fix.MakeNode(944);
        using var target = Fix.MakeNode(945);
        Assert.True(source.WriteField(3, Fix.Rid(944), 0x100, new byte[] { 3 }));
        var chunks = Reconcile.BuildSnapshotChunks(source, 3);
        target.SnapshotApplied += (_, _) => throw new InvalidOperationException("observer failure");

        Assert.True(Reconcile.ApplySnapshotChunks(target, chunks) > 0);
        Assert.True(target.TryGetField(3, Fix.Rid(944), 0x100, out var field));
        Assert.Equal(new byte[] { 3 }, field!.Value);
    }
}
