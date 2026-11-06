using Xunit;

namespace BinSync.Tests;

public sealed class PublicApiIsolationTests
{
    [Fact]
    public void PublicRecordAndFieldReads_ReturnDetachedMutableCopies()
    {
        var node = Fix.MakeNode(901);
        var recordId = Fix.Rid(901);
        var original = System.Text.Encoding.UTF8.GetBytes("original");
        node.WriteField(7, recordId, 0x101, original);

        Assert.True(node.TryGetRecord(7, recordId, out var record));
        Assert.NotNull(record);
        record!.State[0x101].Value[0] = (byte)'X';
        record.State.Add(0x102, new FieldState(new byte[] { 1 }, new Hlc(0, 0), node.NodeId));
        record.LifecycleValue = Model.Dead;

        Assert.True(node.TryGetField(7, recordId, 0x101, out var field));
        Assert.NotNull(field);
        field!.Value[0] = (byte)'Y';

        Assert.True(node.TryGetRecord(7, recordId, out var current));
        Assert.NotNull(current);
        Assert.Equal(Model.Alive, current!.LifecycleValue);
        Assert.DoesNotContain((ushort)0x102, current.State.Keys);
        Assert.True(node.TryGetField(7, recordId, 0x101, out var currentField));
        Assert.NotNull(currentField);
        Assert.Equal(original, currentField!.Value);
    }
    [Fact]
    public void Node_DoesNotExposeSequenceReservationOrUntypedReadHelpers()
    {
        Assert.Null(typeof(Node).GetMethod("NextOutboxSeq"));
        Assert.Null(typeof(Node).GetMethod("SeenState"));
        Assert.DoesNotContain(typeof(Node).GetMethods(),
            method => method.Name == "PushOutbox" &&
                      method.GetParameters().Any(parameter => parameter.ParameterType == typeof(object)));
        Assert.DoesNotContain(typeof(Node).GetMethods(),
            method => method.Name == "PushOutbox" &&
                      method.GetParameters().Any(parameter => parameter.ParameterType == typeof(byte[])));
    }

    [Fact]
    public void PushOutbox_ReturnsDetachedEntry()
    {
        var node = Fix.MakeNode(902);
        var recordId = Fix.Rid(902);
        var op = new OpFieldWrite
        {
            FieldId = 0x101,
            Wall = 1,
            Value = System.Text.Encoding.UTF8.GetBytes("payload"),
        };

        var returned = node.PushOutbox(7, recordId, op, node.NodeId);
        Assert.NotNull(returned);
        var stored = node.Outbox[7][0];
        var storedPayload = stored.Payload.ToArray();
        Assert.NotSame(stored, returned);

        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(
            returned!.Payload, out var payload));
        payload.Array![payload.Offset] = (byte)'X';
        returned.CausalDeps.Add(new BatchId(1, 2));
        returned.Seq = 999;

        Assert.Equal(1L, stored.Seq);
        Assert.Empty(stored.CausalDeps);
        Assert.Equal(storedPayload, stored.Payload.ToArray());
    }

}
