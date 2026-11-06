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

    [Fact]
    public void CreateDeltaBatch_ReturnsDetachedPayloadsAndEntryList()
    {
        var node = Fix.MakeNode(903);
        var recordId = Fix.Rid(903);
        var targetPeer = new NodeId(903, 1903);
        Assert.True(node.WriteField(7, recordId, 0x101, new byte[] { 1, 2, 3 }));

        var stored = node.Outbox[7][0];
        var storedPayload = stored.Payload.ToArray();
        var batch = node.CreateDeltaBatch(targetPeer, 7);

        Assert.NotNull(batch);
        Assert.Single(batch!.Entries);
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(
            batch.Entries[0].Payload, out var payload));
        payload.Array![payload.Offset] ^= 0xFF;
        batch.Entries.Clear();

        Assert.Equal(1, node.GetOutboxCount(7));
        Assert.Equal(storedPayload, stored.Payload.ToArray());
    }

    [Fact]
    public void WriteField_CopiesCallerOwnedValueBuffer()
    {
        var node = Fix.MakeNode(904);
        var recordId = Fix.Rid(904);
        var input = System.Text.Encoding.UTF8.GetBytes("original");

        Assert.True(node.WriteField(7, recordId, 0x101, input));
        input[0] = (byte)'X';

        Assert.True(node.TryGetField(7, recordId, 0x101, out var field));
        Assert.NotNull(field);
        Assert.Equal("original", System.Text.Encoding.UTF8.GetString(field!.Value));
        var outbox = node.Outbox[7][0];
        Assert.Equal("original", System.Text.Encoding.UTF8.GetString(Ops.DecodeOp(outbox.Payload).Op is OpFieldWrite op ? op.Value : Array.Empty<byte>()));
    }

    [Fact]
    public void SyncException_CopiesBatchIdOnInputAndOutput()
    {
        var input = new byte[16];
        input[0] = 0x42;
        var exception = new SyncException("SEQ_GAP", input);
        input[0] = 0x11;

        var exposed = exception.BatchId;
        Assert.NotNull(exposed);
        Assert.Equal((byte)0x42, exposed![0]);
        exposed[0] = 0x22;
        Assert.Equal((byte)0x42, exception.BatchId![0]);
    }

    [Fact]
    public void DigestDomainLabels_AreReturnedAsDetachedCopies()
    {
        var expected = System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/RECORD/v1");
        var exposed = Domains.DomainRecord;
        exposed[0] ^= 0xFF;
        Assert.Equal(expected, Domains.DomainRecord);
    }

    [Fact]
    public void TrustStore_DoesNotExposeStoredPeerOrKeyBuffers()
    {
        var store = new TrustStore { DefaultMode = TrustMode.PreShared };
        var nodeId = new byte[16];
        nodeId[0] = 0x55;
        var (_, publicKey) = BatchSignature.GenerateKeyPair();
        var expectedKey = publicKey.ToArray();
        store.Allow(nodeId, publicKey);

        Assert.True(store.TryGet(nodeId, out var peer));
        peer!.PublicKey[0] ^= 0xFF;
        Assert.Equal(NodeId.From(nodeId), peer.NodeId);
        var resolved = store.ResolvePublicKey(NodeId.From(nodeId));
        Assert.Equal(expectedKey, resolved);
        resolved![0] ^= 0xFF;
        Assert.Equal(expectedKey, store.ResolvePublicKey(NodeId.From(nodeId)));
    }

    [Fact]
    public void MaterializeEntries_CopiesEvenFullArrayPayloads()
    {
        var source = new byte[] { 1, 2, 3 };
        var batch = new Batch { Entries = new List<OutboxEntry> { new() { Payload = source } } };
        batch.MaterializeEntries();
        source[0] = 0xFF;
        Assert.Equal(new byte[] { 1, 2, 3 }, batch.Entries[0].Payload.ToArray());
    }

    [Fact]
    public void Digest16_FromRequiresExactLengthAndTruncationIsExplicit()
    {
        Assert.Throws<ArgumentException>(() => Digest16.From(new byte[15]));
        Assert.Throws<ArgumentException>(() => Digest16.From(new byte[17]));
        Assert.Throws<ArgumentNullException>(() => Digest16.From((byte[])null!));

        var full = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray();
        Assert.Equal(full[..16], Digest16.Truncate(full).ToArray());
        Assert.Equal(full[..16], Digest16.From(full[..16]).ToArray());
    }

    [Fact]
    public void NodeBuilder_CopiesCallerOwnedIdentityBuffer()
    {
        var input = new byte[16];
        input[0] = 0x31;
        var builder = new NodeBuilder().WithNodeId(input);
        input[0] = 0x42;

        var node = builder.Build();
        Assert.Equal((byte)0x31, node.NodeId.ToArray()[0]);
    }

}
