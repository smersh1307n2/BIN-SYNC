using Xunit;

namespace BinSync.Tests;

public sealed class PublicApiIsolationTests
{
    [Fact]
    public void Node_ExposesOnlyAReadOnlyClockSnapshot()
    {
        Assert.Null(typeof(Node).GetProperty(nameof(Node.Clock)));
        var property = typeof(Node).GetProperty(nameof(Node.ClockState));
        Assert.NotNull(property);
        Assert.Equal(typeof(HlcClockSnapshot), property!.PropertyType);
        Assert.Null(property.SetMethod);

        var node = Fix.MakeNode(907);
        var before = node.ClockState;
        node.WriteField(7, Fix.Rid(907), 0x101, new byte[] { 1 });
        var after = node.ClockState;
        Assert.True(after.LastWall >= before.LastWall);
    }

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
    public void PeerCursorState_IsNotPartOfThePublicApi()
    {
        Assert.False(typeof(PeerState).IsPublic);
        Assert.Null(typeof(Node).GetProperty("PeerStates"));
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
    public void ApplyBatch_DoesNotMutateCallerOwnedBatch()
    {
        using var sender = Fix.MakeNode(905);
        using var receiver = Fix.MakeNode(906);
        Transport.MakeSession(sender, receiver);
        Assert.True(sender.WriteField(7, Fix.Rid(905), 0x101, new byte[] { 4, 5, 6 }));

        var batch = sender.CreateDeltaBatch(receiver.NodeId, 7);
        Assert.NotNull(batch);
        var originalAuthor = batch!.AuthorStreamNodeId;

        var result = receiver.ApplyBatch(batch, sender.NodeId);

        Assert.Equal(originalAuthor, batch.AuthorStreamNodeId);
        Assert.Equal(sender.NodeId, result.DurableAuthor);
        Assert.True(receiver.TryGetField(7, Fix.Rid(905), 0x101, out var field));
        Assert.Equal(new byte[] { 4, 5, 6 }, field!.Value);
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
    public void SyncException_StoresBatchIdAsTypedValue()
    {
        var input = new byte[16];
        input[0] = 0x42;
        var expected = BatchId.From(input);
        var exception = new SyncException("SEQ_GAP", expected);
        input[0] = 0x11;

        Assert.Equal(expected, exception.BatchId);
        Assert.Equal(typeof(BatchId?), typeof(SyncException).GetProperty(nameof(SyncException.BatchId))!.PropertyType);
        Assert.DoesNotContain(typeof(SyncException).GetConstructors(),
            constructor => constructor.GetParameters().Any(parameter => parameter.ParameterType == typeof(byte[])));
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
    public void OperationDtos_UseTypedIdentityFields()
    {
        Assert.Equal(typeof(NodeId?), typeof(OpFieldWrite).GetField(nameof(OpFieldWrite.AuthorNodeId))!.FieldType);
        Assert.Equal(typeof(NodeId?), typeof(OpFieldTomb).GetField(nameof(OpFieldTomb.AuthorNodeId))!.FieldType);
        Assert.Equal(typeof(NodeId?), typeof(OpRecordTombstone).GetField(nameof(OpRecordTombstone.AuthorNodeId))!.FieldType);
        Assert.Equal(typeof(RecordId), typeof(OpRecordTombstone).GetField(nameof(OpRecordTombstone.RecordId))!.FieldType);
    }

    [Fact]
    public void FlowControl_UsesImmutableConfigurationAndAtomicCredit()
    {
        Assert.Null(typeof(FlowControl).GetProperty(nameof(FlowControl.Initial))!.GetSetMethod());
        Assert.Null(typeof(FlowControl).GetProperty(nameof(FlowControl.MaxWindow))!.GetSetMethod());

        var flow = new FlowControl(initial: 128);
        var consumed = 0;
        Parallel.For(0, 256, _ =>
        {
            if (flow.TryConsume(1, 1))
                Interlocked.Increment(ref consumed);
        });
        Assert.Equal(128, consumed);
        Assert.Equal(0, flow.Balance(1));
    }

    [Fact]
    public void LoopbackPeerRouting_UsesTypedNodeIds()
    {
        var connect = typeof(LoopbackTransport).GetMethod(nameof(LoopbackTransport.Connect))!;
        var close = typeof(LoopbackTransport).GetMethod(nameof(LoopbackTransport.Close))!;
        Assert.Equal(typeof(NodeId), connect.GetParameters()[0].ParameterType);
        Assert.Equal(typeof(NodeId), close.GetParameters()[0].ParameterType);

        using var left = new Node(new NodeId(1, 2));
        using var right = new Node(new NodeId(3, 4));
        var loopback = new LoopbackTransport();
        loopback.Register(left);
        loopback.Register(right);
        loopback.Connect(left.NodeId, right.NodeId);
        Assert.Contains((left.NodeId, right.NodeId), loopback.Streams.Keys);
        loopback.Close(left.NodeId, right.NodeId);
        Assert.Empty(loopback.Streams);
    }

    [Fact]
    public void MergeAndBucketHelpers_UseTypedIdentities()
    {
        Assert.DoesNotContain(typeof(Merge).GetMethods(),
            method => method.Name == nameof(Merge.ApplyLifecycle) &&
                      method.GetParameters()[2].ParameterType == typeof(byte[]));
        Assert.DoesNotContain(typeof(BucketAccumulator).GetMethods(),
            method => method.Name == nameof(BucketAccumulator.BucketKey) &&
                      method.GetParameters()[0].ParameterType == typeof(byte[]));
    }

    [Fact]
    public void RecordDigestAndBoundedWriteHelpers_UseTypedRecordIds()
    {
        var digest = typeof(Node).GetMethod(nameof(Node.RecordDigestOf))!;
        Assert.Equal(typeof(RecordId), digest.GetParameters()[1].ParameterType);

        var allDigests = typeof(Node).GetMethod(nameof(Node.AllRecordDigests))!;
        Assert.Equal(typeof(Dictionary<RecordId, byte[]>), allDigests.ReturnType);

        var boundedWrite = typeof(BoundedNode).GetMethod(nameof(BoundedNode.WriteField))!;
        Assert.Equal(typeof(RecordId), boundedWrite.GetParameters()[3].ParameterType);
    }

    [Fact]
    public void ModelIdentityConstructors_UseTypedIds()
    {
        Assert.DoesNotContain(typeof(Record).GetConstructors(),
            constructor => constructor.GetParameters()[0].ParameterType == typeof(byte[]));
        Assert.DoesNotContain(typeof(PeerState).GetConstructors(),
            constructor => constructor.GetParameters()[0].ParameterType == typeof(byte[]));
        Assert.DoesNotContain(typeof(BatchResult).GetConstructors(),
            constructor => constructor.GetParameters()[0].ParameterType == typeof(byte[]));
        Assert.DoesNotContain(typeof(PeerState).GetMethods(),
            method => method.Name == "AckedFor" &&
                      method.GetParameters()[0].ParameterType != typeof(NodeId));
    }

    [Fact]
    public void Wal_ExposesAtomicCheckpointButNotRawResetPrimitives()
    {
        Assert.Null(typeof(Wal).GetMethod(nameof(Wal.Truncate)));
        Assert.Null(typeof(Wal).GetMethod(nameof(Wal.AppendCheckpoint)));
        Assert.Null(typeof(Wal).GetProperty(nameof(Wal.Sequence))!.GetSetMethod());
        Assert.NotNull(typeof(Wal).GetMethod(nameof(Wal.Checkpoint)));
    }

    [Fact]
    public void HlcClock_StateCannotBeMutatedOutsideTheAssembly()
    {
        Assert.Null(typeof(HlcClock).GetProperty(nameof(HlcClock.NowFn))!.GetSetMethod());
        Assert.Null(typeof(HlcClock).GetProperty(nameof(HlcClock.MaxSkewUs))!.GetSetMethod());
        Assert.Null(typeof(HlcClock).GetProperty(nameof(HlcClock.LastWall),
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance));
        Assert.Null(typeof(HlcClock).GetProperty(nameof(HlcClock.LastLogical),
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance));
        Assert.NotNull(typeof(HlcClock).GetMethod(nameof(HlcClock.Snapshot)));
    }

    [Fact]
    public void ProtocolLookupTables_AreReadOnly()
    {
        Assert.IsAssignableFrom<IReadOnlyDictionary<int, string>>(Frames.FrameTypes);
        Assert.IsAssignableFrom<IReadOnlyDictionary<string, ushort>>(Frames.ErrorCodes);
        Assert.Throws<NotSupportedException>(() =>
            ((IDictionary<int, string>)Frames.FrameTypes)[0x01] = "CORRUPTED");
        Assert.Throws<NotSupportedException>(() =>
            ((IDictionary<string, ushort>)Frames.ErrorCodes)["INTERNAL"] = 0xFFFF);
        Assert.Equal("HELLO", Frames.FrameTypes[0x01]);
        Assert.Equal((ushort)0x000A, Frames.ErrorCodes["INTERNAL"]);
    }

    [Fact]
    public void NodeConstruction_UsesTypedIdentityContract()
    {
        var constructor = typeof(Node).GetConstructors()
            .Single(candidate => candidate.GetParameters().Length == 5);
        Assert.Equal(typeof(NodeId?), constructor.GetParameters()[0].ParameterType);
        Assert.Equal(typeof(NodeId?), typeof(NodeOptions).GetProperty(nameof(NodeOptions.NodeId))!.PropertyType);
        Assert.DoesNotContain(typeof(NodeBuilder).GetMethods(),
            method => method.Name == "WithNodeId" &&
                      method.GetParameters()[0].ParameterType == typeof(byte[]));

        var expected = new NodeId(0x1234, 0x5678);
        using var node = new Node(expected);
        Assert.Equal(expected, node.NodeId);
        using var configured = new Node(new NodeOptions { NodeId = expected });
        Assert.Equal(expected, configured.NodeId);
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
