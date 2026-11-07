using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageSyncCoordinatorTests
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-coordinator-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task Sync_session_uses_durable_peer_reservations_for_sequential_batches()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-peer-session"), 420);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(18);
        using var receiver = NewNode(19);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, "durable-peer-session");

        RecordId firstRecord = RecordId.From(Guid.Parse("18000000-0000-0000-0000-000000000018").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            HlcWall = 1000, HlcLogical = 0, Value = "first"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        var firstReceive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, "durable-peer-session"));
        Assert.Equal(1, (await firstReceive).Applied);

        var firstState = metadata.GetPeerStreamState("durable-peer-session", sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(1UL, firstState.AckedCursor);
        Assert.Equal(1UL, firstState.PushedCursor);
        Assert.Equal((uint)sender.Epoch, firstState.IssuerEpoch);
        Assert.Null(firstState.PendingBatch);

        RecordId secondRecord = RecordId.From(Guid.Parse("19000000-0000-0000-0000-000000000019").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            HlcWall = 2000, HlcLogical = 0, Value = "second"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        var secondReceive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, "durable-peer-session"));
        Assert.Equal(1, (await secondReceive).Applied);

        var secondState = metadata.GetPeerStreamState("durable-peer-session", sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(2UL, secondState.AckedCursor);
        Assert.Equal(2UL, secondState.PushedCursor);
        Assert.Null(secondState.PendingBatch);
        Assert.True(receiver.TryGetField(1, firstRecord, 0x0101, out var firstValue));
        Assert.Equal("first", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.True(receiver.TryGetField(1, secondRecord, 0x0101, out var secondValue));
        Assert.Equal("second", Encoding.UTF8.GetString(secondValue!.Value));
    }

    [Fact]
    public void Failed_node_checkpoint_does_not_ack_feed_and_replay_does_not_duplicate_outbox()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 401, deleteOnDispose: true);
        Node node = NewNode(4);
        RecordId recordId = provider.ResolveRecordId(Guid.Parse("40000000-0000-0000-0000-000000000004").ToByteArray());
        ushort fieldId = provider.ResolveFieldId("value"u8.ToArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            AuthorNodeId = node.NodeId,
            HlcWall = 1000,
            HlcLogical = 2,
            KeyEpoch = 0,
            Value = Encoding.UTF8.GetBytes("durable")
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1);

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => throw new IOException("simulated checkpoint failure")));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1, node.GetOutboxCount(1));

        int published = coordinator.PublishLocalChanges(_ => { });
        Assert.Equal(1, published);
        Assert.Equal(1, node.GetOutboxCount(1));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }

    [Fact]
    public void Unpublished_local_field_edits_are_coalesced_before_node_outbox_freeze()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce"), 410);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node node = NewNode(17);
        RecordId record = RecordId.From(Guid.Parse("17000000-0000-0000-0000-000000000017").ToByteArray());
        const ushort fieldA = 0x0701;
        const ushort fieldB = 0x0702;

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            HlcWall = 1000, HlcLogical = 0, Value = "first"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldB, AuthorNodeId = node.NodeId,
            HlcWall = 1500, HlcLogical = 0, Value = "independent"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            HlcWall = 2000, HlcLogical = 0, Value = "second"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            HlcWall = 3000, HlcLogical = 0, Value = "final"u8.ToArray()
        });

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce");
        int consumed = coordinator.PublishLocalChanges(_ => { }, limit: 16);

        Assert.Equal(4, consumed);
        Assert.Equal(2, node.GetOutboxCount(1));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce", node.NodeId, 1, node.NodeId));
        Assert.True(provider.TryReadCurrent(record, fieldA, out var providerA));
        Assert.Equal("final", Encoding.UTF8.GetString(providerA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var providerB));
        Assert.Equal("independent", Encoding.UTF8.GetString(providerB!.Value));
        Assert.Empty(provider.ReadLocalChanges(0, 16).Changes);

        var outbox = node.ReadOutboxEntries(1, node.NodeId, 0, 16);
        var values = outbox.Select(entry => Ops.DecodeOp(entry.Payload).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Contains("final", values);
        Assert.Contains("independent", values);
        Assert.DoesNotContain("first", values);
        Assert.DoesNotContain("second", values);
    }

    [Fact]
    public void Pending_local_edits_are_coalesced_across_provider_page_boundaries()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce-pages"), 423);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(24);
        RecordId record = RecordId.From(Guid.Parse("24000000-0000-0000-0000-000000000024").ToByteArray());
        const ushort fieldA = 0x0711;
        const ushort fieldB = 0x0712;

        void Append(ushort field, ulong wall, string value) => provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = field, AuthorNodeId = node.NodeId,
            HlcWall = wall, HlcLogical = 0, Value = Encoding.UTF8.GetBytes(value)
        });

        Append(fieldA, 1000, "a1");
        Append(fieldB, 1500, "b-final");
        Append(fieldA, 2000, "a2");
        Append(fieldA, 3000, "a-final");
        Append(fieldB, 2500, "b-old");

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce-pages");
        Assert.Equal(5, coordinator.PublishLocalChanges(_ => { }, limit: 2));

        Assert.Equal(2, node.GetOutboxCount(1));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce-pages", node.NodeId, 1, node.NodeId));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.True(provider.TryReadCurrent(record, fieldA, out var valueA));
        Assert.Equal("a-final", Encoding.UTF8.GetString(valueA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var valueB));
        Assert.Equal("b-final", Encoding.UTF8.GetString(valueB!.Value));

        var values = node.ReadOutboxEntries(1, node.NodeId, 0, 10)
            .Select(entry => Ops.DecodeOp(entry.Payload).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Contains("a-final", values);
        Assert.Contains("b-final", values);
        Assert.DoesNotContain("a1", values);
        Assert.DoesNotContain("a2", values);
        Assert.DoesNotContain("b-old", values);
    }

    [Fact]
    public void Remote_node_materialization_is_durable_but_does_not_create_local_feed_entries()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 402, deleteOnDispose: true);
        Node remoteNode = NewNode(5);
        RecordId recordId = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000005").ToByteArray());
        const ushort fieldId = 0x010C;
        remoteNode.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("remote"), new Hlc(2000, 1));

        var coordinator = new StorageSyncCoordinator(remoteNode, provider, 1);
        var receipt = coordinator.ApplyRemoteFields(new[] { (recordId, fieldId) });

        Assert.NotNull(receipt);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("remote", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(remoteNode.NodeId, current.AuthorNodeId);
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }


    [Fact]
    public void Provider_commit_before_ack_survives_receiver_restart_and_duplicate_batch_retry()
    {
        string nodePath = Path.Combine(_root, "receiver.node");
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 403, deleteOnDispose: true);
        Node sender = NewNode(6);
        Node receiver = NewNode(7);
        RecordId recordId = RecordId.From(Guid.Parse("60000000-0000-0000-0000-000000000006").ToByteArray());
        const ushort fieldId = 0x0101;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("committed"), new Hlc(3000, 1));

        Transport.MakeSession(sender, receiver);
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);

        Assert.Throws<IOException>(() => Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, _) =>
            {
                // Receiver protocol receipt is durable before external provider apply.
                NodePersistence.Save(target, nodePath);
                coordinator.ApplyRemoteBatch(batch);
                // Simulate process loss after provider commit but before sender ACK.
                throw new IOException("simulated crash before ACK");
            }));

        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var committed));
        Assert.Equal("committed", Encoding.UTF8.GetString(committed!.Value));

        // Advance provider state after the original batch committed. Replaying the
        // old immutable batch must hit its original receipt and must not fingerprint
        // the provider's newer current winner as if it were the old batch payload.
        provider.ApplyRemoteBatch(Guid.Parse("00000000-0000-0000-0000-000000000403"),
            new[]
            {
                new ProviderMutation
                {
                    RecordId = recordId,
                    FieldId = fieldId,
                    HlcWall = 4000,
                    HlcLogical = 0,
                    AuthorNodeId = sender.NodeId,
                    Value = Encoding.UTF8.GetBytes("newer")
                }
            });

        receiver = NodePersistence.Load(nodePath);
        Transport.MakeSession(sender, receiver);
        coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var retry = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, result) =>
            {
                Assert.True(result.Duplicate);
                var receipt = coordinator.ApplyRemoteBatch(batch);
                Assert.True(receipt!.IsDuplicate);
                NodePersistence.Save(target, nodePath);
            });

        Assert.NotNull(retry);
        Assert.True(retry!.Value.Duplicate);
        Assert.Null(Transport.DeliverBatch(sender, receiver, 1));
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var latest));
        Assert.Equal("newer", Encoding.UTF8.GetString(latest!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }


    [Fact]
    public void Received_batch_materializes_multiple_immutable_updates_to_same_field()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "multi-update"), 404, deleteOnDispose: true);
        Node sender = NewNode(8);
        Node receiver = NewNode(9);
        RecordId recordId = RecordId.From(Guid.Parse("80000000-0000-0000-0000-000000000008").ToByteArray());
        const ushort fieldId = 0x0201;

        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("one"), new Hlc(1000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("two"), new Hlc(2000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("three"), new Hlc(3000, 0));

        Transport.MakeSession(sender, receiver);
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var delivered = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));

        Assert.NotNull(delivered);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("three", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }


    [Fact]
    public void Metadata_store_pending_apply_and_provider_commit_are_coordinated_before_ack()
    {
        string directory = Path.Combine(_root, "shared-metadata-provider");
        using var storage = StorageDatabase.Open(directory, 405);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node sender = NewNode(10);
        Node receiver = NewNode(11);
        RecordId recordId = RecordId.From(Guid.Parse("a0000000-0000-0000-0000-00000000000a").ToByteArray());
        const ushort fieldId = 0x0301;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("metadata-backed"), new Hlc(5000, 3));
        Transport.MakeSession(sender, receiver);

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "metadata-integration");
        var delivered = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, _) =>
            {
                var receipt = coordinator.ApplyRemoteBatch(batch);
                Assert.NotNull(receipt);
                NodePersistence.Save(target, Path.Combine(_root, "metadata-receiver.node"));
            });

        Assert.NotNull(delivered);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("metadata-backed", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(1UL, metadata.GetReceiveCursor("metadata-integration", receiver.NodeId,
            sender.NodeId, checked((uint)sender.Epoch), 1));
        Assert.Equal(1UL, metadata.GetRelayHighWatermark("metadata-integration", receiver.NodeId,
            1, sender.NodeId));
        var relay = metadata.ReadRelay("metadata-integration", receiver.NodeId, 1, sender.NodeId, 0, 10);
        Assert.Single(relay.Items);
        Assert.Equal(recordId, RecordId.From(relay.Items[0].Payload.AsSpan(0, 16)));
        Assert.Empty(metadata.ReadPendingApplyIntents("metadata-integration", receiver.NodeId));
        Assert.Equal(0, coordinator.RecoverPendingApplies());
    }


    [Fact]
    public void Metadata_receive_cursors_are_independent_for_two_author_streams_from_one_peer()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "author-stream-cursors"), 406, deleteOnDispose: true);
        NodeId local = NodeId.From(Guid.Parse("b0000000-0000-0000-0000-00000000000b").ToByteArray());
        NodeId sender = NodeId.From(Guid.Parse("c0000000-0000-0000-0000-00000000000c").ToByteArray());
        NodeId authorA = NodeId.From(Guid.Parse("d0000000-0000-0000-0000-00000000000d").ToByteArray());
        NodeId authorB = NodeId.From(Guid.Parse("e0000000-0000-0000-0000-00000000000e").ToByteArray());

        MetadataIncomingBatch Create(NodeId author, byte batchSeed, ulong cursor, byte[] previous, byte[] digest)
            => new()
            {
                ScopeId = "author-stream-test",
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = 1,
                AuthorStreamNodeId = author,
                NamespaceId = 1,
                BatchId = BatchId.From(Guid.Parse($"00000000-0000-0000-0000-{batchSeed:x12}").ToByteArray()),
                CursorStart = cursor,
                CursorEnd = cursor,
                PreviousBatchDigest = previous,
                BatchDigest = digest,
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = author,
                        ChangeId = BatchId.From(Guid.Parse($"10000000-0000-0000-0000-{batchSeed:x12}").ToByteArray()),
                        Fingerprint = Enumerable.Repeat(batchSeed, 16).ToArray(),
                        Payload = new[] { batchSeed }
                    }
                }
            };

        byte[] zero = new byte[16];
        byte[] digestA1 = Enumerable.Repeat((byte)0xA1, 16).ToArray();
        byte[] digestB1 = Enumerable.Repeat((byte)0xB1, 16).ToArray();
        store.AcceptIncomingBatch(Create(authorA, 1, 1, zero, digestA1));
        store.AcceptIncomingBatch(Create(authorB, 2, 1, digestA1, digestB1));

        byte[] digestA2 = Enumerable.Repeat((byte)0xA2, 16).ToArray();
        byte[] digestB2 = Enumerable.Repeat((byte)0xB2, 16).ToArray();
        store.AcceptIncomingBatch(Create(authorA, 3, 2, digestB1, digestA2));
        store.AcceptIncomingBatch(Create(authorB, 4, 2, digestA2, digestB2));

        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1, authorA));
        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1, authorB));
        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1));
    }


    [Fact]
    public void Coordinator_recovers_persisted_metadata_intent_and_provider_apply_after_restart()
    {
        string directory = Path.Combine(_root, "pending-apply-recovery");
        Node local = NewNode(12);
        Node sender = NewNode(13);
        RecordId recordId = RecordId.From(Guid.Parse("c1000000-0000-0000-0000-00000000000c").ToByteArray());
        const ushort fieldId = 0x0401;
        byte[] value = Encoding.UTF8.GetBytes("recovered");
        var op = new OpFieldWrite
        {
            FieldId = fieldId,
            KeyEpoch = 0,
            Wall = 6000,
            Logical = 4,
            AuthorNodeId = sender.NodeId,
            Value = value
        };
        byte[] operationPayload = Ops.EncodeOpFull(op);
        byte[] envelope = new byte[16 + operationPayload.Length];
        recordId.WriteTo(envelope);
        operationPayload.CopyTo(envelope, 16);
        BatchId changeId = BatchId.From(Guid.Parse("c2000000-0000-0000-0000-00000000000c").ToByteArray());
        byte[] fingerprint = ChangeIdentity.Fingerprint(1, recordId, OpsConst.TagFieldWrite,
            op, sender.NodeId).ToArray();
        var recordTombstone = new OpRecordTombstone
        {
            RecordId = recordId,
            Wall = 7000,
            Logical = 5,
            AuthorNodeId = sender.NodeId
        };
        byte[] tombstonePayload = Ops.EncodeOpFull(recordTombstone);
        byte[] tombstoneEnvelope = new byte[16 + tombstonePayload.Length];
        recordId.WriteTo(tombstoneEnvelope);
        tombstonePayload.CopyTo(tombstoneEnvelope, 16);
        byte[] tombstoneFingerprint = ChangeIdentity.Fingerprint(1, recordId,
            OpsConst.TagRecordTomb, recordTombstone, sender.NodeId).ToArray();
        byte[] batchDigest = Enumerable.Repeat((byte)0xC3, 16).ToArray();
        Guid applyToken = Guid.Parse("c3000000-0000-0000-0000-00000000000c");

        using (var storage = StorageDatabase.Open(directory, 407))
        using (var metadata = new StorageMetadataStore(storage))
        {
            metadata.BeginPendingApply(new MetadataIncomingBatch
            {
                ScopeId = "pending-recovery",
                LocalReplica = local.NodeId,
                SenderPeer = sender.NodeId,
                SenderEpoch = checked((uint)sender.Epoch),
                AuthorStreamNodeId = sender.NodeId,
                NamespaceId = 1,
                BatchId = BatchId.From(Guid.Parse("c4000000-0000-0000-0000-00000000000c").ToByteArray()),
                CursorStart = 1,
                CursorEnd = 2,
                PreviousBatchDigest = new byte[16],
                BatchDigest = batchDigest,
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = sender.NodeId,
                        ChangeId = changeId,
                        Fingerprint = fingerprint,
                        Payload = envelope
                    },
                    new MetadataOperation
                    {
                        AuthorNodeId = sender.NodeId,
                        ChangeId = BatchId.From(Guid.Parse("c5000000-0000-0000-0000-00000000000c").ToByteArray()),
                        Fingerprint = tombstoneFingerprint,
                        Payload = tombstoneEnvelope
                    }
                }
            }, applyToken);
        }

        using (var storage = StorageDatabase.Open(directory, 407))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        {
            var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, "pending-recovery");
            Assert.Equal(1, coordinator.RecoverPendingApplies());
            Assert.Equal(0, coordinator.RecoverPendingApplies());
            Assert.False(provider.TryReadCurrent(recordId, fieldId, out _));
            var snapshot = provider.OpenSnapshot();
            try
            {
                var page = provider.ReadSnapshot(snapshot.Token, 0, 10);
                Assert.Contains(page.Entries, entry => entry.IsRecordTombstone);
                Assert.Contains(page.Entries, entry =>
                    !entry.IsRecordTombstone && Encoding.UTF8.GetString(entry.Value) == "recovered");
            }
            finally
            {
                provider.ReleaseSnapshot(snapshot.Token);
            }
            Assert.Equal(2UL, metadata.GetReceiveCursor("pending-recovery", local.NodeId,
                sender.NodeId, checked((uint)sender.Epoch), 1, sender.NodeId));
            Assert.Empty(metadata.ReadPendingApplyIntents("pending-recovery", local.NodeId));
        }
    }


    [Fact]
    public void Received_record_tombstone_and_newer_ghost_field_materialize_with_lifecycle_ordering()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "record-lifecycle"), 408, deleteOnDispose: true);
        Node sender = NewNode(14);
        Node receiver = NewNode(15);
        RecordId recordId = RecordId.From(Guid.Parse("d1000000-0000-0000-0000-000000000001").ToByteArray());
        const ushort fieldId = 0x0501;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("before"), new Hlc(1000, 0));
        Assert.True(sender.DeleteRecord(1, recordId, new Hlc(2000, 0)));
        Transport.MakeSession(sender, receiver);

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var deleted = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));
        Assert.NotNull(deleted);
        Assert.False(provider.TryReadCurrent(recordId, fieldId, out _));

        Assert.True(sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("ghost"), new Hlc(3000, 0)));
        var ghost = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));
        Assert.NotNull(ghost);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("ghost", Encoding.UTF8.GetString(current!.Value));
    }


    [Fact]
    public void Conflicting_metadata_freeze_does_not_ack_local_provider_feed()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "freeze-conflict"), 409);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node node = NewNode(16);
        NodeId foreignAuthor = NodeId.From(Guid.Parse("f3000000-0000-0000-0000-000000000003").ToByteArray());
        RecordId recordId = RecordId.From(Guid.Parse("f4000000-0000-0000-0000-000000000004").ToByteArray());
        const ushort fieldId = 0x0601;
        byte[] value = Encoding.UTF8.GetBytes("same-operation");

        Assert.True(node.WriteField(1, recordId, fieldId, value, new Hlc(8000, 1)));
        OutboxEntry localEntry = Assert.Single(node.ReadOutboxEntries(1, node.NodeId, 0, 10));

        metadata.AcceptIncomingBatch(new MetadataIncomingBatch
        {
            ScopeId = "freeze-conflict",
            LocalReplica = node.NodeId,
            SenderPeer = foreignAuthor,
            SenderEpoch = 1,
            AuthorStreamNodeId = foreignAuthor,
            NamespaceId = 1,
            BatchId = BatchId.From(Guid.Parse("f5000000-0000-0000-0000-000000000005").ToByteArray()),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0xF5, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = foreignAuthor,
                    ChangeId = localEntry.ChangeId,
                    Fingerprint = Enumerable.Repeat((byte)0xA5, 16).ToArray(),
                    Payload = new byte[] { 0x01 }
                }
            }
        });

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            AuthorNodeId = node.NodeId,
            HlcWall = 8000,
            HlcLogical = 1,
            Value = value
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "freeze-conflict");

        Assert.Throws<InvalidDataException>(() =>
            coordinator.PublishLocalChanges(_ => { }));

        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark(
            "freeze-conflict", node.NodeId, 1, node.NodeId));
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
}
