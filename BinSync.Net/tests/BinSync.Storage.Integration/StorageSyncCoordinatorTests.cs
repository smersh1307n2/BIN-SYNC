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
    public async Task Pending_receive_intent_replays_wire_batch_into_node_and_provider_after_interruption()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-receive-recovery"), 427);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(28);
        using var receiver = NewNode(29);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "pending-receive-recovery");

        RecordId record = RecordId.From(Guid.Parse("28000000-0000-0000-0000-000000000028").ToByteArray());
        sender.WriteField(1, record, 0x0812, "replay-after-crash"u8.ToArray(), new Hlc(5000, 0));

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = acceptor.ReceiveBatchAsync(receiver,
            beforeApply: (batch, payload, peer) =>
            {
                coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                throw new InvalidOperationException("simulated interruption after durable intent");
            },
            beforeAck: null,
            ct: receiveCts.Token,
            receiveStateOwnedByMetadataStore: true);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        Assert.Single(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.False(receiver.TryGetField(1, record, 0x0812, out _));
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.RehydrateNodeFromProviderSnapshot(_ =>
                throw new InvalidOperationException("simulated interruption after provider snapshot restore")));
        Assert.Single(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.False(receiver.TryGetField(1, record, 0x0812, out _));
        Assert.False(provider.TryReadCurrent(record, 0x0812, out _));

        Assert.Equal(1, coordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0812, out var nodeValue));
        Assert.Equal("replay-after-crash", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0812, out var providerValue));
        Assert.Equal("replay-after-crash", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task Receive_batch_stages_metadata_intent_before_node_and_provider_apply()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-receive-intent"), 425);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(26);
        using var receiver = NewNode(27);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "durable-receive-intent");

        RecordId record = RecordId.From(Guid.Parse("26000000-0000-0000-0000-000000000026").ToByteArray());
        sender.WriteField(1, record, 0x0811, "received-durably"u8.ToArray(), new Hlc(4000, 0));

        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(sender, 1));
        BatchResult result = await receive;

        Assert.Equal(1, result.Applied);
        Assert.True(receiver.TryGetField(1, record, 0x0811, out var nodeValue));
        Assert.Equal("received-durably", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0811, out var providerValue));
        Assert.Equal("received-durably", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.Empty(metadata.ReadPendingApplyIntents("durable-receive-intent", receiver.NodeId));
        Assert.Equal(new[] { sender.NodeId }, metadata.ReadRelayAuthors("durable-receive-intent", receiver.NodeId, 1));
    }

    [Fact]
    public async Task Metadata_store_owns_receive_cursor_and_digest_after_node_recreation()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-receive-authority"), 451);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(50);
        const string scope = "metadata-receive-authority";
        RecordId firstRecord = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000050").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("51000000-0000-0000-0000-000000000051").ToByteArray());

        sender.WriteField(1, firstRecord, 0x0815, "first-durable-batch"u8.ToArray(), new Hlc(5600, 0));
        {
            using var firstReceiver = NewNode(51);
            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(firstReceiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            var coordinator = new StorageSyncCoordinator(firstReceiver, provider, 1, metadata, scope);

            Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
            Assert.True(await initiator.PushBatchAsync(sender, 1));
            BatchResult result = await receive;
            Assert.Equal(1, result.Applied);
        }

        // This receiver has the same stable node identity but none of the previous
        // Node-local InboxAuthors, PeerStates or LastBatchDigest dictionaries.
        sender.WriteField(1, secondRecord, 0x0815, "second-durable-batch"u8.ToArray(), new Hlc(5700, 0));
        using var secondReceiver = NewNode(51);
        var (nextToSender, nextToReceiver) = PipeTransport.CreatePair();
        var nextAcceptTask = SyncSession.AcceptAsync(secondReceiver, nextToReceiver);
        var nextConnectTask = SyncSession.ConnectAsync(sender, nextToSender);
        await using var nextAcceptor = await nextAcceptTask;
        await using var nextInitiator = await nextConnectTask;
        var nextCoordinator = new StorageSyncCoordinator(secondReceiver, provider, 1, metadata, scope);

        Task<BatchResult> nextReceive = nextCoordinator.ReceiveBatchBeforeAckAsync(nextAcceptor, _ => { });
        Assert.True(await nextInitiator.PushBatchAsync(sender, 1));
        BatchResult nextResult = await nextReceive;

        Assert.Equal(1, nextResult.Applied);
        Assert.Equal(2UL, metadata.GetReceiveCursor(scope, secondReceiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.True(secondReceiver.TryGetField(1, secondRecord, 0x0815, out var nodeValue));
        Assert.Equal("second-durable-batch", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(firstRecord, 0x0815, out var firstValue));
        Assert.Equal("first-durable-batch", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.True(provider.TryReadCurrent(secondRecord, 0x0815, out var secondValue));
        Assert.Equal("second-durable-batch", Encoding.UTF8.GetString(secondValue!.Value));
    }

    [Fact]
    public async Task Lost_ack_after_metadata_finalize_replays_as_durable_duplicate()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-lost-ack"), 452);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(52);
        using var receiver = NewNode(53);
        const string scope = "metadata-lost-ack";
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("52000000-0000-0000-0000-000000000052").ToByteArray());
        sender.WriteField(1, record, 0x0816, "committed-before-lost-ack"u8.ToArray(), new Hlc(5800, 0));

        StorageRemoteApplyPlan? firstPlan = null;
        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        Task<BatchResult> firstReceive = acceptor.ReceiveBatchAsync(receiver,
            beforeApply: (batch, payload, peer) =>
                firstPlan = coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer),
            beforeAck: (batch, _) =>
            {
                var plan = firstPlan ?? throw new InvalidOperationException("Pending intent was not staged.");
                coordinator.ApplyRemoteBatchAfterNodeApply(batch, plan, _ => { });
                throw new InvalidOperationException("simulated ACK loss after durable metadata finalization");
            },
            ct: receiveCts.Token,
            receiveStateOwnedByMetadataStore: true);
        Task<bool> firstSend = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await firstReceive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await firstSend);
        Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));

        Task<BatchResult> replayReceive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(sender, 1));
        BatchResult replayResult = await replayReceive;

        Assert.True(replayResult.Duplicate);
        Assert.Equal(0, replayResult.Applied);
        Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(provider.TryReadCurrent(record, 0x0816, out var value));
        Assert.Equal("committed-before-lost-ack", Encoding.UTF8.GetString(value!.Value));
    }

    [Fact]
    public async Task Provider_failure_after_node_apply_keeps_pending_intent_for_recovery()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-failure-after-node"), 441);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(42);
        using var receiver = NewNode(43);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "provider-failure-after-node";
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("42000000-0000-0000-0000-000000000042").ToByteArray());
        sender.WriteField(1, record, 0x0813, "node-applied-provider-pending"u8.ToArray(), new Hlc(5200, 0));

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => provider.Dispose(), receiveCts.Token);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        Assert.Single(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0813, out var nodeValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(nodeValue!.Value));

        using var recoveredProvider = new StorageDataProvider(storage);
        var recoveredCoordinator = new StorageSyncCoordinator(receiver, recoveredProvider, 1, metadata, scope);
        Assert.Equal(1, recoveredCoordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0813, out var restoredNodeValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(restoredNodeValue!.Value));
        Assert.True(recoveredProvider.TryReadCurrent(record, 0x0813, out var providerValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task Provider_commit_before_metadata_finalize_is_recovered_idempotently()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-commit-before-metadata"), 442);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(44);
        using var receiver = NewNode(45);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "provider-commit-before-metadata";
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("44000000-0000-0000-0000-000000000044").ToByteArray());
        sender.WriteField(1, record, 0x0814, "provider-committed-metadata-pending"u8.ToArray(), new Hlc(5400, 0));

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => metadata.Dispose(), receiveCts.Token);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        Assert.True(provider.TryReadCurrent(record, 0x0814, out var committedValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(committedValue!.Value));

        using var recoveredMetadata = new StorageMetadataStore(storage);
        using var recoveredProvider = new StorageDataProvider(storage);
        Assert.Single(recoveredMetadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        var recoveredCoordinator = new StorageSyncCoordinator(receiver, recoveredProvider, 1, recoveredMetadata, scope);
        Assert.Equal(1, recoveredCoordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(recoveredMetadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0814, out var restoredNodeValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(restoredNodeValue!.Value));
        Assert.True(recoveredProvider.TryReadCurrent(record, 0x0814, out var providerValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task New_peer_incarnation_retires_old_epoch_without_inheriting_its_ack_cursor()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "peer-new-incarnation"), 439);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var local = NewNode(40);
        byte[] peerBytes = new byte[16];
        peerBytes[0] = 41;
        peerBytes[15] = (byte)(41 ^ 0xA5);
        using var peer = new NodeBuilder().WithNodeId(peerBytes).WithEpoch(2).Build();
        const string scope = "peer-new-incarnation";
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("40000000-0000-0000-0000-000000000040").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0104, AuthorNodeId = local.NodeId,
            HlcWall = 4000, Value = "must-replay-for-new-incarnation"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });

        metadata.RegisterInterestedPeer(scope, local.NodeId, peer.NodeId, 1, 1);
        metadata.CompletePeerInitialSync(scope, local.NodeId, peer.NodeId, 1, 1,
            new[] { new MetadataPeerStreamBaseline(local.NodeId, 0, checked((uint)local.Epoch)) });
        var oldEpochBatch = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Guid.Parse("a2000000-0000-0000-0000-0000000000a2").ToByteArray()),
            IssuerEpoch = 1,
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)161, 16).ToArray(),
            Payload = new byte[] { 162 },
            StreamOffset = 0
        };
        metadata.ReserveOutboundBatch(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch);
        metadata.MarkOutboundBatchPushed(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch.BatchId);
        metadata.AcknowledgeOutboundBatch(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch.BatchId, 1, oldEpochBatch.BatchDigest);
        Assert.Equal(1UL, metadata.GetPeerStreamState(scope, local.NodeId,
            peer.NodeId, 1, 1, local.NodeId).AckedCursor);

        var (toLocal, toPeer) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(peer, toPeer);
        var connectTask = SyncSession.ConnectAsync(local, toLocal);
        await using var acceptor = await acceptTask;
        await using var session = await connectTask;

        coordinator.RegisterInterestedPeer(session, 1);
        metadata.CompletePeerInitialSync(scope, local.NodeId, peer.NodeId, 2, 1,
            new[] { new MetadataPeerStreamBaseline(local.NodeId, 0, checked((uint)local.Epoch)) });

        var newEpochState = metadata.GetPeerStreamState(scope, local.NodeId,
            peer.NodeId, 2, 1, local.NodeId);
        Assert.Equal(0UL, newEpochState.AckedCursor);
        Assert.Equal(0UL, newEpochState.PushedCursor);
        Assert.Equal(0UL, newEpochState.StreamOffset);
        Assert.Equal(checked((uint)local.Epoch), newEpochState.IssuerEpoch);
        Assert.Equal(1U, metadata.GetRetiredPeerEpoch(scope, local.NodeId, peer.NodeId, 1));
        Assert.Equal(2U, Assert.Single(metadata.ReadInterestedPeers(scope, local.NodeId, 1)).PeerEpoch);
        Assert.Single(metadata.ReadOutboundForPeer(scope, local.NodeId,
            peer.NodeId, 2, 1, local.NodeId, 10).Items);
    }

    [Fact]
    public async Task Three_replica_relay_retains_one_shared_payload_until_every_downstream_peer_acks()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "relay-three-a"), 434);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "relay-three-b"), 435);
        using var storageC = StorageDatabase.Open(Path.Combine(_root, "relay-three-c"), 436);
        using var storageD = StorageDatabase.Open(Path.Combine(_root, "relay-three-d"), 437);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var providerC = new StorageDataProvider(storageC);
        using var providerD = new StorageDataProvider(storageD);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var metadataC = new StorageMetadataStore(storageC);
        using var metadataD = new StorageMetadataStore(storageD);
        using var nodeA = NewNode(36);
        using var nodeB = NewNode(37);
        using var nodeC = NewNode(38);
        using var nodeD = NewNode(39);
        const string scopeA = "relay-three-a";
        const string scopeB = "relay-three-b";
        const string scopeC = "relay-three-c";
        const string scopeD = "relay-three-d";
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, scopeA);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, scopeB);
        var coordinatorC = new StorageSyncCoordinator(nodeC, providerC, 1, metadataC, scopeC);
        var coordinatorD = new StorageSyncCoordinator(nodeD, providerD, 1, metadataD, scopeD);

        var (aToB, bFromA) = PipeTransport.CreatePair();
        var bAcceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var aConnectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var bAccept = await bAcceptTask;
        await using var aConnect = await aConnectTask;

        RecordId record = RecordId.From(Guid.Parse("36000000-0000-0000-0000-000000000036").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0103, AuthorNodeId = nodeA.NodeId,
            HlcWall = 3600, Value = "shared-relay-payload"u8.ToArray()
        });
        coordinatorA.PublishLocalChanges(_ => { });
        coordinatorA.RegisterInterestedPeer(aConnect, 1);
        metadataA.CompletePeerInitialSync(scopeA, nodeA.NodeId, nodeB.NodeId,
            checked((uint)nodeB.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
        var receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(bAccept, _ => { });
        Assert.True(await aConnect.PushBatchAsync(nodeA, 1, metadataA, scopeA));
        Assert.Equal(1, (await receiveAtB).Applied);
        Assert.Single(metadataB.ReadRelay(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 0, 10).Items);

        // D is durably interested but offline. C's ACK alone must not release the
        // single shared relay payload needed by D.
        metadataB.RegisterInterestedPeer(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1);
        metadataB.CompletePeerInitialSync(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });

        var (bToC, cFromB) = PipeTransport.CreatePair();
        var cAcceptTask = SyncSession.AcceptAsync(nodeC, cFromB);
        var bConnectCTask = SyncSession.ConnectAsync(nodeB, bToC);
        await using var cAccept = await cAcceptTask;
        await using var bConnectC = await bConnectCTask;
        coordinatorB.RegisterInterestedPeer(bConnectC, 1);
        metadataB.CompletePeerInitialSync(scopeB, nodeB.NodeId, nodeC.NodeId,
            checked((uint)nodeC.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
        var receiveAtC = coordinatorC.ReceiveBatchBeforeAckAsync(cAccept, _ => { });
        Assert.True(await bConnectC.PushBatchAsync(nodeB, 1, metadataB, scopeB));
        Assert.Equal(1, (await receiveAtC).Applied);
        Assert.True(providerC.TryReadCurrent(record, 0x0103, out var cValue));
        Assert.Equal("shared-relay-payload", Encoding.UTF8.GetString(cValue!.Value));
        Assert.Equal(0UL, metadataB.GetSafePeerStreamCompactionCursor(scopeB,
            nodeB.NodeId, 1, nodeA.NodeId));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            metadataB.CompactRelayThrough(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 1));

        var (bToD, dFromB) = PipeTransport.CreatePair();
        var dAcceptTask = SyncSession.AcceptAsync(nodeD, dFromB);
        var bConnectDTask = SyncSession.ConnectAsync(nodeB, bToD);
        await using var dAccept = await dAcceptTask;
        await using var bConnectD = await bConnectDTask;
        coordinatorB.RegisterInterestedPeer(bConnectD, 1);
        var receiveAtD = coordinatorD.ReceiveBatchBeforeAckAsync(dAccept, _ => { });
        Assert.True(await bConnectD.PushBatchAsync(nodeB, 1, metadataB, scopeB));
        Assert.Equal(1, (await receiveAtD).Applied);
        Assert.True(providerD.TryReadCurrent(record, 0x0103, out var dValue));
        Assert.Equal("shared-relay-payload", Encoding.UTF8.GetString(dValue!.Value));

        Assert.Equal(1UL, metadataB.GetSafePeerStreamCompactionCursor(scopeB,
            nodeB.NodeId, 1, nodeA.NodeId));
        metadataB.CompactRelayThrough(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 1);
        Assert.Empty(metadataB.ReadRelayForPeer(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1, nodeA.NodeId, 10).Items);
    }

    [Fact]
    public async Task Storage_initial_reconcile_persists_snapshot_and_commits_durable_feed_baseline()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "snapshot-baseline-a"), 441);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "snapshot-baseline-b"), 442);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = NewNode(44);
        using var nodeB = NewNode(45);
        const string scopeA = "snapshot-baseline-a";
        const string scopeB = "snapshot-baseline-b";
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, scopeA);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, scopeB);
        var (aToB, bFromA) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var connectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        RecordId record = RecordId.From(Guid.Parse("44000000-0000-0000-0000-000000000044").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0104, AuthorNodeId = nodeA.NodeId,
            HlcWall = 4400, Value = "snapshot-baseline"u8.ToArray()
        });

        await Task.WhenAll(
            coordinatorA.AutoReconcileWithMetadataAsync(initiator, _ => { }, initiate: true),
            coordinatorB.AutoReconcileWithMetadataAsync(acceptor, _ => { }, initiate: false));

        Assert.True(metadataA.IsPeerInitialSyncComplete(scopeA, nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1));
        Assert.True(metadataB.IsPeerInitialSyncComplete(scopeB, nodeB.NodeId,
            nodeA.NodeId, checked((uint)nodeA.Epoch), 1));
        Assert.True(providerB.TryReadCurrent(record, 0x0104, out var snapshotValue));
        Assert.Equal("snapshot-baseline", Encoding.UTF8.GetString(snapshotValue!.Value));
        Assert.Equal(1UL, metadataA.GetSafePeerStreamCompactionCursor(scopeA, nodeA.NodeId, 1, nodeA.NodeId));
        Assert.Empty(metadataA.ReadOutboundForPeer(scopeA, nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1, nodeA.NodeId, 10).Items);

        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0105, AuthorNodeId = nodeA.NodeId,
            HlcWall = 4500, Value = "after-snapshot"u8.ToArray()
        });
        coordinatorA.PublishLocalChanges(_ => { });
        var receive = coordinatorB.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(nodeA, 1, metadataA, scopeA));
        Assert.Equal(1, (await receive).Applied);
        Assert.True(providerB.TryReadCurrent(record, 0x0105, out var deltaValue));
        Assert.Equal("after-snapshot", Encoding.UTF8.GetString(deltaValue!.Value));
        Assert.Equal(2UL, metadataA.GetSafePeerStreamCompactionCursor(scopeA, nodeA.NodeId, 1, nodeA.NodeId));
    }

    [Fact]
    public async Task Initial_reconcile_rehydrates_empty_node_from_provider_snapshot_after_feed_ack()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "provider-rehydrate-a"), 443);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "provider-rehydrate-b"), 444);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = NewNode(46);
        using var nodeB = NewNode(47);
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, "provider-rehydrate-a");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, "provider-rehydrate-b");
        var (aToB, bFromA) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var connectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        RecordId record = RecordId.From(Guid.Parse("46000000-0000-0000-0000-000000000046").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0106, AuthorNodeId = nodeA.NodeId,
            HlcWall = 4600, Value = "provider-owned"u8.ToArray()
        });
        var seededFeed = providerA.ReadLocalChanges(0, 16);
        providerA.AcknowledgeLocalChanges(seededFeed.ScannedThroughSequence);
        Assert.True(nodeA.IsPristine);

        await Task.WhenAll(
            coordinatorA.AutoReconcileWithMetadataAsync(initiator, _ => { }, initiate: true),
            coordinatorB.AutoReconcileWithMetadataAsync(acceptor, _ => { }, initiate: false));

        Assert.True(nodeA.TryGetField(1, record, 0x0106, out var nodeValue));
        Assert.Equal("provider-owned", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(providerB.TryReadCurrent(record, 0x0106, out var providerValue));
        Assert.Equal("provider-owned", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.True(metadataA.IsPeerInitialSyncComplete("provider-rehydrate-a", nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1));
    }

    [Fact]
    public async Task Durable_push_materializes_metadata_without_mutating_node_outbox_before_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-materialization-isolated"), 433);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(34);
        using var receiver = NewNode(35);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "metadata-materialization-isolated";
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("34000000-0000-0000-0000-000000000034").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0102, AuthorNodeId = sender.NodeId,
            HlcWall = 3400, Value = "metadata-source"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        coordinator.RegisterInterestedPeer(initiator, 1);
        metadata.CompletePeerInitialSync(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(sender.NodeId, 0, checked((uint)sender.Epoch)) });
        var before = sender.ReadOutboxEntries(1, sender.NodeId, 0, 16)
            .Select(entry => (entry.Seq, entry.ChangeId, Payload: entry.Payload.ToArray())).ToArray();

        using var cts = new CancellationTokenSource();
        var receive = acceptor.ReceiveBatchAsync(receiver, ct: CancellationToken.None,
            beforeAck: (_, _) => throw new InvalidOperationException("stop before ACK"));
        var push = initiator.PushBatchAsync(sender, 1, metadata, scope, ct: cts.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await push);

        var after = sender.ReadOutboxEntries(1, sender.NodeId, 0, 16)
            .Select(entry => (entry.Seq, entry.ChangeId, Payload: entry.Payload.ToArray())).ToArray();
        Assert.Equal(before.Length, after.Length);
        for (int i = 0; i < before.Length; i++)
        {
            Assert.Equal(before[i].Seq, after[i].Seq);
            Assert.Equal(before[i].ChangeId, after[i].ChangeId);
            Assert.Equal(before[i].Payload, after[i].Payload);
        }
        var state = metadata.GetPeerStreamState(scope, sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(1UL, state.PushedCursor);
        Assert.Equal(0UL, state.AckedCursor);
        Assert.NotNull(state.PendingBatch);
    }

    [Fact]
    public async Task Durable_batch_reservation_replays_identically_after_sender_restart_and_lost_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-replay-after-restart"), 434);
        using var provider = new StorageDataProvider(storage);
        using var originalSender = NewNode(36);
        using var receiver = NewNode(37);
        var metadata = new StorageMetadataStore(storage);
        const string scope = "durable-replay-after-restart";
        var coordinator = new StorageSyncCoordinator(originalSender, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("36000000-0000-0000-0000-000000000036").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0103, AuthorNodeId = originalSender.NodeId,
            HlcWall = 3600, Value = "replay-exactly"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });

        MetadataOutboundBatchReservation firstReservation;
        var (firstToSender, firstToReceiver) = PipeTransport.CreatePair();
        var firstAcceptTask = SyncSession.AcceptAsync(receiver, firstToReceiver);
        var firstConnectTask = SyncSession.ConnectAsync(originalSender, firstToSender);
        await using (var firstAcceptor = await firstAcceptTask)
        await using (var firstInitiator = await firstConnectTask)
        {
            coordinator.RegisterInterestedPeer(firstInitiator, 1);
            metadata.CompletePeerInitialSync(scope, originalSender.NodeId, receiver.NodeId,
                checked((uint)receiver.Epoch), 1,
                new[] { new MetadataPeerStreamBaseline(originalSender.NodeId, 0, checked((uint)originalSender.Epoch)) });
            using var cts = new CancellationTokenSource();
            var receive = firstAcceptor.ReceiveBatchAsync(receiver,
                beforeAck: (_, _) => throw new IOException("simulated ACK loss"));
            var push = firstInitiator.PushBatchAsync(originalSender, 1, metadata, scope, ct: cts.Token);
            await Assert.ThrowsAsync<IOException>(async () => await receive);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await push);
            firstReservation = Assert.IsType<MetadataOutboundBatchReservation>(
                metadata.GetPendingOutboundBatch(scope, originalSender.NodeId,
                    receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId));
            Assert.Equal(1UL, metadata.GetPeerStreamState(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId).PushedCursor);
            Assert.Equal(0UL, metadata.GetPeerStreamState(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId).AckedCursor);
        }

        metadata.Dispose();
        using var reopenedMetadata = new StorageMetadataStore(storage);
        var replayReservation = Assert.IsType<MetadataOutboundBatchReservation>(
            reopenedMetadata.GetPendingOutboundBatch(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId));
        Assert.Equal(firstReservation.BatchId, replayReservation.BatchId);
        Assert.Equal(firstReservation.CursorStart, replayReservation.CursorStart);
        Assert.Equal(firstReservation.CursorEnd, replayReservation.CursorEnd);
        Assert.Equal(firstReservation.PreviousBatchDigest, replayReservation.PreviousBatchDigest);
        Assert.Equal(firstReservation.BatchDigest, replayReservation.BatchDigest);
        Assert.Equal(firstReservation.Payload, replayReservation.Payload);

        using var restartedSender = new NodeBuilder()
            .WithNodeId(originalSender.NodeId.ToArray())
            .WithEpoch(originalSender.Epoch)
            .Build();
        var (secondToSender, secondToReceiver) = PipeTransport.CreatePair();
        var secondAcceptTask = SyncSession.AcceptAsync(receiver, secondToReceiver);
        var secondConnectTask = SyncSession.ConnectAsync(restartedSender, secondToSender);
        await using var secondAcceptor = await secondAcceptTask;
        await using var secondInitiator = await secondConnectTask;
        var secondReceive = secondAcceptor.ReceiveBatchAsync(receiver);
        Assert.True(await secondInitiator.PushBatchAsync(restartedSender, 1, reopenedMetadata, scope));
        Assert.Equal(0, (await secondReceive).Applied); // receiver already applied the first delivery before its ACK was lost

        var finalState = reopenedMetadata.GetPeerStreamState(scope, restartedSender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, restartedSender.NodeId);
        Assert.Equal(1UL, finalState.AckedCursor);
        Assert.Equal(1UL, finalState.PushedCursor);
        Assert.Null(finalState.PendingBatch);
        Assert.True(receiver.TryGetField(1, record, 0x0103, out var value));
        Assert.Equal("replay-exactly", Encoding.UTF8.GetString(value!.Value));
    }

    [Fact]
    public async Task Durable_push_requires_snapshot_when_first_retained_outbound_sequence_is_not_one()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-push-snapshot-barrier"), 432);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(32);
        using var receiver = NewNode(33);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "durable-push-snapshot-barrier";
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, scope);

        RecordId first = RecordId.From(Guid.Parse("32000000-0000-0000-0000-000000000032").ToByteArray());
        RecordId second = RecordId.From(Guid.Parse("33000000-0000-0000-0000-000000000033").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = first, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            HlcWall = 3200, Value = "first"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = second, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            HlcWall = 3300, Value = "second"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        ulong lease = metadata.AcquireOutboundLease(scope, sender.NodeId, 1, sender.NodeId,
            Guid.Parse("32000000-0000-0000-0000-000000000001"));
        Assert.Equal(2, metadata.ReadOutbound(scope, sender.NodeId, 1, sender.NodeId, lease, 0, 10).Items.Count);
        metadata.AcknowledgeOutbound(scope, sender.NodeId, 1, sender.NodeId, lease, 1);
        metadata.CompactOutboundThrough(scope, sender.NodeId, 1, sender.NodeId, 1);

        coordinator.RegisterInterestedPeer(initiator, 1);
        var error = await Assert.ThrowsAsync<SyncException>(
            () => initiator.PushBatchAsync(sender, 1, metadata, scope));
        Assert.Contains("SNAPSHOT_REQUIRED", error.Message);
        Assert.Null(metadata.GetPendingOutboundBatch(scope, sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, sender.NodeId, 1, sender.NodeId));
    }

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
        coordinator.RegisterInterestedPeer(initiator, 1);
        metadata.CompletePeerInitialSync("durable-peer-session", sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(sender.NodeId, 0, checked((uint)sender.Epoch)) });
        var firstReceive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, "durable-peer-session"));
        Assert.Equal(1, (await firstReceive).Applied);

        var firstState = metadata.GetPeerStreamState("durable-peer-session", sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(1UL, firstState.AckedCursor);
        Assert.Equal(1UL, firstState.PushedCursor);
        Assert.Equal((uint)sender.Epoch, firstState.IssuerEpoch);
        Assert.Null(firstState.PendingBatch);

        // The durable path must not create a second ACK/digest authority in Node.
        // The following push must rehydrate progress from Metadata Store and continue.
        var nodeMirror = sender.ReadPeerStreamProgress(receiver.NodeId, 1, sender.NodeId);
        Assert.Equal(0UL, nodeMirror.AckedCursor);
        Assert.Equal(0UL, nodeMirror.PushedCursor);

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
    public void Unpublished_local_field_edits_are_coalesced_before_metadata_store_freeze()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce"), 410);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node node = NewNode(17);
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce");
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
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

        int consumed = coordinator.PublishLocalChanges(_ => { }, limit: 16);

        Assert.Equal(4, consumed);
        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce", node.NodeId, 1, node.NodeId));
        Assert.True(provider.TryReadCurrent(record, fieldA, out var providerA));
        Assert.Equal("final", Encoding.UTF8.GetString(providerA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var providerB));
        Assert.Equal("independent", Encoding.UTF8.GetString(providerB!.Value));
        Assert.Empty(provider.ReadLocalChanges(0, 16).Changes);

        ulong lease = metadata.AcquireOutboundLease("pending-coalesce", node.NodeId, 1, node.NodeId,
            Guid.Parse("17000000-0000-0000-0000-000000000001"));
        var outbox = metadata.ReadOutbound("pending-coalesce", node.NodeId, 1, node.NodeId, lease, 0, 16).Items;
        Assert.Equal(2, outbox.Count);
        var values = outbox.Select(entry => Ops.DecodeOp(entry.Payload.AsSpan(16).ToArray()).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Contains("final", values);
        Assert.Contains("independent", values);
        Assert.DoesNotContain("first", values);
        Assert.DoesNotContain("second", values);
    }

    [Fact]
    public void Pending_local_edits_are_coalesced_across_provider_page_boundaries_in_metadata_store()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce-pages"), 423);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(24);
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce-pages");
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId record = RecordId.From(Guid.Parse("24000000-0000-0000-0000-000000000024").ToByteArray());
        const ushort fieldA = 0x0711;
        const ushort fieldB = 0x0712;

        void Append(ushort field, ulong wall, string value) => provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = field, AuthorNodeId = node.NodeId,
            HlcWall = wall, HlcLogical = 0, Value = Encoding.UTF8.GetBytes(value)
        });

        Append(fieldA, 1000, "a1");
        Append(fieldB, 2500, "b-final");
        Append(fieldA, 2000, "a2");
        Append(fieldA, 3000, "a-final");
        Append(fieldB, 2000, "b-old");

        Assert.Equal(5, coordinator.PublishLocalChanges(_ => { }, limit: 2));

        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce-pages", node.NodeId, 1, node.NodeId));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.True(provider.TryReadCurrent(record, fieldA, out var valueA));
        Assert.Equal("a-final", Encoding.UTF8.GetString(valueA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var valueB));
        Assert.Equal("b-final", Encoding.UTF8.GetString(valueB!.Value));

        ulong lease = metadata.AcquireOutboundLease("pending-coalesce-pages", node.NodeId, 1, node.NodeId,
            Guid.Parse("24000000-0000-0000-0000-000000000001"));
        var values = metadata.ReadOutbound("pending-coalesce-pages", node.NodeId, 1, node.NodeId, lease, 0, 10).Items
            .Select(entry => Ops.DecodeOp(entry.Payload.AsSpan(16).ToArray()).Op)
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
    public void Unsupported_superseded_local_change_on_an_earlier_page_is_not_hidden()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-invalid-page"), 424);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(25);
        RecordId record = RecordId.From(Guid.Parse("25000000-0000-0000-0000-000000000025").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0711, AuthorNodeId = node.NodeId,
            HlcWall = 1000, HlcLogical = 0, KeyEpoch = 1, Value = "unsupported"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0711, AuthorNodeId = node.NodeId,
            HlcWall = 2000, HlcLogical = 0, KeyEpoch = 0, Value = "winner"u8.ToArray()
        });

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-invalid-page");
        Assert.Throws<NotSupportedException>(() => coordinator.PublishLocalChanges(_ => { }, limit: 1));
        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(2, provider.ReadLocalChanges(0, 10).Changes.Count);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark("pending-invalid-page", node.NodeId, 1, node.NodeId));
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

    [Fact]
    public void Provider_authoritative_rehydration_replaces_node_cache_and_freezes_local_feed()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-authoritative-rehydration"), 453);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(54);
        using var remote = NewNode(55);
        const string scope = "provider-authoritative-rehydration";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });

        RecordId staleRecord = RecordId.From(Guid.Parse("54000000-0000-0000-0000-000000000054").ToByteArray());
        RecordId providerRecord = RecordId.From(Guid.Parse("55000000-0000-0000-0000-000000000055").ToByteArray());
        remote.WriteField(1, staleRecord, 0x0817, "node-cache-only"u8.ToArray(), new Hlc(5800, 0));
        Transport.MakeSession(remote, node);
        Assert.NotNull(Transport.DeliverBatch(remote, node, 1));
        Assert.True(node.TryGetField(1, staleRecord, 0x0817, out _));

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = providerRecord,
            FieldId = 0x0818,
            AuthorNodeId = node.NodeId,
            HlcWall = 5900,
            Value = "provider-authoritative"u8.ToArray()
        });
        BatchId expectedChangeId = Assert.Single(provider.ReadLocalChanges(0, 10).Changes).ChangeId;

        Assert.Equal(0, coordinator.RehydrateNodeFromProviderSnapshot(_ => { }));

        Assert.False(node.TryGetField(1, staleRecord, 0x0817, out _));
        Assert.True(node.TryGetField(1, providerRecord, 0x0818, out var restored));
        Assert.Equal("provider-authoritative", Encoding.UTF8.GetString(restored!.Value));
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        BatchId publishedChangeId = metadata.ReadOutbound(scope, node.NodeId, 1, node.NodeId,
            metadata.AcquireOutboundLease(scope, node.NodeId, 1, node.NodeId,
                Guid.Parse("54000000-0000-0000-0000-000000000001")), 0, 10).Items.Single().ChangeId;
        Assert.Equal(expectedChangeId, publishedChangeId);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }

    [Fact]
    public void Metadata_owned_freeze_retries_after_provider_ack_failure_with_new_feed_entries()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-owned-ack-retry"), 457);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(58);
        const string scope = "metadata-owned-ack-retry";
        var faultingProvider = new FailOnceAcknowledgeProvider(innerProvider);
        var coordinator = new StorageSyncCoordinator(node, faultingProvider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId firstRecord = RecordId.From(Guid.Parse("58000000-0000-0000-0000-000000000058").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000059").ToByteArray());

        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord,
            FieldId = 0x081B,
            AuthorNodeId = node.NodeId,
            HlcWall = 6100,
            Value = "first"u8.ToArray()
        });
        BatchId firstChangeId = Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes).ChangeId;

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes);

        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord,
            FieldId = 0x081C,
            AuthorNodeId = node.NodeId,
            HlcWall = 6200,
            Value = "second"u8.ToArray()
        });
        ProviderChange[] pending = innerProvider.ReadLocalChanges(0, 10).Changes.ToArray();
        Assert.Equal(2, pending.Length);
        BatchId secondChangeId = pending[1].ChangeId;

        Assert.Equal(2, coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Empty(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
        Assert.True(node.TryGetField(1, firstRecord, 0x081B, out var firstValue));
        Assert.Equal("first", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.True(node.TryGetField(1, secondRecord, 0x081C, out var secondValue));
        Assert.Equal("second", Encoding.UTF8.GetString(secondValue!.Value));

        ulong lease = metadata.AcquireOutboundLease(scope, node.NodeId, 1, node.NodeId,
            Guid.Parse("58000000-0000-0000-0000-000000000001"));
        var outbound = metadata.ReadOutbound(scope, node.NodeId, 1, node.NodeId, lease, 0, 10).Items;
        Assert.Equal(new ulong[] { 1, 2 }, outbound.Select(item => item.Sequence));
        Assert.Equal(firstChangeId, outbound[0].ChangeId);
        Assert.Equal(secondChangeId, outbound[1].ChangeId);
    }

    [Fact]
    public void Provider_authoritative_recovery_processes_only_its_namespace_pending_intents()
    {
        using var metadataStorage = StorageDatabase.Open(Path.Combine(_root, "multi-namespace-metadata"), 454);
        using var namespaceOneStorage = StorageDatabase.Open(Path.Combine(_root, "multi-namespace-provider-one"), 455);
        using var namespaceTwoStorage = StorageDatabase.Open(Path.Combine(_root, "multi-namespace-provider-two"), 456);
        using var metadata = new StorageMetadataStore(metadataStorage);
        using var providerOne = new StorageDataProvider(namespaceOneStorage);
        using var providerTwo = new StorageDataProvider(namespaceTwoStorage);
        using var node = NewNode(56);
        using var sender = NewNode(57);
        const string scope = "multi-namespace-provider-recovery";

        RecordId recordOne = RecordId.From(Guid.Parse("56000000-0000-0000-0000-000000000056").ToByteArray());
        RecordId recordTwo = RecordId.From(Guid.Parse("57000000-0000-0000-0000-000000000057").ToByteArray());
        MetadataIncomingBatch batchOne = CreatePendingProviderBatch(
            scope, node, sender, 1, recordOne, 0x0819, "namespace-one", 0xA1);
        MetadataIncomingBatch batchTwo = CreatePendingProviderBatch(
            scope, node, sender, 2, recordTwo, 0x081A, "namespace-two", 0xA2);
        metadata.BeginPendingApply(batchTwo, Guid.NewGuid());

        var coordinatorOne = new StorageSyncCoordinator(node, providerOne, 1, metadata, scope);
        Assert.Equal(0, coordinatorOne.RehydrateNodeFromProviderSnapshot(_ => { }));
        MetadataPendingApplyIntent foreign = Assert.Single(
            metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.Equal(2, foreign.Batch.NamespaceId);

        metadata.BeginPendingApply(batchOne, Guid.NewGuid());
        Assert.Throws<InvalidOperationException>(() =>
            coordinatorOne.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Equal(2, metadata.ReadPendingApplyIntents(scope, node.NodeId).Count);
        Assert.Equal(1, coordinatorOne.RecoverPendingApplies());
        MetadataPendingApplyIntent remaining = Assert.Single(
            metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.Equal(2, remaining.Batch.NamespaceId);
        Assert.True(providerOne.TryReadCurrent(recordOne, 0x0819, out var valueOne));
        Assert.Equal("namespace-one", Encoding.UTF8.GetString(valueOne!.Value));
        Assert.False(providerTwo.TryReadCurrent(recordTwo, 0x081A, out _));

        var coordinatorTwo = new StorageSyncCoordinator(node, providerTwo, 2, metadata, scope);
        Assert.Equal(1, coordinatorTwo.RecoverPendingApplies());
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.True(providerTwo.TryReadCurrent(recordTwo, 0x081A, out var valueTwo));
        Assert.Equal("namespace-two", Encoding.UTF8.GetString(valueTwo!.Value));
    }

    private sealed class FailOnceAcknowledgeProvider : IDataProvider
    {
        private readonly IDataProvider _inner;
        private bool _failNextAcknowledge = true;

        public FailOnceAcknowledgeProvider(IDataProvider inner) => _inner = inner;

        public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey) =>
            _inner.ResolveRecordId(sourceItemKey);
        public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey) =>
            _inner.ResolveFieldId(sourceFieldKey);
        public void AppendLocalChange(ProviderMutation mutation) => _inner.AppendLocalChange(mutation);
        public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit) =>
            _inner.ReadLocalChanges(afterSequence, limit);
        public ulong LocalChangeHighWatermark => _inner.LocalChangeHighWatermark;

        public void AcknowledgeLocalChanges(ulong safeThroughSequence)
        {
            if (_failNextAcknowledge)
            {
                _failNextAcknowledge = false;
                throw new IOException("Injected provider feed ACK interruption.");
            }
            _inner.AcknowledgeLocalChanges(safeThroughSequence);
        }

        public ProviderSnapshotHandle OpenSnapshot() => _inner.OpenSnapshot();
        public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit) =>
            _inner.ReadSnapshot(snapshotToken, afterIndex, limit);
        public void ReleaseSnapshot(Guid snapshotToken) => _inner.ReleaseSnapshot(snapshotToken);
        public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations) =>
            _inner.ApplyRemoteBatch(applyToken, mutations);
        public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation) =>
            _inner.TryReadCurrent(recordId, fieldId, out mutation);
        public void Dispose() { }
    }

    private static MetadataIncomingBatch CreatePendingProviderBatch(
        string scope, Node local, Node sender, int namespaceId, RecordId recordId,
        ushort fieldId, string value, byte fingerprintSeed)
    {
        Assert.True(sender.WriteField(namespaceId, recordId, fieldId,
            Encoding.UTF8.GetBytes(value), new Hlc(6000UL + (ulong)namespaceId, 0)));
        OutboxEntry entry = Assert.Single(sender.ReadOutboxEntries(namespaceId, sender.NodeId, 0, 10));
        byte[] envelope = new byte[16 + entry.Payload.Length];
        recordId.WriteTo(envelope);
        entry.Payload.Span.CopyTo(envelope.AsSpan(16));

        return new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local.NodeId,
            SenderPeer = sender.NodeId,
            SenderEpoch = checked((uint)sender.Epoch),
            AuthorStreamNodeId = sender.NodeId,
            NamespaceId = namespaceId,
            BatchId = BatchId.From(Guid.NewGuid().ToByteArray()),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)(fingerprintSeed + 1), 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = sender.NodeId,
                    ChangeId = entry.ChangeId,
                    Fingerprint = Enumerable.Repeat(fingerprintSeed, 16).ToArray(),
                    Payload = envelope,
                    RelayEligible = false
                }
            }
        };
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
    [Fact]
    public void Metadata_store_owned_publication_requires_legacy_drain_and_fences_node_writes()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-owned-cutover"), 459);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(59);
        const string scope = "metadata-owned-cutover";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        RecordId firstRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000001").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000002").ToByteArray());
        RecordId nodeOnlyRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000003").ToByteArray());

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord, FieldId = 0x081D, AuthorNodeId = node.NodeId,
            HlcWall = 6300, Value = "legacy-pending"u8.ToArray()
        });
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.EnableMetadataStoreOwnedPublication(_ => { }));

        // Before cutover, the existing Node-Outbox path drains and freezes the
        // pending provider feed. Only an empty, aligned boundary can be enabled.
        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord, FieldId = 0x081E, AuthorNodeId = node.NodeId,
            HlcWall = 6400, Value = "metadata-owned"u8.ToArray()
        });
        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Equal(1L, node.GetOutboxHighWatermark(1, node.NodeId));

        // Direct Node writes are forbidden after cutover; the next coordinator
        // pass fails closed without consuming the provider feed.
        node.WriteField(1, nodeOnlyRecord, 0x081F, "node-only"u8.ToArray(), new Hlc(6500, 0));
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = nodeOnlyRecord, FieldId = 0x081F, AuthorNodeId = node.NodeId,
            HlcWall = 6500, Value = "node-only"u8.ToArray()
        });
        Assert.Throws<InvalidOperationException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
    }


}
