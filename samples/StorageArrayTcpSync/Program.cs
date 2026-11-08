using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;

const int NamespaceId = 1;

string root = Path.Combine(Path.GetTempPath(), "binsync-storage-array-tcp-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
string pathA = Path.Combine(root, "replica-a");
string pathB = Path.Combine(root, "replica-b");

Guid itemA = Guid.Parse("10000000-0000-0000-0000-000000000001");
Guid itemB = Guid.Parse("20000000-0000-0000-0000-000000000002");
NodeId nodeAId = default;
NodeId nodeBId = default;

try
{
    Node nodeA;
    Node nodeB;

    using (var storageA = StorageDatabase.Open(pathA, 211))
    using (var storageB = StorageDatabase.Open(pathB, 212))
    using (var providerA = new StorageDataProvider(storageA))
    using (var providerB = new StorageDataProvider(storageB))
    using (var metadataA = new StorageMetadataStore(storageA))
    using (var metadataB = new StorageMetadataStore(storageB))
    {
        nodeA = NewNode(1);
        nodeB = NewNode(2);
        nodeAId = nodeA.NodeId;
        nodeBId = nodeB.NodeId;

        RecordId recordA = providerA.ResolveRecordId(itemA.ToByteArray());
        RecordId recordB = providerB.ResolveRecordId(itemB.ToByteArray());
        ushort nameA = providerA.ResolveFieldId("name"u8.ToArray());
        ushort quantityA = providerA.ResolveFieldId("quantity"u8.ToArray());
        ushort nameB = providerB.ResolveFieldId("name"u8.ToArray());
        ushort quantityB = providerB.ResolveFieldId("quantity"u8.ToArray());
        if (nameA != nameB || quantityA != quantityB)
            throw new InvalidOperationException("Stable field mapping differs between replicas.");

        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId, metadataA, "array-tcp-demo");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId, metadataB, "array-tcp-demo");
        coordinatorA.EnableMetadataStoreOwnedPublication(n => StorageNodePersistence.Save(storageA, n));
        coordinatorB.EnableMetadataStoreOwnedPublication(n => StorageNodePersistence.Save(storageB, n));
        await InitializePeerBaselineOverTcpAsync(
            nodeA, coordinatorA, metadataA, nodeB, coordinatorB, metadataB);

        providerA.AppendLocalChange(Mutation(recordA, nameA, nodeA.NodeId, 1000, "pump-A"u8));
        providerA.AppendLocalChange(Mutation(recordA, quantityA, nodeA.NodeId, 1001, "4"u8));
        providerB.AppendLocalChange(Mutation(recordB, nameB, nodeB.NodeId, 2000, "valve-B"u8));
        providerB.AppendLocalChange(Mutation(recordB, quantityB, nodeB.NodeId, 2001, "7"u8));
        coordinatorA.PublishLocalChanges(n => StorageNodePersistence.Save(storageA, n));
        coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));

        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, metadataA, nodeB, coordinatorB, storageB, metadataB);
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);
        AssertValue(providerA, recordB, nameA, "valve-B");
        AssertValue(providerB, recordA, nameB, "pump-A");
        AssertValue(providerA, recordB, quantityA, "7");
        AssertValue(providerB, recordA, quantityB, "4");
        Console.WriteLine("Initial bidirectional Storage-backed TCP sync: PASS");
    }

    // Reopen the complete replica databases, including provider, metadata and Node state.
    using (var storageA = StorageDatabase.Open(pathA, 211))
    using (var storageB = StorageDatabase.Open(pathB, 212))
    using (var providerA = new StorageDataProvider(storageA))
    using (var providerB = new StorageDataProvider(storageB))
    using (var metadataA = new StorageMetadataStore(storageA))
    using (var metadataB = new StorageMetadataStore(storageB))
    {
        nodeA = StorageNodePersistence.Load(storageA, nodeAId);
        nodeB = StorageNodePersistence.Load(storageB, nodeBId);
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId, metadataA, "array-tcp-demo");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId, metadataB, "array-tcp-demo");
        if (coordinatorA.RecoverPendingApplies() != 0 || coordinatorB.RecoverPendingApplies() != 0)
            throw new InvalidOperationException("Unexpected pending apply in clean restart scenario.");

        RecordId recordB = providerB.ResolveRecordId(itemB.ToByteArray());
        ushort quantity = providerB.ResolveFieldId("quantity"u8.ToArray());
        providerB.AppendLocalChange(Mutation(recordB, quantity, nodeB.NodeId, 3000, "9"u8));
        int published = coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));
        if (published != 1 ||
            nodeB.GetOutboxHighWatermark(NamespaceId, nodeB.NodeId) != 0 ||
            metadataB.GetOutboundHighWatermark("array-tcp-demo", nodeB.NodeId, NamespaceId, nodeB.NodeId) != 3)
            throw new InvalidOperationException("Post-restart local update was not frozen directly by Metadata Store.");

        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, metadataA, nodeB, coordinatorB, storageB, metadataB);
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);
        AssertValue(providerA, recordB, quantity, "9");
        AssertValue(providerB, recordB, quantity, "9");
        Console.WriteLine("Restart + subsequent TCP update: PASS");

        // Record deletion is propagated as one lifecycle tombstone.
        providerB.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordB,
            FieldId = Model.LifecycleFieldId,
            IsRecordTombstone = true,
            AuthorNodeId = nodeB.NodeId,
            HlcWall = 4000,
            HlcLogical = 0,
            Value = Array.Empty<byte>()
        });
        coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));
        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, metadataA, nodeB, coordinatorB, storageB, metadataB);
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        if (providerA.TryReadCurrent(recordB, quantity, out _) ||
            providerB.TryReadCurrent(recordB, quantity, out _))
            throw new InvalidOperationException("Record tombstone was not applied on both replicas.");

        Console.WriteLine("Record tombstone over Storage-backed TCP: PASS");
    }

    Console.WriteLine("Storage-backed array synchronization over real TCP: PASS");
    return 0;
}
finally
{
    try { Directory.Delete(root, recursive: true); } catch { }
}

static async Task InitializePeerBaselineOverTcpAsync(
    Node nodeA, StorageSyncCoordinator coordinatorA, IMetadataPeerStateStore metadataA,
    Node nodeB, StorageSyncCoordinator coordinatorB, IMetadataPeerStateStore metadataB)
{
    // The replicas start with empty provider snapshots. Register both peers and
    // persist that empty initial baseline before either side creates outbound deltas.
    await using var listener = TcpSyncListener.Start();
    var acceptTask = listener.AcceptAsync(nodeA);
    await using var sessionB = await TcpSync.ConnectAsync(nodeB, "127.0.0.1", listener.Port);
    await using var sessionA = await acceptTask;

    coordinatorA.RegisterInterestedPeer(sessionA, NamespaceId);
    coordinatorB.RegisterInterestedPeer(sessionB, NamespaceId);
    metadataA.CompletePeerInitialSync("array-tcp-demo", nodeA.NodeId, nodeB.NodeId,
        checked((uint)nodeB.Epoch), NamespaceId,
        new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
    metadataB.CompletePeerInitialSync("array-tcp-demo", nodeB.NodeId, nodeA.NodeId,
        checked((uint)nodeA.Epoch), NamespaceId,
        new[] { new MetadataPeerStreamBaseline(nodeB.NodeId, 0, checked((uint)nodeB.Epoch)) });
}

static async Task<int> ExchangeOverTcpAsync(
    Node nodeA, StorageSyncCoordinator coordinatorA, StorageDatabase storageA, IMetadataPeerStateStore metadataA,
    Node nodeB, StorageSyncCoordinator coordinatorB, StorageDatabase storageB, IMetadataPeerStateStore metadataB)
{
    await using var listener = TcpSyncListener.Start();
    var acceptTask = listener.AcceptAsync(nodeA);
    await using var sessionB = await TcpSync.ConnectAsync(nodeB, "127.0.0.1", listener.Port);
    await using var sessionA = await acceptTask;

    int aToB = await PushUntilIdleAsync(sessionA, nodeA, metadataA, "array-tcp-demo", sessionB, nodeB,
        (batch, _) => coordinatorB.ApplyRemoteBatchBeforeAck(batch, _ => StorageNodePersistence.Save(storageB, nodeB)));
    int bToA = await PushUntilIdleAsync(sessionB, nodeB, metadataB, "array-tcp-demo", sessionA, nodeA,
        (batch, _) => coordinatorA.ApplyRemoteBatchBeforeAck(batch, _ => StorageNodePersistence.Save(storageA, nodeA)));
    Console.WriteLine($"TCP batches: A→B={aToB}, B→A={bToA}");
    return aToB + bToA;
}

static async Task<int> PushUntilIdleAsync(
    SyncSession senderSession,
    Node sender,
    IMetadataPeerStateStore senderMetadata,
    string scope,
    SyncSession receiverSession,
    Node receiver,
    Action<Batch, BatchResult> beforeAck)
{
    int sentCount = 0;
    while (true)
    {
        using var receiveCts = new CancellationTokenSource();
        Task<BatchResult> receiveTask = receiverSession.ReceiveBatchAsync(receiver, receiveCts.Token, beforeAck);
        Task<bool> pushTask = senderSession.PushBatchAsync(sender, NamespaceId, senderMetadata, scope);

        Task first = await Task.WhenAny(pushTask, receiveTask);
        if (ReferenceEquals(first, receiveTask) && receiveTask.IsFaulted)
        {
            receiveCts.Cancel();
            try { await pushTask; } catch { }
            await receiveTask;
        }

        bool sent;
        try
        {
            sent = await pushTask;
        }
        catch
        {
            receiveCts.Cancel();
            try { await receiveTask; } catch { }
            throw;
        }

        if (!sent)
        {
            receiveCts.Cancel();
            try { await receiveTask; }
            catch (OperationCanceledException) { }
            break;
        }

        await receiveTask;
        sentCount++;
    }

    return sentCount;
}

static Node NewNode(byte seed)
{
    byte[] id = new byte[16];
    id[0] = seed;
    id[15] = (byte)(seed ^ 0xA5);
    return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
}

static ProviderMutation Mutation(RecordId recordId, ushort fieldId, NodeId author, ulong wall, ReadOnlySpan<byte> value)
    => new()
    {
        RecordId = recordId,
        FieldId = fieldId,
        AuthorNodeId = author,
        HlcWall = wall,
        HlcLogical = 0,
        KeyEpoch = 0,
        Value = value.ToArray()
    };

static void AssertValue(IDataProvider provider, RecordId recordId, ushort fieldId, string expected)
{
    if (!provider.TryReadCurrent(recordId, fieldId, out var value) || value is null ||
        Encoding.UTF8.GetString(value.Value) != expected)
        throw new InvalidOperationException($"Expected field value '{expected}'.");
}
