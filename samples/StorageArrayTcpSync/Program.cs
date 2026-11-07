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

        providerA.AppendLocalChange(Mutation(recordA, nameA, nodeA.NodeId, 1000, "pump-A"u8));
        providerA.AppendLocalChange(Mutation(recordA, quantityA, nodeA.NodeId, 1001, "4"u8));
        providerB.AppendLocalChange(Mutation(recordB, nameB, nodeB.NodeId, 2000, "valve-B"u8));
        providerB.AppendLocalChange(Mutation(recordB, quantityB, nodeB.NodeId, 2001, "7"u8));
        coordinatorA.PublishLocalChanges(n => StorageNodePersistence.Save(storageA, n));
        coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));
        Console.WriteLine($"post-restart B outbox={nodeB.GetOutboxCount(NamespaceId)} high={nodeB.GetOutboxHighWatermark(NamespaceId, nodeB.NodeId)} metadata={metadataB.GetOutboundHighWatermark(\"array-tcp-demo\", nodeB.NodeId, NamespaceId, nodeB.NodeId)}");
        if (nodeB.GetOutboxCount(NamespaceId) == 0)
            throw new InvalidOperationException("Post-restart local update was not enqueued in the Node Outbox.");

        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, nodeB, coordinatorB, storageB);
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
        ulong metadataHigh = metadataB.GetOutboundHighWatermark("array-tcp-demo", nodeB.NodeId, NamespaceId, nodeB.NodeId);
        Console.WriteLine($"post-restart publish={published} B outbox={nodeB.GetOutboxCount(NamespaceId)} high={nodeB.GetOutboxHighWatermark(NamespaceId, nodeB.NodeId)} metadata={metadataHigh}");
        if (published != 1 || nodeB.GetOutboxCount(NamespaceId) == 0)
            throw new InvalidOperationException("Post-restart local update was not enqueued in the Node Outbox.");

        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, nodeB, coordinatorB, storageB);
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
        await ExchangeOverTcpAsync(nodeA, coordinatorA, storageA, nodeB, coordinatorB, storageB);
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

static async Task<int> ExchangeOverTcpAsync(
    Node nodeA, StorageSyncCoordinator coordinatorA, StorageDatabase storageA,
    Node nodeB, StorageSyncCoordinator coordinatorB, StorageDatabase storageB)
{
    await using var listener = TcpSyncListener.Start();
    var acceptTask = listener.AcceptAsync(nodeA);
    await using var sessionB = await TcpSync.ConnectAsync(nodeB, "127.0.0.1", listener.Port);
    await using var sessionA = await acceptTask;

    int directions = 0;
    Console.WriteLine($"TCP session outboxes: A={nodeA.GetOutboxCount(NamespaceId)}, B={nodeB.GetOutboxCount(NamespaceId)}");
    if (nodeA.GetOutboxCount(NamespaceId) > 0)
    {
        Task<BatchResult> receive = sessionB.ReceiveBatchAsync(nodeB, beforeAck: (batch, _) =>
            coordinatorB.ApplyRemoteBatchBeforeAck(batch, _ => StorageNodePersistence.Save(storageB, nodeB)));
        bool sent = await sessionA.PushBatchAsync(nodeA, NamespaceId);
        if (!sent) throw new InvalidOperationException("Node A had an Outbox entry but did not send a batch.");
        await receive;
        directions++;
    }

    if (nodeB.GetOutboxCount(NamespaceId) > 0)
    {
        Task<BatchResult> receive = sessionA.ReceiveBatchAsync(nodeA, beforeAck: (batch, _) =>
            coordinatorA.ApplyRemoteBatchBeforeAck(batch, _ => StorageNodePersistence.Save(storageA, nodeA)));
        bool sent = await sessionB.PushBatchAsync(nodeB, NamespaceId);
        if (!sent) throw new InvalidOperationException("Node B had an Outbox entry but did not send a batch.");
        await receive;
        directions++;
    }

    Console.WriteLine($"TCP session completed directions={directions}; remaining outboxes A={nodeA.GetOutboxCount(NamespaceId)}, B={nodeB.GetOutboxCount(NamespaceId)}");
    return directions;
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
