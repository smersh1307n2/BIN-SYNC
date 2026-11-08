using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;

const int NamespaceId = 1;

string root = Path.Combine(Path.GetTempPath(), "binsync-storage-array-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
string providerAPath = Path.Combine(root, "provider-a");
string providerBPath = Path.Combine(root, "provider-b");
NodeId nodeAId = default;
NodeId nodeBId = default;

Guid itemA = Guid.Parse("10000000-0000-0000-0000-000000000001");
Guid itemB = Guid.Parse("20000000-0000-0000-0000-000000000002");

try
{
    Node nodeA;
    Node nodeB;
    using (var storageA = StorageDatabase.Open(providerAPath, 201))
    using (var storageB = StorageDatabase.Open(providerBPath, 202))
    using (var providerA = new StorageDataProvider(storageA))
    using (var providerB = new StorageDataProvider(storageB))
    using (var metadataA = new StorageMetadataStore(storageA))
    using (var metadataB = new StorageMetadataStore(storageB))
    {
        RecordId recordA = providerA.ResolveRecordId(itemA.ToByteArray());
        RecordId recordB = providerB.ResolveRecordId(itemB.ToByteArray());
        ushort nameA = providerA.ResolveFieldId("name"u8.ToArray());
        ushort quantityA = providerA.ResolveFieldId("quantity"u8.ToArray());
        ushort nameB = providerB.ResolveFieldId("name"u8.ToArray());
        ushort quantityB = providerB.ResolveFieldId("quantity"u8.ToArray());
        if (nameA != nameB || quantityA != quantityB)
            throw new InvalidOperationException("Stable source field mapping differs between replicas.");

        nodeA = NewNode(1);
        nodeB = NewNode(2);
        nodeAId = nodeA.NodeId;
        nodeBId = nodeB.NodeId;
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId, metadataA, "array-demo");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId, metadataB, "array-demo");

        providerA.AppendLocalChange(Mutation(recordA, nameA, nodeA.NodeId, 1000, "pump-A"u8));
        providerA.AppendLocalChange(Mutation(recordA, quantityA, nodeA.NodeId, 1001, "4"u8));
        providerB.AppendLocalChange(Mutation(recordB, nameB, nodeB.NodeId, 2000, "valve-B"u8));
        providerB.AppendLocalChange(Mutation(recordB, quantityB, nodeB.NodeId, 2001, "7"u8));

        coordinatorA.PublishLocalChanges(n => StorageNodePersistence.Save(storageA, n));
        coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));
        if (metadataA.GetOutboundHighWatermark("array-demo", nodeA.NodeId, NamespaceId, nodeA.NodeId) != 2 ||
            metadataB.GetOutboundHighWatermark("array-demo", nodeB.NodeId, NamespaceId, nodeB.NodeId) != 2)
            throw new InvalidOperationException("Local Node Outbox was not frozen into durable Storage metadata.");
        ulong outboundLeaseA = metadataA.AcquireOutboundLease(
            "array-demo", nodeA.NodeId, NamespaceId, nodeA.NodeId, Guid.Parse("a1000000-0000-0000-0000-000000000001"));
        var outboundPageA = metadataA.ReadOutbound(
            "array-demo", nodeA.NodeId, NamespaceId, nodeA.NodeId, outboundLeaseA, 0, 10);
        if (outboundPageA.Items.Count != 2 ||
            RecordId.From(outboundPageA.Items[0].Payload.AsSpan(0, 16)) != recordA)
            throw new InvalidOperationException("Frozen outbound array payload is not readable from Storage metadata.");

        Transport.MakeSession(nodeA, nodeB);
        // Persist negotiated aliases before the first inbound batch can stage PendingApply.
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);
        int firstRound = Transport.SyncRound(nodeA, nodeB, NamespaceId, beforeAck: (receiver, batch, _) =>
        {
            // Persist the Node's accepted receipt/state first, then materialize the
            // external provider. Transport sends ACK only after this callback returns.
            if (ReferenceEquals(receiver, nodeA))
            {
                coordinatorA.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageA, n));
            }
            else
            {
                coordinatorB.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageB, n));
            }
        });
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        Console.WriteLine($"initial sync rounds={firstRound}; each Storage provider now has both array items");
    }

    // Reopen both durable providers and both BIN-SYNC nodes before the next edit.
    using (var storageA = StorageDatabase.Open(providerAPath, 201))
    using (var storageB = StorageDatabase.Open(providerBPath, 202))
    using (var providerA = new StorageDataProvider(storageA))
    using (var providerB = new StorageDataProvider(storageB))
    using (var metadataA = new StorageMetadataStore(storageA))
    using (var metadataB = new StorageMetadataStore(storageB))
    {
        nodeA = StorageNodePersistence.Load(storageA, nodeAId);
        nodeB = StorageNodePersistence.Load(storageB, nodeBId);
        RecordId recordA = providerA.ResolveRecordId(itemA.ToByteArray());
        RecordId recordB = providerB.ResolveRecordId(itemB.ToByteArray());
        ushort name = providerA.ResolveFieldId("name"u8.ToArray());
        ushort quantity = providerA.ResolveFieldId("quantity"u8.ToArray());

        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId, metadataA, "array-demo");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId, metadataB, "array-demo");
        if (coordinatorA.RecoverPendingApplies() != 0 || coordinatorB.RecoverPendingApplies() != 0)
            throw new InvalidOperationException("Unexpected pending apply in clean restart scenario.");

        providerB.AppendLocalChange(Mutation(recordB, quantity, nodeB.NodeId, 3000, "9"u8));
        coordinatorB.PublishLocalChanges(n => StorageNodePersistence.Save(storageB, n));
        if (metadataB.GetOutboundHighWatermark("array-demo", nodeB.NodeId, NamespaceId, nodeB.NodeId) != 3)
            throw new InvalidOperationException("Post-restart update was not frozen into the existing outbound stream.");

        Transport.MakeSession(nodeA, nodeB);
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);
        int secondRound = Transport.SyncRound(nodeA, nodeB, NamespaceId, beforeAck: (receiver, batch, _) =>
        {
            if (ReferenceEquals(receiver, nodeA))
            {
                coordinatorA.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageA, n));
            }
            else
            {
                coordinatorB.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageB, n));
            }
        });
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        string aName = Read(providerA, recordA, name);
        string aQuantity = Read(providerA, recordA, quantity);
        string bName = Read(providerA, recordB, name);
        string bQuantity = Read(providerA, recordB, quantity);
        string bReplicaQuantity = Read(providerB, recordB, quantity);

        Console.WriteLine($"after restart + update rounds={secondRound}");
        Console.WriteLine($"A: [{aName}, {aQuantity}]");
        Console.WriteLine($"B: [{bName}, {bQuantity}]");
        Console.WriteLine($"B replica quantity={bReplicaQuantity}");

        if (aName != "pump-A" || aQuantity != "4" ||
            bName != "valve-B" || bQuantity != "9" || bReplicaQuantity != "9")
            return 1;

        // Exercise array-item deletion as a record lifecycle tombstone, not as
        // a fan-out of unrelated field tombstones.
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
        if (metadataB.GetOutboundHighWatermark("array-demo", nodeB.NodeId, NamespaceId, nodeB.NodeId) != 4)
            throw new InvalidOperationException("Record deletion was not frozen into the durable outbound stream.");

        int deleteRound = Transport.SyncRound(nodeA, nodeB, NamespaceId, beforeAck: (receiver, batch, _) =>
        {
            if (ReferenceEquals(receiver, nodeA))
            {
                coordinatorA.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageA, n));
            }
            else
            {
                coordinatorB.ApplyRemoteBatchBeforeAck(batch, n => StorageNodePersistence.Save(storageB, n));
            }
        });
        if (deleteRound == 0 ||
            providerA.TryReadCurrent(recordB, name, out _) ||
            providerA.TryReadCurrent(recordB, quantity, out _) ||
            providerB.TryReadCurrent(recordB, name, out _))
            return 1;

        Console.WriteLine($"record deletion sync rounds={deleteRound}; item B is hidden on both replicas");
        Console.WriteLine("Storage-backed array synchronization: PASS");
        return 0;
    }
}
finally
{
    try { Directory.Delete(root, recursive: true); } catch { }
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

static string Read(IDataProvider provider, RecordId recordId, ushort fieldId)
    => provider.TryReadCurrent(recordId, fieldId, out var value) && value is not null
        ? Encoding.UTF8.GetString(value.Value)
        : string.Empty;
