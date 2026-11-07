using System.Text;
using BinSync;
using BinSync.Storage;

const int NamespaceId = 1;

string root = Path.Combine(Path.GetTempPath(), "binsync-storage-array-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
string providerAPath = Path.Combine(root, "provider-a");
string providerBPath = Path.Combine(root, "provider-b");
string nodeAPath = Path.Combine(root, "node-a.state");
string nodeBPath = Path.Combine(root, "node-b.state");

Guid itemA = Guid.Parse("10000000-0000-0000-0000-000000000001");
Guid itemB = Guid.Parse("20000000-0000-0000-0000-000000000002");

try
{
    Node nodeA;
    Node nodeB;
    using (var providerA = new StorageDataProvider(providerAPath, 201))
    using (var providerB = new StorageDataProvider(providerBPath, 202))
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
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId);

        providerA.AppendLocalChange(Mutation(recordA, nameA, nodeA.NodeId, 1000, "pump-A"u8));
        providerA.AppendLocalChange(Mutation(recordA, quantityA, nodeA.NodeId, 1001, "4"u8));
        providerB.AppendLocalChange(Mutation(recordB, nameB, nodeB.NodeId, 2000, "valve-B"u8));
        providerB.AppendLocalChange(Mutation(recordB, quantityB, nodeB.NodeId, 2001, "7"u8));

        coordinatorA.PublishLocalChanges(n => NodePersistence.Save(n, nodeAPath));
        coordinatorB.PublishLocalChanges(n => NodePersistence.Save(n, nodeBPath));

        Transport.MakeSession(nodeA, nodeB);
        int firstRound = Transport.SyncRound(nodeA, nodeB, NamespaceId, beforeAck: (receiver, _, _) =>
        {
            // Persist the Node's accepted receipt/state first, then materialize the
            // external provider. Transport sends ACK only after this callback returns.
            if (ReferenceEquals(receiver, nodeA))
            {
                NodePersistence.Save(receiver, nodeAPath);
                coordinatorA.ApplyRemoteFields(Fields(recordA, recordB, nameA, quantityA));
            }
            else
            {
                NodePersistence.Save(receiver, nodeBPath);
                coordinatorB.ApplyRemoteFields(Fields(recordA, recordB, nameA, quantityA));
            }
        });
        NodePersistence.Save(nodeA, nodeAPath);
        NodePersistence.Save(nodeB, nodeBPath);

        Console.WriteLine($"initial sync rounds={firstRound}; each Storage provider now has both array items");
    }

    // Reopen both durable providers and both BIN-SYNC nodes before the next edit.
    nodeA = NodePersistence.Load(nodeAPath);
    nodeB = NodePersistence.Load(nodeBPath);
    using (var providerA = new StorageDataProvider(providerAPath, 201))
    using (var providerB = new StorageDataProvider(providerBPath, 202))
    {
        RecordId recordA = providerA.ResolveRecordId(itemA.ToByteArray());
        RecordId recordB = providerB.ResolveRecordId(itemB.ToByteArray());
        ushort name = providerA.ResolveFieldId("name"u8.ToArray());
        ushort quantity = providerA.ResolveFieldId("quantity"u8.ToArray());

        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, NamespaceId);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, NamespaceId);

        providerB.AppendLocalChange(Mutation(recordB, quantity, nodeB.NodeId, 3000, "9"u8));
        coordinatorB.PublishLocalChanges(n => NodePersistence.Save(n, nodeBPath));

        Transport.MakeSession(nodeA, nodeB);
        int secondRound = Transport.SyncRound(nodeA, nodeB, NamespaceId, beforeAck: (receiver, _) =>
        {
            if (ReferenceEquals(receiver, nodeA))
            {
                NodePersistence.Save(receiver, nodeAPath);
                coordinatorA.ApplyRemoteFields(Fields(recordA, recordB, name, quantity));
            }
            else
            {
                NodePersistence.Save(receiver, nodeBPath);
                coordinatorB.ApplyRemoteFields(Fields(recordA, recordB, name, quantity));
            }
        });
        NodePersistence.Save(nodeA, nodeAPath);
        NodePersistence.Save(nodeB, nodeBPath);

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

static IReadOnlyList<(RecordId RecordId, ushort FieldId)> Fields(
    RecordId recordA, RecordId recordB, ushort name, ushort quantity)
    => new[] { (recordA, name), (recordA, quantity), (recordB, name), (recordB, quantity) };

static string Read(IDataProvider provider, RecordId recordId, ushort fieldId)
    => provider.TryReadCurrent(recordId, fieldId, out var value) && value is not null
        ? Encoding.UTF8.GetString(value.Value)
        : string.Empty;
