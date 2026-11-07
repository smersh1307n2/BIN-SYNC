using System.Buffers.Binary;
using System.Text;
using BinSync;
using Fld1.Storage.Engine;

const ushort Ns = 1;
const ushort NameField = 0x0101;
const ushort QuantityField = 0x0102;

string root = Path.Combine(Path.GetTempPath(), "binsync-storage-array-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
Guid itemA = Guid.Parse("10000000-0000-0000-0000-000000000001");
Guid itemB = Guid.Parse("20000000-0000-0000-0000-000000000002");
RecordId ridA = RecordId.From(itemA.ToByteArray());
RecordId ridB = RecordId.From(itemB.ToByteArray());

try
{
    Node nodeA;
    Node nodeB;
    using (var arrayA = new StorageArray(Path.Combine(root, "array-a"), 201))
    using (var arrayB = new StorageArray(Path.Combine(root, "array-b"), 202))
    {
        arrayA.Set(ridA, NameField, "pump-A"u8);
        arrayA.Set(ridA, QuantityField, "4"u8);
        arrayB.Set(ridB, NameField, "valve-B"u8);
        arrayB.Set(ridB, QuantityField, "7"u8);

        nodeA = NewNode(1);
        nodeB = NewNode(2);
        PublishLocalChanges(arrayA, nodeA, Path.Combine(root, "node-a.state"));
        PublishLocalChanges(arrayB, nodeB, Path.Combine(root, "node-b.state"));

        Transport.MakeSession(nodeA, nodeB);
        int firstRound = Transport.SyncRound(nodeA, nodeB, Ns);
        PullNodeIntoArray(nodeA, arrayA, [ridA, ridB]);
        PullNodeIntoArray(nodeB, arrayB, [ridA, ridB]);
        Console.WriteLine($"initial sync rounds={firstRound}; A={arrayA.Count} fields, B={arrayB.Count} fields");

        NodePersistence.Save(nodeA, Path.Combine(root, "node-a.state"));
        NodePersistence.Save(nodeB, Path.Combine(root, "node-b.state"));
    }

    // Both the array data and BIN-SYNC node state are reopened from durable storage.
    nodeA = NodePersistence.Load(Path.Combine(root, "node-a.state"));
    nodeB = NodePersistence.Load(Path.Combine(root, "node-b.state"));
    using (var arrayA = new StorageArray(Path.Combine(root, "array-a"), 201))
    using (var arrayB = new StorageArray(Path.Combine(root, "array-b"), 202))
    {
        Transport.MakeSession(nodeA, nodeB);
        arrayB.Set(ridB, QuantityField, "9"u8);
        PublishLocalChanges(arrayB, nodeB, Path.Combine(root, "node-b.state"));

        int secondRound = Transport.SyncRound(nodeA, nodeB, Ns);
        PullNodeIntoArray(nodeA, arrayA, [ridA, ridB]);
        PullNodeIntoArray(nodeB, arrayB, [ridA, ridB]);
        NodePersistence.Save(nodeA, Path.Combine(root, "node-a.state"));
        NodePersistence.Save(nodeB, Path.Combine(root, "node-b.state"));

        string aName = arrayA.GetText(ridA, NameField);
        string aQuantity = arrayA.GetText(ridA, QuantityField);
        string bName = arrayA.GetText(ridB, NameField);
        string bQuantity = arrayA.GetText(ridB, QuantityField);
        string bReplicaQuantity = arrayB.GetText(ridB, QuantityField);

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

static void PublishLocalChanges(StorageArray array, Node node, string durableNodePath)
{
    var page = array.ReadLocalChanges(afterSequence: 0, limit: 10_000);
    foreach (var change in page.Changes)
        node.WriteField(Ns, change.RecordId, change.FieldId, change.Payload);
    NodePersistence.Save(node, durableNodePath);
    if (page.ScannedThroughSequence > 0)
        array.AcknowledgeLocalChanges(page.ScannedThroughSequence);
}

static void PullNodeIntoArray(Node node, StorageArray array, IReadOnlyList<RecordId> records)
{
    foreach (var recordId in records)
    {
        foreach (ushort fieldId in new[] { NameField, QuantityField })
        {
            if (node.TryGetField(Ns, recordId, fieldId, out var field) && field is not null)
                array.ApplyRemote(recordId, fieldId, field.Value);
        }
    }
}

sealed class StorageArray : IDisposable
{
    private const byte Current = 0x01;
    private const byte Feed = 0x02;
    private const byte High = 0x03;
    private const byte Ack = 0x04;
    private readonly StorageDatabase _db;

    public StorageArray(string directory, ulong partitionId)
        => _db = StorageDatabase.Open(directory, partitionId);

    public int Count
    {
        get
        {
            using var snapshot = _db.OpenSnapshot();
            return snapshot.ScanPrefix([Current], ReadOnlySpan<byte>.Empty, int.MaxValue).Count;
        }
    }

    public void Set(RecordId recordId, ushort fieldId, ReadOnlySpan<byte> value)
    {
        byte[] payload = Encode(recordId, fieldId, value);
        ulong sequence = ReadUInt64(_db, [High]) + 1;
        using var batch = _db.BeginWriteBatch();
        batch.Put(CurrentKey(recordId, fieldId), payload);
        batch.Put(FeedKey(sequence), payload);
        batch.Put([High], EncodeUInt64(sequence));
        batch.Commit();
    }

    public void ApplyRemote(RecordId recordId, ushort fieldId, ReadOnlySpan<byte> value)
    {
        using var batch = _db.BeginWriteBatch();
        batch.Put(CurrentKey(recordId, fieldId), Encode(recordId, fieldId, value));
        batch.Commit();
    }

    public string GetText(RecordId recordId, ushort fieldId)
        => _db.TryGet(CurrentKey(recordId, fieldId), out var bytes)
            ? Encoding.UTF8.GetString(Decode(bytes!).Value)
            : string.Empty;

    public IReadOnlyList<ArrayChange> ReadLocalChanges(ulong afterSequence, int limit)
    {
        ulong acked = ReadUInt64(_db, [Ack]);
        using var snapshot = _db.OpenSnapshot();
        var rows = snapshot.ScanPrefix([Feed], FeedKey(Math.Max(afterSequence, acked)), limit);
        var changes = rows.Select(row => new ArrayChange(
            BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(1)),
            Decode(row.Value))).ToArray();
        ulong scanned = changes.Length == limit ? changes[^1].Sequence : ReadUInt64(_db, [High]);
        return new ArrayChangePage(changes, scanned);
    }

    public void AcknowledgeLocalChanges(ulong safeThroughSequence)
    {
        ulong high = ReadUInt64(_db, [High]);
        ulong acked = ReadUInt64(_db, [Ack]);
        if (safeThroughSequence > high) throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
        if (safeThroughSequence <= acked) return;
        using var snapshot = _db.OpenSnapshot();
        var rows = snapshot.ScanPrefix([Feed], FeedKey(acked), int.MaxValue);
        using var batch = _db.BeginWriteBatch();
        foreach (var row in rows)
        {
            ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(1));
            if (sequence > safeThroughSequence) break;
            batch.Delete(row.Key);
        }
        batch.Put([Ack], EncodeUInt64(safeThroughSequence));
        batch.Commit();
    }

    private static byte[] CurrentKey(RecordId recordId, ushort fieldId)
    {
        byte[] key = new byte[19];
        key[0] = Current;
        recordId.WriteTo(key.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(17), fieldId);
        return key;
    }

    private static byte[] FeedKey(ulong sequence)
    {
        byte[] key = new byte[9];
        key[0] = Feed;
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(1), sequence);
        return key;
    }

    private static byte[] Encode(RecordId recordId, ushort fieldId, ReadOnlySpan<byte> value)
    {
        byte[] bytes = new byte[22 + value.Length];
        recordId.WriteTo(bytes);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(16), fieldId);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(18), checked((uint)value.Length));
        value.CopyTo(bytes.AsSpan(22));
        return bytes;
    }

    private static ArrayChangeValue Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 22) throw new InvalidDataException("Array value is truncated.");
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(18));
        if (length > int.MaxValue || bytes.Length != 22 + (int)length)
            throw new InvalidDataException("Array value length is invalid.");
        return new ArrayChangeValue(
            RecordId.From(bytes.Slice(0, 16)),
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(16)),
            bytes.Slice(22).ToArray());
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static ulong ReadUInt64(StorageDatabase db, ReadOnlySpan<byte> key)
        => db.TryGet(key, out var value) ? BinaryPrimitives.ReadUInt64LittleEndian(value!) : 0;

    public void Dispose() => _db.Dispose();
}

sealed record ArrayChangePage(IReadOnlyList<ArrayChange> Changes, ulong ScannedThroughSequence);

sealed record ArrayChange(ulong Sequence, ArrayChangeValue Mutation)
{
    public RecordId RecordId => Mutation.RecordId;
    public ushort FieldId => Mutation.FieldId;
    public byte[] Payload => Mutation.Value;
}

sealed record ArrayChangeValue(RecordId RecordId, ushort FieldId, byte[] Value);
