using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ChangeIdWireTests
{
    private static byte[] Id(byte value)
    {
        var bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    [Fact]
    public void Batch_payload_roundtrip_preserves_change_id_and_binds_it_to_records_root()
    {
        var node = new Node(Id(1));
        var changeId = BatchId.From(Id(0xA1));
        var entry = new OutboxEntry
        {
            NamespaceId = 7,
            Seq = 1,
            ChangeId = changeId,
            RecordId = RecordId.From(Id(0x22)),
            OpKind = 0x01,
            Payload = new byte[] { 0x10, 0x20, 0x30 },
            AuthorNodeId = NodeId.From(Id(1)),
        };
        var entries = new List<OutboxEntry> { entry };
        var root = Node.ComputeRecordsRoot(entries);
        var batchId = BatchId.From(Id(0xB1));
        var issuer = NodeId.From(Id(1));
        var digest = Node.ComputeBatchDigest(batchId, 7, issuer, 1, 1, 1, root, default);
        var batch = new Batch(7, 0, 1, 0x02, 0, issuer, 1, 1, 1,
            batchId, default, root, digest, entries);

        var encoded = node.EncodeBatchPayload(batch);
        var decoded = node.DecodeBatchPayload(encoded);

        Assert.Equal(changeId, decoded.Entries[0].ChangeId);
        Assert.Equal(root, Node.ComputeRecordsRoot(decoded.Entries));
        Assert.Equal(digest, decoded.BatchDigest);

        var changedIdEntry = new OutboxEntry
        {
            NamespaceId = entry.NamespaceId,
            ChangeId = BatchId.From(Id(0xA2)),
            RecordId = entry.RecordId,
            OpKind = entry.OpKind,
            Payload = entry.Payload,
            AuthorNodeId = entry.AuthorNodeId,
        };
        Assert.NotEqual(root, Node.ComputeRecordsRoot(new List<OutboxEntry> { changedIdEntry }));
    }

    [Fact]
    public void Records_array_roundtrip_includes_change_id_in_wire_layout()
    {
        var entry = new OutboxEntry
        {
            ChangeId = BatchId.From(Id(0xCC)),
            RecordId = RecordId.From(Id(0x11)),
            OpKind = 3,
            Payload = new byte[] { 4, 5 },
        };

        var bytes = Node.EncodeRecordsArray(new List<OutboxEntry> { entry });
        var decoded = Node.DecodeRecordsArray(bytes);

        Assert.Single(decoded);
        Assert.Equal(entry.RecordId.ToArray(), decoded[0].Rid);
        Assert.Equal(entry.ChangeId.ToArray(), decoded[0].ChangeId);
        Assert.Equal(entry.OpKind, decoded[0].Kind);
        Assert.Equal(entry.Payload.ToArray(), decoded[0].Payload);
    }
}
