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
    [Fact]
    public void Change_id_survives_A_to_B_to_C_to_A_and_duplicate_arrival_from_second_peer()
    {
        var a = Fix.MakeNode(41);
        var b = Fix.MakeNode(42);
        var c = Fix.MakeNode(43);

        // Establish all three sessions before the write so A retains its outbox
        // entry until both B and C have acknowledged it.
        Transport.MakeSession(a, b);
        Transport.MakeSession(b, c);
        Transport.MakeSession(a, c);

        Assert.True(a.WriteField(1, Fix.Rid(0x41), 0x100, new byte[] { 0xCA, 0xFE }));
        var originalChangeId = Assert.Single(a.Outbox[1]).ChangeId;
        Assert.False(originalChangeId.IsZero);

        // A -> B: first network hop.
        Assert.NotNull(Transport.DeliverBatch(a, b, 1));
        var bRelay = Assert.Single(b.Outbox[1]);
        Assert.Equal(originalChangeId, bRelay.ChangeId);

        // B -> C: second hop. The issuer changes to B, but ChangeId must not.
        Assert.NotNull(Transport.DeliverBatch(b, c, 1));
        var cRelay = Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, cRelay.ChangeId);

        // A -> C: the same logical mutation reaches C through a second peer.
        // It must be applied idempotently and must not be queued for relay twice.
        Assert.NotNull(Transport.DeliverBatch(a, c, 1));
        Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, c.Outbox[1][0].ChangeId);
        Assert.Contains((a.NodeId, 1, originalChangeId), c.SeenChanges);

        // C -> A closes the mesh loop. A already owns this ChangeId, so it must
        // not create a second outbox item or invent a replacement identity.
        Assert.NotNull(Transport.DeliverBatch(c, a, 1));
        Assert.Single(a.Outbox[1]);
        Assert.Equal(originalChangeId, a.Outbox[1][0].ChangeId);
        Assert.Contains((c.NodeId, 1, originalChangeId), a.SeenChanges);

        Assert.True(a.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(c.Storage[1].ContainsKey(Fix.Rid(0x41)));
    }

}
