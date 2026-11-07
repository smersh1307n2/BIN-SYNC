using BinSync;
using BinSync.MetadataStore.Conformance;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageMetadataStoreConformanceTests : MetadataStoreConformanceSuite
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-metadata-" + Guid.NewGuid().ToString("N"));

    protected override IMetadataStoreConformanceAdapter CreateStore()
        => new StorageMetadataStoreAdapter(Path.Combine(_root, Guid.NewGuid().ToString("N")));

    [Fact]
    public void Receipts_relay_outbound_leases_and_pending_apply_survive_restart_and_compaction()
    {
        string directory = Path.Combine(_root, "restart");
        Directory.CreateDirectory(directory);
        var first = CreateBatch("durable-scope", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        var pending = CreateBatch("durable-scope", 1, 2, 1, 7, 11, 2, 2, 10, 11,
            Operation(21, 31, 41, 51));
        var token = Guid.Parse("00000000-0000-0000-0000-000000000901");
        var consumer = Guid.Parse("00000000-0000-0000-0000-000000000902");

        try
        {
            using (var store = new StorageMetadataStoreAdapter(directory, 302, deleteOnDispose: false))
            {
                store.AcceptIncomingBatch(first);
                store.FreezeOutbound("durable-scope", Node(1), 7, Node(1),
                    new[] { FrozenOperation(60, 70) });
                store.AcquireOutboundLease("durable-scope", Node(1), 7, Node(1), consumer);
                store.BeginPendingApply(pending, token);
            }

            using (var reopened = new StorageMetadataStoreAdapter(directory, 302, deleteOnDispose: false))
            {
                Assert.Equal(1UL, reopened.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));
                Assert.Equal(1UL, reopened.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(20)));
                Assert.Single(reopened.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Equal(1UL, reopened.GetOutboundHighWatermark("durable-scope", Node(1), 7, Node(1)));
                var outbound = reopened.ReadOutbound("durable-scope", Node(1), 7, Node(1), 1, 0, 10);
                Assert.Single(outbound.Items);
                var intent = Assert.Single(reopened.ReadPendingApplyIntents("durable-scope", Node(1)));
                Assert.Equal(token, intent.ApplyToken);
                Assert.Equal(pending.BatchDigest, intent.Batch.BatchDigest);

                var finalized = reopened.FinalizePendingApply("durable-scope", Node(1),
                    new MetadataProviderApplyReceipt(token, pending.BatchDigest.ToArray()));
                Assert.Equal(2UL, finalized.CursorEnd);
                Assert.Empty(reopened.ReadPendingApplyIntents("durable-scope", Node(1)));
                Assert.Equal(2UL, reopened.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));

                reopened.AcknowledgeRelay("durable-scope", Node(1), 7, Node(20), 1);
                reopened.CompactRelayThrough("durable-scope", Node(1), 7, Node(20), 1);
                reopened.AcknowledgeOutbound("durable-scope", Node(1), 7, Node(1), 1, 1);
                reopened.CompactOutboundThrough("durable-scope", Node(1), 7, Node(1), 1);
                Assert.Empty(reopened.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Empty(reopened.ReadOutbound("durable-scope", Node(1), 7, Node(1), 1, 0, 10).Items);
            }

            using (var afterCompaction = new StorageMetadataStoreAdapter(directory, 302, deleteOnDispose: false))
            {
                Assert.Equal(2UL, afterCompaction.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));
                Assert.Equal(1UL, afterCompaction.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(20)));
                Assert.Equal(1UL, afterCompaction.GetOutboundHighWatermark("durable-scope", Node(1), 7, Node(1)));
                Assert.Empty(afterCompaction.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Throws<InvalidDataException>(() => afterCompaction.FreezeOutbound(
                    "durable-scope", Node(1), 7, Node(1), new[] { FrozenOperation(60, 70) }));
                Assert.True(afterCompaction.AcceptIncomingBatch(first).IsDuplicate);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
