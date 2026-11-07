using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.MetadataStore.Conformance;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageMetadataStoreConformanceTests : MetadataStoreConformanceSuite
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-metadata-" + Guid.NewGuid().ToString("N"));

    protected override IMetadataStoreConformanceAdapter CreateStore()
        => new StorageMetadataStoreConformanceAdapter(Path.Combine(_root, Guid.NewGuid().ToString("N")));

    [Fact]
    public void Legacy_outbound_origin_marker_migrates_into_compact_change_ledger_value()
    {
        string directory = Path.Combine(_root, "legacy-ledger");
        Directory.CreateDirectory(directory);
        const string scope = "legacy-ledger-scope";
        NodeId local = Node(1);
        MetadataFrozenOperation frozen = FrozenOperation(61, 71);
        byte[] scopeBytes = Encoding.UTF8.GetBytes(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        SHA256.HashData(scopeBytes).CopyTo(root, 1);
        local.WriteTo(root.AsSpan(33));

        byte[] Suffix(byte kind, ReadOnlySpan<byte> suffix)
        {
            byte[] key = new byte[root.Length + 1 + suffix.Length];
            root.CopyTo(key, 0);
            key[root.Length] = kind;
            suffix.CopyTo(key.AsSpan(root.Length + 1));
            return key;
        }

        byte[] ledgerSuffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(ledgerSuffix, 7);
        frozen.ChangeId.WriteTo(ledgerSuffix.AsSpan(2));
        byte[] streamSuffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(streamSuffix, 7);
        local.WriteTo(streamSuffix.AsSpan(2));
        byte[] relaySuffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(relaySuffix, 7);
        Node(20).WriteTo(relaySuffix.AsSpan(2));
        byte[] outboundSuffix = new byte[26];
        streamSuffix.CopyTo(outboundSuffix, 0);
        BinaryPrimitives.WriteUInt64BigEndian(outboundSuffix.AsSpan(18), 1);
        byte[] outboundValue = new byte[20 + frozen.Payload.Length];
        frozen.ChangeId.WriteTo(outboundValue);
        BinaryPrimitives.WriteInt32LittleEndian(outboundValue.AsSpan(16), frozen.Payload.Length);
        frozen.Payload.CopyTo(outboundValue, 20);

        try
        {
            using var storage = StorageDatabase.Open(directory, 411);
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(Suffix(0, ReadOnlySpan<byte>.Empty), scopeBytes);
                batch.Put(Suffix(3, ledgerSuffix), frozen.Fingerprint.ToArray());
                batch.Put(Suffix(7, outboundSuffix), outboundValue);
                batch.Put(Suffix(5, relaySuffix), EncodeUInt64(1));
                batch.Put(Suffix(6, relaySuffix), EncodeUInt64(3));
                batch.Put(Suffix(8, streamSuffix), EncodeUInt64(1));
                batch.Put(Suffix(9, streamSuffix), EncodeUInt64(1));
                batch.Put(Suffix(11, ledgerSuffix), Array.Empty<byte>());
                batch.Commit();
            }

            using (var metadata = new StorageMetadataStore(storage))
            {
                var retry = Assert.Single(metadata.FreezeOutbound(scope, local, 7, local, new[] { frozen }));
                Assert.Equal(1UL, retry.Sequence);
                metadata.AcquireOutboundLease(scope, local, 7, local,
                    Guid.Parse("61000000-0000-0000-0000-000000000001"));
            }

            byte[] compactLedgerKey = Suffix(3, ledgerSuffix);
            using var snapshot = storage.OpenSnapshot();
            var rows = snapshot.ScanPrefix(root, ReadOnlySpan<byte>.Empty, 100);
            Assert.DoesNotContain(rows, row => row.Key.Length > root.Length && row.Key[root.Length] is 6 or 9 or 11);
            var compactLedger = Assert.Single(rows, row => row.Key.AsSpan().SequenceEqual(compactLedgerKey));
            Assert.Equal(17, compactLedger.Value.Length);
            Assert.Equal((byte)1, compactLedger.Value[16]);

            var compactRelayWatermark = Assert.Single(rows, row =>
                row.Key.AsSpan().SequenceEqual(Suffix(5, relaySuffix)));
            Assert.Equal(16, compactRelayWatermark.Value.Length);
            Assert.Equal(1UL, BinaryPrimitives.ReadUInt64LittleEndian(compactRelayWatermark.Value));
            Assert.Equal(3UL, BinaryPrimitives.ReadUInt64LittleEndian(compactRelayWatermark.Value.AsSpan(8)));

            var compactOutboundWatermark = Assert.Single(rows, row =>
                row.Key.AsSpan().SequenceEqual(Suffix(8, streamSuffix)));
            Assert.Equal(16, compactOutboundWatermark.Value.Length);
            Assert.Equal(1UL, BinaryPrimitives.ReadUInt64LittleEndian(compactOutboundWatermark.Value));
            Assert.Equal(1UL, BinaryPrimitives.ReadUInt64LittleEndian(compactOutboundWatermark.Value.AsSpan(8)));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

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
            using (var store = new StorageMetadataStoreConformanceAdapter(directory, 302, deleteOnDispose: false))
            {
                store.AcceptIncomingBatch(first);
                store.FreezeOutbound("durable-scope", Node(1), 7, Node(1),
                    new[] { FrozenOperation(60, 70) });
                store.AcquireOutboundLease("durable-scope", Node(1), 7, Node(1), consumer);
                store.BeginPendingApply(pending, token);
            }

            using (var reopened = new StorageMetadataStoreConformanceAdapter(directory, 302, deleteOnDispose: false))
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

            using (var afterCompaction = new StorageMetadataStoreConformanceAdapter(directory, 302, deleteOnDispose: false))
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
