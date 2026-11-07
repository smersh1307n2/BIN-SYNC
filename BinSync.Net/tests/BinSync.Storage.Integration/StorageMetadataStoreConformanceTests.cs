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
    public void Peer_batch_reservation_is_immutable_durable_and_acknowledged_atomically()
    {
        string directory = Path.Combine(_root, "peer-reservation");
        Directory.CreateDirectory(directory);
        NodeId local = Node(1);
        NodeId peer = Node(2);
        NodeId author = Node(3);
        const string scope = "peer-reservation-scope";
        var first = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(40), IssuerEpoch = 3,
            CursorStart = 1,
            CursorEnd = 3,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)41, 16).ToArray(),
            Payload = new byte[] { 1, 2, 3, 4, 5 }
        };

        try
        {
            using (var store = new StorageMetadataStore(directory, 415, deleteOnDispose: false))
            {
                var reserved = store.ReserveOutboundBatch(scope, local, peer, 7, 9, author, first);
                reserved.Payload[0] = 99;
                var retryCandidate = new MetadataOutboundBatchReservation
                {
                    BatchId = Batch(42), IssuerEpoch = 3,
                    CursorStart = 1,
                    CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)43, 16).ToArray(),
                    Payload = new byte[] { 9 }
                };
                var retry = store.ReserveOutboundBatch(scope, local, peer, 7, 9, author, retryCandidate);
                Assert.Equal(Batch(40), retry.BatchId);
                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, retry.Payload);
                store.MarkOutboundBatchPushed(scope, local, peer, 7, 9, author, Batch(40));
                var state = store.GetPeerStreamState(scope, local, peer, 7, 9, author);
                Assert.Equal(0UL, state.AckedCursor);
                Assert.Equal(3UL, state.PushedCursor);
                Assert.NotNull(state.PendingBatch);
            }

            using (var reopened = new StorageMetadataStore(directory, 415, deleteOnDispose: false))
            {
                var pending = Assert.IsType<MetadataOutboundBatchReservation>(
                    reopened.GetPendingOutboundBatch(scope, local, peer, 7, 9, author));
                Assert.Equal(Batch(40), pending.BatchId);
                Assert.Equal(3u, pending.IssuerEpoch);
                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, pending.Payload);
                reopened.AcknowledgeOutboundBatch(scope, local, peer, 7, 9, author,
                    pending.BatchId, pending.CursorEnd, pending.BatchDigest.ToArray());

                var state = reopened.GetPeerStreamState(scope, local, peer, 7, 9, author);
                Assert.Equal(3UL, state.AckedCursor);
                Assert.Equal(3UL, state.PushedCursor);
                Assert.Equal(3u, state.IssuerEpoch);
                Assert.Equal(pending.BatchDigest, state.LastBatchDigest);
                Assert.Null(state.PendingBatch);
                Assert.Throws<InvalidOperationException>(() =>
                    reopened.AcknowledgeOutboundBatch(scope, local, peer, 7, 9, author,
                        pending.BatchId, pending.CursorEnd, pending.BatchDigest));
            }

            using (var afterAck = new StorageMetadataStore(directory, 415, deleteOnDispose: false))
            {
                var state = afterAck.GetPeerStreamState(scope, local, peer, 7, 9, author);
                Assert.Equal(3UL, state.AckedCursor);
                Assert.Equal(3UL, state.PushedCursor);
                Assert.Null(state.PendingBatch);
                var next = new MetadataOutboundBatchReservation
                {
                    BatchId = Batch(44), IssuerEpoch = 3,
                    CursorStart = 4,
                    CursorEnd = 4,
                    PreviousBatchDigest = state.LastBatchDigest.ToArray(),
                    BatchDigest = Enumerable.Repeat((byte)45, 16).ToArray(),
                    Payload = new byte[] { 6, 7 }
                };
                Assert.Equal(Batch(44), afterAck.ReserveOutboundBatch(
                    scope, local, peer, 7, 9, author, next).BatchId);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void First_peer_reservation_bootstraps_existing_checkpoint_cursor_and_digest()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-reservation-bootstrap"), 417);
        NodeId local = Node(1), peer = Node(2), author = Node(3);
        const string scope = "peer-reservation-bootstrap";
        byte[] previous = Enumerable.Repeat((byte)61, 16).ToArray();
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(62), IssuerEpoch = 5, StreamOffset = 100,
            CursorStart = 4, CursorEnd = 6,
            PreviousBatchDigest = previous,
            BatchDigest = Enumerable.Repeat((byte)63, 16).ToArray(),
            Payload = new byte[] { 7, 8 }
        };

        store.ReserveOutboundBatch(scope, local, peer, 2, 7, author, reservation);
        var state = store.GetPeerStreamState(scope, local, peer, 2, 7, author);
        Assert.Equal(3UL, state.AckedCursor);
        Assert.Equal(3UL, state.PushedCursor);
        Assert.Equal(5u, state.IssuerEpoch);
        Assert.Equal(100UL, state.StreamOffset);
        Assert.Equal(previous, state.LastBatchDigest);
        Assert.Equal(Batch(62), Assert.IsType<MetadataOutboundBatchReservation>(state.PendingBatch).BatchId);
        Assert.Equal(author, Assert.Single(store.ReadPeerStreamStates(scope, local, peer, 2, 7)).AuthorStream);
    }

    [Fact]
    public void Peer_progress_and_pending_reservations_are_isolated_per_remote_peer()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-progress-isolation"), 419);
        NodeId local = Node(1), peerA = Node(2), peerB = Node(4), author = Node(3);
        const string scope = "peer-progress-isolation";
        MetadataOutboundBatchReservation Make(byte batch, byte digest) => new()
        {
            BatchId = Batch(batch), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat(digest, 16).ToArray(),
            Payload = new[] { batch }
        };

        store.ReserveOutboundBatch(scope, local, peerA, 1, 7, author, Make(80, 81));
        store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(82, 83));
        store.MarkOutboundBatchPushed(scope, local, peerA, 1, 7, author, Batch(80));
        store.AcknowledgeOutboundBatch(scope, local, peerA, 1, 7, author,
            Batch(80), 1, Enumerable.Repeat((byte)81, 16).ToArray());

        var a = store.GetPeerStreamState(scope, local, peerA, 1, 7, author);
        var b = store.GetPeerStreamState(scope, local, peerB, 1, 7, author);
        Assert.Equal(1UL, a.AckedCursor);
        Assert.Null(a.PendingBatch);
        Assert.Equal(0UL, b.AckedCursor);
        Assert.Equal(Batch(82), Assert.IsType<MetadataOutboundBatchReservation>(b.PendingBatch).BatchId);
        Assert.Equal(2, store.ReadPeerStreamStates(scope, local, peerA, 1, 7).Count +
            store.ReadPeerStreamStates(scope, local, peerB, 1, 7).Count);
    }

    [Fact]
    public void Author_epoch_change_starts_a_new_digest_chain_without_resetting_peer_cursor()
    {
        string directory = Path.Combine(_root, "peer-reservation-epoch");
        using (var store = new StorageMetadataStore(directory, 418, deleteOnDispose: false))
        {
            NodeId local = Node(1), peer = Node(2), author = Node(3);
            const string scope = "peer-reservation-epoch";
            var first = new MetadataOutboundBatchReservation
            {
                BatchId = Batch(70), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)71, 16).ToArray(),
                Payload = new byte[] { 1 }
            };
            store.ReserveOutboundBatch(scope, local, peer, 4, 7, author, first);
            store.MarkOutboundBatchPushed(scope, local, peer, 4, 7, author, Batch(70));
            store.AcknowledgeOutboundBatch(scope, local, peer, 4, 7, author,
                Batch(70), 1, first.BatchDigest);

            var nextEpoch = new MetadataOutboundBatchReservation
            {
                BatchId = Batch(72), IssuerEpoch = 2, CursorStart = 2, CursorEnd = 2,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)73, 16).ToArray(),
                Payload = new byte[] { 2 }
            };
            store.ReserveOutboundBatch(scope, local, peer, 4, 7, author, nextEpoch);
        }

        using (var reopened = new StorageMetadataStore(directory, 418, deleteOnDispose: false))
        {
            NodeId local = Node(1), peer = Node(2), author = Node(3);
            var state = reopened.GetPeerStreamState("peer-reservation-epoch", local, peer, 4, 7, author);
            Assert.Equal(1UL, state.AckedCursor);
            Assert.Equal(2u, state.IssuerEpoch);
            Assert.Equal(new byte[16], state.LastBatchDigest);
            var pending = Assert.IsType<MetadataOutboundBatchReservation>(state.PendingBatch);
            Assert.Equal(2u, pending.IssuerEpoch);
            Assert.Equal(new byte[16], pending.PreviousBatchDigest);
            reopened.MarkOutboundBatchPushed("peer-reservation-epoch", local, peer, 4, 7, author, pending.BatchId);
            reopened.AcknowledgeOutboundBatch("peer-reservation-epoch", local, peer, 4, 7, author,
                pending.BatchId, pending.CursorEnd, pending.BatchDigest);
            Assert.Equal(2UL, reopened.GetPeerStreamState("peer-reservation-epoch", local, peer, 4, 7, author).AckedCursor);
        }

        try { Directory.Delete(directory, recursive: true); } catch { }
    }

    [Fact]
    public void Peer_batch_reservation_rejects_unpushed_or_equivocated_ack_and_cursor_gaps()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-reservation-validation"), 416);
        NodeId local = Node(1), peer = Node(2), author = Node(3);
        const string scope = "peer-reservation-validation";
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(50), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 2,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)51, 16).ToArray(),
            Payload = new byte[] { 1 }
        };
        store.ReserveOutboundBatch(scope, local, peer, 1, 7, author, reservation);
        Assert.Throws<InvalidDataException>(() => store.AcknowledgeOutboundBatch(
            scope, local, peer, 1, 7, author, Batch(50), 2, reservation.BatchDigest));
        Assert.Throws<InvalidDataException>(() => store.AcknowledgeOutboundBatch(
            scope, local, peer, 1, 7, author, Batch(50), 2, Enumerable.Repeat((byte)99, 16).ToArray()));

        store.MarkOutboundBatchPushed(scope, local, peer, 1, 7, author, Batch(50));
        store.AcknowledgeOutboundBatch(scope, local, peer, 1, 7, author,
            Batch(50), 2, reservation.BatchDigest);
        Assert.Throws<InvalidDataException>(() => store.ReserveOutboundBatch(scope, local, peer, 1, 7, author,
            new MetadataOutboundBatchReservation
            {
                BatchId = Batch(52), IssuerEpoch = 1, CursorStart = 4, CursorEnd = 4,
                PreviousBatchDigest = reservation.BatchDigest.ToArray(),
                BatchDigest = Enumerable.Repeat((byte)53, 16).ToArray(),
                Payload = new byte[] { 2 }
            }));
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
