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

    private static void CompleteBaseline(StorageMetadataStore store, string scope,
        NodeId local, NodeId peer, uint peerEpoch, int namespaceId,
        NodeId author, ulong streamOffset = 0, uint issuerEpoch = 1)
    {
        store.RegisterInterestedPeer(scope, local, peer, peerEpoch, namespaceId);
        store.CompletePeerInitialSync(scope, local, peer, peerEpoch, namespaceId,
            new[] { new MetadataPeerStreamBaseline(author, streamOffset, issuerEpoch) });
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
                CompleteBaseline(store, scope, local, peer, 7, 9, author, issuerEpoch: 3);
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

                // An unrelated full-partition mutation must preserve the targeted peer rows.
                afterAck.FreezeOutbound(scope, local, 8, local, new[] { FrozenOperation(90, 91) });
            }

            using (var afterUnrelatedMutation = new StorageMetadataStore(directory, 415, deleteOnDispose: false))
            {
                var state = afterUnrelatedMutation.GetPeerStreamState(scope, local, peer, 7, 9, author);
                Assert.Equal(3UL, state.AckedCursor);
                Assert.Equal(Batch(44), Assert.IsType<MetadataOutboundBatchReservation>(state.PendingBatch).BatchId);
                Assert.Equal(1UL, afterUnrelatedMutation.GetOutboundHighWatermark(scope, local, 8, local));
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

        store.RegisterInterestedPeer(scope, local, peer, 2, 7);
        store.RegisterInterestedPeerStream(scope, local, peer, 2, 7, author,
            3, 100, 5, previous);
        var baselineState = store.GetPeerStreamState(scope, local, peer, 2, 7, author);
        Assert.Equal(3UL, baselineState.AckedCursor);
        Assert.Equal(3UL, baselineState.PushedCursor);
        Assert.Equal(5u, baselineState.IssuerEpoch);
        Assert.Equal(100UL, baselineState.StreamOffset);
        Assert.Equal(previous, baselineState.LastBatchDigest);
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
    public void Inbox_receipt_identity_is_independent_of_resolved_author_stream()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "receipt-author-independent"), 440);
        NodeId local = Node(1), sender = Node(2), authorA = Node(3);
        const string scope = "receipt-author-independent";
        var first = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 5,
            AuthorStreamNodeId = authorA, NamespaceId = 9, BatchId = Batch(140),
            CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)141, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = authorA, ChangeId = Batch(142),
                    Fingerprint = Enumerable.Repeat((byte)143, 16).ToArray(),
                    Payload = new byte[] { 1, 4, 2 }, RelayEligible = true
                }
            }
        };
        var secondView = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 5,
            AuthorStreamNodeId = default, NamespaceId = 9, BatchId = first.BatchId,
            CursorStart = first.CursorStart, CursorEnd = first.CursorEnd,
            PreviousBatchDigest = first.PreviousBatchDigest.ToArray(),
            BatchDigest = first.BatchDigest.ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = authorA, ChangeId = Batch(142),
                    Fingerprint = Enumerable.Repeat((byte)143, 16).ToArray(),
                    Payload = new byte[] { 1, 4, 2 }, RelayEligible = true
                }
            }
        };

        var accepted = store.AcceptIncomingBatch(first);
        Assert.False(accepted.IsDuplicate);
        Assert.Equal(authorA, accepted.DurableAuthor);
        var duplicate = store.AcceptIncomingBatch(secondView);
        Assert.True(duplicate.IsDuplicate);
        Assert.Equal(authorA, duplicate.DurableAuthor);
        var duplicatePending = store.BeginPendingApply(secondView,
            Guid.Parse("00000000-0000-0000-0000-000000000440"));
        Assert.True(duplicatePending.IsDuplicate);
        var equivocation = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 5,
            AuthorStreamNodeId = default, NamespaceId = 9, BatchId = first.BatchId,
            CursorStart = first.CursorStart, CursorEnd = first.CursorEnd,
            PreviousBatchDigest = first.PreviousBatchDigest.ToArray(),
            BatchDigest = Enumerable.Repeat((byte)146, 16).ToArray(),
            Operations = first.Operations
        };
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(equivocation));
        Assert.Equal(1UL, store.GetReceiveCursor(scope, local, sender, 5, 9, authorA));
        Assert.Equal(1UL, store.GetRelayHighWatermark(scope, local, 9, authorA));
    }

    [Fact]
    public void Completed_pending_apply_uses_a_distinct_retry_exception()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "completed-pending-token"), 438);
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        const string scope = "completed-pending-token";
        var incoming = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(120),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)121, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(122),
                    Fingerprint = Enumerable.Repeat((byte)123, 16).ToArray(),
                    Payload = new byte[] { 1, 2, 3 },
                    RelayEligible = true
                }
            }
        };
        Guid token = Guid.Parse("00000000-0000-0000-0000-000000000438");

        store.BeginPendingApply(incoming, token);
        store.FinalizePendingApply(scope, local,
            new MetadataProviderApplyReceipt(token, incoming.BatchDigest.ToArray()));

        Assert.Throws<MetadataPendingApplyAlreadyFinalizedException>(
            () => store.BeginPendingApply(incoming, token));
    }

    [Fact]
    public void Finalize_pending_apply_preserves_prior_partition_state_across_reopen()
    {
        string directory = Path.Combine(_root, "pending-finalize-delta-reopen");
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        const string scope = "pending-finalize-delta-reopen";
        byte[] firstDigest = Enumerable.Repeat((byte)131, 16).ToArray();
        byte[] secondDigest = Enumerable.Repeat((byte)132, 16).ToArray();
        var first = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 1,
            AuthorStreamNodeId = author, NamespaceId = 7, BatchId = Batch(130),
            CursorStart = 1, CursorEnd = 1, PreviousBatchDigest = new byte[16],
            BatchDigest = firstDigest,
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author, ChangeId = Batch(133),
                    Fingerprint = Enumerable.Repeat((byte)134, 16).ToArray(),
                    Payload = new byte[] { 1, 3, 3 }, RelayEligible = true
                }
            }
        };
        var second = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 1,
            AuthorStreamNodeId = author, NamespaceId = 7, BatchId = Batch(135),
            CursorStart = 2, CursorEnd = 2, PreviousBatchDigest = firstDigest,
            BatchDigest = secondDigest,
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author, ChangeId = Batch(136),
                    Fingerprint = Enumerable.Repeat((byte)137, 16).ToArray(),
                    Payload = new byte[] { 2, 4, 6 }, RelayEligible = true
                }
            }
        };
        Guid token = Guid.Parse("00000000-0000-0000-0000-000000000439");

        using (var store = new StorageMetadataStore(directory, 439, deleteOnDispose: false))
        {
            store.AcceptIncomingBatch(first);
            store.BeginPendingApply(second, token);
            var finalized = store.FinalizePendingApply(scope, local,
                new MetadataProviderApplyReceipt(token, secondDigest));
            Assert.False(finalized.IsDuplicate);
            Assert.Equal(1, finalized.NewRelayOperationCount);
        }

        using var reopened = new StorageMetadataStore(directory, 439, deleteOnDispose: false);
        Assert.Empty(reopened.ReadPendingApplyIntents(scope, local));
        Assert.Equal(2UL, reopened.GetReceiveCursor(scope, local, sender, 1, 7, author));
        Assert.Equal(2UL, reopened.GetRelayHighWatermark(scope, local, 7, author));
        var relay = reopened.ReadRelay(scope, local, 7, author, 0, 10);
        Assert.Equal(2, relay.Items.Count);
        Assert.Equal(Batch(133), relay.Items[0].ChangeId);
        Assert.Equal(Batch(136), relay.Items[1].ChangeId);
        var retry = reopened.FinalizePendingApply(scope, local,
            new MetadataProviderApplyReceipt(token, secondDigest));
        Assert.True(retry.IsDuplicate);
        Assert.Equal(2UL, retry.CursorEnd);
        Assert.Equal(author, retry.DurableAuthor);
    }

    [Fact]
    public void Pending_apply_wire_payload_survives_restart_for_node_replay()
    {
        string directory = Path.Combine(_root, "pending-wire-replay");
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        const string scope = "pending-wire-replay";
        Guid token = Guid.Parse("a1000000-0000-0000-0000-000000000001");
        byte[] wire = Enumerable.Range(0, 128).Select(value => (byte)value).ToArray();
        var incoming = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 2,
            AuthorStreamNodeId = author, NamespaceId = 7, BatchId = Batch(110),
            CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)111, 16).ToArray(),
            WirePayload = wire,
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author, ChangeId = Batch(112),
                    Fingerprint = Enumerable.Repeat((byte)113, 16).ToArray(),
                    Payload = new byte[17], RelayEligible = true
                }
            }
        };

        using (var store = new StorageMetadataStore(directory, 426, deleteOnDispose: false))
            store.BeginPendingApply(incoming, token);

        using var reopened = new StorageMetadataStore(directory, 426, deleteOnDispose: false);
        var pending = Assert.Single(reopened.ReadPendingApplyIntents(scope, local));
        Assert.Equal(token, pending.ApplyToken);
        Assert.Equal(wire, pending.Batch.WirePayload);
        Assert.Equal(Batch(110), pending.Batch.BatchId);
    }

    [Fact]
    public void Pending_apply_validates_sender_cursor_and_digest_chain_before_Node_mutation()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "pending-receive-validation"), 435);
        NodeId local = Node(1), sender = Node(2), authorA = Node(3), authorB = Node(4);
        const string scope = "pending-receive-validation";
        byte[] digest = Enumerable.Repeat((byte)151, 16).ToArray();

        MetadataIncomingBatch Create(NodeId author, byte batchSeed, ulong start,
            byte[] previous, byte[] nextDigest) => new()
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(batchSeed),
            CursorStart = start,
            CursorEnd = start,
            PreviousBatchDigest = previous.ToArray(),
            BatchDigest = nextDigest.ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch((byte)(batchSeed + 20)),
                    Fingerprint = Enumerable.Repeat(batchSeed, 16).ToArray(),
                    Payload = new byte[] { batchSeed, 1 },
                    RelayEligible = true
                }
            }
        };

        var gap = Create(authorA, 10, 2, new byte[16], digest);
        Assert.Throws<InvalidDataException>(() =>
            store.BeginPendingApply(gap, Guid.Parse("00000000-0000-0000-0000-000000000435")));
        Assert.Empty(store.ReadPendingApplyIntents(scope, local));

        var first = Create(authorA, 11, 1, new byte[16], digest);
        store.BeginPendingApply(first, Guid.Parse("00000000-0000-0000-0000-000000000436"));
        var secondAuthorSameSession = Create(authorB, 12, 2, digest,
            Enumerable.Repeat((byte)152, 16).ToArray());
        Assert.Throws<InvalidOperationException>(() =>
            store.BeginPendingApply(secondAuthorSameSession,
                Guid.Parse("00000000-0000-0000-0000-000000000437")));
        Assert.Single(store.ReadPendingApplyIntents(scope, local));
    }

    [Fact]
    public void Pending_apply_rejects_global_ChangeId_equivocation_before_Node_mutation()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "pending-changeid-equivocation"), 436);
        NodeId local = Node(1), senderA = Node(2), senderB = Node(4), author = Node(3);
        const string scope = "pending-changeid-equivocation";
        BatchId sharedChangeId = Batch(153);

        MetadataIncomingBatch Create(NodeId sender, byte batchSeed, byte fingerprint) => new()
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(batchSeed),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat(batchSeed, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = sharedChangeId,
                    Fingerprint = Enumerable.Repeat(fingerprint, 16).ToArray(),
                    Payload = new byte[] { batchSeed, 2 },
                    RelayEligible = true
                }
            }
        };

        var first = Create(senderA, 20, 21);
        store.BeginPendingApply(first, Guid.Parse("00000000-0000-0000-0000-000000000438"));
        var equivocation = Create(senderB, 22, 23);
        Assert.Throws<InvalidDataException>(() =>
            store.BeginPendingApply(equivocation, Guid.Parse("00000000-0000-0000-0000-000000000439")));
        Assert.Single(store.ReadPendingApplyIntents(scope, local));
    }

    [Fact]
    public void Pending_ChangeId_identity_is_scoped_by_namespace()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "pending-changeid-namespace"), 437);
        NodeId local = Node(1), author = Node(3);
        const string scope = "pending-changeid-namespace";
        BatchId sharedChangeId = Batch(153);

        MetadataIncomingBatch Create(NodeId sender, int namespaceId, byte batchSeed, byte fingerprint) => new()
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = namespaceId,
            BatchId = Batch(batchSeed),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat(batchSeed, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = sharedChangeId,
                    Fingerprint = Enumerable.Repeat(fingerprint, 16).ToArray(),
                    Payload = new byte[] { batchSeed, 2 },
                    RelayEligible = true
                }
            }
        };

        store.BeginPendingApply(Create(Node(2), 7, 20, 21),
            Guid.Parse("00000000-0000-0000-0000-000000000437"));
        store.BeginPendingApply(Create(Node(4), 8, 22, 23),
            Guid.Parse("00000000-0000-0000-0000-000000000438"));

        Assert.Equal(2, store.ReadPendingApplyIntents(scope, local).Count);
    }

    [Fact]
    public void Relay_page_is_read_from_metadata_at_each_downstream_peers_own_cursor()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-relay-page"), 422);
        NodeId local = Node(1), upstream = Node(2), downstream = Node(4), author = Node(3);
        const string scope = "peer-relay-page";
        byte[] envelope = new byte[21];
        Node(9).WriteTo(envelope);
        envelope[16] = 1;
        envelope[17] = 2;
        envelope[18] = 3;
        envelope[19] = 4;
        envelope[20] = 5;
        var incoming = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = upstream,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(100),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)101, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(102),
                    Fingerprint = Enumerable.Repeat((byte)103, 16).ToArray(),
                    Payload = envelope,
                    RelayEligible = true
                }
            }
        };

        var receipt = store.AcceptIncomingBatch(incoming);
        Assert.False(receipt.IsDuplicate);
        Assert.Equal(new[] { author }, store.ReadRelayAuthors(scope, local, 7));
        store.RegisterInterestedPeerStream(scope, local, downstream, 2, 7, author,
            0, 0, 1, new byte[16]);
        store.CompletePeerInitialSync(scope, local, downstream, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        var page = store.ReadRelayForPeer(scope, local, downstream, 2, 7, author, 10);
        var item = Assert.Single(page.Items);
        Assert.Equal(1UL, item.Sequence);
        Assert.Equal(author, item.OriginalAuthor);
        Assert.Equal(Batch(102), item.ChangeId);
        Assert.Equal(envelope, item.Payload);
    }

    [Fact]
    public void Durable_receipt_rejects_full_header_equivocation_after_reopen()
    {
        string directory = Path.Combine(_root, "header-fingerprint-receipt");
        Directory.CreateDirectory(directory);
        const string scope = "header-fingerprint-receipt";
        NodeId local = Node(81), sender = Node(82), author = Node(83);
        static byte[] Fingerprint(byte seed)
        {
            var bytes = new byte[16];
            bytes[15] = seed;
            return bytes;
        }

        var batch = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 3,
            AuthorStreamNodeId = author,
            NamespaceId = 9,
            BatchId = Batch(84),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = Fingerprint(0),
            BatchDigest = Fingerprint(85),
            HeaderFingerprint = Fingerprint(86),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(87),
                    Fingerprint = Fingerprint(88),
                    Payload = new byte[] { 89 },
                    RelayEligible = false
                }
            }
        };
        var equivocation = new MetadataIncomingBatch
        {
            ScopeId = batch.ScopeId,
            LocalReplica = batch.LocalReplica,
            SenderPeer = batch.SenderPeer,
            SenderEpoch = batch.SenderEpoch,
            AuthorStreamNodeId = batch.AuthorStreamNodeId,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = batch.CursorStart,
            CursorEnd = batch.CursorEnd,
            PreviousBatchDigest = batch.PreviousBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            HeaderFingerprint = Fingerprint(90),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = batch.Operations
        };

        using var storage = StorageDatabase.Open(directory, 453);
        using (var store = new StorageMetadataStore(storage))
        {
            MetadataBatchReceipt accepted = store.AcceptIncomingBatch(batch);
            Assert.Equal(batch.HeaderFingerprint, accepted.HeaderFingerprint);
            Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(equivocation));
        }

        using var reopened = new StorageMetadataStore(storage);
        MetadataBatchReceipt duplicate = reopened.AcceptIncomingBatch(batch);
        Assert.True(duplicate.IsDuplicate);
        Assert.Equal(batch.HeaderFingerprint, duplicate.HeaderFingerprint);
        Assert.Throws<InvalidDataException>(() => reopened.AcceptIncomingBatch(equivocation));
    }

    [Fact]
    public void Legacy_pending_v4_and_v3_intents_do_not_claim_header_authority()
    {
        var batch = new MetadataIncomingBatch
        {
            ScopeId = "legacy-pending-codec",
            LocalReplica = Node(91),
            SenderPeer = Node(92),
            SenderEpoch = 1,
            TransportPeerEpoch = 17,
            AuthorStreamNodeId = Node(93),
            NamespaceId = 9,
            BatchId = Batch(94),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)95, 16).ToArray(),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = Array.Empty<MetadataOperation>()
        };
        var encode = typeof(StorageMetadataStore).GetMethod(
            "EncodeIncomingBatch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var decode = typeof(StorageMetadataStore).GetMethod(
            "DecodeIncomingBatch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        byte[] version6 = (byte[])encode.Invoke(null, new object[] { batch })!;
        var decoded6 = (MetadataIncomingBatch)decode.Invoke(null, new object[] { version6 })!;
        Assert.Equal(17U, decoded6.TransportPeerEpoch);

        int scopeLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(version6.AsSpan(4));
        int transportEpochOffset = checked(4 + 4 + scopeLength + 16 + 16 + 4);
        byte[] version5 = new byte[version6.Length - 4];
        version6.AsSpan(0, transportEpochOffset).CopyTo(version5);
        version6.AsSpan(transportEpochOffset + 4).CopyTo(version5.AsSpan(transportEpochOffset));
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(version5.AsSpan(0, 4), -5);

        byte[] version4 = version5[..^1];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(version4.AsSpan(0, 4), -4);
        var decoded4 = (MetadataIncomingBatch)decode.Invoke(null, new object[] { version4 })!;
        Assert.False(decoded4.ReceiveStateOwnedByMetadataStore);
        Assert.Empty(decoded4.HeaderFingerprint);

        byte[] version3 = version5[..^2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(version3.AsSpan(0, 4), -3);
        var decoded3 = (MetadataIncomingBatch)decode.Invoke(null, new object[] { version3 })!;
        Assert.False(decoded3.ReceiveStateOwnedByMetadataStore);
        Assert.Empty(decoded3.HeaderFingerprint);
    }

    [Fact]
    public void Peer_roster_baseline_and_epoch_retirement_commit_as_atomic_deltas()
    {
        string directory = Path.Combine(_root, "peer-lifecycle-deltas");
        Directory.CreateDirectory(directory);
        const string scope = "peer-lifecycle-deltas";
        NodeId local = Node(1), peer = Node(2), author = Node(3);

        try
        {
            using var storage = StorageDatabase.Open(directory, 443);
            using (var store = new StorageMetadataStore(storage))
            {
                store.FreezeOutbound(scope, local, 7, author,
                    new[] { FrozenOperation(130, 131), FrozenOperation(132, 133) });

                ulong beforeRegister = storage.LastCommitSeq;
                store.RegisterInterestedPeer(scope, local, peer, 1, 7);
                Assert.Equal(beforeRegister + 1, storage.LastCommitSeq);
                var registered = Assert.Single(store.ReadPeerStreamStates(scope, local, peer, 1, 7));
                Assert.Equal(author, registered.AuthorStream);
                Assert.Equal(0UL, registered.State.AckedCursor);

                ulong afterRegister = storage.LastCommitSeq;
                store.RegisterInterestedPeer(scope, local, peer, 1, 7);
                Assert.Equal(afterRegister, storage.LastCommitSeq);

                ulong beforeBaseline = storage.LastCommitSeq;
                store.CompletePeerInitialSync(scope, local, peer, 1, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 2, 1) });
                Assert.Equal(beforeBaseline + 1, storage.LastCommitSeq);
                Assert.True(store.IsPeerInitialSyncComplete(scope, local, peer, 1, 7));
                Assert.Equal(2UL, store.GetPeerStreamState(scope, local, peer, 1, 7, author).StreamOffset);

                ulong afterBaseline = storage.LastCommitSeq;
                store.CompletePeerInitialSync(scope, local, peer, 1, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 2, 1) });
                Assert.Equal(afterBaseline, storage.LastCommitSeq);

                store.RegisterInterestedPeer(scope, local, peer, 2, 7);
                store.CompletePeerInitialSync(scope, local, peer, 2, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 2, 1) });

                ulong beforeRetirement = storage.LastCommitSeq;
                store.RetireInterestedPeer(scope, local, peer, 1, 2, 7);
                Assert.Equal(beforeRetirement + 1, storage.LastCommitSeq);
                Assert.Single(store.ReadInterestedPeers(scope, local, 7));
                Assert.Equal(2U, Assert.Single(store.ReadInterestedPeers(scope, local, 7)).PeerEpoch);
                Assert.Empty(store.ReadPeerStreamStates(scope, local, peer, 1, 7));

                ulong afterRetirement = storage.LastCommitSeq;
                store.RetireInterestedPeer(scope, local, peer, 1, 2, 7);
                Assert.Equal(afterRetirement, storage.LastCommitSeq);
            }

            using var reopened = new StorageMetadataStore(storage);
            Assert.Equal(2U, Assert.Single(reopened.ReadInterestedPeers(scope, local, 7)).PeerEpoch);
            Assert.Empty(reopened.ReadPeerStreamStates(scope, local, peer, 1, 7));
            Assert.Equal(2UL, reopened.GetPeerStreamState(scope, local, peer, 2, 7, author).StreamOffset);
            Assert.Equal(1U, reopened.GetRetiredPeerEpoch(scope, local, peer, 7));
            Assert.Equal(0UL, reopened.GetPeerStreamState(scope, local, peer, 1, 7, author).AckedCursor);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Namespace_peer_roster_enrolls_future_outbound_author_streams()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-roster-future-stream"), 428);
        NodeId local = Node(1), peerA = Node(2), peerB = Node(4);
        const string scope = "peer-roster-future-stream";
        store.RegisterInterestedPeer(scope, local, peerA, 1, 7);
        store.RegisterInterestedPeer(scope, local, peerB, 2, 7);

        store.FreezeOutbound(scope, local, 7, local, new[] { FrozenOperation(120, 121) });

        var a = Assert.Single(store.ReadPeerStreamStates(scope, local, peerA, 1, 7));
        var b = Assert.Single(store.ReadPeerStreamStates(scope, local, peerB, 2, 7));
        Assert.Equal(local, a.AuthorStream);
        Assert.Equal(local, b.AuthorStream);
        Assert.Equal(0UL, a.State.AckedCursor);
        Assert.Equal(0UL, b.State.AckedCursor);

        byte[] snapshotDigest = Enumerable.Repeat((byte)122, 16).ToArray();
        store.RegisterInterestedPeerStream(scope, local, peerA, 1, 7, local,
            0, 1, 1, snapshotDigest);
        store.RegisterInterestedPeerStream(scope, local, peerA, 1, 7, local,
            0, 0, 0, new byte[16]);
        var snapshotted = Assert.Single(store.ReadPeerStreamStates(scope, local, peerA, 1, 7));
        Assert.Equal(1UL, snapshotted.State.StreamOffset);
        Assert.Equal(1U, snapshotted.State.IssuerEpoch);
        Assert.Equal(snapshotDigest, snapshotted.State.LastBatchDigest);
        Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
        Assert.Equal(2, store.ReadInterestedPeers(scope, local, 7).Count);
    }

    [Fact]
    public void New_peer_requires_confirmed_snapshot_baseline_even_when_first_outbound_sequence_is_retained()
    {
        string directory = Path.Combine(_root, "peer-snapshot-baseline");
        NodeId local = Node(1), peer = Node(2);
        const string scope = "peer-snapshot-baseline";
        try
        {
            using (var store = new StorageMetadataStore(directory, 440, deleteOnDispose: false))
            {
                store.FreezeOutbound(scope, local, 7, local, new[] { FrozenOperation(155, 156) });
                store.RegisterInterestedPeer(scope, local, peer, 1, 7);
                Assert.False(store.IsPeerInitialSyncComplete(scope, local, peer, 1, 7));
                Assert.Throws<SyncException>(() =>
                    store.ReadOutboundForPeer(scope, local, peer, 1, 7, local, 10));

                store.CompletePeerInitialSync(scope, local, peer, 1, 7,
                    new[] { new MetadataPeerStreamBaseline(local, 1, 1) });
                Assert.True(store.IsPeerInitialSyncComplete(scope, local, peer, 1, 7));
                Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
                Assert.Empty(store.ReadOutboundForPeer(scope, local, peer, 1, 7, local, 10).Items);
                store.CompactOutboundThrough(scope, local, 7, local, 1);
            }

            using var reopened = new StorageMetadataStore(directory, 440, deleteOnDispose: false);
            Assert.True(reopened.IsPeerInitialSyncComplete(scope, local, peer, 1, 7));
            Assert.Equal(1UL, reopened.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void New_peer_epoch_fences_and_retires_old_progress_before_releasing_payload_retention()
    {
        string directory = Path.Combine(_root, "peer-epoch-retirement");
        NodeId local = Node(1), peer = Node(2);
        const string scope = "peer-epoch-retirement";
        try
        {
            using (var store = new StorageMetadataStore(directory, 438, deleteOnDispose: false))
            {
                store.RegisterInterestedPeer(scope, local, peer, 1, 7);
                store.FreezeOutbound(scope, local, 7, local, new[] { FrozenOperation(150, 151) });
                Assert.Throws<InvalidOperationException>(() =>
                    store.RetireInterestedPeer(scope, local, peer, 1, 2, 7));

                store.RegisterInterestedPeer(scope, local, peer, 2, 7);
                store.CompletePeerInitialSync(scope, local, peer, 2, 7,
                    new[] { new MetadataPeerStreamBaseline(local, 0, 1) });
                var reservation = new MetadataOutboundBatchReservation
                {
                    BatchId = Batch(152), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)153, 16).ToArray(),
                    Payload = new byte[] { 154 }
                };
                store.ReserveOutboundBatch(scope, local, peer, 2, 7, local, reservation);
                store.MarkOutboundBatchPushed(scope, local, peer, 2, 7, local, reservation.BatchId);
                store.AcknowledgeOutboundBatch(scope, local, peer, 2, 7, local,
                    reservation.BatchId, 1, reservation.BatchDigest);

                Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
                Assert.Throws<InvalidDataException>(() =>
                    store.ReadOutboundForPeer(scope, local, peer, 1, 7, local, 10));

                store.RetireInterestedPeer(scope, local, peer, 1, 2, 7);
                Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
                Assert.Equal(2U, Assert.Single(store.ReadInterestedPeers(scope, local, 7)).PeerEpoch);
                Assert.Empty(store.ReadPeerStreamStates(scope, local, peer, 1, 7));
                Assert.Throws<InvalidDataException>(() => store.RegisterInterestedPeer(scope, local, peer, 1, 7));

                store.CompactOutboundThrough(scope, local, 7, local, 1);
                Assert.Empty(store.ReadOutboundForPeer(scope, local, peer, 2, 7, local, 10).Items);
            }

            using var reopened = new StorageMetadataStore(directory, 438, deleteOnDispose: false);
            Assert.Equal(1UL, reopened.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
            Assert.Throws<InvalidDataException>(() =>
                reopened.ReadOutboundForPeer(scope, local, peer, 1, 7, local, 10));
            Assert.Equal(2U, Assert.Single(reopened.ReadInterestedPeers(scope, local, 7)).PeerEpoch);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Retiring_transport_peer_epoch_reclaims_only_receipts_bound_to_that_epoch()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "receipt-retention-epoch"), 447);
        const string scope = "receipt-retention-epoch";
        NodeId local = Node(1), sender = Node(2), authorA = Node(3), authorB = Node(4);

        store.RegisterInterestedPeer(scope, local, sender, 1, 7);

        MetadataIncomingBatch MakeBatch(NodeId author, uint authorEpoch, byte batchId,
            byte changeId, byte transportEpoch)
            => new()
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = authorEpoch,
                TransportPeerEpoch = transportEpoch,
                AuthorStreamNodeId = author,
                NamespaceId = 7,
                BatchId = Batch(batchId),
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)(batchId + 1), 16).ToArray(),
                HeaderFingerprint = Enumerable.Repeat((byte)(batchId + 2), 16).ToArray(),
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = author,
                        ChangeId = Batch(changeId),
                        Fingerprint = Enumerable.Repeat((byte)(changeId + 1), 16).ToArray(),
                        Payload = new byte[] { changeId }
                    }
                }
            };

        var epochBound = MakeBatch(authorA, 5, 200, 201, 1);
        var legacyUnbound = MakeBatch(authorB, 5, 202, 203, 0);
        Assert.False(store.AcceptIncomingBatch(epochBound).IsDuplicate);
        Assert.False(store.AcceptIncomingBatch(legacyUnbound).IsDuplicate);

        store.RegisterInterestedPeer(scope, local, sender, 2, 7);
        store.CompletePeerInitialSync(scope, local, sender, 2, 7,
            new[]
            {
                new MetadataPeerStreamBaseline(authorA, 0, 5),
                new MetadataPeerStreamBaseline(authorB, 0, 5)
            });
        store.RetireInterestedPeer(scope, local, sender, 1, 2, 7);

        Assert.Equal(1U, store.GetRetiredPeerEpoch(scope, local, sender, 7));
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(epochBound));
        Assert.Throws<InvalidDataException>(() =>
            store.BeginPendingApply(epochBound, Guid.Parse("00000000-0000-0000-0000-000000000447")));

        // The old transport-bound receipt is gone. The retained receive cursor rejects
        // the replay, rather than treating it as a duplicate and re-ACKing it.
        var replayFromNewEpoch = MakeBatch(authorA, 5, 200, 201, 2);
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(replayFromNewEpoch));

        // A genuinely new author epoch can restart its cursor, but the global
        // ChangeId ledger still suppresses the already accepted operation.
        var sameChangeIdNewAuthorEpoch = MakeBatch(authorA, 6, 230, 201, 2);
        var deduplicated = store.AcceptIncomingBatch(sameChangeIdNewAuthorEpoch);
        Assert.False(deduplicated.IsDuplicate);
        Assert.Equal(0, deduplicated.NewRelayOperationCount);

        // Legacy rows without a recorded transport epoch cannot be attributed to the
        // retired incarnation, so they remain retained rather than being guessed away.
        var legacyReplay = MakeBatch(authorB, 5, 202, 203, 2);
        Assert.True(store.AcceptIncomingBatch(legacyReplay).IsDuplicate);
    }

    [Fact]
    public void Legacy_shared_receive_chain_migrates_from_latest_receipt_per_author()
    {
        string directory = Path.Combine(_root, "legacy-receive-chain-migration");
        Directory.CreateDirectory(directory);
        const string scope = "legacy-receive-chain-migration";
        NodeId local = Node(1), sender = Node(2), authorA = Node(3), authorB = Node(4);
        byte[] digestA = Enumerable.Repeat((byte)31, 16).ToArray();
        byte[] digestB = Enumerable.Repeat((byte)32, 16).ToArray();

        try
        {
            using (var storage = StorageDatabase.Open(directory, 451))
            {
                Type partitionKeyType = typeof(StorageMetadataStore).GetNestedType(
                    "PartitionKey", System.Reflection.BindingFlags.NonPublic)!;
                object partitionKey = Activator.CreateInstance(partitionKeyType,
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.Public |
                    System.Reflection.BindingFlags.NonPublic,
                    binder: null, args: new object[] { scope, local }, culture: null)!;
                var rootMethod = typeof(StorageMetadataStore).GetMethod("PartitionRoot",
                    System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
                byte[] root = (byte[])rootMethod.Invoke(null, new[] { partitionKey })!;

                byte[] Key(byte kind, byte[] suffix)
                {
                    byte[] key = new byte[root.Length + 1 + suffix.Length];
                    root.CopyTo(key, 0);
                    key[root.Length] = kind;
                    suffix.CopyTo(key, root.Length + 1);
                    return key;
                }

                byte[] ReceiptKey(NodeId author, byte batchId)
                {
                    byte[] suffix = new byte[38];
                    sender.WriteTo(suffix);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), 5);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), 7);
                    Batch(batchId).WriteTo(suffix.AsSpan(22));
                    return Key(1, suffix);
                }

                byte[] ReceiptValue(NodeId author, byte[] digest, byte header)
                {
                    byte[] value = new byte[84];
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(value, 1);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), 1);
                    digest.CopyTo(value, 32);
                    author.WriteTo(value.AsSpan(52));
                    Enumerable.Repeat(header, 16).ToArray().CopyTo(value, 68);
                    return value;
                }

                byte[] ReceiveKey(NodeId author)
                {
                    byte[] suffix = new byte[38];
                    sender.WriteTo(suffix);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), 5);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), 7);
                    author.WriteTo(suffix.AsSpan(22));
                    return Key(2, suffix);
                }

                byte[] legacyChainSuffix = new byte[22];
                sender.WriteTo(legacyChainSuffix);
                System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(legacyChainSuffix.AsSpan(16), 5);
                System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(legacyChainSuffix.AsSpan(20), 7);

                using var write = storage.BeginWriteBatch();
                write.Put(Key(0, Array.Empty<byte>()), System.Text.Encoding.UTF8.GetBytes(scope));
                write.Put(ReceiptKey(authorA, 250), ReceiptValue(authorA, digestA, 41));
                write.Put(ReceiptKey(authorB, 251), ReceiptValue(authorB, digestB, 42));
                write.Put(ReceiveKey(authorA), EncodeUInt64ForTest(1));
                write.Put(ReceiveKey(authorB), EncodeUInt64ForTest(1));
                // The old format had one shared chain; its digest is the latest batch
                // from author B. The per-author latest receipts reconstruct both chains.
                write.Put(Key(14, legacyChainSuffix), digestB);
                write.Commit();
            }

            static byte[] EncodeUInt64ForTest(ulong value)
            {
                byte[] bytes = new byte[8];
                System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
                return bytes;
            }

            MetadataIncomingBatch Next(NodeId author, byte batchId, byte changeId,
                byte[] previousDigest, byte[] nextDigest, byte header)
                => new()
                {
                    ScopeId = scope,
                    LocalReplica = local,
                    SenderPeer = sender,
                    SenderEpoch = 5,
                    AuthorStreamNodeId = author,
                    NamespaceId = 7,
                    BatchId = Batch(batchId),
                    CursorStart = 2,
                    CursorEnd = 2,
                    PreviousBatchDigest = previousDigest,
                    BatchDigest = nextDigest,
                    HeaderFingerprint = Enumerable.Repeat(header, 16).ToArray(),
                    Operations = new[]
                    {
                        new MetadataOperation
                        {
                            AuthorNodeId = author,
                            ChangeId = Batch(changeId),
                            Fingerprint = Enumerable.Repeat((byte)(changeId + 1), 16).ToArray(),
                            Payload = new byte[] { changeId }
                        }
                    }
                };

            byte[] digestA2 = Enumerable.Repeat((byte)33, 16).ToArray();
            byte[] digestB2 = Enumerable.Repeat((byte)34, 16).ToArray();
            using (var storage = StorageDatabase.Open(directory, 451))
            using (var store = new StorageMetadataStore(storage))
            {
                Assert.Equal(1, store.AcceptIncomingBatch(
                    Next(authorA, 252, 253, digestA, digestA2, 43)).NewRelayOperationCount);
                Assert.Equal(1, store.AcceptIncomingBatch(
                    Next(authorB, 254, 255, digestB, digestB2, 44)).NewRelayOperationCount);
            }

            using var reopenedStorage = StorageDatabase.Open(directory, 451);
            using var reopened = new StorageMetadataStore(reopenedStorage);
            Assert.Equal(1, reopened.AcceptIncomingBatch(
                Next(authorA, 256, 257, digestA2, Enumerable.Repeat((byte)35, 16).ToArray(), 45)).NewRelayOperationCount);
            Assert.Equal(1, reopened.AcceptIncomingBatch(
                Next(authorB, 258, 259, digestB2, Enumerable.Repeat((byte)36, 16).ToArray(), 46)).NewRelayOperationCount);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Retired_epoch_receipt_gc_and_fence_survive_storage_restart()
    {
        string directory = Path.Combine(_root, "receipt-gc-restart");
        Directory.CreateDirectory(directory);
        const string scope = "receipt-gc-restart";
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        var oldBatch = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 5,
            TransportPeerEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(240),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)241, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)242, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(243),
                    Fingerprint = Enumerable.Repeat((byte)244, 16).ToArray(),
                    Payload = new byte[] { 245 }
                }
            }
        };

        try
        {
            using (var storage = StorageDatabase.Open(directory, 450))
            using (var store = new StorageMetadataStore(storage))
            {
                store.RegisterInterestedPeer(scope, local, sender, 1, 7);
                Assert.False(store.AcceptIncomingBatch(oldBatch).IsDuplicate);
                store.RegisterInterestedPeer(scope, local, sender, 2, 7);
                store.CompletePeerInitialSync(scope, local, sender, 2, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 0, 5) });
                store.RetireInterestedPeer(scope, local, sender, 1, 2, 7);
            }

            using var reopenedStorage = StorageDatabase.Open(directory, 450);
            using var reopened = new StorageMetadataStore(reopenedStorage);
            Assert.Equal(1U, reopened.GetRetiredPeerEpoch(scope, local, sender, 7));
            Assert.Throws<InvalidDataException>(() => reopened.AcceptIncomingBatch(oldBatch));

            var retryFromNewEpoch = new MetadataIncomingBatch
            {
                ScopeId = oldBatch.ScopeId,
                LocalReplica = oldBatch.LocalReplica,
                SenderPeer = oldBatch.SenderPeer,
                SenderEpoch = oldBatch.SenderEpoch,
                TransportPeerEpoch = 2,
                AuthorStreamNodeId = oldBatch.AuthorStreamNodeId,
                NamespaceId = oldBatch.NamespaceId,
                BatchId = oldBatch.BatchId,
                CursorStart = oldBatch.CursorStart,
                CursorEnd = oldBatch.CursorEnd,
                PreviousBatchDigest = oldBatch.PreviousBatchDigest,
                BatchDigest = oldBatch.BatchDigest,
                HeaderFingerprint = oldBatch.HeaderFingerprint,
                Operations = oldBatch.Operations
            };
            Assert.Throws<InvalidDataException>(() => reopened.AcceptIncomingBatch(retryFromNewEpoch));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Duplicate_on_new_transport_epoch_refreshes_receipt_retention()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "receipt-refresh-epoch"), 449);
        const string scope = "receipt-refresh-epoch";
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        store.RegisterInterestedPeer(scope, local, sender, 1, 7);

        var original = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 5,
            TransportPeerEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(220),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)221, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)222, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(223),
                    Fingerprint = Enumerable.Repeat((byte)224, 16).ToArray(),
                    Payload = new byte[] { 225 }
                }
            }
        };
        Assert.False(store.AcceptIncomingBatch(original).IsDuplicate);

        store.RegisterInterestedPeer(scope, local, sender, 2, 7);
        store.CompletePeerInitialSync(scope, local, sender, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 5) });
        var retryOnNewEpoch = new MetadataIncomingBatch
        {
            ScopeId = original.ScopeId,
            LocalReplica = original.LocalReplica,
            SenderPeer = original.SenderPeer,
            SenderEpoch = original.SenderEpoch,
            TransportPeerEpoch = 2,
            AuthorStreamNodeId = original.AuthorStreamNodeId,
            NamespaceId = original.NamespaceId,
            BatchId = original.BatchId,
            CursorStart = original.CursorStart,
            CursorEnd = original.CursorEnd,
            PreviousBatchDigest = original.PreviousBatchDigest,
            BatchDigest = original.BatchDigest,
            HeaderFingerprint = original.HeaderFingerprint,
            Operations = original.Operations
        };
        Assert.True(store.AcceptIncomingBatch(retryOnNewEpoch).IsDuplicate);

        store.RetireInterestedPeer(scope, local, sender, 1, 2, 7);

        // The receipt now belongs to the latest authenticated transport incarnation,
        // so retiring epoch 1 must not discard the ACK evidence epoch 2 still needs.
        Assert.True(store.AcceptIncomingBatch(retryOnNewEpoch).IsDuplicate);
    }

    [Fact]
    public void Retiring_transport_epoch_waits_for_its_pending_apply_to_finalize()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "pending-apply-retirement-fence"), 448);
        const string scope = "pending-apply-retirement-fence";
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        store.RegisterInterestedPeer(scope, local, sender, 1, 7);

        var pending = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 4,
            TransportPeerEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(210),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)211, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)212, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(213),
                    Fingerprint = Enumerable.Repeat((byte)214, 16).ToArray(),
                    Payload = new byte[] { 215 }
                }
            }
        };
        store.BeginPendingApply(pending, Guid.Parse("00000000-0000-0000-0000-000000000448"));

        store.RegisterInterestedPeer(scope, local, sender, 2, 7);
        store.CompletePeerInitialSync(scope, local, sender, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 4) });

        Assert.Throws<InvalidOperationException>(() =>
            store.RetireInterestedPeer(scope, local, sender, 1, 2, 7));
        Assert.Single(store.ReadPendingApplyIntents(scope, local));
        Assert.Null(store.GetRetiredPeerEpoch(scope, local, sender, 7));
    }

    [Fact]
    public void Relay_compaction_waits_for_all_namespace_interested_peers()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-safe-relay-compaction"), 429);
        NodeId local = Node(1), upstream = Node(2), author = Node(3), peerA = Node(4), peerB = Node(5);
        const string scope = "peer-safe-relay-compaction";
        store.RegisterInterestedPeer(scope, local, peerA, 1, 7);
        store.RegisterInterestedPeer(scope, local, peerB, 1, 7);
        store.CompletePeerInitialSync(scope, local, peerA, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        store.CompletePeerInitialSync(scope, local, peerB, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        byte[] envelope = new byte[17];
        Node(9).WriteTo(envelope);
        envelope[16] = 7;
        store.AcceptIncomingBatch(new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = upstream, SenderEpoch = 1,
            AuthorStreamNodeId = author, NamespaceId = 7, BatchId = Batch(130),
            CursorStart = 1, CursorEnd = 1, PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)131, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author, ChangeId = Batch(132),
                    Fingerprint = Enumerable.Repeat((byte)133, 16).ToArray(),
                    Payload = envelope, RelayEligible = true
                }
            }
        });
        store.CompletePeerInitialSync(scope, local, peerA, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        store.CompletePeerInitialSync(scope, local, peerB, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        MetadataOutboundBatchReservation Make(byte id, byte digest) => new()
        {
            BatchId = Batch(id), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat(digest, 16).ToArray(),
            Payload = new[] { id }
        };
        store.ReserveOutboundBatch(scope, local, peerA, 1, 7, author, Make(134, 135));
        store.MarkOutboundBatchPushed(scope, local, peerA, 1, 7, author, Batch(134));
        store.AcknowledgeOutboundBatch(scope, local, peerA, 1, 7, author,
            Batch(134), 1, Enumerable.Repeat((byte)135, 16).ToArray());

        Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
        Assert.Throws<ArgumentOutOfRangeException>(() => store.CompactRelayThrough(scope, local, 7, author, 1));
        Assert.Single(store.ReadRelayForPeer(scope, local, peerB, 1, 7, author, 10).Items);

        store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(136, 137));
        store.MarkOutboundBatchPushed(scope, local, peerB, 1, 7, author, Batch(136));
        store.AcknowledgeOutboundBatch(scope, local, peerB, 1, 7, author,
            Batch(136), 1, Enumerable.Repeat((byte)137, 16).ToArray());
        Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
        store.CompactRelayThrough(scope, local, 7, author, 1);
        Assert.Empty(store.ReadRelayForPeer(scope, local, peerB, 1, 7, author, 10).Items);
    }

    [Fact]
    public void Outbound_compaction_waits_for_every_registered_peer_acknowledgement()
    {
        string directory = Path.Combine(_root, "peer-safe-compaction");
        NodeId local = Node(1), peerA = Node(2), peerB = Node(4), author = Node(1);
        const string scope = "peer-safe-compaction";
        try
        {
            using (var store = new StorageMetadataStore(directory, 421, deleteOnDispose: false))
            {
                store.FreezeOutbound(scope, local, 7, author, new[] { FrozenOperation(90, 91) });
                ulong lease = store.AcquireOutboundLease(scope, local, 7, author,
                    Guid.Parse("91000000-0000-0000-0000-000000000001"));
                Assert.Single(store.ReadOutbound(scope, local, 7, author, lease, 0, 10).Items);
                store.AcknowledgeOutbound(scope, local, 7, author, lease, 1);

                store.RegisterInterestedPeerStream(scope, local, peerA, 1, 7, author,
                    0, 0, 1, new byte[16]);
                store.RegisterInterestedPeerStream(scope, local, peerB, 1, 7, author,
                    0, 0, 1, new byte[16]);
                store.CompletePeerInitialSync(scope, local, peerA, 1, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
                store.CompletePeerInitialSync(scope, local, peerB, 1, 7,
                    new[] { new MetadataPeerStreamBaseline(author, 0, 1) });

                MetadataOutboundBatchReservation Make(byte batch) => new()
                {
                    BatchId = Batch(batch), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat(batch, 16).ToArray(),
                    Payload = new[] { batch }
                };

                store.ReserveOutboundBatch(scope, local, peerA, 1, 7, author, Make(92));
                store.MarkOutboundBatchPushed(scope, local, peerA, 1, 7, author, Batch(92));
                store.AcknowledgeOutboundBatch(scope, local, peerA, 1, 7, author,
                    Batch(92), 1, Enumerable.Repeat((byte)92, 16).ToArray());

                Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                    store.CompactOutboundThrough(scope, local, 7, author, 1));
                Assert.Equal(1UL, store.GetOutboundHighWatermark(scope, local, 7, author));

                store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(93));
                store.MarkOutboundBatchPushed(scope, local, peerB, 1, 7, author, Batch(93));
                store.AcknowledgeOutboundBatch(scope, local, peerB, 1, 7, author,
                    Batch(93), 1, Enumerable.Repeat((byte)93, 16).ToArray());
                Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
                store.CompactOutboundThrough(scope, local, 7, author, 1);
                Assert.Empty(store.ReadOutbound(scope, local, 7, author, 1, 0, 10).Items);
            }

            using var reopened = new StorageMetadataStore(directory, 421, deleteOnDispose: false);
            Assert.Equal(1UL, reopened.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
            Assert.Empty(reopened.ReadOutbound(scope, local, 7, author, 1, 0, 10).Items);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Outbound_peer_ack_horizon_can_compact_without_legacy_local_consumer_lease()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "peer-only-outbound-compaction"), 431);
        NodeId local = Node(1), peer = Node(2);
        const string scope = "peer-only-outbound-compaction";
        store.RegisterInterestedPeer(scope, local, peer, 1, 7);
        store.FreezeOutbound(scope, local, 7, local, new[] { FrozenOperation(140, 141) });
        store.CompletePeerInitialSync(scope, local, peer, 1, 7,
            new[] { new MetadataPeerStreamBaseline(local, 0, 1) });

        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(142), IssuerEpoch = 1, CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)143, 16).ToArray(),
            Payload = new byte[] { 144 }
        };
        store.ReserveOutboundBatch(scope, local, peer, 1, 7, local, reservation);
        store.MarkOutboundBatchPushed(scope, local, peer, 1, 7, local, reservation.BatchId);
        store.AcknowledgeOutboundBatch(scope, local, peer, 1, 7, local,
            reservation.BatchId, 1, reservation.BatchDigest);

        Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, local));
        store.CompactOutboundThrough(scope, local, 7, local, 1);
        Assert.Empty(store.ReadOutboundForPeer(scope, local, peer, 1, 7, local, 10).Items);
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

        CompleteBaseline(store, scope, local, peerA, 1, 7, author);
        CompleteBaseline(store, scope, local, peerB, 1, 7, author);
        store.ReserveOutboundBatch(scope, local, peerA, 1, 7, author, Make(80, 81));
        store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(82, 83));
        store.RegisterInterestedPeerStream(scope, local, peerB, 1, 7, author, 0, 0, 1, new byte[16]);
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
            CompleteBaseline(store, scope, local, peer, 4, 7, author, issuerEpoch: 1);
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
        CompleteBaseline(store, scope, local, peer, 1, 7, author);
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
                Assert.Equal(1UL, store.AcquireOutboundLease("durable-scope", Node(1), 7, Node(1), consumer));
                store.BeginPendingApply(pending, token);
            }

            using (var reopened = new StorageMetadataStoreConformanceAdapter(directory, 302, deleteOnDispose: false))
            {
                Assert.Equal(2UL, reopened.AcquireOutboundLease("durable-scope", Node(1), 7, Node(1), consumer));
                Assert.Equal(1UL, reopened.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));
                Assert.Equal(1UL, reopened.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(20)));
                Assert.Single(reopened.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Equal(1UL, reopened.GetOutboundHighWatermark("durable-scope", Node(1), 7, Node(1)));
                var outbound = reopened.ReadOutbound("durable-scope", Node(1), 7, Node(1), 2, 0, 10);
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
                reopened.AcknowledgeOutbound("durable-scope", Node(1), 7, Node(1), 2, 1);
                reopened.CompactOutboundThrough("durable-scope", Node(1), 7, Node(1), 1);
                Assert.Empty(reopened.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Empty(reopened.ReadOutbound("durable-scope", Node(1), 7, Node(1), 2, 0, 10).Items);
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
    [Fact]
    public void Partial_outbound_freeze_retry_after_reopen_preserves_existing_sequences()
    {
        string directory = Path.Combine(_root, "partial-freeze-reopen");
        Directory.CreateDirectory(directory);
        const string scope = "partial-freeze-reopen";
        var first = FrozenOperation(91, 101);
        var second = FrozenOperation(92, 102);
        var appended = FrozenOperation(93, 103);
        try
        {
            using (var store = new StorageMetadataStoreConformanceAdapter(directory, 303, deleteOnDispose: false))
            {
                var frozen = store.FreezeOutbound(scope, Node(1), 7, Node(1), new[] { first, second });
                Assert.Equal(new ulong[] { 1, 2 }, frozen.Select(item => item.Sequence));
            }

            using (var reopened = new StorageMetadataStoreConformanceAdapter(directory, 303, deleteOnDispose: false))
            {
                var mixed = reopened.FreezeOutbound(scope, Node(1), 7, Node(1), new[] { first, appended });
                Assert.Equal(new ulong[] { 1, 3 }, mixed.Select(item => item.Sequence));
                Assert.Equal(3UL, reopened.GetOutboundHighWatermark(scope, Node(1), 7, Node(1)));
            }

            using (var retried = new StorageMetadataStoreConformanceAdapter(directory, 303, deleteOnDispose: false))
            {
                var mixedRetry = retried.FreezeOutbound(scope, Node(1), 7, Node(1), new[] { first, appended });
                Assert.Equal(new ulong[] { 1, 3 }, mixedRetry.Select(item => item.Sequence));
                Assert.Equal(3UL, retried.GetOutboundHighWatermark(scope, Node(1), 7, Node(1)));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

}
