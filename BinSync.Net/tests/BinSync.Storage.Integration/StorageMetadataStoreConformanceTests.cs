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
    public void Outbound_freeze_rejects_missing_origin_epoch_without_allocating_sequence()
    {
        string directory = Path.Combine(_root, "missing-origin-epoch");
        Directory.CreateDirectory(directory);
        NodeId local = Node(1);
        const string scope = "missing-origin-epoch-scope";
        try
        {
            using var store = new StorageMetadataStore(directory, 418, deleteOnDispose: true);
            var operation = new MetadataFrozenOperation
            {
                ChangeId = Batch(88),
                Fingerprint = Enumerable.Repeat((byte)7, 16).ToArray(),
                Payload = new byte[] { 0x01, 0x02 }
            };

            Assert.Throws<InvalidDataException>(() =>
                store.FreezeOutbound(scope, local, 7, local, new[] { operation }));
            Assert.Equal(0UL, store.GetOutboundHighWatermark(scope, local, 7, local));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Compact_stream_payload_row_recovers_origin_from_global_change_id_ledger()
    {
        string directory = Path.Combine(_root, "compact-stream-payload");
        Directory.CreateDirectory(directory);
        NodeId local = Node(1);
        const string scope = "compact-stream-payload-scope";
        const int namespaceId = 7;
        byte[] payload = { 0x11, 0x22, 0x33, 0x44 };
        byte[] scopeBytes = Encoding.UTF8.GetBytes(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        SHA256.HashData(scopeBytes).CopyTo(root, 1);
        local.WriteTo(root.AsSpan(33));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 419))
            using (var store = new StorageMetadataStore(storage))
            {
                store.FreezeOutbound(scope, local, namespaceId, local, new[]
                {
                    new MetadataFrozenOperation
                    {
                        ChangeId = Batch(89),
                        Fingerprint = Enumerable.Repeat((byte)9, 16).ToArray(),
                        Payload = payload,
                        OriginAuthorEpoch = 7
                    }
                });
            }

            using (var storage = StorageDatabase.Open(directory, 419))
            using (var store = new StorageMetadataStore(storage))
            {
                ulong lease = store.AcquireOutboundLease(scope, local, namespaceId, local, Guid.NewGuid());
                var item = Assert.Single(store.ReadOutbound(scope, local, namespaceId, local, lease, 0, 4).Items);
                Assert.Equal(new OperationOrigin(local, 7, 1), item.Provenance);
                Assert.Equal(payload, item.Payload);

                using var snapshot = storage.OpenSnapshot();
                var rows = snapshot.ScanPrefix(root, ReadOnlySpan<byte>.Empty, 100);
                var payloadRow = Assert.Single(rows, row => row.Key[root.Length] == 21);
                Assert.Equal(20 + payload.Length, payloadRow.Value.Length);
                Assert.Equal(Batch(89), BatchId.From(payloadRow.Value.AsSpan(0, 16)));
                Assert.Equal(payload.Length, BinaryPrimitives.ReadInt32LittleEndian(payloadRow.Value.AsSpan(16)));
                Assert.Equal(payload, payloadRow.Value.AsSpan(20).ToArray());
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public void Compact_stream_payload_hydration_rejects_missing_or_mismatched_change_id_identity(
        bool includeLedger, bool mismatchedAuthor)
    {
        string directory = Path.Combine(_root, "compact-stream-corruption-" + includeLedger);
        Directory.CreateDirectory(directory);
        const string scope = "compact-stream-corruption-scope";
        const int namespaceId = 7;
        NodeId local = Node(1);
        NodeId ledgerAuthor = mismatchedAuthor ? Node(2) : local;
        byte[] scopeBytes = Encoding.UTF8.GetBytes(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        SHA256.HashData(scopeBytes).CopyTo(root, 1);
        local.WriteTo(root.AsSpan(33));
        byte[] identity = new byte[8 + scopeBytes.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(identity, 0x33534D42);
        BinaryPrimitives.WriteInt32LittleEndian(identity.AsSpan(4), scopeBytes.Length);
        scopeBytes.CopyTo(identity, 8);

        byte[] streamSuffix = new byte[26];
        BinaryPrimitives.WriteUInt16LittleEndian(streamSuffix, namespaceId);
        local.WriteTo(streamSuffix.AsSpan(2));
        BinaryPrimitives.WriteUInt64BigEndian(streamSuffix.AsSpan(18), 1);
        byte[] compactPayload = new byte[22];
        Batch(91).WriteTo(compactPayload);
        BinaryPrimitives.WriteInt32LittleEndian(compactPayload.AsSpan(16), 2);
        compactPayload[20] = 0xAA;
        compactPayload[21] = 0xBB;

        byte[] Key(byte kind, ReadOnlySpan<byte> suffix)
        {
            byte[] key = new byte[root.Length + 1 + suffix.Length];
            root.CopyTo(key, 0);
            key[root.Length] = kind;
            suffix.CopyTo(key.AsSpan(root.Length + 1));
            return key;
        }

        try
        {
            using var storage = StorageDatabase.Open(directory, 421);
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(Key(0, ReadOnlySpan<byte>.Empty), identity);
                batch.Put(Key(21, streamSuffix), compactPayload);
                if (includeLedger)
                {
                    byte[] ledgerSuffix = new byte[18];
                    BinaryPrimitives.WriteUInt16LittleEndian(ledgerSuffix, namespaceId);
                    Batch(91).WriteTo(ledgerSuffix.AsSpan(2));
                    byte[] ledgerValue = new byte[45];
                    Enumerable.Repeat((byte)9, 16).ToArray().CopyTo(ledgerValue, 0);
                    ledgerAuthor.WriteTo(ledgerValue.AsSpan(16));
                    BinaryPrimitives.WriteUInt32LittleEndian(ledgerValue.AsSpan(32), 7);
                    BinaryPrimitives.WriteUInt64LittleEndian(ledgerValue.AsSpan(36), 1);
                    ledgerValue[44] = 1;
                    batch.Put(Key(3, ledgerSuffix), ledgerValue);
                }
                batch.Commit();
            }

            using var metadata = new StorageMetadataStore(storage);
            Assert.Throws<InvalidDataException>(() =>
                metadata.GetOutboundHighWatermark(scope, local, namespaceId, local));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Legacy_stream_payload_row_kinds_are_rejected_without_migration()
    {
        string directory = Path.Combine(_root, "legacy-stream-payload");
        Directory.CreateDirectory(directory);
        const string scope = "legacy-stream-payload-scope";
        NodeId local = Node(1), author = Node(2);
        const int namespaceId = 7;
        byte[] scopeBytes = Encoding.UTF8.GetBytes(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        SHA256.HashData(scopeBytes).CopyTo(root, 1);
        local.WriteTo(root.AsSpan(33));
        byte[] identity = new byte[8 + scopeBytes.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(identity, 0x33534D42);
        BinaryPrimitives.WriteInt32LittleEndian(identity.AsSpan(4), scopeBytes.Length);
        scopeBytes.CopyTo(identity, 8);
        byte[] ledgerKeySuffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(ledgerKeySuffix, namespaceId);
        Batch(90).WriteTo(ledgerKeySuffix.AsSpan(2));
        byte[] ledgerValue = new byte[45];
        Enumerable.Repeat((byte)9, 16).ToArray().CopyTo(ledgerValue, 0);
        author.WriteTo(ledgerValue.AsSpan(16));
        BinaryPrimitives.WriteUInt32LittleEndian(ledgerValue.AsSpan(32), 2);
        BinaryPrimitives.WriteUInt64LittleEndian(ledgerValue.AsSpan(36), 1);
        ledgerValue[44] = 1;
        byte[] payloadSuffix = new byte[26];
        BinaryPrimitives.WriteUInt16LittleEndian(payloadSuffix, namespaceId);
        author.WriteTo(payloadSuffix.AsSpan(2));
        BinaryPrimitives.WriteUInt64BigEndian(payloadSuffix.AsSpan(18), 1);
        byte[] legacyPayload = new byte[34];
        Batch(90).WriteTo(legacyPayload);
        BinaryPrimitives.WriteUInt32LittleEndian(legacyPayload.AsSpan(16), 2);
        BinaryPrimitives.WriteUInt64LittleEndian(legacyPayload.AsSpan(20), 1);
        BinaryPrimitives.WriteInt32LittleEndian(legacyPayload.AsSpan(28), 2);
        legacyPayload[32] = 0xAA;
        legacyPayload[33] = 0xBB;

        byte[] Suffix(byte kind, ReadOnlySpan<byte> suffix)
        {
            byte[] key = new byte[root.Length + 1 + suffix.Length];
            root.CopyTo(key, 0);
            key[root.Length] = kind;
            suffix.CopyTo(key.AsSpan(root.Length + 1));
            return key;
        }

        try
        {
            using var storage = StorageDatabase.Open(directory, 420);
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(Suffix(0, ReadOnlySpan<byte>.Empty), identity);
                batch.Put(Suffix(3, ledgerKeySuffix), ledgerValue);
                batch.Put(Suffix(4, payloadSuffix), legacyPayload);
                batch.Put(Suffix(7, payloadSuffix), legacyPayload);
                batch.Commit();
            }

            using var metadata = new StorageMetadataStore(storage);
            Assert.Throws<InvalidDataException>(() =>
                metadata.GetOutboundHighWatermark(scope, local, namespaceId, author));
            using var snapshot = storage.OpenSnapshot();
            Assert.Equal(4, snapshot.ScanPrefix(root, ReadOnlySpan<byte>.Empty, 100).Count);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Legacy_metadata_partition_and_rows_are_rejected_without_migration()
    {
        string directory = Path.Combine(_root, "legacy-ledger");
        Directory.CreateDirectory(directory);
        const string scope = "legacy-ledger-scope";
        NodeId local = Node(1);
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

        byte[] identity = new byte[8 + scopeBytes.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(identity, 0x33534D42); // BMS3
        BinaryPrimitives.WriteInt32LittleEndian(identity.AsSpan(4), scopeBytes.Length);
        scopeBytes.CopyTo(identity, 8);
        byte[] ledgerSuffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(ledgerSuffix, 7);
        Batch(71).WriteTo(ledgerSuffix.AsSpan(2));

        try
        {
            using var storage = StorageDatabase.Open(directory, 411);
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(Suffix(0, ReadOnlySpan<byte>.Empty), identity);
                // A 16-byte pre-v3 fingerprint row must not be migrated or accepted.
                batch.Put(Suffix(3, ledgerSuffix), new byte[16]);
                batch.Commit();
            }

            using var metadata = new StorageMetadataStore(storage);
            Assert.Throws<InvalidDataException>(() =>
                metadata.GetOutboundHighWatermark(scope, local, 7, local));

            using var snapshot = storage.OpenSnapshot();
            var rows = snapshot.ScanPrefix(root, ReadOnlySpan<byte>.Empty, 100);
            Assert.Equal(2, rows.Count);
            Assert.Contains(rows, row => row.Key.AsSpan().SequenceEqual(Suffix(3, ledgerSuffix)));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_live_reference_scan_covers_pending_apply_and_outbound_payloads_and_fails_closed()
    {
        string directory = Path.Combine(_root, "tombstone-live-references");
        NodeId local = Node(1), sender = Node(2), author = Node(3), peer = Node(4);
        const string scope = "tombstone-live-references";
        const int namespaceId = 7;

        ProviderMutation Mutation(byte record, byte change, ulong sequence) => new()
        {
            RecordId = RecordId.From(Enumerable.Repeat((byte)record, 16).ToArray()),
            FieldId = 0x0101,
            AuthorNodeId = author,
            ChangeId = Batch(change),
            OriginAuthorEpoch = 1,
            OriginSequence = sequence,
            HlcWall = sequence,
            Value = new[] { record }
        };

        try
        {
            using var store = new StorageMetadataStore(directory, 454, deleteOnDispose: false);
            ProviderMutation pendingMutation = Mutation(0x31, 0x41, 1);
            byte[] pendingPayload = ProviderMutationCodec.Encode(pendingMutation);
            var incoming = new MetadataIncomingBatch
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = 1,
                AuthorStreamNodeId = author,
                NamespaceId = namespaceId,
                BatchId = Batch(0x42),
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)0x43, 16).ToArray(),
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = author,
                        ChangeId = pendingMutation.ChangeId,
                        OriginAuthorEpoch = pendingMutation.OriginAuthorEpoch,
                        OriginSequence = pendingMutation.OriginSequence,
                        Fingerprint = Enumerable.Repeat((byte)0x44, 16).ToArray(),
                        Payload = pendingPayload,
                        RelayEligible = true
                    }
                }
            };
            store.BeginPendingApply(incoming, Guid.Parse("a4540000-0000-0000-0000-000000000001"));

            Assert.True(store.TryBuildLiveRecordReferenceSet(
                scope, local, namespaceId, out HashSet<RecordId> pendingReferences));
            Assert.Contains(pendingMutation.RecordId, pendingReferences);

            ProviderMutation outboundMutation = Mutation(0x32, 0x45, 2);
            store.FreezeOutbound(scope, local, 8, author, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = outboundMutation.ChangeId,
                    Fingerprint = Enumerable.Repeat((byte)0x46, 16).ToArray(),
                    Payload = ProviderMutationCodec.Encode(outboundMutation),
                    OriginAuthorEpoch = 1
                }
            });
            Assert.True(store.TryBuildLiveRecordReferenceSet(
                scope, local, 8, out HashSet<RecordId> outboundReferences));
            Assert.Contains(outboundMutation.RecordId, outboundReferences);
            Assert.DoesNotContain(outboundMutation.RecordId, pendingReferences);

            CompleteBaseline(store, scope, local, peer, 1, namespaceId, author);
            store.ReserveOutboundBatch(scope, local, peer, 1, namespaceId, author,
                new MetadataOutboundBatchReservation
                {
                    BatchId = Batch(0x47),
                    IssuerEpoch = 1,
                    CursorStart = 1,
                    CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)0x48, 16).ToArray(),
                    Payload = pendingPayload
                });
            Assert.False(store.TryBuildLiveRecordReferenceSet(
                scope, local, namespaceId, out _));

            store.FreezeOutbound(scope, local, 9, author, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = Batch(0x49),
                    Fingerprint = Enumerable.Repeat((byte)0x4A, 16).ToArray(),
                    Payload = new byte[] { 0x01, 0x02 },
                    OriginAuthorEpoch = 1
                }
            });
            Assert.False(store.TryBuildLiveRecordReferenceSet(
                scope, local, 9, out _));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_live_reference_scan_recovers_pending_apply_and_outbound_payloads_after_restart()
    {
        string directory = Path.Combine(_root, "tombstone-live-references-restart");
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        const string scope = "tombstone-live-references-restart";
        const int pendingNamespace = 7;
        const int outboundNamespace = 8;

        ProviderMutation Mutation(byte record, byte change, ulong sequence) => new()
        {
            RecordId = RecordId.From(Enumerable.Repeat((byte)record, 16).ToArray()),
            FieldId = 0x0101,
            AuthorNodeId = author,
            ChangeId = Batch(change),
            OriginAuthorEpoch = 1,
            OriginSequence = sequence,
            HlcWall = sequence,
            Value = new[] { record }
        };

        try
        {
            ProviderMutation pendingMutation = Mutation(0x51, 0x61, 1);
            ProviderMutation outboundMutation = Mutation(0x52, 0x62, 2);
            ProviderMutation reservedMutation = Mutation(0x53, 0x69, 3);
            using (var store = new StorageMetadataStore(directory, 455, deleteOnDispose: false))
            {
                byte[] pendingPayload = ProviderMutationCodec.Encode(pendingMutation);
                var incoming = new MetadataIncomingBatch
                {
                    ScopeId = scope,
                    LocalReplica = local,
                    SenderPeer = sender,
                    SenderEpoch = 1,
                    AuthorStreamNodeId = author,
                    NamespaceId = pendingNamespace,
                    BatchId = Batch(0x63),
                    CursorStart = 1,
                    CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)0x64, 16).ToArray(),
                    Operations = new[]
                    {
                        new MetadataOperation
                        {
                            AuthorNodeId = author,
                            ChangeId = pendingMutation.ChangeId,
                            OriginAuthorEpoch = pendingMutation.OriginAuthorEpoch,
                            OriginSequence = pendingMutation.OriginSequence,
                            Fingerprint = Enumerable.Repeat((byte)0x65, 16).ToArray(),
                            Payload = pendingPayload,
                            RelayEligible = true
                        }
                    }
                };
                store.BeginPendingApply(incoming, Guid.Parse("a4550000-0000-0000-0000-000000000001"));
                store.FreezeOutbound(scope, local, outboundNamespace, author, new[]
                {
                    new MetadataFrozenOperation
                    {
                        ChangeId = outboundMutation.ChangeId,
                        Fingerprint = Enumerable.Repeat((byte)0x66, 16).ToArray(),
                        Payload = ProviderMutationCodec.Encode(outboundMutation),
                        OriginAuthorEpoch = 1
                    }
                });

                CompleteBaseline(store, scope, local, sender, 1, 9, author);
                store.ReserveOutboundBatch(scope, local, sender, 1, 9, author,
                    new MetadataOutboundBatchReservation
                    {
                        BatchId = Batch(0x67),
                        IssuerEpoch = 1,
                        CursorStart = 1,
                        CursorEnd = 1,
                        PreviousBatchDigest = new byte[16],
                        BatchDigest = Enumerable.Repeat((byte)0x68, 16).ToArray(),
                        Payload = ProviderMutationCodec.Encode(reservedMutation)
                    });
            }

            using var recovered = new StorageMetadataStore(directory, 455, deleteOnDispose: true);
            Assert.True(recovered.TryBuildLiveRecordReferenceSet(
                scope, local, pendingNamespace, out HashSet<RecordId> pendingReferences));
            Assert.Contains(pendingMutation.RecordId, pendingReferences);
            Assert.True(recovered.TryBuildLiveRecordReferenceSet(
                scope, local, outboundNamespace, out HashSet<RecordId> outboundReferences));
            Assert.Contains(outboundMutation.RecordId, outboundReferences);
            Assert.DoesNotContain(outboundMutation.RecordId, pendingReferences);
            Assert.DoesNotContain(pendingMutation.RecordId, outboundReferences);
            var peerState = recovered.GetPeerStreamState(scope, local, sender, 1, 9, author);
            var reservation = Assert.IsType<MetadataOutboundBatchReservation>(peerState.PendingBatch);
            Assert.Equal(Batch(0x67), reservation.BatchId);
            Assert.Equal(1UL, reservation.CursorStart);
            Assert.Equal(1UL, reservation.CursorEnd);
            Assert.Equal(ProviderMutationCodec.Encode(reservedMutation), reservation.Payload);
            Assert.False(recovered.TryBuildLiveRecordReferenceSet(
                scope, local, 9, out _)); // Recovered immutable reservation blocks the entire GC proof.
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
                reopened.AcknowledgePeerBatch(scope, local, peer, 7, 9, author,
                    pending.BatchId, pending.CursorEnd, pending.BatchDigest.ToArray());

                var state = reopened.GetPeerStreamState(scope, local, peer, 7, 9, author);
                Assert.Equal(3UL, state.AckedCursor);
                Assert.Equal(3UL, state.PushedCursor);
                Assert.Equal(3u, state.IssuerEpoch);
                Assert.Equal(pending.BatchDigest, state.LastBatchDigest);
                Assert.Null(state.PendingBatch);
                Assert.Throws<InvalidOperationException>(() =>
                    reopened.AcknowledgePeerBatch(scope, local, peer, 7, 9, author,
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
                    AuthorNodeId = authorA,
                    ChangeId = Batch(142),
                    OriginAuthorEpoch = 5,
                    OriginSequence = 142,
                    Fingerprint = Enumerable.Repeat((byte)143, 16).ToArray(),
                    Payload = new byte[] { 1, 4, 2 }, RelayEligible = true
                }
            }
        };
        var secondView = new MetadataIncomingBatch
        {
            ScopeId = scope, LocalReplica = local, SenderPeer = sender, SenderEpoch = 5,
            AuthorStreamNodeId = authorA, NamespaceId = 9, BatchId = first.BatchId,
            CursorStart = first.CursorStart, CursorEnd = first.CursorEnd,
            PreviousBatchDigest = first.PreviousBatchDigest.ToArray(),
            BatchDigest = first.BatchDigest.ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = authorA,
                    ChangeId = Batch(142),
                    OriginAuthorEpoch = 5,
                    OriginSequence = 142,
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
                    OriginAuthorEpoch = 1,
                    OriginSequence = 122,
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
                    AuthorNodeId = author,
                    ChangeId = Batch(133),
                    OriginAuthorEpoch = 1,
                    OriginSequence = 133,
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
                    AuthorNodeId = author,
                    ChangeId = Batch(136),
                    OriginAuthorEpoch = 1,
                    OriginSequence = 136,
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
                    AuthorNodeId = author,
                    ChangeId = Batch(112),
                    OriginAuthorEpoch = 2,
                    OriginSequence = 112,
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
                    OriginAuthorEpoch = 1,
                    OriginSequence = (ulong)(batchSeed + 20),
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
                    OriginAuthorEpoch = 1,
                    OriginSequence = 153,
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
    public void Global_ChangeId_equivocation_from_another_peer_is_rejected_after_reopen()
    {
        string directory = Path.Combine(_root, "global-changeid-equivocation-reopen");
        const ulong partitionId = 460;
        const string scope = "global-changeid-equivocation-reopen";
        NodeId local = Node(1), senderA = Node(2), senderB = Node(4), author = Node(3);
        BatchId sharedChangeId = Batch(153);
        Guid applyToken = Guid.Parse("00000000-0000-0000-0000-000000000460");

        var first = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = senderA,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(160),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)161, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)162, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = sharedChangeId,
                    OriginAuthorEpoch = 1,
                    OriginSequence = 153,
                    Fingerprint = Enumerable.Repeat((byte)163, 16).ToArray(),
                    Payload = new byte[] { 1, 2, 3 },
                    RelayEligible = true
                }
            }
        };
        var equivocation = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = senderB,
            SenderEpoch = 1,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(164),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)165, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)166, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = sharedChangeId,
                    OriginAuthorEpoch = 1,
                    OriginSequence = 154,
                    Fingerprint = Enumerable.Repeat((byte)167, 16).ToArray(),
                    Payload = new byte[] { 9, 8, 7 },
                    RelayEligible = true
                }
            }
        };

        try
        {
            using (var store = new StorageMetadataStore(directory, partitionId, deleteOnDispose: false))
            {
                store.BeginPendingApply(first, applyToken);
                var finalized = store.FinalizePendingApply(scope, local,
                    new MetadataProviderApplyReceipt(applyToken, first.BatchDigest));
                Assert.False(finalized.IsDuplicate);
                Assert.Equal(1, finalized.NewRelayOperationCount);
            }

            using var reopened = new StorageMetadataStore(directory, partitionId, deleteOnDispose: false);
            Assert.Empty(reopened.ReadPendingApplyIntents(scope, local));
            var relay = reopened.ReadRelay(scope, local, 7, author, 0, 10);
            Assert.Single(relay.Items);
            Assert.Equal(sharedChangeId, relay.Items[0].ChangeId);
            Assert.Equal(1UL, reopened.GetRelayHighWatermark(scope, local, 7, author));

            var duplicate = reopened.FinalizePendingApply(scope, local,
                new MetadataProviderApplyReceipt(applyToken, first.BatchDigest));
            Assert.True(duplicate.IsDuplicate);

            Assert.Throws<InvalidDataException>(() =>
                reopened.BeginPendingApply(equivocation,
                    Guid.Parse("00000000-0000-0000-0000-000000000461")));
            Assert.Empty(reopened.ReadPendingApplyIntents(scope, local));
            Assert.Single(reopened.ReadRelay(scope, local, 7, author, 0, 10).Items);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
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
                    OriginAuthorEpoch = 1,
                    OriginSequence = 153,
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
                    OriginAuthorEpoch = 1,
                    OriginSequence = 102,
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
                    OriginAuthorEpoch = 3,
                    OriginSequence = 87,
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
    public void Older_pending_apply_encodings_are_rejected_by_v3()
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
            HeaderFingerprint = Enumerable.Repeat((byte)96, 16).ToArray(),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = Array.Empty<MetadataOperation>()
        };
        var encode = typeof(StorageMetadataStore).GetMethod(
            "EncodeIncomingBatch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var decode = typeof(StorageMetadataStore).GetMethod(
            "DecodeIncomingBatch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        byte[] version7 = (byte[])encode.Invoke(null, new object[] { batch })!;
        var decoded = (MetadataIncomingBatch)decode.Invoke(null, new object[] { version7 })!;
        Assert.Equal(17U, decoded.TransportPeerEpoch);
        Assert.True(decoded.ReceiveStateOwnedByMetadataStore);

        foreach (int oldVersion in new[] { -6, -5, -4, -3 })
        {
            byte[] legacy = version7.ToArray();
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(legacy, oldVersion);
            var error = Assert.Throws<System.Reflection.TargetInvocationException>(
                () => decode.Invoke(null, new object[] { legacy }));
            Assert.IsType<InvalidDataException>(error.InnerException);
        }
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
    public void Peer_epoch_retirement_is_atomic_when_crash_occurs_after_wal_flush_before_memory_publish()
    {
        string directory = Path.Combine(_root, "peer-retirement-commit-crash");
        const ulong partitionId = 458;
        const int namespaceId = 7;
        const string scope = "peer-retirement-commit-crash";
        NodeId local = Node(1), peer = Node(2), author = Node(3);
        ulong beforeRetirementCommit;

        try
        {
            using (var storage = StorageDatabase.Open(directory, partitionId))
            using (var store = new StorageMetadataStore(storage))
            {
                store.FreezeOutbound(scope, local, namespaceId, author,
                    new[] { FrozenOperation(210, 211) });
                store.RegisterInterestedPeer(scope, local, peer, 1, namespaceId);
                store.CompletePeerInitialSync(scope, local, peer, 1, namespaceId,
                    new[] { new MetadataPeerStreamBaseline(author, 1, 1) });
                store.RegisterInterestedPeer(scope, local, peer, 2, namespaceId);
                store.CompletePeerInitialSync(scope, local, peer, 2, namespaceId,
                    new[] { new MetadataPeerStreamBaseline(author, 1, 1) });

                Assert.Equal(2, store.ReadInterestedPeers(scope, local, namespaceId).Count);
                Assert.Single(store.ReadPeerStreamStates(scope, local, peer, 1, namespaceId));
                Assert.Single(store.ReadPeerStreamStates(scope, local, peer, 2, namespaceId));
                beforeRetirementCommit = storage.LastCommitSeq;
            }

            // The WAL record is flushed, then the injected exception interrupts the
            // in-memory publish callback. Reopen must replay the entire retirement batch.
            using (var faultingStorage = StorageDatabase.OpenForTesting(directory, partitionId, point =>
            {
                if (point == StorageFaultPoint.AfterWalFlushBeforePublish)
                    throw new IOException("Injected crash after peer-retirement WAL flush.");
            }))
            using (var faultingStore = new StorageMetadataStore(faultingStorage))
            {
                Assert.Throws<IOException>(() =>
                    faultingStore.RetireInterestedPeer(scope, local, peer, 1, 2, namespaceId));
            }

            using var recoveredStorage = StorageDatabase.Open(directory, partitionId);
            using var recovered = new StorageMetadataStore(recoveredStorage);
            Assert.Equal(beforeRetirementCommit + 1, recoveredStorage.LastCommitSeq);
            var activePeer = Assert.Single(recovered.ReadInterestedPeers(scope, local, namespaceId));
            Assert.Equal(2U, activePeer.PeerEpoch);
            Assert.Equal(1U, recovered.GetRetiredPeerEpoch(scope, local, peer, namespaceId));
            Assert.Empty(recovered.ReadPeerStreamStates(scope, local, peer, 1, namespaceId));
            var replacementStream = Assert.Single(
                recovered.ReadPeerStreamStates(scope, local, peer, 2, namespaceId));
            Assert.Equal(author, replacementStream.AuthorStream);
            Assert.Equal(1UL, replacementStream.State.StreamOffset);

            // Replaying the retirement after reopen is idempotent and creates no
            // second commit: the roster, old-row deletes and retired fence were atomic.
            ulong afterRecovery = recoveredStorage.LastCommitSeq;
            recovered.RetireInterestedPeer(scope, local, peer, 1, 2, namespaceId);
            Assert.Equal(afterRecovery, recoveredStorage.LastCommitSeq);
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
                Assert.Throws<InvalidOperationException>(() =>
                    store.RetireInterestedPeer(scope, local, peer, 1, 2, 7));
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
                store.AcknowledgePeerBatch(scope, local, peer, 2, 7, local,
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
    public void Retiring_transport_peer_epoch_fences_old_epoch_and_reuses_receipt_on_new_epoch_retry()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "receipt-retention-epoch"), 447);
        const string scope = "receipt-retention-epoch";
        NodeId local = Node(1), sender = Node(2), authorA = Node(3), authorB = Node(4);

        store.RegisterInterestedPeer(scope, local, sender, 1, 7);

        MetadataIncomingBatch MakeBatch(NodeId author, uint authorEpoch, byte batchId,
            byte changeId, byte transportEpoch, byte[]? previousDigest = null)
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
                PreviousBatchDigest = previousDigest?.ToArray() ?? new byte[16],
                BatchDigest = Enumerable.Repeat((byte)(batchId + 1), 16).ToArray(),
                HeaderFingerprint = Enumerable.Repeat((byte)(batchId + 2), 16).ToArray(),
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = author,
                        ChangeId = Batch(changeId),
                        OriginAuthorEpoch = authorEpoch,
                        OriginSequence = changeId,
                        Fingerprint = Enumerable.Repeat((byte)(changeId + 1), 16).ToArray(),
                        Payload = new byte[] { changeId }
                    }
                }
            };

        var epochBound = MakeBatch(authorA, 5, 200, 201, 1);
        var legacyUnbound = MakeBatch(authorB, 5, 202, 203, 0, epochBound.BatchDigest);
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

        // The old transport epoch is fenced, but its durable receipt remains
        // available for an idempotent retry over the new transport incarnation.
        var replayFromNewEpoch = MakeBatch(authorA, 5, 200, 201, 2);
        Assert.True(store.AcceptIncomingBatch(replayFromNewEpoch).IsDuplicate);

        // Legacy rows without a recorded transport epoch cannot be attributed to the
        // retired incarnation, so they remain retained rather than being guessed away.
        var legacyReplay = MakeBatch(authorB, 5, 202, 203, 2, epochBound.BatchDigest);
        Assert.True(store.AcceptIncomingBatch(legacyReplay).IsDuplicate);
    }

    [Fact]
    public void Retired_epoch_fence_survives_restart_and_reuses_receipt_on_new_epoch_retry()
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
                    OriginAuthorEpoch = 5,
                    OriginSequence = 243,
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
            Assert.True(reopened.AcceptIncomingBatch(retryFromNewEpoch).IsDuplicate);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Duplicate_on_new_transport_epoch_refreshes_receipt_retention()
    {
        string directory = Path.Combine(_root, "receipt-refresh-epoch");
        const string scope = "receipt-refresh-epoch";
        NodeId local = Node(1), sender = Node(2), author = Node(3);

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
                    OriginAuthorEpoch = 5,
                    OriginSequence = 223,
                    Fingerprint = Enumerable.Repeat((byte)224, 16).ToArray(),
                    Payload = new byte[] { 225 }
                }
            }
        };
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
        var unregisteredEpoch = new MetadataIncomingBatch
        {
            ScopeId = original.ScopeId,
            LocalReplica = original.LocalReplica,
            SenderPeer = original.SenderPeer,
            SenderEpoch = original.SenderEpoch,
            TransportPeerEpoch = 3,
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

        using (var store = new StorageMetadataStore(directory, 449))
        {
            store.RegisterInterestedPeer(scope, local, sender, 1, 7);
            Assert.False(store.AcceptIncomingBatch(original).IsDuplicate);
            store.RegisterInterestedPeer(scope, local, sender, 2, 7);
            store.CompletePeerInitialSync(scope, local, sender, 2, 7,
                new[] { new MetadataPeerStreamBaseline(author, 0, 5) });
            Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(unregisteredEpoch));
            Assert.True(store.AcceptIncomingBatch(retryOnNewEpoch).IsDuplicate);
        }

        using var reopened = new StorageMetadataStore(directory, 449);
        reopened.RetireInterestedPeer(scope, local, sender, 1, 2, 7);

        // The re-attribution survives restart, so retiring epoch 1 must not discard
        // the ACK evidence still required by the replacement transport epoch.
        Assert.True(reopened.AcceptIncomingBatch(retryOnNewEpoch).IsDuplicate);
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
                    OriginAuthorEpoch = 4,
                    OriginSequence = 213,
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
    public void Retiring_transport_epoch_waits_for_legacy_epoch_unknown_pending_apply()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "legacy-pending-retirement-fence"), 452);
        const string scope = "legacy-pending-retirement-fence";
        NodeId local = Node(1), sender = Node(2), author = Node(3);
        store.RegisterInterestedPeer(scope, local, sender, 1, 7);

        var pending = new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 4,
            TransportPeerEpoch = 0,
            AuthorStreamNodeId = author,
            NamespaceId = 7,
            BatchId = Batch(160),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)161, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)162, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = author,
                    ChangeId = Batch(163),
                    OriginAuthorEpoch = 4,
                    OriginSequence = 163,
                    Fingerprint = Enumerable.Repeat((byte)164, 16).ToArray(),
                    Payload = new byte[] { 165 }
                }
            }
        };
        store.BeginPendingApply(pending, Guid.Parse("00000000-0000-0000-0000-000000000452"));

        store.RegisterInterestedPeer(scope, local, sender, 2, 7);
        store.CompletePeerInitialSync(scope, local, sender, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 4) });

        Assert.Throws<InvalidOperationException>(() =>
            store.RetireInterestedPeer(scope, local, sender, 1, 2, 7));
        Assert.Single(store.ReadPendingApplyIntents(scope, local));
        Assert.Null(store.GetRetiredPeerEpoch(scope, local, sender, 7));
    }

    [Fact]
    public void Retiring_peer_epoch_preserves_receipts_and_fences_only_the_target_namespace()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "receipt-gc-namespace-scope"), 453);
        const string scope = "receipt-gc-namespace-scope";
        NodeId local = Node(1), sender = Node(2), authorA = Node(3), authorB = Node(4);
        store.RegisterInterestedPeer(scope, local, sender, 1, 7);
        store.RegisterInterestedPeer(scope, local, sender, 1, 8);

        MetadataIncomingBatch Make(int namespaceId, NodeId author, uint authorEpoch, byte batchId, uint transportPeerEpoch = 1)
            => new()
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = authorEpoch,
                TransportPeerEpoch = transportPeerEpoch,
                AuthorStreamNodeId = author,
                NamespaceId = namespaceId,
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
                        ChangeId = Batch((byte)(batchId + 10)),
                        OriginAuthorEpoch = authorEpoch,
                        OriginSequence = (ulong)(batchId + 10),
                        Fingerprint = Enumerable.Repeat((byte)(batchId + 11), 16).ToArray(),
                        Payload = new byte[] { batchId }
                    }
                }
            };

        var namespace7 = Make(7, authorA, 5, 170);
        var namespace8 = Make(8, authorB, 6, 171);
        Assert.False(store.AcceptIncomingBatch(namespace7).IsDuplicate);
        Assert.False(store.AcceptIncomingBatch(namespace8).IsDuplicate);

        store.RegisterInterestedPeer(scope, local, sender, 2, 7);
        store.CompletePeerInitialSync(scope, local, sender, 2, 7,
            new[] { new MetadataPeerStreamBaseline(authorA, 0, 5) });
        store.RetireInterestedPeer(scope, local, sender, 1, 2, 7);

        // Namespace 7's old transport incarnation is fenced, but retirement does
        // not eagerly delete its durable receipt. The same immutable batch is reusable
        // over the newly accepted transport epoch.
        var staleNamespace7 = Make(7, authorA, 5, 170);
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(staleNamespace7));
        var replayNamespace7 = Make(7, authorA, 5, 170, transportPeerEpoch: 2);
        Assert.True(store.AcceptIncomingBatch(replayNamespace7).IsDuplicate);

        // Namespace 8 has an independent fence and retains the same peer epoch's receipt.
        Assert.True(store.AcceptIncomingBatch(namespace8).IsDuplicate);
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
                    AuthorNodeId = author,
                    ChangeId = Batch(132),
                    OriginAuthorEpoch = 1,
                    OriginSequence = 132,
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
        store.AcknowledgePeerBatch(scope, local, peerA, 1, 7, author,
            Batch(134), 1, Enumerable.Repeat((byte)135, 16).ToArray());

        Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
        Assert.Throws<ArgumentOutOfRangeException>(() => store.CompactRelayThrough(scope, local, 7, author, 1));
        Assert.Single(store.ReadRelayForPeer(scope, local, peerB, 1, 7, author, 10).Items);

        store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(136, 137));
        store.MarkOutboundBatchPushed(scope, local, peerB, 1, 7, author, Batch(136));
        store.AcknowledgePeerBatch(scope, local, peerB, 1, 7, author,
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
                store.AcknowledgePeerBatch(scope, local, peerA, 1, 7, author,
                    Batch(92), 1, Enumerable.Repeat((byte)92, 16).ToArray());

                Assert.Equal(0UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                    store.CompactOutboundThrough(scope, local, 7, author, 1));
                Assert.Equal(1UL, store.GetOutboundHighWatermark(scope, local, 7, author));

                store.ReserveOutboundBatch(scope, local, peerB, 1, 7, author, Make(93));
                store.MarkOutboundBatchPushed(scope, local, peerB, 1, 7, author, Batch(93));
                store.AcknowledgePeerBatch(scope, local, peerB, 1, 7, author,
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
        store.AcknowledgePeerBatch(scope, local, peer, 1, 7, local,
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
        store.AcknowledgePeerBatch(scope, local, peerA, 1, 7, author,
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
            store.AcknowledgePeerBatch(scope, local, peer, 4, 7, author,
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
            reopened.AcknowledgePeerBatch("peer-reservation-epoch", local, peer, 4, 7, author,
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
        Assert.Throws<InvalidDataException>(() => store.AcknowledgePeerBatch(
            scope, local, peer, 1, 7, author, Batch(50), 2, reservation.BatchDigest));
        Assert.Throws<InvalidDataException>(() => store.AcknowledgePeerBatch(
            scope, local, peer, 1, 7, author, Batch(50), 2, Enumerable.Repeat((byte)99, 16).ToArray()));

        store.MarkOutboundBatchPushed(scope, local, peer, 1, 7, author, Batch(50));
        store.AcknowledgePeerBatch(scope, local, peer, 1, 7, author,
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
        var pending = CreateBatch("durable-scope", 1, 2, 1, 7, 11, 1, 1, 10, 11,
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
                Assert.Equal(1UL, finalized.CursorEnd);
                Assert.Empty(reopened.ReadPendingApplyIntents("durable-scope", Node(1)));
                Assert.Equal(1UL, reopened.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));
                Assert.Equal(1UL, reopened.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(21)));

                reopened.AcknowledgeRelay("durable-scope", Node(1), 7, Node(20), 1);
                reopened.CompactRelayThrough("durable-scope", Node(1), 7, Node(20), 1);
                reopened.AcknowledgeOutbound("durable-scope", Node(1), 7, Node(1), 2, 1);
                reopened.CompactOutboundThrough("durable-scope", Node(1), 7, Node(1), 1);
                Assert.Empty(reopened.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Empty(reopened.ReadOutbound("durable-scope", Node(1), 7, Node(1), 2, 0, 10).Items);
            }

            using (var afterCompaction = new StorageMetadataStoreConformanceAdapter(directory, 302, deleteOnDispose: false))
            {
                Assert.Equal(1UL, afterCompaction.GetReceiveCursor("durable-scope", Node(1), Node(2), 1, 7));
                Assert.Equal(1UL, afterCompaction.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(21)));
                Assert.Equal(1UL, afterCompaction.GetRelayHighWatermark("durable-scope", Node(1), 7, Node(20)));
                Assert.Equal(1UL, afterCompaction.GetOutboundHighWatermark("durable-scope", Node(1), 7, Node(1)));
                Assert.Empty(afterCompaction.ReadRelay("durable-scope", Node(1), 7, Node(20), 0, 10).Items);
                Assert.Empty(afterCompaction.ReadOutbound("durable-scope", Node(1), 7, Node(1), 2, 0, 10).Items);
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


    [Fact]
    public void Stale_epoch_can_finish_only_its_exact_durable_reservation_before_retirement()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "stale-peer-exact-ack"), 451);
        NodeId local = Node(1), peer = Node(2), author = Node(1);
        const string scope = "stale-peer-exact-ack";
        store.FreezeOutbound(scope, local, 7, author, new[] { FrozenOperation(201, 202) });
        store.RegisterInterestedPeer(scope, local, peer, 1, 7);
        store.CompletePeerInitialSync(scope, local, peer, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });

        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(203), IssuerEpoch = 1, StreamOffset = 0,
            CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)204, 16).ToArray(),
            Payload = new byte[] { 205, 206 }
        };
        store.ReserveOutboundBatch(scope, local, peer, 1, 7, author, reservation);
        store.MarkOutboundBatchPushed(scope, local, peer, 1, 7, author, reservation.BatchId);

        store.RegisterInterestedPeer(scope, local, peer, 2, 7);
        store.CompletePeerInitialSync(scope, local, peer, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        Assert.Throws<InvalidOperationException>(() =>
            store.RetireInterestedPeer(scope, local, peer, 1, 2, 7));

        var retry = store.ReserveOutboundBatch(scope, local, peer, 1, 7, author,
            new MetadataOutboundBatchReservation
            {
                BatchId = Batch(207), IssuerEpoch = 1, StreamOffset = 0,
                CursorStart = 1, CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)208, 16).ToArray(),
                Payload = new byte[] { 208 }
            });
        Assert.Equal(reservation.BatchId, retry.BatchId);
        Assert.Equal(reservation.Payload, retry.Payload);
        store.MarkOutboundBatchPushed(scope, local, peer, 1, 7, author, reservation.BatchId);
        store.AcknowledgePeerBatch(scope, local, peer, 1, 7, author,
            reservation.BatchId, reservation.CursorEnd, reservation.BatchDigest);

        Assert.Throws<InvalidDataException>(() => store.ReserveOutboundBatch(scope, local, peer, 1, 7, author,
            new MetadataOutboundBatchReservation
            {
                BatchId = Batch(209), IssuerEpoch = 1, StreamOffset = 0,
                CursorStart = 2, CursorEnd = 2,
                PreviousBatchDigest = reservation.BatchDigest,
                BatchDigest = Enumerable.Repeat((byte)209, 16).ToArray(),
                Payload = new byte[] { 209 }
            }));
        store.RetireInterestedPeer(scope, local, peer, 1, 2, 7);
        Assert.Empty(store.ReadPeerStreamStates(scope, local, peer, 1, 7));
    }

    [Fact]
    public void Replacement_snapshot_baseline_can_supersede_old_pending_outbound_reservation()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "replacement-baseline-supersedes-pending"), 452);
        NodeId local = Node(1), peer = Node(2), author = Node(1);
        const string scope = "replacement-baseline-supersedes-pending";
        store.FreezeOutbound(scope, local, 7, author, new[] { FrozenOperation(211, 212) });
        store.RegisterInterestedPeer(scope, local, peer, 1, 7);
        store.CompletePeerInitialSync(scope, local, peer, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(213), IssuerEpoch = 1, StreamOffset = 0,
            CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)214, 16).ToArray(),
            Payload = new byte[] { 215 }
        };
        store.ReserveOutboundBatch(scope, local, peer, 1, 7, author, reservation);

        store.RegisterInterestedPeer(scope, local, peer, 2, 7);
        store.CompletePeerInitialSync(scope, local, peer, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 1, 1) });

        store.RetireInterestedPeer(scope, local, peer, 1, 2, 7);
        Assert.Empty(store.ReadPeerStreamStates(scope, local, peer, 1, 7));
        Assert.Equal(1UL, store.GetSafePeerStreamCompactionCursor(scope, local, 7, author));
    }


    [Fact]
    public void Replacement_baseline_from_different_author_epoch_cannot_discharge_old_reservation()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "replacement-baseline-author-epoch"), 453);
        NodeId local = Node(1), peer = Node(2), author = Node(1);
        const string scope = "replacement-baseline-author-epoch";
        store.FreezeOutbound(scope, local, 7, author, new[] { FrozenOperation(221, 222) });
        store.RegisterInterestedPeer(scope, local, peer, 1, 7);
        store.CompletePeerInitialSync(scope, local, peer, 1, 7,
            new[] { new MetadataPeerStreamBaseline(author, 0, 1) });
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = Batch(223), IssuerEpoch = 1, StreamOffset = 0,
            CursorStart = 1, CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)224, 16).ToArray(),
            Payload = new byte[] { 225 }
        };
        store.ReserveOutboundBatch(scope, local, peer, 1, 7, author, reservation);

        store.RegisterInterestedPeer(scope, local, peer, 2, 7);
        store.CompletePeerInitialSync(scope, local, peer, 2, 7,
            new[] { new MetadataPeerStreamBaseline(author, 1, 2) });

        Assert.Throws<InvalidOperationException>(() =>
            store.RetireInterestedPeer(scope, local, peer, 1, 2, 7));
        Assert.NotNull(store.GetPendingOutboundBatch(scope, local, peer, 1, 7, author));
    }


    [Fact]
    public void Outbound_and_relay_payloads_cannot_share_an_author_stream()
    {
        NodeId local = Node(1), upstream = Node(2), author = Node(3);
        const int namespaceId = 7;

        using (var outboundFirst = new StorageMetadataStore(
            Path.Combine(_root, "exclusive-author-outbound-first"), 451))
        {
            const string scope = "exclusive-author-outbound-first";
            outboundFirst.FreezeOutbound(scope, local, namespaceId, author,
                new[] { FrozenOperation(201, 202) });

            var relayAttempt = CreateBatch(scope, 1, 2, 1, namespaceId, 203,
                1, 1, 0, 204, Operation(3, 205, 206, 207));
            Assert.Throws<InvalidDataException>(() => outboundFirst.AcceptIncomingBatch(relayAttempt));
            Assert.Equal(0UL, outboundFirst.GetRelayHighWatermark(scope, local, namespaceId, author));

            // An exact echo of the already-frozen local ChangeId remains idempotent.
            var exactEcho = CreateBatch(scope, 1, 2, 1, namespaceId, 208,
                1, 1, 0, 209, Operation(3, 201, 202, 202, originSequence: 1));
            Assert.False(outboundFirst.AcceptIncomingBatch(exactEcho).IsDuplicate);
            Assert.Equal(0UL, outboundFirst.GetRelayHighWatermark(scope, local, namespaceId, author));
        }

        using (var relayFirst = new StorageMetadataStore(
            Path.Combine(_root, "exclusive-author-relay-first"), 452))
        {
            const string scope = "exclusive-author-relay-first";
            var incoming = CreateBatch(scope, 1, 2, 1, namespaceId, 210,
                1, 1, 0, 211, Operation(3, 212, 213, 214));
            Assert.False(relayFirst.AcceptIncomingBatch(incoming).IsDuplicate);
            Assert.Throws<InvalidDataException>(() =>
                relayFirst.FreezeOutbound(scope, local, namespaceId, author,
                    new[] { FrozenOperation(215, 216) }));
            Assert.Equal(1UL, relayFirst.GetRelayHighWatermark(scope, local, namespaceId, author));
        }
    }


    [Fact]
    public void Outbound_and_relay_author_exclusivity_survives_reopen()
    {
        const string scope = "exclusive-author-reopen";
        const int namespaceId = 7;
        NodeId local = Node(1), upstream = Node(2), author = Node(3);
        string directory = Path.Combine(_root, "exclusive-author-reopen");

        using (var first = new StorageMetadataStore(directory, 453, deleteOnDispose: false))
            first.FreezeOutbound(scope, local, namespaceId, author,
                new[] { FrozenOperation(221, 222) });

        using (var reopened = new StorageMetadataStore(directory, 453, deleteOnDispose: false))
        {
            var relayAttempt = CreateBatch(scope, 1, 2, 1, namespaceId, 223,
                1, 1, 0, 224, Operation(3, 225, 226, 227));
            Assert.Throws<InvalidDataException>(() => reopened.AcceptIncomingBatch(relayAttempt));
            Assert.Equal(1UL, reopened.GetOutboundHighWatermark(scope, local, namespaceId, author));
            Assert.Equal(0UL, reopened.GetRelayHighWatermark(scope, local, namespaceId, author));
        }

        Directory.Delete(directory, recursive: true);
        directory = Path.Combine(_root, "exclusive-relay-reopen");
        using (var first = new StorageMetadataStore(directory, 454, deleteOnDispose: false))
        {
            var relay = CreateBatch("exclusive-relay-reopen", 1, 2, 1, namespaceId, 228,
                1, 1, 0, 229, Operation(3, 230, 231, 232));
            Assert.False(first.AcceptIncomingBatch(relay).IsDuplicate);
        }

        using (var reopened = new StorageMetadataStore(directory, 454, deleteOnDispose: false))
        {
            Assert.Throws<InvalidDataException>(() =>
                reopened.FreezeOutbound("exclusive-relay-reopen", local, namespaceId, author,
                    new[] { FrozenOperation(233, 234) }));
            Assert.Equal(1UL, reopened.GetRelayHighWatermark(
                "exclusive-relay-reopen", local, namespaceId, author));
            Assert.Equal(0UL, reopened.GetOutboundHighWatermark(
                "exclusive-relay-reopen", local, namespaceId, author));
        }
    }

}
