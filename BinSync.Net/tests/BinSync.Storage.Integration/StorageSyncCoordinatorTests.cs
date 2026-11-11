using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageSyncCoordinatorTests
{
    private static async Task AwaitBothPeerOperationsAsync(Task left, Task right)
    {
        Task first = await Task.WhenAny(left, right).WaitAsync(TimeSpan.FromSeconds(30));
        await first.ConfigureAwait(false);
        await Task.WhenAll(left, right).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
    }

    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-coordinator-" + Guid.NewGuid().ToString("N"));

    private sealed class RecordingAsyncRemoteBatchStore : IAsyncProviderRemoteBatchCommitStore
    {
        private readonly IDataProvider _provider;
        public ProviderRemoteBatchCommit? LastCommit { get; private set; }
        public List<Guid> ApplyTokens { get; } = new();
        public bool LoseNextResponseAfterCommit { get; set; }

        public RecordingAsyncRemoteBatchStore(IDataProvider provider) => _provider = provider;

        public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
            ProviderRemoteBatchCommit batch, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastCommit = batch;
            ApplyTokens.Add(batch.ApplyToken);
            ProviderApplyReceipt applied = _provider.ApplyRemoteBatch(batch.ApplyToken, batch.Mutations);
            if (LoseNextResponseAfterCommit)
            {
                LoseNextResponseAfterCommit = false;
                throw new IOException("simulated response loss after durable provider APPLY");
            }
            return ValueTask.FromResult(new ProviderApplyReceipt(
                batch.ApplyToken, batch.BatchDigest.ToArray(), applied.IsDuplicate));
        }
    }

    private sealed class RecordingAsyncLocalProvider :
        IAsyncDataProvider,
        IAsyncProviderOperationReceiptStore,
        IAsyncProviderPublishedOriginStore,
        IAsyncProviderPublicationMode
    {
        private readonly StorageDataProvider _provider;
        private readonly InProcessAsyncDataProviderAdapter _adapter;
        private readonly Dictionary<BatchId, ulong> _receipts = new();
        private ulong _nextClientSequence = 1;

        public ulong LastOutboxAck { get; private set; }
        public ulong LastReceiptAck { get; private set; }
        public bool LoseNextReceiptAckResponseAfterCommit { get; set; }
        public bool LoseNextAppendResponseAfterCommit { get; set; }
        public bool LoseNextOutboxAckResponseAfterCommit { get; set; }
        public bool LoseNextPublishedOriginResponseAfterCommit { get; set; }
        public Action? AfterNextRead { get; set; }
        public Guid ReceiptStreamId { get; }

        public RecordingAsyncLocalProvider(StorageDataProvider provider, Guid? receiptStreamId = null)
        {
            _provider = provider;
            _adapter = new InProcessAsyncDataProviderAdapter(provider);
            ReceiptStreamId = receiptStreamId ?? Guid.NewGuid();
        }

        public ValueTask<RecordId> ResolveRecordIdAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
            => _adapter.ResolveRecordIdAsync(key, ct);
        public ValueTask<ushort> ResolveFieldIdAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
            => _adapter.ResolveFieldIdAsync(key, ct);
        public ValueTask AppendLocalChangeAsync(ProviderMutation mutation, CancellationToken ct = default)
            => _adapter.AppendLocalChangeAsync(mutation, ct);
        public ValueTask<ProviderChangePage> ReadLocalChangesAsync(ulong after, int limit, CancellationToken ct = default)
        {
            ValueTask<ProviderChangePage> pending = _adapter.ReadLocalChangesAsync(after, limit, ct);
            Action? afterRead = AfterNextRead;
            AfterNextRead = null;
            afterRead?.Invoke();
            return pending;
        }
        public ValueTask AcknowledgeLocalChangesAsync(ulong through, CancellationToken ct = default)
        {
            LastOutboxAck = Math.Max(LastOutboxAck, through);
            ValueTask ack = _adapter.AcknowledgeLocalChangesAsync(through, ct);
            if (LoseNextOutboxAckResponseAfterCommit)
            {
                LoseNextOutboxAckResponseAfterCommit = false;
                return LoseOutboxAckResponseAfterCommitAsync(ack);
            }
            return ack;
        }
        public ValueTask<ulong> GetLocalChangeHighWatermarkAsync(CancellationToken ct = default)
            => _adapter.GetLocalChangeHighWatermarkAsync(ct);
        public ValueTask<ProviderSnapshotHandle> OpenSnapshotAsync(CancellationToken ct = default)
            => _adapter.OpenSnapshotAsync(ct);
        public ValueTask<ProviderSnapshotPage> ReadSnapshotAsync(Guid token, int after, int limit, CancellationToken ct = default)
            => _adapter.ReadSnapshotAsync(token, after, limit, ct);
        public ValueTask ReleaseSnapshotAsync(Guid token, CancellationToken ct = default)
            => _adapter.ReleaseSnapshotAsync(token, ct);
        public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(Guid token, IReadOnlyList<ProviderMutation> mutations, CancellationToken ct = default)
            => _adapter.ApplyRemoteBatchAsync(token, mutations, ct);
        public ValueTask<ProviderMutation?> ReadCurrentAsync(RecordId record, ushort field, CancellationToken ct = default)
            => _adapter.ReadCurrentAsync(record, field, ct);

        public ValueTask<ulong> AppendLocalChangeWithReceiptAsync(
            ProviderMutation mutation, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (_receipts.TryGetValue(mutation.ChangeId, out ulong existing))
                return ValueTask.FromResult(existing);
            ulong sequence = _nextClientSequence++;
            _receipts.Add(mutation.ChangeId, sequence);
            if (LoseNextAppendResponseAfterCommit)
            {
                LoseNextAppendResponseAfterCommit = false;
                throw new IOException("simulated response loss after durable auto-sequence append");
            }
            return ValueTask.FromResult(sequence);
        }

        public ValueTask AcknowledgeOperationReceiptsAsync(ulong throughSequence, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            if (throughSequence >= _nextClientSequence)
                throw new InvalidDataException("Receipt ACK exceeds the assigned ClientSequence high watermark.");
            LastReceiptAck = Math.Max(LastReceiptAck, throughSequence);
            if (LoseNextReceiptAckResponseAfterCommit)
            {
                LoseNextReceiptAckResponseAfterCommit = false;
                throw new IOException("simulated response loss after durable operation receipt ACK");
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask CommitPublishedOriginsAsync(
            IReadOnlyList<ProviderPublishedOrigin> published, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _provider.CommitPublishedOrigins(published);
            if (LoseNextPublishedOriginResponseAfterCommit)
            {
                LoseNextPublishedOriginResponseAfterCommit = false;
                throw new IOException("simulated response loss after durable published-origin stamping");
            }
            return ValueTask.CompletedTask;
        }

        private static async ValueTask LoseOutboxAckResponseAfterCommitAsync(ValueTask ack)
        {
            await ack.ConfigureAwait(false);
            throw new IOException("simulated response loss after durable source Outbox ACK");
        }

        public ValueTask<(bool Exists, ulong NodeOutboxHighWatermark)> TryGetMetadataStoreOwnedPublicationCutoverAsync(
            CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            bool exists = _provider.TryGetMetadataStoreOwnedPublicationCutover(out ulong watermark);
            return ValueTask.FromResult((exists, watermark));
        }

        public ValueTask EnableMetadataStoreOwnedPublicationAsync(
            ulong nodeOutboxHighWatermark, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _provider.EnableMetadataStoreOwnedPublication(nodeOutboxHighWatermark);
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => _adapter.DisposeAsync();
    }

    [Fact]
    public async Task Async_provider_receive_commits_node_provider_and_metadata_before_transport_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-remote-apply"), 489);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(88);
        using var receiver = NewNode(89);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new AsyncStorageSyncCoordinator(receiver, metadata, "async-remote-apply", 1);
        var asyncProvider = new RecordingAsyncRemoteBatchStore(provider);
        RecordId record = RecordId.From(Guid.Parse("88000000-0000-0000-0000-000000000088").ToByteArray());
        Assert.True(sender.WriteField(1, record, 0x0891, "async-ipc-apply"u8.ToArray(), new Hlc(8900, 0)));

        Task<BatchResult> receive = coordinator.ReceiveBatchWithAsyncProviderBeforeAckAsync(
            acceptor, asyncProvider, node => StorageNodePersistence.Save(storage, node));
        Assert.True(await initiator.PushBatchAsync(sender, 1));
        BatchResult result = await receive.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(1, result.Applied);
        Assert.NotNull(asyncProvider.LastCommit);
        Assert.Equal(sender.NodeId, asyncProvider.LastCommit!.SenderPeer);
        Assert.Equal(1u, asyncProvider.LastCommit.IssuerEpoch);
        ProviderMutation appliedMutation = Assert.Single(asyncProvider.LastCommit.Mutations);
        Assert.Equal(record, appliedMutation.RecordId);
        Assert.Equal((ushort)0x0891, appliedMutation.FieldId);
        Assert.NotEqual(Guid.Empty, asyncProvider.LastCommit.ApplyToken);
        Assert.Empty(metadata.ReadPendingApplyIntents("async-remote-apply", receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0891, out var nodeValue));
        Assert.Equal("async-ipc-apply", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0891, out var providerValue));
        Assert.Equal("async-ipc-apply", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task Async_local_publication_coalesces_feed_and_acks_outbox_before_receipts()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-local-publication"), 491);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(90);
        const string scope = "async-local-publication";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));

        string walPath = Path.Combine(_root, "async-local-publication.wal");
        using var wal = Wal.Open(walPath);
        node.AttachWal(wal);
        var asyncProvider = new RecordingAsyncLocalProvider(provider);
        RecordId record = RecordId.From(Guid.Parse("90000000-0000-0000-0000-000000000090").ToByteArray());
        BatchId firstChange = BatchId.From(Guid.Parse("90000000-0000-0000-0000-000000000091").ToByteArray());
        BatchId secondChange = BatchId.From(Guid.Parse("90000000-0000-0000-0000-000000000092").ToByteArray());

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0901, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9001, Value = "coalesced-old"u8.ToArray(), ChangeId = firstChange
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0901, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9002, Value = "coalesced-winner"u8.ToArray(), ChangeId = secondChange
        });

        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
        int published = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));

        Assert.Equal(2, published);
        Assert.Equal(2UL, asyncProvider.LastOutboxAck);
        Assert.Equal(2UL, asyncProvider.LastReceiptAck);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.True(metadata.TryGetOutboundChangeIdentity(
            scope, node.NodeId, 1, secondChange, out _, out OperationOrigin origin));
        Assert.Equal(node.NodeId, origin.AuthorNodeId);
        Assert.Equal(1U, origin.AuthorEpoch);
        Assert.Equal(1UL, origin.Sequence);
        Assert.True(node.TryGetField(1, record, 0x0901, out var value));
        Assert.Equal("coalesced-winner", Encoding.UTF8.GetString(value!.Value));
        Assert.False(node.HasPendingProviderOperationReceipts);
    }

    [Fact]
    public async Task Async_local_publication_recovers_receipt_ack_after_outbox_ack_and_response_loss()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-local-receipt-recovery"), 492);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        const string scope = "async-local-receipt-recovery";
        Node node = NewNode(91);
        string walPath = Path.Combine(_root, "async-local-receipt-recovery.wal");
        Wal wal = Wal.Open(walPath);
        try
        {
            var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
            legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));
            node.AttachWal(wal);
            var asyncProvider = new RecordingAsyncLocalProvider(provider)
            {
                LoseNextReceiptAckResponseAfterCommit = true
            };
            RecordId record = RecordId.From(Guid.Parse("91000000-0000-0000-0000-000000000091").ToByteArray());
            BatchId changeId = BatchId.From(Guid.Parse("91000000-0000-0000-0000-000000000092").ToByteArray());
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0902, AuthorNodeId = node.NodeId,
                OriginAuthorEpoch = 1, HlcWall = 9100, Value = "receipt-recovery"u8.ToArray(), ChangeId = changeId
            });

            var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
            await Assert.ThrowsAsync<IOException>(async () =>
                await coordinator.PublishLocalChangesAsync(
                    asyncProvider, current => StorageNodePersistence.Save(storage, current)));

            Assert.Equal(1UL, asyncProvider.LastOutboxAck);
            Assert.Equal(1UL, asyncProvider.LastReceiptAck);
            Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
            Assert.True(node.HasPendingProviderOperationReceipts);
            Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));

            // Simulate a process restart after source Outbox ACK and provider receipt
            // ACK commit, but before the caller receives the receipt-ACK response.
            NodeId localNodeId = node.NodeId;
            node.Dispose();
            wal.Dispose();
            node = StorageNodePersistence.Load(storage, localNodeId);
            wal = Wal.Open(walPath);
            node.AttachWal(wal);
            Assert.True(node.HasPendingProviderOperationReceipts);

            var retryCoordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
            int retried = await retryCoordinator.PublishLocalChangesAsync(
                asyncProvider, current => StorageNodePersistence.Save(storage, current));

            Assert.Equal(0, retried);
            Assert.Equal(1UL, asyncProvider.LastOutboxAck);
            Assert.Equal(1UL, asyncProvider.LastReceiptAck);
            Assert.False(node.HasPendingProviderOperationReceipts);
            Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
            Assert.True(node.TryGetField(1, record, 0x0902, out var recoveredValue));
            Assert.Equal("receipt-recovery", Encoding.UTF8.GetString(recoveredValue!.Value));
        }
        finally
        {
            wal.Dispose();
            node.Dispose();
        }
    }

    [Fact]
    public async Task Async_local_publication_recovers_after_published_origin_commit_response_loss()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-origin-response-loss"), 498);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(98);
        const string scope = "async-origin-response-loss";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));
        using var wal = Wal.Open(Path.Combine(_root, "async-origin-response-loss.wal"));
        node.AttachWal(wal);
        var asyncProvider = new RecordingAsyncLocalProvider(provider)
        {
            LoseNextPublishedOriginResponseAfterCommit = true
        };
        RecordId record = RecordId.From(Guid.Parse("98000000-0000-0000-0000-000000000098").ToByteArray());
        BatchId changeId = BatchId.From(Guid.Parse("98000000-0000-0000-0000-000000000099").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0981, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9801, Value = "origin-retry"u8.ToArray(), ChangeId = changeId
        });
        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);

        await Assert.ThrowsAsync<IOException>(async () =>
            await coordinator.PublishLocalChangesAsync(
                asyncProvider, current => StorageNodePersistence.Save(storage, current)));
        Assert.Equal(0UL, asyncProvider.LastOutboxAck);
        Assert.Equal(0UL, asyncProvider.LastReceiptAck);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);

        int retried = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));
        Assert.Equal(1, retried);
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        Assert.Equal(1UL, asyncProvider.LastReceiptAck);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.True(node.TryGetField(1, record, 0x0981, out var value));
        Assert.Equal("origin-retry", Encoding.UTF8.GetString(value!.Value));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public async Task Async_local_publication_recovers_receipt_ack_after_source_outbox_ack_response_loss()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-outbox-ack-response-loss"), 499);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(99);
        const string scope = "async-outbox-ack-response-loss";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));
        using var wal = Wal.Open(Path.Combine(_root, "async-outbox-ack-response-loss.wal"));
        node.AttachWal(wal);
        var asyncProvider = new RecordingAsyncLocalProvider(provider)
        {
            LoseNextOutboxAckResponseAfterCommit = true
        };
        RecordId record = RecordId.From(Guid.Parse("99000000-0000-0000-0000-000000000099").ToByteArray());
        BatchId changeId = BatchId.From(Guid.Parse("99000000-0000-0000-0000-000000000100").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0991, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9901, Value = "outbox-ack-retry"u8.ToArray(), ChangeId = changeId
        });
        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);

        await Assert.ThrowsAsync<IOException>(async () =>
            await coordinator.PublishLocalChangesAsync(
                asyncProvider, current => StorageNodePersistence.Save(storage, current)));
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        Assert.Equal(0UL, asyncProvider.LastReceiptAck);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.True(node.HasPendingProviderOperationReceipts);

        int retried = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));
        Assert.Equal(0, retried);
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        Assert.Equal(1UL, asyncProvider.LastReceiptAck);
        Assert.False(node.HasPendingProviderOperationReceipts);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public async Task Async_local_publication_rejects_receipt_client_id_rotation()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-receipt-stream-binding"), 496);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(95);
        const string scope = "async-receipt-stream-binding";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));
        using var wal = Wal.Open(Path.Combine(_root, "async-receipt-stream-binding.wal"));
        node.AttachWal(wal);

        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
        Guid firstClientId = Guid.Parse("95000000-0000-0000-0000-000000000095");
        var firstProvider = new RecordingAsyncLocalProvider(provider, firstClientId);
        int firstPass = await coordinator.PublishLocalChangesAsync(
            firstProvider, current => StorageNodePersistence.Save(storage, current));
        Assert.Equal(0, firstPass);

        var rotatedProvider = new RecordingAsyncLocalProvider(
            provider, Guid.Parse("95000000-0000-0000-0000-000000000096"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await coordinator.PublishLocalChangesAsync(
                rotatedProvider, current => StorageNodePersistence.Save(storage, current)));
        Assert.Equal(0UL, rotatedProvider.LastOutboxAck);
        Assert.Equal(0UL, rotatedProvider.LastReceiptAck);
    }

    [Fact]
    public void Metadata_store_rejects_reusing_one_receipt_client_id_across_namespaces()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-receipt-client-namespace"), 497);
        using var metadata = new StorageMetadataStore(storage);
        using var localNode = NewNode(97);
        NodeId local = localNode.NodeId;
        Guid clientId = Guid.Parse("97000000-0000-0000-0000-000000000097");

        Assert.Equal(clientId, metadata.BindProviderReceiptStream("receipt-client-namespace", local, 1, clientId));
        Assert.Throws<InvalidOperationException>(() =>
            metadata.BindProviderReceiptStream("receipt-client-namespace", local, 2, clientId));
    }

    [Fact]
    public async Task Async_local_publication_retries_host_auto_sequence_after_lost_append_response()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-local-append-response-loss"), 493);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(92);
        const string scope = "async-local-append-response-loss";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));

        using var wal = Wal.Open(Path.Combine(_root, "async-local-append-response-loss.wal"));
        node.AttachWal(wal);
        var asyncProvider = new RecordingAsyncLocalProvider(provider)
        {
            LoseNextAppendResponseAfterCommit = true
        };
        RecordId record = RecordId.From(Guid.Parse("92000000-0000-0000-0000-000000000092").ToByteArray());
        BatchId changeId = BatchId.From(Guid.Parse("92000000-0000-0000-0000-000000000093").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0903, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9200, Value = "append-response-loss"u8.ToArray(), ChangeId = changeId
        });

        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
        await Assert.ThrowsAsync<IOException>(async () =>
            await coordinator.PublishLocalChangesAsync(
                asyncProvider, current => StorageNodePersistence.Save(storage, current)));

        Assert.False(node.HasPendingProviderOperationReceipts);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);

        int published = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));

        Assert.Equal(1, published);
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        Assert.Equal(1UL, asyncProvider.LastReceiptAck);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.False(node.HasPendingProviderOperationReceipts);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public async Task Async_local_publication_leaves_appends_after_captured_ceiling_for_next_pass()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-local-feed-ceiling"), 494);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(93);
        const string scope = "async-local-feed-ceiling";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));

        using var wal = Wal.Open(Path.Combine(_root, "async-local-feed-ceiling.wal"));
        node.AttachWal(wal);
        var asyncProvider = new RecordingAsyncLocalProvider(provider);
        RecordId firstRecord = RecordId.From(Guid.Parse("93000000-0000-0000-0000-000000000093").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("93000000-0000-0000-0000-000000000094").ToByteArray());
        BatchId firstChange = BatchId.From(Guid.Parse("93000000-0000-0000-0000-000000000095").ToByteArray());
        BatchId secondChange = BatchId.From(Guid.Parse("93000000-0000-0000-0000-000000000096").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord, FieldId = 0x0904, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9300, Value = "captured-prefix"u8.ToArray(), ChangeId = firstChange
        });
        asyncProvider.AfterNextRead = () => provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord, FieldId = 0x0904, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9301, Value = "next-prefix"u8.ToArray(), ChangeId = secondChange
        });

        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
        int firstPass = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));

        Assert.Equal(1, firstPass);
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        ProviderChange pending = Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(secondChange, pending.ChangeId);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));

        int secondPass = await coordinator.PublishLocalChangesAsync(
            asyncProvider, current => StorageNodePersistence.Save(storage, current));

        Assert.Equal(1, secondPass);
        Assert.Equal(2UL, asyncProvider.LastOutboxAck);
        Assert.Equal(2UL, asyncProvider.LastReceiptAck);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.False(node.HasPendingProviderOperationReceipts);
    }

    [Fact]
    public async Task Async_startup_recovery_publishes_local_feed_before_accepting_sessions()
    {
        Directory.CreateDirectory(_root);
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-startup-recovery"), 495);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(94);
        const string scope = "async-startup-recovery";
        var legacyCoordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        legacyCoordinator.EnableMetadataStoreOwnedPublication(_ => StorageNodePersistence.Save(storage, node));

        using var wal = Wal.Open(Path.Combine(_root, "async-startup-recovery.wal"));
        node.AttachWal(wal);
        RecordId record = RecordId.From(Guid.Parse("94000000-0000-0000-0000-000000000094").ToByteArray());
        BatchId changeId = BatchId.From(Guid.Parse("94000000-0000-0000-0000-000000000095").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0905, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 9400, Value = "startup-recovery"u8.ToArray(), ChangeId = changeId
        });

        var asyncProvider = new RecordingAsyncLocalProvider(provider);
        var asyncRemoteProvider = new RecordingAsyncRemoteBatchStore(provider);
        var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, 1);
        AsyncStorageStartupRecoveryResult result = await coordinator.RecoverBeforeAcceptingSessionsAsync(
            asyncProvider, asyncRemoteProvider,
            current => StorageNodePersistence.Save(storage, current));

        Assert.Equal(0, result.RecoveredRemoteBatches);
        Assert.Equal(1, result.PublishedLocalChanges);
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, asyncProvider.LastOutboxAck);
        Assert.Equal(1UL, asyncProvider.LastReceiptAck);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.True(node.TryGetField(1, record, 0x0905, out var value));
        Assert.Equal("startup-recovery", Encoding.UTF8.GetString(value!.Value));
    }

    [Fact]
    public async Task Async_pending_apply_recovery_replays_exact_wire_payload_before_finalizing_metadata()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "async-pending-recovery"), 428);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(38);
        using var receiver = NewNode(39);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new AsyncStorageSyncCoordinator(receiver, metadata, "async-pending-recovery", 1);
        var asyncProvider = new RecordingAsyncRemoteBatchStore(provider);

        RecordId record = RecordId.From(Guid.Parse("38000000-0000-0000-0000-000000000038").ToByteArray());
        Assert.True(sender.WriteField(1, record, 0x0838, "async-recover"u8.ToArray(), new Hlc(5380, 0)));

        using var sendCts = new CancellationTokenSource();
        var receive = acceptor.ReceiveBatchAsync(receiver,
            beforeApply: null,
            beforeApplyWithOperationDecisions: (batch, payload, peer) =>
            {
                _ = coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                throw new InvalidOperationException("simulated process interruption after durable PendingApply");
            },
            beforeAck: null,
            ct: CancellationToken.None,
            receiveStateOwnedByMetadataStore: true);
        Task<bool> send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive);
        sendCts.Cancel();
        try { _ = await send; }
        catch (OperationCanceledException) { }
        catch (IOException) { }

        MetadataPendingApplyIntent pending = Assert.Single(
            metadata.ReadPendingApplyIntents("async-pending-recovery", receiver.NodeId));
        Assert.True(pending.Batch.ReceiveStateOwnedByMetadataStore);
        Assert.NotEmpty(pending.Batch.WirePayload);
        Assert.False(receiver.TryGetField(1, record, 0x0838, out _));
        Assert.False(provider.TryReadCurrent(record, 0x0838, out _));

        asyncProvider.LoseNextResponseAfterCommit = true;
        await Assert.ThrowsAsync<IOException>(async () =>
            await coordinator.RecoverPendingAppliesAsync(
                asyncProvider, node => StorageNodePersistence.Save(storage, node)));
        Assert.Single(metadata.ReadPendingApplyIntents("async-pending-recovery", receiver.NodeId));
        Assert.True(provider.TryReadCurrent(record, 0x0838, out _));
        using (Node checkpoint = StorageNodePersistence.Load(storage, receiver.NodeId))
            Assert.True(checkpoint.TryGetField(1, record, 0x0838, out _));

        int recovered = await coordinator.RecoverPendingAppliesAsync(
            asyncProvider, node => StorageNodePersistence.Save(storage, node));

        Assert.Equal(1, recovered);
        Assert.Collection(asyncProvider.ApplyTokens,
            firstToken => Assert.Equal(pending.ApplyToken, firstToken),
            retryToken => Assert.Equal(pending.ApplyToken, retryToken));
        Assert.NotNull(asyncProvider.LastCommit);
        Assert.Equal(pending.ApplyToken, asyncProvider.LastCommit!.ApplyToken);
        Assert.Equal(sender.NodeId, asyncProvider.LastCommit.SenderPeer);
        Assert.Empty(metadata.ReadPendingApplyIntents("async-pending-recovery", receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0838, out var nodeValue));
        Assert.Equal("async-recover", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0838, out var providerValue));
        Assert.Equal("async-recover", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task Pending_receive_intent_replays_wire_batch_into_node_and_provider_after_interruption()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-receive-recovery"), 427);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(28);
        using var receiver = NewNode(29);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "pending-receive-recovery");

        RecordId record = RecordId.From(Guid.Parse("28000000-0000-0000-0000-000000000028").ToByteArray());
        sender.WriteField(1, record, 0x0812, "replay-after-crash"u8.ToArray(), new Hlc(5000, 0));
        var sourceEntry = Assert.Single(sender.ReadOutboxEntries(1, sender.NodeId, 0, 10));
        OperationOrigin expectedOrigin = sourceEntry.Provenance;

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = acceptor.ReceiveBatchAsync(receiver,
            beforeApply: null,
            beforeApplyWithOperationDecisions: (batch, payload, peer) =>
            {
                coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                throw new InvalidOperationException("simulated interruption after durable intent");
            },
            beforeAck: null,
            ct: receiveCts.Token,
            receiveStateOwnedByMetadataStore: true);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        var pendingIntent = Assert.Single(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.Equal(expectedOrigin, Assert.Single(pendingIntent.Batch.Operations).Provenance);
        Assert.False(receiver.TryGetField(1, record, 0x0812, out _));
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.RehydrateNodeFromProviderSnapshot(_ =>
                throw new InvalidOperationException("simulated interruption after provider snapshot restore")));
        Assert.Single(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.False(receiver.TryGetField(1, record, 0x0812, out _));
        Assert.False(provider.TryReadCurrent(record, 0x0812, out _));

        Assert.Equal(1, coordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(metadata.ReadPendingApplyIntents("pending-receive-recovery", receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0812, out var nodeValue));
        Assert.Equal("replay-after-crash", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0812, out var providerValue));
        Assert.Equal("replay-after-crash", Encoding.UTF8.GetString(providerValue!.Value));
        var relayed = Assert.Single(metadata.ReadRelay("pending-receive-recovery", receiver.NodeId,
            1, sender.NodeId, 0, 10).Items);
        Assert.Equal(expectedOrigin, relayed.Provenance);
        Assert.Equal(0, receiver.GlobalChangeLedgerCount);
    }


    [Fact]
    public async Task Node_checkpoint_failure_after_pending_apply_recovers_from_reopened_storage_and_replays_as_duplicate()
    {
        string directory = Path.Combine(_root, "node-checkpoint-crash-recovery");
        const ulong partitionId = 463;
        const string scope = "node-checkpoint-crash-recovery";
        using var sender = NewNode(63);
        byte[] receiverIdentity = new byte[16];
        receiverIdentity[0] = 64;
        receiverIdentity[15] = (byte)(64 ^ 0xA5);
        NodeId receiverId = NodeId.From(receiverIdentity);
        RecordId record = RecordId.From(Guid.Parse("63000000-0000-0000-0000-000000000063").ToByteArray());
        Assert.True(sender.WriteField(1, record, 0x0823, "checkpoint-crash"u8.ToArray(), new Hlc(7000, 0)));
        var checkpointSourceEntry = Assert.Single(sender.ReadOutboxEntries(1, sender.NodeId, 0, 10));
        OperationOrigin checkpointExpectedOrigin = checkpointSourceEntry.Provenance;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var receiver = NewNode(64))
        {
            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            // Persist the pre-batch checkpoint only after the negotiated alias map
            // exists, so restart recovery can decode the staged wire payload.
            StorageNodePersistence.Save(storage, receiver);
            var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

            using var sendCts = new CancellationTokenSource();
            Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(
                acceptor,
                _ => throw new IOException("Injected Node checkpoint failure after Node apply."));
            Task<bool> send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

            await Assert.ThrowsAsync<IOException>(async () => await receive);
            sendCts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

            var staged = Assert.Single(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
            Assert.Equal(checkpointExpectedOrigin, Assert.Single(staged.Batch.Operations).Provenance);
            Assert.True(receiver.TryGetField(1, record, 0x0823, out _));
            Assert.False(provider.TryReadCurrent(record, 0x0823, out _));
        }

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var receiver = StorageNodePersistence.Load(storage, receiverId))
        {
            // The failed checkpoint left the original durable Node image intact.
            Assert.False(receiver.TryGetField(1, record, 0x0823, out _));
            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);
            Assert.Equal(1, coordinator.RecoverPendingApplies(node => StorageNodePersistence.Save(storage, node)));
            Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
            Assert.True(receiver.TryGetField(1, record, 0x0823, out var restored));
            Assert.Equal("checkpoint-crash", Encoding.UTF8.GetString(restored!.Value));
            Assert.True(provider.TryReadCurrent(record, 0x0823, out var providerValue));
            Assert.Equal("checkpoint-crash", Encoding.UTF8.GetString(providerValue!.Value));
            var recoveredRelay = Assert.Single(metadata.ReadRelay(scope, receiver.NodeId,
                1, sender.NodeId, 0, 10).Items);
            Assert.Equal(checkpointExpectedOrigin, recoveredRelay.Provenance);

            var retryCoordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

            Task<BatchResult> retryReceive = retryCoordinator.ReceiveBatchBeforeAckAsync(
                acceptor, node => StorageNodePersistence.Save(storage, node));
            Assert.True(await initiator.PushBatchAsync(sender, 1));
            BatchResult duplicate = await retryReceive;
            Assert.True(duplicate.Duplicate);
            Assert.Equal(0, duplicate.Applied);
            Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        }
    }

    [Fact]
    public async Task Receive_batch_stages_metadata_intent_before_node_and_provider_apply()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-receive-intent"), 425);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(26);
        using var receiver = NewNode(27);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "durable-receive-intent");

        RecordId record = RecordId.From(Guid.Parse("26000000-0000-0000-0000-000000000026").ToByteArray());
        sender.WriteField(1, record, 0x0811, "received-durably"u8.ToArray(), new Hlc(4000, 0));

        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(sender, 1));
        BatchResult result = await receive;

        Assert.Equal(1, result.Applied);
        Assert.True(receiver.TryGetField(1, record, 0x0811, out var nodeValue));
        Assert.Equal("received-durably", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(record, 0x0811, out var providerValue));
        Assert.Equal("received-durably", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.Empty(metadata.ReadPendingApplyIntents("durable-receive-intent", receiver.NodeId));
        Assert.Equal(new[] { sender.NodeId }, metadata.ReadRelayAuthors("durable-receive-intent", receiver.NodeId, 1));
        Assert.Equal(0, receiver.GlobalChangeLedgerCount);
    }

    [Fact]
    public async Task Metadata_store_owns_receive_cursor_and_digest_after_node_recreation()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-receive-authority"), 451);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(50);
        const string scope = "metadata-receive-authority";
        RecordId firstRecord = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000050").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("51000000-0000-0000-0000-000000000051").ToByteArray());

        sender.WriteField(1, firstRecord, 0x0815, "first-durable-batch"u8.ToArray(), new Hlc(5600, 0));
        {
            using var firstReceiver = NewNode(51);
            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(firstReceiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            var coordinator = new StorageSyncCoordinator(firstReceiver, provider, 1, metadata, scope);

            Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
            Assert.True(await initiator.PushBatchAsync(sender, 1));
            BatchResult result = await receive;
            Assert.Equal(1, result.Applied);
        }

        // This receiver has the same stable node identity but none of the previous
        // Node-local InboxAuthors, PeerStates or LastBatchDigest dictionaries.
        sender.WriteField(1, secondRecord, 0x0815, "second-durable-batch"u8.ToArray(), new Hlc(5700, 0));
        using var secondReceiver = NewNode(51);
        var (nextToSender, nextToReceiver) = PipeTransport.CreatePair();
        var nextAcceptTask = SyncSession.AcceptAsync(secondReceiver, nextToReceiver);
        var nextConnectTask = SyncSession.ConnectAsync(sender, nextToSender);
        await using var nextAcceptor = await nextAcceptTask;
        await using var nextInitiator = await nextConnectTask;
        var nextCoordinator = new StorageSyncCoordinator(secondReceiver, provider, 1, metadata, scope);

        Task<BatchResult> nextReceive = nextCoordinator.ReceiveBatchBeforeAckAsync(nextAcceptor, _ => { });
        Assert.True(await nextInitiator.PushBatchAsync(sender, 1));
        BatchResult nextResult = await nextReceive;

        Assert.Equal(1, nextResult.Applied);
        Assert.Equal(2UL, metadata.GetReceiveCursor(scope, secondReceiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.True(secondReceiver.TryGetField(1, secondRecord, 0x0815, out var nodeValue));
        Assert.Equal("second-durable-batch", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(provider.TryReadCurrent(firstRecord, 0x0815, out var firstValue));
        Assert.Equal("first-durable-batch", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.True(provider.TryReadCurrent(secondRecord, 0x0815, out var secondValue));
        Assert.Equal("second-durable-batch", Encoding.UTF8.GetString(secondValue!.Value));
    }

    [Fact]
    public async Task Lost_ack_after_metadata_finalize_replays_as_durable_duplicate()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-lost-ack"), 452);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(52);
        using var receiver = NewNode(53);
        const string scope = "metadata-lost-ack";
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("52000000-0000-0000-0000-000000000052").ToByteArray());
        sender.WriteField(1, record, 0x0816, "committed-before-lost-ack"u8.ToArray(), new Hlc(5800, 0));

        StorageRemoteApplyPlan? firstPlan = null;
        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        Task<BatchResult> firstReceive = acceptor.ReceiveBatchAsync(receiver,
            beforeApply: null,
            beforeApplyWithOperationDecisions: (batch, payload, peer) =>
            {
                firstPlan = coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                return firstPlan.ApplyOperations;
            },
            beforeAck: (batch, _) =>
            {
                var plan = firstPlan ?? throw new InvalidOperationException("Pending intent was not staged.");
                coordinator.ApplyRemoteBatchAfterNodeApply(batch, plan, _ => { });
                throw new InvalidOperationException("simulated ACK loss after durable metadata finalization");
            },
            ct: receiveCts.Token,
            receiveStateOwnedByMetadataStore: true);
        Task<bool> firstSend = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await firstReceive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await firstSend);
        Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));

        Task<BatchResult> replayReceive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(sender, 1));
        BatchResult replayResult = await replayReceive;

        Assert.True(replayResult.Duplicate);
        Assert.Equal(0, replayResult.Applied);
        Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiver.NodeId, sender.NodeId, 1, 1, sender.NodeId));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(provider.TryReadCurrent(record, 0x0816, out var value));
        Assert.Equal("committed-before-lost-ack", Encoding.UTF8.GetString(value!.Value));
    }

    [Fact]
    public async Task Provider_failure_after_node_apply_keeps_pending_intent_for_recovery()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-failure-after-node"), 441);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(42);
        using var receiver = NewNode(43);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "provider-failure-after-node";
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("42000000-0000-0000-0000-000000000042").ToByteArray());
        sender.WriteField(1, record, 0x0813, "node-applied-provider-pending"u8.ToArray(), new Hlc(5200, 0));

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => provider.Dispose(), receiveCts.Token);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        Assert.Single(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0813, out var nodeValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(nodeValue!.Value));

        using var recoveredProvider = new StorageDataProvider(storage);
        var recoveredCoordinator = new StorageSyncCoordinator(receiver, recoveredProvider, 1, metadata, scope);
        Assert.Equal(1, recoveredCoordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0813, out var restoredNodeValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(restoredNodeValue!.Value));
        Assert.True(recoveredProvider.TryReadCurrent(record, 0x0813, out var providerValue));
        Assert.Equal("node-applied-provider-pending", Encoding.UTF8.GetString(providerValue!.Value));
    }

    [Fact]
    public async Task Provider_commit_before_metadata_finalize_is_recovered_idempotently()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-commit-before-metadata"), 442);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(44);
        using var receiver = NewNode(45);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "provider-commit-before-metadata";
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

        RecordId record = RecordId.From(Guid.Parse("44000000-0000-0000-0000-000000000044").ToByteArray());
        sender.WriteField(1, record, 0x0814, "provider-committed-metadata-pending"u8.ToArray(), new Hlc(5400, 0));

        using var sendCts = new CancellationTokenSource();
        using var receiveCts = new CancellationTokenSource();
        var receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => metadata.Dispose(), receiveCts.Token);
        var send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receive);
        sendCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);

        Assert.True(provider.TryReadCurrent(record, 0x0814, out var committedValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(committedValue!.Value));

        using var recoveredMetadata = new StorageMetadataStore(storage);
        using var recoveredProvider = new StorageDataProvider(storage);
        Assert.Single(recoveredMetadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        var recoveredCoordinator = new StorageSyncCoordinator(receiver, recoveredProvider, 1, recoveredMetadata, scope);
        Assert.Equal(1, recoveredCoordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Empty(recoveredMetadata.ReadPendingApplyIntents(scope, receiver.NodeId));
        Assert.True(receiver.TryGetField(1, record, 0x0814, out var restoredNodeValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(restoredNodeValue!.Value));
        Assert.True(recoveredProvider.TryReadCurrent(record, 0x0814, out var providerValue));
        Assert.Equal("provider-committed-metadata-pending", Encoding.UTF8.GetString(providerValue!.Value));
    }


    [Fact]
    public async Task Provider_commit_before_metadata_finalize_survives_database_reopen_and_duplicate_retry()
    {
        string directory = Path.Combine(_root, "provider-commit-metadata-finalize-reopen");
        const ulong partitionId = 464;
        const string scope = "provider-commit-metadata-finalize-reopen";
        using var sender = NewNode(65);
        byte[] receiverIdentity = new byte[16];
        receiverIdentity[0] = 66;
        receiverIdentity[15] = (byte)(66 ^ 0xA5);
        NodeId receiverId = NodeId.From(receiverIdentity);
        RecordId record = RecordId.From(Guid.Parse("65000000-0000-0000-0000-000000000065").ToByteArray());
        Assert.True(sender.WriteField(1, record, 0x0824, "provider-commit-before-finalize"u8.ToArray(), new Hlc(7100, 0)));

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var receiver = NewNode(66))
        {
            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            StorageNodePersistence.Save(storage, receiver);
            var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

            using var sendCts = new CancellationTokenSource();
            Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, node =>
            {
                StorageNodePersistence.Save(storage, node);
                // Provider Apply runs after this callback. Disposing the metadata
                // adapter forces FinalizePendingApply to fail after provider commit.
                metadata.Dispose();
            });
            Task<bool> send = initiator.PushBatchAsync(sender, 1, ct: sendCts.Token);

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receive);
            sendCts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await send);
        }

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var receiver = StorageNodePersistence.Load(storage, receiverId))
        {
            Assert.Single(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
            Assert.True(receiver.TryGetField(1, record, 0x0824, out var checkpointed));
            Assert.Equal("provider-commit-before-finalize", Encoding.UTF8.GetString(checkpointed!.Value));
            Assert.True(provider.TryReadCurrent(record, 0x0824, out var committed));
            Assert.Equal("provider-commit-before-finalize", Encoding.UTF8.GetString(committed!.Value));

            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;
            var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);

            Assert.Equal(1, coordinator.RecoverPendingApplies(node => StorageNodePersistence.Save(storage, node)));
            Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
            Task<BatchResult> retryReceive = coordinator.ReceiveBatchBeforeAckAsync(
                acceptor, node => StorageNodePersistence.Save(storage, node));
            Assert.True(await initiator.PushBatchAsync(sender, 1));
            BatchResult duplicate = await retryReceive;
            Assert.True(duplicate.Duplicate);
            Assert.Equal(0, duplicate.Applied);
            Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiver.NodeId));
            Assert.True(provider.TryReadCurrent(record, 0x0824, out var afterRetry));
            Assert.Equal("provider-commit-before-finalize", Encoding.UTF8.GetString(afterRetry!.Value));
        }
    }

    [Fact]
    public async Task New_peer_incarnation_retires_old_epoch_without_inheriting_its_ack_cursor()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "peer-new-incarnation"), 439);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var local = NewNode(40);
        byte[] peerBytes = new byte[16];
        peerBytes[0] = 41;
        peerBytes[15] = (byte)(41 ^ 0xA5);
        using var peer = new NodeBuilder().WithNodeId(peerBytes).WithEpoch(2).Build();
        const string scope = "peer-new-incarnation";
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("40000000-0000-0000-0000-000000000040").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0104, AuthorNodeId = local.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 4000, Value = "must-replay-for-new-incarnation"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });

        metadata.RegisterInterestedPeer(scope, local.NodeId, peer.NodeId, 1, 1);
        metadata.CompletePeerInitialSync(scope, local.NodeId, peer.NodeId, 1, 1,
            new[] { new MetadataPeerStreamBaseline(local.NodeId, 0, checked((uint)local.Epoch)) });
        var oldEpochBatch = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Guid.Parse("a2000000-0000-0000-0000-0000000000a2").ToByteArray()),
            IssuerEpoch = 1,
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)161, 16).ToArray(),
            Payload = new byte[] { 162 },
            StreamOffset = 0
        };
        metadata.ReserveOutboundBatch(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch);
        metadata.MarkOutboundBatchPushed(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch.BatchId);
        metadata.AcknowledgeOutboundBatch(scope, local.NodeId, peer.NodeId, 1, 1,
            local.NodeId, oldEpochBatch.BatchId, 1, oldEpochBatch.BatchDigest);
        Assert.Equal(1UL, metadata.GetPeerStreamState(scope, local.NodeId,
            peer.NodeId, 1, 1, local.NodeId).AckedCursor);

        var (toLocal, toPeer) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(peer, toPeer);
        var connectTask = SyncSession.ConnectAsync(local, toLocal);
        await using var acceptor = await acceptTask;
        await using var session = await connectTask;

        coordinator.RegisterInterestedPeer(session, 1);
        Assert.Equal(2, metadata.ReadInterestedPeers(scope, local.NodeId, 1).Count);
        Assert.Null(metadata.GetRetiredPeerEpoch(scope, local.NodeId, peer.NodeId, 1));

        // The old incarnation remains valid until the replacement's complete
        // bidirectional snapshot baseline is durably committed by the coordinator.
        var peerReconcile = acceptor.AutoReconcileAsync(peer, 1, initiate: false);
        var localReconcile = coordinator.AutoReconcileWithMetadataAsync(
            session, _ => { }, initiate: true);
        await AwaitBothPeerOperationsAsync(peerReconcile, localReconcile);

        var newEpochState = metadata.GetPeerStreamState(scope, local.NodeId,
            peer.NodeId, 2, 1, local.NodeId);
        Assert.Equal(0UL, newEpochState.AckedCursor);
        Assert.Equal(0UL, newEpochState.PushedCursor);
        // The replacement baseline covers the one current local outbound item;
        // this offset is a fresh snapshot baseline, not inherited old-epoch ACK.
        Assert.Equal(1UL, newEpochState.StreamOffset);
        Assert.Equal(checked((uint)local.Epoch), newEpochState.IssuerEpoch);
        Assert.Equal(1U, metadata.GetRetiredPeerEpoch(scope, local.NodeId, peer.NodeId, 1));
        Assert.Equal(2U, Assert.Single(metadata.ReadInterestedPeers(scope, local.NodeId, 1)).PeerEpoch);
        // The full snapshot baseline already covers the current outbound item.
        Assert.Empty(metadata.ReadOutboundForPeer(scope, local.NodeId,
            peer.NodeId, 2, 1, local.NodeId, 10).Items);
    }

    [Fact]
    public async Task Three_replica_relay_retains_one_shared_payload_until_every_downstream_peer_acks()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "relay-three-a"), 434);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "relay-three-b"), 435);
        using var storageC = StorageDatabase.Open(Path.Combine(_root, "relay-three-c"), 436);
        using var storageD = StorageDatabase.Open(Path.Combine(_root, "relay-three-d"), 437);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var providerC = new StorageDataProvider(storageC);
        using var providerD = new StorageDataProvider(storageD);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var metadataC = new StorageMetadataStore(storageC);
        using var metadataD = new StorageMetadataStore(storageD);
        using var nodeA = NewNode(36);
        using var nodeB = NewNode(37);
        using var nodeC = NewNode(38);
        using var nodeD = NewNode(39);
        const string scopeA = "relay-three-a";
        const string scopeB = "relay-three-b";
        const string scopeC = "relay-three-c";
        const string scopeD = "relay-three-d";
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, scopeA);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, scopeB);
        var coordinatorC = new StorageSyncCoordinator(nodeC, providerC, 1, metadataC, scopeC);
        var coordinatorD = new StorageSyncCoordinator(nodeD, providerD, 1, metadataD, scopeD);

        var (aToB, bFromA) = PipeTransport.CreatePair();
        var bAcceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var aConnectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var bAccept = await bAcceptTask;
        await using var aConnect = await aConnectTask;

        RecordId record = RecordId.From(Guid.Parse("36000000-0000-0000-0000-000000000036").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0103, AuthorNodeId = nodeA.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3600, Value = "shared-relay-payload"u8.ToArray()
        });
        coordinatorA.PublishLocalChanges(_ => { });
        coordinatorA.RegisterInterestedPeer(aConnect, 1);
        metadataA.CompletePeerInitialSync(scopeA, nodeA.NodeId, nodeB.NodeId,
            checked((uint)nodeB.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
        var receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(bAccept, _ => { });
        Assert.True(await aConnect.PushBatchAsync(nodeA, 1, metadataA, scopeA));
        Assert.Equal(1, (await receiveAtB).Applied);
        Assert.Single(metadataB.ReadRelay(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 0, 10).Items);

        // D is durably interested but offline. C's ACK alone must not release the
        // single shared relay payload needed by D.
        metadataB.RegisterInterestedPeer(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1);
        metadataB.CompletePeerInitialSync(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });

        var (bToC, cFromB) = PipeTransport.CreatePair();
        var cAcceptTask = SyncSession.AcceptAsync(nodeC, cFromB);
        var bConnectCTask = SyncSession.ConnectAsync(nodeB, bToC);
        await using var cAccept = await cAcceptTask;
        await using var bConnectC = await bConnectCTask;
        coordinatorB.RegisterInterestedPeer(bConnectC, 1);
        metadataB.CompletePeerInitialSync(scopeB, nodeB.NodeId, nodeC.NodeId,
            checked((uint)nodeC.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
        var receiveAtC = coordinatorC.ReceiveBatchBeforeAckAsync(cAccept, _ => { });
        Assert.True(await bConnectC.PushBatchAsync(nodeB, 1, metadataB, scopeB));
        Assert.Equal(1, (await receiveAtC).Applied);
        Assert.True(providerC.TryReadCurrent(record, 0x0103, out var cValue));
        Assert.Equal("shared-relay-payload", Encoding.UTF8.GetString(cValue!.Value));
        Assert.Equal(0UL, metadataB.GetSafePeerStreamCompactionCursor(scopeB,
            nodeB.NodeId, 1, nodeA.NodeId));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            metadataB.CompactRelayThrough(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 1));

        var (bToD, dFromB) = PipeTransport.CreatePair();
        var dAcceptTask = SyncSession.AcceptAsync(nodeD, dFromB);
        var bConnectDTask = SyncSession.ConnectAsync(nodeB, bToD);
        await using var dAccept = await dAcceptTask;
        await using var bConnectD = await bConnectDTask;
        coordinatorB.RegisterInterestedPeer(bConnectD, 1);
        var receiveAtD = coordinatorD.ReceiveBatchBeforeAckAsync(dAccept, _ => { });
        Assert.True(await bConnectD.PushBatchAsync(nodeB, 1, metadataB, scopeB));
        Assert.Equal(1, (await receiveAtD).Applied);
        Assert.True(providerD.TryReadCurrent(record, 0x0103, out var dValue));
        Assert.Equal("shared-relay-payload", Encoding.UTF8.GetString(dValue!.Value));

        Assert.Equal(1UL, metadataB.GetSafePeerStreamCompactionCursor(scopeB,
            nodeB.NodeId, 1, nodeA.NodeId));
        metadataB.CompactRelayThrough(scopeB, nodeB.NodeId, 1, nodeA.NodeId, 1);
        Assert.Empty(metadataB.ReadRelayForPeer(scopeB, nodeB.NodeId, nodeD.NodeId,
            checked((uint)nodeD.Epoch), 1, nodeA.NodeId, 10).Items);
    }

    [Fact]
    public async Task Storage_initial_reconcile_persists_snapshot_and_commits_durable_feed_baseline()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "snapshot-baseline-a"), 441);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "snapshot-baseline-b"), 442);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = NewNode(44);
        using var nodeB = NewNode(45);
        const string scopeA = "snapshot-baseline-a";
        const string scopeB = "snapshot-baseline-b";
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, scopeA);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, scopeB);
        var (aToB, bFromA) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var connectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        RecordId record = RecordId.From(Guid.Parse("44000000-0000-0000-0000-000000000044").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0104, AuthorNodeId = nodeA.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 4400, Value = "snapshot-baseline"u8.ToArray()
        });

        Task reconcileA = coordinatorA.AutoReconcileWithMetadataAsync(initiator, _ => { }, initiate: true);
        Task reconcileB = coordinatorB.AutoReconcileWithMetadataAsync(acceptor, _ => { }, initiate: false);
        await AwaitBothPeerOperationsAsync(reconcileA, reconcileB);

        Assert.True(metadataA.IsPeerInitialSyncComplete(scopeA, nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1));
        Assert.True(metadataB.IsPeerInitialSyncComplete(scopeB, nodeB.NodeId,
            nodeA.NodeId, checked((uint)nodeA.Epoch), 1));
        Assert.True(providerB.TryReadCurrent(record, 0x0104, out var snapshotValue));
        Assert.Equal("snapshot-baseline", Encoding.UTF8.GetString(snapshotValue!.Value));
        Assert.Equal(1UL, metadataA.GetSafePeerStreamCompactionCursor(scopeA, nodeA.NodeId, 1, nodeA.NodeId));
        Assert.Empty(metadataA.ReadOutboundForPeer(scopeA, nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1, nodeA.NodeId, 10).Items);

        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0105, AuthorNodeId = nodeA.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 4500, Value = "after-snapshot"u8.ToArray()
        });
        coordinatorA.PublishLocalChanges(_ => { });
        var receive = coordinatorB.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Assert.True(await initiator.PushBatchAsync(nodeA, 1, metadataA, scopeA));
        Assert.Equal(1, (await receive).Applied);
        Assert.True(providerB.TryReadCurrent(record, 0x0105, out var deltaValue));
        Assert.Equal("after-snapshot", Encoding.UTF8.GetString(deltaValue!.Value));
        Assert.Equal(2UL, metadataA.GetSafePeerStreamCompactionCursor(scopeA, nodeA.NodeId, 1, nodeA.NodeId));
    }

    [Fact]
    public async Task Auto_reconcile_replaces_stale_Node_checkpoint_after_provider_cutover()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "auto-rehydrate-a"), 463);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "auto-rehydrate-b"), 464);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var originalNodeA = NewNode(63);
        using var nodeB = NewNode(64);
        using var staleRemote = NewNode(65);
        const string scopeA = "auto-rehydrate-a";
        const string scopeB = "auto-rehydrate-b";

        RecordId staleRecord = RecordId.From(Guid.Parse("63000000-0000-0000-0000-000000000001").ToByteArray());
        RecordId providerRecord = RecordId.From(Guid.Parse("63000000-0000-0000-0000-000000000002").ToByteArray());
        var initialCoordinatorA = new StorageSyncCoordinator(originalNodeA, providerA, 1, metadataA, scopeA);
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, scopeB);
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = providerRecord, FieldId = 0x0823, AuthorNodeId = originalNodeA.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 7000, Value = "provider-owned-before-cutover"u8.ToArray()
        });
        Assert.Equal(1, initialCoordinatorA.PublishLocalChanges(_ => { }));
        Assert.Single(originalNodeA.ReadOutboxEntries(1, originalNodeA.NodeId, 0, 10));

        initialCoordinatorA.EnableMetadataStoreOwnedPublication(_ => { });
        coordinatorB.EnableMetadataStoreOwnedPublication(_ => { });
        Assert.Empty(originalNodeA.ReadOutboxEntries(1, originalNodeA.NodeId, 0, 10));
        Assert.Empty(originalNodeA.ReadOutboxEntries(1, staleRemote.NodeId, 0, 10));
        Assert.Equal(0, originalNodeA.GlobalChangeLedgerCount);

        // Reintroduce a stale cache row after the cutover fence without creating a
        // legacy ChangeId ledger entry. The provider snapshot must replace this row.
        originalNodeA.RestoreProviderSnapshot(1, new[]
        {
            new ProviderMutation
            {
                RecordId = staleRecord,
                FieldId = 0x0822,
                AuthorNodeId = staleRemote.NodeId,
                HlcWall = 6900,
                HlcLogical = 0,
                Value = "stale-node-checkpoint"u8.ToArray()
            }
        }, Array.Empty<BatchId>());

        // Capture stale field state after the safe cutover has discarded the
        // represented Node ledger shadow, but before provider rehydration.
        byte[] staleCheckpoint = NodePersistence.SerializeToBytes(originalNodeA);

        // Simulate restart from a pre-rehydration Node checkpoint while the provider's
        // durable cutover marker and empty authoritative snapshot remain current.
        using var restartedNodeA = NodePersistence.LoadFromBytes(staleCheckpoint);
        Assert.True(restartedNodeA.TryGetField(1, staleRecord, 0x0822, out _));
        var coordinatorA = new StorageSyncCoordinator(restartedNodeA, providerA, 1, metadataA, scopeA);
        Assert.Equal(0, restartedNodeA.GlobalChangeLedgerCount);

        var (aToB, bFromA) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var connectTask = SyncSession.ConnectAsync(restartedNodeA, aToB);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        Task reconcileA = coordinatorA.AutoReconcileWithMetadataAsync(initiator, _ => { }, initiate: true);
        Task reconcileB = coordinatorB.AutoReconcileWithMetadataAsync(acceptor, _ => { }, initiate: false);
        await AwaitBothPeerOperationsAsync(reconcileA, reconcileB);

        Assert.False(restartedNodeA.TryGetField(1, staleRecord, 0x0822, out _));
        Assert.False(nodeB.TryGetField(1, staleRecord, 0x0822, out _));
        Assert.False(providerB.TryReadCurrent(staleRecord, 0x0822, out _));
        Assert.Empty(restartedNodeA.ReadOutboxEntries(1, staleRemote.NodeId, 0, 10));
        Assert.True(restartedNodeA.TryGetField(1, providerRecord, 0x0823, out var providerValue));
        Assert.Equal("provider-owned-before-cutover", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.True(providerB.TryReadCurrent(providerRecord, 0x0823, out var peerProviderValue));
        Assert.Equal("provider-owned-before-cutover", Encoding.UTF8.GetString(peerProviderValue!.Value));
        Assert.Empty(restartedNodeA.ReadOutboxEntries(1, restartedNodeA.NodeId, 0, 10));
        Assert.Equal(1L, restartedNodeA.GetOutboxHighWatermark(1, restartedNodeA.NodeId));
        Assert.True(metadataA.IsPeerInitialSyncComplete(scopeA, restartedNodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1));
    }

    [Fact]
    public async Task Initial_reconcile_rehydrates_empty_node_from_provider_snapshot_after_feed_ack()
    {
        using var storageA = StorageDatabase.Open(Path.Combine(_root, "provider-rehydrate-a"), 443);
        using var storageB = StorageDatabase.Open(Path.Combine(_root, "provider-rehydrate-b"), 444);
        using var providerA = new StorageDataProvider(storageA);
        using var providerB = new StorageDataProvider(storageB);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = NewNode(46);
        using var nodeB = NewNode(47);
        var coordinatorA = new StorageSyncCoordinator(nodeA, providerA, 1, metadataA, "provider-rehydrate-a");
        var coordinatorB = new StorageSyncCoordinator(nodeB, providerB, 1, metadataB, "provider-rehydrate-b");
        var (aToB, bFromA) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, bFromA);
        var connectTask = SyncSession.ConnectAsync(nodeA, aToB);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        RecordId record = RecordId.From(Guid.Parse("46000000-0000-0000-0000-000000000046").ToByteArray());
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0106, AuthorNodeId = nodeA.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 4600, Value = "provider-owned"u8.ToArray()
        });
        var seededFeed = providerA.ReadLocalChanges(0, 16);
        providerA.AcknowledgeLocalChanges(seededFeed.ScannedThroughSequence);
        Assert.True(nodeA.IsPristine);

        Task reconcileA = coordinatorA.AutoReconcileWithMetadataAsync(initiator, _ => { }, initiate: true);
        Task reconcileB = coordinatorB.AutoReconcileWithMetadataAsync(acceptor, _ => { }, initiate: false);
        await AwaitBothPeerOperationsAsync(reconcileA, reconcileB);

        Assert.True(nodeA.TryGetField(1, record, 0x0106, out var nodeValue));
        Assert.Equal("provider-owned", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.True(providerB.TryReadCurrent(record, 0x0106, out var providerValue));
        Assert.Equal("provider-owned", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.True(metadataA.IsPeerInitialSyncComplete("provider-rehydrate-a", nodeA.NodeId,
            nodeB.NodeId, checked((uint)nodeB.Epoch), 1));
    }

    [Fact]
    public async Task Durable_push_materializes_metadata_without_mutating_node_outbox_before_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-materialization-isolated"), 433);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(34);
        using var receiver = NewNode(35);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "metadata-materialization-isolated";
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("34000000-0000-0000-0000-000000000034").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0102, AuthorNodeId = sender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3400, Value = "metadata-source"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        coordinator.RegisterInterestedPeer(initiator, 1);
        metadata.CompletePeerInitialSync(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(sender.NodeId, 0, checked((uint)sender.Epoch)) });
        var before = sender.ReadOutboxEntries(1, sender.NodeId, 0, 16)
            .Select(entry => (entry.Seq, entry.ChangeId, Payload: entry.Payload.ToArray())).ToArray();

        using var cts = new CancellationTokenSource();
        var receive = acceptor.ReceiveBatchAsync(receiver, ct: CancellationToken.None,
            beforeAck: (_, _) => throw new InvalidOperationException("stop before ACK"));
        var push = initiator.PushBatchAsync(sender, 1, metadata, scope, ct: cts.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await push);

        var after = sender.ReadOutboxEntries(1, sender.NodeId, 0, 16)
            .Select(entry => (entry.Seq, entry.ChangeId, Payload: entry.Payload.ToArray())).ToArray();
        Assert.Equal(before.Length, after.Length);
        for (int i = 0; i < before.Length; i++)
        {
            Assert.Equal(before[i].Seq, after[i].Seq);
            Assert.Equal(before[i].ChangeId, after[i].ChangeId);
            Assert.Equal(before[i].Payload, after[i].Payload);
        }
        var state = metadata.GetPeerStreamState(scope, sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(1UL, state.PushedCursor);
        Assert.Equal(0UL, state.AckedCursor);
        Assert.NotNull(state.PendingBatch);
    }

    [Fact]
    public async Task Durable_batch_reservation_replays_identically_after_sender_restart_and_lost_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-replay-after-restart"), 434);
        using var provider = new StorageDataProvider(storage);
        using var originalSender = NewNode(36);
        using var receiver = NewNode(37);
        var metadata = new StorageMetadataStore(storage);
        const string scope = "durable-replay-after-restart";
        var coordinator = new StorageSyncCoordinator(originalSender, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("36000000-0000-0000-0000-000000000036").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0103, AuthorNodeId = originalSender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3600, Value = "replay-exactly"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });

        MetadataOutboundBatchReservation firstReservation;
        var (firstToSender, firstToReceiver) = PipeTransport.CreatePair();
        var firstAcceptTask = SyncSession.AcceptAsync(receiver, firstToReceiver);
        var firstConnectTask = SyncSession.ConnectAsync(originalSender, firstToSender);
        await using (var firstAcceptor = await firstAcceptTask)
        await using (var firstInitiator = await firstConnectTask)
        {
            coordinator.RegisterInterestedPeer(firstInitiator, 1);
            metadata.CompletePeerInitialSync(scope, originalSender.NodeId, receiver.NodeId,
                checked((uint)receiver.Epoch), 1,
                new[] { new MetadataPeerStreamBaseline(originalSender.NodeId, 0, checked((uint)originalSender.Epoch)) });
            using var cts = new CancellationTokenSource();
            var receive = firstAcceptor.ReceiveBatchAsync(receiver,
                beforeAck: (_, _) => throw new IOException("simulated ACK loss"));
            var push = firstInitiator.PushBatchAsync(originalSender, 1, metadata, scope, ct: cts.Token);
            await Assert.ThrowsAsync<IOException>(async () => await receive);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await push);
            firstReservation = Assert.IsType<MetadataOutboundBatchReservation>(
                metadata.GetPendingOutboundBatch(scope, originalSender.NodeId,
                    receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId));
            Assert.Equal(1UL, metadata.GetPeerStreamState(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId).PushedCursor);
            Assert.Equal(0UL, metadata.GetPeerStreamState(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId).AckedCursor);
        }

        metadata.Dispose();
        using var reopenedMetadata = new StorageMetadataStore(storage);
        var replayReservation = Assert.IsType<MetadataOutboundBatchReservation>(
            reopenedMetadata.GetPendingOutboundBatch(scope, originalSender.NodeId,
                receiver.NodeId, checked((uint)receiver.Epoch), 1, originalSender.NodeId));
        Assert.Equal(firstReservation.BatchId, replayReservation.BatchId);
        Assert.Equal(firstReservation.CursorStart, replayReservation.CursorStart);
        Assert.Equal(firstReservation.CursorEnd, replayReservation.CursorEnd);
        Assert.Equal(firstReservation.PreviousBatchDigest, replayReservation.PreviousBatchDigest);
        Assert.Equal(firstReservation.BatchDigest, replayReservation.BatchDigest);
        Assert.Equal(firstReservation.Payload, replayReservation.Payload);

        using var restartedSender = new NodeBuilder()
            .WithNodeId(originalSender.NodeId.ToArray())
            .WithEpoch(originalSender.Epoch)
            .Build();
        var (secondToSender, secondToReceiver) = PipeTransport.CreatePair();
        var secondAcceptTask = SyncSession.AcceptAsync(receiver, secondToReceiver);
        var secondConnectTask = SyncSession.ConnectAsync(restartedSender, secondToSender);
        await using var secondAcceptor = await secondAcceptTask;
        await using var secondInitiator = await secondConnectTask;
        var secondReceive = secondAcceptor.ReceiveBatchAsync(receiver);
        Assert.True(await secondInitiator.PushBatchAsync(restartedSender, 1, reopenedMetadata, scope));
        Assert.Equal(0, (await secondReceive).Applied); // receiver already applied the first delivery before its ACK was lost

        var finalState = reopenedMetadata.GetPeerStreamState(scope, restartedSender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, restartedSender.NodeId);
        Assert.Equal(1UL, finalState.AckedCursor);
        Assert.Equal(1UL, finalState.PushedCursor);
        Assert.Null(finalState.PendingBatch);
        Assert.True(receiver.TryGetField(1, record, 0x0103, out var value));
        Assert.Equal("replay-exactly", Encoding.UTF8.GetString(value!.Value));
    }

    [Fact]
    public async Task Durable_push_requires_snapshot_when_first_retained_outbound_sequence_is_not_one()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-push-snapshot-barrier"), 432);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(32);
        using var receiver = NewNode(33);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        const string scope = "durable-push-snapshot-barrier";
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, scope);

        RecordId first = RecordId.From(Guid.Parse("32000000-0000-0000-0000-000000000032").ToByteArray());
        RecordId second = RecordId.From(Guid.Parse("33000000-0000-0000-0000-000000000033").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = first, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3200, Value = "first"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = second, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3300, Value = "second"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        ulong lease = metadata.AcquireOutboundLease(scope, sender.NodeId, 1, sender.NodeId,
            Guid.Parse("32000000-0000-0000-0000-000000000001"));
        Assert.Equal(2, metadata.ReadOutbound(scope, sender.NodeId, 1, sender.NodeId, lease, 0, 10).Items.Count);
        metadata.AcknowledgeOutbound(scope, sender.NodeId, 1, sender.NodeId, lease, 1);
        metadata.CompactOutboundThrough(scope, sender.NodeId, 1, sender.NodeId, 1);

        coordinator.RegisterInterestedPeer(initiator, 1);
        var error = await Assert.ThrowsAsync<SyncException>(
            () => initiator.PushBatchAsync(sender, 1, metadata, scope));
        Assert.Contains("SNAPSHOT_REQUIRED", error.Message);
        Assert.Null(metadata.GetPendingOutboundBatch(scope, sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, sender.NodeId, 1, sender.NodeId));
    }

    [Fact]
    public async Task Sync_session_uses_durable_peer_reservations_for_sequential_batches()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-peer-session"), 420);
        using var metadata = new StorageMetadataStore(storage);
        using var provider = new StorageDataProvider(storage);
        using var sender = NewNode(18);
        using var receiver = NewNode(19);
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        var coordinator = new StorageSyncCoordinator(sender, provider, 1, metadata, "durable-peer-session");

        RecordId firstRecord = RecordId.From(Guid.Parse("18000000-0000-0000-0000-000000000018").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 1000, HlcLogical = 0, Value = "first"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        coordinator.RegisterInterestedPeer(initiator, 1);
        metadata.CompletePeerInitialSync("durable-peer-session", sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1,
            new[] { new MetadataPeerStreamBaseline(sender.NodeId, 0, checked((uint)sender.Epoch)) });
        var firstReceive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, "durable-peer-session"));
        Assert.Equal(1, (await firstReceive).Applied);

        var firstState = metadata.GetPeerStreamState("durable-peer-session", sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(1UL, firstState.AckedCursor);
        Assert.Equal(1UL, firstState.PushedCursor);
        Assert.Equal((uint)sender.Epoch, firstState.IssuerEpoch);
        Assert.Null(firstState.PendingBatch);

        // The durable path must not create a second ACK/digest authority in Node.
        // The following push must rehydrate progress from Metadata Store and continue.
        var nodeMirror = sender.ReadPeerStreamProgress(receiver.NodeId, 1, sender.NodeId);
        Assert.Equal(0UL, nodeMirror.AckedCursor);
        Assert.Equal(0UL, nodeMirror.PushedCursor);

        RecordId secondRecord = RecordId.From(Guid.Parse("19000000-0000-0000-0000-000000000019").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord, FieldId = 0x0101, AuthorNodeId = sender.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 2000, HlcLogical = 0, Value = "second"u8.ToArray()
        });
        coordinator.PublishLocalChanges(_ => { });
        var secondReceive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, "durable-peer-session"));
        Assert.Equal(1, (await secondReceive).Applied);

        var secondState = metadata.GetPeerStreamState("durable-peer-session", sender.NodeId,
            receiver.NodeId, checked((uint)receiver.Epoch), 1, sender.NodeId);
        Assert.Equal(2UL, secondState.AckedCursor);
        Assert.Equal(2UL, secondState.PushedCursor);
        Assert.Null(secondState.PendingBatch);
        Assert.True(receiver.TryGetField(1, firstRecord, 0x0101, out var firstValue));
        Assert.Equal("first", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.True(receiver.TryGetField(1, secondRecord, 0x0101, out var secondValue));
        Assert.Equal("second", Encoding.UTF8.GetString(secondValue!.Value));
    }

    [Fact]
    public async Task Durable_push_selects_lowest_pending_sequence_across_author_streams()
    {
        const string scope = "durable-author-stream-fairness";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "durable-author-stream-fairness"), 472);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(18);
        using var origin = NewNode(20);
        using var receiver = NewNode(19);

        var localOperations = new List<MetadataFrozenOperation>();
        for (int i = 0; i < 5; i++)
        {
            RecordId record = RecordId.From(Guid.Parse($"18000000-0000-0000-0000-{(180 + i):D12}").ToByteArray());
            Assert.True(sender.WriteField(1, record, 0x0101,
                new[] { (byte)(0xA0 + i) }, new Hlc((ulong)(1000 + i), 0)));
            var entry = sender.Outbox[1][i];
            byte[] payload = new byte[16 + entry.Payload.Length];
            entry.RecordId.WriteTo(payload);
            entry.Payload.Span.CopyTo(payload.AsSpan(16));
            localOperations.Add(new MetadataFrozenOperation
            {
                ChangeId = entry.ChangeId,
                Fingerprint = entry.ChangeId.ToArray(),
                Payload = payload,
                OriginAuthorEpoch = entry.OriginAuthorEpoch,
                OriginSequence = entry.OriginSequence
            });
        }
        metadata.FreezeOutbound(scope, sender.NodeId, 1, sender.NodeId, localOperations);

        RecordId relayRecord = RecordId.From(Guid.Parse("20000000-0000-0000-0000-000000000020").ToByteArray());
        Assert.True(origin.WriteField(1, relayRecord, 0x0102, "relay-first"u8.ToArray(), new Hlc(2000, 0)));
        var originEntry = Assert.Single(origin.Outbox[1]);
        byte[] relayPayload = new byte[16 + originEntry.Payload.Length];
        originEntry.RecordId.WriteTo(relayPayload);
        originEntry.Payload.Span.CopyTo(relayPayload.AsSpan(16));
        metadata.AcceptIncomingBatch(new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = sender.NodeId,
            SenderPeer = origin.NodeId,
            SenderEpoch = checked((uint)origin.Epoch),
            AuthorStreamNodeId = origin.NodeId,
            NamespaceId = 1,
            BatchId = BatchId.From(Guid.Parse("20000000-0000-0000-0000-000000000021").ToByteArray()),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0xB2, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = origin.NodeId,
                    ChangeId = originEntry.ChangeId,
                    Fingerprint = originEntry.ChangeId.ToArray(),
                    Payload = relayPayload,
                    RelayEligible = true,
                    OriginAuthorEpoch = originEntry.OriginAuthorEpoch,
                    OriginSequence = originEntry.OriginSequence
                }
            }
        });

        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        // Learn a third-party author only after the sender/receiver TCP session is
        // established. The live ALIAS_MAP control update must reach the receiver
        // before the relay DELTA_BATCH is sent.
        Transport.MakeSession(sender, origin);

        metadata.RegisterInterestedPeer(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1);
        metadata.CompletePeerInitialSync(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, new[]
            {
                new MetadataPeerStreamBaseline(sender.NodeId, 0, checked((uint)sender.Epoch)),
                new MetadataPeerStreamBaseline(origin.NodeId, 0, checked((uint)origin.Epoch))
            });

        // Simulate a proven prior ACK of A's first four operations. Its next local
        // cursor is 5, while the newly available relay stream starts at cursor 1.
        var priorLocalReservation = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Guid.Parse("18000000-0000-0000-0000-000000000022").ToByteArray()),
            IssuerEpoch = checked((uint)sender.Epoch),
            CursorStart = 1,
            CursorEnd = 4,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0xA4, 16).ToArray(),
            Payload = new byte[] { 0x01 }
        };
        metadata.ReserveOutboundBatch(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, sender.NodeId, priorLocalReservation);
        metadata.MarkOutboundBatchPushed(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, sender.NodeId, priorLocalReservation.BatchId);
        metadata.AcknowledgeOutboundBatch(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, sender.NodeId, priorLocalReservation.BatchId,
            4, priorLocalReservation.BatchDigest);

        var receive = acceptor.ReceiveBatchAsync(receiver);
        Assert.True(await initiator.PushBatchAsync(sender, 1, metadata, scope));
        Assert.Equal(1, (await receive).Applied);

        var localProgress = metadata.GetPeerStreamState(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, sender.NodeId);
        var relayProgress = metadata.GetPeerStreamState(scope, sender.NodeId, receiver.NodeId,
            checked((uint)receiver.Epoch), 1, origin.NodeId);
        Assert.Equal(4UL, localProgress.AckedCursor);
        Assert.Equal(1UL, relayProgress.AckedCursor);
        Assert.True(receiver.TryGetField(1, relayRecord, 0x0102, out var received));
        Assert.Equal("relay-first", Encoding.UTF8.GetString(received!.Value));
    }

    [Fact]
    public void Failed_node_checkpoint_does_not_ack_feed_and_replay_does_not_duplicate_outbox()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 401, deleteOnDispose: true);
        Node node = NewNode(4);
        RecordId recordId = provider.ResolveRecordId(Guid.Parse("40000000-0000-0000-0000-000000000004").ToByteArray());
        ushort fieldId = provider.ResolveFieldId("value"u8.ToArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 1000,
            HlcLogical = 2,
            KeyEpoch = 0,
            Value = Encoding.UTF8.GetBytes("durable")
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1);

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => throw new IOException("simulated checkpoint failure")));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1, node.GetOutboxCount(1));

        int published = coordinator.PublishLocalChanges(_ => { });
        Assert.Equal(1, published);
        Assert.Equal(1, node.GetOutboxCount(1));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }

    [Fact]
    public void Unpublished_local_field_edits_are_coalesced_before_metadata_store_freeze()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce"), 410);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node node = NewNode(17);
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce");
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId record = RecordId.From(Guid.Parse("17000000-0000-0000-0000-000000000017").ToByteArray());
        const ushort fieldA = 0x0701;
        const ushort fieldB = 0x0702;

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 1000, HlcLogical = 0, Value = "first"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldB, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 1500, HlcLogical = 0, Value = "independent"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 2000, HlcLogical = 0, Value = "second"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = fieldA, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 3000, HlcLogical = 0, Value = "final"u8.ToArray()
        });

        int consumed = coordinator.PublishLocalChanges(_ => { }, limit: 16);

        Assert.Equal(4, consumed);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce", node.NodeId, 1, node.NodeId));
        Assert.True(provider.TryReadCurrent(record, fieldA, out var providerA));
        Assert.Equal("final", Encoding.UTF8.GetString(providerA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var providerB));
        Assert.Equal("independent", Encoding.UTF8.GetString(providerB!.Value));
        Assert.Empty(provider.ReadLocalChanges(0, 16).Changes);

        ulong lease = metadata.AcquireOutboundLease("pending-coalesce", node.NodeId, 1, node.NodeId,
            Guid.Parse("17000000-0000-0000-0000-000000000001"));
        var outbox = metadata.ReadOutbound("pending-coalesce", node.NodeId, 1, node.NodeId, lease, 0, 16).Items;
        Assert.Equal(2, outbox.Count);
        var values = outbox.Select(entry => Ops.DecodeOp(entry.Payload.AsSpan(16).ToArray()).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Contains("final", values);
        Assert.Contains("independent", values);
        Assert.DoesNotContain("first", values);
        Assert.DoesNotContain("second", values);
    }

    [Fact]
    public void Pending_local_edits_are_coalesced_across_provider_page_boundaries_in_metadata_store()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-coalesce-pages"), 423);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(24);
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-coalesce-pages");
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId record = RecordId.From(Guid.Parse("24000000-0000-0000-0000-000000000024").ToByteArray());
        const ushort fieldA = 0x0711;
        const ushort fieldB = 0x0712;

        void Append(ushort field, ulong wall, string value) => provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = field, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = wall, HlcLogical = 0, Value = Encoding.UTF8.GetBytes(value)
        });

        Append(fieldA, 1000, "a1");
        Append(fieldB, 2500, "b-final");
        Append(fieldA, 2000, "a2");
        Append(fieldA, 3000, "a-final");
        Append(fieldB, 2000, "b-old");

        Assert.Equal(5, coordinator.PublishLocalChanges(_ => { }, limit: 2));

        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark("pending-coalesce-pages", node.NodeId, 1, node.NodeId));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.True(provider.TryReadCurrent(record, fieldA, out var valueA));
        Assert.Equal("a-final", Encoding.UTF8.GetString(valueA!.Value));
        Assert.True(provider.TryReadCurrent(record, fieldB, out var valueB));
        Assert.Equal("b-final", Encoding.UTF8.GetString(valueB!.Value));

        ulong lease = metadata.AcquireOutboundLease("pending-coalesce-pages", node.NodeId, 1, node.NodeId,
            Guid.Parse("24000000-0000-0000-0000-000000000001"));
        var values = metadata.ReadOutbound("pending-coalesce-pages", node.NodeId, 1, node.NodeId, lease, 0, 10).Items
            .Select(entry => Ops.DecodeOp(entry.Payload.AsSpan(16).ToArray()).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Contains("a-final", values);
        Assert.Contains("b-final", values);
        Assert.DoesNotContain("a1", values);
        Assert.DoesNotContain("a2", values);
        Assert.DoesNotContain("b-old", values);
    }

    [Fact]
    public void Unsupported_superseded_local_change_on_an_earlier_page_is_not_hidden()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "pending-invalid-page"), 424);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(25);
        RecordId record = RecordId.From(Guid.Parse("25000000-0000-0000-0000-000000000025").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0711, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 1000, HlcLogical = 0, KeyEpoch = 1, Value = "unsupported"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0711, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 2000, HlcLogical = 0, KeyEpoch = 0, Value = "winner"u8.ToArray()
        });

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "pending-invalid-page");
        Assert.Throws<NotSupportedException>(() => coordinator.PublishLocalChanges(_ => { }, limit: 1));
        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(2, provider.ReadLocalChanges(0, 10).Changes.Count);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark("pending-invalid-page", node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public void Remote_node_materialization_is_durable_but_does_not_create_local_feed_entries()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 402, deleteOnDispose: true);
        Node remoteNode = NewNode(5);
        RecordId recordId = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000005").ToByteArray());
        const ushort fieldId = 0x010C;
        remoteNode.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("remote"), new Hlc(2000, 1));

        var coordinator = new StorageSyncCoordinator(remoteNode, provider, 1);
        var receipt = coordinator.ApplyRemoteFields(new[] { (recordId, fieldId) });

        Assert.NotNull(receipt);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("remote", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(remoteNode.NodeId, current.AuthorNodeId);
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }


    [Fact]
    public void Provider_commit_before_ack_survives_receiver_restart_and_duplicate_batch_retry()
    {
        string nodePath = Path.Combine(_root, "receiver.node");
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 403, deleteOnDispose: true);
        Node sender = NewNode(6);
        Node receiver = NewNode(7);
        RecordId recordId = RecordId.From(Guid.Parse("60000000-0000-0000-0000-000000000006").ToByteArray());
        const ushort fieldId = 0x0101;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("committed"), new Hlc(3000, 1));

        Transport.MakeSession(sender, receiver);
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);

        Assert.Throws<IOException>(() => Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, _) =>
            {
                // Receiver protocol receipt is durable before external provider apply.
                NodePersistence.Save(target, nodePath);
                coordinator.ApplyRemoteBatch(batch);
                // Simulate process loss after provider commit but before sender ACK.
                throw new IOException("simulated crash before ACK");
            }));

        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var committed));
        Assert.Equal("committed", Encoding.UTF8.GetString(committed!.Value));

        // Advance provider state after the original batch committed. Replaying the
        // old immutable batch must hit its original receipt and must not fingerprint
        // the provider's newer current winner as if it were the old batch payload.
        provider.ApplyRemoteBatch(Guid.Parse("00000000-0000-0000-0000-000000000403"),
            new[]
            {
                new ProviderMutation
                {
                    RecordId = recordId,
                    FieldId = fieldId,
                    HlcWall = 4000,
                    HlcLogical = 0,
                    AuthorNodeId = sender.NodeId,
                    Value = Encoding.UTF8.GetBytes("newer"),
                    ChangeId = BatchId.From(Guid.Parse("60000000-0000-0000-0000-000000000403").ToByteArray()),
                    OriginAuthorEpoch = checked((uint)sender.Epoch),
                    OriginSequence = 2
                }
            });

        receiver = NodePersistence.Load(nodePath);
        Transport.MakeSession(sender, receiver);
        coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var retry = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, result) =>
            {
                Assert.True(result.Duplicate);
                var receipt = coordinator.ApplyRemoteBatch(batch);
                Assert.True(receipt!.IsDuplicate);
                NodePersistence.Save(target, nodePath);
            });

        Assert.NotNull(retry);
        Assert.True(retry!.Value.Duplicate);
        Assert.Null(Transport.DeliverBatch(sender, receiver, 1));
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var latest));
        Assert.Equal("newer", Encoding.UTF8.GetString(latest!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }


    [Fact]
    public void Received_batch_materializes_multiple_immutable_updates_to_same_field()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "multi-update"), 404, deleteOnDispose: true);
        Node sender = NewNode(8);
        Node receiver = NewNode(9);
        RecordId recordId = RecordId.From(Guid.Parse("80000000-0000-0000-0000-000000000008").ToByteArray());
        const ushort fieldId = 0x0201;

        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("one"), new Hlc(1000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("two"), new Hlc(2000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("three"), new Hlc(3000, 0));

        Transport.MakeSession(sender, receiver);
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var delivered = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));

        Assert.NotNull(delivered);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("three", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }


    [Fact]
    public void Metadata_store_pending_apply_and_provider_commit_are_coordinated_before_ack()
    {
        string directory = Path.Combine(_root, "shared-metadata-provider");
        using var storage = StorageDatabase.Open(directory, 405);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node sender = NewNode(10);
        Node receiver = NewNode(11);
        RecordId recordId = RecordId.From(Guid.Parse("a0000000-0000-0000-0000-00000000000a").ToByteArray());
        const ushort fieldId = 0x0301;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("metadata-backed"), new Hlc(5000, 3));
        Transport.MakeSession(sender, receiver);

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, "metadata-integration");
        var delivered = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, _) =>
            {
                var receipt = coordinator.ApplyRemoteBatch(batch);
                Assert.NotNull(receipt);
                NodePersistence.Save(target, Path.Combine(_root, "metadata-receiver.node"));
            });

        Assert.NotNull(delivered);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("metadata-backed", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(1UL, metadata.GetReceiveCursor("metadata-integration", receiver.NodeId,
            sender.NodeId, checked((uint)sender.Epoch), 1));
        Assert.Equal(1UL, metadata.GetRelayHighWatermark("metadata-integration", receiver.NodeId,
            1, sender.NodeId));
        var relay = metadata.ReadRelay("metadata-integration", receiver.NodeId, 1, sender.NodeId, 0, 10);
        Assert.Single(relay.Items);
        Assert.Equal(recordId, RecordId.From(relay.Items[0].Payload.AsSpan(0, 16)));
        Assert.Empty(metadata.ReadPendingApplyIntents("metadata-integration", receiver.NodeId));
        Assert.Equal(0, coordinator.RecoverPendingApplies());

        // The legacy Node receive path created a duplicate relay Outbox projection.
        // Provider cutover must remove that cache copy while retaining the durable
        // Metadata Store relay payload as the sole delivery source.
        Assert.Single(receiver.ReadOutboxEntries(1, sender.NodeId, 0, 10));
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        Assert.Empty(receiver.ReadOutboxEntries(1, sender.NodeId, 0, 10));
        Assert.Equal(1L, receiver.GetOutboxHighWatermark(1, sender.NodeId));
        Assert.Single(metadata.ReadRelay("metadata-integration", receiver.NodeId, 1,
            sender.NodeId, 0, 10).Items);
    }


    [Fact]
    public void Metadata_receive_cursors_are_independent_for_two_author_streams_from_one_peer()
    {
        using var store = new StorageMetadataStore(Path.Combine(_root, "author-stream-cursors"), 406, deleteOnDispose: true);
        NodeId local = NodeId.From(Guid.Parse("b0000000-0000-0000-0000-00000000000b").ToByteArray());
        NodeId sender = NodeId.From(Guid.Parse("c0000000-0000-0000-0000-00000000000c").ToByteArray());
        NodeId authorA = NodeId.From(Guid.Parse("d0000000-0000-0000-0000-00000000000d").ToByteArray());
        NodeId authorB = NodeId.From(Guid.Parse("e0000000-0000-0000-0000-00000000000e").ToByteArray());

        MetadataIncomingBatch Create(NodeId author, byte batchSeed, ulong cursor, byte[] previous, byte[] digest)
            => new()
            {
                ScopeId = "author-stream-test",
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = 1,
                AuthorStreamNodeId = author,
                NamespaceId = 1,
                BatchId = BatchId.From(Guid.Parse($"00000000-0000-0000-0000-{batchSeed:x12}").ToByteArray()),
                CursorStart = cursor,
                CursorEnd = cursor,
                PreviousBatchDigest = previous,
                BatchDigest = digest,
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = author,
                        ChangeId = BatchId.From(Guid.Parse($"10000000-0000-0000-0000-{batchSeed:x12}").ToByteArray()),
                        Fingerprint = Enumerable.Repeat(batchSeed, 16).ToArray(),
                        Payload = new[] { batchSeed },
                        OriginAuthorEpoch = 1,
                        OriginSequence = cursor
                    }
                }
            };

        byte[] zero = new byte[16];
        byte[] digestA1 = Enumerable.Repeat((byte)0xA1, 16).ToArray();
        byte[] digestB1 = Enumerable.Repeat((byte)0xB1, 16).ToArray();
        store.AcceptIncomingBatch(Create(authorA, 1, 1, zero, digestA1));
        store.AcceptIncomingBatch(Create(authorB, 2, 1, digestA1, digestB1));

        byte[] digestA2 = Enumerable.Repeat((byte)0xA2, 16).ToArray();
        byte[] digestB2 = Enumerable.Repeat((byte)0xB2, 16).ToArray();
        store.AcceptIncomingBatch(Create(authorA, 3, 2, digestB1, digestA2));
        store.AcceptIncomingBatch(Create(authorB, 4, 2, digestA2, digestB2));

        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1, authorA));
        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1, authorB));
        Assert.Equal(2UL, store.GetReceiveCursor("author-stream-test", local, sender, 1, 1));
    }


    [Fact]
    public void Coordinator_recovers_persisted_metadata_intent_and_provider_apply_after_restart()
    {
        string directory = Path.Combine(_root, "pending-apply-recovery");
        Node local = NewNode(12);
        Node sender = NewNode(13);
        RecordId recordId = RecordId.From(Guid.Parse("c1000000-0000-0000-0000-00000000000c").ToByteArray());
        const ushort fieldId = 0x0401;
        byte[] value = Encoding.UTF8.GetBytes("recovered");
        var op = new OpFieldWrite
        {
            FieldId = fieldId,
            KeyEpoch = 0,
            Wall = 6000,
            Logical = 4,
            AuthorNodeId = sender.NodeId,
            Value = value
        };
        byte[] operationPayload = Ops.EncodeOpFull(op);
        byte[] envelope = new byte[16 + operationPayload.Length];
        recordId.WriteTo(envelope);
        operationPayload.CopyTo(envelope, 16);
        BatchId changeId = BatchId.From(Guid.Parse("c2000000-0000-0000-0000-00000000000c").ToByteArray());
        byte[] fingerprint = ChangeIdentity.Fingerprint(1, recordId, OpsConst.TagFieldWrite,
            op, sender.NodeId).ToArray();
        var recordTombstone = new OpRecordTombstone
        {
            RecordId = recordId,
            Wall = 7000,
            Logical = 5,
            AuthorNodeId = sender.NodeId
        };
        byte[] tombstonePayload = Ops.EncodeOpFull(recordTombstone);
        byte[] tombstoneEnvelope = new byte[16 + tombstonePayload.Length];
        recordId.WriteTo(tombstoneEnvelope);
        tombstonePayload.CopyTo(tombstoneEnvelope, 16);
        byte[] tombstoneFingerprint = ChangeIdentity.Fingerprint(1, recordId,
            OpsConst.TagRecordTomb, recordTombstone, sender.NodeId).ToArray();
        byte[] batchDigest = Enumerable.Repeat((byte)0xC3, 16).ToArray();
        Guid applyToken = Guid.Parse("c3000000-0000-0000-0000-00000000000c");

        using (var storage = StorageDatabase.Open(directory, 407))
        using (var metadata = new StorageMetadataStore(storage))
        {
            metadata.BeginPendingApply(new MetadataIncomingBatch
            {
                ScopeId = "pending-recovery",
                LocalReplica = local.NodeId,
                SenderPeer = sender.NodeId,
                SenderEpoch = checked((uint)sender.Epoch),
                AuthorStreamNodeId = sender.NodeId,
                NamespaceId = 1,
                BatchId = BatchId.From(Guid.Parse("c4000000-0000-0000-0000-00000000000c").ToByteArray()),
                CursorStart = 1,
                CursorEnd = 2,
                PreviousBatchDigest = new byte[16],
                BatchDigest = batchDigest,
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = sender.NodeId,
                        ChangeId = changeId,
                        Fingerprint = fingerprint,
                        Payload = envelope,
                        OriginAuthorEpoch = checked((uint)sender.Epoch),
                        OriginSequence = 1
                    },
                    new MetadataOperation
                    {
                        AuthorNodeId = sender.NodeId,
                        ChangeId = BatchId.From(Guid.Parse("c5000000-0000-0000-0000-00000000000c").ToByteArray()),
                        Fingerprint = tombstoneFingerprint,
                        Payload = tombstoneEnvelope,
                        OriginAuthorEpoch = checked((uint)sender.Epoch),
                        OriginSequence = 2
                    }
                }
            }, applyToken);
        }

        using (var storage = StorageDatabase.Open(directory, 407))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        {
            var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, "pending-recovery");
            Assert.Equal(1, coordinator.RecoverPendingApplies());
            Assert.Equal(0, coordinator.RecoverPendingApplies());
            Assert.False(provider.TryReadCurrent(recordId, fieldId, out _));
            var snapshot = provider.OpenSnapshot();
            try
            {
                var page = provider.ReadSnapshot(snapshot.Token, 0, 10);
                Assert.Contains(page.Entries, entry => entry.IsRecordTombstone);
                Assert.Contains(page.Entries, entry =>
                    !entry.IsRecordTombstone && Encoding.UTF8.GetString(entry.Value) == "recovered");
            }
            finally
            {
                provider.ReleaseSnapshot(snapshot.Token);
            }
            Assert.Equal(2UL, metadata.GetReceiveCursor("pending-recovery", local.NodeId,
                sender.NodeId, checked((uint)sender.Epoch), 1, sender.NodeId));
            Assert.Empty(metadata.ReadPendingApplyIntents("pending-recovery", local.NodeId));
        }
    }


    [Fact]
    public void Received_record_tombstone_and_newer_ghost_field_materialize_with_lifecycle_ordering()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "record-lifecycle"), 408, deleteOnDispose: true);
        Node sender = NewNode(14);
        Node receiver = NewNode(15);
        RecordId recordId = RecordId.From(Guid.Parse("d1000000-0000-0000-0000-000000000001").ToByteArray());
        const ushort fieldId = 0x0501;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("before"), new Hlc(1000, 0));
        Assert.True(sender.DeleteRecord(1, recordId, new Hlc(2000, 0)));
        Transport.MakeSession(sender, receiver);

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var deleted = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));
        Assert.NotNull(deleted);
        Assert.False(provider.TryReadCurrent(recordId, fieldId, out _));

        Assert.True(sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("ghost"), new Hlc(3000, 0)));
        var ghost = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));
        Assert.NotNull(ghost);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("ghost", Encoding.UTF8.GetString(current!.Value));
    }


    [Fact]
    public void Conflicting_metadata_freeze_does_not_ack_local_provider_feed()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "freeze-conflict"), 409);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        Node node = NewNode(16);
        NodeId foreignAuthor = NodeId.From(Guid.Parse("f3000000-0000-0000-0000-000000000003").ToByteArray());
        RecordId recordId = RecordId.From(Guid.Parse("f4000000-0000-0000-0000-000000000004").ToByteArray());
        const ushort fieldId = 0x0601;
        byte[] value = Encoding.UTF8.GetBytes("same-operation");

        Assert.True(node.WriteField(1, recordId, fieldId, value, new Hlc(8000, 1)));
        OutboxEntry localEntry = Assert.Single(node.ReadOutboxEntries(1, node.NodeId, 0, 10));

        metadata.AcceptIncomingBatch(new MetadataIncomingBatch
        {
            ScopeId = "freeze-conflict",
            LocalReplica = node.NodeId,
            SenderPeer = foreignAuthor,
            SenderEpoch = 1,
            AuthorStreamNodeId = foreignAuthor,
            NamespaceId = 1,
            BatchId = BatchId.From(Guid.Parse("f5000000-0000-0000-0000-000000000005").ToByteArray()),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0xF5, 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = foreignAuthor,
                    ChangeId = localEntry.ChangeId,
                    Fingerprint = Enumerable.Repeat((byte)0xA5, 16).ToArray(),
                    Payload = new byte[] { 0x01 },
                    OriginAuthorEpoch = localEntry.OriginAuthorEpoch,
                    OriginSequence = localEntry.OriginSequence
                }
            }
        });

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 8000,
            HlcLogical = 1,
            Value = value
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "freeze-conflict");

        Assert.Throws<InvalidDataException>(() =>
            coordinator.PublishLocalChanges(_ => { }));

        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark(
            "freeze-conflict", node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public void Provider_cutover_refuses_to_discard_unrepresented_legacy_Node_change_ledger()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-cutover-legacy-ledger"), 453);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(54);
        using var remote = NewNode(55);
        const string scope = "provider-cutover-legacy-ledger";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);

        RecordId legacyRecord = RecordId.From(Guid.Parse("54000000-0000-0000-0000-000000000054").ToByteArray());
        remote.WriteField(1, legacyRecord, 0x0817, "legacy-node-only"u8.ToArray(), new Hlc(5800, 0));
        Transport.MakeSession(remote, node);
        Assert.NotNull(Transport.DeliverBatch(remote, node, 1));
        Assert.True(node.TryGetField(1, legacyRecord, 0x0817, out _));
        int retainedLedgerCount = node.GlobalChangeLedgerCount;

        // Cutover must not silently discard a retained Node ChangeId identity that
        // has never been committed to the durable Metadata Store.
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.EnableMetadataStoreOwnedPublication(_ => { }));

        Assert.True(node.TryGetField(1, legacyRecord, 0x0817, out _));
        Assert.Equal(retainedLedgerCount, node.GlobalChangeLedgerCount);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.False(((IProviderPublicationMode)provider)
            .TryGetMetadataStoreOwnedPublicationCutover(out _));
    }

    [Fact]
    public void Incremental_provider_cache_preserves_field_and_record_tombstone_ordering()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "incremental-provider-tombstones"), 466);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(67);
        const string scope = "incremental-provider-tombstones";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId recordId = RecordId.From(Guid.Parse("67000000-0000-0000-0000-000000000001").ToByteArray());

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = 0x0831, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 7200, Value = "old"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = 0x0831, IsTombstone = true,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 7300, Value = Array.Empty<byte>()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = Model.LifecycleFieldId, IsRecordTombstone = true,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 7400, Value = Array.Empty<byte>()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = 0x0832, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 7500, Value = "ghost"u8.ToArray()
        });

        Assert.Equal(4, coordinator.PublishLocalChanges(_ => { }));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(3UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.True(node.TryGetRecord(1, recordId, out var restored));
        Assert.True(restored!.IsDead);
        Assert.True(restored.FieldTombs.ContainsKey(0x0831));
        var visible = restored.VisibleFields();
        Assert.False(visible.ContainsKey(0x0831));
        Assert.True(visible.TryGetValue(0x0832, out var ghost));
        Assert.Equal("ghost", Encoding.UTF8.GetString(ghost!.Value));
    }

    [Fact]
    public void Incremental_provider_cache_coalesces_repeated_field_edits_by_maximum_HLC()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "incremental-provider-hlc-coalesce"), 467);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(68);
        const string scope = "incremental-provider-hlc-coalesce";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId recordId = RecordId.From(Guid.Parse("68000000-0000-0000-0000-000000000001").ToByteArray());

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = 0x0831, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 8200, Value = "maximum-HLC"u8.ToArray()
        });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId, FieldId = 0x0831, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 8100, Value = "later-feed-but-older-HLC"u8.ToArray()
        });

        Assert.Equal(2, coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.True(provider.TryReadCurrent(recordId, 0x0831, out var providerValue));
        Assert.Equal("maximum-HLC", Encoding.UTF8.GetString(providerValue!.Value));
        Assert.True(node.TryGetField(1, recordId, 0x0831, out var nodeValue));
        Assert.Equal("maximum-HLC", Encoding.UTF8.GetString(nodeValue!.Value));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }

    [Fact]
    public void Malformed_provider_feed_is_rejected_before_metadata_freeze()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "malformed-provider-feed"), 465);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(66);
        const string scope = "malformed-provider-feed";
        var provider = new FailOnceAcknowledgeProvider(innerProvider, change =>
        {
            ProviderMutation source = change.Mutation;
            return change with
            {
                Mutation = new ProviderMutation
                {
                    RecordId = source.RecordId,
                    FieldId = 0,
                    IsTombstone = false,
                    IsRecordTombstone = false,
                    HlcWall = source.HlcWall,
                    HlcLogical = source.HlcLogical,
                    AuthorNodeId = source.AuthorNodeId,
                    KeyEpoch = source.KeyEpoch,
                    Value = source.Value
                }
            };
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        RecordId record = RecordId.From(Guid.Parse("66000000-0000-0000-0000-000000000001").ToByteArray());
        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record,
            FieldId = 0x0830,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 7100,
            Value = "must-not-freeze"u8.ToArray()
        });

        Assert.Throws<InvalidDataException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(0UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.False(node.TryGetField(1, record, 0x0830, out _));
    }

    [Fact]
    public void Metadata_owned_freeze_retries_after_provider_ack_failure_with_new_feed_entries()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-owned-ack-retry"), 457);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(58);
        const string scope = "metadata-owned-ack-retry";
        var faultingProvider = new FailOnceAcknowledgeProvider(innerProvider);
        var coordinator = new StorageSyncCoordinator(node, faultingProvider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        int snapshotsAtCutover = faultingProvider.SnapshotOpenCount;
        RecordId firstRecord = RecordId.From(Guid.Parse("58000000-0000-0000-0000-000000000058").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000059").ToByteArray());

        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord,
            FieldId = 0x081B,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6100,
            Value = "first"u8.ToArray()
        });
        BatchId firstChangeId = Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes).ChangeId;

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(snapshotsAtCutover, faultingProvider.SnapshotOpenCount);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.True(node.TryGetField(1, firstRecord, 0x081B, out var firstAfterAckFailure));
        Assert.Equal("first", Encoding.UTF8.GetString(firstAfterAckFailure!.Value));
        Record firstRecordCache = node.Storage[1][firstRecord];

        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord,
            FieldId = 0x081C,
            AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6200,
            Value = "second"u8.ToArray()
        });
        ProviderChange[] pending = innerProvider.ReadLocalChanges(0, 10).Changes.ToArray();
        Assert.Equal(2, pending.Length);
        BatchId secondChangeId = pending[1].ChangeId;

        Assert.Equal(2, coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(snapshotsAtCutover, faultingProvider.SnapshotOpenCount);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Empty(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
        Assert.True(node.TryGetField(1, firstRecord, 0x081B, out var firstValue));
        Assert.Equal("first", Encoding.UTF8.GetString(firstValue!.Value));
        Assert.Same(firstRecordCache, node.Storage[1][firstRecord]);
        Assert.True(node.TryGetField(1, secondRecord, 0x081C, out var secondValue));
        Assert.Equal("second", Encoding.UTF8.GetString(secondValue!.Value));

        ulong lease = metadata.AcquireOutboundLease(scope, node.NodeId, 1, node.NodeId,
            Guid.Parse("58000000-0000-0000-0000-000000000001"));
        var outbound = metadata.ReadOutbound(scope, node.NodeId, 1, node.NodeId, lease, 0, 10).Items;
        Assert.Equal(new ulong[] { 1, 2 }, outbound.Select(item => item.Sequence));
        Assert.Equal(firstChangeId, outbound[0].ChangeId);
        Assert.Equal(secondChangeId, outbound[1].ChangeId);
    }

    [Fact]
    public void Provider_authoritative_recovery_processes_only_its_namespace_pending_intents()
    {
        using var sharedStorage = StorageDatabase.Open(Path.Combine(_root, "multi-namespace-shared"), 454);
        using var metadata = new StorageMetadataStore(sharedStorage);
        using var provider = new StorageDataProvider(sharedStorage);
        using var node = NewNode(56);
        using var sender = NewNode(57);
        const string scope = "multi-namespace-provider-recovery";

        RecordId recordOne = RecordId.From(Guid.Parse("56000000-0000-0000-0000-000000000056").ToByteArray());
        RecordId recordTwo = RecordId.From(Guid.Parse("57000000-0000-0000-0000-000000000057").ToByteArray());
        MetadataIncomingBatch batchOne = CreatePendingProviderBatch(
            scope, node, sender, 1, recordOne, 0x0819, "namespace-one", 0xA1);
        MetadataIncomingBatch batchTwo = CreatePendingProviderBatch(
            scope, node, sender, 2, recordTwo, 0x081A, "namespace-two", 0xA2);
        metadata.BeginPendingApply(batchTwo, Guid.NewGuid());

        var coordinatorOne = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        Assert.Equal(0, coordinatorOne.RehydrateNodeFromProviderSnapshot(_ => { }));
        MetadataPendingApplyIntent foreign = Assert.Single(
            metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.Equal(2, foreign.Batch.NamespaceId);

        metadata.BeginPendingApply(batchOne, Guid.NewGuid());
        Assert.Throws<InvalidOperationException>(() =>
            coordinatorOne.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.Equal(2, metadata.ReadPendingApplyIntents(scope, node.NodeId).Count);
        Assert.Equal(1, coordinatorOne.RecoverPendingApplies());
        MetadataPendingApplyIntent remaining = Assert.Single(
            metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.Equal(2, remaining.Batch.NamespaceId);
        Assert.True(provider.TryReadCurrent(recordOne, 0x0819, out var valueOne));
        Assert.Equal("namespace-one", Encoding.UTF8.GetString(valueOne!.Value));
        Assert.False(provider.TryReadCurrent(recordTwo, 0x081A, out _));

        var coordinatorTwo = new StorageSyncCoordinator(node, provider, 2, metadata, scope);
        Assert.Equal(1, coordinatorTwo.RecoverPendingApplies());
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, node.NodeId));
        Assert.True(provider.TryReadCurrent(recordTwo, 0x081A, out var valueTwo));
        Assert.Equal("namespace-two", Encoding.UTF8.GetString(valueTwo!.Value));
    }

    [Fact]
    public void Missing_provider_cutover_marker_refuses_legacy_publication_when_metadata_is_ahead()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "missing-provider-cutover-marker"), 462);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(62);
        const string scope = "missing-provider-cutover-marker";

        metadata.FreezeOutbound(scope, node.NodeId, 1, node.NodeId, new[]
        {
            new MetadataFrozenOperation
            {
                ChangeId = BatchId.From(Guid.Parse("62000000-0000-0000-0000-000000000062").ToByteArray()),
                Fingerprint = new byte[16],
                Payload = new byte[] { 1, 2, 3 },
                OriginAuthorEpoch = 1,
                OriginSequence = 1
            }
        });

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        Assert.Throws<InvalidOperationException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.False(((IProviderPublicationMode)provider)
            .TryGetMetadataStoreOwnedPublicationCutover(out _));
        Assert.Equal(0L, node.GetOutboxHighWatermark(1, node.NodeId));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
    }

    [Fact]
    public void Provider_authoritative_rehydration_recovers_checkpoint_failure_after_feed_ack()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "provider-rehydrate-after-ack"), 461);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var originalNode = NewNode(61);
        const string scope = "provider-rehydrate-after-ack";
        var originalCoordinator = new StorageSyncCoordinator(originalNode, provider, 1, metadata, scope);
        originalCoordinator.EnableMetadataStoreOwnedPublication(_ => { });

        RecordId record = RecordId.From(Guid.Parse("61000000-0000-0000-0000-000000000001").ToByteArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0821, AuthorNodeId = originalNode.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6800, Value = "durable-provider-value"u8.ToArray()
        });
        ProviderChange source = Assert.Single(provider.ReadLocalChanges(0, 10).Changes);

        // The immutable Metadata Store freeze commits, but the Node cache checkpoint
        // fails before provider ACK. The feed must remain replayable across restart.
        Assert.Throws<IOException>(() => originalCoordinator.PublishLocalChanges(
            _ => throw new IOException("Injected Node incremental checkpoint failure.")));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, originalNode.NodeId, 1, originalNode.NodeId));

        using var restartedNode = NewNode(61);
        var restartedCoordinator = new StorageSyncCoordinator(restartedNode, provider, 1, metadata, scope);
        Assert.Equal(0, restartedCoordinator.RehydrateNodeFromProviderSnapshot(_ => { }));
        Assert.True(restartedNode.TryGetField(1, record, 0x0821, out var restored));
        Assert.Equal("durable-provider-value", Encoding.UTF8.GetString(restored!.Value));
        Assert.Empty(restartedNode.ReadOutboxEntries(1, restartedNode.NodeId, 0, 10));
        // Rehydration first retries the already-frozen feed incrementally, checkpoints
        // the Node cache, ACKs the provider feed, then performs the full startup snapshot.
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, originalNode.NodeId, 1, originalNode.NodeId));

        ulong lease = metadata.AcquireOutboundLease(scope, originalNode.NodeId, 1, originalNode.NodeId,
            Guid.Parse("61000000-0000-0000-0000-000000000061"));
        var outbound = metadata.ReadOutbound(scope, originalNode.NodeId, 1, originalNode.NodeId, lease, 0, 10).Items;
        Assert.Single(outbound);
        Assert.Equal(source.ChangeId, outbound[0].ChangeId);
    }

    [Fact]
    public void Metadata_owned_publication_retry_preserves_frozen_change_when_newer_edit_arrives()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-owned-append-after-freeze"), 460);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(60);
        var provider = new FailOnceAcknowledgeProvider(innerProvider);
        const string scope = "metadata-owned-append-after-freeze";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("60000000-0000-0000-0000-000000000001").ToByteArray());

        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0820, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6600, Value = "first-frozen-edit"u8.ToArray()
        });
        ProviderChange first = Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes);

        // Metadata freeze commits, but the provider feed ACK is interrupted.
        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));

        // A newer edit arrives while the original feed row is still pending.
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0820, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6700, Value = "second-edit"u8.ToArray()
        });
        var pending = innerProvider.ReadLocalChanges(0, 10).Changes;
        Assert.Equal(2, pending.Count);
        ProviderChange second = pending[1];

        // Retry must retain the immutable first operation and append only the new
        // suffix; it must not coalesce the old frozen ChangeId out of the retry.
        Assert.Equal(2, coordinator.PublishLocalChanges(_ => { }));
        Assert.Empty(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Equal(0L, node.GetOutboxHighWatermark(1, node.NodeId));

        ulong lease = metadata.AcquireOutboundLease(scope, node.NodeId, 1, node.NodeId,
            Guid.Parse("60000000-0000-0000-0000-000000000060"));
        var outbound = metadata.ReadOutbound(scope, node.NodeId, 1, node.NodeId, lease, 0, 10).Items;
        Assert.Equal(2, outbound.Count);
        Assert.Equal(first.ChangeId, outbound[0].ChangeId);
        Assert.Equal(second.ChangeId, outbound[1].ChangeId);
    }

    [Fact]
    public async Task Application_commit_guard_orders_host_and_provider_leases_and_blocks_concurrent_provider_writes()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "commit-guard-order"), 477);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(77);
        var events = new List<string>();
        var provider = new FailOnceAcknowledgeProvider(innerProvider, commitGuardEvent: events.Add);
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, "commit-guard-order");

        Assert.Throws<InvalidOperationException>(() =>
            coordinator.AcquireApplicationCommitGuard(() =>
            {
                events.Add("host.acquire.null");
                return null;
            }));
        Assert.Equal(new[] { "host.acquire.null" }, events);

        events.Clear();
        using IDisposable lease = coordinator.AcquireApplicationCommitGuard(() =>
        {
            events.Add("host.acquire");
            return new CallbackLease(() => events.Add("host.release"));
        });
        Assert.Equal(new[] { "host.acquire", "provider.acquire" }, events);

        using var writerStarted = new ManualResetEventSlim();
        using var writerCompleted = new ManualResetEventSlim();
        Task writer = Task.Run(() =>
        {
            writerStarted.Set();
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(Guid.Parse("77000000-0000-0000-0000-000000000001").ToByteArray()),
                FieldId = 0x0877,
                AuthorNodeId = node.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 7700,
                Value = "blocked-until-lease-release"u8.ToArray()
            });
            writerCompleted.Set();
        });

        Assert.True(writerStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.False(writerCompleted.Wait(TimeSpan.FromMilliseconds(150)));
        lease.Dispose();
        Assert.True(writerCompleted.Wait(TimeSpan.FromSeconds(5)));
        await writer.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(
            new[] { "host.acquire", "provider.acquire", "provider.release", "host.release" },
            events);
        Assert.True(innerProvider.TryReadCurrent(
            RecordId.From(Guid.Parse("77000000-0000-0000-0000-000000000001").ToByteArray()),
            0x0877, out var current));
        Assert.Equal("blocked-until-lease-release", Encoding.UTF8.GetString(current!.Value));
    }

    [Theory]
    [InlineData("duplicate")]
    [InlineData("regressing")]
    [InlineData("out-of-range")]
    public void Change_id_gc_provider_scan_rejects_malformed_feed_sequences(string fault)
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "change-id-gc-malformed-" + fault), 498);
        using var innerProvider = new StorageDataProvider(storage);
        for (int i = 1; i <= 2; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(800 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08B2,
                AuthorNodeId = NodeId.From(Enumerable.Repeat((byte)0xB2, 16).ToArray()),
                OriginAuthorEpoch = 1,
                HlcWall = 18000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (_, _, page) => fault switch
            {
                "duplicate" => new ProviderChangePage(
                    new[] { page.Changes[0], page.Changes[0] }, page.ScannedThroughSequence),
                "regressing" => new ProviderChangePage(
                    page.Changes.Reverse().ToArray(), page.ScannedThroughSequence),
                "out-of-range" => new ProviderChangePage(
                    new[] { page.Changes[0] with { Sequence = page.ScannedThroughSequence + 1 } },
                    page.ScannedThroughSequence),
                _ => page
            });

        Assert.Throws<InvalidDataException>(() =>
            StorageMetadataStore.TryCollectProviderChangeIdsForGc(
                provider, provider.LocalChangeHighWatermark, out _));
    }

    [Fact]
    public void Change_id_gc_provider_scan_keeps_later_live_ids_after_empty_advancing_page()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "change-id-gc-empty-advancing-page"), 499);
        using var innerProvider = new StorageDataProvider(storage);
        for (int i = 1; i <= 2; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(900 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08B3,
                AuthorNodeId = NodeId.From(Enumerable.Repeat((byte)0xB3, 16).ToArray()),
                OriginAuthorEpoch = 1,
                HlcWall = 19000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var scannedCursors = new List<ulong>();
        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, _, page) =>
            {
                scannedCursors.Add(afterSequence);
                return afterSequence == 0
                    ? new ProviderChangePage(Array.Empty<ProviderChange>(), page.ScannedThroughSequence - 1)
                    : page;
            });

        Assert.True(StorageMetadataStore.TryCollectProviderChangeIdsForGc(
            provider, provider.LocalChangeHighWatermark, out HashSet<BatchId> liveChangeIds));
        Assert.Equal(new ulong[] { 0, 1 }, scannedCursors);
        Assert.Single(liveChangeIds);
        Assert.Equal(innerProvider.ReadLocalChanges(1, 10).Changes[0].ChangeId,
            Assert.Single(liveChangeIds));
    }

    [Fact]
    public void Change_id_gc_provider_scan_blocks_reclamation_for_legacy_unknown_change_id()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "change-id-gc-legacy-unknown-id"), 500);
        using var innerProvider = new StorageDataProvider(storage);
        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000001").ToByteArray()),
            FieldId = 0x08B4,
            AuthorNodeId = NodeId.From(Enumerable.Repeat((byte)0xB4, 16).ToArray()),
            OriginAuthorEpoch = 1,
            HlcWall = 20000,
            Value = "legacy-unknown-id"u8.ToArray()
        });
        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (_, _, page) => new ProviderChangePage(
                new[] { page.Changes[0] with { ChangeId = default } }, page.ScannedThroughSequence));

        Assert.False(StorageMetadataStore.TryCollectProviderChangeIdsForGc(
            provider, provider.LocalChangeHighWatermark, out HashSet<BatchId> liveChangeIds));
        Assert.Empty(liveChangeIds);
    }

    [Fact]
    public async Task Composed_gc_barrier_serializes_receive_provider_host_and_all_three_gc_callbacks()
    {
        const string scope = "composed-gc-barrier-concurrency";
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 487);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(98);
        using var sender = NewNode(99);
        using var hostGate = new SemaphoreSlim(1, 1);
        var gcStore = new StorageNamespaceGcStore(storage, scope, node.NodeId);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        var scanner = new ThrowingGcReferenceScanner();

        RecordId receiveRecord = RecordId.From(Guid.Parse("98000000-0000-0000-0000-000000000001").ToByteArray());
        Assert.True(sender.WriteField(1, receiveRecord, 0x0898, "receive-after-release"u8.ToArray(), new Hlc(9800, 0)));
        Transport.MakeSession(sender, node);

        int hostLeaseAcquires = 0;
        int hostLeaseReleases = 0;
        Func<IDisposable?> acquireComposedLease = () =>
            coordinator.AcquireApplicationCommitGuard(() =>
            {
                if (!hostGate.Wait(TimeSpan.FromSeconds(10)))
                    return null;
                Interlocked.Increment(ref hostLeaseAcquires);
                return new CallbackLease(() =>
                {
                    Interlocked.Increment(ref hostLeaseReleases);
                    hostGate.Release();
                });
            });

        using IDisposable outerLease = coordinator.AcquireApplicationCommitGuard(() =>
        {
            Assert.True(hostGate.Wait(TimeSpan.FromSeconds(5)));
            Interlocked.Increment(ref hostLeaseAcquires);
            return new CallbackLease(() =>
            {
                Interlocked.Increment(ref hostLeaseReleases);
                hostGate.Release();
            });
        });

        using var providerStarted = new ManualResetEventSlim();
        using var providerCompleted = new ManualResetEventSlim();
        Task providerCommit = Task.Run(() =>
        {
            providerStarted.Set();
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(Guid.Parse("98000000-0000-0000-0000-000000000002").ToByteArray()),
                FieldId = 0x0899,
                AuthorNodeId = node.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 9801,
                Value = "provider-commit"u8.ToArray()
            });
            providerCompleted.Set();
        });

        using var receiveStarted = new ManualResetEventSlim();
        using var receiveCompleted = new ManualResetEventSlim();
        Task<BatchResult?> receive = Task.Run(() =>
        {
            receiveStarted.Set();
            BatchResult? result = Transport.DeliverBatch(sender, node, 1);
            receiveCompleted.Set();
            return result;
        });

        using var hostStarted = new ManualResetEventSlim();
        using var hostCompleted = new ManualResetEventSlim();
        Task hostCommit = Task.Run(() =>
        {
            hostStarted.Set();
            hostGate.Wait();
            try { hostCompleted.Set(); }
            finally { hostGate.Release(); }
        });

        using var gcChangeStarted = new ManualResetEventSlim();
        using var gcReceiptStarted = new ManualResetEventSlim();
        using var gcTombstoneStarted = new ManualResetEventSlim();
        Task<int> gcChange = Task.Run(() =>
        {
            gcChangeStarted.Set();
            return coordinator.CompactChangeLedger(1, 16, acquireComposedLease, scanner);
        });
        Task<int> gcReceipt = Task.Run(() =>
        {
            gcReceiptStarted.Set();
            return coordinator.CompactReceipts(1, 16, acquireComposedLease, scanner);
        });
        Task<int> gcTombstone = Task.Run(() =>
        {
            gcTombstoneStarted.Set();
            return coordinator.CompactRecordTombstones(1, 16, acquireComposedLease, scanner);
        });

        Assert.True(providerStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(receiveStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(hostStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(gcChangeStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(gcReceiptStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(gcTombstoneStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.False(providerCompleted.Wait(TimeSpan.FromMilliseconds(150)));
        Assert.False(receiveCompleted.Wait(TimeSpan.FromMilliseconds(150)));
        Assert.False(hostCompleted.Wait(TimeSpan.FromMilliseconds(150)));
        Assert.False(gcChange.IsCompleted);
        Assert.False(gcReceipt.IsCompleted);
        // Tombstone GC is hard-disabled before lease acquisition; it must return
        // without touching host references or waiting on the composed barrier.
        Task tombstoneCompletion = await Task.WhenAny(gcTombstone, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(gcTombstone, tombstoneCompletion);
        Assert.Equal(0, await gcTombstone);

        outerLease.Dispose();
        await Task.WhenAll(providerCommit, hostCommit, gcChange, gcReceipt)
            .WaitAsync(TimeSpan.FromSeconds(15));
        Assert.NotNull(await receive.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Equal(0, await gcChange);
        Assert.Equal(0, await gcReceipt);
        Assert.Equal(0, await gcTombstone);
        Assert.Equal(3, hostLeaseAcquires);
        Assert.Equal(3, hostLeaseReleases);
        Assert.False(scanner.Opened); // All physical-GC proof switches remain disabled.

        Assert.Throws<InvalidOperationException>(() =>
            coordinator.CompactChangeLedger(1, 16, () => null, scanner));
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.CompactReceipts(1, 16, () => null, scanner));
        int tombstoneLeaseRequests = 0;
        Assert.Equal(0, coordinator.CompactRecordTombstones(1, 16, () =>
        {
            Interlocked.Increment(ref tombstoneLeaseRequests);
            return null;
        }, scanner));
        Assert.Equal(0, tombstoneLeaseRequests); // Hard-disabled before any lease or scan.

        Assert.True(node.TryGetField(1, receiveRecord, 0x0898, out var received));
        Assert.Equal("receive-after-release", Encoding.UTF8.GetString(received!.Value));
        Assert.True(provider.TryReadCurrent(
            RecordId.From(Guid.Parse("98000000-0000-0000-0000-000000000002").ToByteArray()),
            0x0899, out var committed));
        Assert.Equal("provider-commit", Encoding.UTF8.GetString(committed!.Value));
    }

    [Fact]
    public void Applied_origin_frontier_uses_one_non_reentrant_host_lease_and_releases_on_scan_failure()
    {
        const string scope = "frontier-non-reentrant-host-lease";
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 488);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(100);
        using var hostGate = new SemaphoreSlim(1, 1);
        var trust = new TrustStore();
        var (memberSeed, memberPublicKey) = BatchSignature.GenerateKeyPair();
        trust.Allow(local.NodeId, memberPublicKey, TrustMode.PreShared);

        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = local.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = memberPublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, memberSeed), trust);

        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        int hostAcquires = 0;
        int hostReleases = 0;
        Func<IDisposable?> acquireHostLease = () =>
        {
            if (!hostGate.Wait(0))
                throw new InvalidOperationException("The host commit gate is non-reentrant and was already held.");
            Interlocked.Increment(ref hostAcquires);
            return new CallbackLease(() =>
            {
                Interlocked.Increment(ref hostReleases);
                hostGate.Release();
            });
        };

        var scanner = new NonReentrantGcReferenceScanner(hostGate);
        AppliedOriginFrontierReport report = coordinator.PublishAppliedOriginFrontier(
            memberSeed, scanner, acquireHostLease);
        Assert.NotNull(report);
        Assert.Equal(1, hostAcquires);
        Assert.Equal(1, hostReleases);
        Assert.Equal(1, scanner.OpenCount);
        Assert.Equal(1, scanner.DisposeCount);
        Assert.Equal(1, hostGate.CurrentCount);

        var throwingScanner = new ThrowingGcReferenceScanner();
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.PublishAppliedOriginFrontier(memberSeed, throwingScanner, acquireHostLease));
        Assert.True(throwingScanner.Opened);
        Assert.Equal(2, hostAcquires);
        Assert.Equal(2, hostReleases);
        Assert.Equal(1, hostGate.CurrentCount);

        // A subsequent acquisition proves scanner failure released the coordinator,
        // Node, provider and host gates in the required reverse order.
        using (coordinator.AcquireApplicationCommitGuard(acquireHostLease))
            Assert.Equal(0, hostGate.CurrentCount);
        Assert.Equal(3, hostAcquires);
        Assert.Equal(3, hostReleases);
        Assert.Equal(1, hostGate.CurrentCount);
    }

    [Fact]
    public void Applied_origin_frontier_scans_empty_advancing_page_before_later_provider_drafts()
    {
        const string scope = "frontier-empty-advancing-provider-page";
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 489);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(101);
        using var hostGate = new SemaphoreSlim(1, 1);
        var trust = new TrustStore();
        var (memberSeed, memberPublicKey) = BatchSignature.GenerateKeyPair();
        trust.Allow(local.NodeId, memberPublicKey, TrustMode.PreShared);
        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = local.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = memberPublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, memberSeed), trust);

        for (int i = 1; i <= 3; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08A1,
                AuthorNodeId = local.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 10000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, _, page) => afterSequence == 0
                ? new ProviderChangePage(Array.Empty<ProviderChange>(), page.ScannedThroughSequence)
                : page);
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        var scanner = new NonReentrantGcReferenceScanner(hostGate);
        Func<IDisposable?> acquireHostLease = () =>
        {
            if (!hostGate.Wait(0))
                throw new InvalidOperationException("Frontier publication attempted to reacquire the non-reentrant host lease.");
            return new CallbackLease(() => hostGate.Release());
        };

        AppliedOriginFrontierReport report = coordinator.PublishAppliedOriginFrontier(
            memberSeed, scanner, acquireHostLease, pageSize: 1);

        // Sequence 1 is represented by an empty but advancing lifecycle page;
        // the later pages still contain two unpublished drafts and must be scanned.
        Assert.Equal(2UL, report.UnpublishedDraftCount);
        Assert.Equal(10003UL, report.MaxUnpublishedDraftHlcWall);
        Assert.Equal(3UL, provider.LocalChangeHighWatermark);
        Assert.Equal(3, innerProvider.ReadLocalChanges(0, 10).Changes.Count);
        Assert.Equal(1, scanner.OpenCount);
        Assert.Equal(1, scanner.DisposeCount);
        Assert.Equal(1, hostGate.CurrentCount);
    }

    [Theory]
    [InlineData(false, 0UL)]
    [InlineData(true, 1UL)]
    public void Applied_origin_frontier_scans_multiple_empty_advancing_pages_before_zero_or_later_drafts(
        bool includeFinalDraft, ulong expectedDraftCount)
    {
        string scope = "frontier-multiple-empty-pages-" + (includeFinalDraft ? "draft" : "zero");
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 491);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(103);
        using var hostGate = new SemaphoreSlim(1, 1);
        var trust = new TrustStore();
        var (memberSeed, memberPublicKey) = BatchSignature.GenerateKeyPair();
        trust.Allow(local.NodeId, memberPublicKey, TrustMode.PreShared);
        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = local.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = memberPublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, memberSeed), trust);
        for (int i = 1; i <= 3; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(200 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08A3,
                AuthorNodeId = local.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 12000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var scannedCursors = new List<ulong>();
        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, _, page) =>
            {
                scannedCursors.Add(afterSequence);
                if (afterSequence < 2)
                    return new ProviderChangePage(Array.Empty<ProviderChange>(), afterSequence + 1);
                return includeFinalDraft
                    ? page
                    : new ProviderChangePage(Array.Empty<ProviderChange>(), page.ScannedThroughSequence);
            });
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        var scanner = new NonReentrantGcReferenceScanner(hostGate);
        Func<IDisposable?> acquireHostLease = () =>
        {
            if (!hostGate.Wait(0))
                throw new InvalidOperationException("Frontier publication attempted to reacquire the non-reentrant host lease.");
            return new CallbackLease(() => hostGate.Release());
        };

        AppliedOriginFrontierReport report = coordinator.PublishAppliedOriginFrontier(
            memberSeed, scanner, acquireHostLease, pageSize: 1);

        Assert.Equal(new ulong[] { 0, 1, 2 }, scannedCursors);
        Assert.Equal(expectedDraftCount, report.UnpublishedDraftCount);
        Assert.Equal(includeFinalDraft ? 12003UL : 0UL, report.MaxUnpublishedDraftHlcWall);
        Assert.Equal(1, scanner.OpenCount);
        Assert.Equal(1, scanner.DisposeCount);
        Assert.Equal(1, hostGate.CurrentCount);
    }

    [Fact]
    public void Applied_origin_frontier_continues_after_short_advancing_page()
    {
        const string scope = "frontier-short-advancing-provider-page";
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 492);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(104);
        using var hostGate = new SemaphoreSlim(1, 1);
        var trust = new TrustStore();
        var (memberSeed, memberPublicKey) = BatchSignature.GenerateKeyPair();
        trust.Allow(local.NodeId, memberPublicKey, TrustMode.PreShared);
        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = local.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = memberPublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, memberSeed), trust);
        for (int i = 1; i <= 3; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(300 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08A4,
                AuthorNodeId = local.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 13000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, _, page) => afterSequence == 0
                ? new ProviderChangePage(page.Changes.Take(1).ToArray(), 2)
                : page);
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        var scanner = new NonReentrantGcReferenceScanner(hostGate);
        Func<IDisposable?> acquireHostLease = () =>
        {
            if (!hostGate.Wait(0))
                throw new InvalidOperationException("Frontier publication attempted to reacquire the non-reentrant host lease.");
            return new CallbackLease(() => hostGate.Release());
        };

        AppliedOriginFrontierReport report = coordinator.PublishAppliedOriginFrontier(
            memberSeed, scanner, acquireHostLease, pageSize: 2);

        Assert.Equal(2UL, report.UnpublishedDraftCount);
        Assert.Equal(13003UL, report.MaxUnpublishedDraftHlcWall);
        Assert.Equal(1, scanner.OpenCount);
        Assert.Equal(1, scanner.DisposeCount);
        Assert.Equal(1, hostGate.CurrentCount);
    }

    [Fact]
    public void Publish_local_changes_acknowledges_empty_advancing_provider_prefix_after_retry()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "publish-empty-prefix-ack"), 493);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(105);
        const string scope = "publish-empty-prefix-ack";
        for (int i = 1; i <= 3; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(400 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = (ushort)(0x08A5 + i),
                AuthorNodeId = local.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 14000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (_, _, page) =>
                new ProviderChangePage(Array.Empty<ProviderChange>(), page.ScannedThroughSequence));
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => { }, limit: 1));
        Assert.Equal(3, innerProvider.ReadLocalChanges(0, 10).Changes.Count);
        Assert.Equal(0, coordinator.PublishLocalChanges(_ => { }, limit: 1));
        Assert.Empty(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(0, local.GetOutboxCount(1));
    }

    [Fact]
    public void Publish_local_changes_leaves_appends_beyond_captured_high_watermark_for_next_pass()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "publish-captured-high-watermark"), 494);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(106);
        const string scope = "publish-captured-high-watermark";
        RecordId record = RecordId.From(Guid.Parse("49400000-0000-0000-0000-000000000001").ToByteArray());
        innerProvider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x08B0, AuthorNodeId = local.NodeId,
            OriginAuthorEpoch = 1, HlcWall = 15001, Value = "captured-prefix"u8.ToArray()
        });

        bool appendedSuffix = false;
        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, _, page) =>
            {
                if (!appendedSuffix && afterSequence == 0)
                {
                    appendedSuffix = true;
                    innerProvider.AppendLocalChange(new ProviderMutation
                    {
                        RecordId = record, FieldId = 0x08B1, AuthorNodeId = local.NodeId,
                        OriginAuthorEpoch = 1, HlcWall = 15002, Value = "later-suffix"u8.ToArray()
                    });
                }
                return page;
            },
            failFirstAcknowledge: false);
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);

        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }, limit: 1));
        var pendingSuffix = Assert.Single(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(2UL, pendingSuffix.Sequence);
        Assert.Equal("later-suffix", Encoding.UTF8.GetString(pendingSuffix.Mutation.Value));

        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }, limit: 1));
        Assert.Empty(innerProvider.ReadLocalChanges(0, 10).Changes);
        Assert.True(local.TryGetField(1, record, 0x08B0, out var first));
        Assert.Equal("captured-prefix", Encoding.UTF8.GetString(first!.Value));
        Assert.True(local.TryGetField(1, record, 0x08B1, out var suffix));
        Assert.Equal("later-suffix", Encoding.UTF8.GetString(suffix!.Value));
    }

    [Theory]
    [InlineData("stall")]
    [InlineData("regression")]
    [InlineData("overrun")]
    [InlineData("unordered")]
    [InlineData("out-of-range")]
    [InlineData("oversized")]
    public void Applied_origin_frontier_rejects_malformed_provider_scan_pages(string fault)
    {
        string scope = "frontier-malformed-provider-page-" + fault;
        using var storage = StorageDatabase.Open(Path.Combine(_root, scope), 490);
        using var innerProvider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(102);
        using var hostGate = new SemaphoreSlim(1, 1);
        var trust = new TrustStore();
        var (memberSeed, memberPublicKey) = BatchSignature.GenerateKeyPair();
        trust.Allow(local.NodeId, memberPublicKey, TrustMode.PreShared);
        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = local.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = memberPublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, memberSeed), trust);
        for (int i = 1; i <= 2; i++)
        {
            innerProvider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(new Guid(100 + i, 0, 0, new byte[8]).ToByteArray()),
                FieldId = 0x08A2,
                AuthorNodeId = local.NodeId,
                OriginAuthorEpoch = 1,
                HlcWall = 11000UL + (ulong)i,
                Value = BitConverter.GetBytes(i)
            });
        }

        int pageSize = fault is "unordered" or "out-of-range" ? 2 : 1;
        var provider = new FailOnceAcknowledgeProvider(
            innerProvider,
            pageTransform: (afterSequence, limit, page) => fault switch
            {
                "stall" => new ProviderChangePage(Array.Empty<ProviderChange>(), afterSequence),
                "regression" when afterSequence > 0 =>
                    new ProviderChangePage(Array.Empty<ProviderChange>(), afterSequence - 1),
                "overrun" => new ProviderChangePage(page.Changes, innerProvider.LocalChangeHighWatermark + 1),
                "unordered" => new ProviderChangePage(page.Changes.Reverse().ToArray(), page.ScannedThroughSequence),
                "out-of-range" => new ProviderChangePage(
                    new[] { page.Changes[0] with { Sequence = page.ScannedThroughSequence + 1 } },
                    page.ScannedThroughSequence),
                "oversized" => new ProviderChangePage(
                    page.Changes.Concat(page.Changes).ToArray(), page.ScannedThroughSequence),
                _ => page
            });
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);
        var scanner = new NonReentrantGcReferenceScanner(hostGate);
        int hostAcquires = 0;
        int hostReleases = 0;
        Func<IDisposable?> acquireHostLease = () =>
        {
            if (!hostGate.Wait(0))
                throw new InvalidOperationException("Frontier publication attempted to reacquire the non-reentrant host lease.");
            Interlocked.Increment(ref hostAcquires);
            return new CallbackLease(() =>
            {
                Interlocked.Increment(ref hostReleases);
                hostGate.Release();
            });
        };

        Assert.Throws<InvalidDataException>(() => coordinator.PublishAppliedOriginFrontier(
            memberSeed, scanner, acquireHostLease, pageSize));
        Assert.Equal(1, scanner.OpenCount);
        Assert.Equal(1, scanner.DisposeCount);
        Assert.Equal(1, hostAcquires);
        Assert.Equal(1, hostReleases);
        Assert.Equal(1, hostGate.CurrentCount);
    }

    [Fact]
    public void Full_replace_commit_guard_blocks_only_pending_apply_in_target_namespace()
    {
        const string scope = "full-replace-pending-namespace";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-pending-namespace"), 478);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(78);
        using var sender = NewNode(79);
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);

        MetadataIncomingBatch otherNamespace = CreatePendingProviderBatch(
            scope, local, sender, 2,
            RecordId.From(Guid.Parse("78000000-0000-0000-0000-000000000002").ToByteArray()),
            0x0878, "other-namespace-pending", 0x31);
        metadata.BeginPendingApply(otherNamespace, Guid.Parse("78000000-0000-0000-0000-000000000201"));

        int hostAcquires = 0;
        int hostReleases = 0;
        using (coordinator.AcquireFullReplaceCommitGuard(() =>
        {
            Interlocked.Increment(ref hostAcquires);
            return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
        }))
        {
            Assert.Equal(1, hostAcquires);
            Assert.Equal(0, hostReleases);
        }
        Assert.Equal(1, hostReleases);

        MetadataIncomingBatch targetNamespace = CreatePendingProviderBatch(
            scope, local, sender, 1,
            RecordId.From(Guid.Parse("78000000-0000-0000-0000-000000000001").ToByteArray()),
            0x0878, "target-namespace-pending", 0x32);
        metadata.BeginPendingApply(targetNamespace, Guid.Parse("78000000-0000-0000-0000-000000000101"));

        Assert.Throws<InvalidOperationException>(() =>
            coordinator.AcquireFullReplaceCommitGuard(() =>
            {
                Interlocked.Increment(ref hostAcquires);
                return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
            }));
        Assert.Equal(2, hostAcquires);
        Assert.Equal(2, hostReleases);
        IReadOnlyList<MetadataPendingApplyIntent> remaining =
            metadata.ReadPendingApplyIntents(scope, local.NodeId);
        Assert.Single(remaining, intent => intent.Batch.NamespaceId == 1);
        Assert.Single(remaining, intent => intent.Batch.NamespaceId == 2);
    }

    [Fact]
    public void Full_replace_commit_guard_blocks_pushed_unacknowledged_peer_reservations()
    {
        const string scope = "full-replace-pending-reservation";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-pending-reservation"), 479);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(80);
        using var peer = NewNode(81);
        using var relayAuthor = NewNode(84);
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);

        metadata.RegisterInterestedPeer(scope, local.NodeId, peer.NodeId, 7, 1);
        metadata.CompletePeerInitialSync(scope, local.NodeId, peer.NodeId, 7, 1,
            new[]
            {
                new MetadataPeerStreamBaseline(local.NodeId, 0, 3),
                new MetadataPeerStreamBaseline(relayAuthor.NodeId, 0, 3)
            });
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Guid.Parse("80000000-0000-0000-0000-000000000001").ToByteArray()),
            IssuerEpoch = 3,
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0x81, 16).ToArray(),
            Payload = new byte[] { 0x81, 0x01 }
        };
        var relayReservation = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Guid.Parse("80000000-0000-0000-0000-000000000002").ToByteArray()),
            IssuerEpoch = 3,
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0x82, 16).ToArray(),
            Payload = new byte[] { 0x82, 0x02 }
        };
        metadata.ReserveOutboundBatch(scope, local.NodeId, peer.NodeId, 7, 1, local.NodeId, reservation);
        metadata.MarkOutboundBatchPushed(scope, local.NodeId, peer.NodeId, 7, 1,
            local.NodeId, reservation.BatchId);
        metadata.ReserveOutboundBatch(scope, local.NodeId, peer.NodeId, 7, 1, relayAuthor.NodeId, relayReservation);
        metadata.MarkOutboundBatchPushed(scope, local.NodeId, peer.NodeId, 7, 1,
            relayAuthor.NodeId, relayReservation.BatchId);

        int hostAcquires = 0;
        int hostReleases = 0;
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.AcquireFullReplaceCommitGuard(() =>
            {
                Interlocked.Increment(ref hostAcquires);
                return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
            }));
        Assert.Equal(1, hostAcquires);
        Assert.Equal(1, hostReleases);

        IReadOnlyList<MetadataPeerStreamSnapshot> pendingStreams =
            metadata.ReadPeerStreamStates(scope, local.NodeId, peer.NodeId, 7, 1);
        Assert.Equal(2, pendingStreams.Count);
        MetadataPeerStreamSnapshot pendingOutbound = Assert.Single(
            pendingStreams, stream => stream.AuthorStream == local.NodeId);
        MetadataPeerStreamSnapshot pendingRelay = Assert.Single(
            pendingStreams, stream => stream.AuthorStream == relayAuthor.NodeId);
        Assert.Equal(0UL, pendingOutbound.State.AckedCursor);
        Assert.Equal(1UL, pendingOutbound.State.PushedCursor);
        Assert.NotNull(pendingOutbound.State.PendingBatch);
        Assert.Equal(0UL, pendingRelay.State.AckedCursor);
        Assert.Equal(1UL, pendingRelay.State.PushedCursor);
        Assert.NotNull(pendingRelay.State.PendingBatch);

        metadata.AcknowledgeOutboundBatch(scope, local.NodeId, peer.NodeId, 7, 1,
            local.NodeId, reservation.BatchId, reservation.CursorEnd, reservation.BatchDigest.ToArray());
        metadata.AcknowledgeOutboundBatch(scope, local.NodeId, peer.NodeId, 7, 1,
            relayAuthor.NodeId, relayReservation.BatchId, relayReservation.CursorEnd,
            relayReservation.BatchDigest.ToArray());
        using (coordinator.AcquireFullReplaceCommitGuard(() =>
        {
            Interlocked.Increment(ref hostAcquires);
            return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
        }))
        {
            Assert.Equal(2, hostAcquires);
            Assert.Equal(1, hostReleases);
            IReadOnlyList<MetadataPeerStreamSnapshot> acknowledgedStreams =
                metadata.ReadPeerStreamStates(scope, local.NodeId, peer.NodeId, 7, 1);
            Assert.Equal(2, acknowledgedStreams.Count);
            foreach (MetadataPeerStreamSnapshot acknowledged in acknowledgedStreams)
            {
                Assert.Equal(1UL, acknowledged.State.AckedCursor);
                Assert.Equal(1UL, acknowledged.State.PushedCursor);
                Assert.Null(acknowledged.State.PendingBatch);
            }
        }
        Assert.Equal(2, hostReleases);
    }

    [Fact]
    public void Full_replace_commit_guard_does_not_treat_completed_receipts_as_pending_work()
    {
        const string scope = "full-replace-completed-receipt";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-completed-receipt"), 480);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(82);
        using var sender = NewNode(83);
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);

        MetadataIncomingBatch batch = CreatePendingProviderBatch(
            scope, local, sender, 1,
            RecordId.From(Guid.Parse("82000000-0000-0000-0000-000000000001").ToByteArray()),
            0x0882, "completed-receipt", 0x82);
        Guid applyToken = Guid.Parse("82000000-0000-0000-0000-000000000082");
        metadata.BeginPendingApply(batch, applyToken);
        metadata.FinalizePendingApply(scope, local.NodeId,
            new MetadataProviderApplyReceipt(applyToken, batch.BatchDigest.ToArray()));
        Assert.Empty(metadata.ReadPendingApplyIntents(scope, local.NodeId));

        int hostAcquires = 0;
        int hostReleases = 0;
        using (coordinator.AcquireFullReplaceCommitGuard(() =>
        {
            Interlocked.Increment(ref hostAcquires);
            return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
        }))
        {
            Assert.Equal(1, hostAcquires);
            Assert.Equal(0, hostReleases);
        }
        Assert.Equal(1, hostReleases);
    }

    [Fact]
    public async Task Full_replace_commit_guard_blocks_concurrent_metadata_writers_until_disposed()
    {
        const string scope = "full-replace-metadata-writer";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-metadata-writer"), 481);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(85);
        using var sender = NewNode(86);
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
        coordinator.AttachNamespaceGcGuard(gcStore, trust);

        MetadataIncomingBatch batch = CreatePendingProviderBatch(
            scope, local, sender, 1,
            RecordId.From(Guid.Parse("85000000-0000-0000-0000-000000000001").ToByteArray()),
            0x0885, "blocked-metadata-writer", 0x85);
        Guid applyToken = Guid.Parse("85000000-0000-0000-0000-000000000085");
        using var writerStarted = new ManualResetEventSlim();
        using var writerCompleted = new ManualResetEventSlim();

        int hostReleases = 0;
        IDisposable lease = coordinator.AcquireFullReplaceCommitGuard(() =>
            new CallbackLease(() => Interlocked.Increment(ref hostReleases)));
        Task writer = Task.Run(() =>
        {
            writerStarted.Set();
            metadata.BeginPendingApply(batch, applyToken);
            writerCompleted.Set();
        });

        bool started;
        bool blockedByLease;
        try
        {
            started = writerStarted.Wait(TimeSpan.FromSeconds(5));
            blockedByLease = started && !writerCompleted.Wait(TimeSpan.FromMilliseconds(150));
        }
        finally
        {
            lease.Dispose();
        }

        Assert.True(writerCompleted.Wait(TimeSpan.FromSeconds(5)));
        await writer.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(started);
        Assert.True(blockedByLease);
        Assert.Equal(1, hostReleases);
        Assert.Single(metadata.ReadPendingApplyIntents(scope, local.NodeId));
    }

    [Fact]
    public void Full_replace_commit_guard_requires_exact_namespace_gc_binding_and_releases_host_lease()
    {
        const string scope = "full-replace-gc-binding";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-gc-binding"), 482);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var local = NewNode(87);
        var trust = new TrustStore();
        var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);

        int hostAcquires = 0;
        int hostReleases = 0;
        Assert.Throws<NotSupportedException>(() =>
            coordinator.AcquireFullReplaceCommitGuard(() =>
            {
                Interlocked.Increment(ref hostAcquires);
                return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
            }));
        Assert.Equal(0, hostAcquires);
        Assert.Equal(0, hostReleases);

        var wrongScopeStore = new StorageNamespaceGcStore(storage, scope + "-wrong", local.NodeId);
        coordinator.AttachNamespaceGcGuard(wrongScopeStore, trust);
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.AcquireFullReplaceCommitGuard(() =>
            {
                Interlocked.Increment(ref hostAcquires);
                return new CallbackLease(() => Interlocked.Increment(ref hostReleases));
            }));
        Assert.Equal(1, hostAcquires);
        Assert.Equal(1, hostReleases);
    }

    [Theory]
    [InlineData("scope")]
    [InlineData("local")]
    [InlineData("namespace")]
    [InlineData("store")]
    [InlineData("trust")]
    public void Full_replace_receiver_rejects_mismatched_typed_lease_before_provider_switch_or_ack(string mismatch)
    {
        const string scope = "full-replace-receiver-lease-binding";
        using var storage = StorageDatabase.Open(Path.Combine(_root, "full-replace-receiver-lease-binding"), 483);
        using var provider = new StorageDataProvider(storage);
        using var local = NewNode(90);
        using var authorityNode = NewNode(91);
        using var wrongLocal = NewNode(92);
        var (authoritySeed, authorityPublicKey) = BatchSignature.GenerateKeyPair();
        var (localSeed, localPublicKey) = BatchSignature.GenerateKeyPair();
        var trust = new TrustStore();
        trust.Allow(authorityNode.NodeId, authorityPublicKey, TrustMode.PreShared);

        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 1,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = authorityNode.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local.NodeId,
                    PublicKey = localPublicKey,
                    TransportPeerEpoch = 9,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = authorityNode.NodeId,
                    PublicKey = authorityPublicKey,
                    TransportPeerEpoch = 7,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gcStore = new StorageNamespaceGcStore(storage, scope, local.NodeId);
        NamespaceRosterCertificate roster = gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, authoritySeed), trust);

        Guid generation = Guid.Parse("90000000-0000-0000-0000-000000000001");
        byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(
            generation, 0, Array.Empty<ProviderMutation>());
        var unsignedManifest = new FullReplaceTransferProtocol.Begin(
            1, generation, roster.RosterGeneration,
            NamespaceRosterProtocol.ComputeDigest(roster), 0, 0, digest, Array.Empty<byte>());
        FullReplaceTransferProtocol.Begin manifest =
            FullReplaceTransferProtocol.SignBegin(unsignedManifest, authoritySeed);

        var scanner = new ThrowingGcReferenceScanner();
        int leaseDisposals = 0;
        string leaseScope = mismatch == "scope" ? scope + "-wrong" : scope;
        NodeId leaseLocal = mismatch == "local" ? wrongLocal.NodeId : local.NodeId;
        int leaseNamespace = mismatch == "namespace" ? 2 : 1;
        IMetadataNamespaceGcStore leaseGcStore = mismatch == "store"
            ? new StorageNamespaceGcStore(storage, scope, local.NodeId)
            : gcStore;
        TrustStore leaseTrust = mismatch == "trust" ? new TrustStore() : trust;
        var mismatchedLease = new TestFullReplaceCommitLease(
            leaseScope, leaseLocal, leaseNamespace, leaseGcStore, leaseTrust,
            () => Interlocked.Increment(ref leaseDisposals));
        var receiver = new StorageFullReplaceReceiver(
            provider, gcStore, trust, 1,
            authorityNode.NodeId, 7, local.NodeId, 9, localSeed,
            scope, scanner, () => mismatchedLease, (_, _) => { });

        receiver.Begin(manifest);
        var transfer = new FullReplaceTransferProtocol.TransferId(1, generation);
        Assert.Throws<InvalidOperationException>(() => receiver.Commit(transfer));
        Assert.Equal(1, leaseDisposals);
        Assert.False(scanner.Opened);
        Assert.True(provider.TryGetFullReplaceProgress(generation, out ProviderFullReplaceProgress? progress));
        Assert.NotNull(progress);
        Assert.False(progress.IsCommitted);
        Assert.Empty(gcStore.ReadFullReplaceInstalledAcks(roster, generation));
    }

    [Fact]
    public async Task Metadata_authoritative_receive_uses_pending_apply_without_duplicate_node_remote_batch_wal()
    {
        async Task<(int RemoteBatchRecords, int GlobalLedgerCount, int OutboxCount)> Receive(
            bool metadataOwned, string name, byte seed)
        {
            string directory = Path.Combine(_root, name);
            Directory.CreateDirectory(directory);
            using var storage = StorageDatabase.Open(directory, (ulong)(490 + seed));
            using var provider = new StorageDataProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            using var sender = NewNode(seed);
            using var receiver = NewNode((byte)(seed + 1));
            string walPath = Path.Combine(directory, "receiver.wal");
            using var wal = Wal.Open(walPath);
            receiver.AttachWal(wal);

            const string scopePrefix = "receive-wal-ownership-";
            string scope = scopePrefix + name;
            StorageSyncCoordinator? coordinator = metadataOwned
                ? new StorageSyncCoordinator(receiver, provider, 1, metadata, scope)
                : null;
            coordinator?.EnableMetadataStoreOwnedPublication(_ => { });

            RecordId record = RecordId.From(Guid.Parse(
                $"{seed:X2}000000-0000-0000-0000-000000000001").ToByteArray());
            Assert.True(sender.WriteField(1, record, 0x0890,
                "receive-wal-ownership"u8.ToArray(), new Hlc(8000UL + seed, 0)));

            var (toSender, toReceiver) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
            var connectTask = SyncSession.ConnectAsync(sender, toSender);
            await using var acceptor = await acceptTask;
            await using var initiator = await connectTask;

            Task<BatchResult> receive = coordinator is null
                ? acceptor.ReceiveBatchAsync(receiver)
                : coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
            Task<bool> send = initiator.PushBatchAsync(sender, 1);
            await Task.WhenAll(receive, send).WaitAsync(TimeSpan.FromSeconds(30));
            wal.Dispose();

            return (CountWalRecords(walPath, WalRecordType.RemoteBatch),
                receiver.GlobalChangeLedgerCount, receiver.GetOutboxCount(1));
        }

        var legacy = await Receive(false, "node-owned-receive-wal", 95);
        var authoritative = await Receive(true, "metadata-owned-receive-wal", 97);

        Assert.Equal(1, legacy.RemoteBatchRecords);
        Assert.True(legacy.GlobalLedgerCount > 0);
        Assert.Equal(0, authoritative.RemoteBatchRecords);
        Assert.Equal(0, authoritative.GlobalLedgerCount);
        Assert.Equal(0, authoritative.OutboxCount);
    }

    [Theory]
    [InlineData("before-node-checkpoint")]
    [InlineData("after-node-checkpoint")]
    public void Provider_cutover_recovers_legacy_ledger_aliases_across_node_checkpoint_crash(string crashCut)
    {
        const string scope = "legacy-alias-cutover-crash";
        const ulong partitionId = 485;
        string directory = Path.Combine(_root, "legacy-alias-cutover-crash-" + crashCut);
        NodeId localId;
        MetadataLegacyNodeLedgerFingerprint alias;
        int seenChangesBeforeCutover;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var local = NewNode(95))
        {
            localId = local.NodeId;
            RecordId record = RecordId.From(Guid.Parse("95000000-0000-0000-0000-000000000001").ToByteArray());
            Assert.True(local.WriteField(1, record, 0x0895, "legacy-alias-crash"u8.ToArray(), new Hlc(9500, 0)));
            OutboxEntry sent = Assert.Single(local.ReadOutboxEntries(1, local.NodeId, 0, 10));
            Assert.Contains((1, sent.ChangeId), local.GlobalChangeLedger.Keys);
            Assert.Contains((1, sent.ChangeId), local.SeenChanges);
            seenChangesBeforeCutover = local.SeenChanges.Count;

            object operation = Ops.DecodeOp(sent.Payload).Op;
            Digest16 semanticFingerprint = ChangeIdentity.Fingerprint(
                1, sent.RecordId, sent.OpKind, operation, sent.AuthorNodeId, sent.CausalDeps);
            metadata.FreezeOutbound(scope, local.NodeId, 1, local.NodeId, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = sent.ChangeId,
                    Fingerprint = semanticFingerprint.ToArray(),
                    Payload = sent.Payload.ToArray(),
                    OriginAuthorEpoch = sent.OriginAuthorEpoch,
                    OriginSequence = sent.OriginSequence,
                    HlcWall = 9500,
                    HlcLogical = 0
                }
            });

            alias = new MetadataLegacyNodeLedgerFingerprint(
                sent.ChangeId, local.GlobalChangeLedger[(1, sent.ChangeId)].Fingerprint.ToArray());
            StorageNodePersistence.Save(storage, local);

            var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
            int persistCalls = 0;
            IOException crash = Assert.Throws<IOException>(() =>
                coordinator.EnableMetadataStoreOwnedPublication(node =>
                {
                    int call = Interlocked.Increment(ref persistCalls);
                    if (call == 1)
                    {
                        // InstallProviderSnapshot persists the provider-derived Node
                        // state before the durable publication marker is written.
                        StorageNodePersistence.Save(storage, node);
                        return;
                    }

                    if (crashCut == "after-node-checkpoint")
                        StorageNodePersistence.Save(storage, node);
                    throw new IOException("Simulated process loss at the post-cutover Node checkpoint boundary.");
                }));
            Assert.Equal("Simulated process loss at the post-cutover Node checkpoint boundary.", crash.Message);
            Assert.Equal(2, persistCalls);
            Assert.True(((IProviderPublicationMode)provider).TryGetMetadataStoreOwnedPublicationCutover(out _));
        }

        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        using (var reopenedNode = StorageNodePersistence.Load(reopenedStorage, localId))
        {
            Assert.Equal(crashCut == "before-node-checkpoint" ? 1 : 0, reopenedNode.GlobalChangeLedger.Count);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            Assert.True(reopenedMetadata.TryGetChangeIdentity(
                scope, localId, 1, alias.ChangeId,
                out ReadOnlyMemory<byte> semanticFingerprint, out _, out _));
            Assert.Equal(16, semanticFingerprint.Length);

            var conflictingAlias = new MetadataLegacyNodeLedgerFingerprint(
                alias.ChangeId, Enumerable.Repeat((byte)0x96, 16).ToArray());
            Assert.Throws<InvalidDataException>(() =>
                reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1, new[] { conflictingAlias }));
            reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1, new[] { alias });

            var recoveredCoordinator = new StorageSyncCoordinator(
                reopenedNode, reopenedProvider, 1, reopenedMetadata, scope);
            recoveredCoordinator.EnableMetadataStoreOwnedPublication(node =>
                StorageNodePersistence.Save(reopenedStorage, node));

            Assert.DoesNotContain(reopenedNode.GlobalChangeLedger.Keys, key => key.Ns == 1);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            Assert.Empty(reopenedNode.ReadOutboxAuthors(1).SelectMany(author => reopenedNode.ReadOutboxEntries(1, author, 0, 10)));
            Assert.True(((IProviderPublicationMode)reopenedProvider).TryGetMetadataStoreOwnedPublicationCutover(out _));
        }
    }

    [Fact]
    public void Coordinator_migrates_legacy_metadata_publication_marker_into_provider_on_reopen()
    {
        const string scope = "legacy-metadata-publication-marker-recovery";
        const ulong partitionId = 496;
        string directory = Path.Combine(_root, scope);
        NodeId localId;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var local = NewNode(108))
        {
            localId = local.NodeId;
            RecordId record = RecordId.From(Guid.Parse("49600000-0000-0000-0000-000000000001").ToByteArray());
            Assert.True(local.WriteField(1, record, 0x08A7,
                "legacy-marker-state"u8.ToArray(), new Hlc(17000, 0)));
            OutboxEntry entry = Assert.Single(local.ReadOutboxEntries(1, local.NodeId, 0, 1));
            object operation = Ops.DecodeOp(entry.Payload).Op;
            Digest16 fingerprint = ChangeIdentity.Fingerprint(
                1, entry.RecordId, entry.OpKind, operation, entry.AuthorNodeId, entry.CausalDeps);
            metadata.FreezeOutbound(scope, local.NodeId, 1, local.NodeId, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = fingerprint.ToArray(),
                    Payload = entry.Payload.ToArray(),
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence,
                    HlcWall = 17000,
                    HlcLogical = 0
                }
            });
            StorageNodePersistence.Save(storage, local);

            // Seed the MCP1 marker used by the prior Metadata-owned cutover
            // implementation. The provider's newer marker is deliberately absent.
            metadata.EnableProviderPublicationCutover(scope, local.NodeId, 1, 1);
            Assert.True(metadata.TryGetProviderPublicationCutover(
                scope, local.NodeId, 1, out ulong legacyHigh));
            Assert.Equal(1UL, legacyHigh);
            Assert.False(provider.TryGetMetadataStoreOwnedPublicationCutover(out _));
        }

        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        using (var reopenedNode = StorageNodePersistence.Load(reopenedStorage, localId))
        {
            var coordinator = new StorageSyncCoordinator(
                reopenedNode, reopenedProvider, 1, reopenedMetadata, scope);
            Assert.True(coordinator.IsMetadataStoreOwnedPublication);
            Assert.True(reopenedProvider.TryGetMetadataStoreOwnedPublicationCutover(out ulong providerHigh));
            Assert.Equal(1UL, providerHigh);
            Assert.True(reopenedMetadata.TryGetProviderPublicationCutover(
                scope, localId, 1, out ulong retainedLegacyHigh));
            Assert.Equal(providerHigh, retainedLegacyHigh);
            Assert.Equal(1UL, reopenedMetadata.GetOutboundHighWatermark(
                scope, localId, 1, localId));
            Assert.Throws<InvalidOperationException>(() =>
                reopenedNode.WriteField(1,
                    RecordId.From(Guid.Parse("49600000-0000-0000-0000-000000000002").ToByteArray()),
                    0x08A7, "must-remain-fenced"u8.ToArray(), new Hlc(17001, 0)));
        }
    }

    [Fact]
    public void Coordinator_rejects_conflicting_legacy_and_provider_publication_markers()
    {
        const string scope = "legacy-provider-marker-conflict";
        const ulong partitionId = 497;
        string directory = Path.Combine(_root, scope);
        NodeId localId;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var local = NewNode(109))
        {
            localId = local.NodeId;
            RecordId record = RecordId.From(Guid.Parse("49700000-0000-0000-0000-000000000001").ToByteArray());
            Assert.True(local.WriteField(1, record, 0x08A8,
                "marker-conflict-state"u8.ToArray(), new Hlc(17100, 0)));
            OutboxEntry entry = Assert.Single(local.ReadOutboxEntries(1, local.NodeId, 0, 1));
            object operation = Ops.DecodeOp(entry.Payload).Op;
            Digest16 fingerprint = ChangeIdentity.Fingerprint(
                1, entry.RecordId, entry.OpKind, operation, entry.AuthorNodeId, entry.CausalDeps);
            metadata.FreezeOutbound(scope, local.NodeId, 1, local.NodeId, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = fingerprint.ToArray(),
                    Payload = entry.Payload.ToArray(),
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence,
                    HlcWall = 17100,
                    HlcLogical = 0
                }
            });
            StorageNodePersistence.Save(storage, local);
            provider.EnableMetadataStoreOwnedPublication(1);
            metadata.EnableProviderPublicationCutover(scope, local.NodeId, 1, 2);
        }

        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        using (var reopenedNode = StorageNodePersistence.Load(reopenedStorage, localId))
        {
            Assert.Throws<InvalidDataException>(() =>
                new StorageSyncCoordinator(reopenedNode, reopenedProvider, 1, reopenedMetadata, scope));
            Assert.True(reopenedProvider.TryGetMetadataStoreOwnedPublicationCutover(out ulong providerHigh));
            Assert.Equal(1UL, providerHigh);
            Assert.True(reopenedMetadata.TryGetProviderPublicationCutover(
                scope, localId, 1, out ulong legacyHigh));
            Assert.Equal(2UL, legacyHigh);
        }
    }

    [Fact]
    public void Provider_cutover_recovers_after_crash_between_4096_and_final_legacy_alias_batches()
    {
        const string scope = "legacy-alias-crash-between-batches";
        const ulong partitionId = 495;
        const int operationCount = 4097;
        string directory = Path.Combine(_root, scope);
        NodeId localId;
        int seenChangesBeforeCutover;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var local = NewNode(107))
        {
            localId = local.NodeId;
            for (int i = 1; i <= operationCount; i++)
            {
                RecordId record = RecordId.From(new Guid(i + 5000, 0, 0, new byte[8]).ToByteArray());
                Assert.True(local.WriteField(1, record, 0x08A6,
                    BitConverter.GetBytes(i), new Hlc(16000UL + (ulong)i, 0)));
            }

            var entries = new List<OutboxEntry>(operationCount);
            long afterSequence = 0;
            while (entries.Count < operationCount)
            {
                IReadOnlyList<OutboxEntry> page =
                    local.ReadOutboxEntries(1, local.NodeId, afterSequence, 1024);
                Assert.NotEmpty(page);
                entries.AddRange(page);
                afterSequence = page[^1].Seq;
            }
            Assert.Equal(operationCount, entries.Count);
            seenChangesBeforeCutover = local.SeenChanges.Count;
            Assert.Equal(operationCount, seenChangesBeforeCutover);

            var frozen = new MetadataFrozenOperation[entries.Count];
            for (int i = 0; i < entries.Count; i++)
            {
                OutboxEntry entry = entries[i];
                object operation = Ops.DecodeOp(entry.Payload).Op;
                Digest16 semanticFingerprint = ChangeIdentity.Fingerprint(
                    1, entry.RecordId, entry.OpKind, operation, entry.AuthorNodeId, entry.CausalDeps);
                frozen[i] = new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = semanticFingerprint.ToArray(),
                    Payload = entry.Payload.ToArray(),
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence,
                    HlcWall = 16000UL + (ulong)i + 1,
                    HlcLogical = 0
                };
            }
            metadata.FreezeOutbound(scope, local.NodeId, 1, local.NodeId, frozen);
            StorageNodePersistence.Save(storage, local);

            // Reproduce the exact durable state at the boundary between the
            // coordinator's first 4096-alias commit and its final one-alias commit.
            // The process is terminated immediately after this first atomic batch.
            var ledgerEntries = local.GlobalChangeLedger
                .Where(pair => pair.Key.Ns == 1)
                .ToArray();
            Assert.Equal(operationCount, ledgerEntries.Length);
            var firstBatch = ledgerEntries.Take(4096)
                .Select(pair => new MetadataLegacyNodeLedgerFingerprint(
                    pair.Key.ChangeId, pair.Value.Fingerprint.ToArray()))
                .ToArray();
            metadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, firstBatch);
            var firstConflict = new MetadataLegacyNodeLedgerFingerprint(
                firstBatch[0].ChangeId, Enumerable.Repeat((byte)0xA7, 16).ToArray());
            Assert.Throws<InvalidDataException>(() =>
                metadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { firstConflict }));
        }

        // Reopen after the first batch is durable but before the 4097th alias exists.
        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        using (var reopenedNode = StorageNodePersistence.Load(reopenedStorage, localId))
        {
            Assert.Equal(operationCount, reopenedNode.GlobalChangeLedger.Count);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            var first = reopenedNode.GlobalChangeLedger
                .Where(pair => pair.Key.Ns == 1).First();
            var last = reopenedNode.GlobalChangeLedger
                .Where(pair => pair.Key.Ns == 1).Last();
            Assert.Throws<InvalidDataException>(() =>
                reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1,
                    new[] { new MetadataLegacyNodeLedgerFingerprint(
                        first.Key.ChangeId, Enumerable.Repeat((byte)0xA8, 16).ToArray()) }));

            var coordinator = new StorageSyncCoordinator(
                reopenedNode, reopenedProvider, 1, reopenedMetadata, scope);
            coordinator.EnableMetadataStoreOwnedPublication(node =>
                StorageNodePersistence.Save(reopenedStorage, node));

            Assert.DoesNotContain(reopenedNode.GlobalChangeLedger.Keys, key => key.Ns == 1);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            Assert.Throws<InvalidDataException>(() =>
                reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1,
                    new[] { new MetadataLegacyNodeLedgerFingerprint(
                        last.Key.ChangeId, Enumerable.Repeat((byte)0xA9, 16).ToArray()) }));
        }

        // A second restart must treat the completed marker and every alias as
        // authoritative and keep the cutover idempotent.
        using (var finalStorage = StorageDatabase.Open(directory, partitionId))
        using (var finalProvider = new StorageDataProvider(finalStorage))
        using (var finalMetadata = new StorageMetadataStore(finalStorage))
        using (var finalNode = StorageNodePersistence.Load(finalStorage, localId))
        {
            var coordinator = new StorageSyncCoordinator(finalNode, finalProvider, 1, finalMetadata, scope);
            coordinator.EnableMetadataStoreOwnedPublication(node =>
                StorageNodePersistence.Save(finalStorage, node));
            Assert.DoesNotContain(finalNode.GlobalChangeLedger.Keys, key => key.Ns == 1);
            Assert.Equal(seenChangesBeforeCutover, finalNode.SeenChanges.Count);
            Assert.True(((IProviderPublicationMode)finalProvider)
                .TryGetMetadataStoreOwnedPublicationCutover(out _));
        }
    }

    [Fact]
    public void Provider_cutover_migrates_legacy_aliases_across_the_4096_entry_batch_boundary()
    {
        const string scope = "legacy-alias-bounded-batches";
        const ulong partitionId = 486;
        const int operationCount = 4097;
        string directory = Path.Combine(_root, "legacy-alias-bounded-batches");
        NodeId localId;
        int seenChangesBeforeCutover;

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var provider = new StorageDataProvider(storage))
        using (var metadata = new StorageMetadataStore(storage))
        using (var local = NewNode(97))
        {
            localId = local.NodeId;
            for (int i = 1; i <= operationCount; i++)
            {
                RecordId record = RecordId.From(new Guid(i, 0, 0, new byte[8]).ToByteArray());
                Assert.True(local.WriteField(1, record, 0x0896,
                    BitConverter.GetBytes(i), new Hlc(10000UL + (ulong)i, 0)));
            }

            var entries = new List<OutboxEntry>(operationCount);
            long afterSequence = 0;
            while (entries.Count < operationCount)
            {
                IReadOnlyList<OutboxEntry> page =
                    local.ReadOutboxEntries(1, local.NodeId, afterSequence, 1024);
                Assert.NotEmpty(page);
                entries.AddRange(page);
                afterSequence = page[^1].Seq;
            }
            Assert.Equal(operationCount, entries.Count);
            seenChangesBeforeCutover = local.SeenChanges.Count;
            Assert.Equal(operationCount, seenChangesBeforeCutover);

            var frozen = new MetadataFrozenOperation[entries.Count];
            for (int i = 0; i < entries.Count; i++)
            {
                OutboxEntry entry = entries[i];
                object operation = Ops.DecodeOp(entry.Payload).Op;
                Digest16 semanticFingerprint = ChangeIdentity.Fingerprint(
                    1, entry.RecordId, entry.OpKind, operation, entry.AuthorNodeId, entry.CausalDeps);
                frozen[i] = new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = semanticFingerprint.ToArray(),
                    Payload = entry.Payload.ToArray(),
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence,
                    HlcWall = 10000UL + (ulong)i + 1,
                    HlcLogical = 0
                };
            }
            metadata.FreezeOutbound(scope, local.NodeId, 1, local.NodeId, frozen);
            Assert.Equal((ulong)operationCount,
                metadata.GetOutboundHighWatermark(scope, local.NodeId, 1, local.NodeId));
            StorageNodePersistence.Save(storage, local);

            var coordinator = new StorageSyncCoordinator(local, provider, 1, metadata, scope);
            int persistCalls = 0;
            IOException crash = Assert.Throws<IOException>(() =>
                coordinator.EnableMetadataStoreOwnedPublication(node =>
                {
                    if (Interlocked.Increment(ref persistCalls) == 1)
                    {
                        StorageNodePersistence.Save(storage, node);
                        return;
                    }
                    throw new IOException("Simulated process loss after all bounded alias batches and before the Node checkpoint.");
                }));
            Assert.Equal("Simulated process loss after all bounded alias batches and before the Node checkpoint.", crash.Message);
            Assert.Equal(2, persistCalls);
            Assert.True(((IProviderPublicationMode)provider).TryGetMetadataStoreOwnedPublicationCutover(out _));
        }

        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        using (var reopenedNode = StorageNodePersistence.Load(reopenedStorage, localId))
        {
            Assert.Equal(operationCount, reopenedNode.GlobalChangeLedger.Count);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            var first = Assert.Single(reopenedNode.ReadOutboxEntries(1, localId, 0, 1));
            OutboxEntry last = reopenedNode.ReadOutboxEntries(1, localId, operationCount - 1, 1).Single();
            var firstAlias = new MetadataLegacyNodeLedgerFingerprint(
                first.ChangeId, reopenedNode.GlobalChangeLedger[(1, first.ChangeId)].Fingerprint.ToArray());
            var lastAlias = new MetadataLegacyNodeLedgerFingerprint(
                last.ChangeId, reopenedNode.GlobalChangeLedger[(1, last.ChangeId)].Fingerprint.ToArray());
            var conflictingFirst = new MetadataLegacyNodeLedgerFingerprint(
                first.ChangeId, Enumerable.Repeat((byte)0x97, 16).ToArray());
            var conflictingLast = new MetadataLegacyNodeLedgerFingerprint(
                last.ChangeId, Enumerable.Repeat((byte)0x98, 16).ToArray());
            Assert.Throws<InvalidDataException>(() =>
                reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1, new[] { conflictingFirst }));
            Assert.Throws<InvalidDataException>(() =>
                reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1, new[] { conflictingLast }));
            reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, localId, 1, new[] { firstAlias, lastAlias });

            var recoveredCoordinator = new StorageSyncCoordinator(
                reopenedNode, reopenedProvider, 1, reopenedMetadata, scope);
            recoveredCoordinator.EnableMetadataStoreOwnedPublication(node =>
                StorageNodePersistence.Save(reopenedStorage, node));
            Assert.DoesNotContain(reopenedNode.GlobalChangeLedger.Keys, key => key.Ns == 1);
            Assert.Equal(seenChangesBeforeCutover, reopenedNode.SeenChanges.Count);
            Assert.Empty(reopenedNode.ReadOutboxEntries(1, localId, 0, 1));
        }
    }

    [Fact]
    public void Legacy_node_ledger_alias_round_trips_and_rejects_conflicts_after_reopen()
    {
        const string scope = "legacy-node-ledger-alias";
        const ulong partitionId = 484;
        string directory = Path.Combine(_root, "legacy-node-ledger-alias");
        using var local = NewNode(93);
        using var sender = NewNode(94);
        MetadataIncomingBatch incoming;
        Guid applyToken = Guid.Parse("93000000-0000-0000-0000-000000000001");
        byte[] legacyDigest = Enumerable.Repeat((byte)0x93, 16).ToArray();

        using (var storage = StorageDatabase.Open(directory, partitionId))
        using (var metadata = new StorageMetadataStore(storage))
        {
            incoming = CreatePendingProviderBatch(
                scope, local, sender, 1,
                RecordId.From(Guid.Parse("93000000-0000-0000-0000-000000000002").ToByteArray()),
                0x0893, "legacy-ledger-alias", 0x93);
            metadata.BeginPendingApply(incoming, applyToken);
            metadata.FinalizePendingApply(scope, local.NodeId,
                new MetadataProviderApplyReceipt(applyToken, incoming.BatchDigest.ToArray()));
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local.NodeId, 1, incoming.Operations[0].ChangeId,
                out ReadOnlyMemory<byte> semanticFingerprint, out _, out _));
            Assert.Equal(incoming.Operations[0].Fingerprint.ToArray(), semanticFingerprint.ToArray());
        }

        var alias = new MetadataLegacyNodeLedgerFingerprint(
            incoming.Operations[0].ChangeId, legacyDigest);
        using (var reopenedStorage = StorageDatabase.Open(directory, partitionId))
        using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
        {
            Assert.True(reopenedMetadata.TryGetChangeIdentity(
                scope, local.NodeId, 1, incoming.Operations[0].ChangeId,
                out ReadOnlyMemory<byte> semanticFingerprint, out _, out _));
            Assert.Equal(incoming.Operations[0].Fingerprint.ToArray(), semanticFingerprint.ToArray());
            reopenedMetadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { alias });
        }

        using (var aliasStorage = StorageDatabase.Open(directory, partitionId))
        using (var aliasMetadata = new StorageMetadataStore(aliasStorage))
        {
            aliasMetadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { alias });
            var conflictingAlias = new MetadataLegacyNodeLedgerFingerprint(
                incoming.Operations[0].ChangeId, Enumerable.Repeat((byte)0x94, 16).ToArray());
            Assert.Throws<InvalidDataException>(() =>
                aliasMetadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { conflictingAlias }));

            var zeroAlias = new MetadataLegacyNodeLedgerFingerprint(
                incoming.Operations[0].ChangeId, new byte[16]);
            Assert.Throws<ArgumentException>(() =>
                aliasMetadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { zeroAlias }));
            Assert.Throws<ArgumentException>(() =>
                aliasMetadata.StoreLegacyNodeLedgerFingerprints(scope, local.NodeId, 1, new[] { alias, alias }));
        }
    }

    private static int CountWalRecords(string path, WalRecordType type)
    {
        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length < 6)
            throw new InvalidDataException("WAL header is truncated.");

        int offset = 6;
        int count = 0;
        while (offset < bytes.Length)
        {
            if (bytes.Length - offset < 4)
                throw new InvalidDataException("WAL record length prefix is truncated.");
            int length = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(offset, 4));
            offset += 4;
            if (length < 1 || bytes.Length - offset < length + 16)
                throw new InvalidDataException("WAL record payload or digest is truncated.");
            if (bytes[offset] == (byte)type)
                count++;
            offset += length + 16;
        }
        return count;
    }

    private sealed class TestFullReplaceCommitLease : IFullReplaceCommitLease
    {
        private readonly Action _onDispose;
        private bool _disposed;

        public TestFullReplaceCommitLease(
            string scopeId, NodeId localReplica, int namespaceId,
            IMetadataNamespaceGcStore namespaceGcStore, TrustStore trustStore,
            Action onDispose)
        {
            ScopeId = scopeId;
            LocalReplica = localReplica;
            NamespaceId = namespaceId;
            NamespaceGcStore = namespaceGcStore;
            NamespaceGcTrustStore = trustStore;
            _onDispose = onDispose;
        }

        public string ScopeId { get; }
        public NodeId LocalReplica { get; }
        public int NamespaceId { get; }
        public IMetadataNamespaceGcStore NamespaceGcStore { get; }
        public TrustStore NamespaceGcTrustStore { get; }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _onDispose();
        }
    }

    private sealed class ThrowingGcReferenceScanner : IApplicationGcReferenceScanner
    {
        public bool Opened { get; private set; }

        public IApplicationGcReferenceScan OpenScan(string scopeId, NodeId localReplica, int namespaceId)
        {
            Opened = true;
            throw new InvalidOperationException("Host reference scan must not begin for a mismatched typed lease.");
        }
    }

    private sealed class NonReentrantGcReferenceScanner : IApplicationGcReferenceScanner
    {
        private readonly SemaphoreSlim _hostGate;
        private int _openCount;
        private int _disposeCount;

        public NonReentrantGcReferenceScanner(SemaphoreSlim hostGate) => _hostGate = hostGate;
        public int OpenCount => Volatile.Read(ref _openCount);
        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public IApplicationGcReferenceScan OpenScan(string scopeId, NodeId localReplica, int namespaceId)
        {
            if (_hostGate.CurrentCount != 0)
                throw new InvalidOperationException("Host reference scan must run under the already-held host commit lease.");
            Interlocked.Increment(ref _openCount);
            return new Scan(this);
        }

        private sealed class Scan : IApplicationGcReferenceScan
        {
            private readonly NonReentrantGcReferenceScanner _owner;
            public Scan(NonReentrantGcReferenceScanner owner) => _owner = owner;
            public ApplicationDraftHorizon UnpublishedLocalDraftHorizon => new(0, 0, 0);
            public bool HasExternalReceiptReference(NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId) => false;
            public bool HasExternalChangeIdReference(BatchId changeId) => false;
            public bool HasExternalRecordReference(RecordId recordId, BatchId tombstoneChangeId) => false;
            public void Dispose() => Interlocked.Increment(ref _owner._disposeCount);
        }
    }

    private sealed class CallbackLease : IDisposable
    {
        private Action? _onDispose;
        public CallbackLease(Action onDispose) => _onDispose = onDispose;
        public void Dispose() => Interlocked.Exchange(ref _onDispose, null)?.Invoke();
    }

    private sealed class FailOnceAcknowledgeProvider : IDataProvider, IProviderPublicationMode, IProviderCommitGuard, IProviderPublishedOriginStore
    {
        private readonly IDataProvider _inner;
        private readonly Func<ProviderChange, ProviderChange>? _changeTransform;
        private readonly Func<ulong, int, ProviderChangePage, ProviderChangePage>? _pageTransform;
        private readonly Action? _afterRemoteApply;
        private readonly Action<string>? _commitGuardEvent;
        private bool _failNextAcknowledge;

        public FailOnceAcknowledgeProvider(
            IDataProvider inner,
            Func<ProviderChange, ProviderChange>? changeTransform = null,
            Action? afterRemoteApply = null,
            Action<string>? commitGuardEvent = null,
            Func<ulong, int, ProviderChangePage, ProviderChangePage>? pageTransform = null,
            bool failFirstAcknowledge = true)
        {
            _inner = inner;
            _changeTransform = changeTransform;
            _pageTransform = pageTransform;
            _failNextAcknowledge = failFirstAcknowledge;
            _afterRemoteApply = afterRemoteApply;
            _commitGuardEvent = commitGuardEvent;
        }

        public int SnapshotOpenCount { get; private set; }

        public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey) =>
            _inner.ResolveRecordId(sourceItemKey);
        public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey) =>
            _inner.ResolveFieldId(sourceFieldKey);
        public void AppendLocalChange(ProviderMutation mutation) => _inner.AppendLocalChange(mutation);
        public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit)
        {
            ProviderChangePage page = _inner.ReadLocalChanges(afterSequence, limit);
            if (_changeTransform is not null)
                page = new ProviderChangePage(
                    page.Changes.Select(_changeTransform).ToArray(), page.ScannedThroughSequence);
            return _pageTransform is null ? page : _pageTransform(afterSequence, limit, page);
        }
        public ulong LocalChangeHighWatermark => _inner.LocalChangeHighWatermark;
        public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark) =>
            ((IProviderPublicationMode)_inner).TryGetMetadataStoreOwnedPublicationCutover(out nodeOutboxHighWatermark);
        public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark) =>
            ((IProviderPublicationMode)_inner).EnableMetadataStoreOwnedPublication(nodeOutboxHighWatermark);
        public IDisposable AcquireCommitGuard()
        {
            _commitGuardEvent?.Invoke("provider.acquire");
            IDisposable lease = ((IProviderCommitGuard)_inner).AcquireCommitGuard();
            return _commitGuardEvent is null
                ? lease
                : new CallbackLeaseWrapper(lease, () => _commitGuardEvent("provider.release"));
        }

        private sealed class CallbackLeaseWrapper : IDisposable
        {
            private IDisposable? _inner;
            private Action? _onDispose;
            public CallbackLeaseWrapper(IDisposable inner, Action onDispose)
            {
                _inner = inner;
                _onDispose = onDispose;
            }
            public void Dispose()
            {
                Interlocked.Exchange(ref _inner, null)?.Dispose();
                Interlocked.Exchange(ref _onDispose, null)?.Invoke();
            }
        }
        public void CommitPublishedOrigins(IReadOnlyList<ProviderPublishedOrigin> published) =>
            ((IProviderPublishedOriginStore)_inner).CommitPublishedOrigins(published);

        public void AcknowledgeLocalChanges(ulong safeThroughSequence)
        {
            if (_failNextAcknowledge)
            {
                _failNextAcknowledge = false;
                throw new IOException("Injected provider feed ACK interruption.");
            }
            _inner.AcknowledgeLocalChanges(safeThroughSequence);
        }

        public ProviderSnapshotHandle OpenSnapshot()
        {
            SnapshotOpenCount++;
            return _inner.OpenSnapshot();
        }
        public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit) =>
            _inner.ReadSnapshot(snapshotToken, afterIndex, limit);
        public void ReleaseSnapshot(Guid snapshotToken) => _inner.ReleaseSnapshot(snapshotToken);
        public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations)
        {
            ProviderApplyReceipt receipt = _inner.ApplyRemoteBatch(applyToken, mutations);
            _afterRemoteApply?.Invoke();
            return receipt;
        }
        public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation) =>
            _inner.TryReadCurrent(recordId, fieldId, out mutation);
        public void Dispose() { }
    }

    private static MetadataIncomingBatch CreatePendingProviderBatch(
        string scope, Node local, Node sender, int namespaceId, RecordId recordId,
        ushort fieldId, string value, byte fingerprintSeed)
    {
        Assert.True(sender.WriteField(namespaceId, recordId, fieldId,
            Encoding.UTF8.GetBytes(value), new Hlc(6000UL + (ulong)namespaceId, 0)));
        OutboxEntry entry = Assert.Single(sender.ReadOutboxEntries(namespaceId, sender.NodeId, 0, 10));
        byte[] envelope = new byte[16 + entry.Payload.Length];
        recordId.WriteTo(envelope);
        entry.Payload.Span.CopyTo(envelope.AsSpan(16));

        return new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local.NodeId,
            SenderPeer = sender.NodeId,
            SenderEpoch = checked((uint)sender.Epoch),
            AuthorStreamNodeId = sender.NodeId,
            NamespaceId = namespaceId,
            BatchId = BatchId.From(Guid.NewGuid().ToByteArray()),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)(fingerprintSeed + 1), 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = sender.NodeId,
                    ChangeId = entry.ChangeId,
                    Fingerprint = Enumerable.Repeat(fingerprintSeed, 16).ToArray(),
                    Payload = envelope,
                    RelayEligible = false,
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence
                }
            }
        };
    }


    [Theory]
    [InlineData("after-pending-apply")]
    [InlineData("after-node-checkpoint")]
    [InlineData("after-provider-commit")]
    [InlineData("after-metadata-finalize")]
    public async Task Process_kill_at_receive_durability_cut_recovers_pending_apply_and_provider_state(string crashCut)
    {
        string directory = Path.Combine(_root, "process-kill-" + crashCut);
        const ulong partitionId = 468;
        string scope = "process-kill-" + crashCut;
        // Each theory row runs independently and may execute in parallel. Keep its
        // rendezvous files inside the crash-cut-specific directory to avoid cross-row
        // PID/ready-file races that can kill the wrong child or strand a parent.
        string readyPath = Path.Combine(directory, "coordinator-child-ready");
        string ownerPidPath = Path.Combine(directory, "coordinator-child-pid");
        string? projectDirectory = AppContext.BaseDirectory;
        while (projectDirectory is not null &&
               !File.Exists(Path.Combine(projectDirectory, "BinSync.Storage.Integration.csproj")))
            projectDirectory = Directory.GetParent(projectDirectory)?.FullName;
        Assert.NotNull(projectDirectory);

        string configuration = Directory.GetParent(AppContext.BaseDirectory.TrimEnd(
            Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))?.Name ?? "Release";
        var start = new System.Diagnostics.ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            WorkingDirectory = projectDirectory!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add("test");
        start.ArgumentList.Add(Path.Combine(projectDirectory!, "BinSync.Storage.Integration.csproj"));
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-restore");
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add(configuration);
        start.ArgumentList.Add("--filter");
        start.ArgumentList.Add("FullyQualifiedName~Coordinator_process_kill_child_probe");
        start.ArgumentList.Add("--results-directory");
        start.ArgumentList.Add(Path.Combine(directory, "child-test-results"));
        start.Environment["BIN_SYNC_COORDINATOR_CRASH_DIR"] = directory;
        start.Environment["BIN_SYNC_COORDINATOR_CRASH_READY"] = readyPath;
        start.Environment["BIN_SYNC_COORDINATOR_CRASH_PID"] = ownerPidPath;
        start.Environment["BIN_SYNC_COORDINATOR_CRASH_CUT"] = crashCut;

        System.Diagnostics.Process? child = null;
        try
        {
            child = System.Diagnostics.Process.Start(start);
            Assert.NotNull(child);
            Task<string> stdout = child!.StandardOutput.ReadToEndAsync();
            Task<string> stderr = child.StandardError.ReadToEndAsync();

            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!File.Exists(readyPath) && !child.HasExited && DateTime.UtcNow < deadline)
                await Task.Delay(25);
            if (!File.Exists(readyPath))
            {
                if (!child.HasExited)
                    child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(File.Exists(readyPath),
                    $"Child did not reach the post-checkpoint crash cut. stdout={await stdout} stderr={await stderr}");
            }

            int ownerPid = int.Parse(File.ReadAllText(ownerPidPath));
            using (var owner = System.Diagnostics.Process.GetProcessById(ownerPid))
            {
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }

            if (!child.HasExited)
                child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));

            StorageDatabase? reopenedStorage = null;
            Exception? reopenError = null;
            DateTime reopenDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (reopenedStorage is null && DateTime.UtcNow < reopenDeadline)
            {
                try { reopenedStorage = StorageDatabase.Open(directory, partitionId); }
                catch (IOException ex)
                {
                    reopenError = ex;
                    await Task.Delay(50);
                }
            }
            Assert.True(reopenedStorage is not null,
                $"Storage WAL did not release after owner process termination. {reopenError}");
            using var storage = reopenedStorage!;
            using var provider = new StorageDataProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            NodeId receiverId;
            using (var identity = NewNode(122))
                receiverId = identity.NodeId;
            using var receiver = StorageNodePersistence.Load(storage, receiverId);

            IReadOnlyList<MetadataPendingApplyIntent> pendingIntents =
                metadata.ReadPendingApplyIntents(scope, receiverId);
            Assert.Equal(crashCut == "after-metadata-finalize" ? 0 : 1, pendingIntents.Count);
            RecordId record = RecordId.From(Guid.Parse("7a000000-0000-0000-0000-00000000007a").ToByteArray());
            bool nodeAlreadyCheckpointed = receiver.TryGetField(1, record, 0x087a, out var checkpointed);
            Assert.Equal(crashCut != "after-pending-apply", nodeAlreadyCheckpointed);
            if (nodeAlreadyCheckpointed)
                Assert.Equal("crash-cut-" + crashCut, Encoding.UTF8.GetString(checkpointed!.Value));
            bool providerAlreadyCommitted = provider.TryReadCurrent(record, 0x087a, out var providerBeforeRecovery);
            Assert.Equal(crashCut is "after-provider-commit" or "after-metadata-finalize", providerAlreadyCommitted);
            if (providerAlreadyCommitted)
                Assert.Equal("crash-cut-" + crashCut, Encoding.UTF8.GetString(providerBeforeRecovery!.Value));

            var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);
            if (crashCut == "after-metadata-finalize")
            {
                using var senderIdentity = NewNode(121);
                Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiverId, senderIdentity.NodeId, 1, 1));
                Assert.Equal(0, coordinator.RecoverPendingApplies(
                    node => StorageNodePersistence.Save(storage, node)));
            }
            else
            {
                Assert.Equal(1, coordinator.RecoverPendingApplies(
                    node => StorageNodePersistence.Save(storage, node)));
            }
            Assert.Empty(metadata.ReadPendingApplyIntents(scope, receiverId));
            Assert.True(provider.TryReadCurrent(record, 0x087a, out var recovered));
            Assert.Equal("crash-cut-" + crashCut, Encoding.UTF8.GetString(recovered!.Value));
            Assert.True(receiver.TryGetField(1, record, 0x087a, out var recoveredNode));
            Assert.Equal("crash-cut-" + crashCut, Encoding.UTF8.GetString(recoveredNode!.Value));
        }
        finally
        {
            if (child is not null)
            {
                try { if (!child.HasExited) child.Kill(entireProcessTree: true); } catch { }
                try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
                child.Dispose();
            }
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Coordinator_process_kill_child_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("BIN_SYNC_COORDINATOR_CRASH_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("BIN_SYNC_COORDINATOR_CRASH_READY");
        string? ownerPidPath = Environment.GetEnvironmentVariable("BIN_SYNC_COORDINATOR_CRASH_PID");
        string? crashCut = Environment.GetEnvironmentVariable("BIN_SYNC_COORDINATOR_CRASH_CUT");
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(readyPath) ||
            string.IsNullOrWhiteSpace(ownerPidPath) || string.IsNullOrWhiteSpace(crashCut))
            return;

        const ulong partitionId = 468;
        string scope = "process-kill-" + crashCut;
        RecordId record = RecordId.From(Guid.Parse("7a000000-0000-0000-0000-00000000007a").ToByteArray());
        using var storage = StorageDatabase.Open(directory, partitionId);
        using var innerProvider = new StorageDataProvider(storage);
        Action signalAndBlock = () =>
        {
            File.WriteAllText(ownerPidPath, Environment.ProcessId.ToString(
                System.Globalization.CultureInfo.InvariantCulture));
            File.WriteAllText(readyPath, crashCut);
            Thread.Sleep(Timeout.Infinite);
        };
        using IDataProvider provider = crashCut == "after-provider-commit"
            ? new FailOnceAcknowledgeProvider(innerProvider, afterRemoteApply: signalAndBlock)
            : innerProvider;
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(121);
        using var receiver = NewNode(122);

        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        // Persist the negotiated alias table before any durable receive intent can
        // be staged; recovery decodes the exact wire payload after process restart.
        StorageNodePersistence.Save(storage, receiver);
        Assert.True(sender.WriteField(1, record, 0x087a,
            Encoding.UTF8.GetBytes("crash-cut-" + crashCut), new Hlc(8700, 0)));

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);
        if (crashCut == "after-pending-apply")
        {
            Task<BatchResult> receiveBeforeNodeApply = acceptor.ReceiveBatchAsync(receiver,
                beforeApply: null,
                beforeApplyWithOperationDecisions: (batch, payload, peer) =>
                {
                    var plan = coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                    signalAndBlock();
                    return plan.ApplyOperations;
                },
                beforeAck: null,
                receiveStateOwnedByMetadataStore: true);
            Task<bool> sendBeforeNodeApply = initiator.PushBatchAsync(sender, 1);
            await Task.WhenAll(receiveBeforeNodeApply, sendBeforeNodeApply);
        }
        else if (crashCut == "after-metadata-finalize")
        {
            StorageRemoteApplyPlan? plan = null;
            Task<BatchResult> receiveBeforeAck = acceptor.ReceiveBatchAsync(receiver,
                beforeApply: null,
                beforeApplyWithOperationDecisions: (batch, payload, peer) =>
                {
                    plan = coordinator.BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), peer);
                    return plan.ApplyOperations;
                },
                beforeAck: (batch, _) =>
                {
                    var accepted = plan ?? throw new InvalidOperationException("PendingApply was not staged.");
                    coordinator.ApplyRemoteBatchAfterNodeApply(batch, accepted,
                        node => StorageNodePersistence.Save(storage, node));
                    signalAndBlock();
                },
                receiveStateOwnedByMetadataStore: true);
            Task<bool> sendBeforeAck = initiator.PushBatchAsync(sender, 1);
            await Task.WhenAll(receiveBeforeAck, sendBeforeAck);
        }
        else
        {
            Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, node =>
            {
                StorageNodePersistence.Save(storage, node);
                if (crashCut == "after-node-checkpoint")
                    signalAndBlock();
            });
            Task<bool> send = initiator.PushBatchAsync(sender, 1);
            await Task.WhenAll(receive, send).WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
    [Fact]
    public void Metadata_store_owned_publication_requires_legacy_drain_and_fences_node_writes()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-owned-cutover"), 459);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(59);
        const string scope = "metadata-owned-cutover";
        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        RecordId firstRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000001").ToByteArray());
        RecordId secondRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000002").ToByteArray());
        RecordId nodeOnlyRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000003").ToByteArray());

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = firstRecord, FieldId = 0x081D, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6300, Value = "legacy-pending"u8.ToArray()
        });
        Assert.Throws<InvalidOperationException>(() =>
            coordinator.EnableMetadataStoreOwnedPublication(_ => { }));

        // Before cutover, the existing Node-Outbox path drains and freezes the
        // pending provider feed. Only an empty, aligned boundary can be enabled.
        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
        Assert.Equal(0, node.GetOutboxCount(1));
        Assert.Equal(1L, node.GetOutboxHighWatermark(1, node.NodeId));

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = secondRecord, FieldId = 0x081E, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6400, Value = "metadata-owned"u8.ToArray()
        });
        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }));
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Equal(1L, node.GetOutboxHighWatermark(1, node.NodeId));
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
        Assert.Equal(0, node.GetOutboxCount(1));

        // The cutover fences both local Node writes and the legacy Node-owned
        // batch builder before either can create an untracked outbound operation.
        Assert.Throws<InvalidOperationException>(() =>
            node.WriteField(1, nodeOnlyRecord, 0x081F, "node-only"u8.ToArray(), new Hlc(6500, 0)));
        node.E2eeKeys = new E2eeKeyRing();
        node.E2eeKeys.Set(1, new byte[32]);
        Assert.Throws<InvalidOperationException>(() =>
            node.WriteFieldSealed(1, nodeOnlyRecord, 0x081F, "sealed-node-only"u8.ToArray(), new Hlc(6500, 1)));
        // The authority fence is namespace-scoped; unrelated namespaces retain
        // the ordinary Node-owned path.
        RecordId otherNamespaceRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000004").ToByteArray());
        Assert.True(node.WriteField(2, otherNamespaceRecord, 0x0822, "other-namespace"u8.ToArray(), new Hlc(6501, 0)));
        Assert.Equal(1L, node.GetOutboxHighWatermark(2, node.NodeId));
        using var peer = NewNode(60);
        Assert.Throws<InvalidOperationException>(() => node.CreateDeltaBatch(peer.NodeId, 1));
        Assert.Throws<InvalidOperationException>(() => node.PushOutbox(1, nodeOnlyRecord,
            new OpFieldWrite { FieldId = 0x0821, Wall = 6501, Logical = 0, Value = "legacy"u8.ToArray() }));

        // The namespace fence also blocks the legacy Node-owned receive path.
        RecordId legacyInboundRecord = RecordId.From(Guid.Parse("59000000-0000-0000-0000-000000000005").ToByteArray());
        Assert.True(peer.WriteField(1, legacyInboundRecord, 0x0823,
            "legacy-inbound"u8.ToArray(), new Hlc(6502, 0)));
        Transport.MakeSession(peer, node);
        Assert.Throws<InvalidOperationException>(() => Transport.DeliverBatch(peer, node, 1));
        Assert.False(node.TryGetField(1, legacyInboundRecord, 0x0823, out _));

        // The same logical write remains valid through the provider-owned path.
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = nodeOnlyRecord, FieldId = 0x081F, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6500, Value = "node-only"u8.ToArray()
        });
        Assert.Equal(1, coordinator.PublishLocalChanges(_ => { }));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(3UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));
        Assert.Equal(1L, node.GetOutboxHighWatermark(1, node.NodeId));
        Assert.Empty(node.ReadOutboxEntries(1, node.NodeId, 0, 10));
    }


    [Fact]
    public void Reconstructed_coordinator_reinstalls_Node_write_fence_from_durable_cutover_marker()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "reinstall-node-cutover-fence"), 460);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var originalNode = NewNode(61);
        const string scope = "reinstall-node-cutover-fence";

        var originalCoordinator = new StorageSyncCoordinator(originalNode, provider, 1, metadata, scope);
        originalCoordinator.EnableMetadataStoreOwnedPublication(_ => { });

        // A freshly reconstructed Node has no in-memory fence bit. The provider's
        // durable cutover marker must reinstall it in the coordinator constructor.
        using var restartedNode = NewNode(61);
        _ = new StorageSyncCoordinator(restartedNode, provider, 1, metadata, scope);
        RecordId record = RecordId.From(Guid.Parse("61000000-0000-0000-0000-000000000001").ToByteArray());
        Assert.Throws<InvalidOperationException>(() =>
            restartedNode.WriteField(1, record, 0x0820, "must-use-provider"u8.ToArray(), new Hlc(6600, 0)));
        Assert.Equal(0L, restartedNode.GetOutboxHighWatermark(1, restartedNode.NodeId));
    }


    [Fact]
    public void Metadata_owned_freeze_survives_provider_ack_failure_and_appends_later_suffix_with_store_allocated_origin()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-freeze-ack-retry"), 466);
        using var inner = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var node = NewNode(68);
        const string scope = "metadata-freeze-ack-retry";
        RecordId record = RecordId.From(Guid.Parse("68000000-0000-0000-0000-000000000068").ToByteArray());

        for (ushort field = 0x0800; field < 0x0805; field++)
            inner.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = field, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
                HlcWall = field, Value = new byte[] { (byte)field }
            });
        var seedPage = inner.ReadLocalChanges(0, 16);
        inner.AcknowledgeLocalChanges(seedPage.ScannedThroughSequence);
        using var provider = new FailOnceAcknowledgeProvider(inner);

        var coordinator = new StorageSyncCoordinator(node, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0810, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 6000, Value = "first-frozen"u8.ToArray()
        });
        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => { }, limit: 2));

        var firstPendingFeed = Assert.Single(provider.ReadLocalChanges(0, 16).Changes);
        Assert.Equal(6UL, firstPendingFeed.Sequence);
        Assert.Equal(1UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));

        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = record, FieldId = 0x0810, AuthorNodeId = node.NodeId,
            OriginAuthorEpoch = 1,
            HlcWall = 7000, Value = "later-suffix"u8.ToArray()
        });
        var pendingFeed = provider.ReadLocalChanges(0, 16).Changes;
        Assert.Equal(new ulong[] { 6, 7 }, pendingFeed.Select(change => change.Sequence).ToArray());

        Assert.Equal(2, coordinator.PublishLocalChanges(_ => { }, limit: 2));
        Assert.Empty(provider.ReadLocalChanges(0, 16).Changes);
        Assert.Equal(2UL, metadata.GetOutboundHighWatermark(scope, node.NodeId, 1, node.NodeId));

        ulong lease = metadata.AcquireOutboundLease(scope, node.NodeId, 1, node.NodeId, Guid.NewGuid());
        var outbound = metadata.ReadOutbound(scope, node.NodeId, 1, node.NodeId, lease, 0, 16).Items;
        Assert.Equal(2, outbound.Count);
        Assert.Equal(new ulong[] { 1, 2 }, outbound.Select(item => item.Sequence).ToArray());
        Assert.Equal(new ulong[] { 1, 2 }, outbound.Select(item => item.Provenance.Sequence).ToArray());
        Assert.Equal(pendingFeed[0].ChangeId, outbound[0].ChangeId);
        Assert.Equal(pendingFeed[1].ChangeId, outbound[1].ChangeId);

        string[] values = outbound.Select(item =>
                Ops.DecodeOp(item.Payload.AsSpan(16).ToArray()).Op)
            .OfType<OpFieldWrite>()
            .Select(operation => Encoding.UTF8.GetString(operation.Value))
            .ToArray();
        Assert.Equal(new[] { "first-frozen", "later-suffix" }, values);
        Assert.NotEqual(pendingFeed[0].Sequence, outbound[0].Provenance.Sequence);
        Assert.NotEqual(pendingFeed[1].Sequence, outbound[1].Provenance.Sequence);
    }

    [Fact]
    public async Task Metadata_authoritative_receive_remains_enabled_after_provider_cutover()
    {
        using var storage = StorageDatabase.Open(Path.Combine(_root, "metadata-receive-after-cutover"), 465);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(66);
        using var receiver = NewNode(67);
        const string scope = "metadata-receive-after-cutover";
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1, metadata, scope);
        coordinator.EnableMetadataStoreOwnedPublication(_ => { });

        RecordId record = RecordId.From(Guid.Parse("66000000-0000-0000-0000-000000000006").ToByteArray());
        Assert.True(sender.WriteField(1, record, 0x0824,
            "metadata-authoritative"u8.ToArray(), new Hlc(6700, 0)));

        var (toSender, toReceiver) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(receiver, toReceiver);
        var connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        Task<BatchResult> receive = coordinator.ReceiveBatchBeforeAckAsync(acceptor, _ => { });
        Task<bool> send = initiator.PushBatchAsync(sender, 1);
        await Task.WhenAll(receive, send).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(provider.TryReadCurrent(record, 0x0824, out var current));
        Assert.Equal("metadata-authoritative", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(1UL, metadata.GetReceiveCursor(scope, receiver.NodeId, sender.NodeId,
            checked((uint)sender.Epoch), 1));
        Assert.Single(metadata.ReadRelay(scope, receiver.NodeId, 1, sender.NodeId, 0, 10).Items);
        Assert.Empty(receiver.ReadOutboxEntries(1, sender.NodeId, 0, 10));
    }

}
