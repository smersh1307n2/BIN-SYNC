using BinSync;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Storage;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldDatabaseIpcAsyncDataProviderTests
{

    private sealed class CrashAfterTombstoneCandidateRead : IAsyncProviderTombstoneGc
    {
        private readonly IAsyncProviderTombstoneGc _inner;
        private readonly Process _host;

        public CrashAfterTombstoneCandidateRead(IAsyncProviderTombstoneGc inner, Process host)
        {
            _inner = inner;
            _host = host;
        }

        public async ValueTask<IReadOnlyList<ProviderMutation>> ReadTombstoneGcCandidatesAsync(
            int limit, CancellationToken cancellationToken = default)
        {
            // Leave a durable provider snapshot pin open, then kill the Host before
            // candidate enumeration or physical deletion. Reopen must discard the stale
            // process-owned pin while preserving the tombstone.
            var provider = (IAsyncDataProvider)_inner;
            ProviderSnapshotHandle snapshot = await provider.OpenSnapshotAsync(cancellationToken);
            if (snapshot.Token == Guid.Empty)
                throw new InvalidDataException("Crash probe did not obtain a durable provider snapshot.");
            _host.Kill(entireProcessTree: true);
            await _host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            throw new IOException("Injected host death with a provider GC snapshot pin open.");
        }

        public ValueTask<int> CompactTombstonesAsync(
            IReadOnlyList<ProviderMutation> approvedCandidates,
            int limit, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("The pre-delete crash cut must not compact candidates.");
    }

    private sealed class CrashAfterTombstoneDelete : IAsyncProviderTombstoneGc
    {
        private readonly IAsyncProviderTombstoneGc _inner;
        private readonly Process _host;

        public CrashAfterTombstoneDelete(IAsyncProviderTombstoneGc inner, Process host)
        {
            _inner = inner;
            _host = host;
        }

        public ValueTask<IReadOnlyList<ProviderMutation>> ReadTombstoneGcCandidatesAsync(
            int limit, CancellationToken cancellationToken = default)
            => _inner.ReadTombstoneGcCandidatesAsync(limit, cancellationToken);

        public async ValueTask<int> CompactTombstonesAsync(
            IReadOnlyList<ProviderMutation> approvedCandidates,
            int limit, CancellationToken cancellationToken = default)
        {
            int deleted = await _inner.CompactTombstonesAsync(
                approvedCandidates, limit, cancellationToken);
            _host.Kill(entireProcessTree: true);
            await _host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            throw new IOException($"Injected host death after durable tombstone delete ({deleted}) and before coordinator completion.");
        }
    }
    private sealed class BlockingAsyncGcReferenceScanner : IAsyncApplicationGcReferenceScanner
    {
        private readonly IAsyncApplicationGcReferenceScanner _inner;
        private readonly TaskCompletionSource<bool> _referenceCheckReached =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _continueReferenceCheck =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public BlockingAsyncGcReferenceScanner(IAsyncApplicationGcReferenceScanner inner)
            => _inner = inner ?? throw new ArgumentNullException(nameof(inner));

        public Task ReferenceCheckReached => _referenceCheckReached.Task;
        public void ContinueReferenceCheck() => _continueReferenceCheck.TrySetResult(true);

        public async ValueTask<IAsyncApplicationGcReferenceScan> OpenScanAsync(
            string scopeId, NodeId localReplica, int namespaceId,
            CancellationToken cancellationToken = default)
        {
            IAsyncApplicationGcReferenceScan scan = await _inner.OpenScanAsync(
                scopeId, localReplica, namespaceId, cancellationToken).ConfigureAwait(false);
            return new BlockingScan(scan, _referenceCheckReached, _continueReferenceCheck);
        }

        private sealed class BlockingScan(
            IAsyncApplicationGcReferenceScan inner,
            TaskCompletionSource<bool> reached,
            TaskCompletionSource<bool> proceed) : IAsyncApplicationGcReferenceScan
        {
            private int _blocked;

            public ValueTask<ApplicationDraftHorizon> GetUnpublishedLocalDraftHorizonAsync(
                CancellationToken cancellationToken = default)
                => inner.GetUnpublishedLocalDraftHorizonAsync(cancellationToken);

            public ValueTask<bool> HasExternalReceiptReferenceAsync(
                NodeId senderPeer, uint authorEpoch, uint transportPeerEpoch, BatchId batchId,
                CancellationToken cancellationToken = default)
                => inner.HasExternalReceiptReferenceAsync(
                    senderPeer, authorEpoch, transportPeerEpoch, batchId, cancellationToken);

            public async ValueTask<bool> HasExternalChangeIdReferenceAsync(
                BatchId changeId, CancellationToken cancellationToken = default)
            {
                bool referenced = await inner.HasExternalChangeIdReferenceAsync(
                    changeId, cancellationToken).ConfigureAwait(false);
                if (!referenced && Interlocked.Exchange(ref _blocked, 1) == 0)
                {
                    reached.TrySetResult(true);
                    await proceed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                return referenced;
            }

            public ValueTask<bool> HasExternalRecordReferenceAsync(
                RecordId recordId, BatchId tombstoneChangeId,
                CancellationToken cancellationToken = default)
                => inner.HasExternalRecordReferenceAsync(
                    recordId, tombstoneChangeId, cancellationToken);

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    [Fact]
    public async Task Async_provider_fails_closed_when_host_rejects_gc_lease_renewal()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-ipc-provider-gc-renew-fail-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x765;
        byte[] authorBytes = Id(0x66);
        Guid hostOwner = Guid.NewGuid();
        using var shutdown = new CancellationTokenSource();
        using FldDatabase database = FldDatabase.Open(root, namespaceId, authorBytes);
        var dispatcher = new FldDatabaseIpcDispatcher(
            database, gcLeaseIdleTimeout: TimeSpan.FromSeconds(5));
        var renewalRejected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int rejectNextRenewal = 1;
        async ValueTask<ReadOnlyMemory<byte>> HandleAsync(
            StorageIpcRequestContext context, ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken)
        {
            StorageIpcOperationEnvelope envelope = StorageIpcOperationProtocol.Decode(payload);
            if (envelope.OperationCode == FldDatabaseIpcOperationCodes.RenewGcLease &&
                Interlocked.Exchange(ref rejectNextRenewal, 0) == 1)
            {
                renewalRejected.TrySetResult();
                var rejected = new StorageIpcOperationEnvelope(
                    envelope.OperationId, envelope.OperationCode,
                    FldDatabaseIpcProtocol.EncodeGcLeaseToken(Guid.NewGuid()));
                return await dispatcher.HandleAsync(
                    context, StorageIpcOperationProtocol.Encode(rejected), cancellationToken)
                    .ConfigureAwait(false);
            }
            return await dispatcher.HandleAsync(context, payload, cancellationToken).ConfigureAwait(false);
        }
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcAsyncDataProvider? provider = null;
        FldDatabaseIpcClient? competingClient = null;
        IAsyncApplicationGcReferenceScan? scan = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, hostOwner, Environment.ProcessId));
            provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10),
                NodeId.From(authorBytes), () => 1,
                cancellationToken: default,
                gcLeaseRenewalInterval: TimeSpan.FromMilliseconds(200));
            competingClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));
            scan = await provider.OpenScanAsync(
                "lease-renewal-failure", NodeId.From(authorBytes), checked((int)namespaceId));
            Task<byte[]> blockedReader = competingClient.GetLocalAuthorAsync();

            await renewalRejected.Task.WaitAsync(TimeSpan.FromSeconds(5));
            // The test hook signals when the Host rejects the heartbeat, just before
            // the client-side renewal task observes the failed response. Poll the
            // public scan contract until that failure has propagated; do not race it.
            IOException? renewalFailure = null;
            for (int attempt = 0; attempt < 100 && renewalFailure is null; attempt++)
            {
                try
                {
                    _ = await scan.HasExternalChangeIdReferenceAsync(BatchId.From(Id(0x67)));
                }
                catch (IOException error)
                {
                    renewalFailure = error;
                }

                if (renewalFailure is null)
                    await Task.Delay(20);
            }
            Assert.NotNull(renewalFailure);

            // A rejected heartbeat makes the immutable scan unusable. Disposal still
            // releases the snapshot and lease; the waiting Host operation then proceeds.
            await scan.DisposeAsync();
            scan = null;
            Assert.Equal(authorBytes, await blockedReader.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            if (scan is not null)
            {
                try { await scan.DisposeAsync(); } catch { }
            }
            if (competingClient is not null)
                await competingClient.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, hostOwner);
            dispatcher.Dispose();
        }

        try { Directory.Delete(root, recursive: true); } catch { }
    }

    [Fact]
    public async Task Async_provider_renews_host_gc_lease_during_long_lived_reference_scan()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-ipc-provider-gc-renew-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x764;
        byte[] authorBytes = Id(0x64);
        Guid hostOwner = Guid.NewGuid();
        using var shutdown = new CancellationTokenSource();
        using FldDatabase database = FldDatabase.Open(root, namespaceId, authorBytes);
        var dispatcher = new FldDatabaseIpcDispatcher(
            database, gcLeaseIdleTimeout: TimeSpan.FromSeconds(5));
        var renewalReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRenewal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int blockNextRenewal = 0;
        async ValueTask<ReadOnlyMemory<byte>> HandleAsync(
            StorageIpcRequestContext context, ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken)
        {
            StorageIpcOperationEnvelope envelope = StorageIpcOperationProtocol.Decode(payload);
            if (envelope.OperationCode == FldDatabaseIpcOperationCodes.RenewGcLease &&
                Interlocked.Exchange(ref blockNextRenewal, 0) == 1)
            {
                renewalReached.TrySetResult();
                await allowRenewal.Task.WaitAsync(cancellationToken);
            }
            return await dispatcher.HandleAsync(context, payload, cancellationToken).ConfigureAwait(false);
        }
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0),
            HandleAsync,
            clientDisconnected: dispatcher.OnClientDisconnected);
        Task serverTask = server.RunAsync(shutdown.Token);
        FldDatabaseIpcAsyncDataProvider? provider = null;
        FldDatabaseIpcClient? competingClient = null;
        IAsyncApplicationGcReferenceScan? scan = null;

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, hostOwner, Environment.ProcessId));
            provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10),
                NodeId.From(authorBytes), () => 1,
                cancellationToken: default,
                gcLeaseRenewalInterval: TimeSpan.FromMilliseconds(500));
            competingClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, Guid.NewGuid(), TimeSpan.FromSeconds(10));

            Task<byte[]> blockedReader;
            scan = await provider.OpenScanAsync(
                "lease-renewal-test", NodeId.From(authorBytes), checked((int)namespaceId));
            blockedReader = competingClient.GetLocalAuthorAsync();
            await Task.Delay(12000);
            Assert.False(blockedReader.IsCompleted,
                "The Host-wide boundary must remain held beyond multiple idle TTLs while the async provider heartbeat is active.");
            Assert.False(await scan.HasExternalChangeIdReferenceAsync(BatchId.From(Id(0x65))));

            // Stop on a deterministic in-flight heartbeat. Disposing the scan must
            // wait for that already-sent IPC request rather than cancelling it and
            // faulting the single-request named-pipe connection.
            Interlocked.Exchange(ref blockNextRenewal, 1);
            await renewalReached.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Task scanDispose = scan.DisposeAsync().AsTask();
            await Task.Delay(100);
            Assert.False(scanDispose.IsCompleted,
                "Scan disposal must drain the in-flight renewal before releasing the Host lease.");
            allowRenewal.TrySetResult();
            await scanDispose.WaitAsync(TimeSpan.FromSeconds(5));
            scan = null;

            Assert.Equal(authorBytes, await blockedReader.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            // Never strand the provider's single-scan gate if an assertion or
            // heartbeat timeout fails before the normal disposal path.
            allowRenewal.TrySetResult();
            if (scan is not null)
            {
                try { await scan.DisposeAsync(); } catch { }
            }
            if (competingClient is not null)
                await competingClient.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, hostOwner);
            dispatcher.Dispose();
        }

        try { Directory.Delete(root, recursive: true); } catch { }
    }

    [Fact]
    public async Task Async_provider_opens_one_host_owned_reference_scan_for_changeid_record_and_receipt()
    {
        string hostRoot = Path.Combine(
            Path.GetTempPath(), "fld-binsync-host-gc-reference-scan-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x535;
        const string scope = "fld-host-gc-reference-scan";
        byte[] authorBytes = Id(0x65);
        NodeId author = NodeId.From(authorBytes);
        byte[] sender = Id(0x51);
        byte[] issuer = Id(0x53);
        byte[] record = FldDatabase.NewRecordId();
        byte[] changeId = Id(0x71);
        byte[] batchId = Id(0x52);
        Guid ownerId = Guid.Empty;
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? provider = null;

        try
        {
            using (var database = FldDatabase.Open(hostRoot, namespaceId, authorBytes))
            {
                var mutation = new RemoteFieldWrite(
                    record, 5, "gc-reference"u8.ToArray(), 100, 1, issuer, changeId,
                    keyEpoch: 0, isTombstone: false, originAuthorEpoch: 8, originSequence: 9);
                var batch = new RemoteBatchCommit(
                    senderPeer: sender,
                    senderEpoch: 6,
                    namespaceId: checked((int)namespaceId),
                    issuerNodeId: issuer,
                    issuerEpoch: 8,
                    batchId: batchId,
                    cursorStart: 9,
                    cursorEnd: 9,
                    previousBatchDigest: Id(0x54),
                    batchDigest: Id(0x55),
                    recordsRoot: Id(0x56),
                    writes: [mutation],
                    relayWrites: [mutation],
                    protocolStateDelta: [0xA1]);
                Assert.False(database.ApplyRemoteBatch(batch).IsDuplicate);
                Assert.NotEqual(0UL, database.Checkpoint());
            }

            host = StartDatabaseHost(hostRoot, namespaceId, authorBytes, enableTombstoneGc: false);
            StorageIpcEndpointInfo endpoint = await WaitForHostEndpointAsync(
                hostRoot, host, TimeSpan.FromSeconds(45));
            ownerId = endpoint.OwnerId;
            provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15), author, () => 8);

            var scanner = Assert.IsAssignableFrom<IAsyncApplicationGcReferenceScanner>(provider);
            await using IAsyncApplicationGcReferenceScan scan = await scanner.OpenScanAsync(
                scope, author, checked((int)namespaceId));
            ApplicationDraftHorizon horizon =
                await scan.GetUnpublishedLocalDraftHorizonAsync();
            Assert.Equal(0UL, horizon.Count);
            Assert.True(await scan.HasExternalChangeIdReferenceAsync(BatchId.From(changeId)));
            Assert.True(await scan.HasExternalRecordReferenceAsync(
                RecordId.From(record), BatchId.From(changeId)));
            Assert.False(await scan.HasExternalReceiptReferenceAsync(
                NodeId.From(sender), authorEpoch: 8, transportPeerEpoch: 6,
                BatchId.From(batchId)),
                "The sealed FLD ApplyJournal receipt is independent replay evidence, not a live BIN-SYNC Metadata receipt reference.");
            Assert.False(await scan.HasExternalChangeIdReferenceAsync(BatchId.From(Id(0x79))));
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            if (ownerId != Guid.Empty)
                StorageIpcEndpoint.DeleteIfOwned(hostRoot, 0, ownerId);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_host_adapter_compacts_receipt_but_preserves_relay_referenced_changeid()
    {
        string hostRoot = Path.Combine(
            Path.GetTempPath(), "fld-binsync-host-metadata-gc-" + Guid.NewGuid().ToString("N"));
        string nodeRoot = Path.Combine(
            Path.GetTempPath(), "binsync-host-metadata-gc-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x537;
        const string scope = "fld-host-async-metadata-gc";
        byte[] localAuthorBytes = Id(0x65);
        NodeId local = NodeId.From(localAuthorBytes);
        NodeId authority = NodeId.From(Id(0x11));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(authority, keys[0].PublicKey, TrustMode.PreShared);
        Guid ownerId = Guid.Empty;
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? provider = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteApply = null;
        FldDatabaseIpcClient? writerClient = null;
        CancellationTokenSource? gcCancellation = null;
        byte[] recordId = FldDatabase.NewRecordId();
        byte[] changeId = Id(0x76);
        byte[] batchId = Id(0x73);
        byte[] batchDigest = Id(0x75);

        try
        {
            using (var database = FldDatabase.Open(hostRoot, namespaceId, localAuthorBytes))
            {
                var mutation = new RemoteFieldWrite(
                    recordId, 7, "metadata-gc-reference"u8.ToArray(), 100, 1,
                    authority.ToArray(), changeId, keyEpoch: 0, isTombstone: false,
                    originAuthorEpoch: 8, originSequence: 9);
                var remoteBatch = new RemoteBatchCommit(
                    senderPeer: authority.ToArray(),
                    senderEpoch: 1,
                    namespaceId: checked((int)namespaceId),
                    issuerNodeId: authority.ToArray(),
                    issuerEpoch: 8,
                    batchId: batchId,
                    cursorStart: 1,
                    cursorEnd: 1,
                    previousBatchDigest: new byte[16],
                    batchDigest: batchDigest,
                    recordsRoot: Id(0x77),
                    writes: [mutation],
                    relayWrites: [],
                    protocolStateDelta: [0xA1]);
                Assert.False(database.ApplyRemoteBatch(remoteBatch).IsDuplicate);
                Assert.NotEqual(0UL, database.Checkpoint());
            }

            host = StartDatabaseHost(hostRoot, namespaceId, localAuthorBytes, enableTombstoneGc: false);
            StorageIpcEndpointInfo endpoint = await WaitForHostEndpointAsync(
                hostRoot, host, TimeSpan.FromSeconds(45));
            ownerId = endpoint.OwnerId;
            provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15), local, () => 17);
            // Keep a second independent IPC session ready before GC takes the Host-wide
            // lease; it will attempt a local write during the final Metadata commit cut.
            writerClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15));
            remoteApply = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRoot, Guid.NewGuid(), checked((ushort)namespaceId), TimeSpan.FromSeconds(15));

            using var nodeStorage = StorageDatabase.Open(nodeRoot, 903);
            using var metadata = new StorageMetadataStore(nodeStorage);
            using var node = new NodeBuilder().WithNodeId(local).WithEpoch(17).Build();
            var gc = new StorageNamespaceGcStore(
                nodeStorage, scope, local,
                new StorageNamespaceGcFeatureOptions(
                    EnableReceiptGc: true, EnableChangeIdGc: true));

            var roster = new NamespaceRosterCertificate
            {
                NamespaceId = checked((int)namespaceId),
                RosterGeneration = 1,
                PreviousRosterDigest = new byte[32],
                AuthorityNodeId = authority,
                Members =
                [
                    new NamespaceRosterMember
                    {
                        NodeId = authority,
                        PublicKey = keys[0].PublicKey,
                        TransportPeerEpoch = 10,
                        Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = local,
                        PublicKey = keys[1].PublicKey,
                        TransportPeerEpoch = 11,
                        Status = NamespaceMemberStatus.Active
                    }
                ]
            };
            roster = gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);

            // The Metadata receipt belongs to an old transport incarnation. Its
            // durable receive chain remains the replay fence after receipt GC.
            metadata.RegisterInterestedPeer(scope, local, authority, 1, checked((int)namespaceId));
            var incoming = new MetadataIncomingBatch
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = authority,
                SenderEpoch = 8,
                TransportPeerEpoch = 1,
                AuthorStreamNodeId = authority,
                NamespaceId = checked((int)namespaceId),
                BatchId = BatchId.From(batchId),
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = batchDigest,
                HeaderFingerprint = Id(0x78),
                Operations =
                [
                    new MetadataOperation
                    {
                        AuthorNodeId = authority,
                        ChangeId = BatchId.From(changeId),
                        OriginAuthorEpoch = 8,
                        OriginSequence = 9,
                        Fingerprint = Id(0x79),
                        Payload = [0x79]
                    }
                ]
            };
            Assert.False(metadata.AcceptIncomingBatch(incoming).IsDuplicate);
            metadata.RegisterInterestedPeer(scope, local, authority, 2, checked((int)namespaceId));
            metadata.CompletePeerInitialSync(
                scope, local, authority, 2, checked((int)namespaceId),
                Array.Empty<MetadataPeerStreamBaseline>());
            metadata.RetireInterestedPeer(
                scope, local, authority, 1, 2, checked((int)namespaceId));

            var report0 = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                new AppliedOriginFrontierReport
                {
                    NamespaceId = checked((int)namespaceId),
                    RosterGeneration = 1,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    MemberNodeId = authority,
                    MemberTransportEpoch = 10,
                    ReportSequence = 1,
                    Entries =
                    [
                        new AppliedOriginFrontierEntry
                        {
                            OriginAuthorNodeId = authority,
                            OriginAuthorEpoch = 8,
                            ContiguousAppliedOriginSequence = 9
                        }
                    ],
                    IssuedOrigins =
                    [
                        new IssuedOriginFrontierEntry
                        {
                            OriginAuthorEpoch = 8,
                            IssuedOriginSequence = 9,
                            MaxIssuedHlcWall = 100,
                            MaxIssuedHlcLogical = 1
                        }
                    ]
                }, keys[0].Seed), roster);
            var report1 = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                new AppliedOriginFrontierReport
                {
                    NamespaceId = checked((int)namespaceId),
                    RosterGeneration = 1,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    MemberNodeId = local,
                    MemberTransportEpoch = 11,
                    ReportSequence = 1,
                    Entries =
                    [
                        new AppliedOriginFrontierEntry
                        {
                            OriginAuthorNodeId = authority,
                            OriginAuthorEpoch = 8,
                            ContiguousAppliedOriginSequence = 9
                        }
                    ],
                }, keys[1].Seed), roster);

            Guid fullReplaceGeneration = Guid.Parse("12345678-1234-5678-9abc-def012345678");
            byte[] fullReplaceDigest = SHA256.HashData("async Host metadata GC baseline"u8);
            foreach (int memberIndex in new[] { 0, 1 })
            {
                NamespaceRosterMember member = roster.Members[memberIndex];
                byte[] seed = keys[memberIndex].Seed;
                gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                    new FullReplaceInstalledAck
                    {
                        NamespaceId = checked((int)namespaceId),
                        RosterGeneration = 1,
                        RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                        Generation = fullReplaceGeneration,
                        Digest = fullReplaceDigest,
                        MemberNodeId = member.NodeId,
                        MemberTransportEpoch = member.TransportPeerEpoch,
                        HlcWallFloor = 100,
                        HlcLogicalFloor = 1,
                        Installed = true
                    }, seed), roster);
            }
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 },
                fullReplaceGeneration, fullReplaceDigest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 1);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            metadata.AttachNamespaceGcGuard(gc, trust, scope, local);
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, checked((int)namespaceId), BatchId.From(changeId),
                out ReadOnlyMemory<byte> fingerprint, out OperationOrigin origin,
                out bool isLocalOutbound));
            Assert.Equal(16, fingerprint.Length);
            Assert.Equal(authority, origin.AuthorNodeId);
            Assert.Equal(8U, origin.AuthorEpoch);
            Assert.Equal(9UL, origin.Sequence);
            Assert.False(isLocalOutbound);
            Assert.Contains(authority, metadata.ReadRelayAuthors(scope, local, checked((int)namespaceId)));
            Assert.Empty(metadata.ReadPeerStreamStates(
                scope, local, authority, 2, checked((int)namespaceId)));
            var referenceScanner = Assert.IsAssignableFrom<IAsyncApplicationGcReferenceScanner>(provider);
            await using (IAsyncApplicationGcReferenceScan hostScan = await referenceScanner.OpenScanAsync(
                scope, local, checked((int)namespaceId)))
            {
                Assert.Equal(0UL, (await hostScan.GetUnpublishedLocalDraftHorizonAsync()).Count);
                Assert.False(await hostScan.HasExternalChangeIdReferenceAsync(BatchId.From(changeId)));
            }
            var coordinator = new AsyncStorageSyncCoordinator(
                node, metadata, scope, checked((int)namespaceId));
            var adapter = new FldBinSyncAsyncHostAdapter(
                coordinator, provider, remoteApply, _ => { });

            // The real FLD Host has a sealed ApplyJournal receipt, so the retired
            // transport receipt can be compacted. The imported ChangeId is different:
            // it is still referenced by BIN-SYNC's relay stream and must remain live
            // until relay delivery is acknowledged and its payload is safely compacted.
            (int changeIds, int receipts) = await adapter.RunMetadataGcPassAsync(gc, trust, 1);
            Assert.Equal(1, receipts);
            Assert.Equal(0, changeIds);
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, checked((int)namespaceId), BatchId.From(changeId),
                out _, out _, out _));

            // Remove the last relay payload reference so the ChangeId becomes an
            // otherwise eligible Metadata-GC candidate.
            MetadataRelayPage relayPage = metadata.ReadRelay(
                scope, local, checked((int)namespaceId), authority, 0, 16);
            Assert.NotEmpty(relayPage.Items);
            ulong relayThrough = relayPage.Items[^1].Sequence;
            metadata.AcknowledgeRelay(
                scope, local, checked((int)namespaceId), authority, relayThrough);
            metadata.CompactRelayThrough(
                scope, local, checked((int)namespaceId), authority, relayThrough);
            Assert.Empty(metadata.ReadRelay(
                scope, local, checked((int)namespaceId), authority, relayThrough, 16).Items);

            // Cancellation after a negative Host reference answer must abort the
            // Metadata deletion, dispose the immutable scan, and release the Host
            // lease. A competing IPC writer stays blocked until that cleanup finishes.
            var cancelledScanner = new BlockingAsyncGcReferenceScanner(referenceScanner);
            gcCancellation = new CancellationTokenSource();
            Task<int> cancelledGc = coordinator.CompactChangeLedgerAsync(
                cancelledScanner, limit: 1, cancellationToken: gcCancellation.Token);
            await cancelledScanner.ReferenceCheckReached.WaitAsync(TimeSpan.FromSeconds(15));

            // A valid acknowledgement for an untracked writer-client receipt traverses
            // Host write admission without creating a new local provider draft.
            Task<bool> blockedWriter = writerClient!.AcknowledgeThroughAsync(1);
            await Task.Delay(100);
            Assert.False(blockedWriter.IsCompleted,
                "A competing IPC writer must remain blocked while the cancelled GC scan still owns the Host lease.");
            gcCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await cancelledGc.WaitAsync(TimeSpan.FromSeconds(15)));
            bool writerAcknowledged = await blockedWriter.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.False(writerAcknowledged,
                "The independent writer has no local operation receipts to acknowledge.");
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, checked((int)namespaceId), BatchId.From(changeId),
                out _, out _, out _),
                "Cancellation must not commit the ChangeId deletion.");

            // Retry from a fresh Host-owned scan. The same candidate is still safe,
            // and a successful bounded retry may now commit it exactly once.
            int retryDeleted = await coordinator.CompactChangeLedgerAsync(
                referenceScanner, limit: 1).WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(1, retryDeleted);
            Assert.False(metadata.TryGetChangeIdentity(
                scope, local, checked((int)namespaceId), BatchId.From(changeId),
                out _, out _, out _),
                "The retry deletes the ChangeId only after a fresh Host reference answer and final Metadata commit.");
        }
        finally
        {
            // Always release the test-only blocking scanner before disposing its owner.
            // Otherwise an earlier assertion can strand the provider's scan gate and
            // turn the real failure into an unbounded cleanup hang.
            gcCancellation?.Cancel();
            if (writerClient is not null)
                await writerClient.DisposeAsync();
            if (remoteApply is not null)
                await remoteApply.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            gcCancellation?.Dispose();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            if (ownerId != Guid.Empty)
                StorageIpcEndpoint.DeleteIfOwned(hostRoot, 0, ownerId);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
            try { Directory.Delete(nodeRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_coordinator_reclaims_only_signed_safe_tombstones_through_real_host_ipc()
    {
        string hostRoot = Path.Combine(Path.GetTempPath(), "fld-binsync-host-tombstone-gc-" + Guid.NewGuid().ToString("N"));
        string nodeRoot = Path.Combine(Path.GetTempPath(), "binsync-host-tombstone-gc-metadata-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x536;
        const string scope = "fld-host-tombstone-gc";
        byte[] authorBytes = Id(0x65);
        NodeId author = NodeId.From(authorBytes);
        NodeId authority = NodeId.From(Id(0x11));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(authority, keys[0].PublicKey, TrustMode.PreShared);
        Guid clientId = Guid.NewGuid();
        Guid ownerId = Guid.Empty;
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? provider = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteApply = null;
        byte[] recordId = FldDatabase.NewRecordId();
        try
        {
            using (var database = FldDatabase.Open(hostRoot, namespaceId, authorBytes))
            {
                var tombstone = new RemoteFieldWrite(
                    recordId, 7, Array.Empty<byte>(), 100, 1, authorBytes, Id(0x76),
                    keyEpoch: 0, isTombstone: true, originAuthorEpoch: 8, originSequence: 9);
                var batch = new RemoteBatchCommit(
                    senderPeer: Id(0x71),
                    senderEpoch: 6,
                    namespaceId: checked((int)namespaceId),
                    issuerNodeId: Id(0x72),
                    issuerEpoch: 8,
                    batchId: Id(0x73),
                    cursorStart: 9,
                    cursorEnd: 9,
                    previousBatchDigest: Id(0x74),
                    batchDigest: Id(0x75),
                    recordsRoot: Id(0x77),
                    writes: [tombstone],
                    relayWrites: [],
                    protocolStateDelta: [0xA1]);
                Assert.False(database.ApplyRemoteBatch(batch).IsDuplicate);
                Assert.NotEqual(0UL, database.Checkpoint());
            }

            host = StartDatabaseHost(hostRoot, namespaceId, authorBytes, enableTombstoneGc: true);
            StorageIpcEndpointInfo endpoint = await WaitForHostEndpointAsync(
                hostRoot, host, TimeSpan.FromSeconds(45));
            ownerId = endpoint.OwnerId;
            provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 8);
            remoteApply = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRoot, Guid.NewGuid(), checked((ushort)namespaceId), TimeSpan.FromSeconds(15));

            using (var nodeStorage = StorageDatabase.Open(nodeRoot, 902))
            using (var metadata = new StorageMetadataStore(nodeStorage))
            using (var node = new NodeBuilder().WithNodeId(author).WithEpoch(8).Build())
            {
                var roster = new NamespaceRosterCertificate
                {
                    NamespaceId = checked((int)namespaceId),
                    RosterGeneration = 1,
                    PreviousRosterDigest = new byte[32],
                    AuthorityNodeId = authority,
                    Members =
                    [
                        new NamespaceRosterMember
                        {
                            NodeId = authority,
                            PublicKey = keys[0].PublicKey,
                            TransportPeerEpoch = 10,
                            Status = NamespaceMemberStatus.Active
                        },
                        new NamespaceRosterMember
                        {
                            NodeId = author,
                            PublicKey = keys[1].PublicKey,
                            TransportPeerEpoch = 11,
                            Status = NamespaceMemberStatus.Active
                        }
                    ]
                };
                var gc = new StorageNamespaceGcStore(
                    nodeStorage, scope, author,
                    new StorageNamespaceGcFeatureOptions(EnableTombstoneGc: true));
                roster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
                byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(roster);
                var report0 = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = checked((int)namespaceId),
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        MemberNodeId = authority,
                        MemberTransportEpoch = 10,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = author,
                                OriginAuthorEpoch = 8,
                                ContiguousAppliedOriginSequence = 9
                            }
                        ]
                    }, keys[0].Seed), roster);
                var report1 = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = checked((int)namespaceId),
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        MemberNodeId = author,
                        MemberTransportEpoch = 11,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = author,
                                OriginAuthorEpoch = 8,
                                ContiguousAppliedOriginSequence = 9
                            }
                        ],
                        IssuedOrigins =
                        [
                            new IssuedOriginFrontierEntry
                            {
                                OriginAuthorEpoch = 8,
                                IssuedOriginSequence = 9,
                                MaxIssuedHlcWall = 100,
                                MaxIssuedHlcLogical = 1
                            }
                        ]
                    }, keys[1].Seed), roster);
                Guid fullReplaceGeneration = Guid.Parse("12345678-1234-5678-9abc-def012345678");
                byte[] fullReplaceDigest = SHA256.HashData("fld tombstone gc snapshot"u8);
                gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                    new FullReplaceInstalledAck
                    {
                        NamespaceId = checked((int)namespaceId),
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        Generation = fullReplaceGeneration,
                        Digest = fullReplaceDigest,
                        MemberNodeId = authority,
                        MemberTransportEpoch = 10,
                        HlcWallFloor = 100,
                        HlcLogicalFloor = 1,
                        Installed = true
                    }, keys[0].Seed), roster);
                gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                    new FullReplaceInstalledAck
                    {
                        NamespaceId = checked((int)namespaceId),
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        Generation = fullReplaceGeneration,
                        Digest = fullReplaceDigest,
                        MemberNodeId = author,
                        MemberTransportEpoch = 11,
                        HlcWallFloor = 100,
                        HlcLogicalFloor = 1,
                        Installed = true
                    }, keys[1].Seed), roster);
                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], new[] { report0, report1 },
                    fullReplaceGeneration, fullReplaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 1);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

                var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, checked((int)namespaceId));
                coordinator.AttachNamespaceGcGuard(gc, trust);

                // Cut 1: the Host has durably opened its provider snapshot pin, but
                // candidate enumeration and physical deletion have not begun. Killing
                // and reopening must preserve the tombstone and discard the stale pin.
                var beforeDeleteCrash = new CrashAfterTombstoneCandidateRead(provider!, host!);
                await Assert.ThrowsAsync<IOException>(
                    () => coordinator.CompactTombstonesAsync(beforeDeleteCrash, 16));
            }

            async Task RestartHostAndReconnectAsync()
            {
                bool hostAlreadyExited = host?.HasExited == true;
                if (remoteApply is not null)
                {
                    try { await remoteApply.DisposeAsync(); }
                    catch (Exception) when (hostAlreadyExited) { /* IPC cleanup is unavailable after the injected Host death. */ }
                    remoteApply = null;
                }
                if (provider is not null)
                {
                    try { await provider.DisposeAsync(); }
                    catch (Exception) when (hostAlreadyExited) { /* Stale snapshot/lease cleanup cannot reach a dead Host. */ }
                    provider = null;
                }
                if (host is not null)
                {
                    host.Dispose();
                    host = null;
                }
                await WaitForDatabaseWriterReleaseAsync(
                    hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));
                host = StartDatabaseHost(hostRoot, namespaceId, authorBytes, enableTombstoneGc: true);
                StorageIpcEndpointInfo restartedEndpoint = await WaitForHostEndpointAsync(
                    hostRoot, host, TimeSpan.FromSeconds(45), ownerId);
                ownerId = restartedEndpoint.OwnerId;
                provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                    hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15), author, () => 8);
                remoteApply = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                    hostRoot, Guid.NewGuid(), checked((ushort)namespaceId), TimeSpan.FromSeconds(15));
            }

            await RestartHostAndReconnectAsync();
            Assert.NotNull(await provider!.ReadCurrentAsync(RecordId.From(recordId), 7));

            // Cut 2: the provider transaction commits the delete, then the Host dies
            // before the coordinator can observe completion. Reopen must not resurrect
            // the deleted tombstone, and a retry must not be needed to finish the commit.
            var afterDeleteCrash = new CrashAfterTombstoneDelete(provider!, host!);
            using (var nodeStorage = StorageDatabase.Open(nodeRoot, 902))
            using (var metadata = new StorageMetadataStore(nodeStorage))
            using (var node = new NodeBuilder().WithNodeId(author).WithEpoch(8).Build())
            {
                // Reopen the durable signed proof state from the same Metadata partition.
                var reopenedGc = new StorageNamespaceGcStore(
                    nodeStorage, scope, author,
                    new StorageNamespaceGcFeatureOptions(EnableTombstoneGc: true));
                Assert.True(reopenedGc.TryReadNamespaceRoster(
                    checked((int)namespaceId), trust, out NamespaceRosterCertificate? acceptedRoster));
                Assert.NotNull(acceptedRoster);
                var coordinator = new AsyncStorageSyncCoordinator(
                    node, metadata, "fld-host-tombstone-gc", checked((int)namespaceId));
                coordinator.AttachNamespaceGcGuard(reopenedGc, trust);
                await Assert.ThrowsAsync<IOException>(
                    () => coordinator.CompactTombstonesAsync(afterDeleteCrash, 16));
            }

            await RestartHostAndReconnectAsync();
            Assert.Null(await provider!.ReadCurrentAsync(RecordId.From(recordId), 7));

            await remoteApply!.DisposeAsync();
            remoteApply = null;
            await provider.DisposeAsync();
            provider = null;
            await StopDatabaseHostAsync(host!);
            host = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));

            using var reopened = FldDatabase.Open(hostRoot, namespaceId, authorBytes);
            Assert.False(reopened.TryResolve(recordId, 7, out _));
        }
        finally
        {
            if (remoteApply is not null)
            {
                try { await remoteApply.DisposeAsync(); }
                catch (Exception) when (host?.HasExited == true) { }
            }
            if (provider is not null)
            {
                try { await provider.DisposeAsync(); }
                catch (Exception) when (host?.HasExited == true) { }
            }
            if (host is not null)
                await StopDatabaseHostAsync(host);
            if (ownerId != Guid.Empty)
                StorageIpcEndpoint.DeleteIfOwned(hostRoot, 0, ownerId);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
            try { Directory.Delete(nodeRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_two_host_signed_gc_waits_for_peer_acks_then_reclaims_tombstone_on_both_hosts()
    {
        string hostRootA = Path.Combine(Path.GetTempPath(), "fld-binsync-two-host-gc-a-" + Guid.NewGuid().ToString("N"));
        string hostRootB = Path.Combine(Path.GetTempPath(), "fld-binsync-two-host-gc-b-" + Guid.NewGuid().ToString("N"));
        string nodeRootA = Path.Combine(Path.GetTempPath(), "binsync-two-host-gc-node-a-" + Guid.NewGuid().ToString("N"));
        string nodeRootB = Path.Combine(Path.GetTempPath(), "binsync-two-host-gc-node-b-" + Guid.NewGuid().ToString("N"));
        const ushort namespaceId = 0x539;
        const uint nodeStorageNamespace = 703;
        const string scope = "two-host-signed-tombstone-gc";
        byte[] authorABytes = Id(0x91);
        byte[] authorBBytes = Id(0x92);
        NodeId authorA = NodeId.From(authorABytes);
        NodeId authorB = NodeId.From(authorBBytes);
        RecordId record = RecordId.From(Id(0x93));
        Process? hostA = null;
        Process? hostB = null;
        StorageDatabase? nodeStorageA = null;
        StorageDatabase? nodeStorageB = null;
        StorageMetadataStore? metadataA = null;
        StorageMetadataStore? metadataB = null;
        Wal? walA = null;
        Wal? walB = null;
        Node? nodeA = null;
        Node? nodeB = null;
        FldDatabaseIpcAsyncDataProvider? providerA = null;
        FldDatabaseIpcAsyncDataProvider? providerB = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteA = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteB = null;
        SyncSession? acceptor = null;
        SyncSession? initiator = null;
        Node? relayNode = null;
        SyncSession? relayAcceptor = null;
        SyncSession? relayInitiator = null;
        Guid ownerA = Guid.Empty;
        Guid ownerB = Guid.Empty;
        Guid providerClientA = Guid.NewGuid();
        Guid providerClientB = Guid.NewGuid();
        try
        {
            hostA = StartDatabaseHost(hostRootA, namespaceId, authorABytes, enableTombstoneGc: true);
            hostB = StartDatabaseHost(hostRootB, namespaceId, authorBBytes, enableTombstoneGc: true);
            ownerA = (await WaitForHostEndpointAsync(hostRootA, hostA, TimeSpan.FromSeconds(45))).OwnerId;
            ownerB = (await WaitForHostEndpointAsync(hostRootB, hostB, TimeSpan.FromSeconds(45))).OwnerId;

            nodeStorageA = StorageDatabase.Open(nodeRootA, nodeStorageNamespace);
            nodeStorageB = StorageDatabase.Open(nodeRootB, nodeStorageNamespace);
            metadataA = new StorageMetadataStore(nodeStorageA);
            metadataB = new StorageMetadataStore(nodeStorageB);
            walA = Wal.Open(Path.Combine(nodeRootA, "node.wal"));
            walB = Wal.Open(Path.Combine(nodeRootB, "node.wal"));
            nodeA = new NodeBuilder().WithNodeId(authorA).WithEpoch(17).Build();
            nodeB = new NodeBuilder().WithNodeId(authorB).WithEpoch(18).Build();
            nodeA.AttachWal(walA);
            nodeB.AttachWal(walB);

            providerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootA, providerClientA, TimeSpan.FromSeconds(15), authorA, () => 17);
            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, providerClientB, TimeSpan.FromSeconds(15), authorB, () => 18);
            remoteA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            await providerA.EnableMetadataStoreOwnedPublicationAsync(0);
            await providerB.EnableMetadataStoreOwnedPublicationAsync(0);

            var coordinatorA = new AsyncStorageSyncCoordinator(nodeA, metadataA, scope, namespaceId);
            var coordinatorB = new AsyncStorageSyncCoordinator(nodeB, metadataB, scope, namespaceId);
            FldBinSyncAsyncHostAdapter adapterA = new(
                coordinatorA, providerA, remoteA,
                current => StorageNodePersistence.Save(nodeStorageA, current));
            FldBinSyncAsyncHostAdapter adapterB = new(
                coordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                await adapterA.RecoverBeforeAcceptingSessionsAsync());
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                await adapterB.RecoverBeforeAcceptingSessionsAsync());

            async Task RestartHostAAfterGcPageAsync()
            {
                // Crash Host A while its GC reference scan/lease is active. A second
                // IPC session must be blocked before the crash; after process death the
                // blocked request fails, and the reopened Host must have no stale lease.
                Guid previousOwner = ownerA;
                IAsyncApplicationGcReferenceScan? activeScan = null;
                FldDatabaseIpcClient? blockedClient = null;
                Task<byte[]>? blockedRequest = null;
                try
                {
                    activeScan = await providerA!.OpenScanAsync(scope, nodeA!.NodeId, namespaceId);
                    blockedClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                        hostRootA, Guid.NewGuid(), TimeSpan.FromSeconds(15));
                    blockedRequest = blockedClient.GetLocalAuthorAsync();
                    await Task.Delay(100);
                    Assert.False(blockedRequest.IsCompleted);

                    await StopDatabaseHostAsync(hostA!);
                    hostA = null;
                    await Assert.ThrowsAnyAsync<IOException>(
                        async () => await blockedRequest.WaitAsync(TimeSpan.FromSeconds(10)));
                }
                finally
                {
                    if (activeScan is not null)
                    {
                        try { await activeScan.DisposeAsync(); }
                        catch (Exception) { /* The owning Host process has already exited. */ }
                    }
                    if (blockedClient is not null)
                    {
                        try { await blockedClient.DisposeAsync(); }
                        catch (Exception) { /* The IPC endpoint belonged to the dead Host. */ }
                    }
                    if (remoteA is not null)
                    {
                        try { await remoteA.DisposeAsync(); }
                        catch (Exception) { /* The IPC endpoint belonged to the dead Host. */ }
                        remoteA = null;
                    }
                    if (providerA is not null)
                    {
                        try { await providerA.DisposeAsync(); }
                        catch (Exception) { /* The IPC endpoint belonged to the dead Host. */ }
                        providerA = null;
                    }
                }

                await WaitForDatabaseWriterReleaseAsync(
                    hostRootA, namespaceId, authorABytes, TimeSpan.FromSeconds(15));
                hostA = StartDatabaseHost(hostRootA, namespaceId, authorABytes, enableTombstoneGc: true);
                ownerA = (await WaitForHostEndpointAsync(
                    hostRootA, hostA, TimeSpan.FromSeconds(45), previousOwner)).OwnerId;
                providerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                    hostRootA, providerClientA, TimeSpan.FromSeconds(15), authorA, () => 17);
                remoteA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                    hostRootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
                await providerA.EnableMetadataStoreOwnedPublicationAsync(0);
                adapterA = new FldBinSyncAsyncHostAdapter(
                    coordinatorA, providerA, remoteA,
                    current => StorageNodePersistence.Save(nodeStorageA!, current));
                Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                    await adapterA.RecoverBeforeAcceptingSessionsAsync());

                await using var reopenedProbe = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                    hostRootA, Guid.NewGuid(), TimeSpan.FromSeconds(15));
                Assert.Equal(authorABytes, await reopenedProbe.GetLocalAuthorAsync());
            }

            async Task RestartHostBAfterGcPageAsync()
            {
                await remoteB!.DisposeAsync(); remoteB = null;
                await providerB!.DisposeAsync(); providerB = null;
                Guid previousOwner = ownerB;
                await StopDatabaseHostAsync(hostB!); hostB = null;
                await WaitForDatabaseWriterReleaseAsync(
                    hostRootB, namespaceId, authorBBytes, TimeSpan.FromSeconds(15));
                hostB = StartDatabaseHost(hostRootB, namespaceId, authorBBytes, enableTombstoneGc: true);
                ownerB = (await WaitForHostEndpointAsync(
                    hostRootB, hostB, TimeSpan.FromSeconds(45), previousOwner)).OwnerId;
                providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                    hostRootB, providerClientB, TimeSpan.FromSeconds(15), authorB, () => 18);
                remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                    hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
                await providerB.EnableMetadataStoreOwnedPublicationAsync(0);
                adapterB = new FldBinSyncAsyncHostAdapter(
                    coordinatorB, providerB, remoteB,
                    current => StorageNodePersistence.Save(nodeStorageB!, current));
                Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                    await adapterB.RecoverBeforeAcceptingSessionsAsync());
            }

            await providerA.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0951,
                AuthorNodeId = authorA,
                OriginAuthorEpoch = 17,
                HlcWall = 100,
                HlcLogical = 1,
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(Id(0x94)),
                IsTombstone = true
            });
            await providerA.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0952,
                AuthorNodeId = authorA,
                OriginAuthorEpoch = 17,
                HlcWall = 101,
                HlcLogical = 1,
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(Id(0x95)),
                IsTombstone = true
            });

            var (toA, toB) = PipeTransport.CreatePair();
            Task<SyncSession> acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            Task<SyncSession> connectTask = SyncSession.ConnectAsync(nodeA, toA);
            acceptor = await acceptTask;
            initiator = await connectTask;

            metadataA.RegisterInterestedPeer(scope, nodeA.NodeId, nodeB.NodeId,
                checked((uint)initiator.PeerEpoch), namespaceId);
            metadataA.CompletePeerInitialSync(scope, nodeA.NodeId, nodeB.NodeId,
                checked((uint)initiator.PeerEpoch), namespaceId,
                [new MetadataPeerStreamBaseline(nodeA.NodeId, 0, 17)]);
            metadataB.RegisterInterestedPeer(scope, nodeB.NodeId, nodeA.NodeId,
                checked((uint)acceptor.PeerEpoch), namespaceId);
            metadataB.CompletePeerInitialSync(scope, nodeB.NodeId, nodeA.NodeId,
                checked((uint)acceptor.PeerEpoch), namespaceId,
                [new MetadataPeerStreamBaseline(nodeB.NodeId, 0, 18)]);

            Task<BatchResult> receiveAtB = adapterB.ReceiveBatchAsync(acceptor);
            Task<int> sendFromA = adapterA.RunOutboundRoundAsync([initiator]);
            await Task.WhenAll(receiveAtB, sendFromA).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(2, (await receiveAtB).Applied);
            Assert.Equal(1, await sendFromA);

            ProviderMutation? tombstoneAtA = await providerA.ReadCurrentAsync(record, 0x0951);
            ProviderMutation? tombstoneAtB = await providerB.ReadCurrentAsync(record, 0x0951);
            Assert.NotNull(tombstoneAtA);
            Assert.NotNull(tombstoneAtB);
            Assert.True(tombstoneAtA!.IsTombstone);
            Assert.True(tombstoneAtB!.IsTombstone);
            Assert.Equal(BatchId.From(Id(0x94)), tombstoneAtA.ChangeId);
            Assert.Equal(BatchId.From(Id(0x94)), tombstoneAtB.ChangeId);
            Assert.Equal(17U, tombstoneAtA.OriginAuthorEpoch);
            Assert.Equal(17U, tombstoneAtB.OriginAuthorEpoch);
            Assert.Equal(1UL, tombstoneAtA.OriginSequence);
            Assert.Equal(1UL, tombstoneAtB.OriginSequence);
            ProviderMutation? secondTombstoneAtA = await providerA.ReadCurrentAsync(record, 0x0952);
            ProviderMutation? secondTombstoneAtB = await providerB.ReadCurrentAsync(record, 0x0952);
            Assert.NotNull(secondTombstoneAtA);
            Assert.NotNull(secondTombstoneAtB);
            Assert.True(secondTombstoneAtA!.IsTombstone);
            Assert.True(secondTombstoneAtB!.IsTombstone);
            Assert.Equal(BatchId.From(Id(0x95)), secondTombstoneAtA.ChangeId);
            Assert.Equal(BatchId.From(Id(0x95)), secondTombstoneAtB.ChangeId);
            Assert.Equal(2UL, secondTombstoneAtA.OriginSequence);
            Assert.Equal(2UL, secondTombstoneAtB.OriginSequence);

            uint retiredTransportEpoch = checked((uint)initiator.PeerEpoch);
            uint rejoinedTransportEpoch = checked(retiredTransportEpoch + 1);
            var keys = new[]
            {
                BatchSignature.GenerateKeyPair(),
                BatchSignature.GenerateKeyPair(),
                BatchSignature.GenerateKeyPair()
            };
            NodeId relayMember = NodeId.From(Id(0x96));
            var trust = new TrustStore();
            trust.Allow(authorA, keys[0].PublicKey, TrustMode.PreShared);
            var roster = new NamespaceRosterCertificate
            {
                NamespaceId = namespaceId,
                RosterGeneration = 1,
                PreviousRosterDigest = new byte[32],
                AuthorityNodeId = authorA,
                Members =
                [
                    new NamespaceRosterMember
                    {
                        NodeId = authorA,
                        PublicKey = keys[0].PublicKey,
                        TransportPeerEpoch = 10,
                        Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = authorB,
                        PublicKey = keys[1].PublicKey,
                        TransportPeerEpoch = retiredTransportEpoch,
                        Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = relayMember,
                        PublicKey = keys[2].PublicKey,
                        TransportPeerEpoch = 12,
                        Status = NamespaceMemberStatus.Active
                    }
                ]
            };
            Guid fullReplaceGeneration = Guid.Parse("22345678-1234-5678-9abc-def012345678");
            byte[] fullReplaceDigest = SHA256.HashData("two-host signed tombstone gc snapshot"u8);
            void InstallSignedProof(StorageNamespaceGcStore gc)
            {
                NamespaceRosterCertificate initialRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
                byte[] initialDigest = NamespaceRosterProtocol.ComputeDigest(initialRoster);

                // B signs a final, draft-free report before its transport incarnation
                // is retired. The authority then attests retirement, Joining and
                // promotion across signed roster-generation gaps.
                gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 1,
                        RosterDigest = initialDigest,
                        MemberNodeId = authorB,
                        MemberTransportEpoch = retiredTransportEpoch,
                        ReportSequence = 1,
                        FinalForAuthor = true,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = authorA,
                                OriginAuthorEpoch = 17,
                                ContiguousAppliedOriginSequence = 2
                            }
                        ]
                    }, keys[1].Seed), initialRoster);

                NamespaceRosterCertificate retiredRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(new NamespaceRosterCertificate
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 2,
                        PreviousRosterDigest = initialDigest,
                        AuthorityNodeId = authorA,
                        Members =
                        [
                            new NamespaceRosterMember { NodeId = authorA, PublicKey = keys[0].PublicKey, TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active },
                            new NamespaceRosterMember { NodeId = authorB, PublicKey = keys[1].PublicKey, TransportPeerEpoch = retiredTransportEpoch, Status = NamespaceMemberStatus.Retired, RetiredThroughEpoch = retiredTransportEpoch },
                            new NamespaceRosterMember { NodeId = relayMember, PublicKey = keys[2].PublicKey, TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Active }
                        ]
                    }, keys[0].Seed), trust);
                NamespaceRosterCertificate joiningRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(new NamespaceRosterCertificate
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 4,
                        PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(retiredRoster),
                        AuthorityNodeId = authorA,
                        Members =
                        [
                            new NamespaceRosterMember { NodeId = authorA, PublicKey = keys[0].PublicKey, TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active },
                            new NamespaceRosterMember { NodeId = authorB, PublicKey = keys[1].PublicKey, TransportPeerEpoch = rejoinedTransportEpoch, Status = NamespaceMemberStatus.Joining },
                            new NamespaceRosterMember { NodeId = relayMember, PublicKey = keys[2].PublicKey, TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Active }
                        ]
                    }, keys[0].Seed), trust);
                NamespaceRosterCertificate rejoinedRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(new NamespaceRosterCertificate
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 6,
                        PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(joiningRoster),
                        AuthorityNodeId = authorA,
                        Members =
                        [
                            new NamespaceRosterMember { NodeId = authorA, PublicKey = keys[0].PublicKey, TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active },
                            new NamespaceRosterMember { NodeId = authorB, PublicKey = keys[1].PublicKey, TransportPeerEpoch = rejoinedTransportEpoch, Status = NamespaceMemberStatus.Active },
                            new NamespaceRosterMember { NodeId = relayMember, PublicKey = keys[2].PublicKey, TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Active }
                        ]
                    }, keys[0].Seed), trust);

                Assert.Empty(gc.ReadOriginFrontiers(rejoinedRoster));
                Assert.False(gc.TryReadRetentionCheckpoint(rejoinedRoster, trust, out _));
                Assert.Empty(gc.ReadFullReplaceInstalledAcks(rejoinedRoster, fullReplaceGeneration));

                byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(rejoinedRoster);
                var reportA = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 6,
                        RosterDigest = rosterDigest,
                        MemberNodeId = authorA,
                        MemberTransportEpoch = 10,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = authorA,
                                OriginAuthorEpoch = 17,
                                ContiguousAppliedOriginSequence = 2
                            }
                        ],
                        IssuedOrigins =
                        [
                            new IssuedOriginFrontierEntry
                            {
                                OriginAuthorEpoch = 17,
                                IssuedOriginSequence = 2,
                                MaxIssuedHlcWall = 101,
                                MaxIssuedHlcLogical = 1
                            }
                        ]
                    }, keys[0].Seed), rejoinedRoster);
                var reportB = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 6,
                        RosterDigest = rosterDigest,
                        MemberNodeId = authorB,
                        MemberTransportEpoch = rejoinedTransportEpoch,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = authorA,
                                OriginAuthorEpoch = 17,
                                ContiguousAppliedOriginSequence = 2
                            }
                        ]
                    }, keys[1].Seed), rejoinedRoster);
                var reportC = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 6,
                        RosterDigest = rosterDigest,
                        MemberNodeId = relayMember,
                        MemberTransportEpoch = 12,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = authorA,
                                OriginAuthorEpoch = 17,
                                ContiguousAppliedOriginSequence = 2
                            }
                        ]
                    }, keys[2].Seed), rejoinedRoster);

                foreach (var item in new[]
                {
                    (Member: authorA, Epoch: 10U, Key: keys[0]),
                    (Member: authorB, Epoch: rejoinedTransportEpoch, Key: keys[1]),
                    (Member: relayMember, Epoch: 12U, Key: keys[2])
                })
                {
                    gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                        new FullReplaceInstalledAck
                        {
                            NamespaceId = namespaceId,
                            RosterGeneration = 6,
                            RosterDigest = rosterDigest,
                            Generation = fullReplaceGeneration,
                            Digest = fullReplaceDigest,
                            MemberNodeId = item.Member,
                            MemberTransportEpoch = item.Epoch,
                            HlcWallFloor = 101,
                            HlcLogicalFloor = 1,
                            Installed = true
                        }, item.Key.Seed), rejoinedRoster);
                }

                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    rejoinedRoster, 1, new byte[32], [reportA, reportB, reportC],
                    fullReplaceGeneration, fullReplaceDigest,
                    fullReplaceHlcWallFloor: 101, fullReplaceHlcLogicalFloor: 1);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed),
                    rejoinedRoster, trust);
            }

            var gcA = new StorageNamespaceGcStore(
                nodeStorageA, scope, authorA,
                new StorageNamespaceGcFeatureOptions(
                    EnableTombstoneGc: true, EnableChangeIdGc: true, EnableReceiptGc: true));
            var gcB = new StorageNamespaceGcStore(
                nodeStorageB, scope, authorB,
                new StorageNamespaceGcFeatureOptions(
                    EnableTombstoneGc: true, EnableChangeIdGc: true, EnableReceiptGc: true));
            // Before C receives the tombstone there is deliberately no accepted
            // three-member frontier proof. The retained Metadata payloads independently
            // keep both physical GC passes fail-closed.
            Assert.True(metadataA.HasLiveRecordReference(scope, authorA, namespaceId, record),
                "Host A's immutable outbound payload remains retained until a proof-bound Metadata baseline compacts it.");
            Assert.True(metadataB.HasLiveRecordReference(scope, authorB, namespaceId, record),
                "Host B's relay payload remains retained until a proof-bound Metadata baseline compacts it.");

            Assert.Equal(0, await adapterA.RunTombstoneGcPassAsync(gcA, trust, 16));
            Assert.Equal(0, await adapterB.RunTombstoneGcPassAsync(gcB, trust, 16));
            Assert.NotNull(await providerA.ReadCurrentAsync(record, 0x0951));
            Assert.NotNull(await providerB.ReadCurrentAsync(record, 0x0951));

            // Host A's outbound payload is now ACKed by Host B, but Host B's relay
            // payload must remain until a downstream peer (not the original author)
            // receives and applies it. C is a transport-level active member in this
            // frontier test, not a third durable FLD Database Host.
            relayNode = new NodeBuilder()
                .WithNodeId(relayMember)
                .WithEpoch(19)
                .Build();
            var (toBRelay, toCRelay) = PipeTransport.CreatePair();
            Task<SyncSession> relayAcceptTask = SyncSession.AcceptAsync(relayNode, toCRelay);
            Task<SyncSession> relayConnectTask = SyncSession.ConnectAsync(nodeB, toBRelay);
            relayAcceptor = await relayAcceptTask;
            relayInitiator = await relayConnectTask;
            metadataB.RegisterInterestedPeer(scope, nodeB.NodeId, relayNode.NodeId,
                checked((uint)relayInitiator.PeerEpoch), namespaceId);
            metadataB.CompletePeerInitialSync(scope, nodeB.NodeId, relayNode.NodeId,
                checked((uint)relayInitiator.PeerEpoch), namespaceId,
                [new MetadataPeerStreamBaseline(authorA, 0, 17)]);

            using var relayCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            Task<BatchResult> relayReceive = relayAcceptor.ReceiveBatchAsync(
                relayNode, relayCancellation.Token);
            Task<int> relaySend = adapterB.RunOutboundRoundAsync(
                [relayInitiator], relayCancellation.Token);
            // Observe receive failures immediately instead of waiting for the sender's
            // transport-ACK timeout; cancel both sides on failure so session cleanup
            // cannot remain blocked behind an abandoned receive.
            try
            {
                await Task.WhenAll(relayReceive, relaySend);
            }
            catch
            {
                relayCancellation.Cancel();
                throw;
            }
            int relaySent = await relaySend;
            BatchResult relayApplied = await relayReceive;
            Assert.Equal(1, relaySent);
            Assert.Equal(2, relayApplied.Applied);

            // Verify the exact relay author stream was delivered and ACKed by C.
            // A successful send count alone is insufficient: the ACK must advance
            // the durable peer cursor used by the relay compaction horizon.
            MetadataPeerStreamState relayPeerState = metadataB.GetPeerStreamState(
                scope, nodeB.NodeId, relayNode.NodeId,
                checked((uint)relayInitiator.PeerEpoch), namespaceId, authorA);
            Assert.Equal(2UL, relayPeerState.AckedCursor + relayPeerState.StreamOffset);
            Assert.Null(relayPeerState.PendingBatch);
            Assert.Equal(2UL, metadataB.GetSafePeerStreamCompactionCursor(
                scope, nodeB.NodeId, namespaceId, authorA));

            // B is also the local durable consumer of the applied relay change.
            // Its local relay-consumer ACK is independent from C's transport ACK.
            metadataB.AcknowledgeRelay(scope, nodeB.NodeId, namespaceId, authorA, 2);

            // C has now received and applied the tombstone through a real SyncSession
            // before signing its OFR2 report. The accepted roster has three Active
            // members; the checkpoint minimum cannot advance until C's signed frontier
            // is present. This is frontier-protocol coverage, not a third FLD Host test.
            InstallSignedProof(gcA);
            InstallSignedProof(gcB);
            Assert.True(gcA.HasProtocolTombstoneGcProof(
                gcA.TryReadNamespaceRoster(namespaceId, trust, out NamespaceRosterCertificate? acceptedA)
                    ? acceptedA! : throw new InvalidOperationException("Host A roster was not persisted."),
                trust, authorA, 17, 2, secondTombstoneAtA!.HlcWall, secondTombstoneAtA.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host A must prove the second tombstone after B's signed rejoin.");
            Assert.True(gcA.HasProtocolTombstoneGcProof(
                acceptedA!,
                trust, authorA, 17, 1, tombstoneAtA.HlcWall, tombstoneAtA.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host A must independently validate all three Active members' signed applied frontiers.");
            Assert.True(gcB.HasProtocolTombstoneGcProof(
                gcB.TryReadNamespaceRoster(namespaceId, trust, out NamespaceRosterCertificate? acceptedB)
                    ? acceptedB! : throw new InvalidOperationException("Host B roster was not persisted."),
                trust, authorA, 17, 2, secondTombstoneAtB!.HlcWall, secondTombstoneAtB.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host B must prove the second tombstone after B's signed rejoin.");
            Assert.True(gcB.HasProtocolTombstoneGcProof(
                acceptedB!,
                trust, authorA, 17, 1, tombstoneAtB.HlcWall, tombstoneAtB.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host B must independently validate all three Active members' signed applied frontiers.");

            BatchId[] tombstoneChangeIds = [BatchId.From(Id(0x94)), BatchId.From(Id(0x95))];

            // Compose retired-epoch receipt rollover into this same two-Host lifecycle.
            // B's old transport incarnation forwards already-known A-origin changes back
            // through its relay stream. The operation identity is unchanged; only the
            // transport receipt identity/cursor belongs to B's retiring incarnation.
            Assert.True(metadataA.TryGetChangeIdentity(
                scope, nodeA.NodeId, namespaceId, tombstoneChangeIds[0],
                out ReadOnlyMemory<byte> receiptFingerprint,
                out OperationOrigin receiptOrigin, out bool receiptIsLocalOutbound));
            Assert.True(receiptIsLocalOutbound);
            Assert.Equal(nodeA.NodeId, receiptOrigin.AuthorNodeId);
            Assert.Equal(17U, receiptOrigin.AuthorEpoch);

            var retiredReceiptBatches = new List<MetadataIncomingBatch>(3);
            byte[] priorReceiptDigest = new byte[16];
            for (byte index = 0; index < 3; index++)
            {
                var retiredReceiptBatch = new MetadataIncomingBatch
                {
                    ScopeId = scope,
                    LocalReplica = nodeA.NodeId,
                    SenderPeer = nodeB.NodeId,
                    SenderEpoch = receiptOrigin.AuthorEpoch,
                    TransportPeerEpoch = retiredTransportEpoch,
                    AuthorStreamNodeId = nodeA.NodeId,
                    NamespaceId = namespaceId,
                    BatchId = BatchId.From(Id((byte)(0xA1 + index))),
                    CursorStart = (ulong)(index + 1),
                    CursorEnd = (ulong)(index + 1),
                    PreviousBatchDigest = priorReceiptDigest,
                    BatchDigest = Enumerable.Repeat((byte)(0xB1 + index), 16).ToArray(),
                    HeaderFingerprint = Enumerable.Repeat((byte)(0xC1 + index), 16).ToArray(),
                    Operations =
                    [
                        new MetadataOperation
                        {
                            AuthorNodeId = nodeA.NodeId,
                            ChangeId = tombstoneChangeIds[0],
                            Fingerprint = receiptFingerprint.ToArray(),
                            Payload = Array.Empty<byte>(),
                            RelayEligible = false,
                            OriginAuthorEpoch = receiptOrigin.AuthorEpoch,
                            OriginSequence = receiptOrigin.Sequence
                        }
                    ]
                };
                Assert.False(metadataA.AcceptIncomingBatch(retiredReceiptBatch).IsDuplicate);
                retiredReceiptBatches.Add(retiredReceiptBatch);
                priorReceiptDigest = retiredReceiptBatch.BatchDigest;
            }

            // Complete the replacement transport baseline before retiring the old
            // incarnation. The receive cursor/digest chain remains continuous across
            // transport epochs and the old receipt rows remain independently collectible.
            metadataA.RegisterInterestedPeer(
                scope, nodeA.NodeId, nodeB.NodeId, rejoinedTransportEpoch, namespaceId);
            metadataA.CompletePeerInitialSync(
                scope, nodeA.NodeId, nodeB.NodeId, rejoinedTransportEpoch, namespaceId,
                [new MetadataPeerStreamBaseline(nodeA.NodeId, 2, 17)]);
            Assert.Equal(2UL, metadataA.GetSafePeerStreamCompactionCursor(
                scope, nodeA.NodeId, namespaceId, nodeA.NodeId));
            metadataA.RetireInterestedPeer(
                scope, nodeA.NodeId, nodeB.NodeId,
                retiredTransportEpoch, rejoinedTransportEpoch, namespaceId);

            var replacementReceiptBatch = new MetadataIncomingBatch
            {
                ScopeId = scope,
                LocalReplica = nodeA.NodeId,
                SenderPeer = nodeB.NodeId,
                SenderEpoch = receiptOrigin.AuthorEpoch,
                TransportPeerEpoch = rejoinedTransportEpoch,
                AuthorStreamNodeId = nodeA.NodeId,
                NamespaceId = namespaceId,
                BatchId = BatchId.From(Id(0xA4)),
                CursorStart = 4,
                CursorEnd = 4,
                PreviousBatchDigest = priorReceiptDigest,
                BatchDigest = Enumerable.Repeat((byte)0xB4, 16).ToArray(),
                HeaderFingerprint = Enumerable.Repeat((byte)0xC4, 16).ToArray(),
                Operations =
                [
                    new MetadataOperation
                    {
                        AuthorNodeId = nodeA.NodeId,
                        ChangeId = tombstoneChangeIds[0],
                        Fingerprint = receiptFingerprint.ToArray(),
                        Payload = Array.Empty<byte>(),
                        RelayEligible = false,
                        OriginAuthorEpoch = receiptOrigin.AuthorEpoch,
                        OriginSequence = receiptOrigin.Sequence
                    }
                ]
            };
            Assert.False(metadataA.AcceptIncomingBatch(replacementReceiptBatch).IsDuplicate);
            metadataA.AttachNamespaceGcGuard(gcA, trust, scope, nodeA.NodeId);

            // FLD treats an unsealed WAL as an uncertainty barrier for every
            // receipt identity. Receipt GC must therefore remain fail-closed until
            // the first physical tombstone-GC admission seals the committed prefix.
            Assert.Equal(0, await adapterA.RunReceiptGcPassAsync(gcA, trust, limit: 1));

            // Compact only after both the local apply consumer and every interested
            // downstream peer have acknowledged the immutable relay prefix.
            // The signed namespace proof alone never deletes payloads.
            metadataA.CompactOutboundThrough(scope, nodeA.NodeId, namespaceId, authorA, 2);
            metadataB.CompactRelayThrough(scope, nodeB.NodeId, namespaceId, authorA, 2);
            // C's transport ACK also releases B's immutable outbound copy. Relay
            // consumer ACK and peer transport ACK are separate retention references.
            metadataB.CompactOutboundThrough(scope, nodeB.NodeId, namespaceId, authorA, 2);
            Assert.False(metadataA.HasLiveRecordReference(scope, authorA, namespaceId, record));
            Assert.False(metadataB.HasLiveRecordReference(scope, authorB, namespaceId, record));

            int CountTombstoneChangeIds(StorageMetadataStore metadata, NodeId local) =>
                tombstoneChangeIds.Count(changeId => metadata.TryGetChangeIdentity(
                    scope, local, checked((int)namespaceId), changeId, out _, out _, out _));
            Assert.Equal(2, CountTombstoneChangeIds(metadataA, nodeA.NodeId));
            Assert.Equal(2, CountTombstoneChangeIds(metadataB, nodeB.NodeId));

            // Metadata payloads are compacted, but the Host still reports ChangeId
            // references in its unsealed WAL/Outbox/ApplyJournal index. The first
            // tombstone-GC admission seals that committed prefix; until then ChangeId
            // compaction must remain fail-closed on both Hosts.
            Assert.Equal(0, await adapterA.RunChangeIdGcPassAsync(gcA, trust, 16));
            Assert.Equal(0, await adapterB.RunChangeIdGcPassAsync(gcB, trust, 16));
            Assert.Equal(2, CountTombstoneChangeIds(metadataA, nodeA.NodeId));
            Assert.Equal(2, CountTombstoneChangeIds(metadataB, nodeB.NodeId));

            // Distinguish a conservative Metadata reservation block from provider
            // candidate/proof rejection before invoking physical GC. The Host seals
            // the committed WAL prefix under its commit guard before this final
            // reference scan; any uncommitted overlay or unacknowledged Outbox still
            // keeps physical reclamation fail-closed.
            metadataA.AttachNamespaceGcGuard(gcA, trust, scope, nodeA.NodeId);
            using (metadataA.AcquireTombstoneGcCommitGuard(
                scope, nodeA.NodeId, namespaceId, gcA, trust,
                out IReadOnlySet<RecordId>? referencesA))
            {
                Assert.NotNull(referencesA);
                Assert.Empty(referencesA);
            }

            // Deterministic lock cuts: a new outbound reservation and its ACK
            // cannot slip between the final Host-reference snapshot and provider
            // deletion. The operation starts on another thread while the commit
            // lease is held and completes only after the lease is released.
            NodeId racePeer = NodeId.From(Id(0x98));
            metadataA.RegisterInterestedPeer(scope, nodeA.NodeId, racePeer, 1, namespaceId);
            metadataA.CompletePeerInitialSync(scope, nodeA.NodeId, racePeer, 1, namespaceId,
                [new MetadataPeerStreamBaseline(authorA, 0, 17)]);
            var raceReservation = new MetadataOutboundBatchReservation
            {
                BatchId = BatchId.From(Id(0x99)),
                IssuerEpoch = 17,
                StreamOffset = 0,
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)0x9A, 16).ToArray(),
                Payload = [0x9B]
            };
            using (var started = new ManualResetEventSlim())
            {
                Task<MetadataOutboundBatchReservation> reserveTask;
                using (metadataA.AcquireTombstoneGcCommitGuard(
                    scope, nodeA.NodeId, namespaceId, gcA, trust, out _))
                {
                    reserveTask = Task.Run(() =>
                    {
                        started.Set();
                        return metadataA.ReserveOutboundBatch(
                            scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA, raceReservation);
                    });
                    Assert.True(started.Wait(TimeSpan.FromSeconds(5)));
                    Thread.Sleep(100);
                    Assert.False(reserveTask.IsCompleted,
                        "Outbound reservation must wait behind the final GC commit guard.");
                }
                MetadataOutboundBatchReservation reserved =
                    await reserveTask.WaitAsync(TimeSpan.FromSeconds(5));
                metadataA.MarkOutboundBatchPushed(
                    scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA, reserved.BatchId);

                // The batch was durably marked pushed, but its ACK is intentionally
                // lost. Reopen the Metadata Storage database before acknowledging it;
                // the exact immutable reservation must survive and be safely retried.
                metadataA.Dispose();
                metadataA = null;
                nodeStorageA!.Dispose();
                nodeStorageA = null;

                nodeStorageA = StorageDatabase.Open(nodeRootA, nodeStorageNamespace);
                metadataA = new StorageMetadataStore(nodeStorageA);
                gcA = new StorageNamespaceGcStore(
                    nodeStorageA, scope, nodeA!.NodeId,
                    new StorageNamespaceGcFeatureOptions(
                        EnableTombstoneGc: true,
                        EnableChangeIdGc: true,
                        EnableReceiptGc: true));
                metadataA.AttachNamespaceGcGuard(gcA, trust, scope, nodeA.NodeId);
                coordinatorA = new AsyncStorageSyncCoordinator(nodeA, metadataA, scope, namespaceId);
                adapterA = new FldBinSyncAsyncHostAdapter(
                    coordinatorA, providerA!, remoteA!,
                    current => StorageNodePersistence.Save(nodeStorageA!, current));
                Assert.True(gcA.TryReadNamespaceRoster(namespaceId, trust, out _));
                Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                    await adapterA.RecoverBeforeAcceptingSessionsAsync());

                MetadataOutboundBatchReservation? recoveredReservation =
                    metadataA.GetPendingOutboundBatch(
                        scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA);
                Assert.NotNull(recoveredReservation);
                Assert.Equal(raceReservation.BatchId, recoveredReservation!.BatchId);
                Assert.Equal(raceReservation.IssuerEpoch, recoveredReservation.IssuerEpoch);
                Assert.Equal(raceReservation.StreamOffset, recoveredReservation.StreamOffset);
                Assert.Equal(raceReservation.CursorStart, recoveredReservation.CursorStart);
                Assert.Equal(raceReservation.CursorEnd, recoveredReservation.CursorEnd);
                Assert.True(raceReservation.PreviousBatchDigest.AsSpan()
                    .SequenceEqual(recoveredReservation.PreviousBatchDigest));
                Assert.True(raceReservation.BatchDigest.AsSpan()
                    .SequenceEqual(recoveredReservation.BatchDigest));
                Assert.True(raceReservation.Payload.AsSpan()
                    .SequenceEqual(recoveredReservation.Payload));

                MetadataOutboundBatchReservation retriedReservation =
                    metadataA.ReserveOutboundBatch(
                        scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA, raceReservation);
                Assert.Equal(raceReservation.BatchId, retriedReservation.BatchId);
                Assert.True(raceReservation.Payload.AsSpan()
                    .SequenceEqual(retriedReservation.Payload));
                metadataA.MarkOutboundBatchPushed(
                    scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA, retriedReservation.BatchId);

                started.Reset();
                Task ackTask;
                using (metadataA.AcquireTombstoneGcCommitGuard(
                    scope, nodeA.NodeId, namespaceId, gcA, trust, out _))
                {
                    ackTask = Task.Run(() =>
                    {
                        started.Set();
                        metadataA.AcknowledgePeerBatch(
                            scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA,
                            reserved.BatchId, reserved.CursorEnd, reserved.BatchDigest);
                    });
                    Assert.True(started.Wait(TimeSpan.FromSeconds(5)));
                    Thread.Sleep(100);
                    Assert.False(ackTask.IsCompleted,
                        "Outbound ACK must wait behind the final GC commit guard.");
                }
                await ackTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Null(metadataA.GetPendingOutboundBatch(
                    scope, nodeA.NodeId, racePeer, 1, namespaceId, authorA));
            }

            IReadOnlyList<ProviderMutation> candidatesA =
                await providerA.ReadTombstoneGcCandidatesAsync(16);
            Assert.Equal(2, candidatesA.Count);
            ProviderMutation candidateA = candidatesA[0];
            Assert.True(gcA.HasProtocolTombstoneGcProof(
                gcA.TryReadNamespaceRoster(namespaceId, trust, out NamespaceRosterCertificate? rosterA)
                    ? rosterA! : throw new InvalidOperationException("Host A roster disappeared."),
                trust, candidateA.AuthorNodeId, candidateA.OriginAuthorEpoch,
                candidateA.OriginSequence, candidateA.HlcWall, candidateA.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host A provider tombstone identity must match the accepted signed proof.");

            Assert.Equal(1, await adapterA.RunTombstoneGcPassAsync(gcA, trust, 1));
            Assert.Equal(1, await adapterB.RunTombstoneGcPassAsync(gcB, trust, 1));
            Assert.Single(await providerA.ReadTombstoneGcCandidatesAsync(16));
            Assert.Single(await providerB.ReadTombstoneGcCandidatesAsync(16));

            // WAL is now sealed on both Hosts. Receipt GC can advance independently
            // from the remaining physical tombstone page. Commit one bounded receipt
            // page, restart both real Hosts, then resume the remaining pages.
            Assert.Equal(1, await adapterA.RunReceiptGcPassAsync(gcA, trust, limit: 1));

            await relayInitiator.DisposeAsync(); relayInitiator = null;
            await relayAcceptor.DisposeAsync(); relayAcceptor = null;
            relayNode.Dispose(); relayNode = null;
            await initiator.DisposeAsync(); initiator = null;
            await acceptor.DisposeAsync(); acceptor = null;
            await RestartHostAAfterGcPageAsync();
            await RestartHostBAfterGcPageAsync();

            for (int page = 1; page < retiredReceiptBatches.Count; page++)
                Assert.Equal(1, await adapterA.RunReceiptGcPassAsync(gcA, trust, limit: 1));
            Assert.Equal(0, await adapterA.RunReceiptGcPassAsync(gcA, trust, limit: 1));

            foreach (MetadataIncomingBatch retiredReceiptBatch in retiredReceiptBatches)
                Assert.Throws<InvalidDataException>(
                    () => metadataA.AcceptIncomingBatch(retiredReceiptBatch));
            Assert.True(metadataA.AcceptIncomingBatch(replacementReceiptBatch).IsDuplicate,
                "The current transport epoch's receipt must remain a durable replay proof.");
            // The Host's committed prefix is now sealed and its transient
            // ChangeId references are released. The ChangeId horizon can advance
            // independently even though one physical tombstone remains on each Host.
            Assert.Equal(2, await adapterA.RunChangeIdGcPassAsync(gcA, trust, 16));
            Assert.Equal(2, await adapterB.RunChangeIdGcPassAsync(gcB, trust, 16));
            Assert.Equal(0, CountTombstoneChangeIds(metadataA, nodeA.NodeId));
            Assert.Equal(0, CountTombstoneChangeIds(metadataB, nodeB.NodeId));
            Assert.Throws<InvalidDataException>(
                () => metadataA.AcceptIncomingBatch(replacementReceiptBatch));
            Assert.NotEqual(
                await providerA.ReadCurrentAsync(record, 0x0951) is null,
                await providerA.ReadCurrentAsync(record, 0x0952) is null);
            Assert.NotEqual(
                await providerB.ReadCurrentAsync(record, 0x0951) is null,
                await providerB.ReadCurrentAsync(record, 0x0952) is null);

            // Both Hosts were restarted between the bounded Receipt-GC pages above.
            // The remaining physical tombstone page now resumes from those reopened Hosts.

            Assert.Single(await providerA!.ReadTombstoneGcCandidatesAsync(16));
            Assert.Single(await providerB!.ReadTombstoneGcCandidatesAsync(16));
            Assert.Equal(1, await adapterA.RunTombstoneGcPassAsync(gcA, trust, 1));
            Assert.Equal(1, await adapterB.RunTombstoneGcPassAsync(gcB, trust, 1));
            Assert.Empty(await providerA.ReadTombstoneGcCandidatesAsync(16));
            Assert.Empty(await providerB.ReadTombstoneGcCandidatesAsync(16));
            Assert.Null(await providerA.ReadCurrentAsync(record, 0x0951));
            Assert.Null(await providerA.ReadCurrentAsync(record, 0x0952));
            Assert.Null(await providerB.ReadCurrentAsync(record, 0x0951));
            Assert.Null(await providerB.ReadCurrentAsync(record, 0x0952));

            // Tombstone page two also completes after Host restart, independently
            // of the already-reclaimed ChangeId ledger identities.
            Assert.Equal(0, CountTombstoneChangeIds(metadataA, nodeA.NodeId));
            Assert.Equal(0, CountTombstoneChangeIds(metadataB, nodeB.NodeId));

            await remoteA.DisposeAsync(); remoteA = null;
            await remoteB.DisposeAsync(); remoteB = null;
            await providerA.DisposeAsync(); providerA = null;
            await providerB.DisposeAsync(); providerB = null;
            nodeA.Dispose(); nodeA = null;
            nodeB.Dispose(); nodeB = null;
            walA.Dispose(); walA = null;
            walB.Dispose(); walB = null;
            metadataA.Dispose(); metadataA = null;
            metadataB.Dispose(); metadataB = null;
            nodeStorageA.Dispose(); nodeStorageA = null;
            nodeStorageB.Dispose(); nodeStorageB = null;
            await StopDatabaseHostAsync(hostA); hostA = null;
            await StopDatabaseHostAsync(hostB); hostB = null;
            await WaitForDatabaseWriterReleaseAsync(hostRootA, namespaceId, authorABytes, TimeSpan.FromSeconds(15));
            await WaitForDatabaseWriterReleaseAsync(hostRootB, namespaceId, authorBBytes, TimeSpan.FromSeconds(15));

            using var reopenedA = FldDatabase.Open(hostRootA, namespaceId, authorABytes);
            using var reopenedB = FldDatabase.Open(hostRootB, namespaceId, authorBBytes);
            Assert.False(reopenedA.TryResolve(record.ToArray(), 0x0951, out _),
                "Host A's safely reclaimed tombstone must remain absent after durable reopen.");
            Assert.False(reopenedB.TryResolve(record.ToArray(), 0x0951, out _),
                "Host B's safely reclaimed tombstone must remain absent after durable reopen.");
        }
        finally
        {
            if (relayInitiator is not null) await relayInitiator.DisposeAsync();
            if (relayAcceptor is not null) await relayAcceptor.DisposeAsync();
            relayNode?.Dispose();
            if (initiator is not null) await initiator.DisposeAsync();
            if (acceptor is not null) await acceptor.DisposeAsync();
            if (remoteA is not null) await remoteA.DisposeAsync();
            if (remoteB is not null) await remoteB.DisposeAsync();
            if (providerA is not null) await providerA.DisposeAsync();
            if (providerB is not null) await providerB.DisposeAsync();
            nodeA?.Dispose();
            nodeB?.Dispose();
            walA?.Dispose();
            walB?.Dispose();
            metadataA?.Dispose();
            metadataB?.Dispose();
            nodeStorageA?.Dispose();
            nodeStorageB?.Dispose();
            if (hostA is not null) await StopDatabaseHostAsync(hostA);
            if (hostB is not null) await StopDatabaseHostAsync(hostB);
            if (ownerA != Guid.Empty) StorageIpcEndpoint.DeleteIfOwned(hostRootA, namespaceId, ownerA);
            if (ownerB != Guid.Empty) StorageIpcEndpoint.DeleteIfOwned(hostRootB, namespaceId, ownerB);
            try { Directory.Delete(hostRootA, recursive: true); } catch { }
            try { Directory.Delete(hostRootB, recursive: true); } catch { }
            try { Directory.Delete(nodeRootA, recursive: true); } catch { }
            try { Directory.Delete(nodeRootB, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_storage_coordinator_publishes_through_real_database_host_and_survives_host_restart()
    {
        string hostRoot = Path.Combine(Path.GetTempPath(), "fld-binsync-host-process-" + Guid.NewGuid().ToString("N"));
        string nodeRoot = Path.Combine(Path.GetTempPath(), "binsync-host-process-node-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x535;
        byte[] authorBytes = Id(0x55);
        NodeId author = NodeId.From(authorBytes);
        Guid clientId = Guid.NewGuid();
        Guid firstOwner = Guid.Empty;
        Guid secondOwner = Guid.Empty;
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? provider = null;
        try
        {
            using (var nodeStorage = StorageDatabase.Open(nodeRoot, 701))
            using (var metadata = new StorageMetadataStore(nodeStorage))
            using (var nodeWal = Wal.Open(Path.Combine(nodeRoot, "node.wal")))
            {
                Node node = new NodeBuilder().WithNodeId(author).WithEpoch(17).Build();
                node.AttachWal(nodeWal);
                try
                {
                    RecordId record = RecordId.From(Id(0x56));
                    BatchId changeId = BatchId.From(Id(0x57));
                    byte[] value = "real-host-async-publication"u8.ToArray();
                    const string scope = "real-host-async-publication";

                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo firstEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45));
                    firstOwner = firstEndpoint.OwnerId;

                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);
                    await provider.EnableMetadataStoreOwnedPublicationAsync(0);
                    ulong clientSequence = await provider.AppendLocalChangeWithReceiptAsync(new ProviderMutation
                    {
                        RecordId = record,
                        FieldId = 0x0551,
                        AuthorNodeId = author,
                        OriginAuthorEpoch = 17,
                        OriginSequence = 0,
                        HlcWall = 55_700,
                        HlcLogical = 3,
                        Value = value,
                        ChangeId = changeId
                    });
                    Assert.Equal(1UL, clientSequence);

                    // Simulate Host restart after the FLD WAL commit but before
                    // BIN-SYNC has recorded ClientSequence in its Node WAL. Reconnect
                    // with the same ClientId; coordinator retry must recover the
                    // original receipt instead of appending a second Outbox operation.
                    await provider.DisposeAsync();
                    provider = null;
                    await StopDatabaseHostAsync(host);
                    host = null;
                    await WaitForDatabaseWriterReleaseAsync(
                        hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));
                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo receiptRecoveryEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45), firstOwner);
                    Assert.NotEqual(firstOwner, receiptRecoveryEndpoint.OwnerId);
                    secondOwner = receiptRecoveryEndpoint.OwnerId;
                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);

                    var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, checked((int)namespaceId));
                    int published = await coordinator.PublishLocalChangesAsync(
                        provider, current => StorageNodePersistence.Save(nodeStorage, current));

                    Assert.Equal(1, published);
                    Assert.Equal(1UL, metadata.GetOutboundHighWatermark(
                        scope, node.NodeId, checked((int)namespaceId), node.NodeId));
                    Assert.True(node.TryGetField(checked((int)namespaceId), record, 0x0551, out var cached));
                    Assert.Equal(value, cached!.Value);

                    ProviderMutation? current = await provider.ReadCurrentAsync(record, 0x0551);
                    Assert.NotNull(current);
                    Assert.Equal(changeId, current!.ChangeId);
                    Assert.Equal(1UL, current.OriginSequence);
                    Assert.Empty((await provider.ReadLocalChangesAsync(0, 16)).Changes);
                    Assert.Equal(1UL, await provider.GetLocalChangeHighWatermarkAsync());

                    await provider.DisposeAsync();
                    provider = null;
                    await StopDatabaseHostAsync(host);
                    host = null;
                    await WaitForDatabaseWriterReleaseAsync(
                        hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));

                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo thirdEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45), secondOwner);
                    Assert.NotEqual(secondOwner, thirdEndpoint.OwnerId);

                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);
                    current = await provider.ReadCurrentAsync(record, 0x0551);
                    Assert.NotNull(current);
                    Assert.Equal(value, current!.Value);
                    Assert.Equal(changeId, current.ChangeId);
                    Assert.Equal(1UL, current.OriginSequence);
                    Assert.Empty((await provider.ReadLocalChangesAsync(0, 16)).Changes);
                    Assert.Equal(1UL, await provider.GetLocalChangeHighWatermarkAsync());
                    await provider.AcknowledgeOperationReceiptsAsync(clientSequence);
                }
                finally
                {
                    node.Dispose();
                }
            }
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
            try { Directory.Delete(nodeRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_remote_apply_uses_real_database_host_and_is_idempotent_across_ipc_clients()
    {
        string hostRoot = Path.Combine(Path.GetTempPath(), "fld-binsync-async-apply-" + Guid.NewGuid().ToString("N"));
        const ushort namespaceId = 0x536;
        byte[] localAuthor = Id(0x65);
        byte[] sender = Id(0x66);
        byte[] issuer = Id(0x67);
        byte[] recordBytes = Id(0x68);
        byte[] changeBytes = Id(0x69);
        byte[] batchBytes = Id(0x6A);
        byte[] value = "async-remote-apply-over-real-ipc"u8.ToArray();
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? reader = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? applyStore = null;
        try
        {
            host = StartDatabaseHost(
                hostRoot, namespaceId, localAuthor, dropRemoteApplyResponseForTest: true);
            StorageIpcEndpointInfo firstEndpoint =
                await WaitForHostEndpointAsync(hostRoot, host, TimeSpan.FromSeconds(45));

            reader = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15),
                NodeId.From(localAuthor), () => 1);
            applyStore = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRoot, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));

            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(recordBytes),
                FieldId = 0x0651,
                AuthorNodeId = NodeId.From(issuer),
                KeyEpoch = 0,
                Value = value,
                ChangeId = BatchId.From(changeBytes),
                OriginAuthorEpoch = 3,
                OriginSequence = 1,
                HlcWall = 65_100,
                HlcLogical = 2
            };
            var batch = new ProviderRemoteBatchCommit(
                Guid.NewGuid(),
                NodeId.From(sender),
                senderTransportEpoch: 4,
                namespaceId: namespaceId,
                NodeId.From(issuer),
                issuerEpoch: 3,
                BatchId.From(batchBytes),
                cursorStart: 1,
                cursorEnd: 1,
                Id(0x6B),
                Id(0x6C),
                Id(0x6D),
                protocolStateDelta: Array.Empty<byte>(),
                new[] { mutation });

            // Release the independent read provider while the Host is still alive;
            // its Dispose path releases an Outbox lease and must not race the deliberate
            // process death below. A fresh reader is connected after Host recovery.
            await reader.DisposeAsync();
            reader = null;

            // The real Host process exits immediately after ApplyRemoteBatch has
            // committed its WAL/receipt, but before StorageIpcServer writes the IPC
            // response frame. The caller must observe response loss, not a receipt.
            Exception lostResponse = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await applyStore.ApplyRemoteBatchAsync(batch));
            Assert.True(
                lostResponse is IOException or InvalidDataException or ObjectDisposedException,
                $"Expected IPC response loss after durable remote APPLY, received {lostResponse.GetType().Name}.");
            await host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(86, host.ExitCode);

            await applyStore.DisposeAsync();
            applyStore = null;
            await StopDatabaseHostAsync(host);
            host = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRoot, namespaceId, localAuthor, TimeSpan.FromSeconds(15));

            // Restart the real Host against the same database root and retry the
            // exact immutable batch through fresh IPC clients. The ApplyJournal,
            // not the old IPC connection/client identity, must return its receipt.
            host = StartDatabaseHost(hostRoot, namespaceId, localAuthor);
            _ = await WaitForHostEndpointAsync(
                hostRoot, host, TimeSpan.FromSeconds(45), firstEndpoint.OwnerId);

            reader = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRoot, Guid.NewGuid(), TimeSpan.FromSeconds(15),
                NodeId.From(localAuthor), () => 1);
            applyStore = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRoot, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));

            ProviderApplyReceipt retry = await applyStore.ApplyRemoteBatchAsync(batch);
            Assert.Equal(batch.ApplyToken, retry.ApplyToken);
            Assert.Equal(batch.BatchDigest.ToArray(), retry.BatchFingerprint);
            Assert.True(retry.IsDuplicate);

            ProviderMutation? current = await reader.ReadCurrentAsync(
                RecordId.From(recordBytes), 0x0651);
            Assert.NotNull(current);
            Assert.Equal(value, current!.Value);
            Assert.Equal(BatchId.From(changeBytes), current.ChangeId);
            Assert.Equal(3U, current.OriginAuthorEpoch);
            Assert.Equal(1UL, current.OriginSequence);
            Assert.Equal(0UL, await reader.GetLocalChangeHighWatermarkAsync());
        }
        finally
        {
            if (applyStore is not null)
                await applyStore.DisposeAsync();
            if (reader is not null)
                await reader.DisposeAsync();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_host_composition_exchanges_changes_bidirectionally_between_two_database_host_processes()
    {
        string hostRootA = Path.Combine(Path.GetTempPath(), "fld-binsync-two-host-a-" + Guid.NewGuid().ToString("N"));
        string hostRootB = Path.Combine(Path.GetTempPath(), "fld-binsync-two-host-b-" + Guid.NewGuid().ToString("N"));
        string nodeRootA = Path.Combine(Path.GetTempPath(), "binsync-two-host-node-a-" + Guid.NewGuid().ToString("N"));
        string nodeRootB = Path.Combine(Path.GetTempPath(), "binsync-two-host-node-b-" + Guid.NewGuid().ToString("N"));
        const ushort namespaceId = 0x537;
        const uint nodeStorageNamespace = 702;
        byte[] authorABytes = Id(0x71);
        byte[] authorBBytes = Id(0x72);
        NodeId authorA = NodeId.From(authorABytes);
        NodeId authorB = NodeId.From(authorBBytes);
        Process? hostA = null;
        Process? hostB = null;
        StorageDatabase? nodeStorageA = null;
        StorageDatabase? nodeStorageB = null;
        StorageMetadataStore? metadataA = null;
        StorageMetadataStore? metadataB = null;
        Wal? walA = null;
        Wal? walB = null;
        Node? nodeA = null;
        Node? nodeB = null;
        FldDatabaseIpcAsyncDataProvider? providerA = null;
        FldDatabaseIpcAsyncDataProvider? providerB = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteA = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteB = null;
        SyncSession? acceptor = null;
        SyncSession? initiator = null;
        Guid hostOwnerA = Guid.Empty;
        Guid hostOwnerB = Guid.Empty;
        try
        {
            hostA = StartDatabaseHost(hostRootA, namespaceId, authorABytes);
            hostB = StartDatabaseHost(hostRootB, namespaceId, authorBBytes);
            hostOwnerA = (await WaitForHostEndpointAsync(hostRootA, hostA, TimeSpan.FromSeconds(45))).OwnerId;
            hostOwnerB = (await WaitForHostEndpointAsync(hostRootB, hostB, TimeSpan.FromSeconds(45))).OwnerId;

            nodeStorageA = StorageDatabase.Open(nodeRootA, nodeStorageNamespace);
            nodeStorageB = StorageDatabase.Open(nodeRootB, nodeStorageNamespace);
            metadataA = new StorageMetadataStore(nodeStorageA);
            metadataB = new StorageMetadataStore(nodeStorageB);
            walA = Wal.Open(Path.Combine(nodeRootA, "node.wal"));
            walB = Wal.Open(Path.Combine(nodeRootB, "node.wal"));
            nodeA = new NodeBuilder().WithNodeId(authorA).WithEpoch(17).Build();
            nodeB = new NodeBuilder().WithNodeId(authorB).WithEpoch(17).Build();
            nodeA.AttachWal(walA);
            nodeB.AttachWal(walB);

            Guid clientA = Guid.NewGuid();
            Guid clientB = Guid.NewGuid();
            providerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootA, clientA, TimeSpan.FromSeconds(15), authorA, () => 17);
            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, clientB, TimeSpan.FromSeconds(15), authorB, () => 17);
            remoteA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            await providerA.EnableMetadataStoreOwnedPublicationAsync(0);
            await providerB.EnableMetadataStoreOwnedPublicationAsync(0);

            var coordinatorA = new AsyncStorageSyncCoordinator(nodeA, metadataA, "two-host-a", checked((int)namespaceId));
            var coordinatorB = new AsyncStorageSyncCoordinator(nodeB, metadataB, "two-host-b", checked((int)namespaceId));
            var adapterA = new FldBinSyncAsyncHostAdapter(
                coordinatorA, providerA, remoteA,
                current => StorageNodePersistence.Save(nodeStorageA, current));
            var adapterB = new FldBinSyncAsyncHostAdapter(
                coordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));

            AsyncStorageStartupRecoveryResult startupA = await adapterA.RecoverBeforeAcceptingSessionsAsync();
            AsyncStorageStartupRecoveryResult startupB = await adapterB.RecoverBeforeAcceptingSessionsAsync();
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0), startupA);
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0), startupB);

            RecordId recordA = RecordId.From(Id(0x73));
            RecordId recordB = RecordId.From(Id(0x74));
            byte[] valueA = "host-a-to-host-b"u8.ToArray();
            byte[] valueB = "host-b-to-host-a"u8.ToArray();
            await providerA.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = recordA,
                FieldId = 0x0751,
                AuthorNodeId = authorA,
                OriginAuthorEpoch = 17,
                HlcWall = 71_100,
                HlcLogical = 1,
                Value = valueA,
                ChangeId = BatchId.From(Id(0x75))
            });
            await providerB.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = recordB,
                FieldId = 0x0752,
                AuthorNodeId = authorB,
                OriginAuthorEpoch = 17,
                HlcWall = 72_100,
                HlcLogical = 2,
                Value = valueB,
                ChangeId = BatchId.From(Id(0x76))
            });

            var (toA, toB) = PipeTransport.CreatePair();
            Task<SyncSession> acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            Task<SyncSession> connectTask = SyncSession.ConnectAsync(nodeA, toA);
            acceptor = await acceptTask;
            initiator = await connectTask;

            // The test begins after each host has completed its initial snapshot.
            // Persist the empty per-author baseline so incremental DELTA_BATCH
            // delivery is legal for both newly registered peer incarnations.
            metadataA!.RegisterInterestedPeer("two-host-a", nodeA!.NodeId, nodeB!.NodeId,
                checked((uint)initiator.PeerEpoch), checked((int)namespaceId));
            metadataA.CompletePeerInitialSync("two-host-a", nodeA.NodeId, nodeB.NodeId,
                checked((uint)initiator.PeerEpoch), checked((int)namespaceId),
                new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, 17) });
            metadataB!.RegisterInterestedPeer("two-host-b", nodeB.NodeId, nodeA.NodeId,
                checked((uint)acceptor.PeerEpoch), checked((int)namespaceId));
            metadataB.CompletePeerInitialSync("two-host-b", nodeB.NodeId, nodeA.NodeId,
                checked((uint)acceptor.PeerEpoch), checked((int)namespaceId),
                new[] { new MetadataPeerStreamBaseline(nodeB.NodeId, 0, 17) });

            Task<BatchResult> receiveA = adapterA.ReceiveBatchAsync(initiator);
            Task<BatchResult> receiveB = adapterB.ReceiveBatchAsync(acceptor);
            Task<int> sendA = adapterA.RunOutboundRoundAsync([initiator]);
            Task<int> sendB = adapterB.RunOutboundRoundAsync([acceptor]);
            await Task.WhenAll(receiveA, receiveB, sendA, sendB).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.Equal(1, (await receiveA).Applied);
            Assert.Equal(1, (await receiveB).Applied);
            Assert.Equal(1, await sendA);
            Assert.Equal(1, await sendB);

            ProviderMutation? currentAtA = await providerA.ReadCurrentAsync(recordB, 0x0752);
            ProviderMutation? currentAtB = await providerB.ReadCurrentAsync(recordA, 0x0751);
            Assert.NotNull(currentAtA);
            Assert.NotNull(currentAtB);
            Assert.Equal(valueB, currentAtA!.Value);
            Assert.Equal(valueA, currentAtB!.Value);
            Assert.True(nodeA.TryGetField(checked((int)namespaceId), recordB, 0x0752, out var nodeValueAtA));
            Assert.True(nodeB.TryGetField(checked((int)namespaceId), recordA, 0x0751, out var nodeValueAtB));
            Assert.Equal(valueB, nodeValueAtA!.Value);
            Assert.Equal(valueA, nodeValueAtB!.Value);

            Assert.Equal(1UL, metadataA.GetReceiveCursor(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, 17, checked((int)namespaceId))).State.AckedCursor);
            Assert.Equal(1UL, Assert.Single(metadataB.ReadPeerStreamStates(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId))).State.AckedCursor);
            Assert.Empty((await providerA.ReadLocalChangesAsync(0, 16)).Changes);
            Assert.Empty((await providerB.ReadLocalChangesAsync(0, 16)).Changes);

            // Tear down both complete host compositions, not only the FLD Database
            // Host processes. The next phase must reconstruct Node, WAL and Metadata
            // exclusively from their durable checkpoints before accepting sessions.
            await initiator.DisposeAsync();
            initiator = null;
            await acceptor.DisposeAsync();
            acceptor = null;
            await providerA.DisposeAsync();
            providerA = null;
            await providerB.DisposeAsync();
            providerB = null;
            await remoteA.DisposeAsync();
            remoteA = null;
            await remoteB.DisposeAsync();
            remoteB = null;
            nodeA.Dispose();
            nodeA = null;
            nodeB.Dispose();
            nodeB = null;
            walA.Dispose();
            walA = null;
            walB.Dispose();
            walB = null;
            metadataA.Dispose();
            metadataA = null;
            metadataB.Dispose();
            metadataB = null;
            nodeStorageA.Dispose();
            nodeStorageA = null;
            nodeStorageB.Dispose();
            nodeStorageB = null;
            await StopDatabaseHostAsync(hostA);
            hostA = null;
            await StopDatabaseHostAsync(hostB);
            hostB = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRootA, namespaceId, authorABytes, TimeSpan.FromSeconds(15));
            await WaitForDatabaseWriterReleaseAsync(
                hostRootB, namespaceId, authorBBytes, TimeSpan.FromSeconds(15));

            hostA = StartDatabaseHost(hostRootA, namespaceId, authorABytes);
            hostB = StartDatabaseHost(hostRootB, namespaceId, authorBBytes);
            hostOwnerA = (await WaitForHostEndpointAsync(
                hostRootA, hostA, TimeSpan.FromSeconds(45), hostOwnerA)).OwnerId;
            hostOwnerB = (await WaitForHostEndpointAsync(
                hostRootB, hostB, TimeSpan.FromSeconds(45), hostOwnerB)).OwnerId;

            nodeStorageA = StorageDatabase.Open(nodeRootA, nodeStorageNamespace);
            nodeStorageB = StorageDatabase.Open(nodeRootB, nodeStorageNamespace);
            metadataA = new StorageMetadataStore(nodeStorageA);
            metadataB = new StorageMetadataStore(nodeStorageB);
            walA = Wal.Open(Path.Combine(nodeRootA, "node.wal"));
            walB = Wal.Open(Path.Combine(nodeRootB, "node.wal"));
            nodeA = StorageNodePersistence.Load(nodeStorageA, authorA);
            nodeB = StorageNodePersistence.Load(nodeStorageB, authorB);
            nodeA.AttachWal(walA);
            nodeB.AttachWal(walB);

            providerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootA, clientA, TimeSpan.FromSeconds(15), authorA, () => 17);
            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, clientB, TimeSpan.FromSeconds(15), authorB, () => 17);
            remoteA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));

            var recoveredCoordinatorA = new AsyncStorageSyncCoordinator(
                nodeA, metadataA, "two-host-a", checked((int)namespaceId));
            var recoveredCoordinatorB = new AsyncStorageSyncCoordinator(
                nodeB, metadataB, "two-host-b", checked((int)namespaceId));
            var recoveredAdapterA = new FldBinSyncAsyncHostAdapter(
                recoveredCoordinatorA, providerA, remoteA,
                current => StorageNodePersistence.Save(nodeStorageA, current));
            var recoveredAdapterB = new FldBinSyncAsyncHostAdapter(
                recoveredCoordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));

            AsyncStorageStartupRecoveryResult recoveredA =
                await recoveredAdapterA.RecoverBeforeAcceptingSessionsAsync();
            AsyncStorageStartupRecoveryResult recoveredB =
                await recoveredAdapterB.RecoverBeforeAcceptingSessionsAsync();
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0), recoveredA);
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0), recoveredB);

            ProviderMutation? recoveredAtA = await providerA.ReadCurrentAsync(recordB, 0x0752);
            ProviderMutation? recoveredAtB = await providerB.ReadCurrentAsync(recordA, 0x0751);
            Assert.NotNull(recoveredAtA);
            Assert.NotNull(recoveredAtB);
            Assert.Equal(valueB, recoveredAtA!.Value);
            Assert.Equal(valueA, recoveredAtB!.Value);
            Assert.Equal(BatchId.From(Id(0x76)), recoveredAtA.ChangeId);
            Assert.Equal(BatchId.From(Id(0x75)), recoveredAtB.ChangeId);
            Assert.True(nodeA.TryGetField(checked((int)namespaceId), recordB, 0x0752, out var recoveredNodeValueAtA));
            Assert.True(nodeB.TryGetField(checked((int)namespaceId), recordA, 0x0751, out var recoveredNodeValueAtB));
            Assert.Equal(valueB, recoveredNodeValueAtA!.Value);
            Assert.Equal(valueA, recoveredNodeValueAtB!.Value);
            Assert.Equal(1UL, metadataA.GetReceiveCursor(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, 17, checked((int)namespaceId))).State.AckedCursor);
            Assert.Equal(1UL, Assert.Single(metadataB.ReadPeerStreamStates(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId))).State.AckedCursor);
            Assert.Empty((await providerA.ReadLocalChangesAsync(0, 16)).Changes);
            Assert.Empty((await providerB.ReadLocalChangesAsync(0, 16)).Changes);

            // Re-establish the transport sessions only after both hosts have
            // completed durable startup recovery. The persisted peer incarnation
            // and cursors must support a fresh handshake without replaying data.
            var (reconnectToA, reconnectToB) = PipeTransport.CreatePair();
            Task<SyncSession> recoveredAcceptTask = SyncSession.AcceptAsync(nodeB, reconnectToB);
            Task<SyncSession> recoveredConnectTask = SyncSession.ConnectAsync(nodeA, reconnectToA);
            acceptor = await recoveredAcceptTask;
            initiator = await recoveredConnectTask;

            Task<int> resumedSendA = recoveredAdapterA.RunOutboundRoundAsync([initiator]);
            Task<int> resumedSendB = recoveredAdapterB.RunOutboundRoundAsync([acceptor]);
            await Task.WhenAll(resumedSendA, resumedSendB).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(0, await resumedSendA);
            Assert.Equal(0, await resumedSendB);
            Assert.Equal(1UL, metadataA.GetReceiveCursor(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, checked((uint)initiator.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);
            Assert.Equal(1UL, Assert.Single(metadataB.ReadPeerStreamStates(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, checked((uint)acceptor.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);

            // Now prove transport ACK loss across the actual two FLD Database Host
            // processes. A's immutable outbound reservation survives the lost ACK;
            // B has already durably committed the remote APPLY and finalized Metadata.
            await initiator.DisposeAsync();
            initiator = null;
            await acceptor.DisposeAsync();
            acceptor = null;

            RecordId ackLossRecord = RecordId.From(Id(0x77));
            byte[] ackLossValue = "host-a-to-host-b-ack-loss"u8.ToArray();
            await providerA.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = ackLossRecord,
                FieldId = 0x0753,
                AuthorNodeId = authorA,
                OriginAuthorEpoch = 17,
                HlcWall = 73_100,
                HlcLogical = 3,
                Value = ackLossValue,
                ChangeId = BatchId.From(Id(0x78))
            });

            var (ackLossToA, ackLossToB) = PipeTransport.CreatePair();
            var droppingAckTransport = new DropFirstAckTransport(ackLossToB);
            Task<SyncSession> ackLossAcceptTask = SyncSession.AcceptAsync(nodeB, droppingAckTransport);
            Task<SyncSession> ackLossConnectTask = SyncSession.ConnectAsync(nodeA, ackLossToA);
            acceptor = await ackLossAcceptTask;
            initiator = await ackLossConnectTask;

            Task<BatchResult> lostAckReceive = recoveredAdapterB.ReceiveBatchAsync(acceptor);
            Task<int> lostAckSend = recoveredAdapterA.RunOutboundRoundAsync([initiator]);
            await Assert.ThrowsAnyAsync<IOException>(async () =>
                await lostAckReceive.WaitAsync(TimeSpan.FromSeconds(60)));
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await lostAckSend.WaitAsync(TimeSpan.FromSeconds(60)));

            ProviderMutation? appliedBeforeRetry = await providerB.ReadCurrentAsync(ackLossRecord, 0x0753);
            Assert.NotNull(appliedBeforeRetry);
            Assert.Equal(ackLossValue, appliedBeforeRetry!.Value);
            Assert.Equal(BatchId.From(Id(0x78)), appliedBeforeRetry.ChangeId);
            Assert.Equal(2UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(1UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, checked((uint)initiator.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);

            await initiator.DisposeAsync();
            initiator = null;
            await acceptor.DisposeAsync();
            acceptor = null;

            // A fresh session replays the exact reserved batch. B returns a duplicate
            // ACK without another provider mutation; only then does A advance its
            // durable peer ACK cursor.
            var (ackRetryToA, ackRetryToB) = PipeTransport.CreatePair();
            Task<SyncSession> ackRetryAcceptTask = SyncSession.AcceptAsync(nodeB, ackRetryToB);
            Task<SyncSession> ackRetryConnectTask = SyncSession.ConnectAsync(nodeA, ackRetryToA);
            acceptor = await ackRetryAcceptTask;
            initiator = await ackRetryConnectTask;

            Task<BatchResult> replayReceive = recoveredAdapterB.ReceiveBatchAsync(acceptor);
            Task<int> replaySend = recoveredAdapterA.RunOutboundRoundAsync([initiator]);
            await Task.WhenAll(replayReceive, replaySend).WaitAsync(TimeSpan.FromSeconds(60));
            BatchResult replayResult = await replayReceive;
            Assert.True(replayResult.Duplicate);
            Assert.Equal(0, replayResult.Applied);
            Assert.Equal(1, await replaySend);
            Assert.Equal(2UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(2UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, checked((uint)initiator.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);
            ProviderMutation? afterReplay = await providerB.ReadCurrentAsync(ackLossRecord, 0x0753);
            Assert.NotNull(afterReplay);
            Assert.Equal(ackLossValue, afterReplay!.Value);
            Assert.Equal(BatchId.From(Id(0x78)), afterReplay.ChangeId);

            // Crash the real FLD Host after its next durable APPLY but before the
            // IPC response reaches the async coordinator. PendingApply and the
            // Node checkpoint belong to BIN-SYNC's storage and must survive this
            // independent provider-process failure.
            await initiator.DisposeAsync();
            initiator = null;
            await acceptor.DisposeAsync();
            acceptor = null;
            FldDatabaseIpcAsyncDataProvider crashedProviderB = providerB
                ?? throw new InvalidOperationException("Expected the crashed Host provider to remain attached.");
            providerB = null;
            try
            {
                await crashedProviderB.DisposeAsync();
            }
            catch (AggregateException error) when (
                error.Flatten().InnerExceptions.Count > 0 &&
                error.Flatten().InnerExceptions.All(inner => inner is IOException))
            {
                // The Host process is already dead, so releasing its Outbox lease
                // over the broken IPC pipe is impossible; the Host's process exit
                // has already released the partition lease itself.
            }
            await remoteB.DisposeAsync();
            remoteB = null;
            await StopDatabaseHostAsync(hostB);
            hostB = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRootB, namespaceId, authorBBytes, TimeSpan.FromSeconds(15));

            hostB = StartDatabaseHost(
                hostRootB, namespaceId, authorBBytes, dropRemoteApplyResponseForTest: true);
            hostOwnerB = (await WaitForHostEndpointAsync(
                hostRootB, hostB, TimeSpan.FromSeconds(45), hostOwnerB)).OwnerId;

            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, clientB, TimeSpan.FromSeconds(15), authorB, () => 17);
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            recoveredAdapterB = new FldBinSyncAsyncHostAdapter(
                recoveredCoordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));

            RecordId processCrashRecord = RecordId.From(Id(0x79));
            byte[] processCrashValue = "host-b-crash-after-durable-apply"u8.ToArray();
            await providerA.AppendLocalChangeWithReceiptAsync(new ProviderMutation
            {
                RecordId = processCrashRecord,
                FieldId = 0x0754,
                AuthorNodeId = authorA,
                OriginAuthorEpoch = 17,
                HlcWall = 74_100,
                HlcLogical = 4,
                Value = processCrashValue,
                ChangeId = BatchId.From(Id(0x7A))
            });

            var (processCrashToA, processCrashToB) = PipeTransport.CreatePair();
            Task<SyncSession> processCrashAcceptTask = SyncSession.AcceptAsync(nodeB, processCrashToB);
            Task<SyncSession> processCrashConnectTask = SyncSession.ConnectAsync(nodeA, processCrashToA);
            acceptor = await processCrashAcceptTask;
            initiator = await processCrashConnectTask;

            Task<BatchResult> processCrashReceive =
                recoveredAdapterB.ReceiveBatchAsync(acceptor);
            Task<int> processCrashSend =
                recoveredAdapterA.RunOutboundRoundAsync([initiator]);
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await processCrashReceive.WaitAsync(TimeSpan.FromSeconds(60)));
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await processCrashSend.WaitAsync(TimeSpan.FromSeconds(60)));
            await hostB.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(86, hostB.ExitCode);

            // The provider committed its ApplyToken, while Metadata finalization
            // did not run because the IPC response was deliberately lost. Reopen
            // the Host and let coordinator startup recovery replay the same token.
            FldDatabaseIpcAsyncDataProvider crashedAfterApplyProviderB = providerB
                ?? throw new InvalidOperationException("Expected the restarted Host provider to remain attached.");
            providerB = null;
            try
            {
                await crashedAfterApplyProviderB.DisposeAsync();
            }
            catch (AggregateException error) when (
                error.Flatten().InnerExceptions.Count > 0 &&
                error.Flatten().InnerExceptions.All(inner => inner is IOException))
            {
                // The second deliberate Host crash also invalidates the IPC
                // lease-release request. DisposeAsync still closes the client
                // connection and feed gate after collecting that expected IO error.
            }
            await remoteB.DisposeAsync();
            remoteB = null;
            await StopDatabaseHostAsync(hostB);
            hostB = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRootB, namespaceId, authorBBytes, TimeSpan.FromSeconds(15));
            hostB = StartDatabaseHost(hostRootB, namespaceId, authorBBytes);
            hostOwnerB = (await WaitForHostEndpointAsync(
                hostRootB, hostB, TimeSpan.FromSeconds(45), hostOwnerB)).OwnerId;

            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, clientB, TimeSpan.FromSeconds(15), authorB, () => 17);
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            recoveredAdapterB = new FldBinSyncAsyncHostAdapter(
                recoveredCoordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));

            AsyncStorageStartupRecoveryResult processCrashRecovery =
                await recoveredAdapterB.RecoverBeforeAcceptingSessionsAsync();
            Assert.Equal(new AsyncStorageStartupRecoveryResult(1, 0), processCrashRecovery);
            Assert.Empty(metadataB.ReadPendingApplyIntents(
                "two-host-b", nodeB.NodeId));
            ProviderMutation? afterProcessRecovery =
                await providerB.ReadCurrentAsync(processCrashRecord, 0x0754);
            Assert.NotNull(afterProcessRecovery);
            Assert.Equal(processCrashValue, afterProcessRecovery!.Value);
            Assert.Equal(BatchId.From(Id(0x7A)), afterProcessRecovery.ChangeId);
            Assert.True(nodeB.TryGetField(
                checked((int)namespaceId), processCrashRecord, 0x0754, out var recoveredCrashNodeValue));
            Assert.Equal(processCrashValue, recoveredCrashNodeValue!.Value);
            Assert.Equal(3UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(2UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, checked((uint)initiator.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);

            await initiator.DisposeAsync();
            initiator = null;
            await acceptor.DisposeAsync();
            acceptor = null;
            var (processRetryToA, processRetryToB) = PipeTransport.CreatePair();
            Task<SyncSession> processRetryAcceptTask = SyncSession.AcceptAsync(nodeB, processRetryToB);
            Task<SyncSession> processRetryConnectTask = SyncSession.ConnectAsync(nodeA, processRetryToA);
            acceptor = await processRetryAcceptTask;
            initiator = await processRetryConnectTask;

            Task<BatchResult> processRetryReceive =
                recoveredAdapterB.ReceiveBatchAsync(acceptor);
            Task<int> processRetrySend =
                recoveredAdapterA.RunOutboundRoundAsync([initiator]);
            await Task.WhenAll(processRetryReceive, processRetrySend)
                .WaitAsync(TimeSpan.FromSeconds(60));
            Assert.True((await processRetryReceive).Duplicate);
            Assert.Equal(0, (await processRetryReceive).Applied);
            Assert.Equal(1, await processRetrySend);
            Assert.Equal(3UL, metadataB.GetReceiveCursor(
                "two-host-b", nodeB.NodeId, nodeA.NodeId, 17, checked((int)namespaceId)));
            Assert.Equal(3UL, Assert.Single(metadataA.ReadPeerStreamStates(
                "two-host-a", nodeA.NodeId, nodeB.NodeId, checked((uint)initiator.PeerEpoch),
                checked((int)namespaceId))).State.AckedCursor);
            ProviderMutation? afterProcessReplay =
                await providerB.ReadCurrentAsync(processCrashRecord, 0x0754);
            Assert.NotNull(afterProcessReplay);
            Assert.Equal(processCrashValue, afterProcessReplay!.Value);
            Assert.Equal(BatchId.From(Id(0x7A)), afterProcessReplay.ChangeId);
        }
        finally
        {
            if (initiator is not null) await initiator.DisposeAsync();
            if (acceptor is not null) await acceptor.DisposeAsync();
            if (providerA is not null) await providerA.DisposeAsync();
            if (providerB is not null) await providerB.DisposeAsync();
            if (remoteA is not null) await remoteA.DisposeAsync();
            if (remoteB is not null) await remoteB.DisposeAsync();
            nodeA?.Dispose();
            nodeB?.Dispose();
            walA?.Dispose();
            walB?.Dispose();
            metadataA?.Dispose();
            metadataB?.Dispose();
            nodeStorageA?.Dispose();
            nodeStorageB?.Dispose();
            if (hostA is not null) await StopDatabaseHostAsync(hostA);
            if (hostB is not null) await StopDatabaseHostAsync(hostB);
            try { Directory.Delete(hostRootA, recursive: true); } catch { }
            try { Directory.Delete(hostRootB, recursive: true); } catch { }
            try { Directory.Delete(nodeRootA, recursive: true); } catch { }
            try { Directory.Delete(nodeRootB, recursive: true); } catch { }
        }
    }

    private sealed class DropFirstAckTransport : IPeerAwareSyncTransport
    {
        private readonly ISyncTransport _inner;
        private int _drop = 1;
        private NodeId _peerNodeId;

        public DropFirstAckTransport(ISyncTransport inner) => _inner = inner;

        public NodeId PeerNodeId
        {
            get => _peerNodeId;
            set
            {
                _peerNodeId = value;
                if (_inner is IPeerAwareSyncTransport aware)
                    aware.PeerNodeId = value;
            }
        }

        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => _inner.MaxPacketSize;

        public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
        {
            var (frame, _) = FrameCodec.ParseFrame(packet.ToArray());
            if (frame.FrameType == 0x0B && Interlocked.Exchange(ref _drop, 0) == 1)
            {
                await _inner.DisposeAsync().ConfigureAwait(false);
                throw new IOException("Injected transport ACK loss after durable FLD APPLY.");
            }
            await _inner.SendAsync(packet, ct).ConfigureAwait(false);
        }

        public ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default) =>
            _inner.ReceiveAsync(ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private static Process StartDatabaseHost(
        string root, uint namespaceId, byte[] localAuthor,
        bool dropRemoteApplyResponseForTest = false, bool enableTombstoneGc = false)
    {
        AssemblyMetadataAttribute repositoryMetadata = typeof(FldDatabaseIpcAsyncDataProviderTests)
            .Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(attribute => attribute.Key == "Fld1RepositoryRoot");
        string repositoryRoot = repositoryMetadata.Value
            ?? throw new InvalidOperationException("FLD repository root assembly metadata is empty.");
        string project = Path.Combine(repositoryRoot, "src", "Fld1.Database.Host", "Fld1.Database.Host.csproj");
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--project");
        start.ArgumentList.Add(project);
        start.ArgumentList.Add("--configuration");
#if DEBUG
        start.ArgumentList.Add("Debug");
#else
        start.ArgumentList.Add("Release");
#endif
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-launch-profile");
        start.ArgumentList.Add("--");
        start.ArgumentList.Add(root);
        start.ArgumentList.Add(namespaceId.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add(Convert.ToHexString(localAuthor));
        if (enableTombstoneGc)
            start.ArgumentList.Add("--enable-tombstone-gc");
        if (dropRemoteApplyResponseForTest)
            start.ArgumentList.Add("--test-drop-remote-apply-response");
        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start the real FLD Database Host process.");
    }

    private static async Task<StorageIpcEndpointInfo> WaitForHostEndpointAsync(
        string root, Process host, TimeSpan timeout, Guid? previousOwner = null)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (host.HasExited)
            {
                string stdout = await host.StandardOutput.ReadToEndAsync();
                string stderr = await host.StandardError.ReadToEndAsync();
                throw new InvalidOperationException(
                    $"FLD Database Host exited with {host.ExitCode}. stdout={stdout} stderr={stderr}");
            }
            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, 0)))
            {
                try
                {
                    StorageIpcEndpointInfo endpoint = StorageIpcEndpoint.Read(root, 0);
                    if (previousOwner is null || endpoint.OwnerId != previousOwner.Value)
                        return endpoint;
                }
                catch (IOException) { }
                catch (InvalidDataException) { }
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("Real FLD Database Host did not publish a fresh IPC endpoint.");
    }

    private static async Task WaitForDatabaseWriterReleaseAsync(
        string root, uint namespaceId, byte[] localAuthor, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        IOException? lastFailure = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var probe = FldDatabase.Open(root, namespaceId, localAuthor);
                return;
            }
            catch (IOException exception)
            {
                lastFailure = exception;
                await Task.Delay(50);
            }
        }
        throw new TimeoutException("FLD Database Host process exited but its writer fence was not released.", lastFailure);
    }

    private static async Task StopDatabaseHostAsync(Process host)
    {
        try
        {
            if (!host.HasExited)
                host.Kill(entireProcessTree: true);
            await host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
        finally
        {
            host.Dispose();
        }
    }

    [Fact]
    public async Task Async_ipc_provider_appends_reads_snapshots_and_acknowledges_without_sync_bridge()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-async-provider-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x534;
        byte[] authorBytes = Id(0x41);
        NodeId author = NodeId.From(authorBytes);
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();
        byte[] recordBytes = FldDatabase.NewRecordId();
        byte[] changeBytes = Id(0x72);
        byte[] value = "async-provider-value"u8.ToArray();

        using var database = FldDatabase.Open(root, namespaceId, authorBytes);
        using var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                    root, Guid.NewGuid(), TimeSpan.FromSeconds(10), NodeId.From(Id(0x42)), () => 17));

            await using var provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, clientId, TimeSpan.FromSeconds(10), author, () => 17);

            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(recordBytes),
                FieldId = 7,
                HlcWall = 123456,
                HlcLogical = 9,
                AuthorNodeId = author,
                KeyEpoch = 3,
                Value = value,
                ChangeId = BatchId.From(changeBytes),
                OriginAuthorEpoch = 17,
                OriginSequence = 0
            };
            ulong receiptSequence = await provider.AppendLocalChangeWithReceiptAsync(mutation);
            Assert.Equal(1ul, receiptSequence);

            ProviderChangePage changes = await provider.ReadLocalChangesAsync(0, 16);
            ProviderChange change = Assert.Single(changes.Changes);
            Assert.Equal(1ul, change.Sequence);
            Assert.Equal(BatchId.From(changeBytes), change.ChangeId);
            Assert.Equal(value, change.Mutation.Value);
            Assert.Equal(1ul, changes.ScannedThroughSequence);

            ProviderMutation? current = await provider.ReadCurrentAsync(RecordId.From(recordBytes), 7);
            Assert.NotNull(current);
            Assert.Equal(changeBytes, current.ChangeId.ToArray());
            Assert.Equal(value, current.Value);
            Assert.Equal(17u, current.OriginAuthorEpoch);

            await provider.CommitPublishedOriginsAsync([
                new ProviderPublishedOrigin(change, new OperationOrigin(author, 17, 44))
            ]);
            current = await provider.ReadCurrentAsync(RecordId.From(recordBytes), 7);
            Assert.NotNull(current);
            Assert.Equal(44ul, current.OriginSequence);

            ProviderSnapshotHandle snapshot = await provider.OpenSnapshotAsync();
            Assert.Equal(1ul, snapshot.ChangeFeedWatermark);
            ProviderSnapshotPage snapshotPage = await provider.ReadSnapshotAsync(snapshot.Token, 0, 16);
            ProviderMutation snapshotEntry = Assert.Single(snapshotPage.Entries);
            Assert.True(snapshotPage.IsComplete);
            Assert.Equal(value, snapshotEntry.Value);
            Assert.Equal(BatchId.From(changeBytes), snapshotEntry.ChangeId);
            Assert.Equal(44ul, snapshotEntry.OriginSequence);

            await provider.AcknowledgeLocalChangesAsync(1);
            await provider.ReleaseSnapshotAsync(snapshot.Token);
            Assert.Equal(1ul, await provider.GetLocalChangeHighWatermarkAsync());

            byte[] clientBytes = new byte[16];
            clientId.TryWriteBytes(clientBytes, bigEndian: true, out _);
            Guid operationGuid = new(changeBytes, bigEndian: true);
            byte[] operationBytes = new byte[16];
            operationGuid.TryWriteBytes(operationBytes, bigEndian: true, out _);
            var localMutation = new FldLocalProviderMutation(
                recordBytes, 7, value, changeBytes, 17, 123456, 9, 3);
            byte[] request = FldDatabaseIpcLocalProviderProtocol.EncodeAutoSequenceRequest([localMutation]);
            byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
                FldDatabaseIpcOperationCodes.AppendLocalProviderBatchAutoSequence, request);
            Assert.True(database.TryGetLocalOperationReceipt(
                clientBytes, operationBytes, receiptSequence, fingerprint, out _));

            await provider.AcknowledgeOperationReceiptsAsync(receiptSequence);
            await provider.AcknowledgeOperationReceiptsAsync(receiptSequence);
            Assert.Throws<InvalidOperationException>(() =>
                database.TryGetLocalOperationReceipt(clientBytes, operationBytes, receiptSequence, fingerprint, out _));

        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_ipc_provider_roundtrips_record_tombstone_as_empty_provider_value()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "binsync-fld-async-provider-tombstone-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x534;
        byte[] authorBytes = Id(0x43);
        NodeId author = NodeId.From(authorBytes);
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();
        byte[] recordBytes = FldDatabase.NewRecordId();
        byte[] changeBytes = Id(0x74);

        using var database = FldDatabase.Open(root, namespaceId, authorBytes);
        using var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            await using var provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, clientId, TimeSpan.FromSeconds(10), author, () => 17);
            var tombstone = new ProviderMutation
            {
                RecordId = RecordId.From(recordBytes),
                FieldId = FldLifecycle.FieldId,
                IsRecordTombstone = true,
                HlcWall = 123457,
                HlcLogical = 1,
                AuthorNodeId = author,
                KeyEpoch = 0,
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(changeBytes),
                OriginAuthorEpoch = 17,
                OriginSequence = 0
            };

            await provider.AppendLocalChangeAsync(tombstone);

            ProviderChangePage changes = await provider.ReadLocalChangesAsync(0, 8);
            ProviderMutation local = Assert.Single(changes.Changes).Mutation;
            Assert.True(local.IsRecordTombstone);
            Assert.Empty(local.Value);

            ProviderMutation? current = await provider.ReadCurrentAsync(
                RecordId.From(recordBytes), FldLifecycle.FieldId);
            Assert.NotNull(current);
            Assert.True(current.IsRecordTombstone);
            Assert.Empty(current.Value);

            ProviderSnapshotHandle snapshot = await provider.OpenSnapshotAsync();
            ProviderSnapshotPage page = await provider.ReadSnapshotAsync(snapshot.Token, 0, 8);
            ProviderMutation snapshotEntry = Assert.Single(page.Entries);
            Assert.True(snapshotEntry.IsRecordTombstone);
            Assert.Empty(snapshotEntry.Value);

            await provider.AcknowledgeLocalChangesAsync(1);
            await provider.ReleaseSnapshotAsync(snapshot.Token);
            Assert.Equal(1ul, database.GetOutboxWatermark());
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_remote_batch_store_preserves_context_and_deduplicates_apply()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-async-remote-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x535;
        byte[] localAuthor = Id(0x51);
        byte[] sender = Id(0x52);
        byte[] issuer = Id(0x53);
        byte[] record = Id(0x54);
        byte[] change = Id(0x55);
        byte[] batchId = Id(0x56);
        byte[] batchDigest = Id(0x57);
        byte[] recordsRoot = Id(0x58);
        byte[] value = "async-contextual-apply"u8.ToArray();
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();

        using var database = FldDatabase.Open(root, namespaceId, localAuthor);
        using var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);
        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));
            await using var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10));
            var store = new FldDatabaseIpcRemoteBatchApplyStore(client, checked((int)namespaceId));
            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(record),
                FieldId = 0x0102,
                HlcWall = 123456,
                HlcLogical = 3,
                AuthorNodeId = NodeId.From(issuer),
                KeyEpoch = 2,
                Value = value,
                ChangeId = BatchId.From(change),
                OriginAuthorEpoch = 4,
                OriginSequence = 9
            };
            var batch = new ProviderRemoteBatchCommit(
                Guid.NewGuid(),
                NodeId.From(sender),
                senderTransportEpoch: 5,
                namespaceId: checked((int)namespaceId),
                NodeId.From(issuer),
                issuerEpoch: 4,
                BatchId.From(batchId),
                cursorStart: 1,
                cursorEnd: 1,
                new byte[16],
                batchDigest,
                recordsRoot,
                protocolStateDelta: new byte[] { 0xA1, 0xB2 },
                mutations: [mutation]);

            ProviderApplyReceipt first = await store.ApplyRemoteBatchAsync(batch);
            Assert.False(first.IsDuplicate);
            Assert.Equal(batchDigest, first.BatchFingerprint);
            Assert.True(database.TryGetBytes(record, 0x0102, out byte[]? stored));
            Assert.Equal(value, stored);
            Assert.Equal(0ul, database.GetOutboxHighWatermark());
            Assert.Equal(1ul, database.GetRelayJournalHighWatermark());

            ProviderApplyReceipt duplicate = await store.ApplyRemoteBatchAsync(batch);
            Assert.True(duplicate.IsDuplicate);
            Assert.Equal(first.BatchFingerprint, duplicate.BatchFingerprint);
            Assert.Equal(1ul, database.GetRelayJournalHighWatermark());
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
