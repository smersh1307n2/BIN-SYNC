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
    [Fact]
    public async Task Async_coordinator_reclaims_only_signed_safe_tombstones_through_real_host_ipc()
    {
        string hostRoot = Path.Combine(Path.GetTempPath(), "fld-binsync-host-tombstone-gc-" + Guid.NewGuid().ToString("N"));
        string nodeRoot = Path.Combine(Path.GetTempPath(), "binsync-host-tombstone-gc-metadata-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x536;
        byte[] authorBytes = Id(0x65);
        NodeId author = NodeId.From(authorBytes);
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

            const string scope = "fld-host-tombstone-gc";
            using (var nodeStorage = StorageDatabase.Open(nodeRoot, 902))
            using (var metadata = new StorageMetadataStore(nodeStorage))
            using (var node = new NodeBuilder().WithNodeId(author).WithEpoch(8).Build())
            {
                var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
                NodeId authority = NodeId.From(Id(0x11));
                var trust = new TrustStore();
                trust.Allow(authority, keys[0].PublicKey, TrustMode.PreShared);
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
                var adapter = new FldBinSyncAsyncHostAdapter(coordinator, provider, remoteApply, _ => { });
                Assert.Equal(1, await adapter.RunTombstoneGcPassAsync(gc, trust, 16));
            }

            Assert.Null(await provider.ReadCurrentAsync(RecordId.From(recordId), 7));

            await remoteApply.DisposeAsync();
            remoteApply = null;
            await provider.DisposeAsync();
            provider = null;
            await StopDatabaseHostAsync(host);
            host = null;
            await WaitForDatabaseWriterReleaseAsync(
                hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));

            using var reopened = FldDatabase.Open(hostRoot, namespaceId, authorBytes);
            Assert.False(reopened.TryResolve(recordId, 7, out _));
        }
        finally
        {
            if (remoteApply is not null)
                await remoteApply.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            if (ownerId != Guid.Empty)
                StorageIpcEndpoint.DeleteIfOwned(hostRoot, namespaceId, ownerId);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
            try { Directory.Delete(nodeRoot, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Async_two_host_signed_gc_respects_local_metadata_relay_references()
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
        Guid ownerA = Guid.Empty;
        Guid ownerB = Guid.Empty;
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
                hostRootA, Guid.NewGuid(), TimeSpan.FromSeconds(15), authorA, () => 17);
            providerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                hostRootB, Guid.NewGuid(), TimeSpan.FromSeconds(15), authorB, () => 18);
            remoteA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            remoteB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                hostRootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            await providerA.EnableMetadataStoreOwnedPublicationAsync(0);
            await providerB.EnableMetadataStoreOwnedPublicationAsync(0);

            var coordinatorA = new AsyncStorageSyncCoordinator(nodeA, metadataA, scope, namespaceId);
            var coordinatorB = new AsyncStorageSyncCoordinator(nodeB, metadataB, scope, namespaceId);
            var adapterA = new FldBinSyncAsyncHostAdapter(
                coordinatorA, providerA, remoteA,
                current => StorageNodePersistence.Save(nodeStorageA, current));
            var adapterB = new FldBinSyncAsyncHostAdapter(
                coordinatorB, providerB, remoteB,
                current => StorageNodePersistence.Save(nodeStorageB, current));
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                await adapterA.RecoverBeforeAcceptingSessionsAsync());
            Assert.Equal(new AsyncStorageStartupRecoveryResult(0, 0),
                await adapterB.RecoverBeforeAcceptingSessionsAsync());

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
            Assert.Equal(1, (await receiveAtB).Applied);
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

            var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
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
                        TransportPeerEpoch = 11,
                        Status = NamespaceMemberStatus.Active
                    }
                ]
            };
            Guid fullReplaceGeneration = Guid.Parse("22345678-1234-5678-9abc-def012345678");
            byte[] fullReplaceDigest = SHA256.HashData("two-host signed tombstone gc snapshot"u8);
            void InstallSignedProof(StorageNamespaceGcStore gc)
            {
                NamespaceRosterCertificate acceptedRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
                byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(acceptedRoster);
                var reportA = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 1,
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
                                ContiguousAppliedOriginSequence = 1
                            }
                        ],
                        IssuedOrigins =
                        [
                            new IssuedOriginFrontierEntry
                            {
                                OriginAuthorEpoch = 17,
                                IssuedOriginSequence = 1,
                                MaxIssuedHlcWall = 100,
                                MaxIssuedHlcLogical = 1
                            }
                        ]
                    }, keys[0].Seed), acceptedRoster);
                var reportB = gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                    new AppliedOriginFrontierReport
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        MemberNodeId = authorB,
                        MemberTransportEpoch = 11,
                        ReportSequence = 1,
                        Entries =
                        [
                            new AppliedOriginFrontierEntry
                            {
                                OriginAuthorNodeId = authorA,
                                OriginAuthorEpoch = 17,
                                ContiguousAppliedOriginSequence = 1
                            }
                        ]
                    }, keys[1].Seed), acceptedRoster);

                gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                    new FullReplaceInstalledAck
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        Generation = fullReplaceGeneration,
                        Digest = fullReplaceDigest,
                        MemberNodeId = authorA,
                        MemberTransportEpoch = 10,
                        HlcWallFloor = 100,
                        HlcLogicalFloor = 1,
                        Installed = true
                    }, keys[0].Seed), acceptedRoster);
                gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                    new FullReplaceInstalledAck
                    {
                        NamespaceId = namespaceId,
                        RosterGeneration = 1,
                        RosterDigest = rosterDigest,
                        Generation = fullReplaceGeneration,
                        Digest = fullReplaceDigest,
                        MemberNodeId = authorB,
                        MemberTransportEpoch = 11,
                        HlcWallFloor = 100,
                        HlcLogicalFloor = 1,
                        Installed = true
                    }, keys[1].Seed), acceptedRoster);
                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    acceptedRoster, 1, new byte[32], [reportA, reportB],
                    fullReplaceGeneration, fullReplaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 1);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed),
                    acceptedRoster, trust);
            }

            var gcA = new StorageNamespaceGcStore(
                nodeStorageA, scope, authorA,
                new StorageNamespaceGcFeatureOptions(EnableTombstoneGc: true));
            var gcB = new StorageNamespaceGcStore(
                nodeStorageB, scope, authorB,
                new StorageNamespaceGcFeatureOptions(EnableTombstoneGc: true));
            InstallSignedProof(gcA);
            InstallSignedProof(gcB);

            Assert.True(gcA.HasProtocolTombstoneGcProof(
                gcA.TryReadNamespaceRoster(namespaceId, trust, out NamespaceRosterCertificate? acceptedA)
                    ? acceptedA! : throw new InvalidOperationException("Host A roster was not persisted."),
                trust, authorA, 17, 1, tombstoneAtA.HlcWall, tombstoneAtA.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host A must independently validate the signed causal and FULL_REPLACE proof.");
            Assert.True(gcB.HasProtocolTombstoneGcProof(
                gcB.TryReadNamespaceRoster(namespaceId, trust, out NamespaceRosterCertificate? acceptedB)
                    ? acceptedB! : throw new InvalidOperationException("Host B roster was not persisted."),
                trust, authorA, 17, 1, tombstoneAtB.HlcWall, tombstoneAtB.HlcLogical,
                fullReplaceGeneration, fullReplaceDigest),
                "Host B must independently validate the signed causal and FULL_REPLACE proof.");
            Assert.False(metadataA.HasLiveRecordReference(scope, authorA, namespaceId, record),
                "Host A outbound payload must be released after the durable peer ACK.");
            Assert.True(metadataB.HasLiveRecordReference(scope, authorB, namespaceId, record),
                "Host B relay payload must continue to pin the tombstone until a safe snapshot-aware relay baseline is committed.");

            Assert.Equal(1, await adapterA.RunTombstoneGcPassAsync(gcA, trust, 16));
            Assert.Equal(0, await adapterB.RunTombstoneGcPassAsync(gcB, trust, 16));
            Assert.Null(await providerA.ReadCurrentAsync(record, 0x0951));
            Assert.NotNull(await providerB.ReadCurrentAsync(record, 0x0951));

            await initiator.DisposeAsync(); initiator = null;
            await acceptor.DisposeAsync(); acceptor = null;
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
            Assert.False(reopenedA.TryResolve(record.ToArray(), 0x0951, out _));
            Assert.False(reopenedB.TryResolve(record.ToArray(), 0x0951, out _));
        }
        finally
        {
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
