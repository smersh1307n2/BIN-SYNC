using System.Text;
using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.FldHost.Tests;

/// <summary>
/// Exercises the real FLD FULL_REPLACE staging/commit path over BIN-SYNC frames.
/// The receiver is closed after its first durable page and reopened before resume.
/// </summary>
public sealed class FldFullReplaceBootstrapTests
{
    private const ushort NamespaceId = 0x638;
    private const string Scope = "fld-full-replace-bootstrap";
    private const int EntryCount = FullReplaceTransferProtocol.MaxChunkEntries + 1;

    private sealed class CallbackLease(Action release) : IDisposable
    {
        private Action? _release = release;
        public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
    }

    [Fact]
    public async Task Nonempty_namespace_bootstrap_resumes_staged_snapshot_after_receiver_reopen()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-full-replace-bootstrap-" + Guid.NewGuid().ToString("N"));
        string sourceFldRoot = Path.Combine(root, "source-fld");
        string targetFldRoot = Path.Combine(root, "target-fld");
        string sourceMetadataRoot = Path.Combine(root, "source-metadata");
        string targetMetadataRoot = Path.Combine(root, "target-metadata");
        Directory.CreateDirectory(root);

        NodeId sourceId = NodeId.From(Id(1));
        NodeId targetId = NodeId.From(Id(2));
        (byte[] sourceSeed, byte[] sourcePublicKey) = BatchSignature.GenerateKeyPair();
        (byte[] targetSeed, byte[] targetPublicKey) = BatchSignature.GenerateKeyPair();
        var trust = new TrustStore();
        trust.Allow(sourceId, sourcePublicKey, TrustMode.PreShared);
        trust.Allow(targetId, targetPublicKey, TrustMode.PreShared);

        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = NamespaceId,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = sourceId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = sourceId,
                    PublicKey = sourcePublicKey,
                    TransportPeerEpoch = 7,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = targetId,
                    PublicKey = targetPublicKey,
                    TransportPeerEpoch = 8,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate roster = NamespaceRosterProtocol.DecodeAndVerify(
            NamespaceRosterProtocol.Sign(unsignedRoster, sourceSeed), trust);

        using var sourceMetadataStorage = StorageDatabase.Open(sourceMetadataRoot, 1638);
        using var targetMetadataStorage = StorageDatabase.Open(targetMetadataRoot, 1639);
        using var sourceMetadata = new StorageMetadataStore(sourceMetadataStorage);
        using var targetMetadata = new StorageMetadataStore(targetMetadataStorage);
        var sourceGcStore = new StorageNamespaceGcStore(sourceMetadataStorage, Scope, sourceId);
        var targetGcStore = new StorageNamespaceGcStore(targetMetadataStorage, Scope, targetId);
        targetMetadata.AttachNamespaceGcGuard(targetGcStore, trust, Scope, targetId);
        _ = sourceGcStore.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, sourceSeed), trust);
        _ = targetGcStore.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, sourceSeed), trust);

        FldDatabase sourceDatabase = FldDatabase.Open(sourceFldRoot, NamespaceId, sourceId.ToArray());
        var sourceProvider = new FldDatabaseDataProvider(sourceDatabase, static () => 1);
        FldDatabase? targetDatabase = null;
        FldDatabaseDataProvider? targetProvider = null;
        Node? targetNode = null;
        Node sourceNode = new NodeBuilder().WithNodeId(sourceId).WithEpoch(7).Build();
        PreparedFullReplaceSnapshot prepared = CreateSourceSnapshot();

        try
        {
            targetDatabase = FldDatabase.Open(targetFldRoot, NamespaceId, targetId.ToArray());
            targetProvider = new FldDatabaseDataProvider(targetDatabase, static () => 1);

            // The target starts with a different, durable remote row. FULL_REPLACE
            // must remove it atomically rather than merely append the source snapshot.
            ProviderMutation oldTargetRow = Mutation(
                recordId: Id(1),
                changeId: Id(0x710001),
                fieldId: 0x2222,
                author: targetId,
                originSequence: 0,
                value: "obsolete-target-row"u8.ToArray());
            targetDatabase.PutWithIdentity(
                oldTargetRow.RecordId.ToArray(),
                oldTargetRow.FieldId,
                oldTargetRow.Value,
                oldTargetRow.ChangeId.ToArray(),
                oldTargetRow.OriginAuthorEpoch,
                oldTargetRow.HlcWall,
                oldTargetRow.HlcLogical,
                oldTargetRow.KeyEpoch,
                oldTargetRow.IsTombstone);
            _ = targetDatabase.Commit();
            ProviderChangePage targetFeed = targetProvider.ReadLocalChanges(0, 4096);
            Assert.Single(targetFeed.Changes);
            targetProvider.CommitPublishedOrigins(new[]
            {
                new ProviderPublishedOrigin(
                    targetFeed.Changes[0], new OperationOrigin(targetId, 1, 1))
            });
            targetProvider.AcknowledgeLocalChanges(targetFeed.ScannedThroughSequence);

            targetNode = new NodeBuilder().WithNodeId(targetId).WithEpoch(8).Build();
            var targetCoordinator = new StorageSyncCoordinator(
                targetNode!, targetProvider!, NamespaceId, targetMetadata, Scope);
            targetCoordinator.AttachNamespaceGcGuard(targetGcStore, trust);

            IApplicationGcReferenceScanner externalState = CreateEmptyExternalState();
            StorageFullReplaceReceiver CreateReceiver() => new(
                targetProvider!,
                targetGcStore,
                trust,
                NamespaceId,
                sourceId,
                7,
                targetId,
                8,
                targetSeed,
                Scope,
                externalState,
                () => targetCoordinator.AcquireFullReplaceCommitGuard(
                    static () => new CallbackLease(static () => { })),
                (wall, logical) => targetNode!.AdvanceClockToFloor(wall, logical),
                targetMetadata,
                baselineReportSequence: 1);

            // First connection stages exactly one maximum-size canonical page. We then
            // close the receiver database as if its process died before COMMIT/ACK.
            var (firstToSource, firstToTarget) = PipeTransport.CreatePair();
            Task<SyncSession> firstTargetTask = SyncSession.AcceptAsync(
                targetNode, firstToTarget, FullReplaceCapabilities());
            Task<SyncSession> firstSourceTask = SyncSession.ConnectAsync(
                sourceNode, firstToSource, FullReplaceCapabilities());
            await Task.WhenAll(firstTargetTask, firstSourceTask).WaitAsync(TimeSpan.FromSeconds(30));
            SyncSession firstSource = await firstSourceTask;
            SyncSession firstTarget = await firstTargetTask;
            Task<FullReplaceInstalledAck> firstSend = StorageFullReplaceSender.SendPreparedSnapshotAsync(
                prepared, firstSource, sourceGcStore, trust, roster, sourceId, sourceSeed);

            FullReplaceTransferProtocol.Begin firstManifest =
                await firstTarget.ReceiveFullReplaceBeginAsync(
                    NamespaceId, roster, trust, expectedFrameSequence: 1).WaitAsync(TimeSpan.FromSeconds(30));
            StorageFullReplaceReceiver firstReceiver = CreateReceiver();
            ProviderFullReplaceProgress firstProgress = firstReceiver.Begin(firstManifest);
            Assert.Equal(0, firstProgress.NextIndex);
            await firstTarget.SendFullReplaceReadyAsync(firstReceiver.GetWireProgress(), frameSequence: 1);

            FullReplaceTransferProtocol.Chunk firstChunk =
                await firstTarget.ReceiveFullReplaceChunkAsync(NamespaceId, expectedFrameSequence: 2).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(FullReplaceTransferProtocol.MaxChunkEntries, firstChunk.Entries.Count);
            ProviderFullReplaceProgress staged = firstReceiver.Stage(firstChunk);
            Assert.Equal(FullReplaceTransferProtocol.MaxChunkEntries, staged.NextIndex);
            Assert.False(staged.IsCommitted);

            await firstSource.DisposeAsync();
            await firstTarget.DisposeAsync();
            try
            {
                Assert.False(firstSend.IsCompletedSuccessfully,
                    "The sender must not receive an installation ACK before the receiver commits.");
                _ = await firstSend.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception exception) when (
                exception is IOException or ObjectDisposedException or OperationCanceledException)
            {
                // Expected: the first receiver generation was intentionally terminated
                // after its durable page and before the installation ACK.
            }

            targetProvider.Dispose();
            targetProvider = null;
            targetDatabase.Dispose();
            targetDatabase = null;
            targetNode.Dispose();
            targetNode = null;

            // Reopen the same FLD directory. BEGIN must recover the staging cursor,
            // and the sender must resume at entry 4096 without retransmitting page zero.
            targetDatabase = FldDatabase.Open(targetFldRoot, NamespaceId, targetId.ToArray());
            targetProvider = new FldDatabaseDataProvider(targetDatabase, static () => 1);
            targetNode = new NodeBuilder().WithNodeId(targetId).WithEpoch(8).Build();
            targetCoordinator = new StorageSyncCoordinator(
                targetNode, targetProvider, NamespaceId, targetMetadata, Scope);
            targetCoordinator.AttachNamespaceGcGuard(targetGcStore, trust);
            StorageFullReplaceReceiver resumedReceiver = CreateReceiver();

            var (secondToSource, secondToTarget) = PipeTransport.CreatePair();
            Task<SyncSession> secondTargetTask = SyncSession.AcceptAsync(
                targetNode!, secondToTarget, FullReplaceCapabilities());
            Task<SyncSession> secondSourceTask = SyncSession.ConnectAsync(
                sourceNode, secondToSource, FullReplaceCapabilities());
            await Task.WhenAll(secondTargetTask, secondSourceTask).WaitAsync(TimeSpan.FromSeconds(30));
            await using SyncSession secondSource = await secondSourceTask;
            await using SyncSession secondTarget = await secondTargetTask;

            Task<FullReplaceInstalledAck?> receive = resumedReceiver.ReceiveTransferAsync(secondTarget);
            Task<FullReplaceInstalledAck> send = StorageFullReplaceSender.SendPreparedSnapshotAsync(
                prepared, secondSource, sourceGcStore, trust, roster, sourceId, sourceSeed);
            Task firstCompleted = await Task.WhenAny(send, receive).WaitAsync(TimeSpan.FromSeconds(30));
            FullReplaceInstalledAck acknowledgement;
            FullReplaceInstalledAck? receiverAck;
            if (firstCompleted == receive)
            {
                receiverAck = await receive;
                acknowledgement = await send.WaitAsync(TimeSpan.FromSeconds(30));
            }
            else
            {
                acknowledgement = await send;
                receiverAck = await receive.WaitAsync(TimeSpan.FromSeconds(30));
            }

            Assert.NotNull(receiverAck);
            Assert.True(acknowledgement.Installed);
            Assert.True(targetMetadata.IsPeerInitialSyncComplete(
                Scope, targetId, sourceId, 7, NamespaceId));
            Assert.Equal(prepared.Generation, acknowledgement.Generation);
            Assert.Equal(EntryCount, prepared.ExpectedEntryCount);
            Assert.Equal(targetId, acknowledgement.MemberNodeId);
            Assert.Equal(8U, acknowledgement.MemberTransportEpoch);

            Assert.False(targetDatabase!.TryGetBytes(oldTargetRow.RecordId.ToArray(), oldTargetRow.FieldId, out _),
                "The previous target generation must be replaced, not merged.");
            for (int i = 0; i < EntryCount; i++)
            {
                Assert.True(targetDatabase!.TryGetBytes(Id(0x100000 + i), 0x1111, out byte[]? value));
                Assert.Equal(Encoding.UTF8.GetBytes($"source-row-{i:D3}"), value);
            }

            Assert.True(targetDatabase!.TryResolve(Id(0x100000 + EntryCount - 1), 0x1111, out var metadata));
            Assert.Equal(Id(0x200000 + EntryCount - 1), metadata.ChangeId);
            Assert.Equal(1U, metadata.OriginAuthorEpoch);
            Assert.Equal((ulong)EntryCount, metadata.OriginSequence);

            FullReplaceInstalledAck accepted = Assert.Single(
                sourceGcStore.ReadFullReplaceInstalledAcks(roster, prepared.Generation));
            Assert.Equal(targetId, accepted.MemberNodeId);
            Assert.True(accepted.Installed);
        }
        finally
        {
            targetProvider?.Dispose();
            targetDatabase?.Dispose();
            targetNode?.Dispose();
            prepared.Dispose();
            sourceProvider.Dispose();
            sourceDatabase.Dispose();
            sourceNode.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }

        PreparedFullReplaceSnapshot CreateSourceSnapshot()
        {
            // Publish the source's own local feed, stamp immutable Origin values,
            // then ACK that feed so the FULL_REPLACE snapshot contains no drafts.
            FldDatabaseDataProvider provider = sourceProvider;
            var mutations = new ProviderMutation[EntryCount];
            for (int i = 0; i < mutations.Length; i++)
            {
                mutations[i] = Mutation(
                    Id(0x100000 + i),
                    Id(0x200000 + i),
                    0x1111,
                    sourceId,
                    originSequence: 0,
                    Encoding.UTF8.GetBytes($"source-row-{i:D3}"));
                sourceDatabase.PutWithIdentity(
                    mutations[i].RecordId.ToArray(),
                    mutations[i].FieldId,
                    mutations[i].Value,
                    mutations[i].ChangeId.ToArray(),
                    mutations[i].OriginAuthorEpoch,
                    mutations[i].HlcWall,
                    mutations[i].HlcLogical,
                    mutations[i].KeyEpoch,
                    mutations[i].IsTombstone);
            }
            _ = sourceDatabase.Commit();

            var changes = new List<ProviderChange>(EntryCount);
            ulong cursor = 0;
            ulong highWatermark = provider.LocalChangeHighWatermark;
            while (cursor < highWatermark)
            {
                ProviderChangePage page = provider.ReadLocalChanges(cursor, 4096);
                changes.AddRange(page.Changes);
                if (page.ScannedThroughSequence <= cursor)
                    throw new InvalidDataException("FLD source feed did not advance while preparing the full-replace snapshot.");
                cursor = page.ScannedThroughSequence;
            }

            Assert.Equal(EntryCount, changes.Count);
            var published = new ProviderPublishedOrigin[changes.Count];
            for (int i = 0; i < published.Length; i++)
                published[i] = new ProviderPublishedOrigin(
                    changes[i], new OperationOrigin(sourceId, 1, checked((ulong)i + 1)));
            for (int offset = 0; offset < published.Length; offset += 4096)
                provider.CommitPublishedOrigins(
                    published.Skip(offset).Take(Math.Min(4096, published.Length - offset)).ToArray());
            provider.AcknowledgeLocalChanges(cursor);

            return StorageFullReplaceSender.PrepareSnapshot(
                provider, trust, roster, sourceId, Guid.NewGuid(), sourceSeed);
        }
    }

    private static IApplicationGcReferenceScanner CreateEmptyExternalState()
        => new DelegateApplicationGcReferenceScanner(static (_, _, _) =>
            new DelegateApplicationGcReferenceScan(
                new CallbackLease(static () => { }),
                default,
                static (_, _, _, _) => false,
                static _ => false,
                static (_, _) => false));

    private static SessionCapabilities FullReplaceCapabilities()
        => SessionCapabilities.FullAuthorId |
           SessionCapabilities.FlowControl |
           SessionCapabilities.NamespaceRosterV31 |
           SessionCapabilities.OriginFrontierV31 |
           SessionCapabilities.FullReplaceV31;

    private static ProviderMutation Mutation(
        byte[] recordId,
        byte[] changeId,
        ushort fieldId,
        NodeId author,
        ulong originSequence,
        byte[] value)
        => new()
        {
            RecordId = RecordId.From(recordId),
            FieldId = fieldId,
            AuthorNodeId = author,
            ChangeId = BatchId.From(changeId),
            OriginAuthorEpoch = 1,
            OriginSequence = originSequence,
            HlcWall = checked(10_000UL + (ulong)System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(recordId.AsSpan(12))),
            HlcLogical = 0,
            KeyEpoch = 0,
            Value = value
        };

    private static byte[] Id(int value)
    {
        byte[] bytes = new byte[16];
        bytes[0] = 0xA1;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(12), value);
        return bytes;
    }
}
