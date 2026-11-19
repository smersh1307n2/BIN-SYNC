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
        NodeId rowAuthor = NodeId.From(Id(3));
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
                recordId: Id(0x700001),
                changeId: Id(0x710001),
                fieldId: 0x2222,
                author: rowAuthor,
                originSequence: 1,
                value: "obsolete-target-row"u8.ToArray());
            _ = targetProvider.ApplyRemoteBatch(Guid.NewGuid(), new[] { oldTargetRow });

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
                (wall, logical) => targetNode!.AdvanceClockToFloor(wall, logical));

            // First connection stages exactly one canonical 256-entry page. We then
            // close the receiver database as if its process died before COMMIT/ACK.
            var (firstToSource, firstToTarget) = PipeTransport.CreatePair();
            Task<SyncSession> firstTargetTask = SyncSession.AcceptAsync(
                targetNode, firstToTarget, FullReplaceCapabilities());
            Task<SyncSession> firstSourceTask = SyncSession.ConnectAsync(
                sourceNode, firstToSource, FullReplaceCapabilities());
            await Task.WhenAll(firstTargetTask, firstSourceTask);
            SyncSession firstSource = await firstSourceTask;
            SyncSession firstTarget = await firstTargetTask;
            Task<FullReplaceInstalledAck> firstSend = StorageFullReplaceSender.SendPreparedSnapshotAsync(
                prepared, firstSource, sourceGcStore, trust, roster, sourceId, sourceSeed);

            FullReplaceTransferProtocol.Begin firstManifest =
                await firstTarget.ReceiveFullReplaceBeginAsync(
                    NamespaceId, roster, trust, expectedFrameSequence: 1);
            StorageFullReplaceReceiver firstReceiver = CreateReceiver();
            ProviderFullReplaceProgress firstProgress = firstReceiver.Begin(firstManifest);
            Assert.Equal(0, firstProgress.NextIndex);
            await firstTarget.SendFullReplaceReadyAsync(firstReceiver.GetWireProgress(), frameSequence: 1);

            FullReplaceTransferProtocol.Chunk firstChunk =
                await firstTarget.ReceiveFullReplaceChunkAsync(NamespaceId, expectedFrameSequence: 2);
            Assert.Equal(256, firstChunk.Entries.Count);
            ProviderFullReplaceProgress staged = firstReceiver.Stage(firstChunk);
            Assert.Equal(256, staged.NextIndex);
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
                exception is IOException or ObjectDisposedException or OperationCanceledException or TimeoutException)
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
            // and the sender must resume at entry 256 without retransmitting page zero.
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
            await Task.WhenAll(secondTargetTask, secondSourceTask);
            await using SyncSession secondSource = await secondSourceTask;
            await using SyncSession secondTarget = await secondTargetTask;

            Task<FullReplaceInstalledAck?> receive = resumedReceiver.ReceiveTransferAsync(secondTarget);
            FullReplaceInstalledAck acknowledgement = await StorageFullReplaceSender.SendPreparedSnapshotAsync(
                prepared, secondSource, sourceGcStore, trust, roster, sourceId, sourceSeed);
            FullReplaceInstalledAck? receiverAck = await receive.WaitAsync(TimeSpan.FromSeconds(30));

            Assert.NotNull(receiverAck);
            Assert.True(acknowledgement.Installed);
            Assert.Equal(prepared.Generation, acknowledgement.Generation);
            Assert.Equal(257, prepared.ExpectedEntryCount);
            Assert.Equal(targetId, acknowledgement.MemberNodeId);
            Assert.Equal(8U, acknowledgement.MemberTransportEpoch);

            Assert.False(targetDatabase!.TryGetBytes(oldTargetRow.RecordId.ToArray(), oldTargetRow.FieldId, out _),
                "The previous target generation must be replaced, not merged.");
            for (int i = 0; i < 257; i++)
            {
                Assert.True(targetDatabase!.TryGetBytes(Id(0x100000 + i), 0x1111, out byte[]? value));
                Assert.Equal(Encoding.UTF8.GetBytes($"source-row-{i:D3}"), value);
            }

            Assert.True(targetDatabase!.TryResolve(Id(0x100100), 0x1111, out var metadata));
            Assert.Equal(Id(0x200100), metadata.ChangeId);
            Assert.Equal(1U, metadata.OriginAuthorEpoch);
            Assert.Equal(257UL, metadata.OriginSequence);

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
            // Populate source state as already-published remote-origin rows so the
            // immutable snapshot carries stable ChangeIds and non-zero origin cursors.
            FldDatabaseDataProvider provider = sourceProvider;
            var mutations = new ProviderMutation[257];
            for (int i = 0; i < mutations.Length; i++)
            {
                mutations[i] = Mutation(
                    Id(0x100000 + i),
                    Id(0x200000 + i),
                    0x1111,
                    rowAuthor,
                    checked((ulong)i + 1),
                    Encoding.UTF8.GetBytes($"source-row-{i:D3}"));
            }
            _ = provider.ApplyRemoteBatch(Guid.NewGuid(), mutations);
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
            HlcWall = 10_000 + originSequence,
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
