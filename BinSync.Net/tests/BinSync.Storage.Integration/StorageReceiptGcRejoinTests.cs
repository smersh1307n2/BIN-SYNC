using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageReceiptGcRejoinTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate MakeRoster(
        ulong generation, byte[] previousDigest,
        (byte[] Seed, byte[] PublicKey)[] keys,
        NamespaceMemberStatus senderStatus, uint senderEpoch, uint retiredThrough,
        NamespaceRetiredOriginEpoch[] retiredOrigins)
        => new()
        {
            NamespaceId = 19,
            RosterGeneration = generation,
            PreviousRosterDigest = previousDigest,
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = NodeId.From(Id(1)),
                    PublicKey = keys[0].PublicKey,
                    TransportPeerEpoch = 10,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = NodeId.From(Id(2)),
                    PublicKey = keys[1].PublicKey,
                    TransportPeerEpoch = senderEpoch,
                    Status = senderStatus,
                    RetiredThroughEpoch = retiredThrough
                }
            },
            RetiredOriginEpochs = retiredOrigins
        };

    private static AppliedOriginFrontierReport Report(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys,
        ulong reportSequence, ulong appliedSequence, bool finalForAuthor = false)
        => new()
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            ReportSequence = reportSequence,
            FinalForAuthor = finalForAuthor,
            Entries = new[]
            {
                new AppliedOriginFrontierEntry
                {
                    OriginAuthorNodeId = NodeId.From(Id(2)),
                    OriginAuthorEpoch = 3,
                    ContiguousAppliedOriginSequence = appliedSequence
                }
            },
            IssuedOrigins = memberIndex == 1
                ? new[]
                {
                    new IssuedOriginFrontierEntry
                    {
                        OriginAuthorEpoch = 3,
                        IssuedOriginSequence = appliedSequence,
                        MaxIssuedHlcWall = 100,
                        MaxIssuedHlcLogical = 2
                    }
                }
                : Array.Empty<IssuedOriginFrontierEntry>()
        };

    private static byte[] SignedReport(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys,
        ulong reportSequence, ulong appliedSequence, bool finalForAuthor = false)
        => OriginFrontierProtocol.Sign(
            Report(roster, memberIndex, keys, reportSequence, appliedSequence, finalForAuthor),
            keys[memberIndex].Seed);

    private static byte[] SignedAck(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, Guid generation, byte[] digest)
        => FullReplaceInstalledAckProtocol.Sign(new FullReplaceInstalledAck
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            Generation = generation,
            Digest = digest,
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            HlcWallFloor = 100,
            HlcLogicalFloor = 2,
            Installed = true
        }, keys[memberIndex].Seed);

    private static MetadataIncomingBatch Batch(
        string scope, NodeId local, NodeId sender, uint transportEpoch,
        byte cursor, byte previousDigest, byte digest, byte batchId, ulong originSequence)
        => new()
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = 3,
            TransportPeerEpoch = transportEpoch,
            AuthorStreamNodeId = sender,
            NamespaceId = 19,
            BatchId = BatchId.From(Id(batchId)),
            CursorStart = cursor,
            CursorEnd = cursor,
            PreviousBatchDigest = cursor == 1
                ? new byte[16]
                : Enumerable.Repeat(previousDigest, 16).ToArray(),
            BatchDigest = Enumerable.Repeat(digest, 16).ToArray(),
            HeaderFingerprint = Enumerable.Repeat((byte)(digest + 1), 16).ToArray(),
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = sender,
                    ChangeId = BatchId.From(Id((byte)(batchId + 1))),
                    OriginAuthorEpoch = 3,
                    OriginSequence = originSequence,
                    Fingerprint = Enumerable.Repeat((byte)(digest + 2), 16).ToArray(),
                    Payload = new[] { (byte)(digest + 3) }
                }
            }
        };

    private static IApplicationGcReferenceScanner Scanner(
        bool hasReceiptReference, bool hasChangeIdReference = false,
        IReadOnlySet<BatchId>? changeIds = null)
        => new DelegateApplicationGcReferenceScanner((_, _, _) =>
            new DelegateApplicationGcReferenceScan(
                new MemoryStream(),
                new ApplicationDraftHorizon(0, 0, 0),
                (_, _, _, _) => hasReceiptReference,
                id => hasChangeIdReference && (changeIds is null || changeIds.Contains(id)),
                (_, _) => false));

    [Fact]
    public void Receipt_gc_retires_old_epoch_rejoins_new_epoch_and_keeps_replay_fence_after_restart()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-receipt-rejoin-gc-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        const string scope = "receipt-rejoin-gc";
        const int partitionId = 914;
        NodeId local = NodeId.From(Id(0x7F));
        NodeId sender = NodeId.From(Id(2));
        Guid fullReplaceGeneration = Guid.Parse("33445566-7788-499a-bbcc-ddeeff001122");
        byte[] fullReplaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("receipt rejoin FULL_REPLACE"));
        var retiredOrigin = new[]
        {
            new NamespaceRetiredOriginEpoch
            {
                OriginAuthorNodeId = sender,
                OriginAuthorEpoch = 3,
                FinalIssuedOriginSequence = 90,
                MaxIssuedHlcWall = 100,
                MaxIssuedHlcLogical = 2
            }
        };
        var oldBatches = new List<MetadataIncomingBatch>();
        MetadataIncomingBatch? replacementBatch = null;
        var changeIds = new HashSet<BatchId>
        {
            BatchId.From(Id(0x51)), BatchId.From(Id(0x52)),
            BatchId.From(Id(0x53)), BatchId.From(Id(0x59))
        };

        try
        {
            using (var storage = StorageDatabase.Open(directory, partitionId))
            using (var metadata = new StorageMetadataStore(storage))
            using (var provider = new StorageDataProvider(storage))
            {
                var gc = new StorageNamespaceGcStore(
                    storage, scope, local,
                    new StorageNamespaceGcFeatureOptions(
                        EnableReceiptGc: true, EnableChangeIdGc: true));
                NamespaceRosterCertificate roster1 = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(
                        MakeRoster(1, new byte[32], keys,
                            NamespaceMemberStatus.Active, 11, 0, Array.Empty<NamespaceRetiredOriginEpoch>()),
                        keys[0].Seed),
                    trust);
                metadata.AttachNamespaceGcGuard(gc, trust, scope, local);

                metadata.RegisterInterestedPeer(scope, local, sender, 11, 19);
                byte[] priorDigest = new byte[16];
                for (byte index = 0; index < 3; index++)
                {
                    byte cursor = (byte)(index + 1);
                    var batch = Batch(
                        scope, local, sender, 11, cursor,
                        priorDigest[0], (byte)(0x41 + index), (byte)(0x50 + index),
                        (ulong)(50 + index));
                    Assert.False(metadata.AcceptIncomingBatch(batch).IsDuplicate);
                    oldBatches.Add(batch);
                    priorDigest = batch.BatchDigest;
                }

                // The replacement transport incarnation is established before the
                // old one is retired. The receive chain remains continuous.
                metadata.RegisterInterestedPeer(scope, local, sender, 12, 19);
                metadata.CompletePeerInitialSync(
                    scope, local, sender, 12, 19,
                    new[] { new MetadataPeerStreamBaseline(local, 0, 3) });
                metadata.RetireInterestedPeer(scope, local, sender, 11, 12, 19);

                replacementBatch = Batch(
                    scope, local, sender, 12, 4, priorDigest[0], 0x47, 0x58, 53);
                Assert.False(metadata.AcceptIncomingBatch(replacementBatch).IsDuplicate);

                // A downstream immutable batch is pushed but its ACK is lost just
                // before process shutdown. The exact reservation must survive reopen.
                NodeId downstream = NodeId.From(Id(3));
                metadata.RegisterInterestedPeer(scope, local, downstream, 1, 19);
                metadata.CompletePeerInitialSync(
                    scope, local, downstream, 1, 19,
                    new[] { new MetadataPeerStreamBaseline(sender, 0, 3) });
                var lostAckReservation = new MetadataOutboundBatchReservation
                {
                    BatchId = BatchId.From(Id(0x70)),
                    IssuerEpoch = 3,
                    CursorStart = 1,
                    CursorEnd = 4,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)0x71, 16).ToArray(),
                    Payload = new byte[] { 0x72 }
                };
                metadata.ReserveOutboundBatch(
                    scope, local, downstream, 1, 19, sender, lostAckReservation);
                metadata.MarkOutboundBatchPushed(
                    scope, local, downstream, 1, 19, sender, lostAckReservation.BatchId);
                Assert.Equal(0, metadata.CompactChangeLedger(
                    scope, local, 19, provider, 1,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));

                // The retiring author signs a final issued-origin report before
                // the authority closes that origin epoch in the signed roster.
                gc.AcceptOriginFrontier(
                    SignedReport(roster1, 0, keys, 1, 90), roster1);
                gc.AcceptOriginFrontier(
                    SignedReport(roster1, 1, keys, 1, 90, finalForAuthor: true), roster1);

                NamespaceRosterCertificate retiredRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(
                        MakeRoster(2, NamespaceRosterProtocol.ComputeDigest(roster1), keys,
                            NamespaceMemberStatus.Retired, 11, 11, retiredOrigin),
                        keys[0].Seed),
                    trust);
                NamespaceRosterCertificate joiningRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(
                        MakeRoster(4, NamespaceRosterProtocol.ComputeDigest(retiredRoster), keys,
                            NamespaceMemberStatus.Joining, 12, 0, retiredOrigin),
                        keys[0].Seed),
                    trust);
                NamespaceRosterCertificate rejoinedRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(
                        MakeRoster(6, NamespaceRosterProtocol.ComputeDigest(joiningRoster), keys,
                            NamespaceMemberStatus.Active, 12, 0, retiredOrigin),
                        keys[0].Seed),
                    trust);

                // No frontier, FULL_REPLACE ACK or checkpoint is inherited by the
                // new roster generation. Re-establish the complete current proof.
                Assert.Empty(gc.ReadOriginFrontiers(rejoinedRoster));
                Assert.False(gc.TryReadRetentionCheckpoint(rejoinedRoster, trust, out _));
                Assert.Empty(gc.ReadFullReplaceInstalledAcks(rejoinedRoster, fullReplaceGeneration));

                var report0 = gc.AcceptOriginFrontier(
                    SignedReport(rejoinedRoster, 0, keys, 1, 90), rejoinedRoster);
                var report1 = gc.AcceptOriginFrontier(
                    SignedReport(rejoinedRoster, 1, keys, 1, 90, finalForAuthor: true), rejoinedRoster);
                gc.AcceptFullReplaceInstalledAck(
                    SignedAck(rejoinedRoster, 0, keys, fullReplaceGeneration, fullReplaceDigest),
                    rejoinedRoster);
                gc.AcceptFullReplaceInstalledAck(
                    SignedAck(rejoinedRoster, 1, keys, fullReplaceGeneration, fullReplaceDigest),
                    rejoinedRoster);
                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    rejoinedRoster, 1, new byte[32], new[] { report0, report1 },
                    fullReplaceGeneration, fullReplaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed),
                    rejoinedRoster, trust);

                // An application-owned receipt reference vetoes deletion even after
                // the signed current-epoch proof is complete.
                Assert.Equal(0, metadata.CompactReceipts(
                    scope, local, 19, 16, () => new MemoryStream(), Scanner(true)));
                Assert.Equal(0, metadata.CompactChangeLedger(
                    scope, local, 19, provider, 16,
                    () => new MemoryStream(), Scanner(false, true, changeIds)));

                // Commit one bounded page, then reopen the complete durable scope.
                Assert.Equal(1, metadata.CompactReceipts(
                    scope, local, 19, 1, () => new MemoryStream(), Scanner(false)));
            }

            // First reopen: recover the exact pushed reservation, finish its ACK,
            // release the relay payload, then commit one ChangeId-GC page.
            {
                using var reopenedStorage = StorageDatabase.Open(directory, partitionId);
                using var reopenedMetadata = new StorageMetadataStore(reopenedStorage);
                using var reopenedProvider = new StorageDataProvider(reopenedStorage);
                var reopenedGc = new StorageNamespaceGcStore(
                    reopenedStorage, scope, local,
                    new StorageNamespaceGcFeatureOptions(
                        EnableReceiptGc: true, EnableChangeIdGc: true));
                reopenedMetadata.AttachNamespaceGcGuard(reopenedGc, trust, scope, local);

                Assert.True(reopenedGc.TryReadNamespaceRoster(19, trust, out NamespaceRosterCertificate? persistedRoster));
                Assert.NotNull(persistedRoster);
                Assert.Equal(6UL, persistedRoster!.RosterGeneration);
                Assert.True(reopenedGc.TryReadRetentionCheckpoint(persistedRoster, trust, out _));

                Assert.Equal(1, reopenedMetadata.CompactReceipts(
                    scope, local, 19, 1, () => new MemoryStream(), Scanner(false)));
                Assert.Equal(1, reopenedMetadata.CompactReceipts(
                    scope, local, 19, 1, () => new MemoryStream(), Scanner(false)));
                Assert.Equal(0, reopenedMetadata.CompactReceipts(
                    scope, local, 19, 1, () => new MemoryStream(), Scanner(false)));

                MetadataPeerStreamState recovered = reopenedMetadata.GetPeerStreamState(
                    scope, local, NodeId.From(Id(3)), 1, 19, sender);
                Assert.Equal(0UL, recovered.AckedCursor);
                Assert.Equal(4UL, recovered.PushedCursor);
                MetadataOutboundBatchReservation recoveredReservation =
                    Assert.IsType<MetadataOutboundBatchReservation>(recovered.PendingBatch);
                Assert.Equal(BatchId.From(Id(0x70)), recoveredReservation.BatchId);
                Assert.Equal(new byte[] { 0x72 }, recoveredReservation.Payload);

                // Retrying the exact push is idempotent; only the matching ACK can
                // clear the durable reservation and release its immutable payload.
                reopenedMetadata.MarkOutboundBatchPushed(
                    scope, local, NodeId.From(Id(3)), 1, 19, sender, recoveredReservation.BatchId);
                reopenedMetadata.AcknowledgePeerBatch(
                    scope, local, NodeId.From(Id(3)), 1, 19, sender,
                    recoveredReservation.BatchId, 4, recoveredReservation.BatchDigest);
                reopenedMetadata.AcknowledgeRelay(scope, local, 19, sender, 4);
                Assert.Equal(4, reopenedMetadata.CompactRelayThroughBounded(
                    scope, local, 19, sender, 4, maxEntries: 16));

                Assert.Equal(0, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 16,
                    () => new MemoryStream(), Scanner(false, true, changeIds)));
                ProviderSnapshotHandle snapshot = reopenedProvider.OpenSnapshot();
                Assert.Equal(0, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 16,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));
                reopenedProvider.ReleaseSnapshot(snapshot.Token);

                int CountLiveChangeIds() => changeIds.Count(id =>
                    reopenedMetadata.TryGetChangeIdentity(scope, local, 19, id, out _, out _, out _));
                Assert.Equal(4, CountLiveChangeIds());
                Assert.Equal(1, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 1,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));
                Assert.Equal(3, CountLiveChangeIds());
            }

            // Second reopen resumes the ChangeId ledger page sequence. The signed
            // checkpoint and receive cursor survive independently of deleted rows.
            {
                using var reopenedStorage = StorageDatabase.Open(directory, partitionId);
                using var reopenedMetadata = new StorageMetadataStore(reopenedStorage);
                using var reopenedProvider = new StorageDataProvider(reopenedStorage);
                var reopenedGc = new StorageNamespaceGcStore(
                    reopenedStorage, scope, local,
                    new StorageNamespaceGcFeatureOptions(
                        EnableReceiptGc: true, EnableChangeIdGc: true));
                reopenedMetadata.AttachNamespaceGcGuard(reopenedGc, trust, scope, local);

                Assert.True(reopenedGc.TryReadNamespaceRoster(19, trust, out NamespaceRosterCertificate? persistedRoster));
                Assert.NotNull(persistedRoster);
                Assert.Equal(6UL, persistedRoster!.RosterGeneration);
                Assert.True(reopenedGc.TryReadRetentionCheckpoint(persistedRoster, trust, out _));
                Assert.Equal(3, changeIds.Count(id =>
                    reopenedMetadata.TryGetChangeIdentity(scope, local, 19, id, out _, out _, out _)));

                Assert.Equal(1, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 1,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));
                Assert.Equal(1, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 1,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));
                Assert.Equal(1, reopenedMetadata.CompactChangeLedger(
                    scope, local, 19, reopenedProvider, 1,
                    () => new MemoryStream(), Scanner(false, false, changeIds)));
                Assert.Equal(0, changeIds.Count(id =>
                    reopenedMetadata.TryGetChangeIdentity(scope, local, 19, id, out _, out _, out _)));

                // Receipt rows for the retired epoch are gone; the receive cursor
                // and active-epoch receipt still reject duplicates independently.
                foreach (MetadataIncomingBatch oldBatch in oldBatches)
                    Assert.Throws<InvalidDataException>(() =>
                        reopenedMetadata.AcceptIncomingBatch(oldBatch));
                Assert.NotNull(replacementBatch);
                Assert.True(reopenedMetadata.AcceptIncomingBatch(replacementBatch!).IsDuplicate);
                Assert.Equal(0, reopenedMetadata.CompactReceipts(
                    scope, local, 19, 1, () => new MemoryStream(), Scanner(false)));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
