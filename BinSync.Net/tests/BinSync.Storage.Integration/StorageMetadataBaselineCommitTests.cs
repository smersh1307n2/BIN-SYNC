using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageMetadataBaselineCommitTests
{
    private const string Scope = "fld-db/mbr1-atomic-apply";
    private const int NamespaceId = 19;

    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NodeId Node(byte value) => NodeId.From(Id(value));
    private static BatchId Batch(byte value) => BatchId.From(Id(value));
    private static byte[] Digest(byte value) => Enumerable.Repeat(value, 16).ToArray();

    private sealed record Fixture(
        StorageMetadataStore Metadata,
        StorageNamespaceGcStore Gc,
        TrustStore Trust,
        NamespaceRosterCertificate Roster,
        (byte[] Seed, byte[] PublicKey)[] Keys,
        Guid Generation,
        byte[] SnapshotDigest,
        byte[] LocalReport,
        byte[] PeerReport,
        NodeId Local,
        NodeId Peer,
        NodeId RelayAuthor,
        byte[] OutboundDigest,
        byte[] RelayDigest);

    private static Fixture CreateFixture(StorageDatabase storage)
    {
        NodeId local = Node(1), peer = Node(2), relayAuthor = Node(3);
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(local, keys[0].PublicKey, TrustMode.PreShared);
        var proposedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = NamespaceId,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[NamespaceRosterProtocol.DigestSize],
            AuthorityNodeId = local,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local, PublicKey = keys[0].PublicKey,
                    TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = peer, PublicKey = keys[1].PublicKey,
                    TransportPeerEpoch = 11, Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gc = new StorageNamespaceGcStore(storage, Scope, local);
        NamespaceRosterCertificate roster = gc.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(proposedRoster, keys[0].Seed), trust);
        var metadata = new StorageMetadataStore(storage);
        metadata.AttachNamespaceGcGuard(gc, trust, Scope, local);

        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] snapshotDigest = SHA256.HashData(Encoding.UTF8.GetBytes("mbr1 atomic snapshot"));
        for (int i = 0; i < keys.Length; i++)
        {
            gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                new FullReplaceInstalledAck
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = roster.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    Generation = generation,
                    Digest = snapshotDigest,
                    MemberNodeId = roster.Members[i].NodeId,
                    MemberTransportEpoch = roster.Members[i].TransportPeerEpoch,
                    HlcWallFloor = 100,
                    HlcLogicalFloor = 2,
                    Installed = true
                }, keys[i].Seed), roster);
        }
        gc.AcceptRetentionCheckpoint(NamespaceRetentionCheckpointProtocol.Sign(
            new NamespaceRetentionCheckpoint
            {
                NamespaceId = NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                CheckpointSequence = 1,
                PreviousCheckpointDigest = new byte[NamespaceRosterProtocol.DigestSize],
                SafeFrontier = Array.Empty<RetentionFrontierEntry>(),
                FullReplaceGeneration = generation,
                FullReplaceDigest = snapshotDigest,
                FullReplaceHlcWallFloor = 100,
                FullReplaceHlcLogicalFloor = 2
            }, keys[0].Seed), roster, trust);

        metadata.RegisterInterestedPeer(Scope, local, peer, 11, NamespaceId);
        metadata.FreezeOutbound(Scope, local, NamespaceId, local, new[]
        {
            new MetadataFrozenOperation
            {
                ChangeId = Batch(20), Fingerprint = Digest(20), Payload = new byte[] { 0x20 },
                OriginAuthorEpoch = 7
            },
            new MetadataFrozenOperation
            {
                ChangeId = Batch(21), Fingerprint = Digest(21), Payload = new byte[] { 0x21 },
                OriginAuthorEpoch = 7
            }
        });

        byte[] relayDigest = Digest(42);
        metadata.AcceptIncomingBatch(new MetadataIncomingBatch
        {
            ScopeId = Scope,
            LocalReplica = local,
            SenderPeer = peer,
            SenderEpoch = 5,
            TransportPeerEpoch = 11,
            AuthorStreamNodeId = relayAuthor,
            NamespaceId = NamespaceId,
            BatchId = Batch(40),
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = relayDigest,
            Operations = new[]
            {
                new MetadataOperation
                {
                    AuthorNodeId = relayAuthor,
                    ChangeId = Batch(41),
                    OriginAuthorEpoch = 5,
                    OriginSequence = 1,
                    Fingerprint = Digest(41),
                    Payload = new byte[] { 0x41 }
                }
            }
        });

        byte[] outboundDigest = Digest(31);
        var localReport = new MetadataBaselineReport
        {
            NamespaceId = NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            FullReplaceGeneration = generation,
            SnapshotDigest = snapshotDigest,
            SnapshotEntryCount = 4,
            ProviderChangeFeedWatermark = 40,
            HlcWallFloor = 100,
            HlcLogicalFloor = 2,
            MemberNodeId = local,
            MemberTransportEpoch = 10,
            ReportSequence = 1,
            FeedEntries = new[]
            {
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = peer, TargetTransportEpoch = 11,
                    AuthorStream = local, FeedKind = MetadataBaselineFeedKind.Outbound,
                    IssuerEpoch = 7, AbsoluteFeedOffset = 1, DigestAnchor = outboundDigest
                },
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = peer, TargetTransportEpoch = 11,
                    AuthorStream = relayAuthor, FeedKind = MetadataBaselineFeedKind.Relay,
                    IssuerEpoch = 5, AbsoluteFeedOffset = 0, DigestAnchor = new byte[16]
                }
            },
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = peer, SenderTransportEpoch = 11,
                    AuthorStream = relayAuthor, IssuerEpoch = 5,
                    ReceiveCursor = 1, LastBatchDigest = relayDigest
                }
            }
        };
        var peerReport = new MetadataBaselineReport
        {
            NamespaceId = NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            FullReplaceGeneration = generation,
            SnapshotDigest = snapshotDigest,
            SnapshotEntryCount = 4,
            ProviderChangeFeedWatermark = 40,
            HlcWallFloor = 100,
            HlcLogicalFloor = 2,
            MemberNodeId = peer,
            MemberTransportEpoch = 11,
            ReportSequence = 1,
            FeedEntries = new[]
            {
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = local, TargetTransportEpoch = 10,
                    AuthorStream = relayAuthor, FeedKind = MetadataBaselineFeedKind.Relay,
                    IssuerEpoch = 5, AbsoluteFeedOffset = 0, DigestAnchor = relayDigest
                }
            },
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = local, SenderTransportEpoch = 10,
                    AuthorStream = local, IssuerEpoch = 7,
                    ReceiveCursor = 1, LastBatchDigest = outboundDigest
                },
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = local, SenderTransportEpoch = 10,
                    AuthorStream = relayAuthor, IssuerEpoch = 5,
                    ReceiveCursor = 0, LastBatchDigest = new byte[16]
                }
            }
        };
        byte[] signedLocal = MetadataBaselineReportProtocol.Sign(localReport, keys[0].Seed);
        byte[] signedPeer = MetadataBaselineReportProtocol.Sign(peerReport, keys[1].Seed);
        gc.AcceptMetadataBaselineReport(signedLocal, roster, generation, snapshotDigest);
        gc.AcceptMetadataBaselineReport(signedPeer, roster, generation, snapshotDigest);

        return new Fixture(metadata, gc, trust, roster, keys, generation, snapshotDigest,
            signedLocal, signedPeer, local, peer, relayAuthor, outboundDigest, relayDigest);
    }

    [Fact]
    public void Full_replace_baseline_commits_both_directions_atomically_and_survives_reopen()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-apply-" + Guid.NewGuid().ToString("N"));
        try
        {
            Guid generation;
            byte[] snapshotDigest;
            byte[] localReport;
            byte[] peerReport;
            byte[] outboundDigest;
            byte[] relayDigest;
            NodeId local, peer, relayAuthor;

            using (var storage = StorageDatabase.Open(directory, 912))
            {
                Fixture fixture = CreateFixture(storage);
                using StorageMetadataStore metadata = fixture.Metadata;
                generation = fixture.Generation;
                snapshotDigest = fixture.SnapshotDigest;
                localReport = fixture.LocalReport;
                peerReport = fixture.PeerReport;
                outboundDigest = fixture.OutboundDigest;
                relayDigest = fixture.RelayDigest;
                local = fixture.Local;
                peer = fixture.Peer;
                relayAuthor = fixture.RelayAuthor;

                ulong before = storage.LastCommitSeq;
                Assert.True(metadata.ApplyFullReplaceBaseline(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                Assert.Equal(before + 1, storage.LastCommitSeq);
                Assert.True(metadata.IsPeerInitialSyncComplete(Scope, local, peer, 11, NamespaceId));

                MetadataPeerStreamState outbound = metadata.GetPeerStreamState(
                    Scope, local, peer, 11, NamespaceId, local);
                Assert.Equal(1UL, outbound.AckedCursor);
                Assert.Equal(1UL, outbound.PushedCursor);
                Assert.Equal(1UL, outbound.StreamOffset);
                Assert.Equal(7U, outbound.IssuerEpoch);
                Assert.Equal(outboundDigest, outbound.LastBatchDigest);

                MetadataPeerStreamState relay = metadata.GetPeerStreamState(
                    Scope, local, peer, 11, NamespaceId, relayAuthor);
                Assert.Equal(0UL, relay.AckedCursor);
                Assert.Equal(0UL, relay.StreamOffset);
                Assert.Equal(5U, relay.IssuerEpoch);
                Assert.Equal(new byte[16], relay.LastBatchDigest);
                Assert.Equal(1UL, metadata.GetReceiveCursor(
                    Scope, local, peer, 5, NamespaceId, relayAuthor));

                ulong committed = storage.LastCommitSeq;
                Assert.False(metadata.ApplyFullReplaceBaseline(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                Assert.Equal(committed, storage.LastCommitSeq);

                metadata.AcceptIncomingBatch(new MetadataIncomingBatch
                {
                    ScopeId = Scope,
                    LocalReplica = local,
                    SenderPeer = peer,
                    SenderEpoch = 5,
                    TransportPeerEpoch = 11,
                    AuthorStreamNodeId = relayAuthor,
                    NamespaceId = NamespaceId,
                    BatchId = Batch(42),
                    CursorStart = 2,
                    CursorEnd = 2,
                    PreviousBatchDigest = relayDigest,
                    BatchDigest = Digest(43),
                    Operations = Array.Empty<MetadataOperation>()
                });
                Assert.Equal(2UL, metadata.GetReceiveCursor(
                    Scope, local, peer, 5, NamespaceId, relayAuthor));
                Assert.False(metadata.ApplyFullReplaceBaseline(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                Assert.Equal(2UL, metadata.GetReceiveCursor(
                    Scope, local, peer, 5, NamespaceId, relayAuthor));
                _ = localReport;
                _ = peerReport;
            }

            using (var storage = StorageDatabase.Open(directory, 912))
            {
                var gc = new StorageNamespaceGcStore(storage, Scope, local);
                var trust = new TrustStore();
                trust.Allow(local, BatchSignature.GenerateKeyPair().PublicKey, TrustMode.PreShared);
                // The persisted marker and stream rows are validated during Metadata partition reopen.
                using var metadata = new StorageMetadataStore(storage);
                Assert.True(metadata.IsPeerInitialSyncComplete(Scope, local, peer, 11, NamespaceId));
                Assert.Equal(1UL, metadata.GetPeerStreamState(
                    Scope, local, peer, 11, NamespaceId, local).StreamOffset);
                Assert.Equal(2UL, metadata.GetReceiveCursor(
                    Scope, local, peer, 5, NamespaceId, relayAuthor));
                _ = gc;
                _ = trust;
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_baseline_is_blocked_by_pending_apply_without_partial_metadata_commit()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-pending-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 913);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            metadata.BeginPendingApply(new MetadataIncomingBatch
            {
                ScopeId = Scope,
                LocalReplica = fixture.Local,
                SenderPeer = fixture.Peer,
                SenderEpoch = 5,
                TransportPeerEpoch = 11,
                AuthorStreamNodeId = fixture.RelayAuthor,
                NamespaceId = NamespaceId,
                BatchId = Batch(44),
                CursorStart = 2,
                CursorEnd = 2,
                PreviousBatchDigest = fixture.RelayDigest,
                BatchDigest = Digest(45),
                Operations = Array.Empty<MetadataOperation>()
            }, Guid.Parse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"));

            ulong before = storage.LastCommitSeq;
            Assert.Throws<InvalidOperationException>(() => metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            Assert.Equal(before, storage.LastCommitSeq);
            Assert.False(metadata.IsPeerInitialSyncComplete(Scope, fixture.Local, fixture.Peer, 11, NamespaceId));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Same_full_replace_generation_rejects_a_different_accepted_evidence_set()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-fork-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 914);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            MetadataBaselineReport revisedPeerReport = MetadataBaselineReportProtocol.DecodeAndVerify(
                fixture.PeerReport, fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            var revised = new MetadataBaselineReport
            {
                NamespaceId = revisedPeerReport.NamespaceId,
                RosterGeneration = revisedPeerReport.RosterGeneration,
                RosterDigest = revisedPeerReport.RosterDigest,
                FullReplaceGeneration = revisedPeerReport.FullReplaceGeneration,
                SnapshotDigest = revisedPeerReport.SnapshotDigest,
                SnapshotEntryCount = revisedPeerReport.SnapshotEntryCount,
                ProviderChangeFeedWatermark = revisedPeerReport.ProviderChangeFeedWatermark,
                HlcWallFloor = revisedPeerReport.HlcWallFloor,
                HlcLogicalFloor = revisedPeerReport.HlcLogicalFloor,
                MemberNodeId = revisedPeerReport.MemberNodeId,
                MemberTransportEpoch = revisedPeerReport.MemberTransportEpoch,
                ReportSequence = 2,
                FeedEntries = revisedPeerReport.FeedEntries,
                ReceiveEntries = revisedPeerReport.ReceiveEntries
            };
            fixture.Gc.AcceptMetadataBaselineReport(
                MetadataBaselineReportProtocol.Sign(revised, fixture.Keys[1].Seed),
                fixture.Roster, fixture.Generation, fixture.SnapshotDigest);

            Assert.Throws<InvalidDataException>(() => metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
