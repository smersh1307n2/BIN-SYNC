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

    private sealed record MultiPeerFixture(
        StorageMetadataStore Metadata,
        StorageNamespaceGcStore Gc,
        TrustStore Trust,
        NamespaceRosterCertificate Roster,
        (byte[] Seed, byte[] PublicKey)[] Keys,
        Guid Generation,
        byte[] SnapshotDigest,
        NodeId Local,
        NodeId Peer1,
        NodeId Peer2,
        byte[] OutboundDigest);

    private static MultiPeerFixture CreateMultiPeerFixture(StorageDatabase storage)
    {
        NodeId local = Node(1), peer1 = Node(2), peer2 = Node(4);
        var keys = new[]
        {
            BatchSignature.GenerateKeyPair(),
            BatchSignature.GenerateKeyPair(),
            BatchSignature.GenerateKeyPair()
        };
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
                    NodeId = peer1, PublicKey = keys[1].PublicKey,
                    TransportPeerEpoch = 11, Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = peer2, PublicKey = keys[2].PublicKey,
                    TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Active
                }
            }
        };
        var gc = new StorageNamespaceGcStore(storage, Scope, local);
        NamespaceRosterCertificate roster = gc.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(proposedRoster, keys[0].Seed), trust);
        var metadata = new StorageMetadataStore(storage);
        metadata.AttachNamespaceGcGuard(gc, trust, Scope, local);

        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] snapshotDigest = SHA256.HashData(Encoding.UTF8.GetBytes("mbr1 multi-peer snapshot"));
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

        var frontierReports = new List<AppliedOriginFrontierReport>();
        for (int i = 0; i < keys.Length; i++)
        {
            byte[] encodedFrontier = OriginFrontierProtocol.Sign(new AppliedOriginFrontierReport
            {
                NamespaceId = NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[i].NodeId,
                MemberTransportEpoch = roster.Members[i].TransportPeerEpoch,
                ReportSequence = 1,
                FinalForAuthor = false,
                UnpublishedDraftCount = 0,
                MaxUnpublishedDraftHlcWall = 0,
                MaxUnpublishedDraftHlcLogical = 0,
                Entries = Array.Empty<AppliedOriginFrontierEntry>(),
                IssuedOrigins = Array.Empty<IssuedOriginFrontierEntry>()
            }, keys[i].Seed);
            frontierReports.Add(gc.AcceptOriginFrontier(encodedFrontier, roster));
        }
        NamespaceRetentionCheckpoint checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[NamespaceRosterProtocol.DigestSize], frontierReports,
            generation, snapshotDigest, 100, 2);
        gc.AcceptRetentionCheckpoint(NamespaceRetentionCheckpointProtocol.Sign(
            checkpoint, keys[0].Seed), roster, trust);

        metadata.RegisterInterestedPeer(Scope, local, peer1, 11, NamespaceId);
        metadata.RegisterInterestedPeer(Scope, local, peer2, 12, NamespaceId);
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
            },
            new MetadataFrozenOperation
            {
                ChangeId = Batch(22), Fingerprint = Digest(22), Payload = new byte[] { 0x22 },
                OriginAuthorEpoch = 7
            }
        });

        byte[] outboundDigest = Digest(31);
        metadata.RegisterInterestedPeerStream(Scope, local, peer1, 11, NamespaceId,
            local, baselineAckedCursor: 1, streamOffset: 1, issuerEpoch: 7, outboundDigest);
        metadata.RegisterInterestedPeerStream(Scope, local, peer2, 12, NamespaceId,
            local, baselineAckedCursor: 0, streamOffset: 1, issuerEpoch: 7, new byte[16]);

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
                    TargetPeer = peer1, TargetTransportEpoch = 11,
                    AuthorStream = local, FeedKind = MetadataBaselineFeedKind.Outbound,
                    IssuerEpoch = 7, AbsoluteFeedOffset = 1, DigestAnchor = outboundDigest
                },
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = peer2, TargetTransportEpoch = 12,
                    AuthorStream = local, FeedKind = MetadataBaselineFeedKind.Outbound,
                    IssuerEpoch = 7, AbsoluteFeedOffset = 1, DigestAnchor = new byte[16]
                }
            },
            ReceiveEntries = Array.Empty<MetadataBaselineReceiveEntry>()
        };
        var peer1Report = new MetadataBaselineReport
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
            MemberNodeId = peer1,
            MemberTransportEpoch = 11,
            ReportSequence = 1,
            FeedEntries = Array.Empty<MetadataBaselineFeedEntry>(),
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = local, SenderTransportEpoch = 10,
                    AuthorStream = local, IssuerEpoch = 7,
                    ReceiveCursor = 1, LastBatchDigest = outboundDigest
                }
            }
        };
        var peer2Report = new MetadataBaselineReport
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
            MemberNodeId = peer2,
            MemberTransportEpoch = 12,
            ReportSequence = 1,
            FeedEntries = Array.Empty<MetadataBaselineFeedEntry>(),
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = local, SenderTransportEpoch = 10,
                    AuthorStream = local, IssuerEpoch = 7,
                    ReceiveCursor = 0, LastBatchDigest = new byte[16]
                }
            }
        };

        gc.AcceptMetadataBaselineReport(
            MetadataBaselineReportProtocol.Sign(localReport, keys[0].Seed), roster, generation, snapshotDigest);
        gc.AcceptMetadataBaselineReport(
            MetadataBaselineReportProtocol.Sign(peer1Report, keys[1].Seed), roster, generation, snapshotDigest);
        gc.AcceptMetadataBaselineReport(
            MetadataBaselineReportProtocol.Sign(peer2Report, keys[2].Seed), roster, generation, snapshotDigest);

        return new MultiPeerFixture(metadata, gc, trust, roster, keys, generation, snapshotDigest,
            local, peer1, peer2, outboundDigest);
    }

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
        var frontierReports = new List<AppliedOriginFrontierReport>();
        for (int i = 0; i < keys.Length; i++)
        {
            byte[] encodedFrontier = OriginFrontierProtocol.Sign(new AppliedOriginFrontierReport
            {
                NamespaceId = NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[i].NodeId,
                MemberTransportEpoch = roster.Members[i].TransportPeerEpoch,
                ReportSequence = 1,
                FinalForAuthor = false,
                UnpublishedDraftCount = 0,
                MaxUnpublishedDraftHlcWall = 0,
                MaxUnpublishedDraftHlcLogical = 0,
                Entries = Array.Empty<AppliedOriginFrontierEntry>(),
                IssuedOrigins = Array.Empty<IssuedOriginFrontierEntry>()
            }, keys[i].Seed);
            frontierReports.Add(gc.AcceptOriginFrontier(encodedFrontier, roster));
        }
        NamespaceRetentionCheckpoint checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[NamespaceRosterProtocol.DigestSize], frontierReports,
            generation, snapshotDigest, 100, 2);
        gc.AcceptRetentionCheckpoint(NamespaceRetentionCheckpointProtocol.Sign(
            checkpoint, keys[0].Seed), roster, trust);

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
        metadata.RegisterInterestedPeerStream(Scope, local, peer, 11, NamespaceId,
            local, baselineAckedCursor: 0, streamOffset: 1, issuerEpoch: 7, outboundDigest);
        metadata.RegisterInterestedPeerStream(Scope, local, peer, 11, NamespaceId,
            relayAuthor, baselineAckedCursor: 0, streamOffset: 0, issuerEpoch: 5, new byte[16]);
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
    public void Create_metadata_baseline_report_captures_the_local_durable_feed_and_receive_vectors()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-capture-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 915);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;

            MetadataBaselineReport report = metadata.CreateMetadataBaselineReport(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest,
                snapshotEntryCount: 4, providerChangeFeedWatermark: 40, reportSequence: 1,
                fixture.Keys[0].Seed);

            Assert.Equal(fixture.Local, report.MemberNodeId);
            Assert.Equal(10U, report.MemberTransportEpoch);
            Assert.Equal(100UL, report.HlcWallFloor);
            Assert.Equal(2U, report.HlcLogicalFloor);
            Assert.Equal(2, report.FeedEntries.Count);
            MetadataBaselineFeedEntry outbound = Assert.Single(report.FeedEntries,
                entry => entry.AuthorStream == fixture.Local);
            Assert.Equal(fixture.Peer, outbound.TargetPeer);
            Assert.Equal(11U, outbound.TargetTransportEpoch);
            Assert.Equal(MetadataBaselineFeedKind.Outbound, outbound.FeedKind);
            Assert.Equal(7U, outbound.IssuerEpoch);
            Assert.Equal(1UL, outbound.AbsoluteFeedOffset);
            Assert.Equal(fixture.OutboundDigest, outbound.DigestAnchor);

            MetadataBaselineFeedEntry relay = Assert.Single(report.FeedEntries,
                entry => entry.AuthorStream == fixture.RelayAuthor);
            Assert.Equal(MetadataBaselineFeedKind.Relay, relay.FeedKind);
            Assert.Equal(0UL, relay.AbsoluteFeedOffset);
            Assert.Equal(new byte[16], relay.DigestAnchor);

            MetadataBaselineReceiveEntry receive = Assert.Single(report.ReceiveEntries);
            Assert.Equal(fixture.Peer, receive.SenderPeer);
            Assert.Equal(11U, receive.SenderTransportEpoch);
            Assert.Equal(fixture.RelayAuthor, receive.AuthorStream);
            Assert.Equal(5U, receive.IssuerEpoch);
            Assert.Equal(1UL, receive.ReceiveCursor);
            Assert.Equal(fixture.RelayDigest, receive.LastBatchDigest);

            byte[] encoded = MetadataBaselineReportProtocol.EncodeEnvelope(report);
            Assert.Equal(fixture.LocalReport, encoded);
            MetadataBaselineReport verified = MetadataBaselineReportProtocol.DecodeAndVerify(
                encoded, fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            Assert.Equal(report.ReportSequence, verified.ReportSequence);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Mbr1_capture_is_blocked_by_unresolved_pending_apply()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-capture-pending-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 916);
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
                BatchId = Batch(46),
                CursorStart = 2,
                CursorEnd = 2,
                PreviousBatchDigest = fixture.RelayDigest,
                BatchDigest = Digest(47),
                Operations = Array.Empty<MetadataOperation>()
            }, Guid.Parse("cccccccc-dddd-eeee-ffff-000000000001"));

            Assert.Throws<InvalidOperationException>(() => metadata.CreateMetadataBaselineReport(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest,
                snapshotEntryCount: 4, providerChangeFeedWatermark: 40, reportSequence: 2,
                fixture.Keys[0].Seed));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Newly_active_member_can_capture_a_fresh_mbr1_report_for_its_installed_generation()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-promoted-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 913);
            Fixture fixture = CreateFixture(storage);
            var promotedRoster = new NamespaceRosterCertificate
            {
                NamespaceId = fixture.Roster.NamespaceId,
                RosterGeneration = fixture.Roster.RosterGeneration + 1,
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(fixture.Roster),
                AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                Members = fixture.Roster.Members,
                RetiredOriginEpochs = fixture.Roster.RetiredOriginEpochs
            };
            NamespaceRosterCertificate accepted = fixture.Gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(promotedRoster, fixture.Keys[0].Seed), fixture.Trust);
            byte[] rosterDigest = NamespaceRosterProtocol.ComputeDigest(accepted);
            var localMember = accepted.Members.Single(member => member.NodeId == fixture.Local);
            var installedAck = new FullReplaceInstalledAck
            {
                NamespaceId = accepted.NamespaceId,
                RosterGeneration = accepted.RosterGeneration,
                RosterDigest = rosterDigest,
                Generation = fixture.Generation,
                Digest = fixture.SnapshotDigest,
                MemberNodeId = fixture.Local,
                MemberTransportEpoch = localMember.TransportPeerEpoch,
                HlcWallFloor = 100,
                HlcLogicalFloor = 2,
                Installed = true
            };

            MetadataBaselineReport report = fixture.Metadata.AcceptLocalFullReplaceAndCreateBaselineReport(
                Scope, fixture.Local, accepted, installedAck,
                snapshotEntryCount: 4, providerChangeFeedWatermark: 40,
                reportSequence: 1, fixture.Keys[0].Seed);

            Assert.Equal(accepted.RosterGeneration, report.RosterGeneration);
            Assert.Equal(fixture.Generation, report.FullReplaceGeneration);
            Assert.Equal(fixture.SnapshotDigest, report.SnapshotDigest);
            Assert.Equal(fixture.Local, report.MemberNodeId);
            Assert.Equal(localMember.TransportPeerEpoch, report.MemberTransportEpoch);
            Assert.Equal(1UL, report.ReportSequence);
            MetadataBaselineReport persisted = Assert.Single(
                fixture.Gc.ReadMetadataBaselineReports(
                    accepted, fixture.Generation, fixture.SnapshotDigest),
                item => item.MemberNodeId == fixture.Local);
            Assert.Equal(
                MetadataBaselineReportProtocol.EncodeEnvelope(report),
                MetadataBaselineReportProtocol.EncodeEnvelope(persisted));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
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
            NamespaceRosterCertificate roster;
            byte[] localPublicKey;

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
                roster = fixture.Roster;
                localPublicKey = fixture.Keys[0].PublicKey.ToArray();
                relayAuthor = fixture.RelayAuthor;

                Assert.False(metadata.IsFullReplaceBaselineApplied(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                ulong before = storage.LastCommitSeq;
                Assert.True(metadata.ApplyFullReplaceBaseline(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                Assert.Equal(before + 1, storage.LastCommitSeq);
                Assert.True(metadata.IsFullReplaceBaselineApplied(
                    Scope, local, fixture.Roster, generation, snapshotDigest));
                Assert.Throws<InvalidDataException>(() => metadata.IsFullReplaceBaselineApplied(
                    Scope, local, fixture.Roster, generation, Enumerable.Repeat((byte)99, 32).ToArray()));
                var staleRoster = new NamespaceRosterCertificate
                {
                    NamespaceId = fixture.Roster.NamespaceId,
                    RosterGeneration = fixture.Roster.RosterGeneration + 1,
                    PreviousRosterDigest = fixture.Roster.PreviousRosterDigest,
                    AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                    Members = fixture.Roster.Members,
                    RetiredOriginEpochs = fixture.Roster.RetiredOriginEpochs,
                    Signature = fixture.Roster.Signature
                };
                Assert.Throws<InvalidDataException>(() => metadata.IsFullReplaceBaselineApplied(
                    Scope, local, staleRoster, generation, snapshotDigest));
                Assert.Equal(0, metadata.CompactRelayThroughBounded(
                    Scope, local, NamespaceId, relayAuthor, 0, maxEntries: 1));
                Assert.Throws<ArgumentOutOfRangeException>(() => metadata.CompactRelayThroughBounded(
                    Scope, local, NamespaceId, relayAuthor, 0, maxEntries: 4097));
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
                trust.Allow(local, localPublicKey, TrustMode.PreShared);
                // The persisted marker and stream rows are validated during Metadata partition reopen.
                using var metadata = new StorageMetadataStore(storage);
                metadata.AttachNamespaceGcGuard(gc, trust, Scope, local);
                Assert.True(metadata.IsFullReplaceBaselineApplied(
                    Scope, local, roster, generation, snapshotDigest));
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Full_replace_baseline_is_blocked_by_immutable_peer_reservation(bool markPushed)
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-reservation-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 916);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            metadata.CompletePeerInitialSync(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId,
                new[]
                {
                    new MetadataPeerStreamBaseline(fixture.Local, 1, 7),
                    new MetadataPeerStreamBaseline(fixture.RelayAuthor, 0, 5)
                });
            var reservation = new MetadataOutboundBatchReservation
            {
                BatchId = Batch(0x61),
                IssuerEpoch = 7,
                StreamOffset = 1,
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Digest(0x62),
                Payload = new byte[] { 0x63 }
            };
            metadata.ReserveOutboundBatch(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local, reservation);
            if (markPushed)
                metadata.MarkOutboundBatchPushed(
                    Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local, reservation.BatchId);

            MetadataPeerStreamState beforeState = metadata.GetPeerStreamState(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local);
            ulong beforeCommit = storage.LastCommitSeq;
            Assert.Throws<InvalidOperationException>(() => metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            Assert.Equal(beforeCommit, storage.LastCommitSeq);
            Assert.False(metadata.IsFullReplaceBaselineApplied(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            MetadataPeerStreamState afterState = metadata.GetPeerStreamState(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local);
            Assert.Equal(beforeState.AckedCursor, afterState.AckedCursor);
            Assert.Equal(beforeState.PushedCursor, afterState.PushedCursor);
            Assert.Equal(beforeState.PendingBatch?.BatchId, afterState.PendingBatch?.BatchId);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_baseline_rejects_epoch_rollover_over_existing_peer_progress()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-epoch-rollover-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 917);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;

            MetadataBaselineReport local = MetadataBaselineReportProtocol.DecodeAndVerify(
                fixture.LocalReport, fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            MetadataBaselineReport peer = MetadataBaselineReportProtocol.DecodeAndVerify(
                fixture.PeerReport, fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            MetadataBaselineFeedEntry oldFeed = Assert.Single(local.FeedEntries,
                entry => entry.AuthorStream == fixture.Local);
            MetadataBaselineReceiveEntry oldReceive = Assert.Single(peer.ReceiveEntries,
                entry => entry.SenderPeer == fixture.Local && entry.AuthorStream == fixture.Local);

            var revisedLocal = new MetadataBaselineReport
            {
                NamespaceId = local.NamespaceId,
                RosterGeneration = local.RosterGeneration,
                RosterDigest = local.RosterDigest,
                FullReplaceGeneration = local.FullReplaceGeneration,
                SnapshotDigest = local.SnapshotDigest,
                SnapshotEntryCount = local.SnapshotEntryCount,
                ProviderChangeFeedWatermark = local.ProviderChangeFeedWatermark,
                HlcWallFloor = local.HlcWallFloor,
                HlcLogicalFloor = local.HlcLogicalFloor,
                MemberNodeId = local.MemberNodeId,
                MemberTransportEpoch = local.MemberTransportEpoch,
                ReportSequence = 2,
                FeedEntries = local.FeedEntries.Select(entry => entry.AuthorStream != fixture.Local
                    ? entry
                    : new MetadataBaselineFeedEntry
                    {
                        TargetPeer = entry.TargetPeer,
                        TargetTransportEpoch = entry.TargetTransportEpoch,
                        AuthorStream = entry.AuthorStream,
                        FeedKind = entry.FeedKind,
                        IssuerEpoch = 8,
                        AbsoluteFeedOffset = entry.AbsoluteFeedOffset,
                        DigestAnchor = new byte[16]
                    }).ToArray(),
                ReceiveEntries = local.ReceiveEntries
            };
            var revisedPeer = new MetadataBaselineReport
            {
                NamespaceId = peer.NamespaceId,
                RosterGeneration = peer.RosterGeneration,
                RosterDigest = peer.RosterDigest,
                FullReplaceGeneration = peer.FullReplaceGeneration,
                SnapshotDigest = peer.SnapshotDigest,
                SnapshotEntryCount = peer.SnapshotEntryCount,
                ProviderChangeFeedWatermark = peer.ProviderChangeFeedWatermark,
                HlcWallFloor = peer.HlcWallFloor,
                HlcLogicalFloor = peer.HlcLogicalFloor,
                MemberNodeId = peer.MemberNodeId,
                MemberTransportEpoch = peer.MemberTransportEpoch,
                ReportSequence = 2,
                FeedEntries = peer.FeedEntries,
                ReceiveEntries = peer.ReceiveEntries.Concat(new[]
                {
                    new MetadataBaselineReceiveEntry
                    {
                        SenderPeer = fixture.Local,
                        SenderTransportEpoch = 10,
                        AuthorStream = fixture.Local,
                        IssuerEpoch = 8,
                        ReceiveCursor = 0,
                        LastBatchDigest = new byte[16]
                    }
                })
                .OrderBy(entry => Convert.ToHexString(entry.SenderPeer.ToArray()), StringComparer.Ordinal)
                .ThenBy(entry => entry.SenderTransportEpoch)
                .ThenBy(entry => Convert.ToHexString(entry.AuthorStream.ToArray()), StringComparer.Ordinal)
                .ThenBy(entry => entry.IssuerEpoch)
                .ToArray()
            };
            Assert.Equal(7U, oldFeed.IssuerEpoch);
            Assert.Equal(7U, oldReceive.IssuerEpoch);
            fixture.Gc.AcceptMetadataBaselineReport(
                MetadataBaselineReportProtocol.Sign(revisedLocal, fixture.Keys[0].Seed),
                fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            fixture.Gc.AcceptMetadataBaselineReport(
                MetadataBaselineReportProtocol.Sign(revisedPeer, fixture.Keys[1].Seed),
                fixture.Roster, fixture.Generation, fixture.SnapshotDigest);

            ulong beforeCommit = storage.LastCommitSeq;
            Assert.Throws<InvalidDataException>(() => metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            Assert.Equal(beforeCommit, storage.LastCommitSeq);
            Assert.False(metadata.IsFullReplaceBaselineApplied(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            MetadataPeerStreamState retained = metadata.GetPeerStreamState(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local);
            Assert.Equal(7U, retained.IssuerEpoch);
            Assert.Equal(fixture.OutboundDigest, retained.LastBatchDigest);
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

    [Fact]
    public void Full_replace_baseline_payload_compaction_is_bounded_and_resumes_after_reopen()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-payload-compact-" + Guid.NewGuid().ToString("N"));
        NodeId local = default;
        NodeId relayAuthor = default;
        NamespaceRosterCertificate roster = null!;
        Guid generation = Guid.Empty;
        byte[] snapshotDigest = Array.Empty<byte>();
        byte[] localPublicKey = Array.Empty<byte>();
        try
        {
            using (var storage = StorageDatabase.Open(directory, 918))
            {
                Fixture fixture = CreateFixture(storage);
                local = fixture.Local;
                relayAuthor = fixture.RelayAuthor;
                roster = fixture.Roster;
                generation = fixture.Generation;
                snapshotDigest = fixture.SnapshotDigest.ToArray();
                localPublicKey = fixture.Keys[0].PublicKey.ToArray();
                using StorageMetadataStore metadata = fixture.Metadata;

                Assert.Throws<InvalidOperationException>(() =>
                    metadata.CompactFullReplaceBaselinePayloadsBounded(
                        Scope, local, roster, generation, snapshotDigest, maxEntriesPerStream: 1));
                Assert.True(metadata.ApplyFullReplaceBaseline(
                    Scope, local, roster, generation, snapshotDigest));

                metadata.FreezeOutbound(Scope, local, NamespaceId, local, new[]
                {
                    new MetadataFrozenOperation
                    {
                        ChangeId = Batch(22), Fingerprint = Digest(22), Payload = new byte[] { 0x22 },
                        OriginAuthorEpoch = 7
                    }
                });

                // The first page is committed. Simulate process death before the caller
                // can observe/acknowledge completion; the remaining page must be retryable.
                Assert.Equal(1, metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, local, roster, generation, snapshotDigest, maxEntriesPerStream: 1));
            }

            using (var storage = StorageDatabase.Open(directory, 918))
            {
                var gc = new StorageNamespaceGcStore(storage, Scope, local);
                var trust = new TrustStore();
                trust.Allow(local, localPublicKey, TrustMode.PreShared);
                using var metadata = new StorageMetadataStore(storage);
                metadata.AttachNamespaceGcGuard(gc, trust, Scope, local);

                Assert.True(metadata.IsFullReplaceBaselineApplied(
                    Scope, local, roster, generation, snapshotDigest));
                Assert.Equal(1, metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, local, roster, generation, snapshotDigest, maxEntriesPerStream: 1));
                Assert.Equal(0, metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, local, roster, generation, snapshotDigest, maxEntriesPerStream: 1));
                Assert.Equal(0, metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, local, roster, generation, snapshotDigest, maxEntriesPerStream: 1));

                ulong lease = metadata.AcquireOutboundLease(
                    Scope, local, NamespaceId, local, Guid.NewGuid());
                MetadataOutboundPage outbound = metadata.ReadOutbound(
                    Scope, local, NamespaceId, local, lease, afterSequence: 0, limit: 10);
                MetadataOutboundItem remaining = Assert.Single(outbound.Items);
                Assert.Equal(3UL, remaining.Sequence);
                Assert.Equal(Batch(22), remaining.ChangeId);

                MetadataRelayPage relay = metadata.ReadRelay(
                    Scope, local, NamespaceId, relayAuthor, afterSequence: 0, limit: 10);
                Assert.Single(relay.Items);
                Assert.Equal(1UL, relay.Items[0].Sequence);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Full_replace_baseline_payload_compaction_is_blocked_by_new_immutable_reservation(bool markPushed)
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-payload-reservation-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 919);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            var reservation = new MetadataOutboundBatchReservation
            {
                BatchId = Batch(0x73),
                IssuerEpoch = 7,
                StreamOffset = 1,
                CursorStart = 2,
                CursorEnd = 2,
                PreviousBatchDigest = fixture.OutboundDigest.ToArray(),
                BatchDigest = Digest(0x74),
                Payload = new byte[] { 0x74 }
            };
            metadata.ReserveOutboundBatch(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local, reservation);
            if (markPushed)
                metadata.MarkOutboundBatchPushed(
                    Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local, reservation.BatchId);

            ulong beforeCommit = storage.LastCommitSeq;
            Assert.Throws<InvalidOperationException>(() =>
                metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, fixture.Local, fixture.Roster, fixture.Generation,
                    fixture.SnapshotDigest, maxEntriesPerStream: 1));
            Assert.Equal(beforeCommit, storage.LastCommitSeq);
            Assert.True(metadata.IsFullReplaceBaselineApplied(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            Assert.Equal(reservation.BatchId, metadata.GetPendingOutboundBatch(
                Scope, fixture.Local, fixture.Peer, 11, NamespaceId, fixture.Local)?.BatchId);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_baseline_payload_compaction_uses_the_slowest_active_peer_cursor()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-multi-peer-compact-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 920);
            MultiPeerFixture fixture = CreateMultiPeerFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;

            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            metadata.FreezeOutbound(Scope, fixture.Local, NamespaceId, fixture.Local, new[]
            {
                new MetadataFrozenOperation
                {
                    ChangeId = Batch(23), Fingerprint = Digest(23), Payload = new byte[] { 0x23 },
                    OriginAuthorEpoch = 7
                }
            });

            // Peer1 has received two absolute feed positions; peer2 has received only
            // one. The common safe prefix is therefore one, not peer1's faster cursor.
            Assert.Equal(1, metadata.CompactFullReplaceBaselinePayloadsBounded(
                Scope, fixture.Local, fixture.Roster, fixture.Generation,
                fixture.SnapshotDigest, maxEntriesPerStream: 10));

            ulong lease = metadata.AcquireOutboundLease(
                Scope, fixture.Local, NamespaceId, fixture.Local, Guid.NewGuid());
            MetadataOutboundPage outbound = metadata.ReadOutbound(
                Scope, fixture.Local, NamespaceId, fixture.Local, lease, afterSequence: 0, limit: 10);
            Assert.Equal(new ulong[] { 2, 3, 4 }, outbound.Items.Select(item => item.Sequence));
            Assert.Equal(new[] { Batch(21), Batch(22), Batch(23) },
                outbound.Items.Select(item => item.ChangeId));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
 
    [Fact]
    public void Full_replace_baseline_payload_compaction_rejects_report_evidence_changed_after_marker()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-payload-evidence-drift-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 921);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            MetadataBaselineReport peer = MetadataBaselineReportProtocol.DecodeAndVerify(
                fixture.PeerReport, fixture.Roster, fixture.Generation, fixture.SnapshotDigest);
            var changedPeerReport = new MetadataBaselineReport
            {
                NamespaceId = peer.NamespaceId,
                RosterGeneration = peer.RosterGeneration,
                RosterDigest = peer.RosterDigest.ToArray(),
                FullReplaceGeneration = peer.FullReplaceGeneration,
                SnapshotDigest = peer.SnapshotDigest.ToArray(),
                SnapshotEntryCount = peer.SnapshotEntryCount,
                ProviderChangeFeedWatermark = peer.ProviderChangeFeedWatermark,
                HlcWallFloor = peer.HlcWallFloor,
                HlcLogicalFloor = peer.HlcLogicalFloor,
                MemberNodeId = peer.MemberNodeId,
                MemberTransportEpoch = peer.MemberTransportEpoch,
                ReportSequence = checked(peer.ReportSequence + 1),
                FeedEntries = peer.FeedEntries,
                ReceiveEntries = peer.ReceiveEntries
            };
            fixture.Gc.AcceptMetadataBaselineReport(
                MetadataBaselineReportProtocol.Sign(changedPeerReport, fixture.Keys[1].Seed),
                fixture.Roster, fixture.Generation, fixture.SnapshotDigest);

            ulong beforeCompaction = storage.LastCommitSeq;
            Assert.Throws<InvalidDataException>(() =>
                metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, fixture.Local, fixture.Roster, fixture.Generation,
                    fixture.SnapshotDigest, maxEntriesPerStream: 1));
            Assert.Equal(beforeCompaction, storage.LastCommitSeq);
            Assert.True(metadata.IsFullReplaceBaselineApplied(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            ulong lease = metadata.AcquireOutboundLease(
                Scope, fixture.Local, NamespaceId, fixture.Local, Guid.NewGuid());
            MetadataOutboundPage outbound = metadata.ReadOutbound(
                Scope, fixture.Local, NamespaceId, fixture.Local, lease, afterSequence: 0, limit: 10);
            Assert.Equal(new ulong[] { 1, 2 }, outbound.Items.Select(item => item.Sequence));
            MetadataRelayPage relay = metadata.ReadRelay(
                Scope, fixture.Local, NamespaceId, fixture.RelayAuthor, afterSequence: 0, limit: 10);
            Assert.Single(relay.Items);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_baseline_payload_compaction_rejects_retired_roster_after_marker()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-payload-retired-roster-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 922);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            // Retirement is legal only after the author publishes its final signed,
            // draft-free issued-origin report for the current roster generation.
            fixture.Gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                new AppliedOriginFrontierReport
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = fixture.Roster.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(fixture.Roster),
                    MemberNodeId = fixture.Peer,
                    MemberTransportEpoch = 11,
                    ReportSequence = 2,
                    FinalForAuthor = true,
                    UnpublishedDraftCount = 0,
                    MaxUnpublishedDraftHlcWall = 0,
                    MaxUnpublishedDraftHlcLogical = 0,
                    Entries = Array.Empty<AppliedOriginFrontierEntry>(),
                    IssuedOrigins = Array.Empty<IssuedOriginFrontierEntry>()
                }, fixture.Keys[1].Seed), fixture.Roster);

            NamespaceRosterCertificate retiredRoster = new()
            {
                NamespaceId = fixture.Roster.NamespaceId,
                RosterGeneration = checked(fixture.Roster.RosterGeneration + 1),
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(fixture.Roster),
                AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                Members = fixture.Roster.Members.Select(member => new NamespaceRosterMember
                {
                    NodeId = member.NodeId,
                    PublicKey = member.PublicKey.ToArray(),
                    TransportPeerEpoch = member.TransportPeerEpoch,
                    Status = member.NodeId == fixture.Peer
                        ? NamespaceMemberStatus.Retired
                        : member.Status,
                    RetiredThroughEpoch = member.NodeId == fixture.Peer
                        ? member.TransportPeerEpoch
                        : member.RetiredThroughEpoch
                }).ToArray(),
                RetiredOriginEpochs = fixture.Roster.RetiredOriginEpochs
            };
            fixture.Gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(retiredRoster, fixture.Keys[0].Seed), fixture.Trust);

            ulong beforeCompaction = storage.LastCommitSeq;
            Assert.Throws<InvalidDataException>(() =>
                metadata.CompactFullReplaceBaselinePayloadsBounded(
                    Scope, fixture.Local, fixture.Roster, fixture.Generation,
                    fixture.SnapshotDigest, maxEntriesPerStream: 1));
            Assert.Equal(beforeCompaction, storage.LastCommitSeq);

            ulong lease = metadata.AcquireOutboundLease(
                Scope, fixture.Local, NamespaceId, fixture.Local, Guid.NewGuid());
            MetadataOutboundPage outbound = metadata.ReadOutbound(
                Scope, fixture.Local, NamespaceId, fixture.Local, lease, afterSequence: 0, limit: 10);
            Assert.Equal(new ulong[] { 1, 2 }, outbound.Items.Select(item => item.Sequence));
            MetadataRelayPage relay = metadata.ReadRelay(
                Scope, fixture.Local, NamespaceId, fixture.RelayAuthor, afterSequence: 0, limit: 10);
            Assert.Single(relay.Items);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_baseline_marker_and_reports_are_not_inherited_after_retirement_and_rejoin()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-mbr1-payload-rejoin-roster-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 923);
            Fixture fixture = CreateFixture(storage);
            using StorageMetadataStore metadata = fixture.Metadata;
            Assert.True(metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));

            fixture.Gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                new AppliedOriginFrontierReport
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = fixture.Roster.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(fixture.Roster),
                    MemberNodeId = fixture.Peer,
                    MemberTransportEpoch = 11,
                    ReportSequence = 2,
                    FinalForAuthor = true,
                    UnpublishedDraftCount = 0,
                    MaxUnpublishedDraftHlcWall = 0,
                    MaxUnpublishedDraftHlcLogical = 0,
                    Entries = Array.Empty<AppliedOriginFrontierEntry>(),
                    IssuedOrigins = Array.Empty<IssuedOriginFrontierEntry>()
                }, fixture.Keys[1].Seed), fixture.Roster);

            NamespaceRosterCertificate retired = new()
            {
                NamespaceId = NamespaceId,
                RosterGeneration = 2,
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(fixture.Roster),
                AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                Members = new[]
                {
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Local, PublicKey = fixture.Keys[0].PublicKey,
                        TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Peer, PublicKey = fixture.Keys[1].PublicKey,
                        TransportPeerEpoch = 11, Status = NamespaceMemberStatus.Retired,
                        RetiredThroughEpoch = 11
                    }
                },
                RetiredOriginEpochs = fixture.Roster.RetiredOriginEpochs
            };
            retired = fixture.Gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(retired, fixture.Keys[0].Seed), fixture.Trust);

            NamespaceRosterCertificate joining = new()
            {
                NamespaceId = NamespaceId,
                RosterGeneration = 3,
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(retired),
                AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                Members = new[]
                {
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Local, PublicKey = fixture.Keys[0].PublicKey,
                        TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Peer, PublicKey = fixture.Keys[1].PublicKey,
                        TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Joining
                    }
                },
                RetiredOriginEpochs = retired.RetiredOriginEpochs
            };
            joining = fixture.Gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(joining, fixture.Keys[0].Seed), fixture.Trust);

            // Establish fresh current-generation evidence for the Active roster
            // before the returning member can ACK and be promoted.
            byte[] joiningRosterDigest = NamespaceRosterProtocol.ComputeDigest(joining);
            fixture.Gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                new AppliedOriginFrontierReport
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = joining.RosterGeneration,
                    RosterDigest = joiningRosterDigest,
                    MemberNodeId = fixture.Local,
                    MemberTransportEpoch = 10,
                    ReportSequence = 1,
                    FinalForAuthor = false,
                    UnpublishedDraftCount = 0,
                    MaxUnpublishedDraftHlcWall = 0,
                    MaxUnpublishedDraftHlcLogical = 0,
                    Entries = Array.Empty<AppliedOriginFrontierEntry>(),
                    IssuedOrigins = Array.Empty<IssuedOriginFrontierEntry>()
                }, fixture.Keys[0].Seed), joining);

            fixture.Gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                new FullReplaceInstalledAck
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = joining.RosterGeneration,
                    RosterDigest = joiningRosterDigest,
                    Generation = fixture.Generation,
                    Digest = fixture.SnapshotDigest,
                    MemberNodeId = fixture.Local,
                    MemberTransportEpoch = 10,
                    HlcWallFloor = 100,
                    HlcLogicalFloor = 2,
                    Installed = true
                }, fixture.Keys[0].Seed), joining);

            IReadOnlyList<AppliedOriginFrontierReport> joiningFrontiers =
                fixture.Gc.ReadOriginFrontiers(joining);
            NamespaceRetentionCheckpoint joiningCheckpoint =
                NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    joining, 1, new byte[NamespaceRosterProtocol.DigestSize], joiningFrontiers,
                    fixture.Generation, fixture.SnapshotDigest, 100, 2);
            fixture.Gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(joiningCheckpoint, fixture.Keys[0].Seed),
                joining, fixture.Trust);

            // A returning member must install and sign the current FULL_REPLACE
            // generation under its new transport epoch before promotion to Active.
            fixture.Gc.AcceptFullReplaceInstalledAck(FullReplaceInstalledAckProtocol.Sign(
                new FullReplaceInstalledAck
                {
                    NamespaceId = NamespaceId,
                    RosterGeneration = joining.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(joining),
                    Generation = fixture.Generation,
                    Digest = fixture.SnapshotDigest,
                    MemberNodeId = fixture.Peer,
                    MemberTransportEpoch = 12,
                    HlcWallFloor = 100,
                    HlcLogicalFloor = 2,
                    Installed = true
                }, fixture.Keys[1].Seed), joining);

            NamespaceRosterCertificate rejoined = new()
            {
                NamespaceId = NamespaceId,
                RosterGeneration = 4,
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(joining),
                AuthorityNodeId = fixture.Roster.AuthorityNodeId,
                Members = new[]
                {
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Local, PublicKey = fixture.Keys[0].PublicKey,
                        TransportPeerEpoch = 10, Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = fixture.Peer, PublicKey = fixture.Keys[1].PublicKey,
                        TransportPeerEpoch = 12, Status = NamespaceMemberStatus.Active
                    }
                },
                RetiredOriginEpochs = retired.RetiredOriginEpochs
            };
            rejoined = fixture.Gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(rejoined, fixture.Keys[0].Seed), fixture.Trust);

            Assert.Empty(fixture.Gc.ReadMetadataBaselineReports(
                rejoined, fixture.Generation, fixture.SnapshotDigest));
            Assert.False(fixture.Gc.TryReadRetentionCheckpoint(rejoined, fixture.Trust, out _));

            ulong beforeRejectedReuse = storage.LastCommitSeq;
            Assert.Throws<InvalidDataException>(() => metadata.ApplyFullReplaceBaseline(
                Scope, fixture.Local, fixture.Roster, fixture.Generation, fixture.SnapshotDigest));
            Assert.Throws<InvalidDataException>(() => metadata.CompactFullReplaceBaselinePayloadsBounded(
                Scope, fixture.Local, fixture.Roster, fixture.Generation,
                fixture.SnapshotDigest, maxEntriesPerStream: 1));
            Assert.Equal(beforeRejectedReuse, storage.LastCommitSeq);

            ulong lease = metadata.AcquireOutboundLease(
                Scope, fixture.Local, NamespaceId, fixture.Local, Guid.NewGuid());
            MetadataOutboundPage outbound = metadata.ReadOutbound(
                Scope, fixture.Local, NamespaceId, fixture.Local, lease, afterSequence: 0, limit: 10);
            Assert.Equal(new ulong[] { 1, 2 }, outbound.Items.Select(item => item.Sequence));
            MetadataRelayPage relay = metadata.ReadRelay(
                Scope, fixture.Local, NamespaceId, fixture.RelayAuthor, afterSequence: 0, limit: 10);
            Assert.Single(relay.Items);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

}
