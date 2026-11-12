using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class MetadataBaselineReportProtocolTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate CreateRoster(
        (byte[] Seed, byte[] PublicKey)[] keys)
    {
        return new NamespaceRosterCertificate
        {
            NamespaceId = 17,
            RosterGeneration = 4,
            PreviousRosterDigest = Enumerable.Range(1, 32).Select(value => (byte)value).ToArray(),
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = keys.Select((key, index) => new NamespaceRosterMember
            {
                NodeId = NodeId.From(Id((byte)(index + 1))),
                PublicKey = key.PublicKey,
                TransportPeerEpoch = (uint)(20 + index),
                Status = NamespaceMemberStatus.Active
            }).ToArray()
        };
    }

    private static MetadataBaselineReport CreateReport(
        NamespaceRosterCertificate roster, byte[] snapshotDigest)
    {
        return new MetadataBaselineReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            FullReplaceGeneration = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
            SnapshotDigest = snapshotDigest,
            SnapshotEntryCount = 123,
            ProviderChangeFeedWatermark = 456,
            HlcWallFloor = 789,
            HlcLogicalFloor = 3,
            MemberNodeId = roster.Members[0].NodeId,
            MemberTransportEpoch = roster.Members[0].TransportPeerEpoch,
            ReportSequence = 1,
            FeedEntries = new[]
            {
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = roster.Members[1].NodeId,
                    TargetTransportEpoch = roster.Members[1].TransportPeerEpoch,
                    AuthorStream = NodeId.From(Id(3)),
                    FeedKind = MetadataBaselineFeedKind.Outbound,
                    IssuerEpoch = 7,
                    AbsoluteFeedOffset = 99,
                    DigestAnchor = new byte[16]
                }
            },
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = roster.Members[1].NodeId,
                    SenderTransportEpoch = roster.Members[1].TransportPeerEpoch,
                    AuthorStream = NodeId.From(Id(3)),
                    IssuerEpoch = 7,
                    ReceiveCursor = 0,
                    LastBatchDigest = new byte[16]
                }
            }
        };
    }

    [Fact]
    public void MBR1_member_signed_report_roundtrips_and_binds_roster_and_snapshot()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = CreateRoster(keys);
        byte[] snapshotDigest = Enumerable.Range(1, 32).Select(value => (byte)(255 - value)).ToArray();
        Guid generation = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var report = CreateReport(roster, snapshotDigest);

        byte[] encoded = MetadataBaselineReportProtocol.Sign(report, keys[0].Seed);
        var decoded = MetadataBaselineReportProtocol.DecodeAndVerify(
            encoded, roster, generation, snapshotDigest);

        Assert.Equal(report.MemberNodeId, decoded.MemberNodeId);
        Assert.Equal(report.ProviderChangeFeedWatermark, decoded.ProviderChangeFeedWatermark);
        Assert.Equal(report.FullReplaceGeneration, decoded.FullReplaceGeneration);
        Assert.Equal(99UL, Assert.Single(decoded.FeedEntries).AbsoluteFeedOffset);
        Assert.Equal(0UL, Assert.Single(decoded.ReceiveEntries).ReceiveCursor);

        byte[] tampered = encoded.ToArray();
        tampered[^1] ^= 1;
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() =>
            MetadataBaselineReportProtocol.DecodeAndVerify(tampered, roster, generation, snapshotDigest));
    }

    [Fact]
    public void MBR1_rejects_wrong_generation_snapshot_roster_and_non_active_signer()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = CreateRoster(keys);
        byte[] digest = new byte[32];
        Guid generation = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        byte[] encoded = MetadataBaselineReportProtocol.Sign(CreateReport(roster, digest), keys[0].Seed);

        Assert.Throws<InvalidDataException>(() => MetadataBaselineReportProtocol.DecodeAndVerify(
            encoded, roster, Guid.NewGuid(), digest));
        Assert.Throws<InvalidDataException>(() => MetadataBaselineReportProtocol.DecodeAndVerify(
            encoded, roster, generation, Enumerable.Repeat((byte)1, 32).ToArray()));

        var joiningRoster = new NamespaceRosterCertificate
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            PreviousRosterDigest = roster.PreviousRosterDigest,
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = roster.Members.Select((member, index) => new NamespaceRosterMember
            {
                NodeId = member.NodeId,
                PublicKey = member.PublicKey,
                TransportPeerEpoch = member.TransportPeerEpoch,
                Status = index == 0 ? NamespaceMemberStatus.Joining : NamespaceMemberStatus.Active
            }).ToArray()
        };
        var joiningReport = CreateReport(joiningRoster, digest);
        byte[] joiningEncoded = MetadataBaselineReportProtocol.Sign(joiningReport, keys[0].Seed);
        Assert.Throws<InvalidDataException>(() => MetadataBaselineReportProtocol.DecodeAndVerify(
            joiningEncoded, joiningRoster, generation, digest));
    }

    [Fact]
    public void MBR1_rejects_duplicate_peer_author_feed_even_when_kind_differs()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = CreateRoster(keys);
        byte[] digest = new byte[32];
        var report = CreateReport(roster, digest);
        var first = Assert.Single(report.FeedEntries);
        var duplicate = new MetadataBaselineFeedEntry
        {
            TargetPeer = first.TargetPeer,
            TargetTransportEpoch = first.TargetTransportEpoch,
            AuthorStream = first.AuthorStream,
            FeedKind = MetadataBaselineFeedKind.Relay,
            IssuerEpoch = first.IssuerEpoch,
            AbsoluteFeedOffset = first.AbsoluteFeedOffset,
            DigestAnchor = first.DigestAnchor
        };
        var invalid = new MetadataBaselineReport
        {
            NamespaceId = report.NamespaceId,
            RosterGeneration = report.RosterGeneration,
            RosterDigest = report.RosterDigest,
            FullReplaceGeneration = report.FullReplaceGeneration,
            SnapshotDigest = report.SnapshotDigest,
            SnapshotEntryCount = report.SnapshotEntryCount,
            ProviderChangeFeedWatermark = report.ProviderChangeFeedWatermark,
            HlcWallFloor = report.HlcWallFloor,
            HlcLogicalFloor = report.HlcLogicalFloor,
            MemberNodeId = report.MemberNodeId,
            MemberTransportEpoch = report.MemberTransportEpoch,
            ReportSequence = report.ReportSequence,
            FeedEntries = new[] { first, duplicate },
            ReceiveEntries = report.ReceiveEntries
        };
        Assert.Throws<ArgumentException>(() => MetadataBaselineReportProtocol.Sign(invalid, keys[0].Seed));
    }
}
