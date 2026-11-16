using System.Security.Cryptography;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class NamespaceGcSessionTests
{
    [Fact]
    public async Task Negotiated_v31_session_exchanges_signed_roster_frontier_checkpoint_and_install_ack()
    {
        using var nodeA = Fix.MakeNode(1);
        using var nodeB = Fix.MakeNode(2);
        var (authoritySeed, authorityKey) = BatchSignature.GenerateKeyPair();
        var (memberSeed, memberKey) = BatchSignature.GenerateKeyPair();
        var roster = new NamespaceRosterCertificate
        {
            NamespaceId = 19,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = nodeA.NodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = nodeA.NodeId,
                    PublicKey = authorityKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = nodeB.NodeId,
                    PublicKey = memberKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        var trustB = new TrustStore();
        trustB.Allow(nodeA.NodeId, authorityKey, TrustMode.PreShared);

        SessionCapabilities capabilities =
            SessionCapabilities.NamespaceRosterV31 |
            SessionCapabilities.OriginFrontierV31 |
            SessionCapabilities.FullReplaceV31;
        var (transportA, transportB) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, transportB, capabilities);
        var connectTask = SyncSession.ConnectAsync(nodeA, transportA, capabilities);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        Assert.Equal(capabilities | SessionCapabilities.IncrementalAliasMap, initiator.NegotiatedCapabilities);
        Assert.Equal(capabilities | SessionCapabilities.IncrementalAliasMap, acceptor.NegotiatedCapabilities);

        var rosterReceive = acceptor.ReceiveNamespaceRosterAsync(trustB);
        await initiator.SendNamespaceRosterAsync(roster, authoritySeed, frameSequence: 1);
        NamespaceRosterCertificate receivedRoster = await rosterReceive;
        Assert.Equal(roster.RosterGeneration, receivedRoster.RosterGeneration);

        var report = new AppliedOriginFrontierReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = nodeB.NodeId,
            MemberTransportEpoch = 1,
            ReportSequence = 1,
            Entries = Array.Empty<AppliedOriginFrontierEntry>()
        };
        var frontierReceive = initiator.ReceiveOriginFrontierAsync(roster);
        await acceptor.SendOriginFrontierAsync(report, memberSeed, frameSequence: 1);
        AppliedOriginFrontierReport receivedFrontier = await frontierReceive;
        Assert.Equal(nodeB.NodeId, receivedFrontier.MemberNodeId);

        var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[32], Array.Empty<AppliedOriginFrontierReport>());
        var checkpointReceive = acceptor.ReceiveRetentionCheckpointAsync(roster, trustB);
        await initiator.SendRetentionCheckpointAsync(checkpoint, authoritySeed, frameSequence: 2);
        NamespaceRetentionCheckpoint receivedCheckpoint = await checkpointReceive;
        Assert.Equal(1UL, receivedCheckpoint.CheckpointSequence);

        Guid generation = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        byte[] digest = SHA256.HashData(new byte[] { 1, 2, 3 });
        var ack = new FullReplaceInstalledAck
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            Generation = generation,
            Digest = digest,
            MemberNodeId = nodeB.NodeId,
            MemberTransportEpoch = 1,
            Installed = true
        };
        var ackReceive = initiator.ReceiveFullReplaceInstalledAckAsync(roster);
        await acceptor.SendFullReplaceInstalledAckAsync(ack, memberSeed, frameSequence: 2);
        FullReplaceInstalledAck receivedAck = await ackReceive;
        Assert.Equal(generation, receivedAck.Generation);
        Assert.Equal(nodeB.NodeId, receivedAck.MemberNodeId);

        var baselineReport = new MetadataBaselineReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            FullReplaceGeneration = generation,
            SnapshotDigest = digest,
            SnapshotEntryCount = 3,
            ProviderChangeFeedWatermark = 17,
            HlcWallFloor = 123,
            HlcLogicalFloor = 4,
            MemberNodeId = nodeB.NodeId,
            MemberTransportEpoch = 1,
            ReportSequence = 1,
            FeedEntries = Array.Empty<MetadataBaselineFeedEntry>(),
            ReceiveEntries = Array.Empty<MetadataBaselineReceiveEntry>()
        };
        var baselineReceive = initiator.ReceiveMetadataBaselineReportAsync(
            roster, generation, digest);
        await acceptor.SendMetadataBaselineReportAsync(
            baselineReport, memberSeed, roster, generation, digest, frameSequence: 3);
        MetadataBaselineReport receivedBaseline = await baselineReceive;
        Assert.Equal(nodeB.NodeId, receivedBaseline.MemberNodeId);
        Assert.Equal(17UL, receivedBaseline.ProviderChangeFeedWatermark);
        Assert.Equal(123UL, receivedBaseline.HlcWallFloor);
    }

    [Fact]
    public async Task V31_gc_messages_fail_closed_when_required_capabilities_are_not_negotiated()
    {
        using var nodeA = Fix.MakeNode(11);
        using var nodeB = Fix.MakeNode(12);
        var (transportA, transportB) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, transportB, SessionCapabilities.NamespaceRosterV31);
        var connectTask = SyncSession.ConnectAsync(nodeA, transportA, SessionCapabilities.NamespaceRosterV31);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;

        var (seed, key) = BatchSignature.GenerateKeyPair();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            initiator.SendOriginFrontierAsync(new AppliedOriginFrontierReport(), seed));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            initiator.SendFullReplaceInstalledAckAsync(new FullReplaceInstalledAck(), seed));
        Assert.Equal(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.IncrementalAliasMap, acceptor.NegotiatedCapabilities);
    }
}
