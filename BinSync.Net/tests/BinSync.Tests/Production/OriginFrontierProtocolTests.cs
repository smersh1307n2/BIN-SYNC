using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class OriginFrontierProtocolTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate Roster(
        (byte[] Seed, byte[] Key)[] keys, NamespaceMemberStatus[]? statuses = null)
    {
        statuses ??= Enumerable.Repeat(NamespaceMemberStatus.Active, keys.Length).ToArray();
        var members = new NamespaceRosterMember[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            members[i] = new NamespaceRosterMember
            {
                NodeId = NodeId.From(Id((byte)(i + 1))),
                PublicKey = keys[i].Key,
                TransportPeerEpoch = 10u + (uint)i,
                Status = statuses[i],
                RetiredThroughEpoch = statuses[i] == NamespaceMemberStatus.Retired ? 9u : 0u
            };
        }
        return new NamespaceRosterCertificate
        {
            NamespaceId = 12,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = members
        };
    }

    private static AppliedOriginFrontierReport Report(
        NamespaceRosterCertificate roster, int memberIndex, byte[] seed,
        params (byte Author, uint Epoch, ulong Sequence)[] entries)
    {
        var report = new AppliedOriginFrontierReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            ReportSequence = 1,
            Entries = entries.OrderBy(e => e.Author).ThenBy(e => e.Epoch).Select(e =>
                new AppliedOriginFrontierEntry
                {
                    OriginAuthorNodeId = NodeId.From(Id(e.Author)),
                    OriginAuthorEpoch = e.Epoch,
                    ContiguousAppliedOriginSequence = e.Sequence
                }).ToArray(),
            IssuedOrigins = entries.Where(e => e.Author == (byte)(memberIndex + 1))
                .OrderBy(e => e.Epoch).Select(e => new IssuedOriginFrontierEntry
                {
                    OriginAuthorEpoch = e.Epoch,
                    IssuedOriginSequence = e.Sequence
                }).ToArray()
        };
        return OriginFrontierProtocol.DecodeAndVerify(
            OriginFrontierProtocol.Sign(report, seed), roster);
    }

    [Fact]
    public void OFR1_member_signature_roundtrips_and_binds_the_roster()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys);
        var report = Report(roster, 1, keys[1].Seed, (1, 2, 100), (2, 3, 50));
        byte[] encoded = OriginFrontierProtocol.Sign(report, keys[1].Seed);
        var decoded = OriginFrontierProtocol.DecodeAndVerify(encoded, roster);

        Assert.Equal(report.MemberNodeId, decoded.MemberNodeId);
        Assert.Equal(report.MemberTransportEpoch, decoded.MemberTransportEpoch);
        Assert.Equal(report.ReportSequence, decoded.ReportSequence);
        Assert.Equal(2, decoded.Entries.Count);
        Assert.True(OriginFrontierProtocol.Verify(decoded, roster));

        encoded[^1] ^= 1;
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => OriginFrontierProtocol.DecodeAndVerify(encoded, roster));
    }

    [Fact]
    public void OFR1_rejects_joining_member_wrong_epoch_and_wrong_roster_digest()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys, new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
        var joiningReport = new AppliedOriginFrontierReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = roster.Members[1].NodeId,
            MemberTransportEpoch = roster.Members[1].TransportPeerEpoch,
            ReportSequence = 1,
            Entries = new[] { new AppliedOriginFrontierEntry
                { OriginAuthorNodeId = NodeId.From(Id(1)), OriginAuthorEpoch = 2, ContiguousAppliedOriginSequence = 10 } }
        };
        byte[] joiningBytes = OriginFrontierProtocol.Sign(joiningReport, keys[1].Seed);
        Assert.Throws<InvalidDataException>(() => OriginFrontierProtocol.DecodeAndVerify(joiningBytes, roster));

        var activeReport = Report(roster, 0, keys[0].Seed, (1, 2, 10));
        var wrongEpoch = new AppliedOriginFrontierReport
        {
            NamespaceId = activeReport.NamespaceId,
            RosterGeneration = activeReport.RosterGeneration,
            RosterDigest = activeReport.RosterDigest,
            MemberNodeId = activeReport.MemberNodeId,
            MemberTransportEpoch = activeReport.MemberTransportEpoch + 1,
            ReportSequence = activeReport.ReportSequence,
            Entries = activeReport.Entries,
            IssuedOrigins = activeReport.IssuedOrigins
        };
        byte[] wrongEpochBytes = OriginFrontierProtocol.Sign(wrongEpoch, keys[0].Seed);
        Assert.Throws<InvalidDataException>(() => OriginFrontierProtocol.DecodeAndVerify(wrongEpochBytes, roster));

        var wrongDigest = new AppliedOriginFrontierReport
        {
            NamespaceId = activeReport.NamespaceId,
            RosterGeneration = activeReport.RosterGeneration,
            RosterDigest = new byte[32],
            MemberNodeId = activeReport.MemberNodeId,
            MemberTransportEpoch = activeReport.MemberTransportEpoch,
            ReportSequence = activeReport.ReportSequence,
            Entries = activeReport.Entries
        };
        byte[] wrongDigestBytes = OriginFrontierProtocol.Sign(wrongDigest, keys[0].Seed);
        Assert.Throws<InvalidDataException>(() => OriginFrontierProtocol.DecodeAndVerify(wrongDigestBytes, roster));
    }

    [Fact]
    public void RCP1_safe_frontier_is_minimum_across_every_active_member_and_missing_means_zero()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys);
        var reports = new[]
        {
            Report(roster, 0, keys[0].Seed, (1, 2, 100), (2, 3, 20)),
            Report(roster, 1, keys[1].Seed, (1, 2, 90), (2, 3, 15)),
            Report(roster, 2, keys[2].Seed, (2, 3, 10))
        };
        var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[32], reports);
        var safe = Assert.Single(checkpoint.SafeFrontier);
        Assert.Equal(NodeId.From(Id(2)), safe.OriginAuthorNodeId);
        Assert.Equal(3u, safe.OriginAuthorEpoch);
        Assert.Equal(10UL, safe.SafeAppliedOriginSequence);

        var missingMember = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[32], reports.Take(2).ToArray());
        Assert.Empty(missingMember.SafeFrontier);
    }

    [Fact]
    public void RCP1_authority_signature_roundtrips_and_binds_full_replace_generation()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys);
        var reports = new[]
        {
            Report(roster, 0, keys[0].Seed, (2, 3, 10)),
            Report(roster, 1, keys[1].Seed, (2, 3, 12))
        };
        Guid generation = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        byte[] fullReplaceDigest = Enumerable.Range(1, 32).Select(x => (byte)x).ToArray();
        var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[32], reports, generation, fullReplaceDigest);
        byte[] encoded = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed);
        var trust = new TrustStore();
        trust.Allow(roster.AuthorityNodeId, keys[0].PublicKey, TrustMode.PreShared);

        var decoded = NamespaceRetentionCheckpointProtocol.DecodeAndVerify(encoded, roster, trust);

        Assert.Equal(generation, decoded.FullReplaceGeneration);
        Assert.Equal(fullReplaceDigest, decoded.FullReplaceDigest);
        Assert.Equal(10UL, Assert.Single(decoded.SafeFrontier).SafeAppliedOriginSequence);
        Assert.Equal(NamespaceRetentionCheckpointProtocol.ComputeDigest(checkpoint),
            NamespaceRetentionCheckpointProtocol.ComputeDigest(decoded));
    }

    [Fact]
    public void RCP1_rejects_checkpoint_replay_binding_and_signature_tampering()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys);
        var report = Report(roster, 0, keys[0].Seed, (1, 1, 5));
        var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, 1, new byte[32], new[] { report });
        byte[] encoded = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed);
        var trust = new TrustStore();
        trust.Allow(roster.AuthorityNodeId, keys[0].PublicKey, TrustMode.PreShared);

        encoded[^1] ^= 0x40;
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => NamespaceRetentionCheckpointProtocol.DecodeAndVerify(encoded, roster, trust));

        var wrongRoster = new NamespaceRosterCertificate
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = 2,
            PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = roster.Members
        };
        Assert.Throws<InvalidDataException>(() =>
            NamespaceRetentionCheckpointProtocol.DecodeAndVerify(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), wrongRoster, trust));
    }
    [Fact]
    public void FRA1_accepts_joining_member_installation_and_rejects_retired_member()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(keys, new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
        var ack = new FullReplaceInstalledAck
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            Generation = Guid.Parse("10213243-5465-7687-98a9-bacbdcedfe0f"),
            Digest = Enumerable.Range(33, 32).Select(x => (byte)x).ToArray(),
            MemberNodeId = roster.Members[1].NodeId,
            MemberTransportEpoch = roster.Members[1].TransportPeerEpoch,
            Installed = true
        };
        byte[] encoded = FullReplaceInstalledAckProtocol.Sign(ack, keys[1].Seed);
        var decoded = FullReplaceInstalledAckProtocol.DecodeAndVerify(encoded, roster);
        Assert.Equal(ack.Generation, decoded.Generation);
        Assert.Equal(ack.Digest, decoded.Digest);
        Assert.Equal(ack.MemberNodeId, decoded.MemberNodeId);

        var retiredRoster = Roster(keys, new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Retired });
        var retiredAck = new FullReplaceInstalledAck
        {
            NamespaceId = retiredRoster.NamespaceId,
            RosterGeneration = retiredRoster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(retiredRoster),
            Generation = ack.Generation,
            Digest = ack.Digest,
            MemberNodeId = retiredRoster.Members[1].NodeId,
            MemberTransportEpoch = retiredRoster.Members[1].TransportPeerEpoch,
            Installed = true
        };
        byte[] retiredEncoded = FullReplaceInstalledAckProtocol.Sign(retiredAck, keys[1].Seed);
        Assert.Throws<InvalidDataException>(() =>
            FullReplaceInstalledAckProtocol.DecodeAndVerify(retiredEncoded, retiredRoster));
    }

}
