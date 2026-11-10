using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageNamespaceGcStoreTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate Roster(
        int generation, byte[] previousDigest,
        (byte[] Seed, byte[] PublicKey)[] keys,
        NamespaceMemberStatus[]? statuses = null)
    {
        statuses ??= Enumerable.Repeat(NamespaceMemberStatus.Active, keys.Length).ToArray();
        var members = new NamespaceRosterMember[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            members[i] = new NamespaceRosterMember
            {
                NodeId = NodeId.From(Id((byte)(i + 1))),
                PublicKey = keys[i].PublicKey,
                TransportPeerEpoch = 10u + (uint)i,
                Status = statuses[i],
                RetiredThroughEpoch = statuses[i] == NamespaceMemberStatus.Retired ? 9u : 0u
            };
        }
        return new NamespaceRosterCertificate
        {
            NamespaceId = 19,
            RosterGeneration = (ulong)generation,
            PreviousRosterDigest = previousDigest,
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = members
        };
    }

    private static AppliedOriginFrontierReport Report(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, ulong reportSequence, ulong appliedSequence)
        => new()
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            ReportSequence = reportSequence,
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
        (byte[] Seed, byte[] PublicKey)[] keys, ulong reportSequence, ulong appliedSequence)
        => OriginFrontierProtocol.Sign(Report(roster, memberIndex, keys, reportSequence, appliedSequence),
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
            Installed = true
        }, keys[memberIndex].Seed);

    [Fact]
    public void Signed_roster_frontiers_acks_and_checkpoint_survive_restart_and_prove_only_the_safe_horizon()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-gc-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        byte[] rosterBytes = NamespaceRosterProtocol.Sign(roster, keys[0].Seed);
        Guid generation = Guid.Parse("12345678-1234-5678-9abc-def012345678");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("authoritative snapshot"));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 901))
            {
                var gc = new StorageNamespaceGcStore(storage, "fld-db/main", NodeId.From(Id(99)));
                roster = gc.AcceptNamespaceRoster(rosterBytes, trust);

                var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
                var report2 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, replaceDigest), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, replaceDigest), roster);

                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], new[] { report1, report2 }, generation, replaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                byte[] checkpointBytes = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed);
                gc.AcceptRetentionCheckpoint(checkpointBytes, roster, trust);

                Assert.True(gc.AreAllActiveMembersInstalled(roster, generation, replaceDigest));
                Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, NodeId.From(Id(2)), 3, 90));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, NodeId.From(Id(2)), 3, 91));
                Assert.True(gc.HasProtocolTombstoneGcProof(
                    roster, trust, NodeId.From(Id(2)), 3, 90, 0, 0, generation, replaceDigest));
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    roster, trust, NodeId.From(Id(2)), 3, 91, 0, 0, generation, replaceDigest));
            }

            using (var reopened = StorageDatabase.Open(directory, 901))
            {
                var gc = new StorageNamespaceGcStore(reopened, "fld-db/main", NodeId.From(Id(99)));
                Assert.True(gc.TryReadNamespaceRoster(19, trust, out var restoredRoster));
                Assert.NotNull(restoredRoster);
                Assert.Equal(1UL, restoredRoster!.RosterGeneration);
                Assert.True(gc.TryReadRetentionCheckpoint(restoredRoster, trust, out var restoredCheckpoint));
                Assert.NotNull(restoredCheckpoint);
                Assert.Equal(2, gc.ReadOriginFrontiers(restoredRoster).Count);
                Assert.Equal(2, gc.ReadFullReplaceInstalledAcks(restoredRoster, generation).Count);
                Assert.True(gc.HasProtocolTombstoneGcProof(
                    restoredRoster, trust, NodeId.From(Id(2)), 3, 90, 0, 0, generation, replaceDigest));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Joining_member_cannot_become_active_until_current_full_replace_ack_is_durable()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-join-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster1 = Roster(1, new byte[32], keys,
            new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("join baseline"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 902);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/join", NodeId.From(Id(99)));
            roster1 = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster1, keys[0].Seed), trust);
            var report = gc.AcceptOriginFrontier(SignedReport(roster1, 0, keys, 1, 50), roster1);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster1, 0, keys, generation, replaceDigest), roster1);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster1, 1, new byte[32], new[] { report }, generation, replaceDigest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster1, trust);

            var roster2 = Roster(2, NamespaceRosterProtocol.ComputeDigest(roster1), keys,
                new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Active });
            byte[] roster2Bytes = NamespaceRosterProtocol.Sign(roster2, keys[0].Seed);
            Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(roster2Bytes, trust));

            gc.AcceptFullReplaceInstalledAck(SignedAck(roster1, 1, keys, generation, replaceDigest), roster1);
            var accepted = gc.AcceptNamespaceRoster(roster2Bytes, trust);
            Assert.Equal(2UL, accepted.RosterGeneration);
            Assert.True(gc.TryReadNamespaceRoster(19, trust, out var current));
            Assert.Equal(2UL, current!.RosterGeneration);
            Assert.Empty(gc.ReadOriginFrontiers(current));
            Assert.False(gc.AreAllActiveMembersInstalled(current, generation, replaceDigest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_rejects_frontier_regression_checkpoint_overclaim_and_roster_fork()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-fence-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);

        try
        {
            using var storage = StorageDatabase.Open(directory, 903);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/fence", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);

            gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 2, 89), roster));

            var reports = gc.ReadOriginFrontiers(roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], reports);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            var overclaim = new NamespaceRetentionCheckpoint
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                CheckpointSequence = 2,
                PreviousCheckpointDigest = NamespaceRetentionCheckpointProtocol.ComputeDigest(checkpoint),
                SafeFrontier = new[]
                {
                    new RetentionFrontierEntry
                    {
                        OriginAuthorNodeId = NodeId.From(Id(9)),
                        OriginAuthorEpoch = 3,
                        SafeAppliedOriginSequence = 100
                    }
                }
            };
            Assert.Throws<InvalidDataException>(() => gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(overclaim, keys[0].Seed), roster, trust));

            var fork = Roster(1, new byte[32], keys,
                new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(fork, keys[0].Seed), trust));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
