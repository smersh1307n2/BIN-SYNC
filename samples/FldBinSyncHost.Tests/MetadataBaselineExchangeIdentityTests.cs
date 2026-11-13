using BinSync;
using BinSync.FldHost;
using Xunit;

namespace FldBinSyncHost.Tests;

public sealed class MetadataBaselineExchangeIdentityTests
{
    [Fact]
    public void Installed_ack_produces_a_portable_exchange_identity_and_rejects_roster_mismatch()
    {
        byte[] localBytes = new byte[16];
        localBytes[15] = 1;
        NodeId local = NodeId.From(localBytes);
        var keyPair = BatchSignature.GenerateKeyPair();
        var trust = new TrustStore();
        trust.Allow(local, keyPair.PublicKey, TrustMode.PreShared);
        var proposed = new NamespaceRosterCertificate
        {
            NamespaceId = 17,
            RosterGeneration = 4,
            PreviousRosterDigest = new byte[NamespaceRosterProtocol.DigestSize],
            AuthorityNodeId = local,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = local,
                    PublicKey = keyPair.PublicKey,
                    TransportPeerEpoch = 9,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate accepted = NamespaceRosterProtocol.DecodeAndVerify(
            NamespaceRosterProtocol.Sign(proposed, keyPair.Seed), trust);
        byte[] snapshotDigest = Enumerable.Repeat((byte)0x5A,
            MetadataBaselineReportProtocol.DigestSize).ToArray();
        var installedAck = new FullReplaceInstalledAck
        {
            NamespaceId = accepted.NamespaceId,
            RosterGeneration = accepted.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(accepted),
            Generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            Digest = snapshotDigest.ToArray(),
            MemberNodeId = local,
            MemberTransportEpoch = 9,
            HlcWallFloor = 123,
            HlcLogicalFloor = 7,
            Installed = true
        };

        MetadataBaselineExchangeIdentity identity =
            MetadataBaselineExchangeIdentity.FromInstalledAck(accepted, installedAck);

        Assert.Equal(accepted.NamespaceId, identity.NamespaceId);
        Assert.Equal(accepted.RosterGeneration, identity.RosterGeneration);
        Assert.Equal(NamespaceRosterProtocol.ComputeDigest(accepted), identity.RosterDigest);
        Assert.Equal(installedAck.Generation, identity.Generation);
        Assert.Equal(snapshotDigest, identity.SnapshotDigest);
        installedAck.Digest[0] ^= 0xFF;
        Assert.Equal(snapshotDigest, identity.SnapshotDigest);

        var mismatchedAck = new FullReplaceInstalledAck
        {
            NamespaceId = installedAck.NamespaceId,
            RosterGeneration = installedAck.RosterGeneration + 1,
            RosterDigest = installedAck.RosterDigest,
            Generation = installedAck.Generation,
            Digest = snapshotDigest.ToArray(),
            MemberNodeId = installedAck.MemberNodeId,
            MemberTransportEpoch = installedAck.MemberTransportEpoch,
            HlcWallFloor = installedAck.HlcWallFloor,
            HlcLogicalFloor = installedAck.HlcLogicalFloor,
            Installed = true
        };
        Assert.Throws<InvalidDataException>(() =>
            MetadataBaselineExchangeIdentity.FromInstalledAck(accepted, mismatchedAck));
    }
}
