using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageFullReplaceSenderTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static (TrustStore Trust, NamespaceRosterCertificate Roster, byte[] Seed, NodeId Authority)
        CreateAuthority()
    {
        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        NodeId authority = NodeId.From(Id(1));
        var trust = new TrustStore();
        trust.Allow(authority, publicKey, TrustMode.PreShared);
        var unsigned = new NamespaceRosterCertificate
        {
            NamespaceId = 23,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = authority,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = authority,
                    PublicKey = publicKey,
                    TransportPeerEpoch = 7,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate roster = NamespaceRosterProtocol.DecodeAndVerify(
            NamespaceRosterProtocol.Sign(unsigned, seed), trust);
        return (trust, roster, seed, authority);
    }

    [Fact]
    public void Prepared_sender_snapshot_survives_provider_and_metadata_reopen_and_rejects_stale_roster()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-full-replace-sender-" + Guid.NewGuid().ToString("N"));
        const string scope = "full-replace-sender-restart";
        const int partition = 905;
        var (trust, roster, authoritySeed, authority) = CreateAuthority();
        Guid generation = Guid.Parse("90500000-0000-0000-0000-000000000001");
        Guid snapshotToken;
        byte[] digest;
        ulong watermark;
        ulong hlcWall;
        uint hlcLogical;

        try
        {
            using (var storage = StorageDatabase.Open(directory, partition))
            using (var provider = new StorageDataProvider(storage))
            using (var metadata = new StorageMetadataStore(storage))
            {
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = RecordId.From(Guid.Parse("90500000-0000-0000-0000-000000000010").ToByteArray()),
                    FieldId = 0x0923,
                    AuthorNodeId = authority,
                    OriginAuthorEpoch = 1,
                    HlcWall = 1200,
                    HlcLogical = 3,
                    Value = "sender-snapshot-row"u8.ToArray()
                });

                using PreparedFullReplaceSnapshot prepared = StorageFullReplaceSender.PrepareSnapshotAndPersist(
                    provider, trust, roster, authority, generation, authoritySeed,
                    metadata, scope, authority);
                Assert.Equal(23, prepared.NamespaceId);
                Assert.Equal(1, prepared.ExpectedEntryCount);
                Assert.Equal(1UL, prepared.SourceChangeFeedWatermark);
                Assert.Equal(1200UL, prepared.HlcWallFloor);
                Assert.Equal(3U, prepared.HlcLogicalFloor);
                digest = prepared.Digest;
                watermark = prepared.SourceChangeFeedWatermark;
                hlcWall = prepared.HlcWallFloor;
                hlcLogical = prepared.HlcLogicalFloor;

                Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                    scope, authority, 23, generation, out PreparedFullReplaceSnapshotState? state));
                Assert.NotNull(state);
                snapshotToken = state!.SnapshotToken;
                Assert.Equal(generation, state.Generation);
                Assert.Equal(watermark, state.SourceFeedWatermark);
                Assert.True(((IProviderDurableSnapshotStore)provider)
                    .TryGetPersistedSnapshotWatermark(snapshotToken, out ulong persistedWatermark));
                Assert.Equal(watermark, persistedWatermark);
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));

                // Persisted Dispose detaches only the process-local handle. The
                // provider generation and sender manifest remain durable for restart.
                prepared.Dispose();
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            using (var provider = new StorageDataProvider(reopened))
            using (var metadata = new StorageMetadataStore(reopened))
            {
                var staleRosterUnsigned = new NamespaceRosterCertificate
                {
                    NamespaceId = roster.NamespaceId,
                    RosterGeneration = 2,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    AuthorityNodeId = authority,
                    Members = roster.Members
                };
                NamespaceRosterCertificate staleRoster = NamespaceRosterProtocol.DecodeAndVerify(
                    NamespaceRosterProtocol.Sign(staleRosterUnsigned, authoritySeed), trust);

                Assert.Throws<InvalidOperationException>(() =>
                    StorageFullReplaceSender.RestorePreparedSnapshot(
                        provider, metadata, scope, authority, 23, generation,
                        trust, staleRoster, authority, authoritySeed));
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));

                using PreparedFullReplaceSnapshot restored = StorageFullReplaceSender.RestorePreparedSnapshot(
                    provider, metadata, scope, authority, 23, generation,
                    trust, roster, authority, authoritySeed);
                Assert.Equal(generation, restored.Generation);
                Assert.Equal(23, restored.NamespaceId);
                Assert.Equal(1, restored.ExpectedEntryCount);
                Assert.Equal(watermark, restored.SourceChangeFeedWatermark);
                Assert.Equal(hlcWall, restored.HlcWallFloor);
                Assert.Equal(hlcLogical, restored.HlcLogicalFloor);
                Assert.Equal(digest, restored.Digest);

                ProviderSnapshotPage page = provider.ReadSnapshot(snapshotToken, 0, 8);
                var row = Assert.Single(page.Entries);
                Assert.Equal("sender-snapshot-row", Encoding.UTF8.GetString(row.Value));
                Assert.True(page.IsComplete);

                StorageFullReplaceSender.ReleasePreparedSnapshot(
                    restored, provider, metadata, scope, authority);
                Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));
                Assert.False(metadata.TryGetPreparedFullReplaceSnapshot(
                    scope, authority, 23, generation, out _));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Prune_unreferenced_sender_snapshot_repairs_release_before_metadata_cleanup_crash_cut()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-full-replace-prune-" + Guid.NewGuid().ToString("N"));
        const string scope = "full-replace-sender-prune";
        const int partition = 906;
        var (trust, roster, authoritySeed, authority) = CreateAuthority();
        Guid generation = Guid.Parse("90600000-0000-0000-0000-000000000001");

        try
        {
            using var storage = StorageDatabase.Open(directory, partition);
            using var provider = new StorageDataProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(Guid.Parse("90600000-0000-0000-0000-000000000010").ToByteArray()),
                FieldId = 0x0923,
                AuthorNodeId = authority,
                OriginAuthorEpoch = 1,
                HlcWall = 1300,
                Value = "orphaned-sender-snapshot"u8.ToArray()
            });

            using PreparedFullReplaceSnapshot prepared = StorageFullReplaceSender.PrepareSnapshotAndPersist(
                provider, trust, roster, authority, generation, authoritySeed,
                metadata, scope, authority);
            Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out PreparedFullReplaceSnapshotState? state));
            Assert.NotNull(state);
            Guid token = state!.SnapshotToken;
            prepared.Dispose();

            // Simulate a crash after the provider token is released but before the
            // Metadata Store sender-progress row is deleted.
            provider.ReleaseSnapshot(token);
            Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out _));
            Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(token));

            StorageFullReplaceSender.PruneUnreferencedPreparedSnapshots(
                provider, metadata, scope, authority, 23);

            Assert.False(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out _));
            Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(token));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
