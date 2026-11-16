using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageMetadataWriterAdmissionTests
{
    private static async Task WaitForWriterWaitersAsync(StorageDatabase storage, int expected)
    {
        var admission = StorageMetadataWriterAdmission.For(storage);
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (admission.WaitingWriterCount < expected && DateTime.UtcNow < deadline)
            await Task.Delay(1);
        Assert.Equal(expected, admission.WaitingWriterCount);
    }

    [Fact]
    public async Task Direct_public_metadata_writers_wait_for_gc_lease_across_store_instances()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-metadata-writer-admission-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 947);
            using var first = new StorageMetadataStore(storage);
            using var second = new StorageMetadataStore(storage);
            NodeId local = NodeId.From(new byte[]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
            });

            IDisposable gcLease = await first.AcquireGcWriterAdmissionAsync(
                CancellationToken.None);
            Task? firstWriter = null;
            Task? secondWriter = null;
            try
            {
                var firstStarted = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                var secondStarted = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                firstWriter = Task.Run(() =>
                {
                    firstStarted.SetResult();
                    first.EnableProviderPublicationCutover(
                        "writer-admission-test", local, 7, 0);
                });
                secondWriter = Task.Run(() =>
                {
                    secondStarted.SetResult();
                    second.EnableProviderPublicationCutover(
                        "writer-admission-test", local, 8, 0);
                });

                await Task.WhenAll(
                    firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                    secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)));
                await WaitForWriterWaitersAsync(storage, 2);

                Assert.False(firstWriter.IsCompleted);
                Assert.False(secondWriter.IsCompleted);
            }
            finally
            {
                gcLease.Dispose();
                if (firstWriter is not null && secondWriter is not null)
                    await Task.WhenAll(firstWriter, secondWriter)
                        .WaitAsync(TimeSpan.FromSeconds(5));
            }

            Assert.True(first.TryGetProviderPublicationCutover(
                "writer-admission-test", local, 7, out ulong firstWatermark));
            Assert.True(second.TryGetProviderPublicationCutover(
                "writer-admission-test", local, 8, out ulong secondWatermark));
            Assert.Equal(0UL, firstWatermark);
            Assert.Equal(0UL, secondWatermark);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Tombstone_commit_guard_holds_writer_admission_until_provider_delete_finishes()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-metadata-tombstone-guard-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 948);
            using var metadata = new StorageMetadataStore(storage);
            const string scope = "metadata-tombstone-guard";
            const int namespaceId = 7;
            NodeId local = NodeId.From(new byte[]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2
            });
            var gcStore = new StorageNamespaceGcStore(storage, scope, local);
            var trustStore = new TrustStore();
            metadata.AttachNamespaceGcGuard(gcStore, trustStore, scope, local);

            var guardAcquired = new TaskCompletionSource<IReadOnlySet<RecordId>?>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseGuard = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            Task guardOwner = Task.Run(() =>
            {
                using IDisposable commitGuard = metadata.AcquireTombstoneGcCommitGuard(
                    scope, local, namespaceId, gcStore, trustStore,
                    out IReadOnlySet<RecordId>? liveReferences);
                guardAcquired.SetResult(liveReferences);
                releaseGuard.Task.GetAwaiter().GetResult();
            });

            Task? writer = null;
            try
            {
                IReadOnlySet<RecordId>? liveReferences =
                    await guardAcquired.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(liveReferences);

                var writerStarted = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                writer = Task.Run(() =>
                {
                    writerStarted.SetResult();
                    metadata.EnableProviderPublicationCutover(scope, local, namespaceId, 0);
                });

                await writerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
                await WaitForWriterWaitersAsync(storage, 1);
                Assert.False(writer.IsCompleted);

                releaseGuard.SetResult();
                await Task.WhenAll(guardOwner, writer).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.True(metadata.TryGetProviderPublicationCutover(
                    scope, local, namespaceId, out ulong watermark));
                Assert.Equal(0UL, watermark);
            }
            finally
            {
                releaseGuard.TrySetResult();
                await guardOwner.WaitAsync(TimeSpan.FromSeconds(5));
                if (writer is not null)
                    await writer.WaitAsync(TimeSpan.FromSeconds(5));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
    [Fact]
    public async Task Namespace_roster_updates_wait_for_the_shared_metadata_gc_writer_lease()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-namespace-writer-admission-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 949);
            using var metadata = new StorageMetadataStore(storage);
            const string scope = "namespace-writer-admission";
            NodeId local = NodeId.From(new byte[]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3
            });
            var keyPair = BatchSignature.GenerateKeyPair();
            var trust = new TrustStore();
            NodeId authority = NodeId.From(new byte[]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4
            });
            trust.Allow(authority, keyPair.PublicKey, TrustMode.PreShared);
            var gcStore = new StorageNamespaceGcStore(storage, scope, local);

            var firstRoster = new NamespaceRosterCertificate
            {
                NamespaceId = 7,
                RosterGeneration = 1,
                PreviousRosterDigest = new byte[NamespaceRosterProtocol.DigestSize],
                AuthorityNodeId = authority,
                Members = new[]
                {
                    new NamespaceRosterMember
                    {
                        NodeId = authority,
                        PublicKey = keyPair.PublicKey,
                        TransportPeerEpoch = 1,
                        Status = NamespaceMemberStatus.Active
                    }
                }
            };
            firstRoster = gcStore.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(firstRoster, keyPair.Seed), trust);

            var nextRoster = new NamespaceRosterCertificate
            {
                NamespaceId = firstRoster.NamespaceId,
                RosterGeneration = 2,
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(firstRoster),
                AuthorityNodeId = authority,
                Members = firstRoster.Members
            };
            byte[] signedNextRoster = NamespaceRosterProtocol.Sign(nextRoster, keyPair.Seed);

            IDisposable gcLease = await metadata.AcquireGcWriterAdmissionAsync(
                CancellationToken.None);
            Task<NamespaceRosterCertificate>? rosterWriter = null;
            try
            {
                var writerStarted = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                rosterWriter = Task.Run(() =>
                {
                    writerStarted.SetResult();
                    return gcStore.AcceptNamespaceRoster(signedNextRoster, trust);
                });

                await writerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
                await WaitForWriterWaitersAsync(storage, 1);
                Assert.False(rosterWriter.IsCompleted);
            }
            finally
            {
                gcLease.Dispose();
                if (rosterWriter is not null)
                    await rosterWriter.WaitAsync(TimeSpan.FromSeconds(5));
            }

            Assert.Equal(2UL, (await rosterWriter!).RosterGeneration);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

}
