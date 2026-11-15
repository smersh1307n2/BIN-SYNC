using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageMetadataWriterAdmissionTests
{
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

            using IDisposable gcLease = await first.AcquireGcWriterAdmissionAsync(
                CancellationToken.None);

            var firstStarted = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var secondStarted = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);

            Task firstWriter = Task.Run(() =>
            {
                firstStarted.SetResult();
                first.EnableProviderPublicationCutover(
                    "writer-admission-test", local, 7, 0);
            });
            Task secondWriter = Task.Run(() =>
            {
                secondStarted.SetResult();
                second.EnableProviderPublicationCutover(
                    "writer-admission-test", local, 8, 0);
            });

            await Task.WhenAll(
                firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            Assert.False(firstWriter.IsCompleted);
            Assert.False(secondWriter.IsCompleted);

            gcLease.Dispose();

            await Task.WhenAll(firstWriter, secondWriter)
                .WaitAsync(TimeSpan.FromSeconds(5));
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

            using IDisposable commitGuard = metadata.AcquireTombstoneGcCommitGuard(
                scope, local, namespaceId, gcStore, trustStore,
                out IReadOnlySet<RecordId>? liveReferences);
            Assert.NotNull(liveReferences);

            var started = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            Task writer = Task.Run(() =>
            {
                started.SetResult();
                metadata.EnableProviderPublicationCutover(scope, local, namespaceId, 0);
            });
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(writer.IsCompleted);

            commitGuard.Dispose();
            await writer.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(metadata.TryGetProviderPublicationCutover(
                scope, local, namespaceId, out ulong watermark));
            Assert.Equal(0UL, watermark);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

}
