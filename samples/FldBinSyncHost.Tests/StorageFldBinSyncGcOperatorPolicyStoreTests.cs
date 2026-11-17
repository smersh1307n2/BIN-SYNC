using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class StorageFldBinSyncGcOperatorPolicyStoreTests
{
    private static FldBinSyncGcOperatorPolicyBinding Binding() =>
        new(Guid.Parse("c19f9d30-0f9a-4a5c-91d8-6f24e6c3d81b"), default, 17);

    private static (FldBinSyncGcOperatorPolicy Policy, FldBinSyncGcOperatorAuditEntry Audit) Next(
        ulong generation,
        ulong sequence,
        bool receipt = false,
        bool changeId = false,
        bool tombstone = false)
    {
        FldBinSyncGcOperatorPolicyBinding binding = Binding();
        var policy = new FldBinSyncGcOperatorPolicy(
            binding, generation, sequence, "operator-test", "test policy",
            true, receipt, changeId, tombstone);
        var audit = new FldBinSyncGcOperatorAuditEntry(
            binding, sequence, generation, "operator-test", "Update", "test policy",
            true, receipt, changeId, tombstone, DateTimeOffset.UtcNow);
        return (policy, audit);
    }

    [Fact]
    public async Task Policy_and_audit_commit_atomically_and_survive_database_reopen()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-durable-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var (policy, audit) = Next(1, 1, receipt: true, changeId: true);
            using (var storage = StorageDatabase.Open(directory, 971))
            using (var metadata = new StorageMetadataStore(storage))
            {
                var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
                Assert.True(await store.TryCommitAsync(0, policy, audit));
                Assert.Equal(policy, await store.ReadAsync(Binding()));
                Assert.Equal(new[] { audit }, store.ReadAudit(Binding()));
                Assert.False(await store.TryCommitAsync(0, policy, audit));
            }

            using (var storage = StorageDatabase.Open(directory, 971))
            using (var metadata = new StorageMetadataStore(storage))
            {
                var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
                Assert.Equal(policy, await store.ReadAsync(Binding()));
                Assert.Equal(new[] { audit }, store.ReadAudit(Binding()));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Policy_update_waits_for_matching_domain_lease_and_commits_after_release()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-lease-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 972);
            using var metadata = new StorageMetadataStore(storage);
            var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var (initial, initialAudit) = Next(1, 1, changeId: true);
            Assert.True(await store.TryCommitAsync(0, initial, initialAudit));

            IAsyncDisposable? lease = await store.TryAcquireMaintenanceLeaseAsync(
                Binding(), expectedGeneration: 1, FldBinSyncGcDomain.ChangeId);
            Assert.NotNull(lease);

            var (replacement, replacementAudit) = Next(2, 2, receipt: true);
            Task<bool> update = store.TryCommitAsync(1, replacement, replacementAudit).AsTask();
            await Task.Delay(150);
            Assert.False(update.IsCompleted);

            await lease!.DisposeAsync();
            Assert.True(await update.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(replacement, await store.ReadAsync(Binding()));
            Assert.Equal(new[] { initialAudit, replacementAudit }, store.ReadAudit(Binding()));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Disabled_or_stale_domain_cannot_acquire_a_maintenance_lease()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-disabled-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 973);
            using var metadata = new StorageMetadataStore(storage);
            var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var (policy, audit) = Next(1, 1, receipt: true);
            Assert.True(await store.TryCommitAsync(0, policy, audit));

            Assert.Null(await store.TryAcquireMaintenanceLeaseAsync(
                Binding(), 0, FldBinSyncGcDomain.Receipt));
            Assert.Null(await store.TryAcquireMaintenanceLeaseAsync(
                Binding(), 1, FldBinSyncGcDomain.ChangeId));
            IAsyncDisposable? lease = await store.TryAcquireMaintenanceLeaseAsync(
                Binding(), 1, FldBinSyncGcDomain.Receipt);
            Assert.NotNull(lease);
            await lease!.DisposeAsync();
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
