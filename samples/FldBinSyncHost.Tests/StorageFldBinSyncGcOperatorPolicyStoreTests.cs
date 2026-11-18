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
                Binding(), expectedGeneration: 1, domain: FldBinSyncGcDomain.ChangeId);
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

    [Fact]
    public async Task Concurrent_policy_commits_from_independent_store_instances_have_one_winner()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-race-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 974);
            using var metadata = new StorageMetadataStore(storage);
            var firstStore = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var secondStore = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var (initial, initialAudit) = Next(1, 1, changeId: true);
            Assert.True(await firstStore.TryCommitAsync(0, initial, initialAudit));

            var (first, firstAudit) = Next(2, 2, changeId: true);
            first = first with { UpdatedBy = "operator-a", Reason = "concurrent update A" };
            firstAudit = firstAudit with { ActorId = "operator-a", Reason = "concurrent update A" };
            var (second, secondAudit) = Next(2, 2, receipt: true);
            second = second with { UpdatedBy = "operator-b", Reason = "concurrent update B" };
            secondAudit = secondAudit with { ActorId = "operator-b", Reason = "concurrent update B" };

            bool[] committed = await Task.WhenAll(
                firstStore.TryCommitAsync(1, first, firstAudit).AsTask(),
                secondStore.TryCommitAsync(1, second, secondAudit).AsTask());

            Assert.Single(committed, value => value);
            FldBinSyncGcOperatorPolicy persisted = Assert.IsType<FldBinSyncGcOperatorPolicy>(
                await firstStore.ReadAsync(Binding()));
            Assert.Equal(2UL, persisted.Generation);
            Assert.Equal(2, firstStore.ReadAudit(Binding()).Count);
            Assert.Contains(firstStore.ReadAudit(Binding()), entry =>
                entry.Sequence == 2 && entry.ActorId == persisted.UpdatedBy);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Cross_process_lease_owner_death_unblocks_policy_update_and_reacquisition()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-process-lease-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 977);
            using var metadata = new StorageMetadataStore(storage);
            var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var (initial, initialAudit) = Next(1, 1, changeId: true);
            Assert.True(await store.TryCommitAsync(0, initial, initialAudit));

            string leasePath = (string)typeof(StorageFldBinSyncGcOperatorPolicyStore)
                .GetMethod("LeasePath", System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Instance)!
                .Invoke(store, new object[] { Binding(), FldBinSyncGcDomain.ChangeId })!;
            string probePath = Path.Combine(AppContext.BaseDirectory,
                "lease-probe", "FldBinSyncHost.LeaseLockProbe.dll");
            Assert.True(File.Exists(probePath), $"Lease lock probe was not built: {probePath}");

            var start = new System.Diagnostics.ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
            start.ArgumentList.Add(probePath);
            start.ArgumentList.Add(leasePath);
            using System.Diagnostics.Process owner =
                System.Diagnostics.Process.Start(start) ??
                throw new InvalidOperationException("Could not start the lease owner process.");
            try
            {
                string? ready = await owner.StandardOutput.ReadLineAsync()
                    .WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Equal("READY", ready);

                var (replacement, replacementAudit) = Next(2, 2, changeId: true);
                replacement = replacement with { Reason = "cross-process lease release" };
                replacementAudit = replacementAudit with { Reason = "cross-process lease release" };
                Task<bool> update = store.TryCommitAsync(
                    1, replacement, replacementAudit).AsTask();
                await Task.Delay(200);
                Assert.False(update.IsCompleted,
                    "A policy update must wait while another process owns the domain lease.");

                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(await update.WaitAsync(TimeSpan.FromSeconds(5)));

                IAsyncDisposable? reacquired = await store.TryAcquireMaintenanceLeaseAsync(
                    Binding(), 2, FldBinSyncGcDomain.ChangeId);
                Assert.NotNull(reacquired);
                await reacquired!.DisposeAsync();
            }
            finally
            {
                if (!owner.HasExited)
                {
                    owner.Kill(entireProcessTree: true);
                    await owner.WaitForExitAsync();
                }
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Corrupt_persisted_policy_fails_closed_during_restore()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-corrupt-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 975);
            using var metadata = new StorageMetadataStore(storage);
            var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            byte[] key = (byte[])typeof(StorageFldBinSyncGcOperatorPolicyStore)
                .GetMethod("PolicyKey", System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Static)!
                .Invoke(null, new object[] { Binding() })!;
            using (StorageWriteBatch batch = storage.BeginWriteBatch())
            {
                batch.Put(key, new byte[] { 0x42, 0x53, 0x47, 0x43, 0x01 });
                batch.Commit();
            }

            using var runtime = new FldBinSyncGcMaintenanceControl();
            runtime.Start();
            runtime.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
            var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                Binding(), store, new AllowAllAuthorizer(), runtime);

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await coordinator.RestoreAsync());

            Assert.False(runtime.IsRunning);
            Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Corrupt_latest_audit_record_fails_closed_during_policy_restore()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-audit-corrupt-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 978);
            using var metadata = new StorageMetadataStore(storage);
            var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            var (policy, audit) = Next(1, 1, changeId: true);
            Assert.True(await store.TryCommitAsync(0, policy, audit));
            byte[] key = (byte[])typeof(StorageFldBinSyncGcOperatorPolicyStore)
                .GetMethod("AuditKey", System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Static)!
                .Invoke(null, new object[] { Binding(), 1UL })!;
            using (StorageWriteBatch batch = storage.BeginWriteBatch())
            {
                batch.Put(key, new byte[] { 0x00, 0x01 });
                batch.Commit();
            }

            using var runtime = new FldBinSyncGcMaintenanceControl();
            runtime.Start();
            runtime.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
            var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                Binding(), store, new AllowAllAuthorizer(), runtime);

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await coordinator.RestoreAsync());

            Assert.False(runtime.IsRunning);
            Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
            Assert.Throws<InvalidDataException>(() => store.ReadAudit(Binding()));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Closed_database_backup_restores_policy_and_audit_without_restoring_live_leases()
    {
        string root = Path.Combine(Path.GetTempPath(),
            "binsync-gc-policy-backup-" + Guid.NewGuid().ToString("N"));
        string source = Path.Combine(root, "source");
        string backup = Path.Combine(root, "backup");
        Directory.CreateDirectory(source);
        try
        {
            var (policy, audit) = Next(1, 1, receipt: true, changeId: true);
            using (var storage = StorageDatabase.Open(source, 976))
            using (var metadata = new StorageMetadataStore(storage))
            {
                var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
                Assert.True(await store.TryCommitAsync(0, policy, audit));
                IAsyncDisposable? lease = await store.TryAcquireMaintenanceLeaseAsync(
                    Binding(), 1, FldBinSyncGcDomain.ChangeId);
                Assert.NotNull(lease);
                await lease!.DisposeAsync();
            }

            CopyDirectory(source, backup);
            using (var restoredStorage = StorageDatabase.Open(backup, 976))
            using (var restoredMetadata = new StorageMetadataStore(restoredStorage))
            {
                var restoredStore = new StorageFldBinSyncGcOperatorPolicyStore(
                    restoredStorage, restoredMetadata);
                Assert.Equal(policy, await restoredStore.ReadAsync(Binding()));
                Assert.Equal(new[] { audit }, restoredStore.ReadAudit(Binding()));
                IAsyncDisposable? restoredLease = await restoredStore.TryAcquireMaintenanceLeaseAsync(
                    Binding(), 1, FldBinSyncGcDomain.ChangeId);
                Assert.NotNull(restoredLease);
                await restoredLease!.DisposeAsync();
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private sealed class AllowAllAuthorizer : IFldBinSyncGcOperatorAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(
            string actorId,
            FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(true);
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.EnumerateFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        foreach (string directory in Directory.EnumerateDirectories(source))
            CopyDirectory(directory, Path.Combine(destination, Path.GetFileName(directory)));
    }
}
