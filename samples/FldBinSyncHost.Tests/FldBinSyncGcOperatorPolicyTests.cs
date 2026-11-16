using BinSync.FldHost;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncGcOperatorPolicyTests
{
    private static FldBinSyncGcOperatorPolicyBinding Binding() =>
        new(Guid.Parse("c19f9d30-0f9a-4a5c-91d8-6f24e6c3d81b"), default, 17);

    [Fact]
    public async Task Policy_is_audited_before_runtime_domains_are_opened_and_restored_independently()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);

        FldBinSyncGcOperatorPolicy policy = await coordinator.UpdateAsync(
            new(Binding(), 0, "operator-1", "approved maintenance window",
                MaintenanceStarted: true, ReceiptEnabled: false,
                ChangeIdEnabled: true, TombstoneEnabled: false));

        Assert.Equal(1UL, policy.Generation);
        Assert.Equal(1UL, policy.AuditSequence);
        Assert.Single(store.Audit);
        Assert.True(runtime.IsRunning);
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.True(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.Tombstone));

        runtime.Stop();
        FldBinSyncGcOperatorPolicy? restored = await coordinator.RestoreAsync();
        Assert.Equal(policy, restored);
        Assert.True(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));
    }

    [Fact]
    public async Task Stale_generation_is_rejected_without_mutating_runtime_or_audit()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);
        await coordinator.UpdateAsync(new(Binding(), 0, "operator-1", "enable receipt",
            true, true, false, false));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await coordinator.UpdateAsync(new(Binding(), 0, "operator-1", "replay stale request",
                true, false, true, false)));

        Assert.Single(store.Audit);
        Assert.True(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
    }

    [Fact]
    public async Task Audit_store_failure_fails_closed_and_emergency_stop_does_not_need_store()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);
        await coordinator.UpdateAsync(new(Binding(), 0, "operator-1", "enable receipt",
            true, true, false, false));
        store.ThrowOnRead = true;

        await Assert.ThrowsAsync<IOException>(async () => await coordinator.RestoreAsync());
        Assert.False(runtime.IsRunning);
        store.ThrowOnRead = false;
        await coordinator.RestoreAsync();
        Assert.True(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));

        store.ThrowOnRead = true;
        coordinator.EmergencyStop();
        Assert.False(runtime.IsRunning);
    }

    [Fact]
    public async Task Unauthorized_or_wrong_binding_request_is_rejected()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var denied = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new DenyAuthorizer(), runtime);
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await denied.UpdateAsync(new(Binding(), 0, "intruder", "try enable",
                true, true, true, true)));
        Assert.Empty(store.Audit);
        Assert.False(runtime.IsRunning);

        var allowed = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);
        var wrongBinding = Binding() with { NamespaceId = 18 };
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await allowed.UpdateAsync(new(wrongBinding, 0, "operator-1", "wrong namespace",
                true, true, false, false)));
        Assert.Empty(store.Audit);
    }


    [Fact]
    public async Task Cross_process_generation_conflict_stops_stale_local_runtime()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);
        await coordinator.UpdateAsync(new(Binding(), 0, "operator-1", "enable receipt",
            true, true, false, false));
        store.ForceConflict = true;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await coordinator.UpdateAsync(new(Binding(), 1, "operator-1", "change domains",
                true, false, true, false)));

        Assert.False(runtime.IsRunning);
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.Empty(store.Audit.Skip(1));
    }


    [Fact]
    public async Task Emergency_stop_wins_against_policy_restore_that_is_applying()
    {
        var store = new TestPolicyStore();
        using var runtime = new FldBinSyncGcMaintenanceControl();
        var coordinator = new FldBinSyncGcOperatorPolicyCoordinator(
            Binding(), store, new AllowAllAuthorizer(), runtime);
        await coordinator.UpdateAsync(new(Binding(), 0, "operator-1", "enable receipt",
            true, true, false, false));

        using var oldToken = runtime.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Receipt);
        Assert.NotNull(oldToken);
        var callbackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = oldToken!.Token.Register(() =>
        {
            callbackEntered.TrySetResult();
            releaseCallback.Task.GetAwaiter().GetResult();
        });

        Task<FldBinSyncGcOperatorPolicy?> restoring = coordinator.RestoreAsync().AsTask();
        await callbackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        coordinator.EmergencyStop();
        releaseCallback.TrySetResult();
        await restoring.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(runtime.IsRunning);
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.False(runtime.IsEnabled(FldBinSyncGcDomain.ChangeId));
    }

    private sealed class TestPolicyStore : IFldBinSyncGcOperatorPolicyStore
    {
        public FldBinSyncGcOperatorPolicy? Current { get; private set; }
        public List<FldBinSyncGcOperatorAuditEntry> Audit { get; } = new();
        public bool ThrowOnRead { get; set; }
        public bool ForceConflict { get; set; }

        public ValueTask<FldBinSyncGcOperatorPolicy?> ReadAsync(
            FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default)
        {
            if (ThrowOnRead) throw new IOException("simulated audit-store outage");
            return ValueTask.FromResult(Current);
        }

        public ValueTask<bool> TryCommitAsync(
            ulong expectedGeneration,
            FldBinSyncGcOperatorPolicy policy,
            FldBinSyncGcOperatorAuditEntry auditEntry,
            CancellationToken cancellationToken = default)
        {
            if (ForceConflict || (Current?.Generation ?? 0) != expectedGeneration)
                return ValueTask.FromResult(false);
            Assert.Equal(policy.Generation, auditEntry.PolicyGeneration);
            Assert.Equal(policy.AuditSequence, auditEntry.Sequence);
            Current = policy;
            Audit.Add(auditEntry);
            return ValueTask.FromResult(true);
        }
    }

    private sealed class AllowAllAuthorizer : IFldBinSyncGcOperatorAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(
            string actorId, FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default) => ValueTask.FromResult(true);
    }

    private sealed class DenyAuthorizer : IFldBinSyncGcOperatorAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(
            string actorId, FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default) => ValueTask.FromResult(false);
    }
}
