using System.Reflection;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncAsyncGcReadinessTests
{
    [Fact]
    public void Host_adapter_requires_durable_operator_policy_by_default()
    {
        var parameter = typeof(FldBinSyncAsyncHostAdapter).GetConstructors()
            .Single().GetParameters()
            .Single(parameter => parameter.Name == "requireDurableOperatorPolicy");
        Assert.Equal(true, parameter.DefaultValue);
    }

    [Fact]
    public async Task Startup_restores_durable_policy_before_recovery_and_fails_closed()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-startup-policy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 951);
            using var metadata = new StorageMetadataStore(storage);
            byte[] nodeBytes = new byte[16];
            nodeBytes[^1] = 0x51;
            NodeId localReplica = NodeId.From(nodeBytes);
            var node = new NodeBuilder().WithNodeId(localReplica).WithEpoch(1).Build();
            var coordinator = new AsyncStorageSyncCoordinator(node, metadata, "gc-startup-policy", 17);
            IAsyncDataProvider provider =
                DispatchProxy.Create<IAsyncDataProvider, ThrowIfStartupRecoveryRunsProxy>();
            IAsyncProviderRemoteBatchCommitStore remoteApply =
                DispatchProxy.Create<IAsyncProviderRemoteBatchCommitStore, ThrowIfStartupRecoveryRunsProxy>();

            // The production default must refuse startup when no durable policy
            // coordinator is supplied, before touching provider recovery.
            var missingPolicyAdapter = new FldBinSyncAsyncHostAdapter(
                coordinator, provider, remoteApply, _ => { });
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => missingPolicyAdapter.RecoverBeforeAcceptingSessionsAsync());

            // A configured but unavailable/corrupt policy store must also fail
            // before coordinator recovery can reach the provider.
            using var runtime = new FldBinSyncGcMaintenanceControl();
            var policyCoordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                new FldBinSyncGcOperatorPolicyBinding(Guid.NewGuid(), localReplica, 17),
                new UnavailablePolicyStore(),
                new AllowPolicyAuthorizer(),
                runtime);
            var adapter = new FldBinSyncAsyncHostAdapter(
                coordinator, provider, remoteApply, _ => { },
                gcPolicyCoordinator: policyCoordinator);
            Assert.Same(runtime, adapter.GcMaintenanceControl);
            using var mismatchedRuntime = new FldBinSyncGcMaintenanceControl();
            Assert.Throws<ArgumentException>(() => new FldBinSyncAsyncHostAdapter(
                coordinator, provider, remoteApply, _ => { },
                gcControl: mismatchedRuntime,
                gcPolicyCoordinator: policyCoordinator));
            await Assert.ThrowsAsync<IOException>(
                () => adapter.RecoverBeforeAcceptingSessionsAsync());
            Assert.False(runtime.IsRunning);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    public class ThrowIfStartupRecoveryRunsProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            => throw new InvalidOperationException(
                "Provider recovery must not run before durable GC policy restoration.");
    }

    private sealed class UnavailablePolicyStore : IFldBinSyncGcOperatorPolicyStore
    {
        public ValueTask<FldBinSyncGcOperatorPolicy?> ReadAsync(
            FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default)
            => ValueTask.FromException<FldBinSyncGcOperatorPolicy?>(
                new IOException("simulated durable policy store outage"));

        public ValueTask<bool> TryCommitAsync(
            ulong expectedGeneration,
            FldBinSyncGcOperatorPolicy policy,
            FldBinSyncGcOperatorAuditEntry auditEntry,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(false);

        public ValueTask<IAsyncDisposable?> TryAcquireMaintenanceLeaseAsync(
            FldBinSyncGcOperatorPolicyBinding binding,
            ulong expectedGeneration,
            FldBinSyncGcDomain domain,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult<IAsyncDisposable?>(null);
    }

    private sealed class AllowPolicyAuthorizer : IFldBinSyncGcOperatorAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(
            string actorId,
            FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(true);
    }

    [Fact]
    public void Disabled_domain_is_never_ready_even_when_prerequisites_are_satisfied()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "Receipt", Enabled: false, PrerequisitesSatisfied: true,
            BlockedReasons: Array.Empty<string>());

        Assert.True(readiness.PrerequisitesSatisfied);
        Assert.False(readiness.ReadyForAttempt);
    }

    [Fact]
    public void Enabled_domain_is_blocked_by_any_global_readiness_reason()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "ChangeId", Enabled: true, PrerequisitesSatisfied: false,
            BlockedReasons: new[] { "Host reports an unpublished local draft horizon." });

        Assert.False(readiness.ReadyForAttempt);
        Assert.Single(readiness.BlockedReasons);
    }

    [Fact]
    public void Enabled_domain_with_no_global_blockers_is_ready_only_for_candidate_evaluation()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "Record/Field Tombstone", Enabled: true, PrerequisitesSatisfied: true,
            BlockedReasons: Array.Empty<string>());

        Assert.True(readiness.ReadyForAttempt);
        // This type intentionally has no "safe to delete" or "enable GC" state:
        // each physical candidate still needs the coordinator's signed proof checks.
    }

    [Theory]
    [InlineData("No accepted, verifiable namespace roster is available.", FldBinSyncGcBlockerCode.SignedRosterUnavailable)]
    [InlineData("No accepted, verifiable retention checkpoint is available.", FldBinSyncGcBlockerCode.RetentionCheckpointUnavailable)]
    [InlineData("Durable startup Receipt GC option is disabled.", FldBinSyncGcBlockerCode.DurableFeatureDisabled)]
    [InlineData("Runtime ChangeId GC gate is stopped or this domain is disabled.", FldBinSyncGcBlockerCode.RuntimeGateDisabled)]
    [InlineData("Host reports one or more active immutable provider snapshot pins.", FldBinSyncGcBlockerCode.SnapshotPinsPresent)]
    [InlineData("No authoritative FULL_REPLACE generation/digest is covered by the accepted checkpoint.", FldBinSyncGcBlockerCode.FullReplaceBaselineUnavailable)]
    [InlineData("Durable GC operator policy coordinator is required but was not configured.", FldBinSyncGcBlockerCode.DurableOperatorPolicyUnavailable)]
    [InlineData("A future blocker message with no registered mapping.", FldBinSyncGcBlockerCode.Unclassified)]
    public void Blocker_messages_expose_stable_codes_without_losing_operator_detail(
        string message, FldBinSyncGcBlockerCode expectedCode)
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "ChangeId", Enabled: false, PrerequisitesSatisfied: false,
            BlockedReasons: new[] { message });

        FldBinSyncGcBlocker blocker = Assert.Single(readiness.Blockers);
        Assert.Equal(expectedCode, blocker.Code);
        Assert.Equal(message, blocker.Message);
    }

    [Fact]
    public void Blocker_projection_is_a_snapshot_and_preserves_multiple_reasons()
    {
        string[] reasons =
        {
            "Durable startup Receipt GC option is disabled.",
            "Runtime Receipt GC gate is stopped or this domain is disabled."
        };
        var readiness = new FldBinSyncGcDomainReadiness(
            "Receipt", Enabled: false, PrerequisitesSatisfied: false, reasons);

        FldBinSyncGcBlocker[] blockers = readiness.Blockers.ToArray();

        Assert.Equal(2, blockers.Length);
        Assert.Equal(FldBinSyncGcBlockerCode.DurableFeatureDisabled, blockers[0].Code);
        Assert.Equal(FldBinSyncGcBlockerCode.RuntimeGateDisabled, blockers[1].Code);
        Assert.Equal(reasons, readiness.BlockedReasons);
    }

    [Fact]
    public void Change_id_gc_host_entry_points_default_to_4096_while_other_domains_stay_at_256()
    {
        static object? DefaultOf(string methodName, string parameterName)
        {
            var method = typeof(FldBinSyncAsyncHostAdapter).GetMethods()
                .Single(method => method.Name == methodName);
            return method.GetParameters()
                .Single(parameter => parameter.Name == parameterName)
                .DefaultValue;
        }

        Assert.Equal(4096, FldBinSyncAsyncHostAdapter.DefaultChangeIdGcPageLimit);
        Assert.Equal(4096, DefaultOf(nameof(FldBinSyncAsyncHostAdapter.RunChangeIdGcPassAsync), "limit"));
        Assert.Equal(4096, DefaultOf(nameof(FldBinSyncAsyncHostAdapter.RunGcMaintenanceAsync), "changeIdLimit"));
        Assert.Equal(4096, DefaultOf(nameof(FldBinSyncAsyncHostAdapter.RunMetadataGcPassAsync), "changeIdLimit"));
        Assert.Equal(256, DefaultOf(nameof(FldBinSyncAsyncHostAdapter.RunGcMaintenanceAsync), "limit"));
        Assert.Equal(256, DefaultOf(nameof(FldBinSyncAsyncHostAdapter.RunMetadataGcPassAsync), "limit"));
    }
}
