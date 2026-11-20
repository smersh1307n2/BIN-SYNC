using System.Reflection;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncGcHostOperationsTests
{
    [Fact]
    public async Task Operator_surface_reports_independent_flags_policy_audit_and_durable_stop()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-operator-surface-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 981);
            NodeId replica = Replica(0x61);
            const int namespaceId = 41;
            Guid scope = Guid.NewGuid();
            var binding = new FldBinSyncGcOperatorPolicyBinding(scope, replica, namespaceId);

            using (var seedMetadata = new StorageMetadataStore(storage))
            using (var seedRuntime = new FldBinSyncGcMaintenanceControl())
            {
                var seedStore = new StorageFldBinSyncGcOperatorPolicyStore(storage, seedMetadata);
                var seedCoordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                    binding, seedStore, new AllowPolicyAuthorizer(), seedRuntime);
                FldBinSyncGcOperatorPolicy initial = await seedCoordinator.UpdateAsync(
                    new(binding, 0, "operator", "initialize all domains disabled",
                        MaintenanceStarted: false,
                        ReceiptEnabled: false,
                        ChangeIdEnabled: false,
                        TombstoneEnabled: false));
                Assert.Equal(1UL, initial.Generation);
            }

            IAsyncDataProvider provider =
                DispatchProxy.Create<IAsyncDataProvider, UnavailableProviderProxy>();
            IAsyncProviderRemoteBatchCommitStore remoteApply =
                DispatchProxy.Create<IAsyncProviderRemoteBatchCommitStore, UnavailableProviderProxy>();
            var node = new NodeBuilder().WithNodeId(replica).WithEpoch(1).Build();
            using var composition = FldBinSyncAsyncHostComposition.Create(
                storage, node, "operator-surface", scope, namespaceId,
                new TrustStore(), provider, remoteApply, _ => { },
                new AllowPolicyAuthorizer(),
                new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));

            FldBinSyncGcOperatorDiagnosticsSnapshot initialView =
                await composition.Operator.GetDiagnosticsAsync();
            Assert.Equal(1UL, initialView.Policy!.Generation);
            Assert.Single(initialView.RecentAudit);
            Assert.Equal("Update", initialView.RecentAudit[0].Action);
            Assert.False(initialView.ReceiptStartupProofEnabled);
            Assert.True(initialView.ChangeIdStartupProofEnabled);
            Assert.False(initialView.TombstoneStartupProofEnabled);
            Assert.False(initialView.RuntimeRunning);
            Assert.False(initialView.Readiness.ChangeIds.Enabled);

            FldBinSyncGcOperatorPolicy enabled =
                await composition.Operator.UpdatePolicyAsync(
                    expectedGeneration: 1,
                    actorId: "operator",
                    reason: "enable only the approved ChangeId canary",
                    maintenanceStarted: true,
                    receiptEnabled: false,
                    changeIdEnabled: true,
                    tombstoneEnabled: false);
            Assert.Equal(2UL, enabled.Generation);
            Assert.True(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.ChangeId));
            Assert.False(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Receipt));
            Assert.False(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Tombstone));

            Assert.Throws<InvalidOperationException>(() =>
                composition.Operator.UpdatePolicyAsync(
                    expectedGeneration: 2,
                    actorId: "operator",
                    reason: "attempt to override startup configuration",
                    maintenanceStarted: true,
                    receiptEnabled: true,
                    changeIdEnabled: true,
                    tombstoneEnabled: false));

            FldBinSyncGcOperatorPolicy stopped =
                await composition.Operator.EmergencyStopDurablyAsync(
                    "operator", "stop ChangeId canary");
            Assert.Equal(3UL, stopped.Generation);
            Assert.False(stopped.MaintenanceStarted);
            Assert.False(stopped.ReceiptEnabled);
            Assert.False(stopped.ChangeIdEnabled);
            Assert.False(stopped.TombstoneEnabled);

            FldBinSyncGcOperatorDiagnosticsSnapshot finalView =
                await composition.Operator.GetDiagnosticsAsync(auditLimit: 8);
            Assert.Equal(3UL, finalView.Policy!.Generation);
            Assert.Equal(new[] { "Update", "Update", "EmergencyStop" },
                finalView.RecentAudit.Select(entry => entry.Action));
            Assert.False(finalView.RuntimeRunning);
            Assert.False(finalView.ChangeIdRuntimeEnabled);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Host_startup_never_opens_application_work_before_recovery_and_latches_stop_on_failure()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-gc-host-startup-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 982);
            NodeId replica = Replica(0x62);
            const int namespaceId = 42;
            Guid scope = Guid.NewGuid();
            var binding = new FldBinSyncGcOperatorPolicyBinding(scope, replica, namespaceId);

            using (var seedMetadata = new StorageMetadataStore(storage))
            using (var seedRuntime = new FldBinSyncGcMaintenanceControl())
            {
                var seedStore = new StorageFldBinSyncGcOperatorPolicyStore(storage, seedMetadata);
                var seedCoordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                    binding, seedStore, new AllowPolicyAuthorizer(), seedRuntime);
                await seedCoordinator.UpdateAsync(new(
                    binding, 0, "operator", "test startup ordering",
                    MaintenanceStarted: false,
                    ReceiptEnabled: false,
                    ChangeIdEnabled: false,
                    TombstoneEnabled: false));
            }

            IAsyncDataProvider provider =
                DispatchProxy.Create<IAsyncDataProvider, UnavailableProviderProxy>();
            IAsyncProviderRemoteBatchCommitStore remoteApply =
                DispatchProxy.Create<IAsyncProviderRemoteBatchCommitStore, UnavailableProviderProxy>();
            var node = new NodeBuilder().WithNodeId(replica).WithEpoch(1).Build();
            using var composition = FldBinSyncAsyncHostComposition.Create(
                storage, node, "host-startup", scope, namespaceId,
                new TrustStore(), provider, remoteApply, _ => { },
                new AllowPolicyAuthorizer());

            bool applicationWorkStarted = false;
            await Assert.ThrowsAsync<NotSupportedException>(() =>
                composition.StartAsync(_ =>
                {
                    applicationWorkStarted = true;
                    return Task.CompletedTask;
                }));

            Assert.False(applicationWorkStarted);
            Assert.False(composition.StartupCompleted);
            Assert.False(composition.RuntimeControl.IsRunning);
            Assert.False(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Receipt));
            Assert.False(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.ChangeId));
            Assert.False(composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Tombstone));
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                composition.StartAsync(_ => Task.CompletedTask));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static NodeId Replica(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[^1] = value;
        return NodeId.From(bytes);
    }

    private sealed class AllowPolicyAuthorizer : IFldBinSyncGcOperatorAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(
            string actorId,
            FldBinSyncGcOperatorPolicyBinding binding,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(true);
    }

    public class UnavailableProviderProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            => throw new NotSupportedException(
                "The test provider intentionally does not expose Host recovery capabilities.");
    }
}
