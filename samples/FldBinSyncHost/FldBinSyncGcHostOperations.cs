using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

/// <summary>
/// Immutable operator view. Startup proof flags, durable policy intent, runtime gates
/// and readiness are shown separately so an operator cannot mistake one for another.
/// </summary>
public sealed record FldBinSyncGcOperatorDiagnosticsSnapshot(
    DateTimeOffset CapturedAtUtc,
    FldBinSyncGcOperatorPolicy? Policy,
    IReadOnlyList<FldBinSyncGcOperatorAuditEntry> RecentAudit,
    FldBinSyncAsyncGcReadinessReport Readiness,
    bool ReceiptStartupProofEnabled,
    bool ChangeIdStartupProofEnabled,
    bool TombstoneStartupProofEnabled,
    bool RuntimeRunning,
    bool ReceiptRuntimeEnabled,
    bool ChangeIdRuntimeEnabled,
    bool TombstoneRuntimeEnabled);

/// <summary>
/// Application-facing operator surface for one composed namespace. Authentication
/// remains the responsibility of the supplied IFldBinSyncGcOperatorAuthorizer and
/// the application endpoint that calls this service.
/// </summary>
public sealed class FldBinSyncGcHostOperations
{
    private readonly FldBinSyncAsyncHostAdapter _adapter;
    private readonly StorageNamespaceGcStore _gcStore;
    private readonly TrustStore _trustStore;
    private readonly StorageFldBinSyncGcOperatorPolicyStore _policyStore;
    private readonly FldBinSyncGcOperatorPolicyCoordinator _policyCoordinator;
    private readonly FldBinSyncGcMaintenanceControl _runtime;
    private readonly FldBinSyncGcOperatorPolicyBinding _binding;

    internal FldBinSyncGcHostOperations(
        FldBinSyncAsyncHostAdapter adapter,
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        StorageFldBinSyncGcOperatorPolicyStore policyStore,
        FldBinSyncGcOperatorPolicyCoordinator policyCoordinator,
        FldBinSyncGcMaintenanceControl runtime,
        FldBinSyncGcOperatorPolicyBinding binding)
    {
        _adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        _gcStore = gcStore ?? throw new ArgumentNullException(nameof(gcStore));
        _trustStore = trustStore ?? throw new ArgumentNullException(nameof(trustStore));
        _policyStore = policyStore ?? throw new ArgumentNullException(nameof(policyStore));
        _policyCoordinator = policyCoordinator ?? throw new ArgumentNullException(nameof(policyCoordinator));
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        _binding = binding ?? throw new ArgumentNullException(nameof(binding));
    }

    /// <summary>
    /// Return a read-only, generation-consistent policy/audit snapshot together with
    /// current readiness. A concurrent policy change causes a bounded retry, not a
    /// mixed-generation operator view.
    /// </summary>
    public async Task<FldBinSyncGcOperatorDiagnosticsSnapshot> GetDiagnosticsAsync(
        int auditLimit = 64,
        CancellationToken cancellationToken = default)
    {
        if (auditLimit is < 1 or > 256)
            throw new ArgumentOutOfRangeException(nameof(auditLimit));

        for (int attempt = 0; attempt < 3; attempt++)
        {
            FldBinSyncGcOperatorPolicy? before =
                await _policyStore.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);
            FldBinSyncAsyncGcReadinessReport readiness =
                await _adapter.GetGcReadinessAsync(_gcStore, _trustStore, cancellationToken)
                    .ConfigureAwait(false);
            FldBinSyncGcOperatorPolicy? after =
                await _policyStore.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);

            ulong beforeGeneration = before?.Generation ?? 0;
            ulong afterGeneration = after?.Generation ?? 0;
            if (beforeGeneration != afterGeneration)
                continue;

            ulong afterSequence = after is null || after.AuditSequence <= (ulong)auditLimit
                ? 0
                : after.AuditSequence - (ulong)auditLimit;
            IReadOnlyList<FldBinSyncGcOperatorAuditEntry> audit =
                _policyStore.ReadAudit(_binding, afterSequence, auditLimit);

            return new FldBinSyncGcOperatorDiagnosticsSnapshot(
                DateTimeOffset.UtcNow,
                after,
                audit,
                readiness,
                _gcStore.IsReceiptGcProofEnabled,
                _gcStore.IsChangeIdGcProofEnabled,
                _gcStore.IsTombstoneGcProofEnabled,
                _runtime.IsRunning,
                _runtime.IsEnabled(FldBinSyncGcDomain.Receipt),
                _runtime.IsEnabled(FldBinSyncGcDomain.ChangeId),
                _runtime.IsEnabled(FldBinSyncGcDomain.Tombstone));
        }

        throw new InvalidOperationException(
            "GC operator policy changed repeatedly while diagnostics were being collected; retry the read.");
    }

    /// <summary>
    /// Apply an independently selected per-domain operator policy. Startup proof
    /// switches are an upper bound and cannot be enabled or overridden at runtime.
    /// </summary>
    public ValueTask<FldBinSyncGcOperatorPolicy> UpdatePolicyAsync(
        ulong expectedGeneration,
        string actorId,
        string reason,
        bool maintenanceStarted,
        bool receiptEnabled,
        bool changeIdEnabled,
        bool tombstoneEnabled,
        CancellationToken cancellationToken = default)
    {
        if (receiptEnabled && !_gcStore.IsReceiptGcProofEnabled)
            throw new InvalidOperationException("Receipt GC is disabled by startup proof configuration.");
        if (changeIdEnabled && !_gcStore.IsChangeIdGcProofEnabled)
            throw new InvalidOperationException("ChangeId GC is disabled by startup proof configuration.");
        if (tombstoneEnabled && !_gcStore.IsTombstoneGcProofEnabled)
            throw new InvalidOperationException("Tombstone GC is disabled by startup proof configuration.");

        return _policyCoordinator.UpdateAsync(new FldBinSyncGcOperatorPolicyRequest(
            _binding,
            expectedGeneration,
            actorId,
            reason,
            maintenanceStarted,
            receiptEnabled,
            changeIdEnabled,
            tombstoneEnabled), cancellationToken);
    }

    /// <summary>Immediate local brake, independent of durable-store availability.</summary>
    public void EmergencyStop() => _policyCoordinator.EmergencyStop();

    /// <summary>
    /// Immediate local stop followed by an audited, durable all-domains-disabled
    /// policy commit. Failure leaves the local emergency latch set.
    /// </summary>
    public ValueTask<FldBinSyncGcOperatorPolicy> EmergencyStopDurablyAsync(
        string actorId,
        string reason,
        CancellationToken cancellationToken = default)
        => _policyCoordinator.EmergencyStopDurablyAsync(actorId, reason, cancellationToken);

    /// <summary>Execute one bounded round through the adapter's durable per-domain leases.</summary>
    public Task<FldBinSyncAsyncGcPassResult> RunOneMaintenanceRoundAsync(
        int pageLimit = 256,
        int changeIdPageLimit = FldBinSyncAsyncHostAdapter.DefaultChangeIdGcPageLimit,
        CancellationToken cancellationToken = default)
        => _adapter.RunGcMaintenanceAsync(
            _gcStore, _trustStore, pageLimit, cancellationToken, changeIdPageLimit);
}
