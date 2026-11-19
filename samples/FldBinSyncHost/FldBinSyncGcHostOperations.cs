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
    private int _nextMaintenanceSlice;

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
            FldBinSyncGcOperatorPolicy? confirmed =
                await _policyStore.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);
            if ((confirmed?.Generation ?? 0) != afterGeneration)
                continue;

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

    /// <summary>
    /// Run exactly one independently bounded GC domain per scheduler tick.
    /// Receipt and ChangeId pages may run while peers are connected. Tombstone
    /// replacement remains an idle-window operation because its final FLD
    /// checkpoint is an atomic namespace generation rewrite.
    /// </summary>
    public async Task<FldBinSyncAsyncGcPassResult> RunOneMaintenanceSliceAsync(
        int pageLimit = 256,
        int changeIdPageLimit = FldBinSyncAsyncHostAdapter.DefaultChangeIdGcPageLimit,
        Func<IDisposable?>? acquireTombstoneMaintenanceWindow = null,
        CancellationToken cancellationToken = default)
    {
        if (pageLimit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(pageLimit));
        if (changeIdPageLimit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(changeIdPageLimit));

        FldBinSyncAsyncGcReadinessReport readiness =
            await _adapter.GetGcReadinessAsync(_gcStore, _trustStore, cancellationToken)
                .ConfigureAwait(false);
        int start = (Interlocked.Increment(ref _nextMaintenanceSlice) - 1) % 3;
        for (int offset = 0; offset < 3; offset++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            switch ((start + offset) % 3)
            {
                case 0 when readiness.Receipts.ReadyForAttempt:
                {
                    int reclaimed = await _adapter.RunReceiptGcPassAsync(
                        _gcStore, _trustStore, pageLimit, cancellationToken).ConfigureAwait(false);
                    return new FldBinSyncAsyncGcPassResult(readiness, reclaimed, 0, 0);
                }
                case 1 when readiness.ChangeIds.ReadyForAttempt:
                {
                    int reclaimed = await _adapter.RunChangeIdGcPassAsync(
                        _gcStore, _trustStore, changeIdPageLimit, cancellationToken).ConfigureAwait(false);
                    return new FldBinSyncAsyncGcPassResult(readiness, 0, reclaimed, 0);
                }
                case 2 when readiness.Tombstones.ReadyForAttempt:
                {
                    using IDisposable? idleWindow = acquireTombstoneMaintenanceWindow?.Invoke();
                    if (acquireTombstoneMaintenanceWindow is not null && idleWindow is null)
                        break;

                    int reclaimed = await _adapter.RunTombstoneGcPassAsync(
                        _gcStore, _trustStore, pageLimit, cancellationToken).ConfigureAwait(false);
                    return new FldBinSyncAsyncGcPassResult(readiness, 0, 0, reclaimed);
                }
            }
        }

        return new FldBinSyncAsyncGcPassResult(readiness, 0, 0, 0);
    }

}
