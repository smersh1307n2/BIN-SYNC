namespace BinSync.FldHost;

/// <summary>
/// Immutable identity binding for one operator-controlled GC policy. The store key
/// and every committed policy/audit record must use this complete binding.
/// </summary>
public sealed record FldBinSyncGcOperatorPolicyBinding(
    Guid MetadataScopeId,
    NodeId LocalReplica,
    int NamespaceId);

/// <summary>Persisted per-domain operator intent. It can restrict, never override, startup flags.</summary>
public sealed record FldBinSyncGcOperatorPolicy(
    FldBinSyncGcOperatorPolicyBinding Binding,
    ulong Generation,
    ulong AuditSequence,
    string UpdatedBy,
    string Reason,
    bool MaintenanceStarted,
    bool ReceiptEnabled,
    bool ChangeIdEnabled,
    bool TombstoneEnabled);

/// <summary>Authenticated request to replace the operator intent at an expected generation.</summary>
public sealed record FldBinSyncGcOperatorPolicyRequest(
    FldBinSyncGcOperatorPolicyBinding Binding,
    ulong ExpectedGeneration,
    string ActorId,
    string Reason,
    bool MaintenanceStarted,
    bool ReceiptEnabled,
    bool ChangeIdEnabled,
    bool TombstoneEnabled);

/// <summary>Append-only audit event committed atomically with its resulting policy.</summary>
public sealed record FldBinSyncGcOperatorAuditEntry(
    FldBinSyncGcOperatorPolicyBinding Binding,
    ulong Sequence,
    ulong PolicyGeneration,
    string ActorId,
    string Action,
    string Reason,
    bool MaintenanceStarted,
    bool ReceiptEnabled,
    bool ChangeIdEnabled,
    bool TombstoneEnabled,
    DateTimeOffset RecordedAtUtc);

/// <summary>
/// Durable implementation must atomically compare the current generation, persist
/// the replacement policy and append its audit event. A false return means conflict;
/// exceptions mean no success may be assumed. Reads must reject corruption.
/// </summary>
public interface IFldBinSyncGcOperatorPolicyStore
{
    ValueTask<FldBinSyncGcOperatorPolicy?> ReadAsync(
        FldBinSyncGcOperatorPolicyBinding binding,
        CancellationToken cancellationToken = default);

    ValueTask<bool> TryCommitAsync(
        ulong expectedGeneration,
        FldBinSyncGcOperatorPolicy policy,
        FldBinSyncGcOperatorAuditEntry auditEntry,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically verify current generation and enabled domain, then hold a durable
    /// per-domain lease until disposed. Policy updates that disable/change the leased
    /// domain must wait for all such leases to drain.
    /// </summary>
    ValueTask<IAsyncDisposable?> TryAcquireMaintenanceLeaseAsync(
        FldBinSyncGcOperatorPolicyBinding binding,
        ulong expectedGeneration,
        FldBinSyncGcDomain domain,
        CancellationToken cancellationToken = default);
}

/// <summary>Application-owned authentication/authorization boundary for operator actions.</summary>
public interface IFldBinSyncGcOperatorAuthorizer
{
    ValueTask<bool> AuthorizeAsync(
        string actorId,
        FldBinSyncGcOperatorPolicyBinding binding,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Applies durable operator intent to the process-local brake. Policy is committed
/// before opening gates; any failure while applying it closes the runtime control.
/// EmergencyStop never consults the durable store and therefore remains available
/// during audit-store outages.
/// </summary>
public sealed class FldBinSyncGcOperatorPolicyCoordinator
{
    private readonly FldBinSyncGcOperatorPolicyBinding _binding;
    private readonly IFldBinSyncGcOperatorPolicyStore _store;
    private readonly IFldBinSyncGcOperatorAuthorizer _authorizer;
    private readonly FldBinSyncGcMaintenanceControl _runtime;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private long _emergencyStopVersion;

    public FldBinSyncGcOperatorPolicyCoordinator(
        FldBinSyncGcOperatorPolicyBinding binding,
        IFldBinSyncGcOperatorPolicyStore store,
        IFldBinSyncGcOperatorAuthorizer authorizer,
        FldBinSyncGcMaintenanceControl runtime)
    {
        _binding = binding ?? throw new ArgumentNullException(nameof(binding));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _authorizer = authorizer ?? throw new ArgumentNullException(nameof(authorizer));
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        if (binding.MetadataScopeId == Guid.Empty || binding.NamespaceId < 0)
            throw new ArgumentException("A non-empty Metadata scope and non-negative namespace are required.", nameof(binding));
    }

    /// <summary>
    /// Load and validate the persisted policy. Missing, corrupt, mismatched or
    /// unavailable policy closes the runtime gate and fails closed.
    /// </summary>
    public async ValueTask<FldBinSyncGcOperatorPolicy?> RestoreAsync(
        CancellationToken cancellationToken = default)
    {
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                FldBinSyncGcOperatorPolicy? policy =
                    await _store.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);
                if (policy is null)
                {
                    _runtime.Stop();
                    return null;
                }

                ValidatePolicy(policy);
                ApplyRuntimeUnlessEmergencyStopped(policy);
                return policy;
            }
            catch
            {
                _runtime.Stop();
                throw;
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// Commit policy and audit atomically before applying it. Optimistic generation
    /// prevents stale/replayed operator requests and concurrent lost updates.
    /// </summary>
    public async ValueTask<FldBinSyncGcOperatorPolicy> UpdateAsync(
        FldBinSyncGcOperatorPolicyRequest request,
        CancellationToken cancellationToken = default)
    {
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await UpdateCoreAsync(request, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask<FldBinSyncGcOperatorPolicy> UpdateCoreAsync(
        FldBinSyncGcOperatorPolicyRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Binding != _binding)
            throw new InvalidOperationException("Operator policy binding does not match this namespace composition.");
        if (string.IsNullOrWhiteSpace(request.ActorId) || string.IsNullOrWhiteSpace(request.Reason))
            throw new ArgumentException("Actor identity and operator reason are required.", nameof(request));
        if (request.ExpectedGeneration == ulong.MaxValue)
            throw new InvalidOperationException("Operator policy generation is exhausted.");

        if (!await _authorizer.AuthorizeAsync(request.ActorId, _binding, cancellationToken)
                .ConfigureAwait(false))
            throw new UnauthorizedAccessException("The actor is not authorized to change GC operator policy.");

        FldBinSyncGcOperatorPolicy? current;
        try
        {
            current = await _store.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _runtime.Stop();
            throw;
        }

        ulong currentGeneration = current?.Generation ?? 0;
        ulong currentSequence = current?.AuditSequence ?? 0;
        if (current is not null)
            ValidatePolicy(current);
        if (currentGeneration != request.ExpectedGeneration)
            throw new InvalidOperationException("GC operator policy generation conflict; reload before retrying.");
        if (currentSequence == ulong.MaxValue)
            throw new InvalidOperationException("GC operator audit sequence is exhausted.");

        var next = new FldBinSyncGcOperatorPolicy(
            _binding,
            checked(currentGeneration + 1),
            checked(currentSequence + 1),
            request.ActorId,
            request.Reason,
            request.MaintenanceStarted,
            request.ReceiptEnabled,
            request.ChangeIdEnabled,
            request.TombstoneEnabled);
        var audit = new FldBinSyncGcOperatorAuditEntry(
            _binding,
            next.AuditSequence,
            next.Generation,
            request.ActorId,
            current is null ? "Initialize" : "Update",
            request.Reason,
            next.MaintenanceStarted,
            next.ReceiptEnabled,
            next.ChangeIdEnabled,
            next.TombstoneEnabled,
            DateTimeOffset.UtcNow);

        bool committed;
        try
        {
            committed = await _store.TryCommitAsync(
                currentGeneration, next, audit, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _runtime.Stop();
            throw;
        }
        if (!committed)
        {
            // A different process advanced the durable generation. The local runtime
            // must not continue operating under an intent that is now stale.
            _runtime.Stop();
            throw new InvalidOperationException("GC operator policy generation conflict; local maintenance was stopped.");
        }

        try
        {
            ApplyRuntimeUnlessEmergencyStopped(next);
        }
        catch
        {
            _runtime.Stop();
            throw;
        }
        return next;
    }

    /// <summary>
    /// Acquire a cross-process durable lease for one bounded domain pass. The returned
    /// lease must be held until the page completes; a null result means policy is
    /// absent, stale or does not enable this domain.
    /// </summary>
    public async ValueTask<IAsyncDisposable?> TryAcquireMaintenanceLeaseAsync(
        FldBinSyncGcDomain domain,
        CancellationToken cancellationToken = default)
    {
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            FldBinSyncGcOperatorPolicy? policy =
                await _store.ReadAsync(_binding, cancellationToken).ConfigureAwait(false);
            if (policy is null)
            {
                _runtime.Stop();
                return null;
            }

            ValidatePolicy(policy);
            bool enabled = policy.MaintenanceStarted && (domain switch
            {
                FldBinSyncGcDomain.Receipt => policy.ReceiptEnabled,
                FldBinSyncGcDomain.ChangeId => policy.ChangeIdEnabled,
                FldBinSyncGcDomain.Tombstone => policy.TombstoneEnabled,
                _ => throw new ArgumentOutOfRangeException(nameof(domain))
            });
            if (!enabled)
                return null;

            IAsyncDisposable? lease = await _store.TryAcquireMaintenanceLeaseAsync(
                _binding, policy.Generation, domain, cancellationToken).ConfigureAwait(false);
            if (lease is null)
                _runtime.Stop();
            return lease;
        }
        catch
        {
            _runtime.Stop();
            throw;
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>Immediate local emergency brake; deliberately independent of durable audit availability.</summary>
    public void EmergencyStop()
    {
        Interlocked.Increment(ref _emergencyStopVersion);
        _runtime.Stop();
    }

    private void ApplyRuntimeUnlessEmergencyStopped(FldBinSyncGcOperatorPolicy policy)
    {
        long stopVersion = Volatile.Read(ref _emergencyStopVersion);
        ApplyRuntime(policy);
        if (Volatile.Read(ref _emergencyStopVersion) != stopVersion)
            _runtime.Stop();
    }

    private void ValidatePolicy(FldBinSyncGcOperatorPolicy policy)
    {
        if (policy.Binding != _binding ||
            policy.Generation == 0 ||
            policy.AuditSequence == 0 ||
            string.IsNullOrWhiteSpace(policy.UpdatedBy) ||
            string.IsNullOrWhiteSpace(policy.Reason))
            throw new InvalidDataException("Persisted GC operator policy is invalid or bound to another namespace.");
    }

    private void ApplyRuntime(FldBinSyncGcOperatorPolicy policy)
    {
        _runtime.Stop();
        if (!policy.MaintenanceStarted)
            return;

        _runtime.Start();
        if (policy.ReceiptEnabled) _runtime.SetEnabled(FldBinSyncGcDomain.Receipt, true);
        if (policy.ChangeIdEnabled) _runtime.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
        if (policy.TombstoneEnabled) _runtime.SetEnabled(FldBinSyncGcDomain.Tombstone, true);
    }
}
