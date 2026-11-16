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
            ApplyRuntime(policy);
            return policy;
        }
        catch
        {
            _runtime.Stop();
            throw;
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
            throw new InvalidOperationException("GC operator policy generation conflict; no policy change was applied.");

        try
        {
            ApplyRuntime(next);
        }
        catch
        {
            _runtime.Stop();
            throw;
        }
        return next;
    }

    /// <summary>Immediate local emergency brake; deliberately independent of durable audit availability.</summary>
    public void EmergencyStop() => _runtime.Stop();

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
