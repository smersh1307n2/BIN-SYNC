using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;

namespace BinSync.FldHost;

/// <summary>
/// Concrete one-namespace composition for the async FLD/BIN-SYNC Host adapter.
/// The caller owns the StorageDatabase, Node/WAL, IPC provider, remote-apply store,
/// TrustStore, transport sessions and operator authorizer. This composition owns
/// its Metadata adapter and process-local GC runtime control.
/// </summary>
public sealed class FldBinSyncAsyncHostComposition : IDisposable
{
    private bool _disposed;

    private FldBinSyncAsyncHostComposition(
        StorageMetadataStore metadataStore,
        StorageNamespaceGcStore namespaceGcStore,
        StorageFldBinSyncGcOperatorPolicyStore policyStore,
        FldBinSyncGcOperatorPolicyCoordinator policyCoordinator,
        AsyncStorageSyncCoordinator coordinator,
        FldBinSyncAsyncHostAdapter adapter,
        FldBinSyncGcMaintenanceControl runtimeControl)
    {
        MetadataStore = metadataStore;
        NamespaceGcStore = namespaceGcStore;
        PolicyStore = policyStore;
        PolicyCoordinator = policyCoordinator;
        Coordinator = coordinator;
        Adapter = adapter;
        RuntimeControl = runtimeControl;
    }

    /// <summary>The Metadata adapter created over the caller-owned StorageDatabase.</summary>
    public StorageMetadataStore MetadataStore { get; }

    /// <summary>The exact namespace-GC store attached to Coordinator.</summary>
    public StorageNamespaceGcStore NamespaceGcStore { get; }

    /// <summary>Durable policy/audit store sharing MetadataStore's exact StorageDatabase.</summary>
    public StorageFldBinSyncGcOperatorPolicyStore PolicyStore { get; }

    public FldBinSyncGcOperatorPolicyCoordinator PolicyCoordinator { get; }

    public AsyncStorageSyncCoordinator Coordinator { get; }

    public FldBinSyncAsyncHostAdapter Adapter { get; }

    public FldBinSyncGcMaintenanceControl RuntimeControl { get; }

    /// <summary>
    /// Build the production-safe adapter object graph for one namespace.
    /// All physical-GC feature switches remain off unless the caller explicitly
    /// supplies enabled StorageNamespaceGcFeatureOptions. Durable operator policy
    /// is mandatory; the adapter restores it before startup recovery/session accept.
    /// StorageDatabase and all supplied process-lifetime dependencies remain caller-owned.
    /// </summary>
    public static FldBinSyncAsyncHostComposition Create(
        StorageDatabase storage,
        Node node,
        string scopeId,
        Guid metadataScopeId,
        int namespaceId,
        TrustStore trustStore,
        IAsyncDataProvider provider,
        IAsyncProviderRemoteBatchCommitStore remoteApply,
        Action<Node> persistNode,
        IFldBinSyncGcOperatorAuthorizer authorizer,
        StorageNamespaceGcFeatureOptions? gcFeatureOptions = null,
        int publicationLimit = 1024)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(remoteApply);
        ArgumentNullException.ThrowIfNull(persistNode);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (metadataScopeId == Guid.Empty)
            throw new ArgumentException("A stable non-empty Metadata scope identity is required.", nameof(metadataScopeId));
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (publicationLimit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(publicationLimit));

        StorageMetadataStore? metadata = null;
        FldBinSyncGcMaintenanceControl? runtime = null;
        try
        {
            metadata = new StorageMetadataStore(storage);
            var namespaceGcStore = new StorageNamespaceGcStore(
                storage, scopeId, node.NodeId, gcFeatureOptions);
            var policyStore = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
            runtime = new FldBinSyncGcMaintenanceControl();
            var binding = new FldBinSyncGcOperatorPolicyBinding(
                metadataScopeId, node.NodeId, namespaceId);
            var policyCoordinator = new FldBinSyncGcOperatorPolicyCoordinator(
                binding, policyStore, authorizer, runtime);
            var coordinator = new AsyncStorageSyncCoordinator(
                node, metadata, scopeId, namespaceId);
            coordinator.AttachNamespaceGcGuard(namespaceGcStore, trustStore);
            var adapter = new FldBinSyncAsyncHostAdapter(
                coordinator, provider, remoteApply, persistNode,
                publicationLimit: publicationLimit,
                gcPolicyCoordinator: policyCoordinator,
                requireDurableOperatorPolicy: true);

            return new FldBinSyncAsyncHostComposition(
                metadata, namespaceGcStore, policyStore, policyCoordinator,
                coordinator, adapter, runtime);
        }
        catch
        {
            runtime?.Dispose();
            metadata?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Restore the durable policy and complete Node/provider recovery. The caller
    /// must not accept peer sessions or start maintenance scheduling before this returns.
    /// </summary>
    public Task<AsyncStorageStartupRecoveryResult> RecoverBeforeAcceptingSessionsAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return Adapter.RecoverBeforeAcceptingSessionsAsync(cancellationToken);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(FldBinSyncAsyncHostComposition));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        RuntimeControl.Dispose();
        MetadataStore.Dispose();
    }
}
