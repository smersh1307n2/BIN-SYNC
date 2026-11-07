using BinSync;

namespace BinSync.MetadataStore.Conformance;

/// <summary>
/// Test-only durability probe layered over the production IMetadataStore contract.
/// </summary>
public interface IMetadataStoreConformanceAdapter : IMetadataStore
{
    bool SupportsRestartDurability { get; }
}
