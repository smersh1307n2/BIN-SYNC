using BinSync;

namespace BinSync.DataProvider.Conformance;

/// <summary>
/// Test-only durability probe layered over the production IDataProvider contract.
/// </summary>
public interface IDataProviderConformanceAdapter : IDataProvider
{
    bool SupportsRestartDurability { get; }
}
