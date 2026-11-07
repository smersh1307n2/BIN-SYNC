using BinSync.MetadataStore.Conformance;
using Xunit;

namespace BinSync.Tests.MetadataStore;

/// <summary>Runs the shared semantic suite against the explicitly volatile reference adapter.</summary>
public sealed class VolatileMetadataStoreConformanceTests : MetadataStoreConformanceSuite
{
    protected override IMetadataStoreConformanceAdapter CreateStore()
        => new VolatileMetadataStoreAdapter();

    [Fact]
    public void Reference_adapter_explicitly_does_not_claim_restart_durability()
    {
        using var store = CreateStore();
        Assert.False(store.SupportsRestartDurability);
    }
}
