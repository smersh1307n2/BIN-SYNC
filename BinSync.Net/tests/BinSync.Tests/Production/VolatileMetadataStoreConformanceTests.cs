using BinSync.MetadataStore.Conformance;

namespace BinSync.Tests.MetadataStore;

/// <summary>Runs the shared semantic suite against the explicitly volatile reference adapter.</summary>
public sealed class VolatileMetadataStoreConformanceTests : MetadataStoreConformanceSuite
{
    protected override IMetadataStoreConformanceAdapter CreateStore()
        => new VolatileMetadataStoreAdapter();
}
