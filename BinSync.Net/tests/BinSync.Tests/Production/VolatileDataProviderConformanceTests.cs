using BinSync.DataProvider.Conformance;
using Xunit;

namespace BinSync.Tests.DataProvider;

/// <summary>Runs the provider semantic contract against the explicitly volatile reference provider.</summary>
public sealed class VolatileDataProviderConformanceTests : DataProviderConformanceSuite
{
    protected override IDataProviderConformanceAdapter CreateProvider()
        => new VolatileDataProviderAdapter();

    [Fact]
    public void Reference_provider_explicitly_does_not_claim_restart_durability()
    {
        using var provider = CreateProvider();
        Assert.False(provider.SupportsRestartDurability);
    }
}
