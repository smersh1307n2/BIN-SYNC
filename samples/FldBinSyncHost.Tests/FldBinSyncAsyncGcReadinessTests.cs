using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncAsyncGcReadinessTests
{
    [Fact]
    public void Disabled_domain_is_never_ready_even_when_prerequisites_are_satisfied()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "Receipt", Enabled: false, PrerequisitesSatisfied: true,
            BlockedReasons: Array.Empty<string>());

        Assert.True(readiness.PrerequisitesSatisfied);
        Assert.False(readiness.ReadyForAttempt);
    }

    [Fact]
    public void Enabled_domain_is_blocked_by_any_global_readiness_reason()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "ChangeId", Enabled: true, PrerequisitesSatisfied: false,
            BlockedReasons: new[] { "Host reports an unpublished local draft horizon." });

        Assert.False(readiness.ReadyForAttempt);
        Assert.Single(readiness.BlockedReasons);
    }

    [Fact]
    public void Enabled_domain_with_no_global_blockers_is_ready_only_for_candidate_evaluation()
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "Record/Field Tombstone", Enabled: true, PrerequisitesSatisfied: true,
            BlockedReasons: Array.Empty<string>());

        Assert.True(readiness.ReadyForAttempt);
        // This type intentionally has no "safe to delete" or "enable GC" state:
        // each physical candidate still needs the coordinator's signed proof checks.
    }
}
