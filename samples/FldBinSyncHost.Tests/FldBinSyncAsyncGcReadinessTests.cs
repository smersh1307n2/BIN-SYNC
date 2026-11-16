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

    [Theory]
    [InlineData("No accepted, verifiable namespace roster is available.", FldBinSyncGcBlockerCode.SignedRosterUnavailable)]
    [InlineData("No accepted, verifiable retention checkpoint is available.", FldBinSyncGcBlockerCode.RetentionCheckpointUnavailable)]
    [InlineData("Durable startup Receipt GC option is disabled.", FldBinSyncGcBlockerCode.DurableFeatureDisabled)]
    [InlineData("Runtime ChangeId GC gate is stopped or this domain is disabled.", FldBinSyncGcBlockerCode.RuntimeGateDisabled)]
    [InlineData("Host reports one or more active immutable provider snapshot pins.", FldBinSyncGcBlockerCode.SnapshotPinsPresent)]
    [InlineData("No authoritative FULL_REPLACE generation/digest is covered by the accepted checkpoint.", FldBinSyncGcBlockerCode.FullReplaceBaselineUnavailable)]
    [InlineData("A future blocker message with no registered mapping.", FldBinSyncGcBlockerCode.Unclassified)]
    public void Blocker_messages_expose_stable_codes_without_losing_operator_detail(
        string message, FldBinSyncGcBlockerCode expectedCode)
    {
        var readiness = new FldBinSyncGcDomainReadiness(
            "ChangeId", Enabled: false, PrerequisitesSatisfied: false,
            BlockedReasons: new[] { message });

        FldBinSyncGcBlocker blocker = Assert.Single(readiness.Blockers);
        Assert.Equal(expectedCode, blocker.Code);
        Assert.Equal(message, blocker.Message);
    }

    [Fact]
    public void Blocker_projection_is_a_snapshot_and_preserves_multiple_reasons()
    {
        string[] reasons =
        {
            "Durable startup Receipt GC option is disabled.",
            "Runtime Receipt GC gate is stopped or this domain is disabled."
        };
        var readiness = new FldBinSyncGcDomainReadiness(
            "Receipt", Enabled: false, PrerequisitesSatisfied: false, reasons);

        FldBinSyncGcBlocker[] blockers = readiness.Blockers.ToArray();

        Assert.Equal(2, blockers.Length);
        Assert.Equal(FldBinSyncGcBlockerCode.DurableFeatureDisabled, blockers[0].Code);
        Assert.Equal(FldBinSyncGcBlockerCode.RuntimeGateDisabled, blockers[1].Code);
        Assert.Equal(reasons, readiness.BlockedReasons);
    }
}
