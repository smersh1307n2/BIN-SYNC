using BinSync.FldHost;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncGcMaintenanceControlTests
{
    [Fact]
    public void Runtime_control_starts_stopped_and_requires_explicit_per_domain_enable()
    {
        using var control = new FldBinSyncGcMaintenanceControl();

        Assert.False(control.IsRunning);
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.Null(control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Receipt));
        Assert.Throws<InvalidOperationException>(() =>
            control.SetEnabled(FldBinSyncGcDomain.Receipt, true));

        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.ChangeId, true);

        Assert.True(control.IsRunning);
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.True(control.IsEnabled(FldBinSyncGcDomain.ChangeId));
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Tombstone));
        Assert.Null(control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Receipt));
        using var token = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.ChangeId);
        Assert.NotNull(token);
        Assert.False(token!.IsCancellationRequested);
    }

    [Fact]
    public void Emergency_stop_closes_all_domains_and_cancels_in_flight_work()
    {
        using var control = new FldBinSyncGcMaintenanceControl();
        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.Receipt, true);
        control.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
        control.SetEnabled(FldBinSyncGcDomain.Tombstone, true);
        using var receipt = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Receipt);
        using var changeId = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.ChangeId);
        using var tombstone = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Tombstone);

        control.Stop();

        Assert.False(control.IsRunning);
        Assert.True(receipt!.IsCancellationRequested);
        Assert.True(changeId!.IsCancellationRequested);
        Assert.True(tombstone!.IsCancellationRequested);
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Receipt));
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.ChangeId));
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Tombstone));
        Assert.Null(control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Tombstone));
    }

    [Fact]
    public void Stop_invokes_cancellation_callbacks_without_holding_state_lock()
    {
        using var control = new FldBinSyncGcMaintenanceControl();
        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
        using var linked = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.ChangeId);
        Assert.NotNull(linked);

        bool callbackObservedUnlocked = false;
        using CancellationTokenRegistration registration = linked!.Token.Register(() =>
        {
            Task<bool> stateRead = Task.Run(() => control.IsRunning);
            callbackObservedUnlocked = stateRead.Wait(TimeSpan.FromSeconds(2));
        });

        control.Stop();

        Assert.True(callbackObservedUnlocked,
            "A cancellation callback could not read control state; cancellation ran under the state lock.");
    }

    [Fact]
    public async Task Concurrent_restart_uses_new_cancellation_generation()
    {
        using var control = new FldBinSyncGcMaintenanceControl();
        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
        using var oldLinked = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.ChangeId);
        Assert.NotNull(oldLinked);

        var callbackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenRegistration registration = oldLinked!.Token.Register(() =>
        {
            callbackEntered.TrySetResult();
            releaseCallback.Task.GetAwaiter().GetResult();
        });

        Task stopping = Task.Run(control.Stop);
        await callbackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Stop has closed the old generation but is still running user callbacks.
        // A concurrent explicit restart must receive a fresh, uncancelled generation.
        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.ChangeId, true);
        using var newLinked = control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.ChangeId);
        Assert.NotNull(newLinked);
        Assert.False(newLinked!.IsCancellationRequested);

        releaseCallback.TrySetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(control.IsRunning);
        Assert.True(control.IsEnabled(FldBinSyncGcDomain.ChangeId));
        Assert.False(newLinked.IsCancellationRequested);
    }

    [Fact]
    public void Restart_requires_each_domain_to_be_enabled_again()
    {
        using var control = new FldBinSyncGcMaintenanceControl();
        control.Start();
        control.SetEnabled(FldBinSyncGcDomain.Tombstone, true);
        control.Stop();
        control.Start();

        Assert.True(control.IsRunning);
        Assert.False(control.IsEnabled(FldBinSyncGcDomain.Tombstone));
        Assert.Null(control.TryCreateLinkedTokenSource(FldBinSyncGcDomain.Tombstone));
    }
}
