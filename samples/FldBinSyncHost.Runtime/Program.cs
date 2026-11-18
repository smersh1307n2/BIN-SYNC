using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Engine.Sync;
using Fld1.Storage.Engine;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

if (args.Length is not (8 or 9))
{
    Console.Error.WriteLine("Expected: <metadata-root> <fld-root> <metadata-partition> <node-id-hex> <scope-id> <metadata-scope-guid> <namespace-id> <provider-client-guid> [--test-backup-restore-brake]");
    return 64;
}
bool backupRestoreBrake = args.Length == 9 &&
    string.Equals(args[8], "--test-backup-restore-brake", StringComparison.Ordinal);
if (args.Length == 9 && !backupRestoreBrake)
    throw new ArgumentException("Unknown optional test Host argument.", nameof(args));

string metadataRoot = Path.GetFullPath(args[0]);
string fldRoot = Path.GetFullPath(args[1]);
ulong metadataPartition = ulong.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture);
byte[] nodeIdBytes = Convert.FromHexString(args[3]);
if (nodeIdBytes.Length != 16)
    throw new ArgumentException("NodeId must be exactly 16 bytes.", nameof(args));
NodeId nodeId = NodeId.From(nodeIdBytes);
string scopeId = args[4];
Guid metadataScopeId = Guid.Parse(args[5]);
ushort namespaceId = ushort.Parse(args[6], System.Globalization.CultureInfo.InvariantCulture);
Guid providerClientId = Guid.Parse(args[7]);
string operatorToken = Environment.GetEnvironmentVariable("BINSYNC_TEST_HOST_OPERATOR_TOKEN")
    ?? throw new InvalidOperationException("BINSYNC_TEST_HOST_OPERATOR_TOKEN is required.");
if (Encoding.UTF8.GetByteCount(operatorToken) < 32)
    throw new InvalidOperationException("The test operator token must contain at least 32 UTF-8 bytes.");

Directory.CreateDirectory(metadataRoot);
using var metadataStorage = StorageDatabase.Open(metadataRoot, metadataPartition);
using var nodeWal = Wal.Open(Path.Combine(metadataRoot, "node.wal"));
Node node;
if (!StorageNodePersistence.TryLoad(metadataStorage, nodeId, out Node? savedNode))
{
    node = new NodeBuilder().WithNodeId(nodeId).WithEpoch(1).Build();
    node.AttachWal(nodeWal);
    StorageNodePersistence.Save(metadataStorage, node);
}
else
{
    node = savedNode!;
    node.AttachWal(nodeWal);
}

FldDatabaseIpcAsyncDataProvider? provider = null;
FldDatabaseIpcAsyncRemoteBatchApplyStore? remoteApply = null;
FldBinSyncAsyncHostComposition? composition = null;
var peerListener = new TestPeerListener();
var scheduler = new TestMaintenanceScheduler();
WebApplication? app = null;
bool durableStopCommitted = false;

try
{
    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
        fldRoot,
        providerClientId,
        TimeSpan.FromSeconds(15),
        nodeId,
        activeAuthorEpoch: static () => 1);

    remoteApply = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
        fldRoot,
        Guid.NewGuid(),
        namespaceId,
        TimeSpan.FromSeconds(15));

    // The FLD provider must first make the provider-authoritative publication
    // cutover durable. This is part of the real startup contract, not a test bypass.
    await provider.EnableMetadataStoreOwnedPublicationAsync(0);

    composition = FldBinSyncAsyncHostComposition.Create(
        metadataStorage,
        node,
        scopeId,
        metadataScopeId,
        namespaceId,
        new TrustStore(),
        provider,
        remoteApply,
        current => StorageNodePersistence.Save(metadataStorage, current),
        new TestOnlyOperatorAuthorizer());

    // Offline backup restore has no independent anti-rollback authority. The
    // test-only flag models the required operator first-response brake before
    // RestoreAsync reads a possibly older policy/audit snapshot.
    if (backupRestoreBrake)
        composition.Operator.EmergencyStop();

    await composition.StartAsync(_ =>
    {
        // The listener and maintenance loop are deliberately started only after
        // durable policy restoration and provider/Node recovery have completed.
        peerListener.Start();
        scheduler.Start();
        return Task.CompletedTask;
    });

    WebApplicationBuilder builder = WebApplication.CreateBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.UseUrls("http://127.0.0.1:0");
    app = builder.Build();

    byte[] expectedToken = Encoding.UTF8.GetBytes(operatorToken);
    app.Use(async (context, next) =>
    {
        bool protectedPath =
            context.Request.Path.StartsWithSegments("/operator") ||
            context.Request.Path.StartsWithSegments("/admin");
        if (protectedPath)
        {
            string header = context.Request.Headers.Authorization.ToString();
            const string prefix = "Bearer ";
            if (!header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            byte[] supplied = Encoding.UTF8.GetBytes(header[prefix.Length..]);
            if (!CryptographicOperations.FixedTimeEquals(supplied, expectedToken))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            context.Items["operator-actor"] = TestOnlyOperatorAuthorizer.ActorId;
        }

        await next(context);
    });

    app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
    app.MapGet("/health/ready", () => Results.Ok(new
    {
        status = composition.StartupCompleted ? "ready" : "starting",
        peerListenerStarted = peerListener.IsRunning,
        peerPort = peerListener.Port,
        maintenanceSchedulerStarted = scheduler.IsRunning,
        maintenanceTicks = scheduler.TickCount,
        gcExecutionEnabled = false
    }));

    app.MapGet("/operator/gc", async (CancellationToken cancellationToken) =>
    {
        FldBinSyncGcOperatorDiagnosticsSnapshot snapshot =
            await composition.Operator.GetDiagnosticsAsync(cancellationToken: cancellationToken);
        return Results.Ok(snapshot);
    });

    app.MapPost("/operator/gc/policy", async (
        PolicyUpdateRequest request,
        CancellationToken cancellationToken) =>
    {
        try
        {
            FldBinSyncGcOperatorPolicy policy = await composition.Operator.UpdatePolicyAsync(
                request.ExpectedGeneration,
                TestOnlyOperatorAuthorizer.ActorId,
                request.Reason,
                request.MaintenanceStarted,
                request.ReceiptEnabled,
                request.ChangeIdEnabled,
                request.TombstoneEnabled,
                cancellationToken);
            return Results.Ok(policy);
        }
        catch (UnauthorizedAccessException)
        {
            return Results.Unauthorized();
        }
        catch (InvalidOperationException exception)
        {
            return Results.Conflict(new { error = exception.Message });
        }
    });

    app.MapPost("/operator/gc/maintenance/once", async (CancellationToken cancellationToken) =>
    {
        FldBinSyncAsyncGcPassResult result =
            await composition.Operator.RunOneMaintenanceRoundAsync(
                pageLimit: 256,
                changeIdPageLimit: 4096,
                cancellationToken);
        return Results.Ok(result);
    });

    app.MapPost("/operator/gc/emergency-stop", async (
        EmergencyStopRequest request,
        CancellationToken cancellationToken) =>
    {
        composition.Operator.EmergencyStop();
        try
        {
            FldBinSyncGcOperatorPolicy policy =
                await composition.Operator.EmergencyStopDurablyAsync(
                    TestOnlyOperatorAuthorizer.ActorId,
                    request.Reason,
                    cancellationToken);
            await scheduler.StopAsync();
            return Results.Ok(policy);
        }
        catch (Exception exception)
        {
            return Results.Problem(
                title: "Durable emergency stop failed; the process-local GC brake remains latched.",
                detail: exception.Message,
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    });

    app.MapPost("/admin/shutdown", async (ShutdownRequest request, CancellationToken cancellationToken) =>
    {
        try
        {
            composition.Operator.EmergencyStop();
            _ = await composition.Operator.EmergencyStopDurablyAsync(
                TestOnlyOperatorAuthorizer.ActorId,
                request.Reason,
                cancellationToken);
            durableStopCommitted = true;
            await scheduler.StopAsync();
            await peerListener.StopAsync();
            app.Lifetime.StopApplication();
            return Results.Ok(new { status = "stopping", durableGcStop = true });
        }
        catch (Exception exception)
        {
            return Results.Problem(
                title: "Host shutdown was blocked because durable GC stop could not be verified.",
                detail: exception.Message,
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    });

    app.MapPost("/admin/shutdown-clean", async (CancellationToken cancellationToken) =>
    {
        FldBinSyncGcOperatorDiagnosticsSnapshot snapshot =
            await composition.Operator.GetDiagnosticsAsync(cancellationToken: cancellationToken);
        FldBinSyncGcOperatorPolicy? policy = snapshot.Policy;
        if (policy is null ||
            policy.MaintenanceStarted ||
            policy.ReceiptEnabled ||
            policy.ChangeIdEnabled ||
            policy.TombstoneEnabled)
        {
            return Results.Conflict(new
            {
                error = "Offline backup is allowed only after a durable all-domains-off policy has been verified."
            });
        }

        // No policy write here: the existing all-off policy is already durable.
        // The process-local latch is set before the clean close; only then may
        // the test copy/restore the closed database directory.
        composition.Operator.EmergencyStop();
        durableStopCommitted = true;
        await scheduler.StopAsync();
        await peerListener.StopAsync();
        app.Lifetime.StopApplication();
        return Results.Ok(new { status = "stopping", durableAllOffPolicyVerified = true });
    });

    await app.StartAsync();
    string address = app.Services.GetRequiredService<IServer>()
        .Features.Get<IServerAddressesFeature>()?.Addresses.Single()
        ?? throw new InvalidOperationException("Kestrel did not publish its bound address.");
    Console.WriteLine($"READY|{address}|{peerListener.Port}");
    Console.Out.Flush();
    await app.WaitForShutdownAsync();
    return 0;
}
finally
{
    if (app is not null)
    {
        try { await app.DisposeAsync(); } catch { }
    }

    if (composition is not null)
    {
        composition.Operator.EmergencyStop();
        if (!durableStopCommitted)
        {
            try
            {
                _ = await composition.Operator.EmergencyStopDurablyAsync(
                    TestOnlyOperatorAuthorizer.ActorId,
                    "test Host process shutdown");
            }
            catch
            {
                // The local latch remains active; the caller must inspect durable
                // policy before attempting to restart this disposable test Host.
            }
        }

        await scheduler.StopAsync();
        await peerListener.StopAsync();
        composition.Dispose();
    }

    if (remoteApply is not null)
        await remoteApply.DisposeAsync();
    if (provider is not null)
        await provider.DisposeAsync();
}

internal sealed record PolicyUpdateRequest(
    ulong ExpectedGeneration,
    string Reason,
    bool MaintenanceStarted,
    bool ReceiptEnabled,
    bool ChangeIdEnabled,
    bool TombstoneEnabled);

internal sealed record EmergencyStopRequest(string Reason);
internal sealed record ShutdownRequest(string Reason);

internal sealed class TestOnlyOperatorAuthorizer : IFldBinSyncGcOperatorAuthorizer
{
    public const string ActorId = "integration-test-operator";

    public ValueTask<bool> AuthorizeAsync(
        string actorId,
        FldBinSyncGcOperatorPolicyBinding binding,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(
            string.Equals(actorId, ActorId, StringComparison.Ordinal));
}

internal sealed class TestPeerListener
{
    private static readonly byte[] Banner = "BIN-SYNC-TEST-HOST/1 READY\n"u8.ToArray();
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private Task? _acceptLoop;

    public bool IsRunning { get; private set; }
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public void Start()
    {
        if (IsRunning)
            throw new InvalidOperationException("The test peer listener is already running.");
        _listener.Start();
        IsRunning = true;
        _acceptLoop = AcceptLoopAsync(_stop.Token);
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(cancellationToken);
                _ = ReplyAndCloseAsync(client, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested) { }
        catch (SocketException) when (cancellationToken.IsCancellationRequested) { }
    }

    private static async Task ReplyAndCloseAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        {
            try
            {
                await client.GetStream().WriteAsync(Banner, cancellationToken);
            }
            catch (IOException) { }
            catch (OperationCanceledException) { }
        }
    }

    public async Task StopAsync()
    {
        if (!IsRunning)
            return;
        IsRunning = false;
        _stop.Cancel();
        _listener.Stop();
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop; } catch (SocketException) { }
        }
        _stop.Dispose();
    }
}

internal sealed class TestMaintenanceScheduler
{
    private CancellationTokenSource? _stop;
    private Task? _loop;
    private long _ticks;

    public bool IsRunning => _stop is not null && !_stop.IsCancellationRequested;
    public long TickCount => Interlocked.Read(ref _ticks);

    public void Start()
    {
        if (_stop is not null)
            throw new InvalidOperationException("The test maintenance scheduler is already running.");
        _stop = new CancellationTokenSource();
        _loop = RunAsync(_stop.Token);
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(cancellationToken))
            Interlocked.Increment(ref _ticks);
    }

    public async Task StopAsync()
    {
        CancellationTokenSource? stop = Interlocked.Exchange(ref _stop, null);
        if (stop is null)
            return;
        stop.Cancel();
        if (_loop is not null)
        {
            try { await _loop; }
            catch (OperationCanceledException) { }
        }
        stop.Dispose();
        _loop = null;
    }
}
