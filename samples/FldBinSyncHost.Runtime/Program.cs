using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
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
const string HostBootstrapActorId = "system:host-bootstrap";
string hostMode = Environment.GetEnvironmentVariable("BINSYNC_HOST_MODE") ?? "Oidc";
bool testMode = string.Equals(hostMode, "Test", StringComparison.Ordinal);
if (!testMode && !string.Equals(hostMode, "Oidc", StringComparison.Ordinal))
    throw new InvalidOperationException("BINSYNC_HOST_MODE must be either Oidc or Test.");

string? operatorToken = null;
HashSet<string> allowedOperatorSubjects = new(StringComparer.Ordinal);
bool enableGcOnFreshStore = false;
string[] hostUrls;
if (testMode)
{
    operatorToken = Environment.GetEnvironmentVariable("BINSYNC_TEST_HOST_OPERATOR_TOKEN")
        ?? throw new InvalidOperationException("BINSYNC_TEST_HOST_OPERATOR_TOKEN is required in Test mode.");
    if (Encoding.UTF8.GetByteCount(operatorToken) < 32)
        throw new InvalidOperationException("The test operator token must contain at least 32 UTF-8 bytes.");
    hostUrls = new[] { "http://127.0.0.1:0" };
    enableGcOnFreshStore = true;
    if (backupRestoreBrake is false && args.Length == 9)
        throw new ArgumentException("The backup-restore brake is available only in Test mode.", nameof(args));
}
else
{
    string authority = Environment.GetEnvironmentVariable("BINSYNC_OPERATOR_AUTHORITY")
        ?? throw new InvalidOperationException("BINSYNC_OPERATOR_AUTHORITY is required in Oidc mode.");
    if (!Uri.TryCreate(authority, UriKind.Absolute, out Uri? authorityUri) ||
        authorityUri.Scheme != Uri.UriSchemeHttps)
        throw new InvalidOperationException("BINSYNC_OPERATOR_AUTHORITY must be an absolute HTTPS issuer URL.");
    string audience = Environment.GetEnvironmentVariable("BINSYNC_OPERATOR_AUDIENCE")
        ?? throw new InvalidOperationException("BINSYNC_OPERATOR_AUDIENCE is required in OIDC operator mode.");
    string subjects = Environment.GetEnvironmentVariable("BINSYNC_OPERATOR_SUBJECTS")
        ?? throw new InvalidOperationException("BINSYNC_OPERATOR_SUBJECTS must allow-list operator subject IDs.");
    foreach (string subject in subjects.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        allowedOperatorSubjects.Add(subject);
    if (allowedOperatorSubjects.Count == 0 || allowedOperatorSubjects.Contains(HostBootstrapActorId))
        throw new InvalidOperationException("At least one real operator subject is required; the internal bootstrap actor is reserved.");
    string urls = Environment.GetEnvironmentVariable("BINSYNC_HOST_URLS")
        ?? throw new InvalidOperationException("BINSYNC_HOST_URLS must configure one or more HTTPS endpoints.");
    hostUrls = urls.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    if (hostUrls.Length == 0 || hostUrls.Any(value =>
        !Uri.TryCreate(value, UriKind.Absolute, out Uri? endpoint) || endpoint.Scheme != Uri.UriSchemeHttps))
        throw new InvalidOperationException("Every BINSYNC_HOST_URLS endpoint must use HTTPS.");
    enableGcOnFreshStore = string.Equals(
        Environment.GetEnvironmentVariable("BINSYNC_GC_ENABLE_ON_FRESH_STORE"), "true",
        StringComparison.OrdinalIgnoreCase);
    if (backupRestoreBrake)
        throw new ArgumentException("The test-only backup-restore switch is unavailable in OIDC operator mode.", nameof(args));
}

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
TestPeerRuntime? peerRuntime = null;
int maintenanceIntervalMs = int.TryParse(
    Environment.GetEnvironmentVariable(testMode
        ? "BINSYNC_TEST_HOST_MAINTENANCE_INTERVAL_MS"
        : "BINSYNC_HOST_MAINTENANCE_INTERVAL_MS"),
    System.Globalization.NumberStyles.None,
    System.Globalization.CultureInfo.InvariantCulture,
    out int configuredMaintenanceIntervalMs)
        ? configuredMaintenanceIntervalMs : 1000;
if (maintenanceIntervalMs is < 100 or > 60000)
    throw new InvalidOperationException("Host maintenance interval must be between 100 and 60000 ms.");
long gcMaintenanceSlices = 0;
long gcReceiptsReclaimed = 0;
long gcChangeIdsReclaimed = 0;
long gcTombstonesReclaimed = 0;
var scheduler = new TestMaintenanceScheduler(
    async cancellationToken =>
    {
        // A Host reference scan owns the FLD IPC boundary: even Receipt/ChangeId
        // readiness scans temporarily block provider reads and writes. Do not open
        // that boundary when every runtime GC domain is stopped, and never start a
        // scheduled scan while a peer is active. For an enabled domain, acquire the
        // same admission window used by Tombstone GC around the entire readiness +
        // compaction slice so a new peer cannot enter between the idle check and scan.
        bool anyGcDomainEnabled =
            composition!.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Receipt) ||
            composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.ChangeId) ||
            composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Tombstone);
        if (anyGcDomainEnabled)
        {
            if (peerRuntime is null)
            {
                FldBinSyncAsyncGcPassResult pass = await composition.Operator.RunOneMaintenanceSliceAsync(
                    pageLimit: 256,
                    changeIdPageLimit: 4096,
                    cancellationToken: cancellationToken);
                Interlocked.Increment(ref gcMaintenanceSlices);
                Interlocked.Add(ref gcReceiptsReclaimed, pass.ReceiptsReclaimed);
                Interlocked.Add(ref gcChangeIdsReclaimed, pass.ChangeIdsReclaimed);
                Interlocked.Add(ref gcTombstonesReclaimed, pass.TombstonesReclaimed);
            }
            else
            {
                using IDisposable? maintenanceWindow =
                    peerRuntime.TryAcquireIdleMaintenanceWindow();
                if (maintenanceWindow is not null)
                {
                    FldBinSyncAsyncGcPassResult pass = await composition.Operator.RunOneMaintenanceSliceAsync(
                        pageLimit: 256,
                        changeIdPageLimit: 4096,
                        acquireTombstoneMaintenanceWindow: null,
                        cancellationToken: cancellationToken);
                    Interlocked.Increment(ref gcMaintenanceSlices);
                    Interlocked.Add(ref gcReceiptsReclaimed, pass.ReceiptsReclaimed);
                    Interlocked.Add(ref gcChangeIdsReclaimed, pass.ChangeIdsReclaimed);
                    Interlocked.Add(ref gcTombstonesReclaimed, pass.TombstonesReclaimed);
                }
            }
        }

        if (peerRuntime is not null)
            await peerRuntime.RunOutboundRoundAsync(cancellationToken);
    },
    exception =>
    {
        Console.Error.WriteLine($"Test maintenance scheduler stopped: {exception.Message}");
        composition!.Operator.EmergencyStop();
    },
    TimeSpan.FromMilliseconds(maintenanceIntervalMs));
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
        testMode ? new TestOnlyOperatorAuthorizer() : new ConfiguredOperatorAuthorizer(allowedOperatorSubjects),
        // This executable is the disposable integration Host used to exercise
        // the enabled runtime. The library/composition defaults remain fail-closed.
        new StorageNamespaceGcFeatureOptions(
            EnableTombstoneGc: true,
            EnableChangeIdGc: true,
            EnableReceiptGc: true));

    // Offline backup restore has no independent anti-rollback authority. The
    // test-only flag models the required operator first-response brake before
    // RestoreAsync reads a possibly older policy/audit snapshot.
    if (backupRestoreBrake)
        composition.Operator.EmergencyStop();

    peerRuntime = new TestPeerRuntime(
        node,
        composition,
        provider,
        current => StorageNodePersistence.Save(metadataStorage, current),
        Environment.GetEnvironmentVariable("BINSYNC_TEST_HOST_ALLOWED_PEERS"));

    await composition.StartAsync(async cancellationToken =>
    {
        // A fresh Host starts GC through the same durable, audited staged policy
        // used by the operator API. Never overwrite an existing policy: in
        // particular, a previously committed emergency stop must survive restart.
        FldBinSyncGcOperatorDiagnosticsSnapshot startupGc =
            await composition.Operator.GetDiagnosticsAsync(cancellationToken: cancellationToken);
        if (startupGc.Policy is null && enableGcOnFreshStore)
        {
            await composition.Operator.UpdatePolicyAsync(
                expectedGeneration: 0,
                HostBootstrapActorId,
                reason: "fresh Host bootstrap: enable ChangeId GC first",
                maintenanceStarted: true,
                receiptEnabled: false,
                changeIdEnabled: true,
                tombstoneEnabled: false,
                cancellationToken);
            await composition.Operator.UpdatePolicyAsync(
                expectedGeneration: 1,
                HostBootstrapActorId,
                reason: "fresh Host bootstrap: enable Receipt GC after ChangeId",
                maintenanceStarted: true,
                receiptEnabled: true,
                changeIdEnabled: true,
                tombstoneEnabled: false,
                cancellationToken);
            await composition.Operator.UpdatePolicyAsync(
                expectedGeneration: 2,
                HostBootstrapActorId,
                reason: "fresh Host bootstrap: enable proof-gated Tombstone GC last",
                maintenanceStarted: true,
                receiptEnabled: true,
                changeIdEnabled: true,
                tombstoneEnabled: true,
                cancellationToken);
        }

        // The real TCP SyncSession listener and maintenance loop are deliberately
        // started only after durable policy restoration, optional fresh-store GC
        // bootstrap, and provider/Node recovery.
        peerRuntime.Start();
        scheduler.Start();
    });

    WebApplicationBuilder builder = WebApplication.CreateBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.UseUrls(hostUrls);
    if (!testMode)
    {
        string authority = Environment.GetEnvironmentVariable("BINSYNC_OPERATOR_AUTHORITY")!;
        string audience = Environment.GetEnvironmentVariable("BINSYNC_OPERATOR_AUDIENCE")!;
        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.Authority = authority;
                options.Audience = audience;
                options.RequireHttpsMetadata = true;
                options.MapInboundClaims = false;
            });
    }
    app = builder.Build();

    byte[]? expectedToken = operatorToken is null ? null : Encoding.UTF8.GetBytes(operatorToken);
    app.Use(async (context, next) =>
    {
        bool operatorPath = context.Request.Path.StartsWithSegments("/operator");
        bool adminPath = context.Request.Path.StartsWithSegments("/admin");
        if (testMode && (operatorPath || adminPath))
        {
            string header = context.Request.Headers.Authorization.ToString();
            const string prefix = "Bearer ";
            if (!header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            byte[] supplied = Encoding.UTF8.GetBytes(header[prefix.Length..]);
            if (!CryptographicOperations.FixedTimeEquals(supplied, expectedToken!))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            context.Items["operator-actor"] = TestOnlyOperatorAuthorizer.ActorId;
        }
        else if (!testMode && adminPath)
        {
            // Test injection, peer-connect and process-shutdown routes are not part
            // of the production operator surface.
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }
        else if (!testMode && operatorPath)
        {
            AuthenticateResult authentication = await context.AuthenticateAsync(
                JwtBearerDefaults.AuthenticationScheme);
            if (!authentication.Succeeded || authentication.Principal is null)
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            string? actorId = authentication.Principal.FindFirst("sub")?.Value;
            if (string.IsNullOrWhiteSpace(actorId) || !allowedOperatorSubjects.Contains(actorId))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            context.User = authentication.Principal;
            context.Items["operator-actor"] = actorId;
        }

        await next(context);
    });

    app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
    app.MapGet("/health/ready", () => Results.Ok(new
    {
        status = composition.StartupCompleted ? "ready" : "starting",
        peerListenerStarted = peerRuntime.IsRunning,
        peerPort = peerRuntime.Port,
        activePeers = peerRuntime.ActivePeers,
        idleMaintenanceWindowActive = peerRuntime.IsIdleMaintenanceWindowActive,
        baselineReadyPeers = peerRuntime.BaselineReadyPeers,
        maintenanceSchedulerStarted = scheduler.IsRunning,
        maintenanceTicks = scheduler.TickCount,
        maintenanceError = scheduler.LastError,
        gcStartupProofEnabled =
            composition.NamespaceGcStore.IsReceiptGcProofEnabled &&
            composition.NamespaceGcStore.IsChangeIdGcProofEnabled &&
            composition.NamespaceGcStore.IsTombstoneGcProofEnabled,
        gcExecutionEnabled = composition.RuntimeControl.IsRunning &&
            (composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Receipt) ||
             composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.ChangeId) ||
             composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Tombstone)),
        gcReceiptEnabled = composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Receipt),
        gcChangeIdEnabled = composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.ChangeId),
        gcTombstoneEnabled = composition.RuntimeControl.IsEnabled(FldBinSyncGcDomain.Tombstone),
        gcMaintenanceSlices = Interlocked.Read(ref gcMaintenanceSlices),
        gcReceiptsReclaimed = Interlocked.Read(ref gcReceiptsReclaimed),
        gcChangeIdsReclaimed = Interlocked.Read(ref gcChangeIdsReclaimed),
        gcTombstonesReclaimed = Interlocked.Read(ref gcTombstonesReclaimed)
    }));

    app.MapGet("/operator/peers", () => Results.Ok(new
    {
        activePeers = peerRuntime.ActivePeers,
        baselineReadyPeers = peerRuntime.BaselineReadyPeers,
        activePeerIds = peerRuntime.ActivePeerIds,
        outboundRounds = peerRuntime.OutboundRounds,
        outboundBatches = peerRuntime.OutboundBatches,
        inboundBatches = peerRuntime.InboundBatches,
        lastPeerError = peerRuntime.LastError
    }));

    if (testMode)
    {
    // Test-only ACK visibility. ReadOutboundAuthors is a set of author feed
    // identities, not a pending-delivery count: it intentionally remains populated
    // after ACK. Inspect each interested peer's durable in-flight reservation instead.
    app.MapGet("/admin/test/pending-outbound", () =>
    {
        var pending = composition!.MetadataStore.ReadInterestedPeers(
                scopeId, node.NodeId, namespaceId)
            .SelectMany(peer => composition.MetadataStore.ReadPeerStreamStates(
                scopeId, node.NodeId, peer.Peer, peer.PeerEpoch, namespaceId))
            .Where(stream => stream.State.PendingBatch is not null)
            .Select(stream => new
            {
                author = Convert.ToHexString(stream.AuthorStream.ToArray()),
                cursorStart = stream.State.PendingBatch!.CursorStart,
                cursorEnd = stream.State.PendingBatch.CursorEnd
            })
            .ToArray();
        return Results.Ok(new
        {
            pendingOutboundBatches = pending.Length,
            pending
        });
    });

    app.MapPost("/admin/test/verify-empty-namespace", async (CancellationToken cancellationToken) =>
    {
        try
        {
            await peerRuntime.VerifyEmptyNamespaceAsync(cancellationToken);
            return Results.Ok(new { providerEmpty = true, nodeEmpty = true });
        }
        catch (Exception exception) when (exception.Message.Contains(
            "GC lease owner may only access its reference scan", StringComparison.Ordinal))
        {
            return Results.Problem(
                title: "FLD Host is performing a GC reference scan; retry the test verification.",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
        catch (Exception exception)
        {
            return Results.Conflict(new { error = exception.Message });
        }
    });

    app.MapPost("/admin/test/complete-peer-baseline", async (CancellationToken cancellationToken) =>
    {
        try
        {
            await peerRuntime.CompletePeerSessionsAsync(cancellationToken);
            return Results.Ok(new { baselineReadyPeers = peerRuntime.BaselineReadyPeers });
        }
        catch (Exception exception)
        {
            return Results.Conflict(new { error = exception.Message });
        }
    });

    app.MapPost("/admin/test/hold-idle-maintenance-window", async (
        int durationMs,
        CancellationToken cancellationToken) =>
    {
        if (durationMs is < 100 or > 5000)
            return Results.BadRequest(new { error = "durationMs must be between 100 and 5000." });
        using IDisposable? window = peerRuntime.TryAcquireIdleMaintenanceWindow();
        if (window is null)
            return Results.Conflict(new { error = "An active peer prevents an idle maintenance window." });
        await Task.Delay(TimeSpan.FromMilliseconds(durationMs), cancellationToken);
        return Results.Ok(new { idleMaintenanceWindowReleased = true });
    });

    app.MapPost("/admin/test/local-change", async (
        LocalChangeRequest request,
        CancellationToken cancellationToken) =>
    {
        try
        {
            byte[] recordId = Convert.FromHexString(request.RecordIdHex);
            byte[] changeId = Convert.FromHexString(request.ChangeIdHex);
            byte[] value = Convert.FromBase64String(request.ValueBase64);
            if (recordId.Length != 16 || changeId.Length != 16 ||
                request.FieldId == 0 || request.HlcWall == 0)
                return Results.BadRequest(new { error = "Invalid test mutation identity, field or HLC." });

            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(recordId),
                FieldId = request.FieldId,
                AuthorNodeId = node.NodeId,
                KeyEpoch = request.KeyEpoch,
                Value = value,
                ChangeId = BatchId.From(changeId),
                OriginAuthorEpoch = 1,
                OriginSequence = 0,
                HlcWall = request.HlcWall,
                HlcLogical = request.HlcLogical
            };
            ulong receiptSequence = await provider!.AppendLocalChangeWithReceiptAsync(
                mutation, cancellationToken);
            return Results.Ok(new { receiptSequence });
        }
        catch (Exception exception) when (exception is FormatException or ArgumentException or InvalidDataException)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
        catch (Exception exception) when (exception.Message.Contains(
            "GC lease owner may only access its reference scan", StringComparison.Ordinal))
        {
            return Results.Problem(
                title: "FLD Host is performing a GC reference scan; retry the test write.",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Test local-change endpoint failed: {exception}");
            return Results.Problem(title: "Test local-change failed.", detail: exception.ToString(),
                statusCode: StatusCodes.Status500InternalServerError);
        }
    });

    app.MapGet("/admin/test/current/{recordIdHex}/{fieldId:int}", async (
        string recordIdHex,
        int fieldId,
        CancellationToken cancellationToken) =>
    {
        try
        {
            if (fieldId is < 1 or > ushort.MaxValue)
                return Results.BadRequest(new { error = "FieldId is outside the supported range." });
            byte[] recordId = Convert.FromHexString(recordIdHex);
            if (recordId.Length != 16)
                return Results.BadRequest(new { error = "RecordId must be exactly 16 bytes." });

            ProviderMutation? current = await provider!.ReadCurrentAsync(
                RecordId.From(recordId), checked((ushort)fieldId), cancellationToken);
            if (current is null)
                return Results.NotFound();

            return Results.Ok(new
            {
                valueBase64 = Convert.ToBase64String(current.Value),
                changeIdHex = Convert.ToHexString(current.ChangeId.ToArray()),
                authorNodeIdHex = Convert.ToHexString(current.AuthorNodeId.ToArray()),
                originAuthorEpoch = current.OriginAuthorEpoch,
                originSequence = current.OriginSequence,
                hlcWall = current.HlcWall,
                hlcLogical = current.HlcLogical
            });
        }
        catch (Exception exception) when (exception is FormatException or ArgumentException or InvalidDataException)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
        catch (Exception exception) when (exception.Message.Contains(
            "GC lease owner may only access its reference scan", StringComparison.Ordinal))
        {
            return Results.Problem(
                title: "FLD Host is performing a GC reference scan; retry the test read.",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Test current-value endpoint failed: {exception}");
            return Results.Problem(title: "Test current-value read failed.", detail: exception.ToString(),
                statusCode: StatusCodes.Status500InternalServerError);
        }
    });

    app.MapPost("/admin/peers/connect", async (
        PeerConnectRequest request,
        CancellationToken cancellationToken) =>
    {
        try
        {
            NodeId peer = await peerRuntime.ConnectAsync(request.Host, request.Port, cancellationToken);
            return Results.Ok(new { peerNodeId = Convert.ToHexString(peer.ToArray()) });
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException or
            SocketException or TimeoutException)
        {
            return Results.Problem(title: "Test peer connection failed.", detail: exception.Message,
                statusCode: StatusCodes.Status502BadGateway);
        }
    });

    }

    app.MapGet("/operator/gc", async (CancellationToken cancellationToken) =>
    {
        FldBinSyncGcOperatorDiagnosticsSnapshot snapshot =
            await composition.Operator.GetDiagnosticsAsync(cancellationToken: cancellationToken);
        return Results.Ok(snapshot);
    });

    app.MapPost("/operator/gc/policy", async (
        PolicyUpdateRequest request,
        HttpContext httpContext,
        CancellationToken cancellationToken) =>
    {
        try
        {
            FldBinSyncGcOperatorPolicy policy = await composition.Operator.UpdatePolicyAsync(
                request.ExpectedGeneration,
                (string)httpContext.Items["operator-actor"]!,
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
                cancellationToken: cancellationToken);
        return Results.Ok(result);
    });

    app.MapPost("/operator/gc/emergency-stop", async (
        EmergencyStopRequest request,
        HttpContext httpContext,
        CancellationToken cancellationToken) =>
    {
        composition.Operator.EmergencyStop();
        try
        {
            FldBinSyncGcOperatorPolicy policy =
                await composition.Operator.EmergencyStopDurablyAsync(
                    (string)httpContext.Items["operator-actor"]!,
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

    if (testMode)
    {
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
            await peerRuntime.StopAsync();
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
        await peerRuntime.StopAsync();
        app.Lifetime.StopApplication();
        return Results.Ok(new { status = "stopping", durableAllOffPolicyVerified = true });
    });

    }

    await app.StartAsync();
    string address = app.Services.GetRequiredService<IServer>()
        .Features.Get<IServerAddressesFeature>()?.Addresses.Single()
        ?? throw new InvalidOperationException("Kestrel did not publish its bound address.");
    Console.WriteLine($"READY|{address}|{peerRuntime.Port}");
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
                    HostBootstrapActorId,
                    "Host process shutdown");
            }
            catch
            {
                // The local latch remains active; the caller must inspect durable
                // policy before attempting to restart this disposable test Host.
            }
        }

        await scheduler.StopAsync();
        if (peerRuntime is not null)
            await peerRuntime.StopAsync();
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
internal sealed record PeerConnectRequest(string Host, int Port);
internal sealed record LocalChangeRequest(
    string RecordIdHex,
    ushort FieldId,
    string ChangeIdHex,
    string ValueBase64,
    ushort KeyEpoch,
    ulong HlcWall,
    uint HlcLogical);

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

internal sealed class ConfiguredOperatorAuthorizer : IFldBinSyncGcOperatorAuthorizer
{
    private readonly HashSet<string> _allowedSubjects;
    private readonly string _bootstrapActorId;

    public ConfiguredOperatorAuthorizer(IEnumerable<string> allowedSubjects, string bootstrapActorId)
    {
        _allowedSubjects = new HashSet<string>(
            allowedSubjects ?? throw new ArgumentNullException(nameof(allowedSubjects)),
            StringComparer.Ordinal);
        _bootstrapActorId = string.IsNullOrWhiteSpace(bootstrapActorId)
            ? throw new ArgumentException("A bootstrap actor identity is required.", nameof(bootstrapActorId))
            : bootstrapActorId;
    }

    public ValueTask<bool> AuthorizeAsync(
        string actorId,
        FldBinSyncGcOperatorPolicyBinding binding,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(
            string.Equals(actorId, _bootstrapActorId, StringComparison.Ordinal) ||
            _allowedSubjects.Contains(actorId));
}

internal sealed class TestMaintenanceScheduler
{
    private readonly Func<CancellationToken, Task> _runMaintenance;
    private readonly Action<Exception> _onFailure;
    private readonly TimeSpan _interval;
    private CancellationTokenSource? _stop;
    private Task? _loop;
    private long _ticks;
    private string? _lastError;
    private int _running;

    public TestMaintenanceScheduler(
        Func<CancellationToken, Task> runMaintenance,
        Action<Exception> onFailure,
        TimeSpan interval)
    {
        _runMaintenance = runMaintenance ?? throw new ArgumentNullException(nameof(runMaintenance));
        _onFailure = onFailure ?? throw new ArgumentNullException(nameof(onFailure));
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        _interval = interval;
    }

    public bool IsRunning => Volatile.Read(ref _running) != 0;
    public long TickCount => Interlocked.Read(ref _ticks);
    public string? LastError => Volatile.Read(ref _lastError);

    public void Start()
    {
        if (Interlocked.Exchange(ref _running, 1) != 0)
            throw new InvalidOperationException("The test maintenance scheduler is already running.");
        _stop = new CancellationTokenSource();
        _loop = RunAsync(_stop.Token);
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Delay after the previous round completes. PeriodicTimer can
                // immediately replay a queued tick after a long scan and starve
                // ordinary Host operations under a continuous GC lease.
                await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
                // StopAsync cancels only the wait for the next round. Once a round has
                // started, let its bounded work drain before disposing provider IPC.
                // Cancelling an in-flight Storage IPC request faults that session because
                // the response frame may be incomplete; the Host must disconnect cleanly
                // only after the current round has released its provider resources.
                await _runMaintenance(CancellationToken.None).ConfigureAwait(false);
                Interlocked.Increment(ref _ticks);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                Volatile.Write(ref _lastError, exception.ToString());
                _onFailure(exception);
                Interlocked.Exchange(ref _running, 0);
                return;
            }
        }
    }

    public async Task StopAsync()
    {
        CancellationTokenSource? stop = Interlocked.Exchange(ref _stop, null);
        Interlocked.Exchange(ref _running, 0);
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
