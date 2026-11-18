using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Net.Http.Json;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncRuntimeHostTests
{
    private const ushort NamespaceId = 53;
    private const ulong MetadataPartition = 985;
    private const string ScopeId = "stage0-runtime-host";
    private static readonly Guid MetadataScopeId =
        Guid.Parse("d1b9b04e-c4d2-4a9a-9c13-1d3f51d3e071");
    private static readonly Guid ProviderClientId =
        Guid.Parse("b2b7c14e-9e13-4c1b-8a23-8d7b3c9a4d01");
    private static readonly byte[] LocalAuthor = Id(0x53);
    private const string OperatorToken =
        "stage0-test-host-operator-token-0123456789abcdef0123456789abcdef";

    [Fact]
    public async Task Executable_host_authenticates_operator_and_restores_durable_stop_after_process_restart()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-stage0-runtime-" + Guid.NewGuid().ToString("N"));
        string metadataRoot = Path.Combine(root, "metadata");
        string fldRoot = Path.Combine(root, "fld");
        Directory.CreateDirectory(root);
        Process? databaseHost = null;
        Process? runtimeHost = null;

        try
        {
            databaseHost = StartDatabaseHost(fldRoot);
            await WaitForDatabaseHost(fldRoot, databaseHost, TimeSpan.FromSeconds(45));

            runtimeHost = StartRuntimeHost(metadataRoot, fldRoot);
            (Uri address, int peerPort) = await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45));

            using var anonymous = new HttpClient { BaseAddress = address };
            using HttpResponseMessage anonymousResponse =
                await anonymous.GetAsync("/operator/gc");
            Assert.Equal(HttpStatusCode.Unauthorized, anonymousResponse.StatusCode);

            using var wrongCredentials = CreateClient(address, "wrong-token");
            using HttpResponseMessage wrongResponse =
                await wrongCredentials.GetAsync("/operator/gc");
            Assert.Equal(HttpStatusCode.Unauthorized, wrongResponse.StatusCode);

            using var client = CreateClient(address, OperatorToken);
            using JsonDocument ready = JsonDocument.Parse(
                await client.GetStringAsync("/health/ready"));
            Assert.Equal("ready", ready.RootElement.GetProperty("status").GetString());
            Assert.True(ready.RootElement.GetProperty("peerListenerStarted").GetBoolean());
            Assert.True(ready.RootElement.GetProperty("maintenanceSchedulerStarted").GetBoolean());
            Assert.False(ready.RootElement.GetProperty("gcExecutionEnabled").GetBoolean());

            using (var peer = new TcpClient())
            {
                await peer.ConnectAsync(IPAddress.Loopback, peerPort);
                byte[] banner = new byte[64];
                int count = await peer.GetStream().ReadAsync(banner).AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("BIN-SYNC-TEST-HOST/1 READY\n", Encoding.UTF8.GetString(banner, 0, count));
            }

            using JsonDocument initial = JsonDocument.Parse(await client.GetStringAsync("/operator/gc"));
            Assert.Equal(JsonValueKind.Null, initial.RootElement.GetProperty("policy").ValueKind);
            Assert.False(initial.RootElement.GetProperty("receiptStartupProofEnabled").GetBoolean());
            Assert.False(initial.RootElement.GetProperty("changeIdStartupProofEnabled").GetBoolean());
            Assert.False(initial.RootElement.GetProperty("tombstoneStartupProofEnabled").GetBoolean());
            Assert.False(initial.RootElement.GetProperty("runtimeRunning").GetBoolean());

            using HttpResponseMessage maintenance = await client.PostAsJsonAsync(
                "/operator/gc/maintenance/once",
                new { });
            Assert.Equal(HttpStatusCode.OK, maintenance.StatusCode);
            using JsonDocument maintenanceResult =
                JsonDocument.Parse(await maintenance.Content.ReadAsStringAsync());
            Assert.Equal(0, maintenanceResult.RootElement.GetProperty("receiptsReclaimed").GetInt32());
            Assert.Equal(0, maintenanceResult.RootElement.GetProperty("changeIdsReclaimed").GetInt32());
            Assert.Equal(0, maintenanceResult.RootElement.GetProperty("tombstonesReclaimed").GetInt32());
            JsonElement readiness = maintenanceResult.RootElement.GetProperty("readiness");
            Assert.False(readiness.GetProperty("receipts").GetProperty("enabled").GetBoolean());
            Assert.False(readiness.GetProperty("changeIds").GetProperty("enabled").GetBoolean());
            Assert.False(readiness.GetProperty("tombstones").GetProperty("enabled").GetBoolean());

            using HttpResponseMessage updated = await client.PostAsJsonAsync(
                "/operator/gc/policy",
                new
                {
                    expectedGeneration = 0,
                    reason = "initialize disposable integration Host",
                    maintenanceStarted = false,
                    receiptEnabled = false,
                    changeIdEnabled = false,
                    tombstoneEnabled = false
                });
            Assert.Equal(HttpStatusCode.OK, updated.StatusCode);
            using JsonDocument policy1 = JsonDocument.Parse(await updated.Content.ReadAsStringAsync());
            Assert.Equal(1UL, policy1.RootElement.GetProperty("generation").GetUInt64());

            using HttpResponseMessage blockedEnable = await client.PostAsJsonAsync(
                "/operator/gc/policy",
                new
                {
                    expectedGeneration = 1,
                    reason = "must not override immutable startup proof flags",
                    maintenanceStarted = true,
                    receiptEnabled = false,
                    changeIdEnabled = true,
                    tombstoneEnabled = false
                });
            Assert.Equal(HttpStatusCode.Conflict, blockedEnable.StatusCode);

            using HttpResponseMessage stopped = await client.PostAsJsonAsync(
                "/operator/gc/emergency-stop",
                new { reason = "Stage 0 process-restart test: durable all-domain stop" });
            Assert.Equal(HttpStatusCode.OK, stopped.StatusCode);
            using JsonDocument policy2 = JsonDocument.Parse(await stopped.Content.ReadAsStringAsync());
            Assert.Equal(2UL, policy2.RootElement.GetProperty("generation").GetUInt64());
            Assert.False(policy2.RootElement.GetProperty("maintenanceStarted").GetBoolean());
            Assert.False(policy2.RootElement.GetProperty("receiptEnabled").GetBoolean());
            Assert.False(policy2.RootElement.GetProperty("changeIdEnabled").GetBoolean());
            Assert.False(policy2.RootElement.GetProperty("tombstoneEnabled").GetBoolean());

            // Simulate an ungraceful process death after the durable stop commit.
            await KillProcess(runtimeHost);
            runtimeHost = null;

            runtimeHost = StartRuntimeHost(metadataRoot, fldRoot);
            (address, peerPort) = await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45));
            using var restarted = CreateClient(address, OperatorToken);
            using JsonDocument restored = JsonDocument.Parse(await restarted.GetStringAsync("/operator/gc"));
            JsonElement restoredPolicy = restored.RootElement.GetProperty("policy");
            Assert.Equal(2UL, restoredPolicy.GetProperty("generation").GetUInt64());
            Assert.False(restoredPolicy.GetProperty("maintenanceStarted").GetBoolean());
            Assert.False(restoredPolicy.GetProperty("receiptEnabled").GetBoolean());
            Assert.False(restoredPolicy.GetProperty("changeIdEnabled").GetBoolean());
            Assert.False(restoredPolicy.GetProperty("tombstoneEnabled").GetBoolean());
            Assert.False(restored.RootElement.GetProperty("runtimeRunning").GetBoolean());
            Assert.False(restored.RootElement.GetProperty("receiptRuntimeEnabled").GetBoolean());
            Assert.False(restored.RootElement.GetProperty("changeIdRuntimeEnabled").GetBoolean());
            Assert.False(restored.RootElement.GetProperty("tombstoneRuntimeEnabled").GetBoolean());

            using HttpResponseMessage shutdown = await restarted.PostAsJsonAsync(
                "/admin/shutdown",
                new { reason = "finish Stage 0 disposable Host test" });
            Assert.Equal(HttpStatusCode.OK, shutdown.StatusCode);
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(0, runtimeHost.ExitCode);
            runtimeHost.Dispose();
            runtimeHost = null;
        }
        finally
        {
            if (runtimeHost is not null)
                await KillProcess(runtimeHost);
            if (databaseHost is not null)
                await KillProcess(databaseHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Offline_backup_restore_keeps_local_brake_until_new_authorized_policy_generation()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-stage0-backup-restore-" + Guid.NewGuid().ToString("N"));
        string metadataRoot = Path.Combine(root, "metadata");
        string fldRoot = Path.Combine(root, "fld");
        string backupRoot = Path.Combine(root, "offline-backup");
        string externalOperatorRecord = Path.Combine(root, "operator-record.txt");
        Directory.CreateDirectory(root);
        Process? databaseHost = null;
        Process? runtimeHost = null;

        try
        {
            databaseHost = StartDatabaseHost(fldRoot);
            await WaitForDatabaseHost(fldRoot, databaseHost, TimeSpan.FromSeconds(45));

            // Create a durable all-off policy (generation 1), then persist the
            // emergency stop (generation 2) before making a closed offline backup.
            runtimeHost = StartRuntimeHost(metadataRoot, fldRoot);
            Uri address = (await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45))).Address;
            using (var client = CreateClient(address, OperatorToken))
            {
                using HttpResponseMessage policy = await client.PostAsJsonAsync(
                    "/operator/gc/policy",
                    new
                    {
                        expectedGeneration = 0,
                        reason = "prepare all-off offline backup",
                        maintenanceStarted = false,
                        receiptEnabled = false,
                        changeIdEnabled = false,
                        tombstoneEnabled = false
                    });
                Assert.Equal(HttpStatusCode.OK, policy.StatusCode);

                using HttpResponseMessage stop = await client.PostAsJsonAsync(
                    "/operator/gc/emergency-stop",
                    new { reason = "durable stop before offline backup" });
                Assert.Equal(HttpStatusCode.OK, stop.StatusCode);

                using HttpResponseMessage clean = await client.PostAsJsonAsync(
                    "/admin/shutdown-clean",
                    new { reason = "close database before offline backup" });
                Assert.Equal(HttpStatusCode.OK, clean.StatusCode);
            }
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(0, runtimeHost.ExitCode);
            runtimeHost.Dispose();
            runtimeHost = null;

            CopyDirectory(metadataRoot, backupRoot);
            // This record is deliberately outside the database backup. It models
            // the operator's independently retained evidence for restore review.
            await File.WriteAllTextAsync(externalOperatorRecord, "generation=2\nauditSequence=2\nallDomainsOff=true\n");

            // Advance the live database after the backup so restoring the backup
            // demonstrably moves policy generation/audit history backwards.
            runtimeHost = StartRuntimeHost(metadataRoot, fldRoot);
            address = (await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45))).Address;
            using (var client = CreateClient(address, OperatorToken))
            {
                using HttpResponseMessage advance = await client.PostAsJsonAsync(
                    "/operator/gc/policy",
                    new
                    {
                        expectedGeneration = 2,
                        reason = "advance policy after offline backup",
                        maintenanceStarted = false,
                        receiptEnabled = false,
                        changeIdEnabled = false,
                        tombstoneEnabled = false
                    });
                Assert.Equal(HttpStatusCode.OK, advance.StatusCode);
                using JsonDocument advanced = JsonDocument.Parse(await advance.Content.ReadAsStringAsync());
                Assert.Equal(3UL, advanced.RootElement.GetProperty("generation").GetUInt64());

                using HttpResponseMessage clean = await client.PostAsJsonAsync(
                    "/admin/shutdown-clean",
                    new { reason = "close newer database before backup restore" });
                Assert.Equal(HttpStatusCode.OK, clean.StatusCode);
            }
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(0, runtimeHost.ExitCode);
            runtimeHost.Dispose();
            runtimeHost = null;

            Directory.Delete(metadataRoot, recursive: true);
            CopyDirectory(backupRoot, metadataRoot);

            // The restore brake is latched before StartAsync reads the restored
            // (older) policy. RestoreAsync must not clear it; only a fresh,
            // authorized all-off generation may do so.
            runtimeHost = StartRuntimeHost(metadataRoot, fldRoot, backupRestoreBrake: true);
            address = (await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45))).Address;
            using (var client = CreateClient(address, OperatorToken))
            {
                string[] operatorRecord = await File.ReadAllLinesAsync(externalOperatorRecord);
                Assert.Contains("generation=2", operatorRecord);
                Assert.Contains("auditSequence=2", operatorRecord);
                Assert.Contains("allDomainsOff=true", operatorRecord);

                using JsonDocument restored = JsonDocument.Parse(await client.GetStringAsync("/operator/gc"));
                JsonElement oldPolicy = restored.RootElement.GetProperty("policy");
                Assert.Equal(2UL, oldPolicy.GetProperty("generation").GetUInt64());
                Assert.Equal(2UL, oldPolicy.GetProperty("auditSequence").GetUInt64());
                Assert.False(oldPolicy.GetProperty("maintenanceStarted").GetBoolean());
                Assert.False(oldPolicy.GetProperty("receiptEnabled").GetBoolean());
                Assert.False(oldPolicy.GetProperty("changeIdEnabled").GetBoolean());
                Assert.False(oldPolicy.GetProperty("tombstoneEnabled").GetBoolean());
                Assert.False(restored.RootElement.GetProperty("runtimeRunning").GetBoolean());
                Assert.False(restored.RootElement.GetProperty("receiptRuntimeEnabled").GetBoolean());
                Assert.False(restored.RootElement.GetProperty("changeIdRuntimeEnabled").GetBoolean());
                Assert.False(restored.RootElement.GetProperty("tombstoneRuntimeEnabled").GetBoolean());

                using HttpResponseMessage reauthorized = await client.PostAsJsonAsync(
                    "/operator/gc/policy",
                    new
                    {
                        expectedGeneration = 2,
                        reason = "external operational record verified restored backup; reaffirm all-off before re-enable",
                        maintenanceStarted = false,
                        receiptEnabled = false,
                        changeIdEnabled = false,
                        tombstoneEnabled = false
                    });
                Assert.Equal(HttpStatusCode.OK, reauthorized.StatusCode);
                using JsonDocument fresh = JsonDocument.Parse(await reauthorized.Content.ReadAsStringAsync());
                Assert.Equal(3UL, fresh.RootElement.GetProperty("generation").GetUInt64());

                using JsonDocument afterReauthorization = JsonDocument.Parse(
                    await client.GetStringAsync("/operator/gc"));
                Assert.False(afterReauthorization.RootElement.GetProperty("runtimeRunning").GetBoolean());
                Assert.False(afterReauthorization.RootElement.GetProperty("receiptRuntimeEnabled").GetBoolean());
                Assert.False(afterReauthorization.RootElement.GetProperty("changeIdRuntimeEnabled").GetBoolean());
                Assert.False(afterReauthorization.RootElement.GetProperty("tombstoneRuntimeEnabled").GetBoolean());

                using HttpResponseMessage clean = await client.PostAsJsonAsync(
                    "/admin/shutdown-clean",
                    new { reason = "close restored database after reauthorization" });
                Assert.Equal(HttpStatusCode.OK, clean.StatusCode);
            }
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(0, runtimeHost.ExitCode);
            runtimeHost.Dispose();
            runtimeHost = null;
        }
        finally
        {
            if (runtimeHost is not null)
                await KillProcess(runtimeHost);
            if (databaseHost is not null)
                await KillProcess(databaseHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Startup_without_the_FLD_database_host_fails_before_publishing_readiness()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-stage0-runtime-failclosed-" + Guid.NewGuid().ToString("N"));
        string metadataRoot = Path.Combine(root, "metadata");
        string fldRoot = Path.Combine(root, "missing-fld-host");
        Directory.CreateDirectory(root);
        Process process = StartRuntimeHost(metadataRoot, fldRoot);

        try
        {
            Task<string> stdout = process.StandardOutput.ReadToEndAsync();
            Task<string> stderr = process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(25));
            Assert.NotEqual(0, process.ExitCode);
            Assert.DoesNotContain("READY|", await stdout);
            Assert.NotEmpty(await stderr);
        }
        finally
        {
            await KillProcess(process);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static HttpClient CreateClient(Uri address, string token)
    {
        var client = new HttpClient { BaseAddress = address };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        return client;
    }

    private static Process StartRuntimeHost(
        string metadataRoot, string fldRoot, bool backupRestoreBrake = false)
    {
        string runtime = Path.Combine(AppContext.BaseDirectory, "host-runtime", "FldBinSyncHost.Runtime.dll");
        if (!File.Exists(runtime))
            throw new FileNotFoundException("The executable integration Host was not copied to the test output.", runtime);

        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.Environment["BINSYNC_TEST_HOST_OPERATOR_TOKEN"] = OperatorToken;
        var runtimeArgs = new List<string>
        {
            runtime,
            metadataRoot,
            fldRoot,
            MetadataPartition.ToString(CultureInfo.InvariantCulture),
            Convert.ToHexString(LocalAuthor),
            ScopeId,
            MetadataScopeId.ToString("D"),
            NamespaceId.ToString(CultureInfo.InvariantCulture),
            ProviderClientId.ToString("D")
        };
        if (backupRestoreBrake)
            runtimeArgs.Add("--test-backup-restore-brake");
        foreach (string arg in runtimeArgs)
            start.ArgumentList.Add(arg);

        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start FldBinSyncHost.Runtime.");
    }

    private static async Task<(Uri Address, int PeerPort)> WaitForRuntimeHost(
        Process process, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (process.HasExited)
                throw new InvalidOperationException(
                    $"Integration Host exited with {process.ExitCode}: {await process.StandardError.ReadToEndAsync()}");

            TimeSpan remaining = deadline - DateTime.UtcNow;
            string? line;
            try { line = await process.StandardOutput.ReadLineAsync().WaitAsync(remaining); }
            catch (TimeoutException) { break; }

            if (line is null)
                continue;
            if (!line.StartsWith("READY|", StringComparison.Ordinal))
                continue;

            string[] parts = line.Split('|');
            Assert.Equal(3, parts.Length);
            return (new Uri(parts[1]), int.Parse(parts[2], CultureInfo.InvariantCulture));
        }

        throw new TimeoutException("Executable integration Host did not publish its READY line.");
    }

    private static Process StartDatabaseHost(string root)
    {
        AssemblyMetadataAttribute metadata = typeof(FldBinSyncRuntimeHostTests).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(x => x.Key == "Fld1RepositoryRoot");
        string repositoryRoot = metadata.Value
            ?? throw new InvalidOperationException("FLD repository root metadata is empty.");
        string project = Path.Combine(
            repositoryRoot, "src", "Fld1.Database.Host", "Fld1.Database.Host.csproj");

        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        foreach (string arg in new[]
        {
            "run", "--project", project, "--configuration",
#if DEBUG
            "Debug",
#else
            "Release",
#endif
            "--no-build", "--no-launch-profile", "--", root,
            NamespaceId.ToString(CultureInfo.InvariantCulture),
            Convert.ToHexString(LocalAuthor)
        })
            start.ArgumentList.Add(arg);

        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start the FLD Database Host process.");
    }

    private static async Task WaitForDatabaseHost(string root, Process process, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (process.HasExited)
                throw new InvalidOperationException(
                    $"FLD Database Host exited with {process.ExitCode}: {await process.StandardError.ReadToEndAsync()}");
            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, 0)))
            {
                try
                {
                    _ = StorageIpcEndpoint.Read(root, 0);
                    return;
                }
                catch (IOException) { }
                catch (InvalidDataException) { }
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("FLD Database Host did not publish its IPC endpoint.");
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            string relative = Path.GetRelativePath(source, file);
            string target = Path.Combine(destination, relative);
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            File.Copy(file, target, overwrite: true);
        }
    }

    private static async Task KillProcess(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
        }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
        finally { process.Dispose(); }
    }

    private static byte[] Id(byte value) => Enumerable.Repeat(value, 16).ToArray();
}
