using BinSync;
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
using Fld1.Engine.Db;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldBinSyncRuntimeHostTests
{
    [Fact]
    public async Task Idle_tombstone_window_rejects_new_peer_until_atomic_generation_slice_finishes()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-gc-idle-window-" + Guid.NewGuid().ToString("N"));
        string metadataRoot = Path.Combine(root, "metadata");
        string fldRoot = Path.Combine(root, "fld");
        byte[] localNode = Id(0x53);
        byte[] peerNodeId = Id(0x54);
        Process? databaseHost = null;
        Process? runtimeHost = null;
        SyncSession? acceptedSession = null;

        try
        {
            databaseHost = StartDatabaseHost(fldRoot, localNode);
            await WaitForDatabaseHost(fldRoot, databaseHost, TimeSpan.FromSeconds(45));
            runtimeHost = StartRuntimeHost(
                metadataRoot, fldRoot, MetadataPartition, localNode, ScopeId,
                MetadataScopeId, NamespaceId, ProviderClientId, maintenanceIntervalMs: 60000);
            (Uri address, int peerPort) = await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45));
            using var client = CreateClient(address, OperatorToken);
            using var peerNode = new Node(NodeId.From(peerNodeId));

            Task<HttpResponseMessage> windowTask = client.PostAsync(
                "/admin/test/hold-idle-maintenance-window?durationMs=1000", content: null);
            bool windowActive = false;
            for (int attempt = 0; attempt < 40 && !windowActive; attempt++)
            {
                using HttpResponseMessage ready = await client.GetAsync("/health/ready");
                using JsonDocument state = JsonDocument.Parse(await ready.Content.ReadAsStringAsync());
                windowActive = state.RootElement.GetProperty("idleMaintenanceWindowActive").GetBoolean();
                if (!windowActive)
                    await Task.Delay(TimeSpan.FromMilliseconds(25));
            }
            Assert.True(windowActive, "The test Host did not acquire the idle maintenance window.");

            SyncSession? rejectedSession = null;
            try
            {
                rejectedSession = await TcpSync.ConnectAsync(
                    peerNode, "127.0.0.1", peerPort,
                    SessionCapabilities.FullAuthorId |
                    SessionCapabilities.FlowControl |
                    SessionCapabilities.NamespaceRosterV31 |
                    SessionCapabilities.OriginFrontierV31 |
                    SessionCapabilities.FullReplaceV31).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception exception) when (
                exception is IOException or SocketException or ObjectDisposedException or TimeoutException)
            {
                // The acceptor may close the just-negotiated session as soon as it
                // observes the active idle-maintenance fence.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(150));
            await WaitForActivePeer(client, expected: 0, TimeSpan.FromSeconds(2));
            if (rejectedSession is not null)
                await rejectedSession.DisposeAsync();

            using HttpResponseMessage windowResponse = await windowTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(HttpStatusCode.OK, windowResponse.StatusCode);

            // Once the atomic idle window is released, the same peer can connect.
            for (int attempt = 0; attempt < 10 && acceptedSession is null; attempt++)
            {
                try
                {
                    SyncSession candidate = await TcpSync.ConnectAsync(
                        peerNode, "127.0.0.1", peerPort,
                        SessionCapabilities.FullAuthorId |
                        SessionCapabilities.FlowControl |
                        SessionCapabilities.NamespaceRosterV31 |
                        SessionCapabilities.OriginFrontierV31 |
                        SessionCapabilities.FullReplaceV31).WaitAsync(TimeSpan.FromSeconds(3));
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                    using HttpResponseMessage peers = await client.GetAsync("/operator/peers");
                    using JsonDocument state = JsonDocument.Parse(await peers.Content.ReadAsStringAsync());
                    if (state.RootElement.GetProperty("activePeers").GetInt32() == 1)
                        acceptedSession = candidate;
                    else
                        await candidate.DisposeAsync();
                }
                catch (Exception exception) when (
                    exception is IOException or SocketException or ObjectDisposedException or TimeoutException)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                }
            }

            Assert.NotNull(acceptedSession);
            await WaitForActivePeer(client, expected: 1, TimeSpan.FromSeconds(3));
            using HttpResponseMessage activePeerWindow = await client.PostAsync(
                "/admin/test/hold-idle-maintenance-window?durationMs=200", content: null);
            Assert.Equal(HttpStatusCode.Conflict, activePeerWindow.StatusCode);

            using HttpResponseMessage shutdown = await client.PostAsJsonAsync(
                "/admin/shutdown", new { reason = "finish idle Tombstone admission-window integration" });
            Assert.Equal(HttpStatusCode.OK, shutdown.StatusCode);
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(0, runtimeHost.ExitCode);
            runtimeHost.Dispose();
            runtimeHost = null;
        }
        finally
        {
            if (acceptedSession is not null)
                await acceptedSession.DisposeAsync();
            if (runtimeHost is not null)
                await KillProcess(runtimeHost);
            if (databaseHost is not null)
                await KillProcess(databaseHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Active_tcp_peer_survives_host_gc_reference_lease_and_provider_write_resumes_after_release()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-gc-peer-pause-" + Guid.NewGuid().ToString("N"));
        string metadataRoot = Path.Combine(root, "metadata");
        string fldRoot = Path.Combine(root, "fld");
        byte[] localNode = Id(0x53);
        byte[] peerNodeId = Id(0x54);
        byte[] recordId = Id(0x75);
        byte[] changeId = Id(0x76);
        Process? databaseHost = null;
        Process? runtimeHost = null;
        FldDatabaseIpcClient? gcClient = null;

        try
        {
            databaseHost = StartDatabaseHost(fldRoot, localNode);
            await WaitForDatabaseHost(fldRoot, databaseHost, TimeSpan.FromSeconds(45));

            runtimeHost = StartRuntimeHost(
                metadataRoot, fldRoot, MetadataPartition, localNode, ScopeId,
                MetadataScopeId, NamespaceId, ProviderClientId,
                maintenanceIntervalMs: 250);
            (Uri address, int peerPort) = await WaitForRuntimeHost(runtimeHost, TimeSpan.FromSeconds(45));
            // WaitForRuntimeHost owns StandardOutput until it consumes READY. Start
            // draining both redirected streams only after that read has completed.
            Task<string> runtimeStdout = runtimeHost.StandardOutput.ReadToEndAsync();
            Task<string> runtimeStderr = runtimeHost.StandardError.ReadToEndAsync();
            using var client = CreateClient(address, OperatorToken);
            await WaitForMaintenanceTick(client, TimeSpan.FromSeconds(20));

            using var peerNode = new Node(NodeId.From(peerNodeId));
            await using SyncSession peerSession = await TcpSync.ConnectAsync(
                peerNode,
                "127.0.0.1",
                peerPort,
                SessionCapabilities.FullAuthorId |
                SessionCapabilities.FlowControl |
                SessionCapabilities.NamespaceRosterV31 |
                SessionCapabilities.OriginFrontierV31 |
                SessionCapabilities.FullReplaceV31).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(NodeId.From(localNode), peerSession.PeerNodeId);
            await WaitForActivePeer(client, expected: 1, TimeSpan.FromSeconds(5));

            Task<HttpResponseMessage> hostBaselineTask = client.PostAsJsonAsync(
                "/admin/test/complete-peer-baseline", new { });
            Task<(int Prefixes, int FieldsApplied)> peerBaselineTask =
                peerSession.AutoReconcileAsync(
                    peerNode, NamespaceId, initiate: false, maxPrefixes: 16);
            // Bound the two independent sides separately. Task.WhenAll can hide
            // which side stopped making progress and turns a protocol failure into
            // the outer five-minute testhost hang.
            using HttpResponseMessage hostBaseline = await hostBaselineTask
                .WaitAsync(TimeSpan.FromSeconds(15));
            string hostBaselineBody = await hostBaseline.Content.ReadAsStringAsync();
            Assert.True(hostBaseline.StatusCode == HttpStatusCode.OK,
                $"Host baseline endpoint returned {(int)hostBaseline.StatusCode}: {hostBaselineBody}");

            (int baselinePrefixes, int baselineFieldsApplied) = await peerBaselineTask
                .WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(baselinePrefixes >= 0);
            Assert.True(baselineFieldsApplied >= 0);

            // Hold the exact Host-wide FLD reference-scan lease used by physical GC.
            // A live SyncSession remains connected, but provider IPC must not cross
            // the reference snapshot until the scan is revalidated and released.
            gcClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                fldRoot, Guid.NewGuid(), TimeSpan.FromSeconds(10));
            Guid lease = await gcClient.AcquireGcLeaseAsync();
            FldDatabaseIpcGcReferenceScan scan = await gcClient.OpenGcReferenceScanAsync();
            Assert.NotEqual(Guid.Empty, scan.Token);

            var blockedMutation = new
            {
                recordIdHex = Convert.ToHexString(recordId),
                fieldId = (ushort)0x1775,
                changeIdHex = Convert.ToHexString(changeId),
                valueBase64 = Convert.ToBase64String("write-during-live-peer-gc"u8.ToArray()),
                keyEpoch = (ushort)0,
                hlcWall = 987_654_321UL,
                hlcLogical = 1U
            };
            Task<HttpResponseMessage> blockedWrite = client.PostAsJsonAsync(
                "/admin/test/local-change", blockedMutation);

            await Task.Delay(TimeSpan.FromMilliseconds(350));
            Assert.False(blockedWrite.IsCompleted,
                "Provider mutation must remain outside the Host reference-scan critical section.");
            await WaitForActivePeer(client, expected: 1, TimeSpan.FromSeconds(2));

            await gcClient.ReleaseGcReferenceScanAsync(scan.Token);
            await gcClient.ReleaseGcLeaseAsync(lease);

            HttpResponseMessage firstWriteResponse =
                await blockedWrite.WaitAsync(TimeSpan.FromSeconds(8));
            HttpResponseMessage resumedWrite = firstWriteResponse;
            if (firstWriteResponse.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                // A scheduled bounded metadata slice may acquire the Host lease
                // immediately after the explicit test lease. Retry the same
                // ChangeId until that slice releases; the TCP peer stays connected.
                firstWriteResponse.Dispose();
                resumedWrite = await PostJsonWithRetry(
                    client, "/admin/test/local-change", blockedMutation, TimeSpan.FromSeconds(8));
            }
            using (resumedWrite)
            {
                Assert.Equal(HttpStatusCode.OK, resumedWrite.StatusCode);
                using JsonDocument receipt = JsonDocument.Parse(
                    await resumedWrite.Content.ReadAsStringAsync());
                Assert.True(receipt.RootElement.GetProperty("receiptSequence").GetUInt64() > 0);
            }
            

            // The same TCP session must still be live after the bounded GC pause;
            // a follow-up provider write must complete without reconnecting.
            using HttpResponseMessage followUp = await PostJsonWithRetry(
                client,
                "/admin/test/local-change",
                new
                {
                    recordIdHex = Convert.ToHexString(Id(0x77)),
                    fieldId = (ushort)0x1776,
                    changeIdHex = Convert.ToHexString(Id(0x78)),
                    valueBase64 = Convert.ToBase64String("post-gc-peer-still-live"u8.ToArray()),
                    keyEpoch = (ushort)0,
                    hlcWall = 987_654_322UL,
                    hlcLogical = 1U
                },
                TimeSpan.FromSeconds(8));
            Assert.Equal(HttpStatusCode.OK, followUp.StatusCode);
            await WaitForActivePeer(client, expected: 1, TimeSpan.FromSeconds(2));

            // Prove the peer can be closed cleanly before shutting down its Host.
            // DisposeAsync is bounded so a session teardown regression is reported
            // as a focused failure instead of hanging the entire test process.
            await peerSession.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            await WaitForActivePeer(client, expected: 0, TimeSpan.FromSeconds(5));

            using HttpResponseMessage shutdown = await client.PostAsJsonAsync(
                "/admin/shutdown", new { reason = "finish GC/live-peer pause-resume integration" });
            if (shutdown.StatusCode != HttpStatusCode.OK)
            {
                string shutdownBody = await shutdown.Content.ReadAsStringAsync();
                string health = await client.GetStringAsync("/health/ready");
                string peers = await client.GetStringAsync("/operator/peers");
                Assert.True(false,
                    $"Host shutdown returned {(int)shutdown.StatusCode}: {shutdownBody}. " +
                    $"health={health}; peers={peers}");
            }
            await runtimeHost.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            string stdout = await runtimeStdout;
            string stderr = await runtimeStderr;
            Assert.True(runtimeHost.ExitCode == 0,
                $"Runtime Host exited with {runtimeHost.ExitCode}.{Environment.NewLine}" +
                $"STDOUT:{Environment.NewLine}{stdout}{Environment.NewLine}" +
                $"STDERR:{Environment.NewLine}{stderr}");
            runtimeHost.Dispose();
            runtimeHost = null;
        }
        finally
        {
            if (gcClient is not null)
                await gcClient.DisposeAsync();
            if (runtimeHost is not null)
                await KillProcess(runtimeHost);
            if (databaseHost is not null)
                await KillProcess(databaseHost);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

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
    public async Task Two_executable_hosts_exchange_provider_changes_over_tcp_and_reconnect_after_peer_restart()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-two-host-tcp-" + Guid.NewGuid().ToString("N"));
        string metadataA = Path.Combine(root, "metadata-a");
        string metadataB = Path.Combine(root, "metadata-b");
        string fldA = Path.Combine(root, "fld-a");
        string fldB = Path.Combine(root, "fld-b");
        byte[] nodeA = Id(0x61);
        byte[] nodeB = Id(0x62);
        byte[] recordA = Id(0x63);
        byte[] changeA = Id(0x64);
        byte[] recordB = Id(0x65);
        byte[] changeB = Id(0x66);
        const ushort fieldId = 0x1234;
        const string scopeId = "two-host-tcp-integration";
        Guid metadataScope = Guid.Parse("b4ad91e8-9d47-4c8d-ae1f-16f31c7b0021");
        byte[] valueA = "provider-change-from-host-a"u8.ToArray();
        byte[] valueB = "provider-change-from-host-b"u8.ToArray();
        Process? databaseA = null;
        Process? databaseB = null;
        Process? runtimeA = null;
        Process? runtimeB = null;
        FldDatabaseIpcClient? gcClient = null;

        ulong partitionA = 1201;
        ulong partitionB = 1202;
        Guid providerClientA = Guid.NewGuid();
        Guid providerClientB = Guid.NewGuid();

        try
        {
            databaseA = StartDatabaseHost(fldA, nodeA);
            databaseB = StartDatabaseHost(fldB, nodeB);
            await WaitForDatabaseHost(fldA, databaseA, TimeSpan.FromSeconds(45));
            await WaitForDatabaseHost(fldB, databaseB, TimeSpan.FromSeconds(45));

            runtimeA = StartRuntimeHost(
                metadataA, fldA, partitionA, nodeA, scopeId, metadataScope,
                NamespaceId, providerClientA, Convert.ToHexString(nodeB), maintenanceIntervalMs: 5000);
            runtimeB = StartRuntimeHost(
                metadataB, fldB, partitionB, nodeB, scopeId, metadataScope,
                NamespaceId, providerClientB, Convert.ToHexString(nodeA), maintenanceIntervalMs: 5000);

            (Uri addressA, _) = await WaitForRuntimeHost(runtimeA, TimeSpan.FromSeconds(45));
            (Uri addressB, int peerPortB) = await WaitForRuntimeHost(runtimeB, TimeSpan.FromSeconds(45));
            using var clientA = CreateClient(addressA, OperatorToken);
            using var clientB = CreateClient(addressB, OperatorToken);
            await WaitForMaintenanceTick(clientA, TimeSpan.FromSeconds(20));
            await WaitForMaintenanceTick(clientB, TimeSpan.FromSeconds(20));

            using HttpResponseMessage connect = await clientA.PostAsJsonAsync(
                "/admin/peers/connect",
                new { host = "127.0.0.1", port = peerPortB });
            Assert.Equal(HttpStatusCode.OK, connect.StatusCode);
            await WaitForActivePeer(clientA, expected: 1, TimeSpan.FromSeconds(10));
            await WaitForActivePeer(clientB, expected: 1, TimeSpan.FromSeconds(10));

            using HttpResponseMessage verifyEmptyA = await PostJsonWithRetry(
                clientA, "/admin/test/verify-empty-namespace", new { }, TimeSpan.FromSeconds(10));
            using HttpResponseMessage verifyEmptyB = await PostJsonWithRetry(
                clientB, "/admin/test/verify-empty-namespace", new { }, TimeSpan.FromSeconds(10));
            Assert.Equal(HttpStatusCode.OK, verifyEmptyA.StatusCode);
            Assert.Equal(HttpStatusCode.OK, verifyEmptyB.StatusCode);

            HttpResponseMessage[] initialBaselineResponses = await Task.WhenAll(
                clientA.PostAsJsonAsync("/admin/test/complete-peer-baseline", new { }),
                clientB.PostAsJsonAsync("/admin/test/complete-peer-baseline", new { }));
            try
            {
                Assert.Equal(HttpStatusCode.OK, initialBaselineResponses[0].StatusCode);
                Assert.Equal(HttpStatusCode.OK, initialBaselineResponses[1].StatusCode);
            }
            finally
            {
                foreach (HttpResponseMessage response in initialBaselineResponses)
                    response.Dispose();
            }

            // Final GC regression: hold the real FLD reference-scan lease while
            // both executable Hosts keep the same TCP SyncSession established.
            // A provider mutation must wait; after release it must durably apply
            // remotely and its outbound stream must disappear only after ACK.
            gcClient = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                fldA, Guid.NewGuid(), TimeSpan.FromSeconds(10));
            Guid gcLease = await gcClient.AcquireGcLeaseAsync();
            FldDatabaseIpcGcReferenceScan gcScan = await gcClient.OpenGcReferenceScanAsync();
            Assert.NotEqual(Guid.Empty, gcScan.Token);

            var postLeaseMutation = new
            {
                recordIdHex = Convert.ToHexString(recordA),
                fieldId,
                changeIdHex = Convert.ToHexString(changeA),
                valueBase64 = Convert.ToBase64String(valueA),
                keyEpoch = (ushort)0,
                hlcWall = 123_456_789UL,
                hlcLogical = 1U
            };
            Task<HttpResponseMessage> heldWrite = clientA.PostAsJsonAsync(
                "/admin/test/local-change", postLeaseMutation);
            await Task.Delay(TimeSpan.FromMilliseconds(300));
            Assert.False(heldWrite.IsCompleted,
                "Provider mutation crossed the active GC reference-scan lease.");
            await WaitForActivePeer(clientA, expected: 1, TimeSpan.FromSeconds(2));
            await WaitForActivePeer(clientB, expected: 1, TimeSpan.FromSeconds(2));

            await gcClient.ReleaseGcReferenceScanAsync(gcScan.Token);
            await gcClient.ReleaseGcLeaseAsync(gcLease);
            using HttpResponseMessage writeA = await heldWrite.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(HttpStatusCode.OK, writeA.StatusCode);
            using JsonDocument receiptA = JsonDocument.Parse(await writeA.Content.ReadAsStringAsync());
            Assert.True(receiptA.RootElement.GetProperty("receiptSequence").GetUInt64() > 0);

            using JsonDocument receivedAtB = await WaitForCurrentMutation(
                clientB, Convert.ToHexString(recordA), fieldId, TimeSpan.FromSeconds(20), peer: clientA);
            Assert.Equal(Convert.ToBase64String(valueA),
                receivedAtB.RootElement.GetProperty("valueBase64").GetString());
            Assert.Equal(Convert.ToHexString(changeA),
                receivedAtB.RootElement.GetProperty("changeIdHex").GetString());
            Assert.Equal(Convert.ToHexString(nodeA),
                receivedAtB.RootElement.GetProperty("authorNodeIdHex").GetString());

            // Remote durable apply is necessary but not sufficient: wait until
            // sender Metadata has committed the delivery ACK and no outbound
            // author stream remains. Both peer counts must stay at one throughout.
            DateTime ackDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            bool deliveryAcked = false;
            while (DateTime.UtcNow < ackDeadline)
            {
                using JsonDocument pending = JsonDocument.Parse(
                    await clientA.GetStringAsync("/admin/test/pending-outbound"));
                if (pending.RootElement.GetProperty("pendingOutboundBatches").GetInt32() == 0)
                {
                    deliveryAcked = true;
                    break;
                }
                await WaitForActivePeer(clientA, expected: 1, TimeSpan.FromSeconds(2));
                await WaitForActivePeer(clientB, expected: 1, TimeSpan.FromSeconds(2));
                await Task.Delay(100);
            }
            Assert.True(deliveryAcked,
                "The sender did not durably retire its outbound stream after the remote apply ACK.");
            await WaitForActivePeer(clientA, expected: 1, TimeSpan.FromSeconds(2));
            await WaitForActivePeer(clientB, expected: 1, TimeSpan.FromSeconds(2));

            using HttpResponseMessage writeB = await PostJsonWithRetry(
                clientB, "/admin/test/local-change",
                new
                {
                    recordIdHex = Convert.ToHexString(recordB),
                    fieldId,
                    changeIdHex = Convert.ToHexString(changeB),
                    valueBase64 = Convert.ToBase64String(valueB),
                    keyEpoch = (ushort)0,
                    hlcWall = 123_456_790UL,
                    hlcLogical = 1U
                },
                TimeSpan.FromSeconds(10));
            Assert.Equal(HttpStatusCode.OK, writeB.StatusCode);

            using JsonDocument receivedAtA = await WaitForCurrentMutation(
                clientA, Convert.ToHexString(recordB), fieldId, TimeSpan.FromSeconds(20), peer: clientB);
            Assert.Equal(Convert.ToBase64String(valueB),
                receivedAtA.RootElement.GetProperty("valueBase64").GetString());
            Assert.Equal(Convert.ToHexString(changeB),
                receivedAtA.RootElement.GetProperty("changeIdHex").GetString());
            Assert.Equal(Convert.ToHexString(nodeB),
                receivedAtA.RootElement.GetProperty("authorNodeIdHex").GetString());

            // Restart one executable Host while leaving its FLD Database Host and
            // the remote peer alive. The durable Node/Metadata state must reopen,
            // and the peer must be able to establish a fresh TCP SyncSession.
            using HttpResponseMessage shutdownB = await clientB.PostAsJsonAsync(
                "/admin/shutdown",
                new { reason = "two-host TCP integration restart" });
            Assert.Equal(HttpStatusCode.OK, shutdownB.StatusCode);
            await runtimeB.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            runtimeB.Dispose();
            runtimeB = null;

            await WaitForActivePeer(clientA, expected: 0, TimeSpan.FromSeconds(10));

            runtimeB = StartRuntimeHost(
                metadataB, fldB, partitionB, nodeB, scopeId, metadataScope,
                NamespaceId, providerClientB, Convert.ToHexString(nodeA));
            (addressB, peerPortB) = await WaitForRuntimeHost(runtimeB, TimeSpan.FromSeconds(45));
            clientB.Dispose();
            using var restartedClientB = CreateClient(addressB, OperatorToken);
            await WaitForMaintenanceTick(restartedClientB, TimeSpan.FromSeconds(20));

            using HttpResponseMessage reconnect = await clientA.PostAsJsonAsync(
                "/admin/peers/connect",
                new { host = "127.0.0.1", port = peerPortB });
            Assert.Equal(HttpStatusCode.OK, reconnect.StatusCode);
            await WaitForActivePeer(clientA, expected: 1, TimeSpan.FromSeconds(10));
            await WaitForActivePeer(restartedClientB, expected: 1, TimeSpan.FromSeconds(10));

            HttpResponseMessage[] resumedBaselineResponses = await Task.WhenAll(
                clientA.PostAsJsonAsync("/admin/test/complete-peer-baseline", new { }),
                restartedClientB.PostAsJsonAsync("/admin/test/complete-peer-baseline", new { }));
            try
            {
                Assert.Equal(HttpStatusCode.OK, resumedBaselineResponses[0].StatusCode);
                Assert.Equal(HttpStatusCode.OK, resumedBaselineResponses[1].StatusCode);
            }
            finally
            {
                foreach (HttpResponseMessage response in resumedBaselineResponses)
                    response.Dispose();
            }

            using JsonDocument persistedAtB = await WaitForCurrentMutation(
                restartedClientB, Convert.ToHexString(recordA), fieldId, TimeSpan.FromSeconds(10));
            Assert.Equal(Convert.ToBase64String(valueA),
                persistedAtB.RootElement.GetProperty("valueBase64").GetString());
            using JsonDocument persistedAtA = await WaitForCurrentMutation(
                clientA, Convert.ToHexString(recordB), fieldId, TimeSpan.FromSeconds(10));
            Assert.Equal(Convert.ToBase64String(valueB),
                persistedAtA.RootElement.GetProperty("valueBase64").GetString());

            using HttpResponseMessage cleanA = await clientA.PostAsJsonAsync(
                "/admin/shutdown", new { reason = "finish two-host TCP integration" });
            Assert.Equal(HttpStatusCode.OK, cleanA.StatusCode);
            await runtimeA.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            runtimeA.Dispose();
            runtimeA = null;

            using HttpResponseMessage cleanB = await restartedClientB.PostAsJsonAsync(
                "/admin/shutdown", new { reason = "finish restarted two-host TCP integration" });
            Assert.Equal(HttpStatusCode.OK, cleanB.StatusCode);
            await runtimeB.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            runtimeB.Dispose();
            runtimeB = null;
        }
        finally
        {
            if (gcClient is not null) await gcClient.DisposeAsync();
            if (runtimeA is not null) await KillProcess(runtimeA);
            if (runtimeB is not null) await KillProcess(runtimeB);
            if (databaseA is not null) await KillProcess(databaseA);
            if (databaseB is not null) await KillProcess(databaseB);
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Executable_host_establishes_real_tcp_sync_session_and_restores_durable_stop_after_process_restart()
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

            runtimeHost = StartRuntimeHost(
                metadataRoot, fldRoot, MetadataPartition, LocalAuthor, ScopeId,
                MetadataScopeId, NamespaceId, ProviderClientId,
                maintenanceIntervalMs: 5000);
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
            await WaitForMaintenanceTick(client, TimeSpan.FromSeconds(20));

            using var peerNode = new Node(NodeId.From(Id(0x54)));
            await using (SyncSession peerSession = await TcpSync.ConnectAsync(
                peerNode,
                "127.0.0.1",
                peerPort,
                SessionCapabilities.FullAuthorId |
                SessionCapabilities.FlowControl |
                SessionCapabilities.NamespaceRosterV31 |
                SessionCapabilities.OriginFrontierV31 |
                SessionCapabilities.FullReplaceV31).WaitAsync(TimeSpan.FromSeconds(5)))
            {
                Assert.Equal(NodeId.From(LocalAuthor), peerSession.PeerNodeId);
                await WaitForActivePeer(client, expected: 1, TimeSpan.FromSeconds(5));

                Task<HttpResponseMessage> hostBaselineTask = client.PostAsJsonAsync(
                    "/admin/test/complete-peer-baseline", new { });
                Task<(int Prefixes, int FieldsApplied)> peerBaselineTask =
                    peerSession.AutoReconcileAsync(
                        peerNode, NamespaceId, initiate: false, maxPrefixes: 16);
                await Task.WhenAll(hostBaselineTask, peerBaselineTask);
                using HttpResponseMessage hostBaseline = await hostBaselineTask;
                Assert.Equal(HttpStatusCode.OK, hostBaseline.StatusCode);
                (int prefixes, int fieldsApplied) = await peerBaselineTask;
                Assert.Equal(0, prefixes);
                Assert.Equal(0, fieldsApplied);
            }
            await WaitForActivePeer(client, expected: 0, TimeSpan.FromSeconds(5));

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

            await Task.Delay(TimeSpan.FromMilliseconds(1100));
            using JsonDocument schedulerTick = JsonDocument.Parse(
                await client.GetStringAsync("/health/ready"));
            Assert.True(schedulerTick.RootElement.GetProperty("maintenanceTicks").GetInt64() > 0);

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

    private static async Task<HttpResponseMessage> PostJsonWithRetry(
        HttpClient client, string path, object payload, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            HttpResponseMessage response = await client.PostAsJsonAsync(path, payload);
            if (response.StatusCode != HttpStatusCode.ServiceUnavailable)
                return response;
            response.Dispose();
            await Task.Delay(100);
        }

        throw new TimeoutException($"Test write endpoint {path} remained unavailable during GC reference scans.");
    }

    private static async Task<JsonDocument> WaitForCurrentMutation(
        HttpClient client, string recordIdHex, ushort fieldId, TimeSpan timeout, HttpClient? peer = null)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        string path = $"/admin/test/current/{recordIdHex}/{fieldId.ToString(CultureInfo.InvariantCulture)}";
        while (DateTime.UtcNow < deadline)
        {
            using HttpResponseMessage response = await client.GetAsync(path);
            if (response.StatusCode == HttpStatusCode.OK)
                return JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            if (response.StatusCode is not (HttpStatusCode.NotFound or HttpStatusCode.ServiceUnavailable))
                throw new InvalidOperationException(
                    $"Current-value probe failed with {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}");
            await Task.Delay(100);
        }

        string health = await client.GetStringAsync("/health/ready");
        string peers = await client.GetStringAsync("/operator/peers");
        string remotePeers = peer is null
            ? string.Empty
            : await peer.GetStringAsync("/operator/peers");
        throw new TimeoutException(
            $"Remote FLD value {recordIdHex}/{fieldId} was not observed. " +
            $"receiverHealth={health}; receiverPeers={peers}; senderPeers={remotePeers}");
    }

    private static async Task WaitForMaintenanceTick(HttpClient client, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            using JsonDocument ready = JsonDocument.Parse(await client.GetStringAsync("/health/ready"));
            if (ready.RootElement.GetProperty("maintenanceSchedulerStarted").GetBoolean() &&
                ready.RootElement.GetProperty("maintenanceTicks").GetInt64() >= 1)
                return;
            await Task.Delay(50);
        }

        throw new TimeoutException("The Host did not complete its first GC maintenance round.");
    }

    private static async Task WaitForActivePeer(HttpClient client, int expected, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            using JsonDocument ready = JsonDocument.Parse(await client.GetStringAsync("/health/ready"));
            if (ready.RootElement.GetProperty("activePeers").GetInt32() == expected)
                return;
            await Task.Delay(50);
        }

        throw new TimeoutException($"Host did not reach activePeers={expected}.");
    }

    private static HttpClient CreateClient(Uri address, string token)
    {
        var client = new HttpClient { BaseAddress = address };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        return client;
    }

    private static Process StartRuntimeHost(
        string metadataRoot, string fldRoot, bool backupRestoreBrake = false)
        => StartRuntimeHost(
            metadataRoot, fldRoot, MetadataPartition, LocalAuthor, ScopeId,
            MetadataScopeId, NamespaceId, ProviderClientId,
            allowedPeerIds: null, backupRestoreBrake: backupRestoreBrake);

    private static Process StartRuntimeHost(
        string metadataRoot,
        string fldRoot,
        ulong metadataPartition,
        byte[] nodeId,
        string scopeId,
        Guid metadataScopeId,
        ushort namespaceId,
        Guid providerClientId,
        string? allowedPeerIds = null,
        bool backupRestoreBrake = false,
        int maintenanceIntervalMs = 1000)
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
        if (allowedPeerIds is not null)
            start.Environment["BINSYNC_TEST_HOST_ALLOWED_PEERS"] = allowedPeerIds;
        start.Environment["BINSYNC_TEST_HOST_MAINTENANCE_INTERVAL_MS"] =
            maintenanceIntervalMs.ToString(CultureInfo.InvariantCulture);
        var runtimeArgs = new List<string>
        {
            runtime,
            metadataRoot,
            fldRoot,
            metadataPartition.ToString(CultureInfo.InvariantCulture),
            Convert.ToHexString(nodeId),
            scopeId,
            metadataScopeId.ToString("D"),
            namespaceId.ToString(CultureInfo.InvariantCulture),
            providerClientId.ToString("D")
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
        => StartDatabaseHost(root, LocalAuthor);

    private static Process StartDatabaseHost(string root, byte[] localAuthor)
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
            Convert.ToHexString(localAuthor)
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
