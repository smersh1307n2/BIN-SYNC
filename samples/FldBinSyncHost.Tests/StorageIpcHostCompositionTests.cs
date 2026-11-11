using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipes;
using Fld1.Engine.Db;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class StorageIpcHostCompositionTests
{
    [Fact]
    public async Task Local_ipc_request_commits_through_host_handler_into_real_fld_database()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-" + Guid.NewGuid().ToString("N"));
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        const uint namespaceId = 31;
        byte[] localAuthor = Id(0x11);
        byte[] recordId = Id(0x22);
        const ushort fieldId = 0x0317;
        const ushort keyEpoch = 4;
        byte[] value = "ipc-to-fld-durable-write"u8.ToArray();
        var requestIds = new ConcurrentQueue<ulong>();
        var operationIds = new ConcurrentQueue<Guid>();

        try
        {
            using var database = FldDatabase.Open(directory, namespaceId, localAuthor);
            using var serverCancellation = new CancellationTokenSource();
            var server = new StorageIpcServer(
                pipeName,
                (context, request, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (context.ClientId == Guid.Empty || context.SessionGeneration == 0 ||
                        context.RequestId == 0)
                        throw new InvalidDataException("IPC request context is not authenticated.");
                    requestIds.Enqueue(context.RequestId);
                    StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(request);
                    if (operation.OperationCode != 1)
                        throw new InvalidDataException("Unknown FLD host operation code.");
                    operationIds.Enqueue(operation.OperationId);
                    ReadOnlyMemory<byte> operationPayload = operation.Payload;

                    if (operationPayload.Length < 20)
                        throw new InvalidDataException("FLD write request is truncated.");

                    byte[] requestedRecord = operationPayload.Slice(0, 16).ToArray();
                    ushort requestedField = BinaryPrimitives.ReadUInt16LittleEndian(operationPayload.Span[16..]);
                    ushort requestedKeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(operationPayload.Span[18..]);
                    byte[] requestedValue = operationPayload[20..].ToArray();

                    database.Put(requestedRecord, requestedField, requestedValue, requestedKeyEpoch);
                    _ = database.Commit();
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(requestedRecord);
                },
                maxConcurrentClients: 1);
            Task serverTask = server.RunAsync(serverCancellation.Token);

            try
            {
                await using var client = await StorageIpcClientConnection.ConnectAsync(
                    pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10));
                Exception overload = await Assert.ThrowsAnyAsync<Exception>(async () =>
                    await StorageIpcClientConnection.ConnectAsync(
                        pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
                Assert.True(overload is IOException or InvalidDataException,
                    $"Expected a transport-level overload rejection, received {overload.GetType().Name}.");

                byte[] request = new byte[20 + value.Length];
                recordId.CopyTo(request, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(request.AsSpan(16), fieldId);
                BinaryPrimitives.WriteUInt16LittleEndian(request.AsSpan(18), keyEpoch);
                value.CopyTo(request, 20);

                byte[] firstOperation = StorageIpcOperationProtocol.Encode(
                    new StorageIpcOperationEnvelope(
                        Guid.Parse("31000000-0000-0000-0000-000000000001"), 1, request));
                ReadOnlyMemory<byte> response = await client.RequestAsync(firstOperation);
                Assert.Equal(recordId, response.ToArray());

                byte[] secondValue = "same-session-after-overload"u8.ToArray();
                byte[] secondRequest = new byte[20 + secondValue.Length];
                recordId.CopyTo(secondRequest, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(16), checked((ushort)(fieldId + 1)));
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(18), keyEpoch);
                secondValue.CopyTo(secondRequest, 20);
                byte[] secondOperation = StorageIpcOperationProtocol.Encode(
                    new StorageIpcOperationEnvelope(
                        Guid.Parse("31000000-0000-0000-0000-000000000002"), 1, secondRequest));

                ReadOnlyMemory<byte> secondResponse = await client.RequestAsync(secondOperation);
                Assert.Equal(recordId, secondResponse.ToArray());
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            }

            Assert.True(database.TryGetBytes(recordId, fieldId, out byte[]? stored));
            Assert.Equal(value, stored);
            Assert.True(database.TryGetBytes(recordId, checked((ushort)(fieldId + 1)), out byte[]? storedAfterOverload));
            Assert.Equal("same-session-after-overload"u8.ToArray(), storedAfterOverload);
            Assert.Equal(new ulong[] { 1, 2 }, requestIds.ToArray());
            Assert.Equal(new[]
            {
                Guid.Parse("31000000-0000-0000-0000-000000000001"),
                Guid.Parse("31000000-0000-0000-0000-000000000002")
            }, operationIds.ToArray());
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Previous_ipc_connection_generation_is_rejected_before_any_fld_write()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-fence-" + Guid.NewGuid().ToString("N"));
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        const uint namespaceId = 32;
        byte[] localAuthor = Id(0x31);
        byte[] recordId = Id(0x32);
        const ushort fieldId = 0x0321;
        Guid stableClientId = Guid.NewGuid();

        try
        {
            using var database = FldDatabase.Open(directory, namespaceId, localAuthor);
            using var serverCancellation = new CancellationTokenSource();
            int handlerCalls = 0;
            var server = new StorageIpcServer(
                pipeName,
                (context, request, cancellationToken) =>
                {
                    Interlocked.Increment(ref handlerCalls);
                    database.Put(recordId, fieldId, request.ToArray(), 1);
                    _ = database.Commit();
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(recordId);
                });
            Task serverTask = server.RunAsync(serverCancellation.Token);
            ulong previousGeneration;

            try
            {
                await using (var previousPipe = new NamedPipeClientStream(
                    ".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous))
                {
                    await previousPipe.ConnectAsync(10_000);
                    await StorageIpcProtocol.WriteAsync(previousPipe,
                        new StorageIpcFrame(
                            StorageIpcFrameType.Hello, 0, 0, stableClientId.ToByteArray()));
                    StorageIpcFrame? previousAck = await StorageIpcProtocol.ReadAsync(previousPipe);
                    Assert.NotNull(previousAck);
                    Assert.Equal(StorageIpcFrameType.HelloAck, previousAck.Value.Type);
                    previousGeneration = previousAck.Value.SessionGeneration;
                }

                await using var currentPipe = new NamedPipeClientStream(
                    ".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
                await currentPipe.ConnectAsync(10_000);
                await StorageIpcProtocol.WriteAsync(currentPipe,
                    new StorageIpcFrame(
                        StorageIpcFrameType.Hello, 0, 0, stableClientId.ToByteArray()));
                StorageIpcFrame? currentAck = await StorageIpcProtocol.ReadAsync(currentPipe);
                Assert.NotNull(currentAck);
                Assert.Equal(StorageIpcFrameType.HelloAck, currentAck.Value.Type);
                Assert.NotEqual(previousGeneration, currentAck.Value.SessionGeneration);

                await StorageIpcProtocol.WriteAsync(currentPipe,
                    new StorageIpcFrame(
                        StorageIpcFrameType.Request, 1, previousGeneration, "must-not-apply"u8.ToArray()));

                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                StorageIpcFrame? response = null;
                Exception? transportFailure = null;
                try
                {
                    response = await StorageIpcProtocol.ReadAsync(currentPipe, timeout.Token);
                }
                catch (Exception exception) when (exception is IOException or ObjectDisposedException)
                {
                    transportFailure = exception;
                }

                Assert.True(response is null || transportFailure is not null,
                    "A previous connection generation must be rejected after reconnect.");
                Assert.Equal(0, Volatile.Read(ref handlerCalls));
                Assert.False(database.TryGetBytes(recordId, fieldId, out _));
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_ipc_operation_ledger_replays_durable_result_and_rejects_conflicting_identity()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-ledger-" + Guid.NewGuid().ToString("N"));
        string foreignDirectory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-ledger-foreign-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x3103;
        Guid clientId = Guid.Parse("31030000-0000-0000-0000-000000000001");
        Guid operationId = Guid.Parse("31030000-0000-0000-0000-000000000002");
        byte[] dataKey = "fld-host-storage-key"u8.ToArray();
        byte[] dataValue = "durable-storage-value"u8.ToArray();
        byte[] requestPayload = "put-storage-value"u8.ToArray();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(3, requestPayload);
        byte[] result = "stored"u8.ToArray();

        try
        {
            using (var storage = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(storage);
                using var batch = storage.BeginWriteBatch();
                batch.Put(dataKey, dataValue);
                Assert.True(ledger.TryStageResult(
                    batch, clientId, operationId, fingerprint, result, out byte[]? prior));
                Assert.Null(prior);
                batch.Commit();

                using var foreign = StorageDatabase.Open(foreignDirectory, partition);
                using var foreignBatch = foreign.BeginWriteBatch();
                Assert.Throws<ArgumentException>(() => ledger.TryStageResult(
                    foreignBatch, clientId, Guid.NewGuid(), fingerprint, result, out _));
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(reopened);
                Assert.True(reopened.TryGet(dataKey, out byte[]? stored));
                Assert.Equal(dataValue, stored);
                Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? replayed));
                Assert.Equal(result, replayed);

                using var duplicate = reopened.BeginWriteBatch();
                Assert.False(ledger.TryStageResult(
                    duplicate, clientId, operationId, fingerprint,
                    "different-local-result"u8, out byte[]? prior));
                Assert.Equal(result, prior);

                byte[] conflictingFingerprint =
                    StorageIpcOperationLedger.ComputeFingerprint(3, "different-request"u8);
                Assert.Throws<InvalidOperationException>(() =>
                    ledger.TryGetResult(clientId, operationId, conflictingFingerprint, out _));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
            try { Directory.Delete(foreignDirectory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_ipc_ledger_rejects_concurrent_duplicate_operation_effects()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-ledger-race-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x3105;
        Guid clientId = Guid.NewGuid();
        Guid operationId = Guid.NewGuid();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(5, "same-operation"u8);

        try
        {
            using var storage = StorageDatabase.Open(directory, partition);
            var ledger = new StorageIpcOperationLedger(storage);
            using var first = storage.BeginWriteBatch();
            using var second = storage.BeginWriteBatch();

            first.Put("first-effect"u8, "committed"u8);
            second.Put("second-effect"u8, "must-not-commit"u8);
            Assert.True(ledger.TryStageResult(
                first, clientId, operationId, fingerprint, "durable-result"u8, out _));
            Assert.True(ledger.TryStageResult(
                second, clientId, operationId, fingerprint, "durable-result"u8, out _));

            first.Commit();
            Assert.Throws<StorageConflictException>(() => second.Commit());

            Assert.True(storage.TryGet("first-effect"u8, out byte[]? firstValue));
            Assert.Equal("committed"u8.ToArray(), firstValue);
            Assert.False(storage.TryGet("second-effect"u8, out _));
            Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? replayed));
            Assert.Equal("durable-result"u8.ToArray(), replayed);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Storage_ipc_replays_committed_operation_after_response_loss_and_host_restart()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-ledger-restart-" + Guid.NewGuid().ToString("N"));
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        const ulong partition = 0x3106;
        Guid clientId = Guid.Parse("31060000-0000-0000-0000-000000000001");
        Guid operationId = Guid.Parse("31060000-0000-0000-0000-000000000002");
        byte[] dataKey = "restart-proof-key"u8.ToArray();
        byte[] dataValue = "one-durable-effect"u8.ToArray();
        byte[] requestPayload = "write-restart-proof"u8.ToArray();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(6, requestPayload);
        byte[] result = "durably-committed"u8.ToArray();
        byte[] envelope = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(operationId, 6, requestPayload));

        try
        {
            using (var storage = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(storage);
                using var serverCancellation = new CancellationTokenSource();
                var server = new StorageIpcServer(
                    pipeName,
                    (context, request, cancellationToken) =>
                    {
                        StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(request);
                        byte[] requestFingerprint = StorageIpcOperationLedger.ComputeFingerprint(
                            operation.OperationCode, operation.Payload.Span);
                        if (ledger.TryGetResult(
                            context.ClientId, operation.OperationId, requestFingerprint, out byte[]? prior))
                            return ValueTask.FromResult<ReadOnlyMemory<byte>>(prior!);

                        using var batch = storage.BeginWriteBatch();
                        batch.Put(dataKey, dataValue);
                        if (!ledger.TryStageResult(
                            batch, context.ClientId, operation.OperationId,
                            requestFingerprint, result, out prior))
                            return ValueTask.FromResult<ReadOnlyMemory<byte>>(prior!);

                        batch.Commit();
                        // Simulate process shutdown after the durable commit but before
                        // the response frame can be written to the connected client.
                        serverCancellation.Cancel();
                        return ValueTask.FromResult<ReadOnlyMemory<byte>>(result);
                    });
                Task serverTask = server.RunAsync(serverCancellation.Token);

                try
                {
                    await using var client = await StorageIpcClientConnection.ConnectAsync(
                        pipeName, clientId, TimeSpan.FromSeconds(10));
                    Exception lostResponse = await Assert.ThrowsAnyAsync<Exception>(async () =>
                        await client.RequestAsync(envelope));
                    Assert.True(lostResponse is IOException or InvalidDataException or ObjectDisposedException,
                        $"Expected response loss after durable commit, received {lostResponse.GetType().Name}.");
                }
                finally
                {
                    serverCancellation.Cancel();
                    await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(1UL, storage.LastCommitSeq);
                Assert.True(storage.TryGet(dataKey, out byte[]? firstStored));
                Assert.Equal(dataValue, firstStored);
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(reopened);
                using var serverCancellation = new CancellationTokenSource();
                var server = new StorageIpcServer(
                    pipeName,
                    (context, request, cancellationToken) =>
                    {
                        StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(request);
                        byte[] requestFingerprint = StorageIpcOperationLedger.ComputeFingerprint(
                            operation.OperationCode, operation.Payload.Span);
                        if (!ledger.TryGetResult(
                            context.ClientId, operation.OperationId, requestFingerprint, out byte[]? prior))
                            throw new InvalidOperationException("The committed operation result was not recovered.");
                        return ValueTask.FromResult<ReadOnlyMemory<byte>>(prior!);
                    });
                Task serverTask = server.RunAsync(serverCancellation.Token);

                try
                {
                    await using var reconnectedClient = await StorageIpcClientConnection.ConnectAsync(
                        pipeName, clientId, TimeSpan.FromSeconds(10));
                    ReadOnlyMemory<byte> replayed = await reconnectedClient.RequestAsync(envelope);
                    Assert.Equal(result, replayed.ToArray());
                }
                finally
                {
                    serverCancellation.Cancel();
                    await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(1UL, reopened.LastCommitSeq);
                Assert.True(reopened.TryGet(dataKey, out byte[]? recoveredData));
                Assert.Equal(dataValue, recoveredData);
                Assert.True(ledger.TryGetResult(
                    clientId, operationId, fingerprint, out byte[]? recoveredResult));
                Assert.Equal(result, recoveredResult);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Storage_ipc_server_dispatches_put_get_delete_and_replays_after_reconnect()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-dispatch-" + Guid.NewGuid().ToString("N"));
        string pipeName = StorageIpcEndpoint.ComputePipeName(directory, partition);
        const ulong partition = 0x3107;
        Guid clientId = Guid.NewGuid();
        Guid ownerId = Guid.NewGuid();
        Guid putOperationId = Guid.NewGuid();
        Guid deleteOperationId = Guid.NewGuid();
        byte[] key = "host-dispatch-key"u8.ToArray();
        byte[] value = "host-dispatch-value"u8.ToArray();

        byte[] putPayload = new byte[8 + key.Length + value.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(putPayload, (uint)key.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(putPayload.AsSpan(4), (uint)value.Length);
        key.CopyTo(putPayload, 8);
        value.CopyTo(putPayload, 8 + key.Length);
        byte[] putEnvelope = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(putOperationId, StorageIpcOperationCodes.Put, putPayload));
        byte[] getEnvelope = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(Guid.NewGuid(), StorageIpcOperationCodes.Get, key));
        byte[] deleteEnvelope = StorageIpcOperationProtocol.Encode(
            new StorageIpcOperationEnvelope(deleteOperationId, StorageIpcOperationCodes.Delete, key));

        try
        {
            using var storage = StorageDatabase.Open(directory, partition);
            var dispatcher = new StorageIpcOperationDispatcher(storage);
            using var serverCancellation = new CancellationTokenSource();
            var server = new StorageIpcServer(pipeName, dispatcher.HandleAsync);
            Task serverTask = server.RunAsync(serverCancellation.Token);

            try
            {
                await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
                StorageIpcEndpoint.Publish(directory, StorageIpcEndpoint.Create(
                    directory, partition, ownerId, Environment.ProcessId));
                StorageIpcEndpointInfo publishedEndpoint = StorageIpcEndpoint.Read(directory, partition);
                Assert.Equal(ownerId, publishedEndpoint.OwnerId);
                Assert.Equal(pipeName, publishedEndpoint.PipeName);
                Assert.Equal(
                    $".fld1-storage-ipc-{partition:X16}.endpoint",
                    Path.GetFileName(StorageIpcEndpoint.GetEndpointFilePath(directory, partition)));

                await using (var client = await StorageIpcClientConnection.ConnectToDatabaseAsync(
                    directory, partition, clientId, TimeSpan.FromSeconds(10)))
                {
                    ReadOnlyMemory<byte> putResult = await client.RequestAsync(putEnvelope);
                    Assert.Equal(new byte[] { 1 }, putResult.ToArray());
                }

                await using (var reconnected = await StorageIpcClientConnection.ConnectToDatabaseAsync(
                    directory, partition, clientId, TimeSpan.FromSeconds(10)))
                {
                    ReadOnlyMemory<byte> replayedPut = await reconnected.RequestAsync(putEnvelope);
                    Assert.Equal(new byte[] { 1 }, replayedPut.ToArray());

                    ReadOnlyMemory<byte> getResult = await reconnected.RequestAsync(getEnvelope);
                    Assert.Equal(1, getResult.Span[0]);
                    Assert.Equal((uint)value.Length, BinaryPrimitives.ReadUInt32LittleEndian(getResult.Span[1..]));
                    Assert.Equal(value, getResult.Span[5..].ToArray());

                    ReadOnlyMemory<byte> deleteResult = await reconnected.RequestAsync(deleteEnvelope);
                    Assert.Equal(new byte[] { 1 }, deleteResult.ToArray());
                }
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
                StorageIpcEndpoint.DeleteIfOwned(directory, partition, ownerId);
            }

            Assert.Equal(2UL, storage.LastCommitSeq);
            Assert.False(storage.TryGet(key, out _));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }
}
