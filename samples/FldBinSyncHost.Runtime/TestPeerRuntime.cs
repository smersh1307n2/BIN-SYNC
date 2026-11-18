using System.Collections.Concurrent;
using System.Net.Sockets;
using BinSync;
using BinSync.FldHost;

internal sealed class TestPeerRuntime : IAsyncDisposable
{
    private const SessionCapabilities Capabilities =
        SessionCapabilities.FullAuthorId |
        SessionCapabilities.FlowControl |
        SessionCapabilities.NamespaceRosterV31 |
        SessionCapabilities.OriginFrontierV31 |
        SessionCapabilities.FullReplaceV31;

    private readonly Node _node;
    private readonly FldBinSyncAsyncHostComposition _composition;
    private readonly HashSet<NodeId>? _allowedPeers;
    private readonly ConcurrentDictionary<NodeId, SyncSession> _sessions = new();
    private readonly ConcurrentDictionary<NodeId, Task> _receiveLoops = new();
    private readonly CancellationTokenSource _stop = new();
    private TcpSyncListener? _listener;
    private Task? _acceptLoop;
    private int _running;
    private long _outboundRounds;
    private long _outboundBatches;
    private long _inboundBatches;
    private string? _lastError;

    public TestPeerRuntime(
        Node node,
        FldBinSyncAsyncHostComposition composition,
        string? allowedPeerIds = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));

        if (!string.IsNullOrWhiteSpace(allowedPeerIds))
        {
            _allowedPeers = new HashSet<NodeId>();
            foreach (string value in allowedPeerIds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                byte[] bytes = Convert.FromHexString(value);
                if (bytes.Length != 16)
                    throw new ArgumentException("Every allowed test peer identity must be a 16-byte NodeId.", nameof(allowedPeerIds));
                if (!_allowedPeers.Add(NodeId.From(bytes)))
                    throw new ArgumentException("Duplicate test peer identity.", nameof(allowedPeerIds));
            }
        }
    }

    public bool IsRunning => Volatile.Read(ref _running) != 0;
    public int Port => _listener?.Port ?? 0;
    public int ActivePeers => _sessions.Count;
    public long OutboundRounds => Interlocked.Read(ref _outboundRounds);
    public long OutboundBatches => Interlocked.Read(ref _outboundBatches);
    public long InboundBatches => Interlocked.Read(ref _inboundBatches);
    public string? LastError => Volatile.Read(ref _lastError);
    public IReadOnlyList<string> ActivePeerIds => _sessions.Keys
        .Select(static id => Convert.ToHexString(id.ToArray()))
        .OrderBy(static id => id, StringComparer.Ordinal)
        .ToArray();

    private void RecordError(Exception exception)
        => Volatile.Write(ref _lastError, exception.ToString());

    public void Start()
    {
        if (Interlocked.Exchange(ref _running, 1) != 0)
            throw new InvalidOperationException("The test peer runtime is already running.");

        try
        {
            _listener = TcpSyncListener.Start();
            _acceptLoop = AcceptLoopAsync(_listener, _stop.Token);
        }
        catch
        {
            Interlocked.Exchange(ref _running, 0);
            throw;
        }
    }

    public async Task<NodeId> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default)
    {
        if (!IsRunning)
            throw new InvalidOperationException("The test peer runtime is not accepting peer connections.");

        SyncSession session = await TcpSync.ConnectAsync(
            _node, host, port, Capabilities, ct: cancellationToken).ConfigureAwait(false);
        if (!TryRegister(session))
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException("The peer is not allowed or already has an active session.");
        }

        return session.PeerNodeId;
    }

    public async Task<int> RunOutboundRoundAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _outboundRounds);
        try
        {
            SyncSession[] sessions = _sessions.Values.ToArray();
            int sent = sessions.Length == 0
                ? 0
                : await _composition.Adapter.RunOutboundRoundAsync(sessions, cancellationToken)
                    .ConfigureAwait(false);
            Interlocked.Add(ref _outboundBatches, sent);
            return sent;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            RecordError(exception);
            throw;
        }
    }

    private async Task AcceptLoopAsync(TcpSyncListener listener, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            SyncSession session;
            try
            {
                session = await listener.AcceptAsync(
                    _node, Capabilities, ct: cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (SocketException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (IOException) when (cancellationToken.IsCancellationRequested) { break; }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                RecordError(exception);
                continue;
            }

            if (!TryRegister(session))
                await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private bool TryRegister(SyncSession session)
    {
        if (_allowedPeers is not null && !_allowedPeers.Contains(session.PeerNodeId))
            return false;
        if (!_sessions.TryAdd(session.PeerNodeId, session))
            return false;

        // Start while holding the same lock used by cleanup so the task cannot
        // remove itself before it has been registered in _receiveLoops.
        lock (_receiveLoops)
        {
            Task loop = ReceiveLoopAsync(session, _stop.Token);
            _receiveLoops[session.PeerNodeId] = loop;
        }
        return true;
    }

    private async Task ReceiveLoopAsync(SyncSession session, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                _ = await _composition.Adapter.ReceiveAndPublishAsync(
                    session, _sessions.Values.ToArray(), cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _inboundBatches);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (IOException) when (!cancellationToken.IsCancellationRequested)
        {
            // A remote disconnect is an ordinary session lifecycle event.
        }
        catch (ObjectDisposedException) when (!cancellationToken.IsCancellationRequested)
        {
            // A concurrently closed session is removed below.
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            RecordError(exception);
            Console.Error.WriteLine($"Test peer session {session.PeerNodeId} failed: {exception}");
        }
        finally
        {
            _sessions.TryRemove(session.PeerNodeId, out _);
            try { await session.DisposeAsync().ConfigureAwait(false); } catch { }
        }
    }

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _running, 0) == 0)
            return;

        _stop.Cancel();
        if (_listener is not null)
        {
            await _listener.DisposeAsync().ConfigureAwait(false);
            _listener = null;
        }

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.ConfigureAwait(false); } catch { }
            _acceptLoop = null;
        }

        foreach (SyncSession session in _sessions.Values)
        {
            try { await session.DisposeAsync().ConfigureAwait(false); } catch { }
        }

        Task[] loops;
        lock (_receiveLoops)
            loops = _receiveLoops.Values.ToArray();
        try { await Task.WhenAll(loops).ConfigureAwait(false); } catch { }
        _sessions.Clear();
        _receiveLoops.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _stop.Dispose();
    }
}
