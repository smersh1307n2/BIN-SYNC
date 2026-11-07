using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace BinSync;

/// <summary>Transport-independent BIN-SYNC session over any complete-packet channel.</summary>
public partial class SyncSession : IAsyncDisposable
{
    protected readonly ISyncTransport _transport;
    private sealed class TypedFragmentState
    {
        public readonly object Gate = new();
        public readonly FragmentReassembler Reassembler = new();
        public readonly Dictionary<BatchId, FragmentContext> Contexts = new();

        public void Clear()
        {
            lock (Gate)
            {
                Reassembler.Clear();
                Contexts.Clear();
            }
        }
    }

    private readonly record struct FragmentContext(byte FrameType, uint StreamId,
        ulong BaseSequence, byte Flags, ushort Count);

    private readonly bool _ownsTransport;
    private readonly bool _flowControlEnabled;
    private readonly FlowControl _sendFlowControl = new();
    private readonly FlowControl _receiveFlowControl = new();
    private long _windowUpdateSequence;
    private int _queuedFrameCount;
    private long _queuedFrameBytes;
    private const int MaxQueuedReceiveFrames = 4096;
    private const long MaxQueuedReceiveBytes = 64L * 1024 * 1024;
    private IDisposable? _sessionLease;
    private int _disposed;
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly SemaphoreSlim _pushGate = new(1, 1);
    private readonly SemaphoreSlim _batchReceiveGate = new(1, 1);
    // SNAPSHOT_CHUNK has no transfer/correlation ID: serialize complete multi-frame transfers.
    private readonly SemaphoreSlim _snapshotSendGate = new(1, 1);
    private readonly SemaphoreSlim _snapshotReceiveGate = new(1, 1);
    // Auto-reconciliation reserves its participating frame types in a stable order.
    // Different frame types remain independently usable as required by §12.6.
    private readonly SemaphoreSlim _reconcileGate = new(1, 1);
    private readonly SemaphoreSlim _stateDigestGate = new(1, 1);
    private readonly SemaphoreSlim _needRangeGate = new(1, 1);
    private readonly SemaphoreSlim _needKeysGate = new(1, 1);
    private readonly SemaphoreSlim _snapshotGate = new(1, 1);
    private readonly SemaphoreSlim _exclusiveReceiveGate = new(1, 1);
    private readonly object _receiveSync = new();
    private readonly ConcurrentDictionary<byte, Channel<byte[]>> _receiveChannels = new();
    private readonly Channel<byte[]> _rawReceiveChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
    { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false });
    private readonly ConcurrentDictionary<byte, SemaphoreSlim> _receiveTypeGates = new();
    private readonly TypedFragmentState _typedFragmentState = new();
    private readonly CancellationTokenSource _receiveCts = new();
    private Task? _receivePump;
    private bool _rawReceiveActive;
    private bool _receiveCompleted;
    private Exception? _receiveFailure;
    /// <summary>Remote peer identity established by HELLO.</summary>
    public NodeId PeerNodeId { get; }
    /// <summary>Peer incarnation epoch established by HELLO.</summary>
    public int PeerEpoch { get; }
    /// <summary>Capabilities advertised by the peer.</summary>
    public SessionCapabilities PeerCapabilities { get; }

    protected SyncSession(ISyncTransport transport, HelloPayload peer, SessionCapabilities localCapabilities, bool ownsTransport, IDisposable sessionLease)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _ownsTransport = ownsTransport;
        _sessionLease = sessionLease ?? throw new ArgumentNullException(nameof(sessionLease));
        PeerNodeId = peer.NodeId;
        PeerEpoch = peer.Epoch;
        PeerCapabilities = peer.Capabilities;
        _flowControlEnabled = (localCapabilities & peer.Capabilities & SessionCapabilities.FlowControl) != 0;
        if (transport is IPeerAwareSyncTransport aware) aware.PeerNodeId = peer.NodeId;
    }

    /// <summary>Establish as initiator; local NodeId must be lower than the peer NodeId.</summary>
    public static async Task<SyncSession> ConnectAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        IDisposable? lease = null;
        try
        {
            EnsureTransportCapabilities(transport);
            var handshake = await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false);
            lease = handshake.Lease;
            var session = new SyncSession(transport, handshake.Peer, caps, ownsTransport, lease);
            lease = null;
            return session;
        }
        catch
        {
            lease?.Dispose();
            if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Establish as acceptor; local NodeId must be higher than the peer NodeId.</summary>
    public static async Task<SyncSession> AcceptAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        IDisposable? lease = null;
        try
        {
            EnsureTransportCapabilities(transport);
            var handshake = await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false);
            lease = handshake.Lease;
            var session = new SyncSession(transport, handshake.Peer, caps, ownsTransport, lease);
            lease = null;
            return session;
        }
        catch
        {
            lease?.Dispose();
            if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Establish a session symmetrically. Both transport endpoints call this method;
    /// the lower NodeId becomes the protocol initiator regardless of which endpoint
    /// opened the underlying transport connection.
    /// </summary>
    public static async Task<SyncSession> EstablishAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        IDisposable? lease = null;
        try
        {
            EnsureTransportCapabilities(transport);
            await transport.SendAsync(SessionWire.EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
            var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO");
            var (helloFrame, _) = FrameCodec.ParseFrame(raw);
            if (helloFrame.FrameType != SessionWire.TypeHello) throw new FrameException("FRAME_REJECTED");
            var peer = SessionWire.DecodeHello(raw);
            local.ValidateSessionEpoch(peer.NodeId, peer.Epoch);

            int order = CompareNodeIds(local.NodeId, peer.NodeId);
            if (order == 0) throw new FrameException("FRAME_REJECTED");

            if (order < 0)
            {
                raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO_ACK");
                var (ackFrame, _) = FrameCodec.ParseFrame(raw);
                if (ackFrame.FrameType != SessionWire.TypeHelloAck) throw new FrameException("FRAME_REJECTED");
                var ack = SessionWire.DecodeHelloAck(raw);
                if (ack.NodeId != peer.NodeId || ack.Epoch != peer.Epoch)
                    throw new FrameException("FRAME_REJECTED");

                lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
                var aliases = SessionWire.BuildAliasMapTable(local, peer.NodeId, peer.Epoch, selfAlias: 1, peerAlias: 2);
                await transport.SendAsync(SessionWire.EncodeAliasMap(aliases), ct).ConfigureAwait(false);
                raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
                var remoteAliases = SessionWire.DecodeAliasMap(raw);
                local.CommitSession(peer.NodeId, peer.Epoch, remoteAliases);
            }
            else
            {
                await transport.SendAsync(SessionWire.EncodeHelloAck(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
                raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
                var remoteAliases = SessionWire.DecodeAliasMap(raw);
                lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
                var merged = MergeAliasMap(local, peer.NodeId, peer.Epoch, remoteAliases);
                await transport.SendAsync(SessionWire.EncodeAliasMap(merged), ct).ConfigureAwait(false);
                local.CommitSession(peer.NodeId, peer.Epoch, merged);
            }

            var session = new SyncSession(transport, peer, caps, ownsTransport, lease!);
            lease = null;
            return session;
        }
        catch
        {
            lease?.Dispose();
            if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Push one pending delta and wait for its ACK.</summary>
    public async Task<bool> PushBatchAsync(Node sender, int namespaceId, int schemaVersion = 1, CancellationToken ct = default)
    {
        await _pushGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
        var batch = sender.CreateDeltaBatchPayloadForTransport(PeerNodeId, namespaceId,
            schemaVersion: schemaVersion);
        if (batch is null) return false;
        await SendFrameAsync(0x09, 0x0001_0000u | (uint)namespaceId, 1,
            batch.Value.Payload, ct).ConfigureAwait(false);
        var raw = await ReceivePacketAsync(0x0B, ct).ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, 0x0B);
        ValidateNamespaceStream(frame, namespaceId);
        var (id, seq, author) = Transport.DecodeAck(frame.Payload);
        if (!batch.Value.BatchId.EqualsBytes(id)) throw new InvalidOperationException("ACK batch id mismatch");
        if (seq != batch.Value.CursorEnd || author is null || NodeId.From(author) != batch.Value.DurableAuthor)
            throw new SyncException("ACK_OUT_OF_RANGE", batch.Value.BatchId);
        sender.AdvanceStreamForTransport(PeerNodeId, namespaceId, batch.Value);
        sender.ProcessAck(PeerNodeId, namespaceId, seq, authorStream: NodeId.From(author));
        return true;
        }
        finally { _pushGate.Release(); }
    }

    /// <summary>Receive one DELTA_BATCH, apply it and return the ACK.</summary>
    public Task<BatchResult> ReceiveBatchAsync(Node receiver, CancellationToken ct = default,
        Action<Batch, BatchResult>? beforeAck = null)
        => ReceiveBatchAsync(receiver, beforeApply: null, beforeAck: beforeAck, ct: ct);

    /// <summary>
    /// Receive one batch with an optional durable intent callback before Node mutation
    /// and a durability callback after Node mutation but before ACK.
    /// </summary>
    public async Task<BatchResult> ReceiveBatchAsync(Node receiver,
        Action<Batch, ReadOnlyMemory<byte>, NodeId, uint>? beforeApply,
        Action<Batch, BatchResult>? beforeAck,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        await _batchReceiveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var raw = await ReceivePacketAsync(0x09, ct).ConfigureAwait(false);
            var frame = ParseExpectedSessionFrame(raw, 0x09);
            if (frame.Payload.Length < sizeof(ushort))
                throw new FrameException("FRAME_REJECTED");
            int namespaceId = BinaryPrimitives.ReadUInt16LittleEndian(frame.Payload);
            ValidateNamespaceStream(frame, namespaceId);

            // Decode without mutation so a host can persist the exact wire payload as
            // a PendingApply intent before Node's receive cursor or records advance.
            var decoded = receiver.PrepareBatchPayloadForMetadata(frame.Payload, PeerNodeId);
            beforeApply?.Invoke(decoded, frame.Payload, PeerNodeId, checked((uint)PeerEpoch));

            var (batch, result) = receiver.ApplyBatchPayloadWithBatch(frame.Payload, PeerNodeId);
            beforeAck?.Invoke(batch, result);
            var ack = Transport.EncodeAck(result.BatchId, result.CursorEnd, result.DurableAuthor);
            await SendFrameAsync(0x0B, 0x0001_0000u | (uint)result.NamespaceId, 2, ack, ct).ConfigureAwait(false);
            return result;
        }
        finally { _batchReceiveGate.Release(); }
    }

    /// <summary>Build and send one framed packet through the configured channel.</summary>
    protected async Task SendFrameAsync(byte type, uint streamId, ulong seq, ReadOnlyMemory<byte> payload,
        CancellationToken ct, byte flags = 0)
    {
        if (payload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        bool flowControlled = _flowControlEnabled && streamId != 0 && type != Frames.TYPE_WINDOW_UPDATE;
        if (flowControlled && payload.Length > _sendFlowControl.Initial)
        {
            if ((flags & Frames.FLAG_FRAGMENT) != 0 || _sendFlowControl.Initial <= 24)
                throw new FrameException("FLOW_CONTROL_VIOLATION");
            int maxBody = _sendFlowControl.Initial - 24;
            int fragmentCount = checked((payload.Length + maxBody - 1) / maxBody);
            if (fragmentCount > ushort.MaxValue || (ulong)(fragmentCount - 1) > ulong.MaxValue - seq)
                throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
            byte[] messageId = Guid.NewGuid().ToByteArray();
            for (int i = 0; i < fragmentCount; i++)
            {
                int offset = i * maxBody;
                int bodyLength = Math.Min(maxBody, payload.Length - offset);
                byte[] fragmentPayload = FragmentCodec.EncodeOne(messageId, (ushort)i,
                    (ushort)fragmentCount, payload.Span.Slice(offset, bodyLength));
                await SendFrameAsync(type, streamId, seq + (ulong)i, fragmentPayload, ct,
                    (byte)(flags | Frames.FLAG_FRAGMENT)).ConfigureAwait(false);
            }
            return;
        }
        byte[]? rented = null;
        bool creditReserved = false;
        bool sendStarted = false;
        try
        {
            if (flowControlled)
            {
                if (payload.Length > _sendFlowControl.Initial)
                    throw new FrameException("FLOW_CONTROL_VIOLATION");
                EnsureReceivePumpStarted();
                await _sendFlowControl.WaitForCreditAsync(streamId, payload.Length, ct).ConfigureAwait(false);
                creditReserved = true;
            }
            int size = Frames.HEADER_SIZE + payload.Length;
            rented = ArrayPool<byte>.Shared.Rent(size);
            int length = FrameCodec.BuildFrame(type, streamId, seq, payload.Span, rented.AsSpan(0, size), flags);
            await _sendGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                sendStarted = true;
                await _transport.SendAsync(rented.AsMemory(0, length), ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _sendFlowControl.Fail(ex);
                _receiveCts.Cancel();
                throw;
            }
            finally { _sendGate.Release(); }
        }
        finally
        {
            if (creditReserved && !sendStarted)
                _sendFlowControl.ApplyUpdate(streamId, payload.Length);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private void EnsureReceivePumpStarted()
    {
        lock (_receiveSync)
        {
            if (_rawReceiveActive && _receivePump is null)
                throw new InvalidOperationException("Raw receive mode is active");
            _receivePump ??= ReceivePumpAsync(_receiveCts.Token);
        }
    }

    private async Task CompleteReceiveAsync(byte[] raw)
    {
        Interlocked.Decrement(ref _queuedFrameCount);
        Interlocked.Add(ref _queuedFrameBytes, -raw.Length);
        if (!_flowControlEnabled) return;
        uint streamId = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(5));
        int payloadLength = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(17)));
        if (streamId == 0 || payloadLength == 0) return;
        _receiveFlowControl.ApplyUpdate(streamId, payloadLength);
        // Once a frame has been dequeued, its credit must be returned even if the
        // application cancels the receive operation. Only session shutdown may cancel it.
        await SendWindowUpdateAsync(streamId, payloadLength, _receiveCts.Token).ConfigureAwait(false);
    }

    private async Task SendWindowUpdateAsync(uint streamId, int credits, CancellationToken ct)
    {
        if (credits <= 0) return;
        byte[] payload = SessionWire.EncodeWindowUpdate((uint)credits);
        await _sendGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Allocate the sequence while holding the wire send gate so sequence order
            // is identical to the order in which WINDOW_UPDATE frames reach the transport.
            ulong sequence = (ulong)Interlocked.Increment(ref _windowUpdateSequence);
            int size = Frames.HEADER_SIZE + payload.Length;
            byte[] rented = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                int length = FrameCodec.BuildFrame(Frames.TYPE_WINDOW_UPDATE, streamId,
                    sequence, payload, rented.AsSpan(0, size));
                await _transport.SendAsync(rented.AsMemory(0, length), ct).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
        catch (Exception ex)
        {
            _sendFlowControl.Fail(ex);
            _receiveCts.Cancel();
            throw;
        }
        finally
        {
            _sendGate.Release();
        }
    }

    /// <summary>Dispose the underlying channel when this session owns it.</summary>
    public virtual async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _sendFlowControl.Fail(new ObjectDisposedException(nameof(SyncSession)));
        _typedFragmentState.Clear();
        _receiveCts.Cancel();
        try
        {
            Task? pump;
            lock (_receiveSync) pump = _receivePump;
            if (pump is not null)
            {
                try { await pump.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
            }
            if (_ownsTransport) await _transport.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _sessionLease, null)?.Dispose();
            _receiveCts.Dispose();
        }
    }

    /// <summary>Receive a specific frame type through the session's single-reader dispatcher.</summary>
    private async Task<byte[]> ReceivePacketAsync(byte expectedType, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (expectedType < 0x04 || expectedType > 0x13)
            throw new InvalidOperationException("Only established-session frame types can be dispatched");
        Channel<byte[]> channel;
        lock (_receiveSync)
        {
            if (_rawReceiveActive)
                throw new InvalidOperationException("An exclusive receive operation is already active on this session");
            channel = _receiveChannels.GetOrAdd(expectedType, static _ =>
                Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
                {
                    SingleReader = true, SingleWriter = true,
                    AllowSynchronousContinuations = false,
                }));
            if (_receiveCompleted) channel.Writer.TryComplete(_receiveFailure);
            _receivePump ??= ReceivePumpAsync(_receiveCts.Token);
        }

        while (true)
        {
            byte[] raw;
            try { raw = await channel.Reader.ReadAsync(ct).ConfigureAwait(false); }
            catch (ChannelClosedException) when (_receiveFailure is not null)
            { throw new IOException("Session receive pump failed", _receiveFailure); }
            catch (ChannelClosedException)
            { throw new IOException("Peer closed the session"); }

            byte[]? logicalFrame;
            try
            {
                if (raw[3] != expectedType)
                    throw new FrameException("FRAME_REJECTED");
                logicalFrame = TryReassembleFrame(raw);
            }
            finally
            {
                await CompleteReceiveAsync(raw).ConfigureAwait(false);
            }

            if (logicalFrame is not null)
                return logicalFrame;
        }
    }

    private async Task ReceivePumpAsync(CancellationToken ct)
    {
        Exception? failure = null;
        try
        {
            while (true)
            {
                var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                    ?? throw new IOException("Peer closed the session");
                FrameCodec.ValidateFrame(raw);
                uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(17));
                if (raw.Length != Frames.HEADER_SIZE + (int)payloadLength)
                    throw new FrameException("FRAME_REJECTED");
                byte frameType = raw[3];
                byte flags = raw[4];
                uint streamId = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(5));
                if (frameType < 0x04 || frameType > 0x13)
                    throw new FrameException("FRAME_REJECTED");

                if (frameType == Frames.TYPE_WINDOW_UPDATE)
                {
                    ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(raw.AsSpan(9));
                    if (!_flowControlEnabled || streamId == 0 || flags != 0 || sequence == 0 ||
                        !SessionWire.TryDecodeWindowUpdate(raw.AsSpan(Frames.HEADER_SIZE, (int)payloadLength), out uint credits) ||
                        !_sendFlowControl.TryApplyUpdate(streamId, credits, sequence))
                        throw new FrameException("FLOW_CONTROL_VIOLATION");
                    continue;
                }

                if (_flowControlEnabled && streamId != 0 &&
                    !_receiveFlowControl.TryConsume(streamId, payloadLength))
                    throw new FrameException("FLOW_CONTROL_VIOLATION");

                Channel<byte[]> channel;
                lock (_receiveSync)
                {
                    channel = _rawReceiveActive
                        ? _rawReceiveChannel
                        : _receiveChannels.GetOrAdd(frameType, static _ =>
                            Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
                            {
                                SingleReader = true, SingleWriter = true,
                                AllowSynchronousContinuations = false,
                            }));
                }

                int queuedFrames = Interlocked.Increment(ref _queuedFrameCount);
                long queuedBytes = Interlocked.Add(ref _queuedFrameBytes, raw.Length);
                if (queuedFrames > MaxQueuedReceiveFrames || queuedBytes > MaxQueuedReceiveBytes)
                {
                    Interlocked.Decrement(ref _queuedFrameCount);
                    Interlocked.Add(ref _queuedFrameBytes, -raw.Length);
                    throw new FrameException("FLOW_CONTROL_VIOLATION");
                }
                if (!channel.Writer.TryWrite(raw))
                {
                    Interlocked.Decrement(ref _queuedFrameCount);
                    Interlocked.Add(ref _queuedFrameBytes, -raw.Length);
                    throw new FrameException("FLOW_CONTROL_VIOLATION");
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { failure = ex; }
        finally
        {
            if (failure is not null) _sendFlowControl.Fail(failure);
            lock (_receiveSync)
            {
                _receiveFailure = failure;
                _receiveCompleted = true;
                foreach (var channel in _receiveChannels.Values)
                    channel.Writer.TryComplete(failure);
                _rawReceiveChannel.Writer.TryComplete(failure);
            }
        }
    }

    private async Task<byte[]> ReceiveExpectedExclusiveAsync(byte expectedType, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (expectedType < 0x04 || expectedType > 0x13)
            throw new ArgumentOutOfRangeException(nameof(expectedType));
        var gate = _receiveTypeGates.GetOrAdd(expectedType, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try { return await ReceivePacketAsync(expectedType, ct).ConfigureAwait(false); }
        finally { gate.Release(); }
    }

    private void BeginRawReceiveMode()
    {
        lock (_receiveSync)
        {
            if (_rawReceiveActive) return;
            if (_queuedFrameCount != 0 || !_receiveChannels.IsEmpty)
                throw new InvalidOperationException("Raw receive cannot be mixed with typed receive operations");
            _rawReceiveActive = true;
            _receivePump ??= ReceivePumpAsync(_receiveCts.Token);
        }
    }

    private static void EnsureTransportCapabilities(ISyncTransport transport)
    {
        const SyncTransportCapabilities required =
            SyncTransportCapabilities.ReliableDelivery | SyncTransportCapabilities.OrderedDelivery;
        if ((transport.Capabilities & required) != required)
            throw new ArgumentException(
                "BIN-SYNC sessions require reliable, ordered packet delivery. Wrap datagram transports in a reliable ordered adapter.",
                nameof(transport));
    }

    internal static async Task<SessionHandshakeResult> InitiateAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        await transport.SendAsync(SessionWire.EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO_ACK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHelloAck) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHelloAck(raw);
        EnsureHandshakeRole(local.NodeId, peer.NodeId, initiator: true);
        var lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
        try
        {
        var aliases = SessionWire.BuildAliasMapTable(local, peer.NodeId, peer.Epoch, selfAlias: 1, peerAlias: 2);
        await transport.SendAsync(SessionWire.EncodeAliasMap(aliases), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        var remoteAliases = SessionWire.DecodeAliasMap(raw);

        // A completed handshake starts a fresh session. Delay reset until the peer's
        // complete map has been decoded, so a failed/incomplete handshake preserves
        // the previous live session state.
        // The acceptor's returned map is the negotiated union; commit it atomically.
        local.CommitSession(peer.NodeId, peer.Epoch, remoteAliases);
        return new SessionHandshakeResult(peer, lease);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    internal static async Task<SessionHandshakeResult> AcceptHandshakeAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHello) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHello(raw);
        EnsureHandshakeRole(local.NodeId, peer.NodeId, initiator: false);
        await transport.SendAsync(SessionWire.EncodeHelloAck(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        var remoteAliases = SessionWire.DecodeAliasMap(raw);
        var lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
        try
        {
        var merged = MergeAliasMap(local, peer.NodeId, peer.Epoch, remoteAliases);
        await transport.SendAsync(SessionWire.EncodeAliasMap(merged), ct).ConfigureAwait(false);

        // Commit the new session only after the complete peer map was decoded and
        // our response was sent. Failed handshakes leave the previous session intact.
        local.CommitSession(peer.NodeId, peer.Epoch, merged);
        return new SessionHandshakeResult(peer, lease);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    private static int CompareNodeIds(NodeId left, NodeId right)
    {
        Span<byte> leftBytes = stackalloc byte[16];
        Span<byte> rightBytes = stackalloc byte[16];
        left.WriteTo(leftBytes);
        right.WriteTo(rightBytes);
        return leftBytes.SequenceCompareTo(rightBytes);
    }

    internal static void EnsureHandshakeRole(NodeId local, NodeId peer, bool initiator)
    {
        int order = CompareNodeIds(local, peer);
        if (order == 0 || (initiator ? order >= 0 : order <= 0))
            throw new FrameException("FRAME_REJECTED");
    }

    internal static List<AliasMapEntry> MergeAliasMap(Node local, NodeId peerNodeId, int peerEpoch,
        IReadOnlyList<AliasMapEntry> remote)
    {
        // HELLO establishes the current incarnation of both session endpoints.
        // An ALIAS_MAP may add third parties, but it must not silently downgrade or
        // replace either endpoint's identity/epoch binding.
        foreach (var entry in remote)
        {
            if ((entry.NodeId == local.NodeId && entry.Epoch != local.Epoch) ||
                (entry.NodeId == peerNodeId && entry.Epoch != peerEpoch))
                throw new FrameException("FRAME_REJECTED");
        }

        var result = new List<AliasMapEntry>(remote);
        var known = new HashSet<NodeId>(result.Select(x => x.NodeId));
        var extras = new List<(NodeId Id, int Epoch)>();
        void Add(NodeId id, int epoch) { if (known.Add(id)) extras.Add((id, epoch)); }
        Add(local.NodeId, local.Epoch); Add(peerNodeId, peerEpoch);
        foreach (var (id, epoch) in local.DurableAuthorsSnapshot()) Add(id, epoch);
        extras.Sort(static (a, b) =>
        {
            Span<byte> left = stackalloc byte[16]; Span<byte> right = stackalloc byte[16];
            a.Id.WriteTo(left); b.Id.WriteTo(right); return left.SequenceCompareTo(right);
        });
        int next = 1;
        foreach (var item in extras)
        {
            while (result.Any(x => x.Alias == next)) next++;
            result.Add(new AliasMapEntry((ushort)next, item.Id, item.Epoch)); next++;
        }
        return result;
    }
}


internal readonly record struct SessionHandshakeResult(HelloPayload Peer, IDisposable Lease);

/// <summary>Reserves the peer-scoped alias/session slot for the lifetime of a SyncSession.</summary>
internal static class PeerSessionLease
{
    private static readonly ConditionalWeakTable<Node, ConcurrentDictionary<NodeId, SemaphoreSlim>> Gates = new();

    internal static async ValueTask<IDisposable> AcquireAsync(Node local, NodeId peer, CancellationToken ct)
    {
        var perPeer = Gates.GetValue(local, static _ => new ConcurrentDictionary<NodeId, SemaphoreSlim>());
        var gate = perPeer.GetOrAdd(peer, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        return new Lease(gate);
    }

    private sealed class Lease : IDisposable
    {
        private SemaphoreSlim? _gate;
        internal Lease(SemaphoreSlim gate) => _gate = gate;
        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }
}
