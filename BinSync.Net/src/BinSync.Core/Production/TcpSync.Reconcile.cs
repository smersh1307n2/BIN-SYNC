// STATE_DIGEST / NEED_RANGE / SNAPSHOT / AutoReconcile (§13.1).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    /// <summary>
    /// Send our STATE_DIGEST and receive the peer's. When initiate is true we send first;
    /// otherwise we wait then reply (matching pair on the other side).
    /// </summary>
    public async Task<(StateDigestMessage Local, StateDigestMessage Remote, List<byte[]> Diverged)>
        ExchangeStateDigestAsync(Node local, int namespaceId, bool initiate, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var mine = Reconcile.BuildStateDigest(local, namespaceId);
        if (initiate)
        {
            await _transport.SendAsync(
                FrameCodec.BuildFrame(ReconcileFrames.StateDigest, streamId, 10, Reconcile.EncodeStateDigest(mine)),
                ct).ConfigureAwait(false);
            var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during STATE_DIGEST");
            var (frame, _) = FrameCodec.ParseFrame(raw);
            if (frame.FrameType != ReconcileFrames.StateDigest)
                throw new FrameException("FRAME_REJECTED");
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
        else
        {
            var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during STATE_DIGEST");
            var (frame, _) = FrameCodec.ParseFrame(raw);
            if (frame.FrameType != ReconcileFrames.StateDigest)
                throw new FrameException("FRAME_REJECTED");
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            await _transport.SendAsync(
                FrameCodec.BuildFrame(ReconcileFrames.StateDigest, streamId, 10, Reconcile.EncodeStateDigest(mine)),
                ct).ConfigureAwait(false);
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
    }

    public async Task SendNeedRangeAsync(int namespaceId, byte[] prefix, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var payload = Reconcile.EncodeNeedRange(new NeedRangeMessage { NamespaceId = namespaceId, Prefix = prefix });
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.NeedRange, streamId, 11, payload), ct).ConfigureAwait(false);
    }

    public async Task<NeedRangeMessage> ReceiveNeedRangeAsync(CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during NEED_RANGE");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.NeedRange)
            throw new FrameException("FRAME_REJECTED");
        return Reconcile.DecodeNeedRange(frame.Payload);
    }

    public async Task SendNeedKeysAsync(NeedKeysMessage keys, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)keys.NamespaceId;
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.NeedKeys, streamId, 12, Reconcile.EncodeNeedKeys(keys)),
            ct).ConfigureAwait(false);
    }

    public async Task<NeedKeysMessage> ReceiveNeedKeysAsync(CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during NEED_KEYS");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.NeedKeys)
            throw new FrameException("FRAME_REJECTED");
        return Reconcile.DecodeNeedKeys(frame.Payload);
    }

    /// <summary>
    /// Fulfill NEED_RANGE via republish + DELTA_BATCH. Peer must ReceiveBatchAsync
    /// for each pushed batch.
    /// </summary>
    public async Task<int> FulfillNeedRangeAsync(
        Node local, NeedRangeMessage need, CancellationToken ct = default)
    {
        local.RepublishUnderPrefix(need.NamespaceId, need.Prefix);
        int pushed = 0;
        for (int i = 0; i < 64; i++)
        {
            if (!await PushBatchAsync(local, need.NamespaceId, ct: ct).ConfigureAwait(false))
                break;
            pushed++;
        }
        return pushed;
    }

    /// <summary>Send one SNAPSHOT_CHUNK covering records under optional prefix.</summary>
    public async Task SendSnapshotAsync(
        Node local, int namespaceId, byte[]? prefix = null, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var chunk = Reconcile.BuildSnapshot(local, namespaceId, prefix);
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.SnapshotChunk, streamId, 13, Reconcile.EncodeSnapshotChunk(chunk)),
            ct).ConfigureAwait(false);
    }

    /// <summary>Receive and apply one SNAPSHOT_CHUNK. Returns fields applied.</summary>
    public async Task<int> ReceiveSnapshotAsync(Node local, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during SNAPSHOT_CHUNK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.SnapshotChunk)
            throw new FrameException("FRAME_REJECTED");
        var chunk = Reconcile.DecodeSnapshotChunk(frame.Payload);
        return Reconcile.ApplySnapshot(local, chunk);
    }

    /// <summary>
    /// Fulfill NEED_RANGE with a SNAPSHOT_CHUNK (no ACK required — fire-and-apply).
    /// Prefer for large prefixes or when outbox is empty of pending deltas.
    /// </summary>
    public async Task FulfillNeedRangeWithSnapshotAsync(
        Node local, NeedRangeMessage need, CancellationToken ct = default)
    {
        await SendSnapshotAsync(local, need.NamespaceId, need.Prefix, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// One-shot reconcile for a namespace: exchange digests, then NEED_RANGE + SNAPSHOT.
    /// Initiator requests; acceptor serves. Empty-prefix NEED_RANGE terminates acceptor.
    /// </summary>
    public async Task<(int Prefixes, int FieldsApplied)> AutoReconcileAsync(
        Node local, int namespaceId, bool initiate, int maxPrefixes = 16, CancellationToken ct = default)
    {
        var (_, _, diverged) = await ExchangeStateDigestAsync(local, namespaceId, initiate, ct)
            .ConfigureAwait(false);

        if (initiate)
        {
            int fields = 0;
            int n = Math.Min(diverged.Count, maxPrefixes);
            for (int i = 0; i < n; i++)
            {
                await SendNeedRangeAsync(namespaceId, diverged[i], ct).ConfigureAwait(false);
                fields += await ReceiveSnapshotAsync(local, ct).ConfigureAwait(false);
            }
            await SendNeedRangeAsync(namespaceId, Array.Empty<byte>(), ct).ConfigureAwait(false);
            return (n, fields);
        }
        else
        {
            int handled = 0;
            for (int i = 0; i < maxPrefixes + 1; i++)
            {
                var need = await ReceiveNeedRangeAsync(ct).ConfigureAwait(false);
                if (need.Prefix.Length == 0) break;
                await FulfillNeedRangeWithSnapshotAsync(local, need, ct).ConfigureAwait(false);
                handled++;
            }
            return (handled, 0);
        }
    }
}
