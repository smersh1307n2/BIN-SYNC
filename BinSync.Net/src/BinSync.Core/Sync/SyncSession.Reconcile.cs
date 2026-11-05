// STATE_DIGEST / NEED_RANGE / SNAPSHOT / AutoReconcile (§13.1).
namespace BinSync;

/// <summary>Transport-independent reconciliation operations for a sync session.</summary>
public partial class SyncSession
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
            await SendFrameAsync(ReconcileFrames.StateDigest, streamId, 10ul, Reconcile.EncodeStateDigest(mine), ct).ConfigureAwait(false);
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
            await SendFrameAsync(ReconcileFrames.StateDigest, streamId, 10ul, Reconcile.EncodeStateDigest(mine), ct).ConfigureAwait(false);
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
    }

    /// <summary>Send a NEED_RANGE reconcile message for a bucket prefix.</summary>
    public async Task SendNeedRangeAsync(int namespaceId, byte[] prefix, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var payload = Reconcile.EncodeNeedRange(new NeedRangeMessage { NamespaceId = namespaceId, Prefix = prefix });
        await SendFrameAsync(ReconcileFrames.NeedRange, streamId, 11ul, payload, ct).ConfigureAwait(false);
    }

    /// <summary>Receive a NEED_RANGE reconcile message.</summary>
    public async Task<NeedRangeMessage> ReceiveNeedRangeAsync(CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during NEED_RANGE");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.NeedRange)
            throw new FrameException("FRAME_REJECTED");
        return Reconcile.DecodeNeedRange(frame.Payload);
    }

    /// <summary>Send a NEED_KEYS reconcile message listing missing keys.</summary>
    public async Task SendNeedKeysAsync(NeedKeysMessage keys, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)keys.NamespaceId;
        await SendFrameAsync(ReconcileFrames.NeedKeys, streamId, 12ul, Reconcile.EncodeNeedKeys(keys), ct).ConfigureAwait(false);
    }

    /// <summary>Receive a NEED_KEYS reconcile message.</summary>
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
        var chunks = Reconcile.BuildSnapshotChunks(local, namespaceId, prefix);
        for (int i = 0; i < chunks.Count; i++)
            await SendFrameAsync(ReconcileFrames.SnapshotChunk, streamId, 13ul,
                Reconcile.EncodeSnapshotChunk(chunks[i]), ct).ConfigureAwait(false);
    }

    /// <summary>Receive and apply one SNAPSHOT_CHUNK. Returns fields applied.</summary>
    public async Task<int> ReceiveSnapshotAsync(Node local, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during SNAPSHOT_CHUNK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.SnapshotChunk)
            throw new FrameException("FRAME_REJECTED");
        var first = Reconcile.DecodeSnapshotChunk(frame.Payload);
        if (first.ChunkCount == 0 || first.ChunkIndex != 0)
            throw new FormatException("Snapshot must begin with chunk zero");
        var chunks = new List<SnapshotChunkMessage>((int)first.ChunkCount) { first };
        for (uint expected = 1; expected < first.ChunkCount; expected++)
        {
            raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during multi-chunk SNAPSHOT_CHUNK");
            (frame, _) = FrameCodec.ParseFrame(raw);
            if (frame.FrameType != ReconcileFrames.SnapshotChunk)
                throw new FrameException("FRAME_REJECTED");
            var chunk = Reconcile.DecodeSnapshotChunk(frame.Payload);
            if (chunk.NamespaceId != first.NamespaceId ||
                chunk.ChunkCount != first.ChunkCount || chunk.ChunkIndex != expected)
                throw new FormatException("Snapshot chunk sequence mismatch");
            chunks.Add(chunk);
        }

        // Do not publish any part of the baseline until every chunk has arrived and validated.
        return Reconcile.ApplySnapshotChunks(local, chunks);
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
    /// Reconcile both directions for a namespace. Initial baseline is committed only
    /// after both directions completed and each sent snapshot set was confirmed applied.
    /// </summary>
    public async Task<(int Prefixes, int FieldsApplied)> AutoReconcileAsync(
        Node local, int namespaceId, bool initiate, int maxPrefixes = 16, CancellationToken ct = default)
    {
        if (maxPrefixes <= 0) throw new ArgumentOutOfRangeException(nameof(maxPrefixes));

        // Both directions must exchange a complete baseline before peer-relative
        // Outbox offsets can be initialized safely.
        var first = await AutoReconcileRoundAsync(local, namespaceId, initiate, maxPrefixes, ct)
            .ConfigureAwait(false);
        var second = await AutoReconcileRoundAsync(local, namespaceId, !initiate, maxPrefixes, ct)
            .ConfigureAwait(false);

        // A single completed direction is not a baseline: the peer may still not
        // have received our state. Commit only after both directional rounds finish.
        if (first.Complete && second.Complete)
            local.MarkInitialSyncComplete(PeerNodeId, namespaceId);

        return (first.Prefixes + second.Prefixes, first.FieldsApplied + second.FieldsApplied);
    }

    private async Task<(int Prefixes, int FieldsApplied, bool Complete)> AutoReconcileRoundAsync(
        Node local, int namespaceId, bool initiate, int maxPrefixes, CancellationToken ct)
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
            return (n, fields, diverged.Count <= maxPrefixes);
        }

        int handled = 0;
        for (int i = 0; i < maxPrefixes + 1; i++)
        {
            var need = await ReceiveNeedRangeAsync(ct).ConfigureAwait(false);
            if (need.Prefix.Length == 0)
            {
                // The initiator sends this terminator only after every requested
                // snapshot was fully received and atomically applied. This confirms
                // our outbound baseline, but not the reverse direction; the outer
                // two-round operation commits InitialSyncComplete after both finish.
                break;
            }
            await FulfillNeedRangeWithSnapshotAsync(local, need, ct).ConfigureAwait(false);
            handled++;
        }
        return (handled, 0, diverged.Count <= maxPrefixes);
    }
}
