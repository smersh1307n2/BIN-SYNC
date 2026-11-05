// STATE_DIGEST / NEED_RANGE / SNAPSHOT / AutoReconcile (§13.1).
namespace BinSync;

/// <summary>Transport-independent reconciliation operations for a sync session.</summary>
public partial class SyncSession
{
    private static Frame ParseExpectedSessionFrame(byte[] raw, byte expectedType)
    {
        ArgumentNullException.ThrowIfNull(raw);
        var (frame, consumed) = FrameCodec.ParseFrame(raw);
        if (consumed != raw.Length || frame.FrameType != expectedType)
            throw new FrameException("FRAME_REJECTED");
        return frame;
    }

    private static void ValidateNamespaceStream(Frame frame, int namespaceId)
    {
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new FormatException("Namespace is outside u16 range");
        uint expectedStreamId = 0x0001_0000u | (uint)namespaceId;
        if (frame.StreamId != expectedStreamId)
            throw new FrameException("FRAME_REJECTED");
    }

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
            var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.StateDigest, ct).ConfigureAwait(false)
               ;
            var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.StateDigest);
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            ValidateNamespaceStream(frame, peer.NamespaceId);
            if (peer.NamespaceId != namespaceId)
                throw new FormatException("STATE_DIGEST namespace does not match requested namespace");
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
        else
        {
            var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.StateDigest, ct).ConfigureAwait(false)
               ;
            var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.StateDigest);
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            ValidateNamespaceStream(frame, peer.NamespaceId);
            if (peer.NamespaceId != namespaceId)
                throw new FormatException("STATE_DIGEST namespace does not match requested namespace");
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
        var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.NeedRange, ct).ConfigureAwait(false)
           ;
        var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.NeedRange);
        var need = Reconcile.DecodeNeedRange(frame.Payload);
        ValidateNamespaceStream(frame, need.NamespaceId);
        return need;
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
        var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.NeedKeys, ct).ConfigureAwait(false)
           ;
        var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.NeedKeys);
        var need = Reconcile.DecodeNeedKeys(frame.Payload);
        ValidateNamespaceStream(frame, need.NamespaceId);
        return need;
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
        var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.SnapshotChunk, ct).ConfigureAwait(false)
           ;
        var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.SnapshotChunk);
        var first = Reconcile.DecodeSnapshotChunk(frame.Payload);
        ValidateNamespaceStream(frame, first.NamespaceId);
        if (first.ChunkCount == 0 || first.ChunkIndex != 0)
            throw new FormatException("Snapshot must begin with chunk zero");
        var chunks = new List<SnapshotChunkMessage>((int)first.ChunkCount) { first };
        for (uint expected = 1; expected < first.ChunkCount; expected++)
        {
            raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.SnapshotChunk, ct).ConfigureAwait(false)
               ;
            frame = ParseExpectedSessionFrame(raw, ReconcileFrames.SnapshotChunk);
            var chunk = Reconcile.DecodeSnapshotChunk(frame.Payload);
            ValidateNamespaceStream(frame, chunk.NamespaceId);
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
        var remainingPrefixes = diverged
            .Select(Convert.ToHexString)
            .ToHashSet(StringComparer.Ordinal);
        bool receivedTerminator = false;

        // At most maxPrefixes requests plus one terminator are accepted. Use long
        // for the counter so maxPrefixes == int.MaxValue cannot overflow.
        for (long i = 0; i <= maxPrefixes; i++)
        {
            var need = await ReceiveNeedRangeAsync(ct).ConfigureAwait(false);
            if (need.NamespaceId != namespaceId)
                throw new FormatException("NEED_RANGE namespace does not match STATE_DIGEST");

            if (need.Prefix.Length == 0)
            {
                // The initiator sends this terminator only after every requested
                // snapshot was fully received and atomically applied. This confirms
                // our outbound baseline, but not the reverse direction; the outer
                // two-round operation commits InitialSyncComplete after both finish.
                receivedTerminator = true;
                break;
            }

            if (handled >= maxPrefixes)
                throw new FormatException("NEED_RANGE exceeds maxPrefixes");

            // A peer must request each divergent prefix exactly once. Merely
            // matching the request count is insufficient: duplicates could omit
            // another divergent bucket and falsely establish the baseline.
            var prefixKey = Convert.ToHexString(need.Prefix);
            if (!remainingPrefixes.Remove(prefixKey))
                throw new FormatException("NEED_RANGE prefix is unexpected or duplicated");

            await FulfillNeedRangeWithSnapshotAsync(local, need, ct).ConfigureAwait(false);
            handled++;
        }

        // Do not accept an early terminator as a complete baseline: every divergent
        // prefix in our digest must have been requested and fulfilled by the peer.
        bool complete = receivedTerminator &&
            remainingPrefixes.Count == 0 &&
            handled == diverged.Count &&
            diverged.Count <= maxPrefixes;
        return (handled, 0, complete);
    }
}
