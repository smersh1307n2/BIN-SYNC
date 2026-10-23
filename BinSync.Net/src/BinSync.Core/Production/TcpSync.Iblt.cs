// IBLT exchange over TcpSyncSession (§13.2 set-diff under a prefix).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    /// <summary>Send local IBLT for namespace (optional prefix filter).</summary>
    public async Task SendIbltAsync(
        Node local, int namespaceId, byte[]? prefix = null, int cellCount = 64,
        CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var table = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.Iblt, streamId, 15, Iblt.Encode(table)),
            ct).ConfigureAwait(false);
    }

    /// <summary>Receive one IBLT frame and decode.</summary>
    public async Task<Iblt> ReceiveIbltAsync(CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during IBLT");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.Iblt)
            throw new FrameException("FRAME_REJECTED");
        return Iblt.Decode(frame.Payload);
    }

    /// <summary>
    /// Pairwise IBLT exchange: initiate sends first then receives; acceptor reverse.
    /// Returns peeled symmetric difference from the initiator's perspective
    /// (local − remote after subtract).
    /// </summary>
    public async Task<(List<byte[]> OnlyLocal, List<byte[]> OnlyRemote)> ExchangeIbltAsync(
        Node local, int namespaceId, bool initiate,
        byte[]? prefix = null, int cellCount = 64, CancellationToken ct = default)
    {
        if (initiate)
        {
            var mine = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
            await _transport.SendAsync(
                FrameCodec.BuildFrame(
                    ReconcileFrames.Iblt,
                    0x0001_0000u | (uint)namespaceId,
                    15,
                    Iblt.Encode(mine)),
                ct).ConfigureAwait(false);
            var peer = await ReceiveIbltAsync(ct).ConfigureAwait(false);
            mine.Subtract(peer);
            return mine.Peel();
        }
        else
        {
            var peer = await ReceiveIbltAsync(ct).ConfigureAwait(false);
            var mine = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
            await _transport.SendAsync(
                FrameCodec.BuildFrame(
                    ReconcileFrames.Iblt,
                    0x0001_0000u | (uint)namespaceId,
                    15,
                    Iblt.Encode(mine)),
                ct).ConfigureAwait(false);
            mine.Subtract(peer);
            return mine.Peel();
        }
    }

    /// <summary>
    /// After IBLT peel: request missing keys via NEED_KEYS and fulfill with SNAPSHOT.
    /// </summary>
    public async Task<int> FulfillMissingKeysAsync(
        Node local, int namespaceId, bool initiate,
        List<byte[]> keysNeeded, CancellationToken ct = default)
    {
        if (initiate)
        {
            await SendNeedKeysAsync(new NeedKeysMessage
            {
                NamespaceId = namespaceId,
                RecordIds = keysNeeded ?? new List<byte[]>(),
            }, ct).ConfigureAwait(false);
            return await ReceiveSnapshotAsync(local, ct).ConfigureAwait(false);
        }
        else
        {
            var need = await ReceiveNeedKeysAsync(ct).ConfigureAwait(false);
            var chunk = Reconcile.BuildSnapshot(local, namespaceId);
            if (need.RecordIds.Count > 0)
            {
                var want = new HashSet<string>(
                    need.RecordIds.Select(k => Convert.ToHexString(Iblt.NormalizeKey(k))),
                    StringComparer.Ordinal);
                chunk.Records = chunk.Records
                    .Where(r => want.Contains(Convert.ToHexString(Iblt.NormalizeKey(r.RecordId))))
                    .ToList();
            }
            uint streamId = 0x0001_0000u | (uint)namespaceId;
            await _transport.SendAsync(
                FrameCodec.BuildFrame(
                    ReconcileFrames.SnapshotChunk, streamId, 13, Reconcile.EncodeSnapshotChunk(chunk)),
                ct).ConfigureAwait(false);
            return chunk.Records.Count;
        }
    }

    /// <summary>
    /// IBLT-driven reconcile: exchange tables → peel → NEED_KEYS + SNAPSHOT for missing keys.
    /// </summary>
    public async Task<(int OnlyLocal, int OnlyRemote, int FieldsApplied)> ReconcileViaIbltAsync(
        Node local, int namespaceId, bool initiate,
        byte[]? prefix = null, int cellCount = 64, CancellationToken ct = default)
    {
        var (onlyLocal, onlyRemote) = await ExchangeIbltAsync(
            local, namespaceId, initiate, prefix, cellCount, ct).ConfigureAwait(false);

        int fields;
        if (initiate)
        {
            fields = await FulfillMissingKeysAsync(local, namespaceId, initiate: true, onlyRemote, ct)
                .ConfigureAwait(false);
        }
        else
        {
            fields = await FulfillMissingKeysAsync(local, namespaceId, initiate: false, onlyLocal, ct)
                .ConfigureAwait(false);
        }
        return (onlyLocal.Count, onlyRemote.Count, fields);
    }
}
