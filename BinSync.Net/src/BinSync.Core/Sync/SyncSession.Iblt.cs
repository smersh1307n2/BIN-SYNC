// IBLT exchange over SyncSession (§13.2 set-diff under a prefix).
namespace BinSync;

/// <summary>Established TCP (optionally TLS) sync session with a peer.</summary>
public partial class SyncSession
{
    /// <summary>Send local IBLT for namespace (optional prefix filter).</summary>
    public async Task SendIbltAsync(
        Node local, int namespaceId, byte[]? prefix = null, int cellCount = 64,
        CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var table = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
        await SendFrameAsync(ReconcileFrames.IbltRequest, streamId, 15, Iblt.Encode(table), ct).ConfigureAwait(false);
    }

    /// <summary>Receive one IBLT frame and decode.</summary>
    public async Task<Iblt> ReceiveIbltAsync(int namespaceId, CancellationToken ct = default)
    {
        var raw = await ReceiveExpectedExclusiveAsync(ReconcileFrames.IbltResponse, ct).ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, ReconcileFrames.IbltResponse);
        ValidateNamespaceStream(frame, namespaceId);
        return Iblt.Decode(frame.Payload);
    }

    private async Task<Iblt> ReceiveExpectedIbltAsync(byte frameType, int namespaceId, CancellationToken ct)
    {
        var raw = await ReceiveExpectedExclusiveAsync(frameType, ct).ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, frameType);
        ValidateNamespaceStream(frame, namespaceId);
        return Iblt.Decode(frame.Payload);
    }

    /// <summary>
    /// Pairwise IBLT exchange: initiate sends first then receives; acceptor reverse.
    /// Returns peeled symmetric difference (local − remote after subtract).
    /// </summary>
    public async Task<(List<byte[]> OnlyLocal, List<byte[]> OnlyRemote)> ExchangeIbltAsync(
        Node local, int namespaceId, bool initiate,
        byte[]? prefix = null, int cellCount = 64, CancellationToken ct = default)
    {
        if (initiate)
        {
            var mine = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
            await SendFrameAsync(ReconcileFrames.IbltRequest, 0x0001_0000u | (uint)namespaceId, 15, Iblt.Encode(mine), ct).ConfigureAwait(false);
            var peer = await ReceiveExpectedIbltAsync(ReconcileFrames.IbltResponse, namespaceId, ct).ConfigureAwait(false);
            mine.Subtract(peer);
            return mine.Peel();
        }
        else
        {
            var peer = await ReceiveExpectedIbltAsync(ReconcileFrames.IbltRequest, namespaceId, ct).ConfigureAwait(false);
            var mine = Iblt.BuildFromNode(local, namespaceId, prefix, cellCount);
            await SendFrameAsync(ReconcileFrames.IbltResponse, 0x0001_0000u | (uint)namespaceId, 15, Iblt.Encode(mine), ct).ConfigureAwait(false);
            mine.Subtract(peer);
            return mine.Peel();
        }
    }

    /// <summary>
    /// One direction: request missing keys via NEED_KEYS and fulfill with SNAPSHOT.
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
                    .Where(r => want.Contains(Convert.ToHexString(Iblt.NormalizeKey(r.RecordId.ToArray()))))
                    .ToList();
            }
            uint streamId = 0x0001_0000u | (uint)namespaceId;
            await SendFrameAsync(ReconcileFrames.SnapshotChunk, streamId, 13, Reconcile.EncodeSnapshotChunk(chunk), ct).ConfigureAwait(false);
            return chunk.Records.Count;
        }
    }

    /// <summary>
    /// One-way IBLT reconcile: initiator pulls onlyRemote; acceptor only serves.
    /// Prefer <see cref="BilateralReconcileViaIbltAsync"/> for mutual convergence.
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

    /// <summary>
    /// Bilateral IBLT reconcile: both peers pull their missing keys in one session.
    /// Phase 1 — initiator requests <c>onlyRemote</c>, acceptor serves.
    /// Phase 2 — roles reverse: acceptor requests its <c>onlyRemote</c>, initiator serves.
    /// Returns (onlyLocal, onlyRemote, fieldsAppliedOnThisNode).
    /// </summary>
    public async Task<(int OnlyLocal, int OnlyRemote, int FieldsApplied)> BilateralReconcileViaIbltAsync(
        Node local, int namespaceId, bool initiate,
        byte[]? prefix = null, int cellCount = 64, CancellationToken ct = default)
    {
        var (onlyLocal, onlyRemote) = await ExchangeIbltAsync(
            local, namespaceId, initiate, prefix, cellCount, ct).ConfigureAwait(false);

        int fieldsApplied = 0;

        // Phase 1: initiator pulls keys only on remote
        if (initiate)
        {
            fieldsApplied += await FulfillMissingKeysAsync(
                local, namespaceId, initiate: true, onlyRemote, ct).ConfigureAwait(false);
        }
        else
        {
            await FulfillMissingKeysAsync(
                local, namespaceId, initiate: false, onlyLocal, ct).ConfigureAwait(false);
        }

        // Phase 2: acceptor pulls keys only on initiator (roles swapped)
        if (initiate)
        {
            await FulfillMissingKeysAsync(
                local, namespaceId, initiate: false, onlyLocal, ct).ConfigureAwait(false);
        }
        else
        {
            fieldsApplied += await FulfillMissingKeysAsync(
                local, namespaceId, initiate: true, onlyRemote, ct).ConfigureAwait(false);
        }

        return (onlyLocal.Count, onlyRemote.Count, fieldsApplied);
    }
}
