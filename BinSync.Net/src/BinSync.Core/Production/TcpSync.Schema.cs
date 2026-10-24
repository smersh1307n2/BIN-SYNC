// SCHEMA_MAP (0x04) exchange on TcpSyncSession (§18.3).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    public async Task SendSchemaMapAsync(Node local, CancellationToken ct = default)
    {
        var payload = local.ExportSchemaMap();
        await _transport.SendAsync(
            FrameCodec.BuildFrame(SchemaMap.FrameType, 0, 18, payload),
            ct).ConfigureAwait(false);
    }

    public async Task<int> ReceiveSchemaMapAsync(Node local, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during SCHEMA_MAP");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SchemaMap.FrameType)
            throw new FrameException("FRAME_REJECTED");
        return local.LearnSchemaMap(frame.Payload);
    }

    public async Task<(int LearnedLocal, int LearnedRemote)> ExchangeSchemaMapAsync(
        Node local, bool initiate, CancellationToken ct = default)
    {
        if (initiate)
        {
            await SendSchemaMapAsync(local, ct).ConfigureAwait(false);
            int learned = await ReceiveSchemaMapAsync(local, ct).ConfigureAwait(false);
            return (0, learned);
        }
        else
        {
            int learned = await ReceiveSchemaMapAsync(local, ct).ConfigureAwait(false);
            await SendSchemaMapAsync(local, ct).ConfigureAwait(false);
            return (learned, 0);
        }
    }
}
