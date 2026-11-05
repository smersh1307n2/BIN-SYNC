// SCHEMA_MAP (0x04) exchange on SyncSession (§18.3).
namespace BinSync;

/// <summary>Established TCP (optionally TLS) sync session with a peer.</summary>
public partial class SyncSession
{
    /// <summary>Send the local schema map to the peer.</summary>
    public async Task SendSchemaMapAsync(Node local, CancellationToken ct = default)
    {
        var payload = local.ExportSchemaMap();
        await SendFrameAsync(SchemaMap.FrameType, 0u, 18ul, payload, ct).ConfigureAwait(false);
    }

    /// <summary>Receive a schema map and learn unknown schemas; returns how many were learned.</summary>
    public async Task<int> ReceiveSchemaMapAsync(Node local, CancellationToken ct = default)
    {
        var raw = await ReceiveExpectedExclusiveAsync(SchemaMap.FrameType, ct).ConfigureAwait(false)
           ;
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SchemaMap.FrameType)
            throw new FrameException("FRAME_REJECTED");
        return local.LearnSchemaMap(frame.Payload);
    }

    /// <summary>Bidirectional schema-map exchange; returns learned counts both ways.</summary>
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
