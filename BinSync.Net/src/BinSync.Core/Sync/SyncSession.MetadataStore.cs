namespace BinSync;

/// <summary>
/// Durable outbound transport path. The exact encoded payload is reserved before send
/// and remains replayable until the matching ACK is committed to Metadata Store.
/// </summary>
public partial class SyncSession
{
    /// <summary>
    /// Push one batch using durable per-peer Metadata Store progress. The legacy overload
    /// remains available for in-memory sessions and compatibility.
    /// </summary>
    public async Task<bool> PushBatchAsync(Node sender, int namespaceId,
        IMetadataPeerStateStore peerStateStore, string scopeId,
        int schemaVersion = 1, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(peerStateStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (sender.NodeId.IsZero) throw new ArgumentException("Sender identity must be non-zero.", nameof(sender));
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        await _pushGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            uint peerEpoch = checked((uint)PeerEpoch);
            var streams = peerStateStore.ReadPeerStreamStates(
                scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId);

            foreach (var stream in streams)
            {
                uint issuerEpoch = stream.State.PendingBatch?.IssuerEpoch ?? stream.State.IssuerEpoch;
                sender.RestorePeerStreamProgress(PeerNodeId, namespaceId, stream.AuthorStream,
                    issuerEpoch, stream.State.AckedCursor, stream.State.PushedCursor,
                    stream.State.StreamOffset, Digest16.From(stream.State.LastBatchDigest));
            }

            MetadataOutboundBatchReservation reservation;
            NodeId authorStream;
            var pending = streams.FirstOrDefault(stream => stream.State.PendingBatch is not null);
            OutboundBatchPayload batch;
            if (pending is not null)
            {
                authorStream = pending.AuthorStream;
                reservation = pending.State.PendingBatch!;
                batch = Materialize(reservation, authorStream, namespaceId);
            }
            else
            {
                var created = sender.CreateDeltaBatchPayloadForTransport(
                    PeerNodeId, namespaceId, schemaVersion: schemaVersion);
                if (created is null) return false;
                batch = created.Value;
                authorStream = batch.AuthorStreamId;
                var candidate = new MetadataOutboundBatchReservation
                {
                    BatchId = batch.BatchId,
                    IssuerEpoch = checked((uint)batch.IssuerEpoch),
                    StreamOffset = batch.StreamOffset,
                    CursorStart = checked((ulong)batch.CursorStart),
                    CursorEnd = checked((ulong)batch.CursorEnd),
                    PreviousBatchDigest = batch.PreviousBatchDigest.ToArray(),
                    BatchDigest = batch.BatchDigest.ToArray(),
                    Payload = batch.Payload
                };
                reservation = peerStateStore.ReserveOutboundBatch(
                    scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, authorStream, candidate);
                batch = Materialize(reservation, authorStream, namespaceId);
            }

            await SendFrameAsync(0x09, 0x0001_0000u | (uint)namespaceId, 1,
                reservation.Payload, ct).ConfigureAwait(false);
            peerStateStore.MarkOutboundBatchPushed(
                scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, authorStream, reservation.BatchId);

            var raw = await ReceivePacketAsync(0x0B, ct).ConfigureAwait(false);
            var frame = ParseExpectedSessionFrame(raw, 0x0B);
            ValidateNamespaceStream(frame, namespaceId);
            var (id, seq, author) = Transport.DecodeAck(frame.Payload);
            if (!reservation.BatchId.EqualsBytes(id))
                throw new InvalidOperationException("ACK batch id mismatch");
            if (seq != checked((long)reservation.CursorEnd) || author is null ||
                NodeId.From(author) != authorStream)
                throw new SyncException("ACK_OUT_OF_RANGE", reservation.BatchId);

            // Commit the durable cursor/digest first. If the process stops before the
            // Node mirror is updated, the next call restores it from this committed row.
            peerStateStore.AcknowledgeOutboundBatch(scopeId, sender.NodeId,
                PeerNodeId, peerEpoch, namespaceId, authorStream,
                reservation.BatchId, reservation.CursorEnd, reservation.BatchDigest);
            sender.AdvanceStreamForTransport(PeerNodeId, namespaceId, batch);
            sender.ProcessAck(PeerNodeId, namespaceId, seq, authorStream: authorStream);
            return true;
        }
        finally
        {
            _pushGate.Release();
        }
    }

    private static OutboundBatchPayload Materialize(
        MetadataOutboundBatchReservation reservation, NodeId authorStream, int namespaceId)
        => new(reservation.Payload.ToArray(), reservation.BatchId, authorStream, authorStream,
            namespaceId, checked((int)reservation.IssuerEpoch),
            checked((long)reservation.CursorStart), checked((long)reservation.CursorEnd),
            reservation.StreamOffset, Digest16.From(reservation.PreviousBatchDigest), Digest16.From(reservation.BatchDigest));
}
