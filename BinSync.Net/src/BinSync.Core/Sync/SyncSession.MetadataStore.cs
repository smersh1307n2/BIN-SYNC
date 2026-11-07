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
                if (stream.State.PendingBatch is null && stream.State.PushedCursor > stream.State.AckedCursor)
                    throw new InvalidDataException("Durable peer pushed cursor has no immutable replay reservation.");
                uint issuerEpoch = stream.State.PendingBatch?.IssuerEpoch ?? stream.State.IssuerEpoch;
                sender.RestorePeerStreamProgress(PeerNodeId, namespaceId, stream.AuthorStream,
                    issuerEpoch, stream.State.AckedCursor, stream.State.PushedCursor,
                    stream.State.StreamOffset, Digest16.From(stream.State.LastBatchDigest));
            }

            MetadataOutboundBatchReservation reservation;
            NodeId authorStream;
            Node.OutboundBatchPayload batch;
            var pending = streams.FirstOrDefault(stream => stream.State.PendingBatch is not null);
            if (pending is not null)
            {
                authorStream = pending.AuthorStream;
                reservation = pending.State.PendingBatch!;
                batch = Materialize(reservation, authorStream, namespaceId);
            }
            else
            {
                var authors = peerStateStore.ReadOutboundAuthors(scopeId, sender.NodeId, namespaceId)
                    .Concat(peerStateStore.ReadRelayAuthors(scopeId, sender.NodeId, namespaceId))
                    .Distinct()
                    .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                    .ToArray();
                bool found = false;
                reservation = null!;
                authorStream = default;
                batch = default;

                foreach (NodeId candidateAuthor in authors)
                {
                    if (candidateAuthor == PeerNodeId) continue;
                    if (!streams.Any(stream => stream.AuthorStream == candidateAuthor))
                    {
                        var legacy = sender.ReadPeerStreamProgress(PeerNodeId, namespaceId, candidateAuthor);
                        if (legacy.PushedCursor > legacy.AckedCursor)
                            throw new InvalidDataException("Legacy Node peer state has an unacknowledged batch without a durable replay reservation; finish that retry before enabling Metadata Store transport.");
                        peerStateStore.RegisterInterestedPeerStream(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor,
                            legacy.AckedCursor, legacy.StreamOffset, legacy.IssuerEpoch, legacy.LastBatchDigest);
                        sender.RestorePeerStreamProgress(PeerNodeId, namespaceId, candidateAuthor,
                            legacy.IssuerEpoch, legacy.AckedCursor, legacy.PushedCursor,
                            legacy.StreamOffset, Digest16.From(legacy.LastBatchDigest));
                    }

                    IReadOnlyList<(ulong Sequence, BatchId ChangeId, byte[] Payload)> pageItems;
                    if (candidateAuthor == sender.NodeId)
                    {
                        var page = peerStateStore.ReadOutboundForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 256);
                        pageItems = page.Items.Select(item => (item.Sequence, item.ChangeId, item.Payload)).ToArray();
                    }
                    else
                    {
                        var page = peerStateStore.ReadRelayForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 256);
                        pageItems = page.Items.Select(item => (item.Sequence, item.ChangeId, item.Payload)).ToArray();
                    }

                    if (pageItems.Count == 0) continue;
                    var created = sender.CreateDeltaBatchPayloadFromMetadata(
                        PeerNodeId, namespaceId, candidateAuthor, pageItems,
                        schemaVersion: schemaVersion);
                    if (created is null) continue;
                    batch = created.Value;
                    authorStream = candidateAuthor;
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
                    found = true;
                    break;
                }

                peerStateStore.RegisterInterestedPeer(scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId);
                if (!found) return false;
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

    private static Node.OutboundBatchPayload Materialize(
        MetadataOutboundBatchReservation reservation, NodeId authorStream, int namespaceId)
        => new Node.OutboundBatchPayload(reservation.Payload.ToArray(), reservation.BatchId, authorStream, authorStream,
            namespaceId, checked((int)reservation.IssuerEpoch),
            checked((long)reservation.CursorStart), checked((long)reservation.CursorEnd),
            reservation.StreamOffset, Digest16.From(reservation.PreviousBatchDigest), Digest16.From(reservation.BatchDigest));
}
