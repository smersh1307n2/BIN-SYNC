namespace BinSync.Storage;

/// <summary>
/// Sends an authoritative provider snapshot using the v3.1 FULL_REPLACE protocol.
/// The provider snapshot is held stable for both digest and transfer passes.
/// </summary>
public static class StorageFullReplaceSender
{
    public static async Task<FullReplaceInstalledAck> SendAsync(
        IDataProvider provider,
        SyncSession session,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ProviderSnapshotHandle snapshot = provider.OpenSnapshot();
        try
        {
            return await SendSnapshotAsync(
                provider, snapshot, session, gcStore, trustStore, roster,
                authorityNodeId, generation, authoritySeed, ct).ConfigureAwait(false);
        }
        finally
        {
            provider.ReleaseSnapshot(snapshot.Token);
        }
    }

    /// <summary>
    /// Transfer an already-open immutable provider snapshot. Hosts sending one
    /// generation to several members should share this handle so every recipient
    /// receives exactly the same ChangeFeedWatermark, entry count and digest even
    /// while the live provider continues accepting later writes.
    /// The caller owns and must release the snapshot handle.
    /// </summary>
    public static async Task<FullReplaceInstalledAck> SendSnapshotAsync(
        IDataProvider provider,
        ProviderSnapshotHandle snapshot,
        SyncSession session,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (generation == Guid.Empty)
            throw new ArgumentException("FULL_REPLACE generation must be non-empty.", nameof(generation));
        if (authorityNodeId.IsZero || authoritySeed.Length != 32)
            throw new ArgumentException("Namespace authority identity and Ed25519 seed are required.");

        NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
        if (roster.AuthorityNodeId != authorityNodeId)
            throw new InvalidDataException("Only the pinned namespace authority may originate FULL_REPLACE.");
        NamespaceRosterMember authority = roster.Members.FirstOrDefault(member => member.NodeId == authorityNodeId)
            ?? throw new InvalidDataException("Namespace authority is not listed as a namespace member.");
        if (authority.Status != NamespaceMemberStatus.Active)
            throw new InvalidDataException("Retired or Joining namespace authority cannot originate FULL_REPLACE.");
        if (session.PeerNodeId.IsZero || session.PeerEpoch <= 0)
            throw new InvalidDataException("FULL_REPLACE target session has no valid peer identity.");
        NamespaceRosterMember target = roster.Members.FirstOrDefault(member => member.NodeId == session.PeerNodeId)
            ?? throw new InvalidDataException("FULL_REPLACE target is not in the accepted namespace roster.");
        if (target.Status == NamespaceMemberStatus.Retired ||
            target.TransportPeerEpoch != checked((uint)session.PeerEpoch))
            throw new InvalidDataException("FULL_REPLACE target is retired or its transport epoch does not match the roster.");

        byte[] digest;
        int entryCount;
        using (var builder = new ProviderFullReplaceDigestBuilder(generation, snapshot.ChangeFeedWatermark))
        {
            int index = 0;
            while (true)
            {
                ProviderSnapshotPage page = provider.ReadSnapshot(
                    snapshot.Token, index, FullReplaceTransferProtocol.MaxChunkEntries);
                ValidatePage(page, index, FullReplaceTransferProtocol.MaxChunkEntries);
                builder.AppendPage(page.Entries);
                index = page.NextIndex;
                if (page.IsComplete) break;
            }
            entryCount = builder.EntryCount;
            digest = builder.Complete();
        }

        var unsignedManifest = new FullReplaceTransferProtocol.Begin(
            roster.NamespaceId, generation, roster.RosterGeneration,
            NamespaceRosterProtocol.ComputeDigest(roster), snapshot.ChangeFeedWatermark,
            entryCount, digest, Array.Empty<byte>());
        FullReplaceTransferProtocol.Begin manifest =
            FullReplaceTransferProtocol.SignBegin(unsignedManifest, authoritySeed);
        FullReplaceTransferProtocol.VerifyBegin(manifest, roster, trustStore);

        await session.SendFullReplaceBeginAsync(manifest, frameSequence: 1, ct).ConfigureAwait(false);
        FullReplaceTransferProtocol.Progress progress =
            await session.ReceiveFullReplaceReadyAsync(roster.NamespaceId,
                expectedFrameSequence: 1, ct).ConfigureAwait(false);
        ValidateProgress(progress, manifest);

        int nextIndex = progress.NextIndex;
        ulong frameSequence = progress.NextFrameSequence;
        while (nextIndex < entryCount)
        {
            ProviderSnapshotPage page = provider.ReadSnapshot(
                snapshot.Token, nextIndex, FullReplaceTransferProtocol.MaxChunkEntries);
            ValidatePage(page, nextIndex, FullReplaceTransferProtocol.MaxChunkEntries);
            if (page.Entries.Count == 0 || page.NextIndex > entryCount ||
                page.IsComplete != (page.NextIndex == entryCount))
                throw new InvalidDataException("Provider snapshot changed or ended before the signed entry count.");
            await session.SendFullReplaceChunkAsync(
                new FullReplaceTransferProtocol.Chunk(roster.NamespaceId, generation, nextIndex, page.Entries),
                frameSequence, ct).ConfigureAwait(false);
            nextIndex = page.NextIndex;
            frameSequence = checked(frameSequence + 1);
        }

        await session.SendFullReplaceCommitAsync(
            new FullReplaceTransferProtocol.TransferId(roster.NamespaceId, generation),
            frameSequence, ct).ConfigureAwait(false);

        FullReplaceInstalledAck acknowledgement =
            await session.ReceiveFullReplaceInstalledAckAsync(roster, ct).ConfigureAwait(false);
        if (acknowledgement.Generation != generation ||
            !acknowledgement.Digest.AsSpan().SequenceEqual(digest) ||
            acknowledgement.MemberNodeId != session.PeerNodeId ||
            acknowledgement.MemberTransportEpoch != checked((uint)session.PeerEpoch))
            throw new InvalidDataException("FULL_REPLACE installed ACK does not match the target, generation or digest.");

        gcStore.AcceptFullReplaceInstalledAck(
            FullReplaceInstalledAckProtocol.EncodeSigned(acknowledgement), roster);
        return acknowledgement;
    }

    private static void ValidatePage(ProviderSnapshotPage page, int expectedStart, int limit)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (page.Entries is null || page.NextIndex < expectedStart ||
            page.NextIndex != checked(expectedStart + page.Entries.Count) ||
            page.Entries.Count > limit || (!page.IsComplete && page.Entries.Count != limit))
            throw new InvalidDataException("Provider snapshot returned a non-contiguous or non-canonical page.");
    }

    private static void ValidateProgress(
        FullReplaceTransferProtocol.Progress progress,
        FullReplaceTransferProtocol.Begin manifest)
    {
        if (progress.NamespaceId != manifest.NamespaceId ||
            progress.Generation != manifest.Generation ||
            progress.ExpectedEntryCount != manifest.ExpectedEntryCount ||
            !progress.ExpectedDigest.AsSpan().SequenceEqual(manifest.ExpectedDigest) ||
            progress.NextIndex > progress.ExpectedEntryCount ||
            (progress.NextIndex != progress.ExpectedEntryCount &&
             progress.NextIndex % FullReplaceTransferProtocol.MaxChunkEntries != 0))
            throw new InvalidDataException("FULL_REPLACE_READY does not match the signed manifest or durable page boundary.");
    }
}
