namespace BinSync.Storage;

/// <summary>
/// Sends an authoritative provider snapshot using the v3.1 FULL_REPLACE protocol.
/// PrepareSnapshot computes and signs one immutable manifest; that prepared snapshot
/// can then be transferred to every roster member without rescanning it for each peer.
/// </summary>
public static class StorageFullReplaceSender
{
    /// <summary>
    /// Open one immutable provider snapshot, compute/sign its manifest once, send it
    /// to one member and release the snapshot after the transfer completes.
    /// </summary>
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
        using PreparedFullReplaceSnapshot prepared = PrepareSnapshot(
            provider, trustStore, roster, authorityNodeId, generation, authoritySeed);
        return await SendPreparedSnapshotAsync(
            prepared, session, gcStore, trustStore, roster, authorityNodeId,
            authoritySeed, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Capture one stable provider generation and compute its signed transfer manifest.
    /// The returned lease owns the provider snapshot; dispose it after every intended
    /// recipient has either installed the generation or the host abandons that attempt.
    /// </summary>
    public static PreparedFullReplaceSnapshot PrepareSnapshot(
        IDataProvider provider,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        ValidateAuthorityInputs(authorityNodeId, generation, authoritySeed);
        NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
        ValidateAuthorityRoster(roster, authorityNodeId);

        ProviderSnapshotHandle snapshot = provider.OpenSnapshot();
        try
        {
            PreparedManifest prepared = CreateManifest(
                provider, snapshot, trustStore, roster, authorityNodeId,
                generation, authoritySeed);
            return new PreparedFullReplaceSnapshot(
                provider, snapshot, prepared.Manifest,
                prepared.HlcWallFloor, prepared.HlcLogicalFloor);
        }
        catch
        {
            provider.ReleaseSnapshot(snapshot.Token);
            throw;
        }
    }

    /// <summary>
    /// Prepares a FULL_REPLACE source generation and durably records its provider
    /// token, signed manifest and HLC floor before the caller starts network transfer.
    /// Dispose detaches the in-process handle but preserves the generation for restart.
    /// </summary>
    public static PreparedFullReplaceSnapshot PrepareSnapshotAndPersist(
        IDataProvider provider,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed,
        StorageMetadataStore metadataStore,
        string scopeId,
        NodeId localReplica)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(metadataStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        if (provider is not IProviderDurableSnapshotStore durableSnapshots)
            throw new NotSupportedException("Restart-durable FULL_REPLACE requires IProviderDurableSnapshotStore.");
        PreparedFullReplaceSnapshot prepared = PrepareSnapshot(
            provider, trustStore, roster, authorityNodeId, generation, authoritySeed);
        try
        {
            if (!durableSnapshots.TryGetPersistedSnapshotWatermark(
                    prepared.Snapshot.Token, out ulong persistedWatermark) ||
                persistedWatermark != prepared.SourceChangeFeedWatermark)
                throw new InvalidOperationException("Provider did not durably persist the exact prepared snapshot token/watermark pair.");
            metadataStore.SavePreparedFullReplaceSnapshot(
                scopeId,
                localReplica,
                prepared.NamespaceId,
                new PreparedFullReplaceSnapshotState(
                    prepared.Generation,
                    prepared.Snapshot.Token,
                    prepared.SourceChangeFeedWatermark,
                    prepared.HlcWallFloor,
                    prepared.HlcLogicalFloor,
                    FullReplaceTransferProtocol.EncodeBegin(prepared.Manifest)));
            prepared.MarkPersisted();
            return prepared;
        }
        catch
        {
            prepared.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Prepare and durably persist a source snapshot while the coordinator's typed
    /// FULL_REPLACE commit barrier is held. The barrier excludes host publication,
    /// remote APPLY, provider commits and Metadata reservations for the target namespace.
    /// The host lease must cover every application-owned writer that is not represented
    /// by the coordinator itself.
    ///
    /// This establishes source-side snapshot admission only. It does not yet certify a
    /// per-author outbound/relay/receive baseline vector; callers must not use this
    /// overload alone as authorization for physical GC.
    /// </summary>
    public static PreparedFullReplaceSnapshot PrepareSnapshotAndPersistUnderCommitGuard(
        IDataProvider provider,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed,
        StorageMetadataStore metadataStore,
        string scopeId,
        NodeId localReplica,
        StorageSyncCoordinator coordinator,
        Func<IDisposable?> acquireHostCommitLease)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (!coordinator.UsesDataProvider(provider))
            throw new ArgumentException("FULL_REPLACE coordinator must own the exact source provider instance.", nameof(coordinator));
        if (!coordinator.UsesMetadataPeerStateStore(metadataStore))
            throw new ArgumentException("FULL_REPLACE coordinator must own the exact durable Metadata Store instance.", nameof(coordinator));
        if (coordinator.NamespaceId != roster.NamespaceId ||
            coordinator.LocalReplicaId != localReplica)
            throw new ArgumentException("FULL_REPLACE coordinator identity does not match the source namespace and replica.");

        using IFullReplaceCommitLease commitLease =
            coordinator.AcquireFullReplaceCommitGuard(acquireHostCommitLease);
        if (commitLease.ScopeId != scopeId ||
            commitLease.NamespaceId != roster.NamespaceId ||
            commitLease.LocalReplica != localReplica)
            throw new InvalidOperationException("Acquired FULL_REPLACE lease is bound to a different namespace or local replica.");

        return PrepareSnapshotAndPersist(
            provider, trustStore, roster, authorityNodeId, generation, authoritySeed,
            metadataStore, scopeId, localReplica);
    }

    /// <summary>
    /// Restores a persisted provider generation after process restart and revalidates
    /// its signed manifest against the currently accepted roster and authority.
    /// </summary>
    public static PreparedFullReplaceSnapshot RestorePreparedSnapshot(
        IDataProvider provider,
        StorageMetadataStore metadataStore,
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        Guid generation,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        byte[] authoritySeed)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(metadataStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (provider is not IProviderDurableSnapshotStore durableSnapshots)
            throw new NotSupportedException("Restoring FULL_REPLACE requires IProviderDurableSnapshotStore.");
        if (!metadataStore.TryGetPreparedFullReplaceSnapshot(
                scopeId, localReplica, namespaceId, generation, out PreparedFullReplaceSnapshotState? state) ||
            state is null)
            throw new KeyNotFoundException("No durable prepared FULL_REPLACE source generation exists.");

        FullReplaceTransferProtocol.Begin manifest =
            FullReplaceTransferProtocol.DecodeBegin(state.ManifestBytes);
        if (manifest.Generation != generation ||
            manifest.NamespaceId != namespaceId ||
            manifest.SourceChangeFeedWatermark != state.SourceFeedWatermark)
            throw new InvalidDataException("Persisted FULL_REPLACE manifest and snapshot metadata disagree.");

        if (!durableSnapshots.TryGetPersistedSnapshotWatermark(
                state.SnapshotToken, out ulong persistedWatermark) ||
            persistedWatermark != state.SourceFeedWatermark)
            throw new InvalidDataException("The persisted FULL_REPLACE provider snapshot token/watermark pair is missing or inconsistent.");
        var snapshot = new ProviderSnapshotHandle(state.SnapshotToken, state.SourceFeedWatermark);
        ProviderSnapshotPage firstPage = provider.ReadSnapshot(snapshot.Token, 0, 1);
        if (firstPage.Entries.Count != firstPage.NextIndex ||
            firstPage.NextIndex > manifest.ExpectedEntryCount ||
            firstPage.IsComplete != (firstPage.NextIndex == manifest.ExpectedEntryCount))
            throw new InvalidDataException("Restored provider snapshot does not match the signed entry count.");

        var prepared = new PreparedFullReplaceSnapshot(
            provider, snapshot, manifest, state.HlcWallFloor, state.HlcLogicalFloor);
        prepared.MarkPersisted();
        try
        {
            ValidatePreparedSnapshot(prepared, trustStore, roster, authorityNodeId, authoritySeed);
            return prepared;
        }
        catch
        {
            prepared.Dispose(); // Persisted generation is retained for explicit recovery or cleanup.
            throw;
        }
    }

    /// <summary>
    /// Permanently releases a prepared generation after all recipient transfers have
    /// completed, then removes its durable sender-progress record.
    /// </summary>
    public static void ReleasePreparedSnapshot(
        PreparedFullReplaceSnapshot prepared,
        IDataProvider provider,
        StorageMetadataStore metadataStore,
        string scopeId,
        NodeId localReplica)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(metadataStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        prepared.ReleasePermanently(provider);
        metadataStore.DeletePreparedFullReplaceSnapshot(
            scopeId, localReplica, prepared.NamespaceId, prepared.Generation);
    }

    /// <summary>Reclaims provider snapshot generations not present in durable sender progress.</summary>
    public static void PruneUnreferencedPreparedSnapshots(
        IDataProvider provider,
        StorageMetadataStore metadataStore,
        string scopeId,
        NodeId localReplica,
        int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(metadataStore);
        if (provider is not IProviderDurableSnapshotStore durableSnapshots)
            throw new NotSupportedException("The provider does not expose durable snapshot presence/pruning.");
        IReadOnlyList<Guid> generations = metadataStore.ListPreparedFullReplaceSnapshotGenerations(
            scopeId, localReplica, namespaceId);
        var activeSnapshotTokens = new HashSet<Guid>();
        foreach (Guid generation in generations)
        {
            if (!metadataStore.TryGetPreparedFullReplaceSnapshot(
                    scopeId, localReplica, namespaceId, generation, out PreparedFullReplaceSnapshotState? state) ||
                state is null)
                throw new InvalidDataException("Prepared FULL_REPLACE index references a missing source generation.");
            if (durableSnapshots.HasPersistedSnapshot(state.SnapshotToken))
                activeSnapshotTokens.Add(state.SnapshotToken);
            else
                // Provider generation was released before a crash interrupted metadata cleanup.
                metadataStore.DeletePreparedFullReplaceSnapshot(scopeId, localReplica, namespaceId, generation);
        }
        durableSnapshots.PruneSnapshotsExcept(activeSnapshotTokens);
    }

    /// <summary>
    /// Validate a retained prepared snapshot against the currently accepted roster,
    /// authority key and source snapshot watermark before any network side effect.
    /// </summary>
    public static void ValidatePreparedSnapshot(
        PreparedFullReplaceSnapshot prepared,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        byte[] authoritySeed)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        ValidateAuthorityInputs(authorityNodeId, prepared.Generation, authoritySeed);
        NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
        ValidateAuthorityRoster(roster, authorityNodeId);
        if (prepared.NamespaceId != roster.NamespaceId ||
            prepared.RosterGeneration != roster.RosterGeneration ||
            !prepared.RosterDigest.AsSpan().SequenceEqual(
                NamespaceRosterProtocol.ComputeDigest(roster)) ||
            prepared.SourceChangeFeedWatermark != prepared.Snapshot.ChangeFeedWatermark)
            throw new InvalidOperationException(
                "Prepared FULL_REPLACE snapshot belongs to a different roster or provider generation.");
        FullReplaceTransferProtocol.VerifyBegin(
            prepared.Manifest, roster, trustStore);
    }

    /// <summary>
    /// Transfer a previously prepared immutable snapshot to one roster member.
    /// The provider and snapshot token must be the exact pair returned by PrepareSnapshot.
    /// This method does not dispose the prepared snapshot; the caller may reuse it for
    /// the next recipient and owns its final disposal.
    /// </summary>
    public static async Task<FullReplaceInstalledAck> SendPreparedSnapshotAsync(
        PreparedFullReplaceSnapshot prepared,
        SyncSession session,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        byte[] authoritySeed,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(prepared);
        using IDisposable transferLease = prepared.AcquireTransferLease();
        ValidatePreparedSnapshot(
            prepared, trustStore, roster, authorityNodeId, authoritySeed);
        IDataProvider provider = prepared.Provider;
        return await SendSnapshotCoreAsync(
            provider, prepared.Snapshot, prepared.Manifest,
            prepared.HlcWallFloor, prepared.HlcLogicalFloor,
            session, gcStore, trustStore, roster, authorityNodeId, authoritySeed, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Transfer an already-open immutable provider snapshot. The caller owns the
    /// ProviderSnapshotHandle and must release it after the transfer. This overload
    /// computes a manifest for compatibility with callers that manage snapshot lifetime.
    /// Use PrepareSnapshot + SendPreparedSnapshotAsync for multiple recipients.
    /// </summary>
    public static Task<FullReplaceInstalledAck> SendSnapshotAsync(
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
        PreparedManifest prepared = CreateManifest(
            provider, snapshot, trustStore, roster, authorityNodeId,
            generation, authoritySeed);
        return SendSnapshotCoreAsync(
            provider, snapshot, prepared.Manifest,
            prepared.HlcWallFloor, prepared.HlcLogicalFloor,
            session, gcStore, trustStore, roster, authorityNodeId, authoritySeed, ct);
    }

    private sealed record PreparedManifest(
        FullReplaceTransferProtocol.Begin Manifest,
        ulong HlcWallFloor,
        uint HlcLogicalFloor);

    private static PreparedManifest CreateManifest(
        IDataProvider provider,
        ProviderSnapshotHandle snapshot,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        Guid generation,
        byte[] authoritySeed)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(roster);
        ValidateAuthorityInputs(authorityNodeId, generation, authoritySeed);
        NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
        ValidateAuthorityRoster(roster, authorityNodeId);

        byte[] digest;
        int entryCount;
        ulong maxHlcWall = 0;
        uint maxHlcLogical = 0;
        using (var builder = new ProviderFullReplaceDigestBuilder(
            generation, snapshot.ChangeFeedWatermark))
        {
            int index = 0;
            while (true)
            {
                ProviderSnapshotPage page = provider.ReadSnapshot(
                    snapshot.Token, index, FullReplaceTransferProtocol.MaxChunkEntries);
                ValidatePage(page, index, FullReplaceTransferProtocol.MaxChunkEntries);
                foreach (ProviderMutation mutation in page.Entries)
                {
                    if (mutation is null)
                        throw new InvalidDataException("FULL_REPLACE source snapshot contains a null mutation.");
                    if (mutation.HlcWall > maxHlcWall ||
                        (mutation.HlcWall == maxHlcWall && mutation.HlcLogical > maxHlcLogical))
                    {
                        maxHlcWall = mutation.HlcWall;
                        maxHlcLogical = mutation.HlcLogical;
                    }
                }
                builder.AppendPage(page.Entries);
                index = page.NextIndex;
                if (page.IsComplete)
                    break;
            }

            entryCount = builder.EntryCount;
            digest = builder.Complete();
        }

        var unsignedManifest = new FullReplaceTransferProtocol.Begin(
            roster.NamespaceId,
            generation,
            roster.RosterGeneration,
            NamespaceRosterProtocol.ComputeDigest(roster),
            snapshot.ChangeFeedWatermark,
            entryCount,
            digest,
            Array.Empty<byte>());
        FullReplaceTransferProtocol.Begin manifest =
            FullReplaceTransferProtocol.SignBegin(unsignedManifest, authoritySeed);
        FullReplaceTransferProtocol.VerifyBegin(manifest, roster, trustStore);
        return new PreparedManifest(manifest, maxHlcWall, maxHlcLogical);
    }

    private static async Task<FullReplaceInstalledAck> SendSnapshotCoreAsync(
        IDataProvider provider,
        ProviderSnapshotHandle snapshot,
        FullReplaceTransferProtocol.Begin manifest,
        ulong expectedHlcWallFloor,
        uint expectedHlcLogicalFloor,
        SyncSession session,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        NamespaceRosterCertificate roster,
        NodeId authorityNodeId,
        byte[] authoritySeed,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        ValidateAuthorityInputs(authorityNodeId, manifest.Generation, authoritySeed);
        NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
        ValidateAuthorityRoster(roster, authorityNodeId);
        FullReplaceTransferProtocol.VerifyBegin(manifest, roster, trustStore);

        if (manifest.NamespaceId != roster.NamespaceId ||
            manifest.SourceChangeFeedWatermark != snapshot.ChangeFeedWatermark ||
            manifest.RosterGeneration != roster.RosterGeneration ||
            !manifest.RosterDigest.AsSpan().SequenceEqual(
                NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException(
                "Prepared FULL_REPLACE manifest does not match the supplied snapshot or accepted roster.");

        if (session.PeerNodeId.IsZero || session.PeerEpoch <= 0)
            throw new InvalidDataException("FULL_REPLACE target session has no valid peer identity.");
        NamespaceRosterMember target = roster.Members.FirstOrDefault(
                member => member.NodeId == session.PeerNodeId)
            ?? throw new InvalidDataException(
                "FULL_REPLACE target is not in the accepted namespace roster.");
        if (target.Status == NamespaceMemberStatus.Retired ||
            target.TransportPeerEpoch != checked((uint)session.PeerEpoch))
            throw new InvalidDataException(
                "FULL_REPLACE target is retired or its transport epoch does not match the roster.");

        await session.SendFullReplaceBeginAsync(
            manifest, frameSequence: 1, ct).ConfigureAwait(false);
        FullReplaceTransferProtocol.Progress progress =
            await session.ReceiveFullReplaceReadyAsync(
                roster.NamespaceId, expectedFrameSequence: 1, ct).ConfigureAwait(false);
        ValidateProgress(progress, manifest);

        int nextIndex = progress.NextIndex;
        ulong frameSequence = progress.NextFrameSequence;
        while (nextIndex < manifest.ExpectedEntryCount)
        {
            ProviderSnapshotPage page = provider.ReadSnapshot(
                snapshot.Token, nextIndex, FullReplaceTransferProtocol.MaxChunkEntries);
            ValidatePage(page, nextIndex, FullReplaceTransferProtocol.MaxChunkEntries);
            if (page.Entries.Count == 0 ||
                page.NextIndex > manifest.ExpectedEntryCount ||
                page.IsComplete != (page.NextIndex == manifest.ExpectedEntryCount))
                throw new InvalidDataException(
                    "Provider snapshot changed or ended before the signed entry count.");

            await session.SendFullReplaceChunkAsync(
                new FullReplaceTransferProtocol.Chunk(
                    roster.NamespaceId, manifest.Generation, nextIndex, page.Entries),
                frameSequence, ct).ConfigureAwait(false);
            nextIndex = page.NextIndex;
            frameSequence = checked(frameSequence + 1);
        }

        await session.SendFullReplaceCommitAsync(
            new FullReplaceTransferProtocol.TransferId(
                roster.NamespaceId, manifest.Generation),
            frameSequence, ct).ConfigureAwait(false);

        FullReplaceInstalledAck acknowledgement =
            await session.ReceiveFullReplaceInstalledAckAsync(roster, ct)
                .ConfigureAwait(false);
        if (!acknowledgement.Installed ||
            acknowledgement.Generation != manifest.Generation ||
            !acknowledgement.Digest.AsSpan().SequenceEqual(manifest.ExpectedDigest) ||
            acknowledgement.MemberNodeId != session.PeerNodeId ||
            acknowledgement.MemberTransportEpoch != checked((uint)session.PeerEpoch) ||
            acknowledgement.RosterGeneration != roster.RosterGeneration ||
            !acknowledgement.RosterDigest.AsSpan().SequenceEqual(manifest.RosterDigest) ||
            acknowledgement.HlcWallFloor != expectedHlcWallFloor ||
            acknowledgement.HlcLogicalFloor != expectedHlcLogicalFloor)
            throw new InvalidDataException(
                "FULL_REPLACE installed ACK does not match the target, roster, generation, digest or source snapshot HLC floor.");

        gcStore.AcceptFullReplaceInstalledAck(
            FullReplaceInstalledAckProtocol.EncodeSigned(acknowledgement), roster);
        return acknowledgement;
    }

    private static void ValidateAuthorityInputs(
        NodeId authorityNodeId, Guid generation, byte[] authoritySeed)
    {
        if (generation == Guid.Empty)
            throw new ArgumentException(
                "FULL_REPLACE generation must be non-empty.", nameof(generation));
        if (authorityNodeId.IsZero || authoritySeed.Length != 32)
            throw new ArgumentException(
                "Namespace authority identity and Ed25519 seed are required.");
    }

    private static void ValidateAuthorityRoster(
        NamespaceRosterCertificate roster, NodeId authorityNodeId)
    {
        if (roster.AuthorityNodeId != authorityNodeId)
            throw new InvalidDataException(
                "Only the pinned namespace authority may originate FULL_REPLACE.");
        NamespaceRosterMember authority = roster.Members.FirstOrDefault(
                member => member.NodeId == authorityNodeId)
            ?? throw new InvalidDataException(
                "Namespace authority is not listed as a namespace member.");
        if (authority.Status != NamespaceMemberStatus.Active)
            throw new InvalidDataException(
                "Retired or Joining namespace authority cannot originate FULL_REPLACE.");
    }

    private static void ValidatePage(
        ProviderSnapshotPage page, int expectedStart, int limit)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (page.Entries is null || page.NextIndex < expectedStart ||
            page.NextIndex != checked(expectedStart + page.Entries.Count) ||
            page.Entries.Count > limit ||
            (!page.IsComplete && page.Entries.Count != limit))
            throw new InvalidDataException(
                "Provider snapshot returned a non-contiguous or non-canonical page.");
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
            throw new InvalidDataException(
                "FULL_REPLACE_READY does not match the signed manifest or durable page boundary.");
    }
}
