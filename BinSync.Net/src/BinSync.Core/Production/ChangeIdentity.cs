// Global ChangeId identity ledger. Route/alias-independent semantic fingerprints.
namespace BinSync;

/// <summary>One durable ChangeId identity fingerprint; zero is reserved for unknown compacted entries.</summary>
internal readonly struct ChangeLedgerValue
{
    /// <summary>
    /// Zero is reserved for entries whose original semantic payload is no longer available
    /// to fingerprint. Every known origin-bound identity must be non-zero. This keeps
    /// the global ledger value at 16 bytes without a padded Boolean.
    /// </summary>
    public Digest16 Fingerprint { get; }
    public bool IsKnown => !Fingerprint.IsZero;

    public ChangeLedgerValue(Digest16 fingerprint, bool isKnown)
    {
        if (isKnown && fingerprint.IsZero)
            throw new ArgumentException("Zero is reserved for an unknown legacy fingerprint.", nameof(fingerprint));
        Fingerprint = isKnown ? fingerprint : default;
    }
}

/// <summary>Canonical semantic identity for a mutation, independent of transport aliases.</summary>
public static class ChangeIdentity
{
    private static readonly byte[] Domain = System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/CHANGE-ID/v1");
    private static readonly byte[] LedgerDomain = System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/CHANGE-LEDGER/v1");

    /// <summary>Canonical semantic fingerprint, independent of transport aliases and Origin.</summary>
    public static Digest16 Fingerprint(int namespaceId, RecordId recordId, byte opKind,
        object operation, NodeId resolvedAuthor, IReadOnlyList<BatchId>? causalDeps = null)
        => FingerprintCore(namespaceId, recordId, opKind, operation, resolvedAuthor,
            includeOrigin: false, originAuthorEpoch: 0, originSequence: 0, causalDeps);

    /// <summary>
    /// Compact global-ledger identity fingerprint. The immutable original-author tuple
    /// is written into the same BLAKE3 input as the semantic mutation, so this is one
    /// hash pass rather than hashing a semantic fingerprint and then hashing it again.
    /// </summary>
    internal static Digest16 LedgerFingerprint(int namespaceId, RecordId recordId, byte opKind,
        object operation, NodeId originAuthorNodeId, uint originAuthorEpoch, ulong originSequence,
        IReadOnlyList<BatchId>? causalDeps = null)
    {
        if (originAuthorNodeId.IsZero || originAuthorEpoch == 0 || originSequence == 0)
            throw new InvalidDataException("A known ChangeId identity requires a complete immutable Origin tuple.");
        return FingerprintCore(namespaceId, recordId, opKind, operation, originAuthorNodeId,
            includeOrigin: true, originAuthorEpoch, originSequence, causalDeps);
    }

    private static Digest16 FingerprintCore(int namespaceId, RecordId recordId, byte opKind,
        object operation, NodeId resolvedAuthor, bool includeOrigin,
        uint originAuthorEpoch, ulong originSequence, IReadOnlyList<BatchId>? causalDeps)
    {
        Node.ValidateNamespaceId(namespaceId);
        // Never mutate the decoded operation: ApplyBatch still needs to know whether
        // its original payload was compact or full-author when deciding relay encoding.
        // Encode the canonical full-author operation directly into one writer.
        int operationLength = operation switch
        {
            OpFieldWrite x => checked(17 + 16 + Codec.VarintLength((ulong)x.Value.Length) + x.Value.Length),
            OpFieldTomb => 17 + 16,
            OpRecordTombstone => 29 + 16,
            _ => throw new InvalidDataException("Unsupported operation for ChangeId fingerprint.")
        };

        byte[] domain = includeOrigin ? LedgerDomain : Domain;
        int originLength = includeOrigin ? 16 + 4 + 8 : 0;
        var w = new Writer(domain.Length + 2 + 16 + originLength + 1 +
            Codec.VarintLength((ulong)operationLength) + operationLength + 1);
        w.Raw(domain).U16((ushort)namespaceId);
        if (includeOrigin)
            w.B128(resolvedAuthor).U32(originAuthorEpoch).U64(originSequence);
        w.B128(recordId).U8(opKind).Varint((ulong)operationLength);

        switch (operation)
        {
            case OpFieldWrite x:
                w.U8(OpsConst.TagFieldWrite).U16(x.FieldId).U16(x.KeyEpoch)
                    .U64(x.Wall).U32(x.Logical).B128(resolvedAuthor)
                    .Varint((ulong)x.Value.Length).Raw(x.Value);
                break;
            case OpFieldTomb x:
                w.U8(OpsConst.TagFieldTomb).U16(x.FieldId).U16(x.KeyEpoch)
                    .U64(x.Wall).U32(x.Logical).B128(resolvedAuthor);
                break;
            case OpRecordTombstone x:
                w.U8(OpsConst.TagRecordTomb).B128(x.RecordId)
                    .U64(x.Wall).U32(x.Logical).B128(resolvedAuthor);
                break;
        }

        // Causal dependencies are semantically relevant when ApplyBatchCausal is
        // enabled. Their order and duplicate repetitions are not, so normalize as
        // a sorted set while keeping the empty/common path allocation-free.
        if (causalDeps is null || causalDeps.Count == 0)
            w.Varint(0);
        else if (causalDeps.Count == 1)
        {
            if (causalDeps[0].IsZero)
                throw new InvalidDataException("Causal dependency ID cannot be zero.");
            w.Varint(1).B128(causalDeps[0]);
        }
        else
        {
            var sorted = new BatchId[causalDeps.Count];
            for (int i = 0; i < causalDeps.Count; i++)
            {
                var dependency = causalDeps[i];
                if (dependency.IsZero)
                    throw new InvalidDataException("Causal dependency ID cannot be zero.");
                sorted[i] = dependency;
            }
            Array.Sort(sorted, static (left, right) =>
            {
                int order = left.Lo.CompareTo(right.Lo);
                return order != 0 ? order : left.Hi.CompareTo(right.Hi);
            });
            int uniqueCount = 1;
            for (int i = 1; i < sorted.Length; i++)
            {
                if (sorted[i].Equals(sorted[uniqueCount - 1]))
                    continue;
                sorted[uniqueCount++] = sorted[i];
            }
            w.Varint((ulong)uniqueCount);
            for (int i = 0; i < uniqueCount; i++) w.B128(sorted[i]);
        }
        var fingerprint = Digests.Blake3Digest16(w.WrittenSpan);
        if (fingerprint.IsZero)
            throw new InvalidDataException("The reserved zero ChangeId fingerprint was produced.");
        return fingerprint;
    }

    /// <summary>
    /// Fingerprint a provider mutation directly, without materializing a temporary
    /// operation object. The encoding is byte-identical to the canonical full-author
    /// operation fingerprint used by the wire receive path.
    /// </summary>
    public static Digest16 Fingerprint(int namespaceId, ProviderMutation mutation, NodeId resolvedAuthor)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        Node.ValidateNamespaceId(namespaceId);
        if (resolvedAuthor.IsZero)
            throw new ArgumentException("Resolved author must be non-zero.", nameof(resolvedAuthor));

        byte opKind;
        int operationLength;
        if (mutation.IsRecordTombstone)
        {
            opKind = OpsConst.TagRecordTomb;
            operationLength = 29 + 16;
        }
        else if (mutation.IsTombstone)
        {
            opKind = OpsConst.TagFieldTomb;
            operationLength = 17 + 16;
        }
        else
        {
            opKind = OpsConst.TagFieldWrite;
            operationLength = checked(17 + 16 +
                Codec.VarintLength((ulong)mutation.Value.Length) + mutation.Value.Length);
        }

        var w = new Writer(Domain.Length + 2 + 16 + 1 +
            Codec.VarintLength((ulong)operationLength) + operationLength + 1);
        w.Raw(Domain).U16((ushort)namespaceId).B128(mutation.RecordId).U8(opKind)
            .Varint((ulong)operationLength);

        if (mutation.IsRecordTombstone)
        {
            w.U8(OpsConst.TagRecordTomb).B128(mutation.RecordId)
                .U64(mutation.HlcWall).U32(mutation.HlcLogical).B128(resolvedAuthor);
        }
        else if (mutation.IsTombstone)
        {
            w.U8(OpsConst.TagFieldTomb).U16(mutation.FieldId).U16(mutation.KeyEpoch)
                .U64(mutation.HlcWall).U32(mutation.HlcLogical).B128(resolvedAuthor);
        }
        else
        {
            w.U8(OpsConst.TagFieldWrite).U16(mutation.FieldId).U16(mutation.KeyEpoch)
                .U64(mutation.HlcWall).U32(mutation.HlcLogical).B128(resolvedAuthor)
                .Varint((ulong)mutation.Value.Length).Raw(mutation.Value);
        }

        w.Varint(0); // Provider mutations carry no causal dependency set.
        var fingerprint = Digests.Blake3Digest16(w.WrittenSpan);
        if (fingerprint.IsZero)
            throw new InvalidDataException("The reserved zero ChangeId fingerprint was produced.");
        return fingerprint;
    }

    internal static Digest16 Fingerprint(OutboxEntry entry)
    {
        if (!TryFingerprint(entry, out var fingerprint))
            throw new InvalidDataException("Cannot fingerprint an Outbox operation with a deferred field value.");
        return fingerprint;
    }

    internal static bool TryFingerprint(OutboxEntry entry, out Digest16 fingerprint)
    {
        var operation = Ops.DecodeOp(entry.Payload).Op;
        // Persistence may retain only the FieldWrite header when the value is
        // recoverable from the current field state. That is not enough to prove
        // the identity of an old relay loser, so keep its legacy marker unknown.
        if (operation is OpFieldWrite fieldWrite &&
            fieldWrite.ValueLenPos >= 0 && fieldWrite.Value.Length == 0)
        {
            fingerprint = default;
            return false;
        }

        fingerprint = LedgerFingerprint(entry.NamespaceId, entry.RecordId, entry.OpKind,
            operation, entry.AuthorNodeId, entry.OriginAuthorEpoch,
            entry.OriginSequence, entry.CausalDepsOrNull);
        return true;
    }
}
