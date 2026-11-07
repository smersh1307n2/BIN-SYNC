// Global ChangeId identity ledger. Route/alias-independent semantic fingerprints.
namespace BinSync;

/// <summary>One durable ChangeId fingerprint; unknown entries are migrated legacy markers.</summary>
internal readonly struct ChangeLedgerValue
{
    /// <summary>
    /// Zero is reserved as the explicit legacy-unknown marker; every known semantic
    /// fingerprint must be non-zero. This keeps the value at 16 bytes instead of
    /// adding a padded Boolean to every global-ledger entry.
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
internal static class ChangeIdentity
{
    private static readonly byte[] Domain = System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/CHANGE-ID/v1");

    internal static Digest16 Fingerprint(int namespaceId, RecordId recordId, byte opKind,
        object operation, NodeId resolvedAuthor, IReadOnlyList<BatchId>? causalDeps = null)
    {
        Node.ValidateNamespaceId(namespaceId);
        // Never mutate the decoded operation: ApplyBatch still needs to know whether
        // its original payload was compact or full-author when deciding relay encoding.
        // Encode the canonical full-author operation directly into the fingerprint
        // writer. The former path cloned the decoded operation (including Value.ToArray),
        // allocated a second encoded byte[], then copied it into another Writer.
        // A single writer preserves the exact canonical bytes without those intermediates.
        int operationLength = operation switch
        {
            OpFieldWrite x => checked(17 + 16 + Codec.VarintLength((ulong)x.Value.Length) + x.Value.Length),
            OpFieldTomb => 17 + 16,
            OpRecordTombstone => 29 + 16,
            _ => throw new InvalidDataException("Unsupported operation for ChangeId fingerprint.")
        };

        var w = new Writer(Domain.Length + 2 + 16 + 1 +
            Codec.VarintLength((ulong)operationLength) + operationLength + 1);
        w.Raw(Domain).U16((ushort)namespaceId).B128(recordId).U8(opKind)
            .Varint((ulong)operationLength);

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
            var unique = new HashSet<BatchId>();
            for (int i = 0; i < causalDeps.Count; i++)
            {
                if (causalDeps[i].IsZero)
                    throw new InvalidDataException("Causal dependency ID cannot be zero.");
                unique.Add(causalDeps[i]);
            }
            var sorted = unique.OrderBy(id => id.Lo).ThenBy(id => id.Hi).ToArray();
            w.Varint((ulong)sorted.Length);
            foreach (var dependency in sorted) w.B128(dependency);
        }
        var fingerprint = Digests.Blake3Digest16(w.Value());
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

        fingerprint = Fingerprint(entry.NamespaceId, entry.RecordId, entry.OpKind,
            operation, entry.AuthorNodeId, entry.CausalDepsOrNull);
        return true;
    }
}
