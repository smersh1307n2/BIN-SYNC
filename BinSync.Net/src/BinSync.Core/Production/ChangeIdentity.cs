// Global ChangeId identity ledger. Route/alias-independent semantic fingerprints.
namespace BinSync;

/// <summary>One durable ChangeId fingerprint; unknown entries are migrated legacy markers.</summary>
internal readonly struct ChangeLedgerValue
{
    public Digest16 Fingerprint { get; }
    public bool IsKnown { get; }

    public ChangeLedgerValue(Digest16 fingerprint, bool isKnown)
    {
        Fingerprint = fingerprint;
        IsKnown = isKnown;
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
        object canonical = operation switch
        {
            OpFieldWrite x => new OpFieldWrite
            {
                FieldId = x.FieldId, KeyEpoch = x.KeyEpoch, Wall = x.Wall,
                Logical = x.Logical, AuthorNodeId = resolvedAuthor,
                FullAuthorMode = true, Value = x.Value.ToArray()
            },
            OpFieldTomb x => new OpFieldTomb
            {
                FieldId = x.FieldId, KeyEpoch = x.KeyEpoch, Wall = x.Wall,
                Logical = x.Logical, AuthorNodeId = resolvedAuthor, FullAuthorMode = true
            },
            OpRecordTombstone x => new OpRecordTombstone
            {
                RecordId = x.RecordId, Wall = x.Wall, Logical = x.Logical,
                AuthorNodeId = resolvedAuthor, FullAuthorMode = true
            },
            _ => throw new InvalidDataException("Unsupported operation for ChangeId fingerprint.")
        };

        var canonicalOperation = Ops.EncodeOp(canonical, fullAuthor: true);
        var w = new Writer(Domain.Length + 2 + 16 + 1 + 5 + canonicalOperation.Length);
        w.Raw(Domain).U16((ushort)namespaceId).B128(recordId).U8(opKind)
            .Varint((ulong)canonicalOperation.Length).Raw(canonicalOperation);

        // Causal dependencies are semantically relevant when ApplyBatchCausal is
        // enabled. Their order and duplicate repetitions are not, so normalize as
        // a sorted set while keeping the empty/common path allocation-free.
        if (causalDeps is null || causalDeps.Count == 0)
            w.Varint(0);
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
        return Digests.Blake3Digest16(w.Value());
    }

    internal static Digest16 Fingerprint(OutboxEntry entry)
    {
        var operation = Ops.DecodeOp(entry.Payload).Op;
        return Fingerprint(entry.NamespaceId, entry.RecordId, entry.OpKind, operation,
            entry.AuthorNodeId, entry.CausalDepsOrNull);
    }
}
