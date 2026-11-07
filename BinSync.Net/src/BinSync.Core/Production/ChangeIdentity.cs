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
        object operation, NodeId resolvedAuthor)
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
        return Digests.Blake3Digest16(w.Value());
    }

    internal static Digest16 Fingerprint(OutboxEntry entry)
    {
        var operation = Ops.DecodeOp(entry.Payload, fullAuthor: false).Op;
        return Fingerprint(entry.NamespaceId, entry.RecordId, entry.OpKind, operation, entry.AuthorNodeId);
    }
}
