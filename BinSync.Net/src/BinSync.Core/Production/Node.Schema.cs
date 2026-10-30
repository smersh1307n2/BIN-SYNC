// Opt-in schema gate on ApplyBatch (§18).
namespace BinSync;

public sealed partial class Node
{
    /// <summary>
    /// When set, <see cref="ApplyBatchWithSchema"/> checks schema version before apply.
    /// Plain <see cref="ApplyBatch"/> ignores this registry.
    /// </summary>
    public SchemaRegistry? Schemas { get; set; }

    /// <summary>
    /// Apply a batch after schema gate. On Pending → throw SCHEMA_PENDING;
    /// on Mismatch → SCHEMA_MISMATCH; on Ok → ApplyBatchInternal.
    /// </summary>
    public BatchResult ApplyBatchWithSchema(Batch batch, byte[] senderPeer)
    {
        lock (_lockMeta)
        {
            var reg = Schemas;
            if (reg is not null)
            {
                var outcome = reg.Evaluate(
                    batch.NamespaceId, batch.SchemaId, batch.SchemaVersion, batch.MergePolicy,
                    out var detail);
                if (outcome == SchemaOutcome.Pending)
                {
                    ErrorsOut.Add($"SCHEMA_PENDING:{detail}");
                    throw new SyncException("SCHEMA_PENDING", batch.BatchId);
                }
                if (outcome == SchemaOutcome.Mismatch || outcome == SchemaOutcome.FieldReuse)
                {
                    ErrorsOut.Add($"SCHEMA_MISMATCH:{detail}");
                    throw new SyncException("SCHEMA_MISMATCH", batch.BatchId);
                }
            }
            return ApplyBatchInternal(batch, senderPeer);
        }
    }

    /// <summary>Publish local schemas into the registry (convenience).</summary>
    public void RegisterSchema(SchemaDescriptor desc)
    {
        lock (_lockMeta)
        {
            Schemas ??= new SchemaRegistry();
            Schemas.Register(desc);
            // Keep legacy SchemaVersions map in sync (max version per namespace)
            int max = Schemas.MaxVersion(desc.NamespaceId, desc.SchemaId) ?? desc.Version;
            if (!SchemaVersions.TryGetValue(desc.NamespaceId, out var cur) || max > cur)
                SchemaVersions[desc.NamespaceId] = max;
        }
    }

    /// <summary>Learn schemas from a decoded SCHEMA_MAP payload.</summary>
    public int LearnSchemaMap(byte[] payload)
    {
        lock (_lockMeta)
        {
            Schemas ??= new SchemaRegistry();
            var list = SchemaMap.Decode(payload);
            return Schemas.Learn(list);
        }
    }

    /// <summary>Encode current registry as SCHEMA_MAP payload (empty if none).</summary>
    public byte[] ExportSchemaMap()
    {
        lock (_lockMeta)
        {
            var list = Schemas?.Snapshot() ?? new List<SchemaDescriptor>();
            return SchemaMap.Encode(list);
        }
    }
}
