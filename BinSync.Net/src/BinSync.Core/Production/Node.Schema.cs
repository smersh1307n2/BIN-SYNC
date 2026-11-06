// Opt-in schema gate on ApplyBatch (§18).
namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// When set, <see cref="ApplyBatchWithSchema(Batch, NodeId)"/> checks schema version before apply.
    /// Plain <see cref="ApplyBatch"/> ignores this registry.
    /// </summary>
    private SchemaRegistry? _schemas;

    /// <summary>Optional schema registry. Access is serialized with schema-gated apply operations.</summary>
    public SchemaRegistry? Schemas
    {
        get { lock (_lockMeta) return _schemas; }
        set { lock (_lockMeta) _schemas = value; }
    }

    /// <summary>
    /// Apply a batch after schema gate. On Pending → throw SCHEMA_PENDING;
    /// on Mismatch → SCHEMA_MISMATCH; on Ok → ApplyBatchInternal.
    /// </summary>
    public BatchResult ApplyBatchWithSchema(Batch batch, NodeId senderPeer)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var ownedBatch = CloneBatchForPublicRead(batch);
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var registry = _schemas;
            if (registry is null)
                result = ApplyBatchInternal(ownedBatch, senderPeer);
            else
            {
                var gate = registry.EvaluateAndApply(
                    ownedBatch.NamespaceId, ownedBatch.SchemaId, ownedBatch.SchemaVersion, ownedBatch.MergePolicy,
                    () => ApplyBatchInternal(ownedBatch, senderPeer));
                if (gate.Outcome == SchemaOutcome.Pending)
                {
                    ErrorsOut.Add($"SCHEMA_PENDING:{gate.Detail}");
                    throw new SyncException("SCHEMA_PENDING", ownedBatch.BatchId);
                }
                if (gate.Outcome != SchemaOutcome.Ok)
                {
                    ErrorsOut.Add($"SCHEMA_MISMATCH:{gate.Detail}");
                    throw new SyncException("SCHEMA_MISMATCH", ownedBatch.BatchId);
                }
                result = gate.Result ?? throw new InvalidOperationException("Schema gate returned no apply result.");
            }
        }
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return result;
    }

    /// <summary>Publish local schemas into the registry (convenience).</summary>
    public void RegisterSchema(SchemaDescriptor desc)
    {
        ArgumentNullException.ThrowIfNull(desc);
        int namespaceId = desc.NamespaceId, schemaId = desc.SchemaId, version = desc.Version;
        lock (_lockMeta)
        {
            Schemas ??= new SchemaRegistry();
            Schemas.Register(desc);
            int max = Schemas.MaxVersion(namespaceId, schemaId) ?? version;
            if (!SchemaVersions.TryGetValue(namespaceId, out var cur) || max > cur)
                SchemaVersions[namespaceId] = max;
        }
        SchemaRegistered?.Invoke(namespaceId, schemaId, version);
    }

    /// <summary>Learn schemas from a decoded SCHEMA_MAP payload.</summary>
    public int LearnSchemaMap(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        int learned;
        lock (_lockMeta)
        {
            Schemas ??= new SchemaRegistry();
            var list = SchemaMap.Decode(payload);
            learned = Schemas.Learn(list);
        }
        if (learned > 0) SchemaMapLearned?.Invoke(learned);
        return learned;
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
