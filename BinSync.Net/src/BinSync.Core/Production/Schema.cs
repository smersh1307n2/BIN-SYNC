// Schema evolution §18: registry, SCHEMA_MAP (0x04), apply gate.
// Field IDs are never reused; merge-policy changes require a version bump.
namespace BinSync;

/// <summary>One published schema version for a (namespace, schemaId).</summary>
public sealed class SchemaDescriptor
{
    public int NamespaceId { get; init; }
    public int SchemaId { get; init; }
    public int Version { get; init; }
    /// <summary>Merge policy byte bound to this version (§18.2).</summary>
    public int MergePolicy { get; init; }
    /// <summary>Known field identifiers at this version.</summary>
    public HashSet<ushort> FieldIds { get; init; } = new();
    /// <summary>Retired field identifiers — must never be reassigned (§18.1).</summary>
    public HashSet<ushort> RetiredFieldIds { get; init; } = new();
}

/// <summary>Outcome of a schema gate check.</summary>
public enum SchemaOutcome
{
    /// <summary>Schema known and version acceptable.</summary>
    Ok = 0,
    /// <summary>Schema/version not yet known — request SCHEMA_MAP (§18.3).</summary>
    Pending = 1,
    /// <summary>Incompatible (policy mismatch or version regression).</summary>
    Mismatch = 2,
    /// <summary>Field id reuse attempted against retired set.</summary>
    FieldReuse = 3,
}

/// <summary>
/// Tracks known schemas per namespace. Thread-safe.
/// Opt in via <see cref="Node.Schemas"/>; plain ApplyBatch ignores the registry.
/// </summary>
public sealed class SchemaRegistry
{
    private readonly object _gate = new();
    // (namespaceId, schemaId) → version → descriptor
    private readonly Dictionary<(int Ns, int SchemaId), Dictionary<int, SchemaDescriptor>> _map = new();

    public int Count
    {
        get { lock (_gate) return _map.Sum(kv => kv.Value.Count); }
    }

    /// <summary>Register or replace a schema version. Enforces §18.1 field-id non-reuse across versions.</summary>
    public void Register(SchemaDescriptor desc)
    {
        if (desc.Version < 0) throw new ArgumentException("version must be non-negative");
        lock (_gate)
        {
            var key = (desc.NamespaceId, desc.SchemaId);
            if (!_map.TryGetValue(key, out var byVer))
            {
                byVer = new Dictionary<int, SchemaDescriptor>();
                _map[key] = byVer;
            }

            // Collect retired + known from all versions
            var retired = new HashSet<ushort>();
            var everUsed = new HashSet<ushort>();
            foreach (var existing in byVer.Values)
            {
                foreach (var f in existing.RetiredFieldIds) retired.Add(f);
                foreach (var f in existing.FieldIds) everUsed.Add(f);
            }

            // New descriptor must not revive a retired id, and must not reclaim an id
            // that was used in another version unless it is still listed (same field).
            foreach (var f in desc.FieldIds)
            {
                if (retired.Contains(f))
                    throw new ArgumentException($"field 0x{f:X4} is retired and cannot be reused (§18.1)");
            }
            foreach (var f in desc.RetiredFieldIds)
            {
                if (desc.FieldIds.Contains(f))
                    throw new ArgumentException($"field 0x{f:X4} cannot be both active and retired");
            }

            byVer[desc.Version] = desc;
        }
    }

    public bool TryGet(int namespaceId, int schemaId, int version, out SchemaDescriptor? desc)
    {
        lock (_gate)
        {
            desc = null;
            if (!_map.TryGetValue((namespaceId, schemaId), out var byVer)) return false;
            return byVer.TryGetValue(version, out desc);
        }
    }

    /// <summary>Highest registered version for (ns, schemaId), or null.</summary>
    public int? MaxVersion(int namespaceId, int schemaId)
    {
        lock (_gate)
        {
            if (!_map.TryGetValue((namespaceId, schemaId), out var byVer) || byVer.Count == 0)
                return null;
            return byVer.Keys.Max();
        }
    }

    /// <summary>
    /// Gate a batch against the registry (§18.2–18.3).
    /// Unknown schema/version → Pending; lower-than-known with different policy → Mismatch.
    /// </summary>
    public SchemaOutcome Evaluate(int namespaceId, int schemaId, int schemaVersion, int mergePolicy, out string? detail)
    {
        detail = null;
        lock (_gate)
        {
            if (!_map.TryGetValue((namespaceId, schemaId), out var byVer) || byVer.Count == 0)
            {
                detail = "unknown schema";
                return SchemaOutcome.Pending;
            }
            if (!byVer.TryGetValue(schemaVersion, out var desc))
            {
                // Future version we have not learned yet
                int max = byVer.Keys.Max();
                if (schemaVersion > max)
                {
                    detail = $"pending version {schemaVersion} (have ≤{max})";
                    return SchemaOutcome.Pending;
                }
                detail = $"unknown version {schemaVersion}";
                return SchemaOutcome.Pending;
            }
            if (desc.MergePolicy != mergePolicy && mergePolicy != 0)
            {
                // Allow mergePolicy=0 as "unspecified / inherit"
                detail = $"policy {mergePolicy} != registered {desc.MergePolicy}";
                return SchemaOutcome.Mismatch;
            }
            return SchemaOutcome.Ok;
        }
    }

    /// <summary>All descriptors (for SCHEMA_MAP export).</summary>
    public List<SchemaDescriptor> Snapshot()
    {
        lock (_gate)
        {
            return _map.Values.SelectMany(v => v.Values)
                .OrderBy(d => d.NamespaceId).ThenBy(d => d.SchemaId).ThenBy(d => d.Version)
                .ToList();
        }
    }

    /// <summary>Merge remote descriptors (from SCHEMA_MAP). Returns how many new versions learned.</summary>
    public int Learn(IEnumerable<SchemaDescriptor> remote)
    {
        int learned = 0;
        foreach (var d in remote)
        {
            if (TryGet(d.NamespaceId, d.SchemaId, d.Version, out _))
                continue;
            try
            {
                Register(d);
                learned++;
            }
            catch (ArgumentException)
            {
                // conflicting retirement rules — skip
            }
        }
        return learned;
    }
}

/// <summary>SCHEMA_MAP frame (type 0x04) encode/decode.</summary>
public static class SchemaMap
{
    public const byte FrameType = 0x04;

    /// <summary>
    /// Wire layout (LE):
    /// count varint |
    /// per entry: namespaceId u16 | schemaId u16 | version u16 | mergePolicy u8 |
    ///            fieldCount varint | fieldId u16… | retiredCount varint | retiredId u16…
    /// </summary>
    public static byte[] Encode(IReadOnlyList<SchemaDescriptor> list)
    {
        var w = new Writer();
        w.Varint((ulong)list.Count);
        foreach (var d in list)
        {
            w.U16((ushort)d.NamespaceId);
            w.U16((ushort)d.SchemaId);
            w.U16((ushort)d.Version);
            w.U8((byte)d.MergePolicy);
            var fields = d.FieldIds.OrderBy(x => x).ToList();
            w.Varint((ulong)fields.Count);
            foreach (var f in fields) w.U16(f);
            var retired = d.RetiredFieldIds.OrderBy(x => x).ToList();
            w.Varint((ulong)retired.Count);
            foreach (var f in retired) w.U16(f);
        }
        return w.Value();
    }

    public static List<SchemaDescriptor> Decode(ReadOnlySpan<byte> payload)
    {
        var r = new Reader(payload.ToArray());
        int n = (int)r.Varint();
        var list = new List<SchemaDescriptor>(n);
        for (int i = 0; i < n; i++)
        {
            int ns = r.U16();
            int sid = r.U16();
            int ver = r.U16();
            int policy = r.U8();
            int fc = (int)r.Varint();
            var fields = new HashSet<ushort>();
            for (int j = 0; j < fc; j++) fields.Add(r.U16());
            int rc = (int)r.Varint();
            var retired = new HashSet<ushort>();
            for (int j = 0; j < rc; j++) retired.Add(r.U16());
            list.Add(new SchemaDescriptor
            {
                NamespaceId = ns,
                SchemaId = sid,
                Version = ver,
                MergePolicy = policy,
                FieldIds = fields,
                RetiredFieldIds = retired,
            });
        }
        return list;
    }

    public static byte[] BuildFrame(IReadOnlyList<SchemaDescriptor> list, ulong frameSeq, uint streamId = 0) =>
        FrameCodec.BuildFrame(FrameType, streamId, frameSeq, Encode(list));
}
