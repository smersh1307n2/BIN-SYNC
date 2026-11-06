// Schema evolution §18: registry, SCHEMA_MAP (0x04), apply gate.
// Field IDs are never reused; merge-policy changes require a version bump.
namespace BinSync;

/// <summary>One published schema version for a (namespace, schemaId).</summary>
public sealed class SchemaDescriptor
{
    /// <summary>Logical namespace this entity belongs to.</summary>
    public int NamespaceId { get; init; }
    /// <summary>Schema identifier for the namespace.</summary>
    public int SchemaId { get; init; }
    /// <summary>Format version for this on-disk or wire structure.</summary>
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

    /// <summary>Signed multiplicity of digests folded into this cell.</summary>
    public int Count
    {
        get { lock (_gate) return _map.Sum(kv => kv.Value.Count); }
    }

    internal static void ValidateDescriptorRange(SchemaDescriptor descriptor)
    {
        Node.ValidateNamespaceId(descriptor.NamespaceId);
        if ((uint)descriptor.SchemaId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(descriptor.SchemaId));
        if ((uint)descriptor.Version > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(descriptor.Version));
        if ((uint)descriptor.MergePolicy > byte.MaxValue) throw new ArgumentOutOfRangeException(nameof(descriptor.MergePolicy));
    }

    private static bool DescriptorsEqual(SchemaDescriptor left, SchemaDescriptor right) =>
        left.MergePolicy == right.MergePolicy &&
        left.FieldIds.SetEquals(right.FieldIds) &&
        left.RetiredFieldIds.SetEquals(right.RetiredFieldIds);

    private static SchemaDescriptor CloneDescriptor(SchemaDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor.FieldIds);
        ArgumentNullException.ThrowIfNull(descriptor.RetiredFieldIds);
        return new SchemaDescriptor
        {
            NamespaceId = descriptor.NamespaceId,
            SchemaId = descriptor.SchemaId,
            Version = descriptor.Version,
            MergePolicy = descriptor.MergePolicy,
            FieldIds = new HashSet<ushort>(descriptor.FieldIds),
            RetiredFieldIds = new HashSet<ushort>(descriptor.RetiredFieldIds),
        };
    }

    /// <summary>Register or replace a schema version. Enforces §18.1 field-id non-reuse across versions.</summary>
    public void Register(SchemaDescriptor desc)
    {
        ArgumentNullException.ThrowIfNull(desc);
        var owned = CloneDescriptor(desc);
        ValidateDescriptorRange(owned);
        if (owned.FieldIds.Overlaps(owned.RetiredFieldIds))
            throw new ArgumentException("a field cannot be both active and retired");
        lock (_gate)
        {
            var key = (owned.NamespaceId, owned.SchemaId);
            bool hadHistory = _map.TryGetValue(key, out var existingHistory);
            var byVer = existingHistory ?? new Dictionary<int, SchemaDescriptor>();

            // Validate the complete known version history in version order. Once a field
            // disappears from a later descriptor (or is explicitly retired), its id
            // cannot become active again in any subsequent version.
            var history = byVer.Values
                .Where(existing => existing.Version != owned.Version)
                .Append(owned)
                .OrderBy(existing => existing.Version);
            var seen = new HashSet<ushort>();
            var retired = new HashSet<ushort>();
            var closed = new HashSet<ushort>();

            foreach (var descriptor in history)
            {
                foreach (var fieldId in descriptor.FieldIds)
                {
                    if (retired.Contains(fieldId) || closed.Contains(fieldId))
                        throw new ArgumentException(
                            $"field 0x{fieldId:X4} was retired or removed and cannot be reused (§18.1)");
                }

                foreach (var fieldId in descriptor.RetiredFieldIds)
                {
                    if (descriptor.FieldIds.Contains(fieldId))
                        throw new ArgumentException(
                            $"field 0x{fieldId:X4} cannot be both active and retired");
                    retired.Add(fieldId);
                }

                foreach (var fieldId in seen)
                {
                    if (!descriptor.FieldIds.Contains(fieldId))
                        closed.Add(fieldId);
                }

                foreach (var fieldId in descriptor.FieldIds)
                    seen.Add(fieldId);
            }

            byVer[owned.Version] = owned;
            if (!hadHistory)
                _map.Add(key, byVer);
        }
    }

    /// <summary>Try to resolve a schema definition by namespace, schema id, and version.</summary>
    public bool TryGet(int namespaceId, int schemaId, int version, out SchemaDescriptor? desc)
    {
        ValidateSchemaKey(namespaceId, schemaId, version);
        lock (_gate)
        {
            desc = null;
            if (!_map.TryGetValue((namespaceId, schemaId), out var byVer)) return false;
            if (!byVer.TryGetValue(version, out var stored)) return false;
            desc = CloneDescriptor(stored);
            return true;
        }
    }

    /// <summary>Highest registered version for (ns, schemaId), or null.</summary>
    public int? MaxVersion(int namespaceId, int schemaId)
    {
        ValidateSchemaKey(namespaceId, schemaId);
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
    private static void ValidateSchemaKey(int namespaceId, int schemaId, int? version = null, int? mergePolicy = null)
    {
        Node.ValidateNamespaceId(namespaceId);
        if ((uint)schemaId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(schemaId));
        if (version.HasValue && (uint)version.Value > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(version));
        if (mergePolicy.HasValue && (uint)mergePolicy.Value > byte.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(mergePolicy));
    }

    private SchemaOutcome EvaluateLocked(int namespaceId, int schemaId, int schemaVersion, int mergePolicy, out string? detail)
    {
        ValidateSchemaKey(namespaceId, schemaId, schemaVersion, mergePolicy);
        detail = null;
        if (!_map.TryGetValue((namespaceId, schemaId), out var byVer) || byVer.Count == 0)
        {
            detail = "unknown schema";
            return SchemaOutcome.Pending;
        }
        if (!byVer.TryGetValue(schemaVersion, out var desc))
        {
            int max = byVer.Keys.Max();
            detail = schemaVersion > max
                ? $"pending version {schemaVersion} (have ≤{max})"
                : $"unknown version {schemaVersion}";
            return SchemaOutcome.Pending;
        }
        // Policy 0 is the legacy/default spelling of LWW_FIELD (wire code 0x02),
        // not a wildcard that may bypass schema compatibility checks.
        int registeredPolicy = desc.MergePolicy == 0 ? 0x02 : desc.MergePolicy;
        int incomingPolicy = mergePolicy == 0 ? 0x02 : mergePolicy;
        if (registeredPolicy != incomingPolicy)
        {
            detail = $"policy {incomingPolicy} != registered {registeredPolicy}";
            return SchemaOutcome.Mismatch;
        }
        return SchemaOutcome.Ok;
    }

    public SchemaOutcome Evaluate(int namespaceId, int schemaId, int schemaVersion, int mergePolicy, out string? detail)
    {
        lock (_gate)
            return EvaluateLocked(namespaceId, schemaId, schemaVersion, mergePolicy, out detail);
    }

    internal (SchemaOutcome Outcome, string? Detail, BatchResult? Result) EvaluateAndApply(
        int namespaceId, int schemaId, int schemaVersion, int mergePolicy, Func<BatchResult> apply)
    {
        ArgumentNullException.ThrowIfNull(apply);
        lock (_gate)
        {
            var outcome = EvaluateLocked(namespaceId, schemaId, schemaVersion, mergePolicy, out var detail);
            return (outcome, detail, outcome == SchemaOutcome.Ok ? apply() : null);
        }
    }

    /// <summary>All descriptors (for SCHEMA_MAP export).</summary>
    public List<SchemaDescriptor> Snapshot()
    {
        lock (_gate)
        {
            return _map.Values.SelectMany(v => v.Values)
                .OrderBy(d => d.NamespaceId).ThenBy(d => d.SchemaId).ThenBy(d => d.Version)
                .Select(CloneDescriptor)
                .ToList();
        }
    }

    /// <summary>Merge remote descriptors (from SCHEMA_MAP). Returns how many new versions learned.</summary>
    public int Learn(IEnumerable<SchemaDescriptor> remote)
    {
        ArgumentNullException.ThrowIfNull(remote);
        // Materialize caller-owned descriptors first, then apply a stable version order.
        // Otherwise a shuffled SCHEMA_MAP can cause a different valid prefix to be kept
        // when a later descriptor proves that an intermediate version retired a field.
        var ordered = remote.Select(CloneDescriptor)
            .OrderBy(d => d.NamespaceId)
            .ThenBy(d => d.SchemaId)
            .ThenBy(d => d.Version)
            .ToList();

        // Descriptor-local errors are malformed input, not ignorable history conflicts.
        // Validate before the catch below, which is reserved for non-reusable field history.
        foreach (var descriptor in ordered)
        {
            ValidateDescriptorRange(descriptor);
            if (descriptor.FieldIds.Overlaps(descriptor.RetiredFieldIds))
                throw new InvalidDataException("A schema field cannot be both active and retired.");
        }

        int learned = 0;
        lock (_gate)
        {
            // Preflight immutable-version conflicts before mutating the registry.
            // Also detect conflicting duplicates inside one incoming map.
            var incoming = new Dictionary<(int Ns, int SchemaId, int Version), SchemaDescriptor>();
            foreach (var descriptor in ordered)
            {
                var key = (Ns: descriptor.NamespaceId, SchemaId: descriptor.SchemaId, Version: descriptor.Version);
                if (incoming.TryGetValue(key, out var prior) &&
                    !DescriptorsEqual(prior, descriptor))
                    throw new InvalidDataException(
                        $"Conflicting schema descriptors in map for namespace {key.Ns}, schema {key.SchemaId}, version {key.Version}.");
                incoming[key] = descriptor;

                if (_map.TryGetValue((descriptor.NamespaceId, descriptor.SchemaId), out var byVersion) &&
                    byVersion.TryGetValue(descriptor.Version, out var existing) &&
                    !DescriptorsEqual(existing, descriptor))
                    throw new InvalidDataException(
                        $"Conflicting schema descriptor for namespace {descriptor.NamespaceId}, schema {descriptor.SchemaId}, version {descriptor.Version}.");
            }

            foreach (var descriptor in ordered)
            {
                if (_map.TryGetValue((descriptor.NamespaceId, descriptor.SchemaId), out var byVersion) &&
                    byVersion.ContainsKey(descriptor.Version))
                    continue;
                try
                {
                    Register(descriptor);
                    learned++;
                }
                catch (ArgumentException)
                {
                    // Conflicting retirement rules: skip the invalid later version.
                }
            }
        }
        return learned;
    }
}

/// <summary>SCHEMA_MAP frame (type 0x04) encode/decode.</summary>
public static class SchemaMap
{
    /// <summary>Wire frame type byte (e.g. DELTA_BATCH, ACK).</summary>
    public const byte FrameType = 0x04;

    /// <summary>
    /// Wire layout (LE):
    /// count varint |
    /// per entry: namespaceId u16 | schemaId u16 | version u16 | mergePolicy u8 |
    ///            fieldCount varint | fieldId u16… | retiredCount varint | retiredId u16…
    /// </summary>
    public static byte[] Encode(IReadOnlyList<SchemaDescriptor> list)
    {
        ArgumentNullException.ThrowIfNull(list);
        var keys = new HashSet<(int NamespaceId, int SchemaId, int Version)>();
        foreach (var descriptor in list)
        {
            ArgumentNullException.ThrowIfNull(descriptor);
            if (!keys.Add((descriptor.NamespaceId, descriptor.SchemaId, descriptor.Version)))
                throw new ArgumentException("Duplicate schema version in map.", nameof(list));
        }

        var w = new Writer();
        w.Varint((ulong)list.Count);
        foreach (var d in list)
        {
            ArgumentNullException.ThrowIfNull(d);
            SchemaRegistry.ValidateDescriptorRange(d);
            ArgumentNullException.ThrowIfNull(d.FieldIds);
            ArgumentNullException.ThrowIfNull(d.RetiredFieldIds);
            if (d.FieldIds.Overlaps(d.RetiredFieldIds))
                throw new ArgumentException("a field cannot be both active and retired", nameof(list));
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

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static List<SchemaDescriptor> Decode(ReadOnlySpan<byte> payload)
    {
        var r = new Reader(payload.ToArray());
        ulong count = r.Varint();
        if (count > (ulong)(payload.Length / 9))
            throw new InvalidDataException("Schema count exceeds the available payload.");
        var list = new List<SchemaDescriptor>((int)count);
        var keys = new HashSet<(int NamespaceId, int SchemaId, int Version)>();
        for (int i = 0; i < (int)count; i++)
        {
            int ns = r.U16();
            int sid = r.U16();
            int ver = r.U16();
            int policy = r.U8();
            if (!keys.Add((ns, sid, ver)))
                throw new InvalidDataException("Duplicate schema version in map.");

            ulong fieldCount = r.Varint();
            if (fieldCount > (ulong)(r.Remaining / 2))
                throw new InvalidDataException("Schema field count exceeds the available payload.");
            var fields = new HashSet<ushort>();
            ushort previousField = 0;
            for (int j = 0; j < (int)fieldCount; j++)
            {
                ushort field = r.U16();
                if ((j > 0 && field <= previousField) || !fields.Add(field))
                    throw new InvalidDataException("Schema field IDs must be strictly ordered and unique.");
                previousField = field;
            }

            ulong retiredCount = r.Varint();
            if (retiredCount > (ulong)(r.Remaining / 2))
                throw new InvalidDataException("Schema retired-field count exceeds the available payload.");
            var retired = new HashSet<ushort>();
            ushort previousRetired = 0;
            for (int j = 0; j < (int)retiredCount; j++)
            {
                ushort field = r.U16();
                if ((j > 0 && field <= previousRetired) || !retired.Add(field))
                    throw new InvalidDataException("Retired schema field IDs must be strictly ordered and unique.");
                previousRetired = field;
            }
            if (fields.Overlaps(retired))
                throw new InvalidDataException("A schema field cannot be both active and retired.");

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
        if (!r.Eof())
            throw new InvalidDataException("Schema map payload contains trailing bytes.");
        return list;
    }

    /// <summary>Build a framed packet with CRC32C header.</summary>
    public static byte[] BuildFrame(IReadOnlyList<SchemaDescriptor> list, ulong frameSeq, uint streamId = 0) =>
        FrameCodec.BuildFrame(FrameType, streamId, frameSeq, Encode(list));
}
