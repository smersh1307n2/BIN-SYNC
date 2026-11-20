// Data model (§7): Record, FieldState, lifecycle (delete-wins), ghosts.
namespace BinSync;

/// <summary>Record/field model constants and lifecycle helpers.</summary>
public static class Model
{
    /// <summary>Reserved field id for record lifecycle (§7.2).</summary>
    public const ushort LifecycleFieldId = 0x0000;   // §7.2 reserved
    /// <summary>Reserved field id for mandatory field-resolve markers (§8.9).</summary>
    public const ushort FieldResolveId = 0x00FF;     // §8.9 MANUAL resolve field
    /// <summary>Minimum field id available for user-defined fields (0x0100).</summary>
    public const ushort UserFieldMin = 0x0100;
    /// <summary>Lifecycle constant: record is alive.</summary>
    public const byte Alive = 0x00;
    /// <summary>Lifecycle constant: record is tombstoned.</summary>
    public const byte Dead = 0x01;
}

/// <summary>class: class.</summary>
public sealed record class FieldState(
    byte[] Value, Hlc Timestamp, NodeId AuthorNodeId, ushort KeyEpoch = 0);

/// <summary>Per-record state: fields map, lifecycle, tombstones.</summary>
public class Record
{
    /// <summary>128-bit record identity (value type — no per-record byte[] copy of the map key).</summary>
    public RecordId RecordId { get; set; }
    /// <summary>Logical namespace this entity belongs to.</summary>
    public ushort NamespaceId { get; set; }
    /// <summary>Map of field id to field state for this record.</summary>
    public Dictionary<ushort, FieldState> State { get; set; }
    /// <summary>HLC of the last record-level lifecycle change (alive/dead).</summary>
    public Hlc? LifecycleHlc { get; set; }
    /// <summary>Author of the last record-level lifecycle change.</summary>
    public NodeId? LifecycleAuthor { get; set; }
    /// <summary>Lifecycle byte: alive or tombstoned.</summary>
    public byte LifecycleValue { get; set; } = Model.Alive;
    // Lazy: most records never carry field tombs — avoid empty Dictionary per record.
    private Dictionary<ushort, FieldState>? _fieldTombs;
    /// <summary>Map of field-id to tombstone field state for this record.</summary>
    public Dictionary<ushort, FieldState> FieldTombs
    {
        get => _fieldTombs ??= new Dictionary<ushort, FieldState>();
        set => _fieldTombs = value;
    }
    internal bool HasFieldTombs => _fieldTombs is { Count: > 0 };

    /// <summary>Per-record state: fields map, lifecycle, tombstones.</summary>
    public Record(RecordId recordId, ushort namespaceId, int fieldCapacity = 1)
    {
        RecordId = recordId;
        NamespaceId = namespaceId;
        State = new Dictionary<ushort, FieldState>(Math.Max(1, fieldCapacity));
    }

    /// <summary>True when the record is lifecycle-tombstoned.</summary>
    public bool IsDead => LifecycleValue == Model.Dead;

    /// <summary>Ghost semantics (§7.4): dead records hide fields ordered ≤ lifecycle.</summary>
    public Dictionary<ushort, FieldState> VisibleFields()
    {
        var outp = new Dictionary<ushort, FieldState>();
        foreach (var (fid, fs) in State)
        {
            if (IsDead && !IsGhost(fs, this)) outp[fid] = fs;
            else if (!IsDead) outp[fid] = fs;
        }
        return outp;
    }

    /// <summary>True when the field is overshadowed by a newer record-level tombstone.</summary>
    public static bool IsGhost(FieldState fs, Record rec)
    {
        if (rec.LifecycleHlc is null || rec.LifecycleAuthor is null) return false;
        if (!rec.IsDead) return false;
        if (fs.Timestamp.Key() != rec.LifecycleHlc.Key())
            return CompareKeys(fs.Timestamp, rec.LifecycleHlc) < 0;
        return HlcOps.CompareHlc(fs.Timestamp, fs.AuthorNodeId,
                                 rec.LifecycleHlc, rec.LifecycleAuthor) <= 0;
    }

    private static int CompareKeys(Hlc a, Hlc b)
    {
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        return 0;
    }
}
