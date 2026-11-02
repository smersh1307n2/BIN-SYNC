// Data model (§7): Record, FieldState, lifecycle (delete-wins), ghosts.
namespace BinSync;

/// <summary>Represents Model.</summary>
public static class Model
{
    /// <summary>LifecycleFieldId.</summary>
    public const ushort LifecycleFieldId = 0x0000;   // §7.2 reserved
    /// <summary>FieldResolveId.</summary>
    public const ushort FieldResolveId = 0x00FF;     // §8.9 MANUAL resolve field
    /// <summary>UserFieldMin.</summary>
    public const ushort UserFieldMin = 0x0100;
    /// <summary>Alive.</summary>
    public const byte Alive = 0x00;
    /// <summary>Dead.</summary>
    public const byte Dead = 0x01;
}

/// <summary>Represents FieldState.</summary>
public sealed record class FieldState(
    byte[] Value, Hlc Timestamp, byte[] AuthorNodeId, ushort KeyEpoch = 0);

/// <summary>Represents Record.</summary>
public class Record
{
    /// <summary>Gets or sets RecordId.</summary>
    public byte[] RecordId { get; set; }
    /// <summary>Gets or sets NamespaceId.</summary>
    public ushort NamespaceId { get; set; }
    /// <summary>Gets or sets Dictionary.</summary>
    public Dictionary<ushort, FieldState> State { get; set; }
    /// <summary>Gets or sets LifecycleHlc.</summary>
    public Hlc? LifecycleHlc { get; set; }
    /// <summary>Gets or sets LifecycleAuthor.</summary>
    public byte[]? LifecycleAuthor { get; set; }
    /// <summary>Gets or sets LifecycleValue.</summary>
    public byte LifecycleValue { get; set; } = Model.Alive;
    // Lazy: most records never carry field tombs — avoid empty Dictionary per record.
    private Dictionary<ushort, FieldState>? _fieldTombs;
    /// <summary>Dictionary.</summary>
    public Dictionary<ushort, FieldState> FieldTombs
    {
        get => _fieldTombs ??= new Dictionary<ushort, FieldState>();
        set => _fieldTombs = value;
    }
    internal bool HasFieldTombs => _fieldTombs is { Count: > 0 };

    /// <summary>Record.</summary>
    public Record(byte[] recordId, ushort namespaceId, int fieldCapacity = 1)
    {
        RecordId = recordId;
        NamespaceId = namespaceId;
        State = new Dictionary<ushort, FieldState>(Math.Max(1, fieldCapacity));
    }

    /// <summary>Gets IsDead.</summary>
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

    /// [D7]/[У7] with SPEC HARDENING ghost rule (see binsync/model.py docstring):
    /// ghost status only for DEAD records; time dominates, durable author is the
    /// tie-break at exactly equal times.
    /// <summary>IsGhost.</summary>
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
