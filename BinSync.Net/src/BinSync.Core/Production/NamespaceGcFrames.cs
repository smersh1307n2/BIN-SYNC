namespace BinSync;

/// <summary>v3.1 negotiated namespace-GC and authoritative FULL_REPLACE frame ids.</summary>
public static class NamespaceGcFrames
{
    public const byte NamespaceRoster = 0x14;
    public const byte OriginFrontier = 0x15;
    public const byte RetentionCheckpoint = 0x16;
    public const byte FullReplaceBegin = 0x17;
    public const byte FullReplaceChunk = 0x18;
    public const byte FullReplaceCommit = 0x19;
    public const byte FullReplaceAck = 0x1A;
    public const byte FullReplaceAbort = 0x1B;
    public const byte FullReplaceReady = 0x1C;
    public const byte MetadataBaselineReport = 0x1D;
}
