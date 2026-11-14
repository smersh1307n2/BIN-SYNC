namespace BinSync;

/// <summary>Canonical BIN-SYNC wire error codes (§12.23).</summary>
public enum SyncErrorCode : ushort
{
    /// <summary>Unrecognized or missing symbolic code.</summary>
    Unknown = 0,
    UnknownAlias = 0x0001,
    EpochRegression = 0x0002,
    ClockDrift = 0x0003,
    InvalidVarint = 0x0004,
    InvalidFloat = 0x0005,
    SchemaMismatch = 0x0006,
    SeqGap = 0x0007,
    DuplicateBatch = 0x0008,
    NamespaceForbidden = 0x0009,
    Internal = 0x000A,
    OutdatedWatermark = 0x000B,
    DecompressionLimitExceeded = 0x000C,
    BatchTransactionFailed = 0x000D,
    PayloadTooLarge = 0x000E,
    FlowControlViolation = 0x000F,
    UnknownKeyEpoch = 0x0010,
    SchemaPending = 0x0011,
    CausalDepMissing = 0x0012,
    SignatureInvalid = 0x0013,
    TrustRevoked = 0x0014,
    CorruptedFrame = 0x0015,
    FrameRejected = 0x0016,
    BatchChainBroken = 0x0017,
    AckOutOfRange = 0x0018,
    SnapshotRequired = 0x0019,
}

/// <summary>Helpers mapping symbolic names ↔ <see cref="SyncErrorCode"/>.</summary>
public static class SyncErrorCodes
{
    private static readonly Dictionary<string, SyncErrorCode> ByName =
        new(StringComparer.Ordinal)
        {
            ["UNKNOWN_ALIAS"] = SyncErrorCode.UnknownAlias,
            ["EPOCH_REGRESSION"] = SyncErrorCode.EpochRegression,
            ["CLOCK_DRIFT"] = SyncErrorCode.ClockDrift,
            ["INVALID_VARINT"] = SyncErrorCode.InvalidVarint,
            ["INVALID_FLOAT"] = SyncErrorCode.InvalidFloat,
            ["SCHEMA_MISMATCH"] = SyncErrorCode.SchemaMismatch,
            ["SEQ_GAP"] = SyncErrorCode.SeqGap,
            ["DUPLICATE_BATCH"] = SyncErrorCode.DuplicateBatch,
            ["NAMESPACE_FORBIDDEN"] = SyncErrorCode.NamespaceForbidden,
            ["INTERNAL"] = SyncErrorCode.Internal,
            ["OUTDATED_WATERMARK"] = SyncErrorCode.OutdatedWatermark,
            ["DECOMPRESSION_LIMIT_EXCEEDED"] = SyncErrorCode.DecompressionLimitExceeded,
            ["BATCH_TRANSACTION_FAILED"] = SyncErrorCode.BatchTransactionFailed,
            ["PAYLOAD_TOO_LARGE"] = SyncErrorCode.PayloadTooLarge,
            ["FLOW_CONTROL_VIOLATION"] = SyncErrorCode.FlowControlViolation,
            ["UNKNOWN_KEY_EPOCH"] = SyncErrorCode.UnknownKeyEpoch,
            ["SCHEMA_PENDING"] = SyncErrorCode.SchemaPending,
            ["CAUSAL_DEP_MISSING"] = SyncErrorCode.CausalDepMissing,
            ["SIGNATURE_INVALID"] = SyncErrorCode.SignatureInvalid,
            ["TRUST_REVOKED"] = SyncErrorCode.TrustRevoked,
            ["CORRUPTED_FRAME"] = SyncErrorCode.CorruptedFrame,
            ["FRAME_REJECTED"] = SyncErrorCode.FrameRejected,
            ["BATCH_CHAIN_BROKEN"] = SyncErrorCode.BatchChainBroken,
            ["ACK_OUT_OF_RANGE"] = SyncErrorCode.AckOutOfRange,
            ["SNAPSHOT_REQUIRED"] = SyncErrorCode.SnapshotRequired,
        };

    private static readonly Dictionary<SyncErrorCode, string> ToName = ByName
        .ToDictionary(kv => kv.Value, kv => kv.Key);

    /// <summary>Parse a symbolic code name; unknown names map to <see cref="SyncErrorCode.Unknown"/>.</summary>
    public static SyncErrorCode Parse(string? codeName) =>
        codeName is not null && ByName.TryGetValue(codeName, out var c) ? c : SyncErrorCode.Unknown;

    /// <summary>Wire/symbolic name for a known code; null if unknown.</summary>
    public static string? GetName(SyncErrorCode code) =>
        ToName.TryGetValue(code, out var n) ? n : null;
}
