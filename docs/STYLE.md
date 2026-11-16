# Code style & analyzers

## StyleCop

`StyleCop.Analyzers` is referenced from `BinSync.Core.csproj`.

**Enforced (fail build with TreatWarningsAsErrors):**
- `SA1107` — no multiple statements on one line
- other non-suppressed SA* if any

**Suppressed (intentional):**
- spacing/layout/ordering/`this.` prefix — noise vs port style
- `SA1402` multi-type files (Codec, comparers, models)
- member-level XML doc rules (`SA1600`…) while type-level docs + `PUBLIC-API.md` cover the surface
- vendored `Crypto/**` — do not restyle

Re-enable rules incrementally by removing codes from `<NoWarn>` in the csproj.

## Exceptions

| Type | Use |
|------|-----|
| `SyncException` | Protocol/sync failures (SEQ_GAP, UNKNOWN_ALIAS, …) |
| `FrameException` | Frame magic/CRC/size |
| `ArgumentException` / `ArgumentNullException` | Caller contract (bad lengths, null) |
| `InvalidOperationException` | Internal invariant / programming error (not a wire code) |

`SyncException` and `FrameException` expose standard `(code)`, `(code, message)`, `(code, message, inner)` constructors.


## Encapsulation (2.2+)

Engine collections on `Node` are **internal**. External hosts use:

- `TryGetRecord` / `TryGetField` / `GetRecordCount` / `GetOutboxCount`
- write APIs: `WriteField`, `CreateDeltaBatch`, `ApplyBatch` / `ApplyBatchPayload`
- optional `Node.Limits = ResourceLimits.Default` (or custom caps)

`EnterAllGates()` remains the supported multi-domain lock scope.
