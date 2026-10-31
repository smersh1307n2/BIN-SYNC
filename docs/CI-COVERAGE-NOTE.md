# CI coverage

Enabled on `main` (commit with XPlat Code Coverage collect + `coverage` artifact).

- Collector: `coverlet.collector` in `BinSync.Tests`
- Format: cobertura
- Artifact name: **coverage** (`**/coverage.cobertura.xml`)
- No coverage % gate yet (soft collect only)

Download from the Actions run → Artifacts → `coverage`.
