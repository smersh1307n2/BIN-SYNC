# CI coverage

Enabled in the current branch CI (`.github/workflows/dotnet.yml`).

- Collector: `coverlet.collector` in `BinSync.Tests`
- Format: cobertura
- Artifact name: **coverage** (`**/coverage.cobertura.xml`)
- Required line-coverage gate: **≥50%** (Cobertura `line-rate`).

Download from the Actions run → Artifacts → `coverage`.
