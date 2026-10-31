# CI coverage (pending `workflow` scope)

Local change to `.github/workflows/dotnet.yml` (not pushed: token without `workflow` scope):

1. On the **Test** step, add:
   - `--collect:"XPlat Code Coverage"`
   - results already go to `${{ runner.temp }}/test-results`
2. Upload artifact `coverage` from `**/coverage.cobertura.xml`

`coverlet.collector` is already referenced in `BinSync.Tests.csproj`.

Apply when a PAT / GitHub App with **workflow** scope is available, then push the workflow diff.
