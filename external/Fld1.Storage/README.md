# Vendored Fld1.Storage source

This directory is a source snapshot copied from the authoritative FLD-DB project.

- Source repository: https://github.com/smersh1307n2/FLD-DB
- Source branch: `fix/crash-consistency-outbox`
- Pinned source commit: `0a3c3fa118b5503a7b0f8991b72d1c3c417205f7`
- Source project: `src/Fld1.Storage/Fld1.Storage.csproj`
- File-level Git blob hashes: `UPSTREAM-MANIFEST.json`

The snapshot is kept in BIN-SYNC so public CI can compile and test the cross-repository adapter without requiring checkout access to FLD-DB. FLD-DB remains the sole source of truth. Do not make independent semantic changes in this directory.

After updating the source snapshot, regenerate the manifest from the exact FLD-DB commit and run `python eng/verify-fld-storage-vendor.py`. The Storage Integration workflow runs this verification before build/test, so a stale or locally modified vendor copy fails CI.
