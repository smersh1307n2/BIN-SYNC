# Vendored Fld1.Storage source

This directory is a source snapshot copied from the private FLD-DB repository.

- Source repository: https://github.com/smersh1307n2/FLD-DB
- Source branch: `fix/crash-consistency-outbox`
- Source commit: `dcd4334b40d33cbe42b1c4f9a646d9a5dd514301`
- Source project: `src/Fld1.Storage/Fld1.Storage.csproj`

The copy exists only so BIN-SYNC's public CI can compile and test the cross-repository adapter without requiring access to the private FLD-DB repository. The authoritative implementation remains in FLD-DB. When updating this snapshot, copy the complete `src/Fld1.Storage` project from a tested FLD-DB commit and update this pinned source commit. Do not make independent semantic changes here.
