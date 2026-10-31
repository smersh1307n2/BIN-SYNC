# BinSync.Core 2.0.0

**Date:** 2026-10-31 · **Tag:** `v2.0.0` · **TFM:** `net10.0`

Major **API-shape** release. **Wire-compatible** with 1.0.x.

## Why major
Public fields → properties; typed `RecordId` / `NodeId` on primary write/apply APIs; `BatchId` struct.

## Migration
Rebuild. `byte[]` still converts implicitly to `NodeId`/`RecordId`. Reflection on public *fields* must switch to properties.

See [PUBLIC-API.md](PUBLIC-API.md) and [CHANGELOG.md](CHANGELOG.md).
