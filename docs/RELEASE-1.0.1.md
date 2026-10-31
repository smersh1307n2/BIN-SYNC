# BinSync.Core 1.0.1

**Date:** 2026-10-31  
**Tag:** `v1.0.1`  
**Package:** `BinSync.Core` `1.0.1` · **TFM:** `net10.0`

Patch over **1.0.0**. Public API unchanged ([PUBLIC-API.md](PUBLIC-API.md)).

## Highlights
- Engine structure: Node partials, comparer cleanup, format hygiene
- ApplyBatch internal COW + single decode pass (wire-compatible)
- CI coverage artifact; sample `samples/TwoNodeLoopback`
- **274** tests green

## Install
```bash
dotnet add package BinSync.Core --version 1.0.1
```
(when published to nuget.org)

## Upgrade from 1.0.0
Binary/wire compatible. Rebuild against 1.0.1; no source changes required for public API consumers.
