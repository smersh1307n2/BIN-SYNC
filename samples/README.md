# Samples

## TwoNodeLoopback

Minimal in-process sync of two `Node` instances over the built-in session helpers (`Transport.MakeSession` + `Transport.SyncRound`).

```bash
# from repository root (after vendoring crypto once)
bash scripts/vendor-ed25519-from-ztsharp.sh
dotnet run --project samples/TwoNodeLoopback -c Release
```

Expected output ends with `OK — two-node loopback sample passed.`
