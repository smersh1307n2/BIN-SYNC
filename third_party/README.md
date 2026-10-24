# third_party

## ed25519-zlib/

Optional offline blobs (zlib + base64) of the patched Bouncy Castle MIT subset
used for Ed25519. Primary production path is:

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

Do not add truncated or partial `*.part*` files here.
