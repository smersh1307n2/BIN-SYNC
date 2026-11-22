# BIN-SYNC v3.1 signed control-plane golden vectors

**Status:** deterministic v3.1 reference vectors are authored. `samples/V31ControlPlaneVectors` now exercises exact payload/signature bytes, decode/verify/re-sign round-trips and negative domain/key/roster/signature/tamper cases. The sample has been added to the mandatory Core GitHub Actions workflow; exact-head execution result is pending. These vectors are conformance fixtures for the mandatory v3.1 addendum, not an authorization for physical GC.

All integers are little-endian unless stated otherwise. `B128` and NodeId are raw 16 bytes. GUIDs use RFC-4122/network byte order. Signatures are Ed25519 over UTF-8 domain string concatenated directly with canonical payload; no separator or length prefix.

## Deterministic test key and shared roster

- Test-only Ed25519 seed: `000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f`
- Public key: `03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8`
- NodeId / authority / member: `01000000000000000000000000000000`
- NamespaceId `7`, roster generation `1`, transport epoch `1`; one Active member, no retired-origin closures; previous roster digest is zero.
- NRS2 roster digest (`SHA-256(domain || payload)`): `0672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe`

The seed is a public test fixture only. Never use it for a real namespace authority or member.

## NRS2 — signed namespace roster

Domain: `BIN-SYNC/NAMESPACE-ROSTER/v2`. Payload: 123 bytes.

```text
4e5253320700010000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000001000100000000000000000000000000000003a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b80100000001000000000000
```

Signature:

```text
b27ce494fdde7f1236bd626bc02c3ced5573cecc557298093089e0f911c3f4d9f6d13ebb600918ab5fad3a8004c27fe1511b6f768c6ad62b3f9250b651640500
```

## OFR2 — member applied-origin frontier

Domain: `BIN-SYNC/APPLIED-ORIGIN-FRONTIER/v2`. Uses the NRS2 roster digest; one applied entry `(member, epoch 1, sequence 1)`, one issued-origin entry `(epoch 1, sequence 1, max HLC 100:0)`, and no unpublished drafts. Payload: 151 bytes.

```text
4f465232070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe010000000000000000000000000000000100000001000000000000000000000000000000000000000000000000000000000100010000000000000000000000000000000100000001000000000000000100010000000100000000000000640000000000000000000000
```

Signature:

```text
8436fa07da6cddd101a7624abbedec43484029df97bedf813d6eb1c6dbb0e33d381786ac6a22918f977c2fb86a9d80031d82386fbeae459876b981dc938fd805
```

## RCP2 — authority retention checkpoint

Domain: `BIN-SYNC/RETENTION-CHECKPOINT/v2`. Uses the NRS2 roster digest, checkpoint sequence 1, zero previous-checkpoint digest, one safe frontier `(member, epoch 1, sequence 1)`, FULL_REPLACE generation `00112233-4455-6677-8899-aabbccddeeff`, digest `22` repeated 32 times, HLC floor `100:2`. Payload: 177 bytes.

```text
52435032070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe010000000000000000000000000000000000000000000000000000000000000000000000000000000100010000000000000000000000000000000100000001000000000000000100112233445566778899aabbccddeeff2222222222222222222222222222222222222222222222222222222222222222640000000000000002000000
```

Signature:

```text
5a44c4244802d49e440a39b1221456b5ab89b0290240cc017ac5692029d4702318735c49327879c90527514eedd0b6d23c0d86ed278d0d88188ed4e8c7bb5107
```

## FRA2 — installed FULL_REPLACE acknowledgement

Domain: `BIN-SYNC/FULL-REPLACE-INSTALLED-ACK/v2`. Uses the same roster digest, generation, FULL_REPLACE digest and HLC floor as RCP2; member transport epoch 1; Installed=true. Payload: 127 bytes.

```text
46524132070001000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe00112233445566778899aabbccddeeff2222222222222222222222222222222222222222222222222222222222222222010000000000000000000000000000000100000064000000000000000200000001
```

Signature:

```text
a069973a84f277ea298c7e07cf8fafda8305f50dd9bc0034cf32a41aba23a7775c7fd55af4487a41b2166d5cde30a35a93dc506a6ade9b85bb27499d97585b0e
```

## FULL_REPLACE_BEGIN — signed manifest

Domain: `BIN-SYNC/FULL-REPLACE-MANIFEST/v1`. Uses namespace 7, the same generation/roster digest, source feed watermark 5, expected entry count 1 and expected digest `33` repeated 32 times. Payload: 102 bytes.

```text
070000112233445566778899aabbccddeeff01000000000000000672402f6f22bbf4cdf6a059c0fb2ae1ebb5893c0eb617f3cfeb1dd275fdb6fe0500000000000000010000003333333333333333333333333333333333333333333333333333333333333333
```

Signature:

```text
c1aa9215a61eeb2b5ef8abd371ed23b1fda68fda1a171b5918d3e5f00e1f624133d885e8482a2e15ae511e3f52a87e827d3129d838d7a106b7e977f998021f0a
```

## Required T-pass cross-check

Run `dotnet run --project samples/V31ControlPlaneVectors/V31ControlPlaneVectors.csproj -c Release` on the exact release candidate and make it a required CI sample. A successful manual source review is not a substitute for the executable result.

- Instantiate the exact objects and compare `EncodePayload` byte-for-byte.
- Compare `Sign` / `SignBegin` output byte-for-byte using the test-only seed; verify each signature with the pinned public key and correct roster context.
- Decode every signed payload, run `DecodeAndVerify` / `VerifyBegin`, and assert every field and byte array.
- Add negative vectors for wrong domain, one-bit payload mutation, wrong roster generation/digest, wrong member key, truncated signature and appended trailing bytes.
- These are signed-payload vectors, not complete 25-byte frame-envelope vectors; frame envelope vectors remain separate.