# BYN-SYNC / BinSync.Core Licensing

BYN-SYNC is a binary offline-first sync protocol and .NET reference engine.
The codebase is distributed under several licenses, depending on the origin
of each component.

## Component overview

| Component | Location | License |
|---|---|---|
| Original BYN-SYNC / BinSync.Net code (protocol engine, wire, merge, transport hosts, tests, docs, scripts except pure third-party blobs) | `BinSync.Net/` (except vendored crypto), `docs/` (except third-party notices), `scripts/`, spec texts, root project files | **AGPL-3.0-or-later** (see commercial licensing below) |
| Vendored Ed25519 (RFC 8032) — Bouncy Castle C# subset via ZtSharp `Crypto/External` | `BinSync.Net/src/BinSync.Core/Crypto/` (produced by `scripts/vendor-ed25519-from-ztsharp.sh`), `third_party/ed25519-zlib/` | **MIT** (© The Legion of the Bouncy Castle Inc.) |
| Blake3.Managed (NuGet) | package reference in `BinSync.Core.csproj` | **Apache-2.0** (upstream package terms) |

Full AGPL text: [https://www.gnu.org/licenses/agpl-3.0.html](https://www.gnu.org/licenses/agpl-3.0.html)  
Also see `docs/CRYPTO-NOTICE.md` and `THIRD-PARTY-NOTICES.md`.

## AGPL-3.0-or-later (original BYN-SYNC code)

Copyright (c) Smersh / BYN-SYNC authors.

The original BYN-SYNC code is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the License,
or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

**Network use (AGPL §13):** if you modify this software and provide it as a
service over a network, you must offer the corresponding source to users of
that service under AGPL-3.0-or-later.

## Commercial licensing

The original BYN-SYNC / BinSync code (the AGPL-3.0-or-later parts listed
above) is also available under commercial license terms.

For commercial licensing inquiries contact: **smersh@bk.ru**

A commercial license can only cover code owned by the BYN-SYNC authors.
Third-party components (vendored Bouncy Castle Ed25519 under MIT, Blake3.Managed,
and any other upstream packages) remain under their original licenses
regardless of any commercial agreement.

## Cryptography (not original BYN-SYNC code)

Ed25519 sources under `BinSync.Net/src/BinSync.Core/Crypto/` are a minimal
subset of the Bouncy Castle C# crypto library (MIT), obtained via the same
path as ZtSharp (`ZtSharp/Crypto/External/`). They are **not** dual-licensed
by BYN-SYNC authors and are **not** covered by a commercial BYN-SYNC license.

See `docs/CRYPTO-NOTICE.md`.
