# BIN-SYNC v3 durable metadata byte audit

**Status:** source-derived layout inventory; not a benchmark and not a complete cross-store total.  
**Scope:** `BinSync.Storage.StorageMetadataStore` and `StorageRelayProvenanceMap`.  
**Rule:** fixed key/value byte counts below are encoded payload sizes. Storage engine page headers, key-index entries, allocator slack, WAL records, filesystem block rounding and CLR object overhead are excluded unless stated.

## 1. Partition key framing

`StorageMetadataStore.PartitionRoot` is 49 bytes:

- root kind: 1 byte (`0x70`);
- SHA-256(UTF-8 ScopeId): 32 bytes;
- LocalReplica NodeId: 16 bytes.

Every metadata row key is `root[49] + kind[1] + suffix`. Therefore a row's fixed key cost is 50 bytes plus the suffix shown below. Partition identity is stored once per partition, not repeated in each row.

Partition identity row:
- key: 50 bytes;
- value: 8 + UTF-8 ScopeId byte length (magic u32 + scope length i32 + bytes).

## 2. StorageMetadataStore row inventory

| Kind | Row | Key bytes | Value bytes | Cardinality / ownership |
|---:|---|---:|---:|---|
| 0 | Partition identity | 50 | 8 + scope bytes | Exactly 1 per persisted partition |
| 1 | Inbox receipt | 88 | Canonical 96; older accepted encodings 76/80/92 | One per retained (sender, transport epoch, namespace, BatchId) |
| 2 | Receive cursor | 88 | 8 | One per (sender, sender epoch, namespace, author stream) |
| 3 | Global ChangeId ledger | 68 | 45 | One per retained (namespace, ChangeId), shared across all peers |
| 5 | Relay ACK + high watermark | 68 | 16 | One per (namespace, original author); canonical combined row only |
| 8 | Outbound ACK + high watermark | 68 | 16 | One per (namespace, local author); canonical combined row only |
| 10 | Outbound lease | 68 | 24 | At most one active lease per (namespace, local author) |
| 12 | PendingApply | 66 | Dynamic | One per unresolved apply token; removed on completion |
| 13 | CompletedApply receipt | 66 | Canonical 85; older accepted encoding 69 | One per completed apply token while retained |
| 14 | Receive digest-chain tail | 72 | 16 | One per (sender, sender epoch, namespace); retained as replay fence |
| 15 | Peer/author stream state | 88 | 46 + (pending batch ? 80 + pending payload bytes : 0) | One per (peer, peer epoch, namespace, author stream) |
| 16 | Required peer stream | 88 | 0 | One per required (peer, epoch, namespace, author stream) |
| 17 | Interested peer | 72 | 0 | One per (peer, epoch, namespace) |
| 18 | Retired peer epoch fence | 68 | 4 | One per (namespace, peer) |
| 19 | Completed peer baseline | 72 | 0 | One per (peer, epoch, namespace) |
| 20 | Relay payload | 76 | 20 + payload bytes | One per retained (namespace, author, OutboxSeq); shared across all destination peers |
| 21 | Outbound payload | 76 | 20 + payload bytes | One per retained local (namespace, author, OutboxSeq) |
| 22 | Issued-origin watermark | 72 | 20 | One per (namespace, author, author epoch) |

The 45-byte ChangeId ledger value is: fingerprint[16] + author NodeId[16] + author epoch[4] + OriginSequence[8] + local-outbound flag[1]. The 20-byte payload-row envelope is ChangeId[16] + payload length[4]. Immutable origin is deliberately not duplicated in relay/outbound payload rows; it is hydrated from the global ChangeId ledger.

The 16-byte stream watermark is acknowledged sequence[8] + high watermark[8]. Decoder-only split-row kinds 6/9 and 8-byte ACK/high-watermark values are rejected; current writers emit only the canonical combined kind 5/8 rows. The 24-byte lease value is generation[8] + consumer Guid[16].

The 46-byte peer stream base value is version[1] + ACK cursor[8] + pushed cursor[8] + stream offset[8] + issuer epoch[4] + last digest[16] + pending-present flag[1]. A pending batch adds 80 fixed bytes plus its encoded payload.

### PendingApply dynamic value formula

The v3 PendingApply encoder has a fixed 140-byte envelope plus UTF-8 scope bytes, wire payload bytes, operation records and optional header fingerprint:

`140 + scopeBytes + wirePayloadBytes + Σ(65 + operationPayloadBytes) + (headerFingerprintPresent ? 16 : 0)`.

Each operation's 65-byte fixed part is author[16] + ChangeId[16] + author epoch[4] + OriginSequence[8] + fingerprint[16] + relayEligible[1] + payload length[4]. The encoder also writes the operation count and both receive-state/header-fingerprint booleans; these are included in the 140-byte fixed envelope. PendingApply is temporary intent, not a second long-lived payload journal.

## 3. Relay provenance mapping primitive

`StorageRelayProvenanceMap` uses a separate root kind (`0x6D`) and the same 49-byte root framing (scope hash + local replica). It does not collide with MetadataStore's `0x70` root.

| Row | Key bytes | Value bytes | Cardinality |
|---|---:|---:|---|
| Per-author scan/ACK cursor | 68 | 60 | One per (namespace, original author) that has been scanned |
| JournalSeq/OutboxSeq mapping (RPM3) | 76 | 8 | One per unacknowledged relayed ChangeId |

The mapping key is root[49] + kind[1] + namespace[2] + author[16] + big-endian OutboxSeq[8]. The RPM3 value is JournalSeq[8] only; each persisted mapping row is therefore 84 bytes total. ChangeId remains solely in BIN-SYNC's global ChangeId ledger and is resolved before AppendPage. Historical RPM2 rows with a 24-byte value are superseded and are not a supported fallback. The cursor value is marker[4] (RPM3) + scanned JournalSeq[8] + ACKed OutboxSeq[8] + ACKed JournalSeq[8] + mapping-set digest[32]. The digest is XOR(SHA-256(key || value)) over all retained mapping rows; append/prune updates rows and digest in one atomic Storage batch, and restart recomputes it from bounded pages.

The RPM3 mapping index is intentionally proportional to unacknowledged relay mappings. It is not a per-peer copy and contains no operation payload, fingerprint, receipt or OriginSequence. ACK translation is lower-bound O(log N); restart reads mapping rows in OutboxSeq order through 1,024-entry pages. Mapping rows are deleted only after FLD durably accepts the translated JournalSeq ACK.

## 4. Cardinality model and non-duplication invariants

For a namespace with:
- (R) retained inbox receipts;
- (C) retained global ChangeIds;
- (L) retained relay payloads;
- (O) retained outbound payloads;
- (P) peer/author streams;
- (Q) required peer streams;
- (M) retained member records;
- (A) unresolved PendingApply records;
- (X) unacknowledged synchronized-FLD relay mappings;

the fixed row overhead is linear in these row counts. Payload bytes are counted once in the relevant relay/outbound row; the same operation is not copied per destination peer. ChangeId provenance is counted once in the global ledger. Per-peer progress is stream-granular, not a per-operation ACK bitmap.

No total byte estimate is asserted without the actual cardinalities, payload-size distribution, storage-engine page/index overhead and WAL amplification. The C-pass does not enable physical receipt, ChangeId or tombstone/Ghost GC.

## 5. Remaining cross-store measurement

Still required in O/T:
- actual row cardinalities and average/max payload distributions from representative FLD workloads;
- Fld1.Storage key-index/page overhead and write amplification;
- FLD Outbox and ApplyJournal row bytes, including unsealed WAL/checkpoint behavior;
- cold-start mapping index memory, hot append/translation allocations and mapping retention horizon;
- before/after metadata footprint under multiple peers, multiple authors and sparse ACK progress.

Do not add another pending journal or per-source/per-peer duplicate payload table to improve the apparent accounting. Preserve the contract: WAL is recovery, FLD Outbox is unpublished local intent, BIN-SYNC is published-operation identity and delivery progress.


## 6. FLD synchronized-database Outbox (cross-repository source audit)

Source: FLD-DB `src/Fld1.Engine/Outbox/OutboxLog.cs` on `fix/crash-consistency-outbox`.

- Per-namespace Outbox file header: 16 bytes.
- Each Outbox record: fixed part 88 bytes + value payload bytes + CRC32C 4 bytes = **92 + valueBytes**.
- Fixed 88-byte record fields: OutboxSeq[8] + CommitSeq[8] + ChangeId[16] + RecordId[16] + FieldId[2] + OpKind[1] + reserved[1] + AuthorNodeId[16] + HLC wall[8] + HLC logical[4] + KeyEpoch[2] + reserved[2] + value length[4].
- Sidecars per namespace: ACK 12 bytes; materialized-state watermark 20 bytes; consumer lease 36 bytes.
- In-memory sparse index stride: one (OutboxSeq[8], file offset[8]) point per 64 records, excluding collection/runtime overhead.

The FLD Outbox is the unpublished local-intent source and already carries ChangeId, RecordId, author and HLC with the payload. The BIN-SYNC provenance map adds only the 84-byte key/value row per unacknowledged relay identity plus one 60-byte per-author cursor; it must not copy the payload or replace the Outbox. FLD's Outbox compaction still depends on the WAL seal/checkpoint horizon and is independent of BIN-SYNC mapping pruning.

**Not included:** ApplyJournal record sizes, WAL record/segment framing, manifest/run overhead and Fld1.Storage page/index/write amplification. Those require a source-complete audit of the corresponding FLD formats; do not infer them from Outbox sizes.


## 7. FLD ApplyJournal (cross-repository source audit)

Source: FLD-DB `src/Fld1.Engine/Outbox/ApplyJournal.cs` on `fix/crash-consistency-outbox`.

- ApplyJournal file header: 16 bytes (`FLDA` magic + version/namespace/header fields).
- Each record: 20-byte record header + encoded APPLY_COMMIT payload + 4-byte CRC32C = **24 + payloadBytes**. Records are not 8-byte aligned.
- The 20-byte header stores JournalSeq[8] + WAL CommitSeq[8] + payload length[4].
- Relay-ACK sidecar: 12-byte header + 24 bytes per author (AuthorNodeId[16] + safe JournalSeq[8]) + 4-byte CRC32C = **16 + 24 × authorCount**.
- In-memory sparse index: one 16-byte (JournalSeq[8], file offset[8]) anchor per 256 records. Batch identity lookup is an in-memory dictionary and is not a persisted per-peer row.

### APPLY_COMMIT v3 payload formula

The v3 body has a 139-byte fixed envelope, then:
- ProtocolStateDelta: 4-byte length + delta bytes;
- field list: 4-byte count + Σ(69-byte fixed mutation header + value bytes);
- for non-relay-only WAL records: 4-byte relay-index count + varuint32 index per relay field.

The 69-byte mutation header is RecordId[16] + FieldId[2] + OpKind[1] + KeyEpoch[2] + HLC wall[8] + HLC logical[4] + AuthorNodeId[16] + ChangeId[16] + value length[4]. A relay-only journal record stores exactly the relay fields in the same field-list format and omits the separate relay-index list.

A normal full APPLY WAL record is therefore:
`139 + deltaBytes + Σ(69 + valueBytes) + (4 + Σ(varuint32IndexBytes) when RelayOnly=false)`.

A relay-only ApplyJournal payload is:
`139 + deltaBytes + Σ(relayField(69 + valueBytes))`.

After relay payload acknowledgement and WAL sealing, `CompactAcknowledgedRelayPayloads` can rewrite the ApplyJournal record with an empty relay field list while retaining the protocol/receipt metadata. The minimum retained record is therefore 24-byte journal framing + 139-byte empty relay-only body + ProtocolStateDelta bytes = **163 + deltaBytes**. Receipt metadata and relay payload still share one append-only ApplyJournal file; their physical retention lifetimes are not yet independent.

## 8. FLD Micro-WAL v2 (cross-repository source audit)

Source: FLD-DB `src/Fld1.Wal/Format/WalConstants.cs`, `WalEnvelope.cs`, `WalHeader.cs`, `WalWriter.cs` and `Codec/ApplyCommit.cs`.

- WAL segment header: 64 bytes (`FLDW` magic, v2 header fields and CRC32C).
- Each WAL record: 20-byte envelope + payload bytes + 4-byte record CRC32C + 0–7 bytes alignment padding = **24 + payloadBytes + pad(0..7)**.
- Envelope CRC32C covers its first 16 bytes; record CRC32C covers the first 16 envelope bytes plus payload. The record payload is not copied into a second payload-sized buffer just to calculate CRC.
- APPLY_COMMIT body is described in §7; current body version is v3 and v2 remains readable.
- Segment header cost is per segment, not per record. WAL record padding is `(- (24 + payloadBytes)) mod 8`.

WAL is the crash-recovery source. ApplyJournal is the durable remote receipt/relay source after WAL sealing. The full WAL APPLY payload and the compact relay-only ApplyJournal payload may coexist until the corresponding WAL commit is sealed/truncated; that is a bounded handoff duplication, not a second long-lived full field store. Do not count WAL and ApplyJournal as permanently independent full payload copies.

## 9. Cross-store reference-snapshot memory (not persistent bytes)

`FldDatabase.OpenReferenceSnapshot()` builds three in-memory indexes while the FLD commit gate is held: ChangeId references (16-byte `FldId128` keys), RecordId references (16-byte keys), and receipt references keyed by sender[16] + transport epoch[4] + author epoch[4] + namespace[2] + BatchId[16]. Hash-table capacity, padding and managed-object overhead are runtime-dependent and are not persistent row bytes. If WAL is unsealed, the snapshot conservatively reports every queried identity/receipt as referenced and keeps the draft horizon non-zero.

The snapshot is one scan per GC/frontier operation, not a per-candidate rescan. The adapter must keep its lifetime inside the composed application commit lease and dispose it before releasing that lease.


## 10. Fld1.Storage engine framing (cross-repository source audit)

Source: FLD-DB `src/Fld1.Storage/Engine/StorageDatabase.cs`, `StorageSnapshotFile.cs`, and `Wal/Format/*`.

### Compacted snapshot

- Snapshot file header: 36 bytes (`FLDS`).
- Each live key/value row: keyLength[4] + valueLength[4] + key + value = **8 + keyBytes + valueBytes**.
- Snapshot footer: 4-byte CRC32C over the complete snapshot body.
- Per-partition manifest: 48 bytes (`FLDM`) with its own CRC32C and snapshot length/CRC reference.

Therefore a retained provenance mapping row occupies **92 bytes** in the compacted snapshot (8 + 76-byte key + 8-byte value); its per-author cursor occupies **136 bytes** (8 + 68-byte key + 60-byte RPM3 value). The snapshot header/footer/manifest are amortized per snapshot/partition.

### Storage WAL write-batch framing

The encoded write-batch payload is:
- batch operation count: 4 bytes;
- each Put: kind[1] + keyLength[4] + valueLength[4] + key + value = **9 + keyBytes + valueBytes**;
- each Delete: kind[1] + keyLength[4] + valueLength[4] (zero) + key = **9 + keyBytes**.

The Storage WAL uses the same framing sizes as the FLD Micro-WAL: 64-byte segment header; 20-byte record envelope + payload + 4-byte CRC32C + 0–7 byte alignment padding.

For one AppendPage transaction with (N) mapping Puts plus one cursor Put:
- WAL payload bytes = (4 + 93N + 137 = 141 + 93N);
- WAL record bytes = (165 + 93N + pad(0..7)).

For one prune transaction deleting (N) mapping rows plus one cursor Put:
- WAL payload bytes = (4 + 85N + 137 = 141 + 85N);
- WAL record bytes = (165 + 85N + pad(0..7)).

An empty-page cursor-only update is one Put: 141-byte WAL payload + 24-byte record framing + 0–7 padding. These formulas exclude segment-header amortization, WAL rotation/flush policy and the in-memory sorted-key index.

The compacted snapshot stores only live rows; deletes are represented in the WAL until the next snapshot generation. The Storage snapshot reader copies the sorted key index while sharing immutable value arrays, so snapshot payload arrays are not duplicated wholesale. Runtime dictionary/list/HashSet overhead remains implementation- and cardinality-dependent. Write-batch encoding also sorts all mutations lexicographically by StorageKey before WAL append (currently via LINQ `OrderBy`); this is an O(N log N) per-batch CPU/allocation cost, not a byte cost. Benchmark it before replacing it with an array sort or a sorted mutation collection.


## 11. FLD Manifest and Run structural overhead (cross-repository source audit)

Sources: FLD-DB `src/Fld1.Manifest/Format/ManifestCodec.cs`, `ManifestModel.cs`, `AtomicManifestStore.cs`, and `src/Fld1.Run/Format/RunConstants.cs`.

### Manifest

The encoded manifest has an 80-byte fixed base plus variable entries:
- Namespace entry: 4 bytes.
- RunEntry: 64 bytes (RunId[8], Level[2], pad[2], Namespace[4], MinRec[16], MaxRec[16], SeqLo[8], SeqHi[8]).
- RetainEntry: 16 bytes (RunId[8], RetainUntilEpoch[8]).
- ResultClient entry: 52 bytes + AcceptedBitmap bytes (ClientInstanceId[16], SeqLo[8], SeqHi[8], FinalizedThroughSeq[8], FloorSeq[8], bitmap length[4], bitmap).
- ProvenanceIndex entry: 40 bytes (ClientInstanceId[16], RequestSeq[8], Outcome[1], pad[3], CommitSeq[8], ErrorCode[2], pad[2]).

Total encoded length:
`80 + 4×NamespaceCount + 64×RunCount + 16×RetainCount + Σ(52 + AcceptedBitmapBytes) + 40×ProvenanceIndexCount`.

The fixed 80 bytes include Generation[8], CRC32C[4], last flushed CommitSeq[8], certified HLC+padding[16], five count fields[20], coordinator lease epoch[8] and session ID[16]. AtomicManifestStore publishes the complete CRC-checked manifest atomically; a present corrupt manifest fails closed.

### Run files

- Run header: 256 bytes; footer: 8 bytes; empty run: 264 bytes.
- Cluster header: 40 bytes.
- Field descriptor: 8 bytes per field; tombstone and HLC-override are flags in this descriptor.
- Optional per-field HLC override: 24 bytes.
- Structural constants: CompactSlot 16 bytes, PGM segment 32 bytes, ValueRef 16 bytes, InternEntry 16 bytes.

Run files are the materialized field-state/data layer, not a second synchronization history. The header carries AliveFields and GhostCount; a tombstone is represented by the field descriptor flag, not a separate fixed-size tombstone row. Variable payload, dictionary, PGM and compaction layout costs depend on the actual run and are not added to the relay metadata totals above.


## 12. BIN-SYNC frame registry (source audit)

Sources: `BinSync.Core/Frame.cs`, `Production/NamespaceGcFrames.cs`, `Production/SessionWire.cs`, and `Sync/SyncSession.cs`.

The current registry is contiguous with no gaps or duplicate IDs:

| ID | Frame |
|---:|---|
| 0x01 | HELLO |
| 0x02 | HELLO_ACK |
| 0x03 | ALIAS_MAP |
| 0x04 | SCHEMA_MAP |
| 0x05 | MEMBERSHIP |
| 0x06 | STATE_DIGEST |
| 0x07 | NEED_RANGE |
| 0x08 | NEED_KEYS |
| 0x09 | DELTA_BATCH |
| 0x0A | SNAPSHOT_CHUNK |
| 0x0B | ACK |
| 0x0C | GC_NOTICE |
| 0x0D | ERROR |
| 0x0E | WINDOW_UPDATE |
| 0x0F | PING |
| 0x10 | PONG |
| 0x11 | KEY_ROTATION |
| 0x12 | IBLT_REQUEST |
| 0x13 | IBLT_RESPONSE |
| 0x14 | NAMESPACE_ROSTER |
| 0x15 | ORIGIN_FRONTIER |
| 0x16 | RETENTION_CHECKPOINT |
| 0x17 | FULL_REPLACE_BEGIN |
| 0x18 | FULL_REPLACE_CHUNK |
| 0x19 | FULL_REPLACE_COMMIT |
| 0x1A | FULL_REPLACE_ACK |
| 0x1B | FULL_REPLACE_ABORT |
| 0x1C | FULL_REPLACE_READY |

`NamespaceGcFrames` exactly matches the `FrameTypeMap`; the session parser accepts 0x04–0x1C for non-ALIAS_MAP session frames, and 0x03 is handled by the alias-map path. Frame envelope is 25-byte header, magic 0x4253, version 0x00 and CRC32C. This closes the registry inventory only; signed payload golden vectors and per-message encode/decode round trips remain T-pass items.
