> **v3.0.0 integration override (2026-11-08):** The current normative contract is BIN-SYNC-SPECIFICATION.txt v3.0.0. This document retains the earlier Node-Outbox/FLD-ApplyJournal bridge design for migration traceability; §§3.1.1–3.1.2 are transition-only and are not the final v3 ownership model. In v3, FLD OutboxSeq, FLD JournalSeq and FLD CommitSeq are source-local coordinates, never OriginSequence. BIN-SYNC Metadata Store allocates and persists OriginSequence at durable publication freeze. The Metadata Store is the sole owner of published ChangeId/Origin identity, receipts, peer delivery state and relay payload; FLD APPLY/WAL owns field-state recovery and tombstones, not a second authoritative relay history. No duplicate PendingLocal journal is permitted. Storage remains opaque and domain-neutral; Database remains the field-semantic overlay. The two-DLL target remains Fld1.Storage.dll and Fld1.Database.dll.

# Контракт интеграции BIN-SYNC ↔ FLD-DB

**Статус:** специализированный проект контракта интеграции FLD-DB как синхронизируемой базы.  
**Общая архитектура:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Физический движок:** [FLD Storage Core architecture](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/FLD-STORAGE-CORE-ARCHITECTURE.md).  
**Область:** FLD-специфичная граница между durable field state и BIN-SYNC. Это не wire-spec и не контракт универсального Metadata Store.  
**Владелец durable field state и физических гарантий хранения:** FLD-DB.  
**Владелец протокольной семантики и решений синхронизации:** BIN-SYNC.  
**Важно:** этот документ описывает режим, когда FLD-DB хранит сами синхронизируемые пользовательские данные. Режим FLD-DB как отдельного Storage-only Metadata Store описывается общей архитектурой и требует отдельной реализации. До реализации полной транзакционной границы прямое подключение текущего `FldDatabase` к текущему `Node` не считается безопасным.

## 1. Цель и разделение ответственности

### FLD-DB owns (в режиме синхронизируемой базы)

- durable field values, field/record tombstones и lifecycle state;
- локальный author identity и генерацию HLC для локальных записей;
- локальный durable Outbox и его namespace cursor;
- WAL, group commit, checkpoint, recovery и compaction;
- физическую атомарность durable field state и переданных BIN-SYNC metadata updates;
- durable хранение opaque protocol commit receipt/state delta и relay journal, не интерпретируя их протокольный смысл.

### BIN-SYNC owns

- wire protocol, framing, compression/signatures/E2EE, trust и schema validation;
- session aliases, peer membership, peer-specific cursors и ACK validation;
- batch digest chain, inbox/duplicate semantics, ChangeId dedup policy;
- per-peer acknowledgement aggregation и решение, когда локальный Outbox prefix безопасно подтверждён;
- relay policy и назначение relay sequence для author stream;
- формирование DELTA_BATCH, STATE_DIGEST, IBLT и snapshot protocol messages.

BIN-SYNC не должен держать вторую копию durable field state или второй WAL с теми же field mutations при использовании FLD как Data Provider/Metadata Store. FLD-DB не должен самостоятельно интерпретировать wire protocol, вычислять peer-safe ACK или определять протокольную политику хранения. Общая модель разделения ответственности, включая независимые внешние источники данных и Storage-only режим FLD, определена в [общей архитектуре](METADATA-STORAGE-ARCHITECTURE.md).

## 2. Идентичность и отображение курсоров

1. `FldDatabase.LocalAuthor` обязан побайтно совпадать с `BinSync.NodeId`. Один FLD database instance обслуживает один namespace; несколько namespace используют один и тот же local author.
2. `FldDatabase.NamespaceId` соответствует BIN-SYNC `namespaceId`; допустимый диапазон — `0..65535`.
3. FLD `OutboxSeq` — durable cursor локального provider feed/Outbox. Он нужен для чтения и ACK source changes, но не является BIN-SYNC OriginSequence и не переносится как origin в wire.
4. `OriginSequence` выделяется BIN-SYNC Metadata Store атомарно при freeze опубликованной операции, монотонно в пределах `(scope, namespaceId, originAuthorNodeId)`. Он не выводится из FLD `OutboxSeq`, `JournalSeq` или `CommitSeq`. Повторный freeze того же ChangeId возвращает ранее сохранённый Origin.
5. FLD `CommitSeq` — WAL durability/commit sequence, **не** BIN-SYNC stream cursor и не OriginSequence.
6. FLD `MaterializedThroughCommitSeq`, `LastSealedCommitSeq`, `CertifiedWatermark`, `OutboxSeq`, `JournalSeq`, BIN-SYNC `SenderCursor` и `OriginSequence` — разные шкалы. Их нельзя подменять друг другом.
7. `ChangeId` — стабильный 16-byte идентификатор mutation. Он не генерируется заново при remote apply, replay, retry или relay.
8. Author tie-break использует порядок BIN-SYNC `NodeId`: `Lo = UInt64LE(bytes[0..7])`, затем `Hi = UInt64LE(bytes[8..15])`. RecordId sort остаётся unsigned BE memcmp. HLC wall — микросекунды UTC; `KeyEpoch` — u16.

## 3. Предлагаемая граница API

Адаптер реализуется отдельным слоем, который зависит от обоих проектов. FLD-DB не должен ссылаться на `BinSync.Core` и wire-типы напрямую. DTO на границе — нейтральные storage DTO с фиксированными размерами идентификаторов.

Минимальные операции адаптера:

| Операция | Контракт |
|---|---|
| `OpenReadSnapshot(namespaceId)` | Возвращает стабильный read-only snapshot поколения базы; чтение не удерживает writer lock на всё время отправки |
| `FreezePendingLocalChanges(limit, lease)` | Атомарно переводит выбранные durable pending local changes в immutable Outbox, назначает постоянные OutboxSeq/ChangeId и фиксирует payload/reference до возможности отправки |
| `ReadLocalOutboxBatch(afterSeq, limit, lease)` | Читает только frozen локальные изменения FLD author; возвращает стабильные OutboxSeq/ChangeId и полные HLC/op/value |
| `AcknowledgeLocalOutbox(safeThroughSeq, lease)` | Подтверждает только непрерывный безопасный prefix, вычисленный BIN-SYNC по всем interested peers |
| `ApplyRemoteBatch(commit)` | Одна атомарная durable транзакция: field state + dedup/inbox/cursor + relay journal/protocol extension |
| `ReadRelayBatchPage(author, afterJournalSeq, limit, lease)` | Возвращает relay-eligible mutations и отдельный `ScannedThroughJournalSeq`, включая пустые диапазоны; legacy `ReadRelayBatches` остаётся wrapper |
| `AcknowledgeRelay(author, safeThroughJournalSeq, lease)` | Разрешает reclamation только по per-author JournalSeq watermark, вычисленному BIN-SYNC после ACK всех interested peers |

### 3.1 RemoteBatchCommit

Граница `ApplyRemoteBatch` принимает не только список победивших полей. В commit DTO должны быть доступны:

- `SenderPeer`, `SenderEpoch`, `NamespaceId`;
- `BatchId`, `CursorStart`, `CursorEnd`, `DurableAuthor`;
- `PreviousBatchDigest`, `BatchDigest`, `RecordsRoot` и остальные данные, необходимые для восстановления chain/inbox state;
- полный набор decoded mutations: `RecordId`, `FieldId`, operation kind, value, HLC, `AuthorNodeId`, `KeyEpoch`, обязательный `ChangeId`;
- opaque `ProtocolStateDelta`, сформированный BIN-SYNC: inbox cursor/chain update, global ChangeId ledger delta, relay journal delta и необходимые provenance/session данные.

FLD хранит opaque protocol extension как часть той же durable transaction, не разбирая wire semantics. BIN-SYNC остаётся владельцем кодирования и интерпретации extension.

### 3.1.1 Relay journal cursor and safe acknowledgement

`ReadRelayBatchPage(author, afterSeq, limit)` returns the matching batches plus `ScannedThroughJournalSeq`, the last global journal record inspected. This cursor advances across records that contain no relay operation for the requested author, avoiding repeated scans of empty ranges. The older `ReadRelayBatches` method remains a compatibility wrapper that returns only the batch list.

JournalSeq is global to the namespace ApplyJournal and is not interchangeable with BIN-SYNC OutboxSeq, peer cursor, author stream sequence or WAL CommitSeq. For a given author, matching JournalSeq values may have gaps because intervening batches can contain no relay operation for that author.

For every returned relay operation, the adapter forms `OutboxChangeIdentity(authorNodeId, changeId)` and resolves the local sequence through `Node.TryResolveOutboxSequences(namespaceId, identities, sequences)`. The resolver requires every identity to be unique and still retained in the Outbox; it returns false and clears the output if any entry is missing, and rejects author/ChangeId conflicts. The adapter must not advance the scan cursor on false. It must atomically persist the resolved `(source JournalSeq, author, OutboxSeq)` sequence mapping and the page's `ScannedThroughJournalSeq` before allowing those operations to be sent or ACKed. ChangeId remains in BIN-SYNC's global ChangeId ledger and is not copied into the provenance map. This mapping must survive restart; it cannot be reconstructed from current field winners because relay-eligible LWW losers may no longer be visible in field state. A scan cursor is a read-progress marker only; it is not authorization to reclaim relay payload.

`AcknowledgeRelay(author, safeThroughJournalSeq, lease)` maps to FLD's implemented `FldDatabase.AcknowledgeRelayBatches(author, safeThroughJournalSeq)`. It may advance only to the largest contiguous source JournalSeq for which every relay operation for that author has been durably published and all interested downstream peers have validated ACKs covering the corresponding local Outbox entries. BIN-SYNC computes the minimum safe local Outbox watermark across peers through `Node.GetSafeOutboxAckedThrough(namespaceId, authorNodeId)`; this value must then be translated through the durable source JournalSeq → ChangeId → local OutboxSeq provenance map to a per-author source JournalSeq. FLD must not infer or calculate peer safety. FLD persists the watermark immediately and hides those payloads from reads, but physical removal is deferred until a checkpoint has sealed and truncated the corresponding APPLY WAL commits. The acknowledgement is per original author, not per immediate sender. If one source batch contains relay operations from multiple authors, each author's source payload can be released independently only after its own safe watermark covers that batch.

Never pass CommitSeq, OutboxSeq or a peer-relative ACK cursor as this API's JournalSeq. Consumed/read is not the same as durably acknowledged downstream.

### 3.1.2 Outbound publication gate and ACK translation

The ordering above requires an adapter-level publication gate; the requirement cannot be satisfied by merely calling the resolver eventually:

1. The adapter is the sole owner of transport scheduling for the BIN-SYNC Node/namespace. After a remote APPLY may have created relay Outbox entries, it MUST NOT expose those entries to any sender until their source JournalSeq provenance mappings and scan cursor are durably committed. In particular, no `CreateDeltaBatch`, Outbox lease/read, or transport send may race ahead of provenance persistence.
2. On process start, the namespace remains gated until the adapter has resumed its persisted scan cursor, reconciled any uncommitted page, resolved every returned `(author, ChangeId)` against retained Outbox entries and committed the page mapping + new scan cursor. A crash before that commit is recovered by rereading the same FLD page; the resolver is idempotent and no cursor may be advanced on partial resolution.
3. Use an adapter-owned per-namespace async gate around inbound apply→relay-page publication and outbound batch construction. Do not hold FLD internal locks while waiting for network I/O, and do not hold BIN-SYNC Node locks while performing disk I/O in FLD. The gate serializes only the adapter's protocol publication boundary; the two engines retain their own lock order.
4. Mapping records are keyed by original author and contain only `(JournalSeq, OutboxSeq)`; the ChangeId→OutboxSeq identity mapping remains in the global BIN-SYNC ledger. All mappings from one page and `ScannedThroughJournalSeq` are one durable atomic update. A page with zero operations for an author still advances that author's scan cursor.
5. To translate BIN-SYNC `safeOutboxSeq` back to FLD, find the first mapped source JournalSeq for that author containing any OutboxSeq greater than the safe watermark. The source safe-through is one less than that JournalSeq; if none exists, it is the persisted scanned-through cursor. This is valid only because the adapter publishes source pages in increasing JournalSeq order and BIN-SYNC assigns monotonically increasing OutboxSeq within each original author stream. Validate this monotonicity; on violation, fail closed rather than sorting away the discrepancy.
6. Call FLD `AcknowledgeRelayBatches(author, safeThroughJournalSeq)` only after the translated source watermark is durable in adapter state. Prune mapping rows only after FLD confirms the relay ACK sidecar is durable; if the process crashes between the FLD ACK and mapping cleanup, stale mapping rows are harmless and may be pruned on restart. Never delete the mapping first.
7. If `TryResolveOutboxSequences` returns false because any operation is no longer retained and no prior durable mapping exists, stop publication/ACK advancement for that namespace and require recovery/reconciliation. Do not reconstruct OutboxSeq from current field winners or allocate a replacement sequence.

This gate is a correctness requirement for the future adapter, not an API currently enforced inside `BinSync.Core`. Keeping the source JournalSeq mapping outside the protocol core avoids leaking FLD-specific journal coordinates into BIN-SYNC's peer/author stream model.



### 3.1.3 Current concrete API surface (2026-11-09)

The source-level integration boundary now has concrete BIN-SYNC coordinator orchestration primitives and reusable host adapters. The application still has to compose them with its concrete FldDatabase callbacks and route every namespace sender through the same publication gate; no direct FLD project dependency is introduced into BIN-SYNC. The approved physical FLD project layout is `Fld1.Storage.dll` plus `Fld1.Database.dll`. The adapter must be a host/integration layer depending on both products; neither FLD Database nor the generic Storage project may reference BIN-SYNC protocol types.

**FLD Database APIs (in `Fld1.Database`):**

- `FldDatabase.AcquireCommitGuard()` — serializes local commits, remote apply, checkpoint/recovery-sensitive operations and reference snapshots.
- `FldDatabase.ReadRelayBatchPage(author, afterJournalSeq, limit)` — returns complete journal batches plus `ScannedThroughJournalSeq`. The page is now bounded to 4,096 matching operations in aggregate and never splits a JournalSeq group.
- `FldDatabase.AcknowledgeRelayBatches(author, safeThroughJournalSeq)` — durably acknowledges a per-original-author source prefix; physical removal still waits for the FLD checkpoint/compaction path.
- `FldDatabase.OpenReferenceSnapshot()` — builds one immutable index of host-side ChangeId, RecordId and receipt references plus the unpublished-draft horizon. It must be opened while the composed application commit lease is held.

**BIN-SYNC APIs (in `BinSync.Storage`):**

- `StorageSyncCoordinator.PublishAndSendAppliedOriginFrontierAsync(...)` — creates and persists a new frontier report before sending it.
- `StorageSyncCoordinator.SendAcceptedAppliedOriginFrontierAsync(...)` — retries transport of the latest already-persisted local report without allocating a new ReportSequence; it verifies the stored signature against the accepted roster and supplied member key.

- `StorageSyncCoordinator.AcquireApplicationCommitGuard(hostLeaseFactory)` — composes coordinator, Node, host FLD and shared Storage gates in the documented lock order.
- `StorageSyncCoordinator.ReadAndAppendRelayProvenancePage(map, originalAuthor, hostLeaseFactory, readSourcePage)` — acquires the composed application guard, invokes the host's synchronous page-read/flatten callback while the FLD commit lease is held, then calls the resolver/atomic RPM3 append. The callback returns ordered `StorageRelaySourceChange[]` plus `ScannedThroughJournalSeq`; it must not perform network I/O.
- `StorageSyncCoordinator.AppendRelayProvenancePage(map, originalAuthor, sourceChanges, scannedThroughJournalSeq)` — the lower-level variant for callers already holding the composed application guard. It resolves each transient `(JournalSeq, ChangeId)` through `Node.TryResolveOutboxSequences` and atomically persists only `(JournalSeq, OutboxSeq)` plus the scan cursor. It returns false without cursor advancement if any ChangeId is no longer in the retained Node Outbox.
- `StorageRelayProvenanceMap.TranslateSafeOutboxSequence(namespaceId, author, safeOutboxSeq)` — translates the minimum downstream-safe Node Outbox watermark to a whole-source-JournalSeq watermark.
- `StorageRelayProvenanceMap.CommitAfterFldRelayAck(...)` — persists the translated ACK and prunes map rows only after FLD has durably accepted `AcknowledgeRelayBatches`.
- `StorageSyncCoordinator.AcknowledgeRelayProvenance(map, originalAuthor, hostLeaseFactory, acknowledgeSource)` — provides the complete safe-ACK ordering helper. It acquires the composed application guard, reads `Node.GetSafeOutboxAckedThrough`, translates through RPM3, invokes the host's durable FLD ACK callback, and prunes map rows only after that callback returns successfully.
- `StorageRelayPublicationGate` — serializes page publication, downstream send scheduling and safe-ACK work. `PublishPageAsync` commits the source page's RPM3 mapping before invoking the sender callback; `AcknowledgeAsync` performs the FLD ACK before RPM3 pruning. All other outbound send/build paths for the namespace must use `RunOutboundAsync` or the same host gate; bypassing it violates the publication fence.
- `DelegateApplicationGcReferenceScanner` / `DelegateApplicationGcReferenceScan` — adapt a host-created immutable snapshot to `IApplicationGcReferenceScanner` without adding FLD types to BIN-SYNC. The scan wrapper owns both the snapshot and FLD commit lease and releases the snapshot before the lease.

**Required host sequence:**

1. Obtain the gate only through `StorageRelayPublicationGate.For(coordinator)` and route remote APPLY and all outbound batch construction/send for the namespace through that instance. For normal receive, use `ApplyAndPublishPageAsync`; at startup, keep the sender closed and use `RecoverAndPublishPageAsync` with `fld.GetRelayJournalHighWatermark()` and `fld.ReadRelayBatchPage(...)` callbacks before opening it.
2. The gate calls `ReadAndAppendRelayProvenancePage` with `() => fld.AcquireCommitGuard()` and a synchronous callback that calls `ReadRelayBatchPage`, flattens each complete batch's `RelayWrites` into ordered `StorageRelaySourceChange(JournalSeq, ChangeId)` entries, and returns the page cursor. Preserve original author and JournalSeq order; do not use CommitSeq or feed OutboxSeq as OriginSequence.
3. If the helper returns false, keep the namespace publication-gated and reconcile from retained source/Node state; never reconstruct OutboxSeq from current field winners. Do not expose Outbox entries to `CreateDeltaBatch`, a lease or transport before the map+cursor commit succeeds.
4. The helper releases the composed FLD/Node/Storage commit lease after the provenance page is durable. Only then may the publication gate invoke the downstream sender. The adapter's async gate—not the Node or FLD internal lock—serializes remote APPLY, mapping commit and outbound scheduling.
5. After downstream ACK aggregation, call `StorageSyncCoordinator.AcknowledgeRelayProvenance` with a host lease factory and a callback that invokes `fld.AcknowledgeRelayBatches(author, safeJournalSeq)`. The coordinator reads the safe per-author Node Outbox watermark, translates it through RPM3, runs the FLD ACK callback under the composed guard, and prunes mappings only after successful return. Never prune the map before FLD's ACK sidecar is durable.
6. On restart, reconcile the persisted RPM3 cursor with the FLD journal and retained Node Outbox before opening publication. A crash after FLD ACK but before map pruning is harmless: replaying the same FLD ACK through the helper is idempotent, then the map is pruned.

**Reference scanner implementation boundary:** under the same composed application guard, open exactly one `FldReferenceSnapshot` and keep the FLD commit lease held for the entire GC/frontier operation. Its concrete public members are `UnpublishedLocalDraftHorizon` (`Count`, `MaxHlcWall`, `MaxHlcLogical`), `HasChangeIdReference(changeId)`, `HasRecordReference(recordId, tombstoneChangeId)`, and `HasReceiptReference(senderPeer, authorEpoch, transportPeerEpoch, namespaceId, batchId)`. Map these to `IApplicationGcReferenceScan` and keep both the snapshot and FLD lease alive until the scan wrapper is disposed. `FldReferenceSnapshot.Dispose()` releases only its in-memory indexes; the host wrapper must also dispose the acquired FLD commit lease. A per-candidate rescan is not acceptable. The adapter must fail closed if snapshot creation or any reference query fails.

This section records the actual API names and lock/order contract, not a claim that the adapter has been implemented or tested. The coordinator-level page/ACK orchestration, remote-apply-aware serialized publication gate and delegate-based reference-snapshot adapter are now implemented. The remaining integration work is application composition against the actual `FldDatabase` methods, startup cursor reconciliation before opening the sender, routing every namespace sender through the gate, and wiring `FldReferenceSnapshot` predicates into the delegate scan. These host-specific callbacks cannot live in either runtime DLL without creating a forbidden dependency.


### 3.2 Семантика ApplyRemoteBatch

`ApplyRemoteBatch` обязана выполнять атомарно:

1. Проверить namespace и размеры идентификаторов.
2. Проверить idempotency по входящему batch и ChangeId согласно ключам BIN-SYNC.
3. Применить к field state только LWW-победителей, используя канонический comparator.
4. Сохранить все новые принятые relay-eligible mutations, даже если они проиграли LWW.
5. Сохранить inbox/cursor/digest-chain/global ChangeId ledger и relay journal delta.
6. Записать WAL commit и выполнить требуемый durability flush.
7. Только после durable commit опубликовать состояние и вернуть receipt.

**Успешный return означает, что можно отправлять ACK.** При ошибке ни field state, ни inbox/cursor, ни global ChangeId ledger, ни relay journal не должны частично публиковаться; ACK не отправляется.

Повтор того же batch после lost ACK возвращает duplicate receipt с тем же `CursorEnd`/author result и не создаёт повторных field mutations или relay entries. Повтор ChangeId через другого peer не должен повторно применять/relay mutation, но входящий peer cursor должен быть корректно обработан.

Повтор ChangeId через другого peer с тем же canonical fingerprint не пере-применяется и не ставится в relay повторно, но новый batch receipt и receive cursor этого peer фиксируются. Тот же ChangeId с другим fingerprint отклоняет весь batch до публикации любых изменений.

Remote mutation не создаёт запись в **локальном** FLD Outbox. Self-echo уже локально authored mutation не ставится в relay повторно.

`ProtocolStateDelta` — bounded per-batch recovery envelope. Его изменения материализуются в индексированные protocol metadata records при финализации; raw delta удерживается только внутри pending-apply intent для separate-store recovery и удаляется после durable finalization. Duplicate BatchId возвращает compact receipt/ACK result, а не полный ProtocolStateDelta.

## 4. Локальный Outbox и ACK

### 4.1 Текущее поведение

Текущая реализация FLD создаёт Outbox entries при каждом durable `Commit()` для dirty field mutations. Несколько Put одного поля до одного Commit уже схлопываются через `_dirty`; отдельные Commit того же поля создают отдельные immutable Outbox entries. Это безопасно, но хранит промежуточные локальные версии, которые могли ещё не быть доступны ни одному peer.

### 4.2 Целевое поведение: pending staging → frozen Outbox

Детальная политика определена в [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md). В целевой модели local Put/Delete атомарно обновляет current field state и durable pending staging. Для LWW_FIELD pending операции одного и того же `(namespace, recordId, fieldId)` могут заменять друг друга, пока они не были frozen/exposed to any peer. Pending payload должен оставаться доступным даже если remote APPLY позже заменит visible field winner.

Перед тем как BIN-SYNC сможет отправить pending operation, FLD атомарно freeze-ит её в append-only Outbox, назначая неизменяемые OutboxSeq и ChangeId. После freeze запрещено переписывать или коалесцировать запись, даже если ни один ACK ещё не получен. Текущий append-only Outbox API сохраняет этот контракт; оптимизация относится к staging до Outbox, а не к переписыванию Outbox tail.

Freeze и перенос pending→Outbox должны быть одной durable transaction/WAL recovery boundary. Сетевой send не может начаться до успешного durable freeze.

### 4.3 Cursor and ACK rules

- BIN-SYNC преобразует frozen entries в author stream локального Node. `OutboxSeq` используется как cursor; `CommitSeq` не используется в DELTA_BATCH cursor.
- `AcknowledgeLocalOutbox` получает не ACK отдельного peer, а вычисленный BIN-SYNC `SafeOutboxSeq`: минимальный непрерывный cursor, подтверждённый всеми interested peers.
- Peer без PeerState/ACK не разрешает удалять соответствующий prefix. Это сохраняет relay/reconnect safety.
- Compaction сохраняет исходные OutboxSeq и следующий sequence; никакой renumbering после restart/compaction.
- Outbox consumer должен иметь fenced lease: owner/session identity + монотонный lease epoch/token. Устаревший consumer не может читать/ACK-ать после выдачи нового lease.
- FLD API должен предоставить high watermark (последний назначенный OutboxSeq) отдельно от ACK watermark.

## 5. Relay journal

Локальный FLD Outbox и BIN-SYNC relay queue — разные логические очереди.

Relay journal сохраняет исходную mutation identity и содержимое: ChangeId, RecordId, FieldId/op kind, HLC, author, KeyEpoch и value/tombstone. BIN-SYNC назначает и восстанавливает relay sequence по своим правилам author stream. FLD local OutboxSeq не используется для remote author.

Relay retention/compaction рассчитывается по ACK всех interested peers. LWW loser остаётся relay-eligible: локальное значение может быть новее, но операция всё равно нужна другим узлам для детерминированной сходимости. Global ChangeId ledger отдельно подавляет повторную постановку этой операции после compaction payload.

Global ChangeId ledger — отдельное состояние BIN-SYNC, не FLD relay journal. Его ключ — (scope, localReplicaId, namespace, ChangeId), fingerprint проверяет неизменность canonical operation across peers. В текущем protocol revision нет safe GC watermark; ledger удерживается до fenced namespace/scope epoch reset. FLD не должен самостоятельно удалять opaque ProtocolStateDelta или ChangeId ledger по TTL либо по relay ACK.

### 5.1. Текущая стоимость APPLY journal и целевое разделение

Реализация FLD-DB уже устранила основное дублирование:
- ApplyCommitCodec v3 кодирует входящий `Fields` один раз и представляет `RelayFields` canonical varint-индексами. Декодер читает прежний v2 и проверяет relay subset.
- FldDatabase.ApplyRemoteBatch сначала flush-ит полный APPLY_COMMIT в WAL, затем ApplyJournal сохраняет receipt metadata и только relay-selected operations. Non-relay payload больше не копируется в sidecar.
- ApplyJournal materialization выполняется до возврата ApplyRemoteBatch и flush-ится до последующего checkpoint/WAL truncation. LWW losers, выбранные для relay, сохраняются.
- Journal promotion идемпотентен по batch identity; WAL recovery восстанавливает компактный relay-only journal из v2 и v3 WAL records.
- ReadRelayBatchPage теперь использует lower-bound binary search по JournalSeq: O(log N + page size), без повторного сканирования уже прочитанного префикса; legacy ReadRelayBatches остаётся wrapper.

Оставшиеся ограничения:
- Receipt metadata и relay payload всё ещё co-located в одном append-only ApplyJournal файле; физические retention lifetimes пока не независимы.
- FLD now exposes `AcknowledgeRelayBatches(author, safeThroughJournalSeq)`: it persists a per-author watermark; reads suppress acknowledged payload immediately, and physical compaction runs only after WAL seal/truncation. The BIN-SYNC core now exposes `Node.GetSafeOutboxAckedThrough(namespaceId, authorNodeId)` and uses the same O(C + A×P) per-author minimum aggregation in `CompactOutbox`. The adapter still does not call FLD's relay ACK API because the new RPM3 sequence-pair map is not yet wired to the current pre-extraction FldDatabase APIs; the core resolver and map primitive now exist, but the cross-assembly host adapter is deliberately deferred until the approved two-DLL extraction.
- BatchKey Dictionary still grows with receipt history; a disk-backed/rebuildable index and independent receipt GC are not implemented.
- Receipt GC требует отдельного Inbox retry horizon; relay payload GC — минимального safe ACK всех interested downstream peers по каждому author stream. Эти watermark должен вычислять BIN-SYNC, а не FLD.

Целевая модель:

1. WAL APPLY_COMMIT хранит все входящие операции один раз и компактную relay-selection index list.
2. Durable batch receipt хранит только identity, cursor range, BatchDigest/RecordsRoot/PreviousBatchDigest, author-bound ACK/result metadata и opaque ProtocolStateDelta.
3. Relay journal хранит только relay-selected operations, включая LWW losers; его cursor — локальный JournalSeq, не peer/author cursor.
4. Receipt lifetime и relay payload lifetime независимы. Receipt GC определяется Inbox retry horizon; relay payload GC — минимальным safe ACK всех downstream peers по каждому author stream.
5. WAL truncation допускается только после durable promotion relay payload и metadata receipt.
6. Индексы должны быть bounded/rebuildable, а paging — seekable по JournalSeq.

## 6. Read snapshot

Snapshot API должен давать один согласованный view поверх overlay и immutable runs:

- field value и tombstone/lifecycle state;
- HLC wall/logical и full AuthorNodeId;
- KeyEpoch;
- namespace и стабильное поколение snapshot.

Snapshot lifetime защищает необходимые run generations от reclamation. BIN-SYNC строит digest/IBLT/snapshot payload из этого read view без удержания FLD writer lock на время сетевой отправки.

Snapshot — это current-state baseline, а не автоматическое подтверждение каждой записи локального Outbox или relay journal. Snapshot не должен продвигать Outbox/relay ACK, разрешать GC или удалять pending/frozen operations, пока BIN-SYNC явно не определит, какие операции покрыты snapshot baseline. В частности, LWW-loser операции могут отсутствовать среди текущих field winners, но оставаться relay-eligible. Текущий SnapshotChunkMessage/BuildSnapshotChunks runtime path не несёт per-field ChangeId или Outbox cursor; нормативный SnapshotTail/outbox checkpoint из wire specification требует отдельного согласования с runtime до проектирования соответствующей storage-транзакции.

FLD Run `rootDigest` (BLAKE3-256) не подменяет BIN-SYNC wire `Digest16` (BLAKE3 truncated): это разные digest domains.

## 7. Текущее состояние API и блокирующие расхождения

Code/API baseline revalidated for this documentation pass: FLD-DB `fix/crash-consistency-outbox` code baseline `eacff36f1d3ff513508100d27f9d572bf3cfff66`; BIN-SYNC `feature/fld-external-storage` code baseline `d2e0313c7de685066d663d5e14901b3a67abd1b7`. Comparing each baseline with the current branch head shows only documentation and test/conformance-fixture changes after these code baselines; no production runtime source changes were made in this review. The BIN-SYNC baseline includes persistence v12, bounded snapshot parsing, adaptive in-memory receipt grouping, per-author Outbox compaction, the public safe-ACK query and Outbox ChangeId-to-sequence resolver; the FLD baseline includes the relay-only ApplyJournal and per-author ACK/retention boundary.

| Текущее API / состояние (проверено на FLD fix/crash-consistency-outbox) | Статус / оставшийся разрыв |
|---|---|
| FldDatabase.ApplyRemote(ReadOnlySpan<RemoteFieldWrite>) | Оставлен как obsolete volatile merge-only compatibility API; не durable и запрещён для BIN-SYNC receive |
| RemoteFieldWrite.ChangeId | Теперь обязательный 16-byte nonzero идентификатор; конструктор валидирует длину и ненулевое значение |
| FldDatabase.ApplyRemoteBatch(RemoteBatchCommit) | Реализован durable receive boundary: проверяет duplicate batch, формирует WAL APPLY commit, сохраняет protocol delta и relay subset, flush-ит WAL/apply journal, затем публикует LWW winners; remote writes не попадают в local Outbox |
| RemoteBatchCommit.ProtocolStateDelta | Передаётся и сохраняется opaque; FLD не интерпретирует его. BIN-SYNC ещё должен обеспечить корректное формирование, применение/recovery и атомарное согласование этого delta со своим runtime state |
| ApplyJournal / ReadRelayBatchPage / AcknowledgeRelayBatches | Durable append-only journal, seekable per-author pages and durable per-author safe-ACK/retention boundary are implemented in FLD; the BIN-SYNC adapter still needs the durable source-to-Outbox provenance map before it can call the ACK API |
| ApplyCommitCodec v3 / ApplyJournal payload | Fields кодируется один раз, RelayFields задаётся varint-индексами; ApplyJournal хранит receipt metadata и только relay-selected payload. v2 остаётся читаемым. Независимые receipt/relay GC и bounded/rebuildable ApplyJournal indexes ещё открыты |
| AcquireOutboxLease / ReleaseOutboxLease / VisitOutboxBatch / AcknowledgeOutbox(..., lease) | Durable fenced lease, borrowed-span enumeration и fenced ACK реализованы |
| GetOutboxHighWatermark | Отдельный high watermark реализован; WAL CommitSeq и OutboxSeq остаются разными шкалами |
| Node.GetSafeOutboxAckedThrough(namespaceId, authorNodeId) | Implemented in BIN-SYNC; returns the minimum local OutboxSeq acknowledged by every interested peer after applying per-peer StreamOffset; returns zero when there are no interested peers or a known peer lacks PeerState. CompactOutbox uses the same per-author aggregate. The adapter still needs source JournalSeq → ChangeId → OutboxSeq provenance |
| Node.TryResolveOutboxSequences(namespaceId, identities, sequences) | Implemented in BIN-SYNC; resolves a page's unique (author, ChangeId) identities to retained local OutboxSeq values without copying payloads; returns false and clears output if any entry is no longer retained. Adapter must durably persist the resulting JournalSeq mapping and scan cursor before send/ACK | 
| Node.InboxAuthors / InboxReceiptStore | Implemented as adaptive singleton/grouped in-memory receipt index; NodePersistence v12 writes adaptive flat/author-grouped receipt layouts. The admission cap remains opt-in via NodeOptions.Limits; expiry/GC remains open |
| FLD Storage-only Metadata Store | Не реализован; target is direct use of the domain-neutral `Fld1.Storage` API, without constructing `FldDatabase` or LocalAuthor/HLC/Outbox |
| BIN-SYNC Node | Storage/Outbox/PeerStates/SeenChanges и протокольные структуры остаются внутренними коллекциями Node; InboxAuthors уже использует adaptive grouped index. Общего Metadata Store boundary пока нет. Target persistence uses the global ChangeId ledger described in the architecture contract |

**Запрет до появления целевого API:** не использовать последовательность `FldDatabase.ApplyRemote(...); FldDatabase.Commit();` как реализацию BIN-SYNC receive. Она не обеспечивает atomic batch/idempotency/relay semantics и может добавить remote author entries в локальный Outbox.

## 7.1. Two-DLL Storage Core boundary

The runtime architecture is exactly two independently buildable DLLs:

1. `Fld1.Storage.dll` — universal byte-oriented physical engine: atomic byte batches, WAL/recovery, pages/runs, manifest, snapshots, checkpoint, ordered scans and storage-native reclamation. It does not reference FLD types, HLC, NodeId, ChangeId, BIN-SYNC or wire DTOs.
2. `Fld1.Database.dll` — FLD domain layer: FldDatabase, records/fields, field and record lifecycle tombstones, HLC/LWW, field codecs, indexes/projections, FLD Outbox/APPLY and the BIN-SYNC integration boundary.

Dependency is strictly one-way: `Fld1.Database.dll → Fld1.Storage.dll`. No third Common/Shared runtime DLL is introduced.

Tombstone responsibility is split deliberately. Storage must durably store the opaque bytes representing FLD field/record tombstones and may use its own generic deletion markers for physical keys, but it must not interpret FLD tombstone semantics or decide when they are safe to remove. Fld1.Database owns lifecycle/HLC/author semantics and protocol-safe tombstone retention/GC. An old HLC timestamp alone never authorizes physical deletion. Storage-only mode opens Fld1.Storage directly and creates no synthetic synchronized field state. See the FLD-DB Storage Core architecture document.

## 8. Приёмочные тесты контракта

1. Local writes: restart сохраняет OutboxSeq, ChangeId, HLC и payload без renumbering.
2. Remote commit: field winners, inbox cursor, global ChangeId ledger and relay journal appear atomically.
3. Crash/lost ACK: повтор того же BatchId возвращает duplicate receipt; нет повторного применения и relay append.
4. Cross-peer duplicate ChangeId: identical fingerprint does not reapply or relay twice; each incoming peer cursor and Inbox receipt still advance atomically. Conflicting fingerprint rejects the complete batch.
5. LWW loser: field state не меняется, но новая операция остаётся relay-eligible.
6. Remote apply → commit/reopen: remote operation не появляется в local Outbox.
7. Safe ACK: один отставший/неизвестный peer удерживает local/relay prefix; stale lease token отклоняется.
8. Compaction/restart: OutboxSeq и relay cursors сохраняют continuity; новый peer получает корректный gap-normal cursor.
9. Snapshot under concurrent commit/checkpoint: один стабильный view, без пропусков/дубликатов и без use-after-reclaim.
10. Lifecycle/tombstone/KeyEpoch: digest и LWW результаты совпадают с BIN-SYNC comparator и FLD canonical encoding.
11. APPLY_COMMIT stores each incoming payload once; relay selection does not duplicate selected field values.
12. ApplyJournal receipt survives WAL checkpoint/truncation without retaining non-relay field payloads.
13. RelayJournal contains only relay-selected operations and is retained independently of Inbox receipt expiry.
14. Crash between WAL flush and receipt/relay sidecar flush rebuilds both sidecars exactly once from CommitSeq.
15. Reuse of the same Inbox key with conflicting cursor/digest metadata is rejected by the compact receipt without retaining the full original Fields payload.

## 9. Решение по текущему этапу

Следующий этап — согласовать общую Metadata Store архитектуру, определить нейтральные semantic operations и conformance tests, затем спроектировать Storage-only роль FLD и вынести durable state BIN-SYNC за storage boundary. Уже реализованные FLD primitives (ApplyRemoteBatch, apply journal, Outbox lease) следует переиспользовать и довести, а не переписывать. До появления единой транзакционной границы не подключать FLD к Node набором точечных callback-ов.


## 10. History-storage audit reference

The cross-layer code audit is maintained in [FLD-DB: Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md). It confirms that BIN-SYNC already shares one Outbox operation across peers and owns per-peer/per-author progress; FLD must not duplicate this progress model. The FLD APPLY payload duplication has been reduced with ApplyCommitCodec v3 and relay-only ApplyJournal materialization; remaining work is independent receipt/relay retention and bounded indexes.

This audit is an internal storage design follow-up. It does not alter the wire contract or the atomic acceptance requirements in this document. Further implementation must preserve the atomic transition and ACK-after-durable-acceptance guarantee while completing independent receipt/relay retention and bounded/rebuildable journal indexing.

## 11. Storage compaction alignment (2026-11-07)

The cross-repository phase plan is in [Sync Storage Compaction Roadmap](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-STORAGE-COMPACTION-ROADMAP.md); code-backed findings are in [Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md).

- APPLY WAL is authoritative until receipt and relay payload are durably materialized.
- Each incoming operation is encoded once; relay selection references operation indexes rather than repeating RelayFields payload.
- The durable batch receipt is metadata-only with respect to non-relay operation payload; it remains co-located with relay payload in ApplyJournal until independent GC is implemented.
- Relay payload contains only relay-selected immutable operations, including eligible LWW losers. Promotion is idempotent and durable before WAL truncation; reclaim only after a validated downstream safe watermark.
- Global ChangeId dedup is route-independent BIN-SYNC metadata; issuer-specific receipts are not a substitute.
- Local pending coalescing happens before the FLD Outbox freeze boundary. The Outbox itself remains immutable.
- Per-peer progress and ACK aggregation remain BIN-SYNC responsibilities; FLD receives only the safe aggregate local Outbox watermark.

These decisions do not weaken atomic APPLY or ACK-after-durable-acceptance. If FLD and BIN-SYNC metadata cannot share one atomic transaction, the recoverable apply protocol and idempotent sidecar promotion must be explicit and crash-tested. See roadmap §7.19 for the later synchronized-database provenance adapter and §7.20 for the controlling cross-repository implementation order.


## 9. v3.1 application draft horizon and physical-GC scanner boundary

The BIN-SYNC v3.1 coordinator now requires the host integration to implement \`IApplicationGcReferenceScanner\`; FLD itself must remain protocol-neutral and must not reference \`BinSync.Core\`. The adapter belongs in the layer that depends on both FLD and BIN-SYNC.

### 9.1. One application commit lock order

All operations that publish an OFR2 frontier, commit FULL_REPLACE, or physically compact receipts/ChangeId/tombstones must use one composed lease in this order:

1. BIN-SYNC coordinator gate;
2. Node's complete gate set;
3. FLD/WAL transaction or writer gate;
4. shared BIN-SYNC Storage transaction gate.

Release in reverse order. The host passes its WAL/FLD lease factory to \`StorageSyncCoordinator.AcquireApplicationCommitGuard(hostLeaseFactory)\`. For GC, pass \`() => coordinator.AcquireApplicationCommitGuard(hostLeaseFactory)\` as the \`acquireGcLease\` callback. For FULL_REPLACE, use the same composed callback as the receiver's commit-guard factory. `StorageFullReplaceReceiver` also requires the host scanner and refuses to switch while the host draft horizon or provider feed is non-zero. Do not acquire the shared Storage gate and then wait for the FLD/WAL gate: that reverses the order and can deadlock with a normal FLD commit.

### 9.2. Complete unpublished local draft horizon

The current FLD branch exposes this as `FldDatabase.ReadUnpublishedLocalDraftHorizon()` (the database instance is already bound to one namespace/partition). The later neutral adapter should expose it as `ReadUnpublishedLocalDraftHorizon(scopeId, localReplica, namespaceId)`. It must account for every durable local operation that has not yet been frozen into BIN-SYNC Metadata Store, including:

- FLD Outbox entries after \`OutboxLog.AckedThrough\`;
- WAL commits beyond \`OutboxLog.MaterializedThroughCommitSeq\` that have not yet been materialized into Outbox;
- any additional durable local-intent/source queue used by the host.

The scanner returns a conservative \`(Count, MaxHlcWall, MaxHlcLogical)\`. The WAL/Outbox ranges above are disjoint by the materialization watermark; do not count one WAL commit twice. \`CommitSeq\`, \`JournalSeq\` and \`OutboxSeq\` remain source-local coordinates and must never be copied into \`OriginSequence\`. A zero count is meaningful only while the composed host lease is held. A final-author report is rejected before provider epoch closure if either host drafts or provider-feed drafts remain.

### 9.3. Live references for physical reclamation

The scan object must implement all three reference checks:

- \`HasExternalReceiptReference\`: inspect durable apply/retry/relay state that still names (namespace, sender, transport epoch, author epoch, batch id);
- \`HasExternalChangeIdReference\`: inspect WAL, Outbox and any durable local/remote intent that still names the ChangeId;
- \`HasExternalRecordReference\`: inspect WAL, Outbox, pending apply and other durable application state for the RecordId or tombstone ChangeId.

Unknown/corrupt retained records are live references. Scanner exceptions fail closed. The scanner must be partition-scoped by (scopeId, localReplica, namespaceId) and must not infer namespace from an unrelated global cursor.

This is an integration contract, not an assertion that the FLD adapter already exists. Until the adapter, lock composition and cross-store crash tests are implemented, all physical GC feature flags remain false. The current FLD `FldReferenceSnapshot` already provides the source-side queries needed by the adapter: `UnpublishedLocalDraftHorizon`, `HasChangeIdReference`, `HasRecordReference`, and `HasReceiptReference`. It is built once by `OpenReferenceSnapshot()` while `AcquireCommitGuard()` is held and owns in-memory indexes only; the wrapper should dispose it with the BIN-SYNC scan lease.


### 9.4 Freeze-before-stamp crash recovery and ChangeId GC

The provider feed remains a live ChangeId reference until \`CommitPublishedOrigins\` has durably stamped the exact immutable \`OperationOrigin\` and the feed sequence is acknowledged. If a crash occurs after Metadata Store freeze but before provider stamping, retry resolves both fingerprint and origin from the durable ChangeLedger row using \`TryGetOutboundChangeIdentity\`; it must not allocate a new origin or require the outbound payload row to remain retained. ChangeId compaction also scans the provider's pending local feed and blocks every matching ChangeId. The external FLD snapshot alone is insufficient because the FLD Outbox may already have been ACKed after the provider accepted the entry.


### 9.5 Durable synchronized-FLD relay provenance map

BIN-SYNC's `BinSync.Storage.StorageRelayProvenanceMap` is the durable mapping primitive for the synchronized-FLD adapter. It is scoped by (scope, local replica) at construction and by (namespace, original author) per stream. The current pre-extraction FLD branch already exposes `FldDatabase.ReadRelayBatchPage(ReadOnlySpan<byte> authorNodeId, ulong afterJournalSeq, int limit)`, `FldDatabase.AcknowledgeRelayBatches(ReadOnlySpan<byte> authorNodeId, ulong safeThroughJournalSeq)`, `FldDatabase.AcquireCommitGuard()`, `FldDatabase.ReadUnpublishedLocalDraftHorizon()`, and `FldDatabase.OpenReferenceSnapshot()`. The neutral adapter should preserve these semantics after extraction. `ReadRelayBatchPage` scans whole ApplyJournal records, returns all matching relay fields for the requested original author, and advances `ScannedThroughJournalSeq` even across records with no matching author. ChangeId→OutboxSeq resolution remains a BIN-SYNC `Node.TryResolveOutboxSequences` operation, not an FLD lookup.

For each page returned by FLD `ReadRelayBatchPage(author, afterJournalSeq, limit)`. Pages end on complete JournalSeq boundaries: a single FLD journal transaction containing multiple ChangeIds cannot be split across pages, and `ScannedThroughJournalSeq` means every source journal through that boundary has been examined:

1. Materialize the page's unique `(author, ChangeId)` identities from each `RelayJournalBatch.Batch.RelayWrites`. Do not infer OutboxSeq from current field winners; the coordinator helper performs the durable Node-ledger resolution.
2. Preserve the original FLD `JournalSeq` for every resolved ChangeId. Do not substitute JournalSeq, CommitSeq or OutboxSeq for protocol `OriginSequence`.
3. Call `StorageSyncCoordinator.AppendRelayProvenancePage(map, author, sourceChanges, ScannedThroughJournalSeq)` while the same application commit guard is still held. The coordinator resolves every transient `(JournalSeq, ChangeId)` through `Node.TryResolveOutboxSequences`, builds only `(JournalSeq, resolved OutboxSeq)` entries, and calls the map. ChangeId is never copied into the map. If any ChangeId is no longer retained, the method returns false and writes neither mappings nor scan cursor. The complete page mapping, RPM3 mapping-set digest and scan cursor commit in one Storage transaction. Empty author pages still advance the cursor. On restart the map recomputes XOR(SHA-256(key || value)) over retained mapping rows through bounded prefix pages and fails closed if the checksum differs from the cursor.
4. Only after the mapping commit may the host open the corresponding BIN-SYNC outbound publication path. The adapter-level namespace gate must serialize this boundary against inbound relay publication and outbound batching.
5. Compute BIN-SYNC's per-author safe `OutboxSeq` with `Node.GetSafeOutboxAckedThrough`; translate it with `TranslateSafeOutboxSequence`. The first JournalSeq containing any OutboxSeq above the safe watermark is retained in full.
6. Call FLD `AcknowledgeRelayBatches(author, safeThroughJournalSeq)` and wait for its durable return. Only then call `CommitAfterFldRelayAck` to persist both translated ACK watermarks and prune mapping rows.

Concrete current-API shape (the adapter assembly remains deferred until FLD extraction):

```csharp
using BinSync;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;

using IDisposable commitGuard =
    coordinator.AcquireApplicationCommitGuard(database.AcquireCommitGuard);

RelayBatchPage page = database.ReadRelayBatchPage(author.ToArray(), afterJournalSeq, 1);
int changeCount = 0;
foreach (RelayJournalBatch batch in page.Batches)
    changeCount = checked(changeCount + batch.Batch.RelayWrites.Count);
if (changeCount > 4096)
    throw new InvalidDataException("One FLD JournalSeq group exceeds the current bounded RPM3 page limit.");

var sourceChanges = new StorageRelaySourceChange[changeCount];
int writeIndex = 0;
foreach (RelayJournalBatch batch in page.Batches)
{
    foreach (RemoteFieldWrite write in batch.Batch.RelayWrites)
        sourceChanges[writeIndex++] =
            new StorageRelaySourceChange(batch.JournalSeq, BatchId.From(write.ChangeId));
}

if (!coordinator.AppendRelayProvenancePage(
        map, author, sourceChanges, page.ScannedThroughJournalSeq))
{
    // A ChangeId is no longer retained in Node Outbox: do not send and do not
    // advance the FLD scan cursor. Recover the publication gap first.
    throw new InvalidOperationException("Relay provenance could not be resolved.");
}
```

Keep the application guard alive across the FLD page read and coordinator append. The guard acquires coordinator → Node → FLD commit gate → shared Storage gate in the documented order. The sample reads one whole FLD JournalSeq group per call and uses one bounded array of at most 4,096 transient source identities. A single oversized FLD group fails closed rather than being split across map commits; durable RPM3 rows contain only sequence pairs.


Crash behavior is intentionally asymmetric: a crash before mapping commit rereads the same FLD page; a crash after mapping commit reuses the durable mapping and cursor; a crash after FLD ACK but before map pruning replays the same idempotent FLD ACK and then prunes. Never prune the mapping before FLD confirms its relay ACK is durable.

The map's durable mapping key is ordered by big-endian OutboxSeq; each value carries only JournalSeq. ChangeId→OutboxSeq resolution remains in the global BIN-SYNC ChangeId ledger; no duplicate ChangeId is persisted here. Recovery reads the prefix in destination-stream order without a post-load sort, and safe-ACK translation uses lower-bound binary search over that ordered index (O(log N)). A safe OutboxSeq above the last durable mapping is rejected; the host must catch up the FLD source scan and ChangeId resolver before advancing the source ACK. If the safe OutboxSeq equals an already-positive durable ACK and no mappings remain, the map may advance through newly scanned empty author pages; a zero OutboxSeq never authorizes a positive FLD JournalSeq ACK. The map stores only sequence coordinates, not operation payloads, per-peer ACKs or OriginSequence. It is not itself the full FLD adapter: host code still owns the FLD commit guard, source-page read, ChangeId resolver, namespace publication gate, transport scheduling and ACK call. The generic Storage engine remains unaware of FLD and BIN-SYNC semantics.


### 9.6 Integration sequencing and two-DLL dependency

Section 9.5 is the target contract for synchronized-FLD mode; it is not permission to add a direct `Node ↔ FldDatabase` bridge against the current pre-extraction FLD project graph. FLD-DB's controlling roadmap §7.20 places this adapter after: (1) neutral semantic operations and Data Provider atomicity/conformance, (2) BIN-SYNC persistence refactoring behind the neutral boundary, (3) FLD's two-public-DLL Storage/Database extraction, and (4) the Storage-only Metadata Store adapter. The current FLD source tree still has separate Core/Engine/Manifest/Run/Wal projects and `FldDatabase` does not yet use `Fld1.Storage` as its persistence backend.

Keep the two database runtime DLLs exactly `Fld1.Storage.dll` and `Fld1.Database.dll`; adapters are optional integration packages outside those two database runtimes. Do not put either adapter in `Fld1.Storage`, do not make Storage depend on BinSync.Core, and do not create a premature NuGet bridge. The BIN-SYNC `StorageRelayProvenanceMap` is an independent durable primitive and does not change this dependency order.
