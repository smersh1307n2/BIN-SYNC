# Контракт интеграции BIN-SYNC ↔ FLD-DB

**Статус:** специализированный проект контракта интеграции FLD-DB как синхронизируемой базы.  
**Общая архитектура:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
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
3. FLD `OutboxSeq` — durable монотонный cursor локального Outbox внутри namespace. Он напрямую отображается на BIN-SYNC stream `(LocalAuthor, namespaceId)`.
4. Не добавлять второй author sequence в FLD: при одном local author на database его глобальный namespace OutboxSeq уже является sequence этого author stream.
5. FLD `CommitSeq` — WAL durability/commit sequence, **не** BIN-SYNC stream cursor.
6. FLD `MaterializedThroughCommitSeq`, `LastSealedCommitSeq`, `CertifiedWatermark` и BIN-SYNC `SafeOutboxSeq` — разные шкалы. Их нельзя подменять друг другом.
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
| `ReadRelayBatch(author, afterSeq, limit, lease)` | Возвращает сохранённые relay-eligible mutations, включая операции, проигравшие LWW |
| `AcknowledgeRelay(author, safeThroughSeq, lease)` | Удаляет relay prefix только после подтверждения всеми interested peers |

### 3.1 RemoteBatchCommit

Граница `ApplyRemoteBatch` принимает не только список победивших полей. В commit DTO должны быть доступны:

- `SenderPeer`, `SenderEpoch`, `NamespaceId`;
- `BatchId`, `CursorStart`, `CursorEnd`, `DurableAuthor`;
- `PreviousBatchDigest`, `BatchDigest`, `RecordsRoot` и остальные данные, необходимые для восстановления chain/inbox state;
- полный набор decoded mutations: `RecordId`, `FieldId`, operation kind, value, HLC, `AuthorNodeId`, `KeyEpoch`, обязательный `ChangeId`;
- opaque `ProtocolStateDelta`, сформированный BIN-SYNC: inbox cursor/chain update, SeenChanges delta, relay journal delta и необходимые provenance/session данные.

FLD хранит opaque protocol extension как часть той же durable transaction, не разбирая wire semantics. BIN-SYNC остаётся владельцем кодирования и интерпретации extension.

### 3.2 Семантика ApplyRemoteBatch

`ApplyRemoteBatch` обязана выполнять атомарно:

1. Проверить namespace и размеры идентификаторов.
2. Проверить idempotency по входящему batch и ChangeId согласно ключам BIN-SYNC.
3. Применить к field state только LWW-победителей, используя канонический comparator.
4. Сохранить все новые принятые relay-eligible mutations, даже если они проиграли LWW.
5. Сохранить inbox/cursor/digest-chain/SeenChanges и relay journal delta.
6. Записать WAL commit и выполнить требуемый durability flush.
7. Только после durable commit опубликовать состояние и вернуть receipt.

**Успешный return означает, что можно отправлять ACK.** При ошибке ни field state, ни inbox/cursor, ни SeenChanges, ни relay journal не должны частично публиковаться; ACK не отправляется.

Повтор того же batch после lost ACK возвращает duplicate receipt с тем же `CursorEnd`/author result и не создаёт повторных field mutations или relay entries. Повтор ChangeId через другого peer не должен повторно применять/relay mutation, но входящий peer cursor должен быть корректно обработан.

Remote mutation не создаёт запись в **локальном** FLD Outbox. Self-echo уже локально authored mutation не ставится в relay повторно.

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

Relay retention/compaction рассчитывается по ACK всех interested peers. LWW loser остаётся relay-eligible: локальное значение может быть новее, но операция всё равно нужна другим узлам для детерминированной сходимости и SeenChanges provenance.

SeenChanges provenance dedup — отдельное состояние BIN-SYNC, не FLD relay journal. Текущая wire specification задаёт channel-scoped key через senderPeer/namespace/ChangeId, но не задаёт safe GC watermark/expiry для этого набора. До появления нормативного правила BIN-SYNC обязан удерживать exact dedup state; FLD не должен самостоятельно удалять opaque ProtocolStateDelta по TTL или по relay ACK.

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

Code/API baseline reviewed: FLD-DB `fix/crash-consistency-outbox` commit `331206106f7d7d645dabe849957278a3a5a8b008`; BIN-SYNC runtime/spec baseline commit `f31e043cddaf269df313e5201fc45f75b06c9ec8`. Later commits in these branches during this documentation pass update architecture documents only:

| Текущее API / состояние (проверено на FLD fix/crash-consistency-outbox) | Статус / оставшийся разрыв |
|---|---|
| FldDatabase.ApplyRemote(ReadOnlySpan<RemoteFieldWrite>) | Оставлен как obsolete volatile merge-only compatibility API; не durable и запрещён для BIN-SYNC receive |
| RemoteFieldWrite.ChangeId | Теперь обязательный 16-byte nonzero идентификатор; конструктор валидирует длину и ненулевое значение |
| FldDatabase.ApplyRemoteBatch(RemoteBatchCommit) | Реализован durable receive boundary: проверяет duplicate batch, формирует WAL APPLY commit, сохраняет protocol delta и relay subset, flush-ит WAL/apply journal, затем публикует LWW winners; remote writes не попадают в local Outbox |
| RemoteBatchCommit.ProtocolStateDelta | Передаётся и сохраняется opaque; FLD не интерпретирует его. BIN-SYNC ещё должен обеспечить корректное формирование, применение/recovery и атомарное согласование этого delta со своим runtime state |
| ApplyJournal / ReadRelayBatches | Durable append-only apply journal и чтение relay-eligible операций реализованы; отдельный safe relay ACK/retention/compaction boundary в публичном API ещё не подтверждён |
| AcquireOutboxLease / ReleaseOutboxLease / VisitOutboxBatch / AcknowledgeOutbox(..., lease) | Durable fenced lease, borrowed-span enumeration и fenced ACK реализованы |
| GetOutboxHighWatermark | Отдельный high watermark реализован; WAL CommitSeq и OutboxSeq остаются разными шкалами |
| FLD Storage-only Metadata Store | Не реализован как отдельная семантическая роль/API; текущий FldDatabase по-прежнему создаёт синхронизируемую модель данных с LocalAuthor/HLC/Outbox |
| BIN-SYNC Node | Текущие Storage/Outbox/PeerStates/SeenChanges и другие протокольные структуры остаются внутренними коллекциями Node; общего Metadata Store boundary пока нет |

**Запрет до появления целевого API:** не использовать последовательность `FldDatabase.ApplyRemote(...); FldDatabase.Commit();` как реализацию BIN-SYNC receive. Она не обеспечивает atomic batch/idempotency/relay semantics и может добавить remote author entries в локальный Outbox.

## 8. Приёмочные тесты контракта

1. Local writes: restart сохраняет OutboxSeq, ChangeId, HLC и payload без renumbering.
2. Remote commit: field winners, inbox cursor, SeenChanges и relay journal появляются атомарно.
3. Crash/lost ACK: повтор того же BatchId возвращает duplicate receipt; нет повторного применения и relay append.
4. Cross-peer duplicate ChangeId: поле и relay не дублируются; cursor каждого входящего peer корректен.
5. LWW loser: field state не меняется, но новая операция остаётся relay-eligible.
6. Remote apply → commit/reopen: remote operation не появляется в local Outbox.
7. Safe ACK: один отставший/неизвестный peer удерживает local/relay prefix; stale lease token отклоняется.
8. Compaction/restart: OutboxSeq и relay cursors сохраняют continuity; новый peer получает корректный gap-normal cursor.
9. Snapshot under concurrent commit/checkpoint: один стабильный view, без пропусков/дубликатов и без use-after-reclaim.
10. Lifecycle/tombstone/KeyEpoch: digest и LWW результаты совпадают с BIN-SYNC comparator и FLD canonical encoding.

## 9. Решение по текущему этапу

Следующий этап — согласовать общую Metadata Store архитектуру, определить нейтральные semantic operations и conformance tests, затем спроектировать Storage-only роль FLD и вынести durable state BIN-SYNC за storage boundary. Уже реализованные FLD primitives (ApplyRemoteBatch, apply journal, Outbox lease) следует переиспользовать и довести, а не переписывать. До появления единой транзакционной границы не подключать FLD к Node набором точечных callback-ов.
