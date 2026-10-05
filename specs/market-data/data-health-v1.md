# DataHealth v1 — transition proposal

Status: **PROPOSED**. SPEC-001 / DESIGN REVIEW.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Основания: [events](events-v1.md), [types](../domain/types-v1.md), [WAL](../recording/wal-v1.md), [matrix](../domain/test-matrix-v1.md).
Это проект чистой reference model, не production order book, supervisor, queues или resync algorithm.

## 1. Четыре оси, не connected=true

| Ось | Область | Состояния |
|---|---|---|
| Transport | ConnectionId + ConnectionEpoch | Unknown, Up, Down |
| Freshness | StreamId + полный текущий tag | Unknown, Fresh, QuietVerified, Stale |
| BookValidity | BookRef + полный текущий tag | NoSnapshot, Warming, Usable, Invalid(reason) |
| Recording | ArchiveId / capture session | Unknown, Healthy, Degraded(reason), Failed(reason) |

Trades не имеют BookValidity; их consumers требуют собственные current-tag freshness/evidence. Fresh trades не восстанавливают book и наоборот.
State также содержит current SpecRef/tag, active ConfigVersion/NormalizerVersion, snapshot_anchor RawFrameId, verification profile/evidence, warm-up counters, последний valid-data monotonic sample, evaluation clock scope и recording watermarks.
Никакие unknown fields не принимают положительные defaults.

## 2. Versioned policy inputs

ConfigDefinition записывает version, normalizer version, происхождение (`Engineering`, `Synthetic`, `SourceVerified`) и evidence reference.
HealthPolicy содержит `silence_rule`, Option<freshness_deadline_ns>, Option<warmup_min_updates>, Option<warmup_min_elapsed_ns>, `allow_quiet_with_proof`, `require_two_sided_snapshot`, `recording_gate`.
Все durations — u64 nanoseconds; update counts — u32; freshness deadline, если есть, >0.
Для warm-up требуется хотя бы один заданный threshold; explicit 0 допустим только как явно записанный engineering/synthetic choice и всё равно требует отдельного WarmupEvidence.
Нет универсальных timeout/warm-up чисел. Конфигурация с отсутствующим обязательным policy evidence => UnknownConfiguration/MissingPolicyEvidence, не usable.
Engineering parameters не превращаются в проверенные Bitget thresholds. Production snapshot/delta verification profile и quiet-market proof остаются **BLOCKED_BY_MD_001**.

SilenceRule: UnknownOnSilence=1; StaleAfterDeadline=2. При 2 deadline обязателен.
QuietVerified требует allow_quiet_with_proof=true и профиль, способный доказать unchanged/quiet stream; само отсутствие сообщений такого proof не даёт.
RecordingGate: Written=1, Flushed=2, Durable=3. Предлагаемый безопасный профиль для зависимого использования — Durable; Buffered capture допускается с явно ослабленным gate, не выдавая его за durable recording.
`require_two_sided_snapshot` — инженерная policy проверки структурной пригодности, не знание о реальной глубине/состоянии feed.
Policy или normalizer change сбрасывает evidence/warm-up и требует новой проверки текущего snapshot; новая конфигурация не наследует usable молча.

## 3. Чистая модель и приоритет

`step(state, recorded_input, recorded_definitions) -> (new_state, diagnostics)` не делает I/O и не читает live clock.
Входы вызываются в порядке [EventCursor](events-v1.md). Повтор идентичной delivery — no-op по EventId; conflict — ошибка.
Порядок обработки: проверить schema/definitions/identity -> owner и epochs -> config -> причинные references -> transition/evidence -> вычислить readiness.
Невалидный current-stream critical input создаёт явную diagnostic/loss причину и fail-closed, не тихий skip; old-tag input сохраняется только как diagnostic и не делает текущую generation хуже/лучше без отдельного доказанного current gap.
Неизвестный verification profile возвращает diagnostic BLOCKED_BY_MD_001 для реального feed и не переводит книгу в Warming/Usable.

Время evaluation = max предыдущего и уже записанных monotonic samples одной clock domain. На recorded TimerFired/любом новом sample проверяется age свежести.
Поздний кадр с меньшим receive_ns не откатывает evaluation time и не делает себя свежим автоматически.
Если clock scope несовместим, duration не вычисляется, freshness=Unknown и readiness=false.
При отсутствии новых inputs истечение должно быть представлено TimerFired; query не читает wall clock. Отсутствующий timer нельзя заменить выдуманным фактом о времени.

## 4. Transition table

T/F/B/R означают четыре оси. Неуказанные оси сохраняются; все доказательства обязаны относиться к текущим identity/tag/config и causal prefix.

| Input / guard | Переход | Что запрещено |
|---|---|---|
| StreamDefinition впервые | T=Unknown; F=Unknown; B=NoSnapshot (book); R остаётся archive state | default connected/usable |
| Transport Up current connection | T=Up | не меняет F/B; socket не равен snapshot |
| Heartbeat / transport observation | обновляет только transport evidence | heartbeat не обновляет last_valid_data_ns |
| Transport Down | T=Down; F=Unknown; B=Invalid(TransportDown) у связанных books; clear anchor/warm-up | восстановление B одним Up |
| Verified current-tag snapshot + применимый profile + структурные guards | B=Warming; новый anchor; counters=0; записать valid-data time; F пересчитать по policy/evaluation time | reuse старого anchor, bypass warm-up, UNKNOWN profile |
| Snapshot без verification / неподходящий tag | diagnostic, usable не устанавливается; current invalid input fail-closed | структурно корректный snapshot не равен verified resync |
| Verified current-tag delta с существующим anchor, B=Warming/Usable | пересчитать F; checked warm-up update count; B сохраняется до отдельного WarmupEvidence | delta без snapshot не делает книгу valid |
| Delta при NoSnapshot/Invalid | B не восстанавливается; diagnostic NeedsSnapshot | implicit REST stitching |
| WarmupEvidence + anchor/tag/config/profile совпали + thresholds действительно достигнуты | Warming -> Usable; readiness затем проверяет остальные оси | witness count/elapsed не принимаются без сверки с reference state |
| GAP critical / overflow / continuity validation failure для текущего scope | B=Invalid(reason), F=Unknown, clear anchor/counters; recording loss => R=Degraded | следующий delta не снимает invalidity |
| EpochAdvance connection | новый connection epoch; T=Unknown; F=Unknown/B=NoSnapshot для всех связанных streams; clear evidence | старый snapshot из прежнего composite tag |
| EpochAdvance subscription/book | сменить только owner epoch; F=Unknown/B=NoSnapshot только зависимых streams/books | глобальный reset несвязанных инструментов |
| SpecActivate / ConfigDefinition с новой действующей version | invalidation зависимой B, F=Unknown, clear warm-up; новая SpecRef/config фиксируется | применить старые ticks/evidence к новой версии |
| Old-epoch input / old WarmupEvidence | diagnostic EpochMismatch, current generation не восстанавливается | заменить current tag входным old tag |
| Timer / silence, политика не доказывает отказ | F=Unknown после утраты freshness evidence; T/B структурно не обязаны меняться | объявить disconnect или automatic resync из тишины |
| Timer, age > recorded deadline, StaleAfterDeadline | F=Stale; readiness=false | heartbeat не отменяет stale |
| Explicit applicable quiet proof, policy разрешает | F=QuietVerified в границах proof и policy validity | тишина сама не proof |
| RecordingEvidence Healthy, известен нужный watermark | R=Healthy; заново вычислить guard | не лечит B после GAP |
| Recording fault / невозможно записать даже GAP | R=Failed; readiness=false; completion=Unknown/Incomplete вне WAL, если запись невозможна | обещать durable GAP, которого нет |
| Restart/new capture session | новый ArchiveId/clock scope; T/F/R=Unknown, B=NoSnapshot | перенос elapsed time или usable из старой session |

TransportDown и source gap могут быть independent: сохранённый socket Up не отменяет Invalid(Gap).
Записанный GAP делает потерю наблюдаемой, но не восстанавливает утраченное событие. Healthy recorder после recovery также не заменяет market resync.

## 5. Readiness predicate

Для book-dependent data:

```text
usable_data = known_current_definitions
           && identity/spec/tag/config точно ожидаемые
           && transport == Up
           && (freshness == Fresh || applicable_policy_allows(QuietVerified))
           && book == Usable
           && current_anchor_verified && warmup_witness_valid
capture_usable = usable_data && recording == Healthy
             && selected_watermark >= causal_input_frontier
```

`causal_input_frontier` — последний RawInput/control input, от которого зависит используемое market state, а не произвольный будущий watermark. Самоподтверждающий durability ack запрещён: RecordingEvidence может ссылаться только на более ранний prefix, а не объявлять себя durable.
В replay RecordingEvidence — записанный input о тогдашнем наблюдении, **не доказательство физической durability носителя после crash**. Контракт публикации/ack storage и его OS tests остаются REC-001.
Структурная BookValidity и capture_usable хранятся/сообщаются отдельно. Можно иметь корректную книгу при отказавшей записи, но зависимая capture/shadow работа fail-closed.
`usable_data`/`capture_usable` — пригодность данных, **не торговое разрешение**; live execution не существует в этой задаче.

## 6. Обязательные negative assertions

NoSnapshot + heartbeat != usable. Old-tag snapshot + warm-up != usable. Current snapshot + missing policy != usable.
GAP/overflow/epoch change из Usable => false до нового verified snapshot и нового warm-up.
Normal book evidence не переводит RPI book в usable. Trades freshness не заменяет book freshness.
Unknown aggressor/join сохраняются независимо от health; quality не делает unknown известным.
Любые externally supplied count/elapsed в witness сверяются с записанным anchor, checked counters и monotonic difference; ложное evidence не является командой force-ready.

Review D4: thresholds/proof provenance, quiet-market policy, minimum guards и reset scope.
Чистая исполнимая модель и проверки этой таблицы — **NOT_IMPLEMENTED / NOT_RUN до явного design approval в PR**. QA-001 (#6) требуется независимый negative review; worker не принимает собственный контракт.
