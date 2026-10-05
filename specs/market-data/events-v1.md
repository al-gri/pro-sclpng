# Events, identity, epochs and causal order v1 — проект

Status: **PROPOSED**. SPEC-001 / DESIGN REVIEW.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Связанные документы: [types](../domain/types-v1.md), [DataHealth](data-health-v1.md), [WAL](../recording/wal-v1.md), [matrix](../domain/test-matrix-v1.md), [ADR-0002](../../docs/adr/0002-domain-event-wal-contracts.md).
Это логический DTO-контракт, не принятый Rust layout/JSON schema. На КП1 нет adapter или reducer implementation.

## 1. Область идентичности

ArchiveId отличает один журнал. CaptureSessionId + ClockId определяют одну записанную monotonic clock domain.
Предлагаемый WAL v1 содержит одну capture session на архив; process restart создаёт новый ArchiveId/CaptureSessionId. Связь с прежним архивом не означает непрерывный monotonic clock.
ID поступают при capture setup и сохраняются в ArchiveStart; replay не вызывает clock/random для их создания.
Коллизия ArchiveId с отличающимся содержимым — ArchiveIdentityConflict, не новый capture с переиспользованными IDs.

`RawFrameId = (ArchiveId, RecordNo)` только для RawInput record.
`EventId = (ArchiveId, cause_record_no, sub_event_index, normalizer_version)`.
`EventCursor = (cause_record_no, sub_event_index)` упорядочивается лексикографически в одном ArchiveId и одной выбранной normalization revision.
SubEventIndex начинается с 0 и идёт без пропусков для результатов одного cause record.
Два события одного кадра, например trade[0] и trade[1], имеют разные EventId.
Версия normalizer — часть идентичности, чтобы изменение схемы декодирования не создало тот же ID с другим смыслом. ConfigVersion отдельно присутствует в envelope; повторное использование config version с иными bytes запрещено.

EventId стабилен при двух replay **того же архива с записанной конфигурацией и normalizer version**.
Новый live capture не обязан повторять IDs. EventId **не** exchange dedup key; повторная отправка биржей в другом raw frame имеет другой RawFrameId. Дедупликация exchange trade/sequence — только по проверенному MD-001 profile.
Повторная доставка того же EventId и идентичных canonical fields — idempotent no-op; отличающийся payload/metadata при том же ID => IdentityConflict. При чтении самого WAL повтор RecordNo является ошибкой порядка, а не допустимым повторным frame.
Новая экспериментальная конфигурация не переопределяет canonical replay: это отдельная явно именованная research revision, не результат v1 parity.

## 2. Envelope

| Поле | Тип / требование |
|---|---|
| schema_version | u16, 1 в этом proposal; unsupported => ошибка |
| event_id | EventId выше, вычисляется из записанных данных |
| capture_session_id, clock_id | обязательны, совпадают с ArchiveStart |
| channel, stream_id | зарегистрированный source stream; control может иметь явный scope вместо stream |
| instrument, spec_version | SpecRef; обязателен для snapshot/update/trade; control перечисляет затронутые identity или scope |
| connection_id, connection_epoch | текущая зарегистрированная connection generation для stream |
| subscription_epoch | текущая generation данного StreamId |
| book_id, book_epoch | Some для book events, None для trades; никогда borrowed из соседней книги |
| source_timestamp | Unknown либо Known(value:i64, unit, origin:Token32); unit Seconds=1, Millis=2, Micros=3, Nanos=4 |
| local_receive_unix_ns | i64 Unix nanoseconds, наблюдение локального времени, не источник ordering |
| local_receive_monotonic | (CaptureSessionId, ClockId, ns:u64) |
| ingest_order / cause_record_no | RecordNo записанного input, не exchange sequence |
| available_at | EventCursor события, включая sub-index |
| as_of | EventCursor максимальной причинной предпосылки; <= available_at |
| config_version, normalizer_version, record_schema_version | соответствуют записанным определениям и cause input |
| raw_input_ref | Some RawFrameId для market events; None для непосредственно записанного control input |
| evidence_refs | отсортированные уникальные ссылки на предшествующие/текущий inputs, не на будущее |

Для snapshot/update/trade cause record есть RawInput, и raw_input_ref указывает на него; его stream/spec/epochs должны совпасть с envelope.
Для control cause record — соответствующий Control/ConfigDefinition/Gap record; связь с прежним snapshot задаётся отдельным evidence ref.
`as_of` без предпосылок у непосредственно нормализованного input равен available_at; для подтверждения прежних данных указывает на существующий causal basis, но available_at остаётся текущим input.
Нельзя делать событие доступным до записи причины его подтверждения. Все references должны уже разрешаться; ссылка на тот же cause допустима только на уже доступный sub-event либо непосредственно raw input, не на будущий sub-index.
Для reference на raw record используется отдельный RawFrameId, не вымышленный EventCursor.

Source timestamp Unknown сохраняет raw bytes через provenance, а не превращается в 0 или local time. Known origin и units требуют profile evidence; нельзя угадывать ms/ns по числу цифр.
Checked перевод единиц возможен отдельно; overflow — ошибка. Даже валидное exchange time не подтверждает cross-channel causality.
`aggressor = Unknown | Buy | Sell`; `trade_book_link = Unknown | Proven(reference, evidence)`.
Отсутствие доказательства не допускает inferred Buy/Sell или Proven по соседству timestamps. RPI attribute может быть Unknown/Yes/No независимо от aggressor.

## 3. Единственный авторитетный порядок

RecordNo присваивается single-writer при выбранном admission ordering всех raw и control inputs. Сначала сохраняется этот выбор, затем consumers используют его.
При конкурирующих потоках выбор scheduler/очередей является частью записанного результата; exchange_ts его не заменяет. Это **не утверждение общего биржевого порядка** и не обещание восстановить незаписанное физическое межпоточное arrival ordering.
CaptureAttemptNo ведётся отдельно для каждого stream до потенциальной потери в очереди и служит диагностикой GAP; он не заменяет RecordNo.
Raw socket/message boundaries сохраняются: RawInput — полный входной source message bytes, не уже разобранный trade. Fragment reassembly/profile verification остаются в MD-001/REC-001.

Например (RecordNo=10, exchange_ts=200), затем (11,100) => replay 10 -> 11.
Равные и отсутствующие exchange timestamps разрешены. Сортировка по ним запрещена.
При нескольких events кадра decoder profile фиксирует порядок sub-events; нельзя опираться на Rust HashMap iteration. Для synthetic array profile — порядок массива; реальный Bitget profile **BLOCKED_BY_MD_001**.
Для одного versioned decoder повторный запуск обязан дать тот же число/порядок outputs. Непрерывность sub-index проверяется вместе с числом результатов cause input; перестановка или пропуск => SubEventOrderError.

Receive monotonic samples сравнимы только внутри одинаковых CaptureSessionId/ClockId; разный scope => IncomparableClock.
Record ordering может отличаться от порядка исходных receive samples из-за admission arbitration; уменьшение receive timestamp между записанными потоками само по себе не меняет порядок и не является доказанным reset.
Для health evaluation применяется записанное продвижение времени: максимум уже наблюдённых receive/timer samples в этой clock domain; поздний input не откатывает этот максимум. При отсутствии новых inputs timer должен быть отдельным записанным событием. Никакого SystemTime/Instant::now внутри replay/reducer.
Unix clock jump не меняет RecordNo, EventId или прошедшую monotonic duration. Межархивное сравнение monotonic запрещено без отдельного принятого mapping contract; ссылка previous_archive такого mapping не даёт.

## 4. Epoch rules

Текущий input tag = (SpecVersion, ConnectionEpoch, SubscriptionEpoch, Option<BookEpoch>).
Сравнение generation требует совпадения owner identity, а не только числа epoch.
`EpochAdvance(scope, owner_id, expected, next, reason)` требует expected=current, next>expected и checked range; rollback/reuse запрещены.
Connection advance инвалидирует все связанные streams; subscription advance — только свой StreamId; book advance — только свой BookRef. Соседние инструменты не инвалидируются без общей затронутой зависимости.
Не все числовые epochs обязаны расти одновременно: полный composite tag меняется при любой затронутой generation. Старый snapshot/anchor нельзя переносить в новый composite tag.
SpecActivate ожидает прежнюю версию, ссылается на заранее записанную новую версию **того же** instrument и инвалидирует все зависимые состояния.
Raw inputs старой generation могут сохраняться для диагностики, но не обновляют текущую книгу, freshness или warm-up. Future/unregistered generation также не принимается молча.
Восстановление требует нового verified current-tag snapshot и нового warm-up. Heartbeat и произвольный ResetComplete этого не заменяют.

## 5. Минимальные market events

| Event | Поля и чистая структурная проверка |
|---|---|
| BookSnapshot | book ref/tag; ordered sides/levels; PriceTicks>0, QuantitySteps>0; уникальный (side,price); bids descending/asks ascending; empty side допустима структурно, usable решается отдельно |
| BookUpdate | book ref/tag; упорядоченный список SetLevel(side,price,qty>0) / DeleteLevel(side,price); повтор (side,price) внутри одного update отклоняется в v1 вместо скрытого last-write-wins |
| Trade | PriceTicks>0, QuantitySteps>0, aggressor и RPI attribute с Unknown, опциональная source trade identity и trade/book link |

Side: Bid=1/Ask=2 для level, не aggressor. Source sequence token сохраняется опционально как opaque Token128/evidence, но не сравнивается generic contract как подтверждённый contiguous u64.
Structural acceptance snapshot не означает verified sequence/resync. Proof profile отдельно утверждает применимость snapshot/delta; REST/WS stitching без такого evidence запрещён.
Snapshot/update максимум 4096 entries в synthetic generic v1 validator — engineering safety bound, не заявленная глубина Bitget. Превышение => EventTooLarge, без coalescing. Предел требует review D6.

## 6. Записанные внешние inputs

ArchiveStart/InstrumentSpec/StreamDefinition/ConfigDefinition фиксируют metadata, identities, clock и versions.
RawInput сохраняет source bytes. Control содержит TimerFired, TransportObservation, EpochAdvance, SpecActivate, VerificationEvidence, WarmupEvidence, FreshnessEvidence, RecordingEvidence.
GAP — отдельный input с явным scope/reason/unknown count. Configuration activation действует со следующего RecordNo, а не retroactively.
Evidence records — результаты внешних проверок с provenance. Сам факт наличия слова Verified не доказывает корректность биржевого алгоритма; unknown/unavailable profile не допускается к usable.
Raw/control являются authoritative replay input; v1 WAL **не хранит отдельный normalized-event payload**. Normalized DTO воспроизводится из raw, control, recorded config и выбранной normalizer version. Отдельный persisted projection требует нового schema/ADR, не implicit encoding Rust struct.

Предлагаемые ошибки: UnknownDefinition, IdentityConflict, IdentityMismatch, SpecMismatch, EpochMismatch, EpochRollback, RecordOrderError, SubEventOrderError, FutureCausalReference, MissingRawInput, UnknownConfiguration, IncomparableClock, UnsupportedSchema, EventTooLarge, MissingVerificationProfile.
Все они проверяются чистыми functions/models после design approval; pipeline, queue и replay engine здесь не реализуются.

Review D3: включение normalizer version в EventId, односессионный архив с новым ID после restart, authoritative raw/control-only WAL и правила available_at/as_of. Review D4: ownership/fan-out epoch transitions и evidence, которые MD-001 должен предоставить.
