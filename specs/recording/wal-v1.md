# WAL v1 — byte, recovery and completion proposal

Status: **PROPOSED**. SPEC-001 / DESIGN REVIEW.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Связанные контракты: [types](../domain/types-v1.md), [events](../market-data/events-v1.md), [DataHealth](../market-data/data-health-v1.md), [matrix](../domain/test-matrix-v1.md), [ADR-0002](../../docs/adr/0002-domain-event-wal-contracts.md).
Это проект wire contract. Production recorder, файловый I/O, recovery engine и platform sync здесь **не реализуются**. После design approval допустим только memory-only reference codec в domain tests/support.

## 1. Область v1

Один ArchiveId содержит одну CaptureSessionId и ClockId, последовательность segments и один authoritative порядок RecordNo. Process restart создаёт новый архив и clock scope; прежний архив не дописывается. Опциональная previous_archive ссылка не доказывает непрерывность данных или часов.
Записываются raw source messages, metadata, configuration и control inputs. Отдельного normalized-event record kind в v1 нет: [EventId и DTO](../market-data/events-v1.md) воспроизводятся из этих inputs и записанной normalization revision. Не сериализовать Rust structs, usize, native endian, float или случайный порядок HashMap.
Все значения, лимиты и tags ниже — инженерное предложение, **не параметры Bitget**.

## 2. Frame layout

Каждый frame: **header[32] || payload[L] || crc32[4]**, без padding/alignment bytes. Все многобайтовые integers little-endian. Offsets ниже от начала frame, интервалы полуоткрытые.

| Offset | Bytes | Поле | Правило |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `PSRW`, hex `50 53 52 57` |
| 4 | 2 | frame_version | u16, строго 1 |
| 6 | 2 | record_schema_version | u16, строго 1 |
| 8 | 2 | record_kind | u16, известный tag из раздела 4 |
| 10 | 2 | flags | u16, 0 в v1 |
| 12 | 4 | payload_len L | u32; число только payload bytes |
| 16 | 8 | record_no | u64, 1..=MAX; плотный возрастающий порядок всего архива |
| 24 | 4 | segment_no | u32, начинается с 0, возрастает на 1 при rotation |
| 28 | 4 | reserved | u32, 0 |
| 32 | L | payload | точная canonical schema своего kind |
| 32+L | 4 | checksum | u32 LE; CRC header || payload, без trailer |

`MAX_PAYLOAD = 1_048_576`; `frame_len = checked_add(36, L)`, максимум **1_048_612 bytes**.
Перед allocation: прочитать фиксированные 32 bytes; проверить magic/version/kind/flags/reserved и bound L; checked-вычислить frame_len, absolute_offset + frame_len и преобразование в usize; сравнить с доступным validated slice/остатком. Непроверенная длина не передаётся в reserve/resize/allocation.
Variable-length поля затем ограничиваются оставшимися payload bytes и собственными caps; произведение count*minimum_entry_size проверяется до выделения памяти. Frame cap не оправдывает allocation по ложному вложенному count.
Ненулевые flags/reserved, trailing payload bytes, неоднозначные enum tags и неканоничные decimals отклоняются. У формата нет режима «пропустить неизвестное и продолжить».

### Checksum

CRC-32/ISO-HDLC (IEEE): width=32, polynomial=0x04C11DB7, reflected polynomial=0xEDB88320, init=0xFFFFFFFF, refin=true, refout=true, xorout=0xFFFFFFFF. Reflected алгоритм обрабатывает bytes в сохранённом порядке и восемь младших битов каждого byte; final complement только один раз.
Покрытие: ровно bytes `[0,32+L)` данного frame, включая magic, versions, length, RecordNo и SegmentNo. Trailer содержит полученное значение little-endian.
Для streaming prefix CRC state продолжается между chunks без промежуточного final complement; итоговый xor применяется при получении значения.
Primary reference/independent method: [RFC 1952, section 8, sample CRC code](https://www.rfc-editor.org/rfc/rfc1952#section-8). CRC — проверка случайной порчи, **не аутентификация, не collision-resistant hash и не доказательство отсутствия malicious edits**.

## 3. Canonical payload primitives

Fields конкатенируются в перечисленном порядке, без implicit fields и padding.
`Opt<T>` = u8 tag 0 без последующих bytes либо tag 1 + T; другой tag ошибочен. `bool` = ровно u8 0/1.
`TokenN` = u8 length + length ASCII bytes; 1..N, алфавит из [types](../domain/types-v1.md), N<=128.
`ExactDecimal` = u128 coefficient + u8 scale в canonical форме types-v1; increments/multiplier положительны.
`InstrumentRef` = venue:Token32, market_kind:u8, product_namespace:Token32, native_symbol:Token64.
`EpochTag` = spec_version:u32, connection_epoch:u64, subscription_epoch:u64, book_epoch:Opt<u64>. Все присутствующие значения положительны; book epoch Some только для book stream.
Raw/event references в пределах архива используют RecordNo; ArchiveId восстанавливается из ArchiveStart, не из текущего процесса. Ссылка на неизвестный или будущий record ошибочна.

Kinds **2..7** начинаются с общего **Context[24]**:
`local_receive_unix_ns:i64, local_receive_monotonic_ns:u64, config_version:u32, normalizer_version:u32`.
CaptureSessionId/ClockId наследуются из ArchiveStart. Context показывает действующую **до обработки input** конфигурацию. До первой ConfigDefinition только administrative metadata/definition records могут использовать (config,normalizer)=(0,0); смешанная пара 0/nonzero запрещена. RawInput, Control и Gap требуют активную ненулевую конфигурацию.
Definitions доступны до references на них. У ConfigDefinition новый config действует со следующего RecordNo; старый Context не делает изменение retroactive. Никаких неявных default metadata/config.
Определения version/identity immutable. Повтор определения с тем же ключом в журнале отклоняется, даже если bytes совпали; idempotent transport redelivery уже прочитанного события — отдельный consumer contract.

## 4. Record kinds и точные тела

### 1 — ArchiveStart

Без Context: `archive_id:[u8;16], capture_session_id:[u8;16], clock_id:u32, durability_mode:u8, previous_archive:Opt<[u8;16]>`.
Оба собственных ID не все нули; clock_id>0; previous_archive, если есть, не все нули и не равен своему ArchiveId. mode: Buffered=1, GroupSynced=2, SyncBeforePublish=3.
Только RecordNo=1, SegmentNo=0, offset=0 первого segment. Его отсутствие — NoArchive, не архив с guessed identity.
Durability mode immutable в этом архиве. Будущая смена mode требует нового архива или отдельной schema revision.

### 2 — InstrumentSpec

После Context: `instrument_slot:u32, spec_version:u32, InstrumentRef, price_quote_unit:Token32, price_basis_unit:Token32, quantity_unit:Token32, base_asset:Token32, price_increment:ExactDecimal, quantity_increment:ExactDecimal, quantity_to_base_multiplier:Opt<ExactDecimal>, provenance:Token128`.
Slot связывается с одной identity навсегда; новая spec version может повторить этот slot/identity, но не переопределить прежнюю version. Новые версии становятся активными только через SpecActivate, кроме первой версии до регистрации stream. Units/multiplier и checked conversion — types-v1, отсутствие multiplier не равно 1.

### 3 — StreamDefinition

После Context: `stream_id:u32, instrument_slot:u32, spec_version:u32, connection_id:u32, connection_epoch:u64, subscription_epoch:u64, channel:u8, book_id:Opt<u32>, book_epoch:Opt<u64>, feed_profile_version:u32, provenance:Token128`.
Tags channel и ownership — types-v1. ConnectionId может разделяться несколькими streams, но объявленная current connection epoch должна совпадать. InstrumentSpec должен существовать. Book ID/epoch присутствуют вместе только у book channel.
В v1 один BookRef имеет один зарегистрированный writer-stream в архиве. Переназначение writer другому StreamId в том же архиве не поддерживается: требуется новый capture archive, а не неописанный rebind. Переподписка того же StreamId выражается EpochAdvance. Это сужение первой вертикали, review D4.
Feed profile version/provenance идентифицируют immutable проверенный decoder profile, который обязан быть доступен replay как входной registry. Отсутствующий/непроверенный профиль не загружается из сети и не подменяется догадкой: MissingVerificationProfile / BLOCKED_BY_MD_001 для Bitget. Наличие номера само по себе не доказывает feed semantics.

### 4 — ConfigDefinition

После Context: `new_config_version:u32, new_normalizer_version:u32, provenance_kind:u8, evidence:Token128, silence_rule:u8, freshness_deadline_ns:Opt<u64>, warmup_min_updates:Opt<u32>, warmup_min_elapsed_ns:Opt<u64>, allow_quiet_with_proof:bool, require_two_sided_snapshot:bool, recording_gate:u8`.
Provenance tags: Engineering=1, Synthetic=2, SourceVerified=3. SilenceRule и RecordingGate — [DataHealth](../market-data/data-health-v1.md). Новые version>0 и не переиспользуются; new normalizer version должна разрешаться в immutable revision registry. Registry и evidence — явные inputs окружения replay, не текущий случайно установленный decoder.
Policy validation (positive deadline, обязательные thresholds/evidence и прочее) выполняется до activation. Недоступная normalization revision/config блокирует зависимый replay. Group sync batching policy относится к REC-001; этот payload не задаёт несуществующий универсальный fsync interval.

### 5 — RawInput

После Context: `stream_id:u32, EpochTag, capture_attempt_no:u64, payload_encoding:u8, raw_len:u32, raw_bytes:[u8;raw_len]`.
Encoding tag=1 означает opaque source-message bytes; неизвестный tag отклоняется. `raw_len` точно равен остатку; пустой source message представим как diagnostic input, но не становится валидным market event автоматически.
CaptureAttemptNo>0, относится к StreamId, сохраняет локальный порядок попыток приёма до возможной queue loss. Последовательность успешных raw records одного stream не может уменьшаться/повторяться; обнаруженный пропуск требует предшествующего GAP с соответствующим известным range или явно Unknown-loss scope. AttemptNo **не exchange sequence** и не RecordNo.
Raw epochs сохраняются даже у диагностического old-epoch input; semantic market validator не применяет его к current state. Непривязанный StreamId или неизвестная SpecVersion — ошибка reference.

### 6 — Control

После Context — `control_tag:u8`, затем ровно тело варианта:

| Tag | Variant | Body в byte order |
|---:|---|---|
| 1 | TimerFired | stream_id:u32, timer_id:u64, deadline_monotonic_ns:u64 |
| 2 | TransportObservation | connection_id:u32, connection_epoch:u64, liveness:u8 (Unknown=0, Up=1, Down=2) |
| 3 | EpochAdvance | scope:u8 (Connection=1, Subscription=2, Book=3), owner_id:u32, expected:u64, next:u64, reason:u8 |
| 4 | SpecActivate | instrument_slot:u32, expected_spec_version:u32, new_spec_version:u32 |
| 5 | VerificationEvidence | stream_id:u32, EpochTag, raw_record_no:u64, evidence_kind:u8 (Snapshot=1, Delta=2), feed_profile_version:u32, proof:Token128 |
| 6 | WarmupEvidence | stream_id:u32, EpochTag, snapshot_raw_record_no:u64, update_count:u32, elapsed_ns:u64, proof:Token128 |
| 7 | FreshnessEvidence | stream_id:u32, EpochTag, freshness:u8 (Unknown=0, Fresh=1, QuietVerified=2, Stale=3), basis_raw_record_no:Opt<u64>, proof:Token128 |
| 8 | RecordingEvidence | health:u8 (Unknown=0, Healthy=1, Degraded=2, Failed=3), watermark_kind:u8 (Accepted=1, Appended=2, Written=3, Flushed=4, Durable=5), through_record_no:Opt<u64>, reason:u8 |

Reason tags, здесь и в GAP: UserReset=1, Reconnect=2, SourceGap=3, QueueOverflow=4, DecodeRejected=5, WriteFailure=6, NoFault=7, Unknown=255. Другие tags unsupported. NoFault допустим только у RecordingEvidence Healthy, не у GAP/epoch reset. Fault reason не заменяет scope.
TimerId>0; TimerFired должен иметь Context monotonic sample >= deadline. Ни decoder, ни reference model не читают live clock для проверки.
EpochAdvance проверяет owner identity, expected=current, next>expected и bounded arithmetic; влияние на states — DataHealth table. SpecActivate требует объявленную новую версию того же instrument и new>expected.
Verification/Warmup/Freshness evidence могут ссылаться только на более ранний доступный raw prefix. Witness сверяется с профилем, current tag/config, anchor, вычисленными counts/elapsed; произвольная строка proof не командует force-ready.
RecordingEvidence through, если есть, строго меньше собственного RecordNo; None — неизвестный watermark, **не 0 и не успешная durability**. Для Healthy нужен известный применимый frontier. Такое наблюдение не доказывает физическую сохранность носителя после crash.
Unsupported control tag останавливает semantic recovery; неизвестный обязательный input не пропускается.

### 7 — Gap

После Context: `scope:u8, reason:u8, target_count:u16, targets:[Target;target_count]`.
Scope ExplicitTargets=1: count 1..=256, Target entries отсортированы по StreamId, без дубликатов.
Scope AllDeclaredStreams=2: count=0; затронуты все объявленные current stream/tag, range/count **Unknown**. Пустой explicit scope запрещён.
`Target = stream_id:u32, EpochTag, first_lost_attempt:Opt<u64>, last_lost_attempt:Opt<u64>, loss_count:Opt<u64>`.
Range относится только к CaptureAttemptNo этого stream, никогда к guessed exchange seq. Оба конца присутствуют вместе или оба отсутствуют; first>0, last>=first; известный count>0. При известном полном range loss_count обязан быть Some(checked(last-first+1)); переполнение отвергается. При неизвестном range count может быть None либо известным положительным количеством без точных позиций.
Unknown loss count **не превращается в 0**. GAP с причиной NoFault запрещён.
GAP означает обнаруженную потерю, не её компенсацию: affected book/freshness fail-closed до verified resync/warm-up. Source gap может иметь неизвестный attempt range, поскольку локальных попыток для биржевого пропуска вообще не было.
Невозможность записать GAP при отказе носителя не создаёт фиктивный durable record: recorder Failed, завершённость Incomplete/Unknown, dependent work остановлена. Сообщение об отказе может быть только out-of-band, если WAL уже не принимает bytes.

### 8 — SegmentSeal

Без Context: `prefix_frame_count:u64, prefix_physical_len:u64, prefix_crc32:u32, prior_record_no:u64, has_known_gap:bool, is_final_segment:bool` (30 bytes).
Prefix начинается с offset=0 данного segment и заканчивается непосредственно перед этим seal. Count включает все предшествующие frames данного segment, physical_len — их полные bytes вместе с trailers, prior_record_no=seal.RecordNo-1.
**Prefix CRC вычисляется по конкатенации header || payload каждого prefix frame, исключая каждый индивидуальный CRC trailer.** Включение готовых trailers даёт нежелательную CRC residue-конструкцию; оно не является предложенным aggregate integrity check.
Seal имеет обычный собственный frame CRC, защищающий counts/length/prefix CRC/flags. has_known_gap должен совпасть с наличием Gap в этом segment prefix. Нельзя закрыть segment при незаписанном accepted input.

### 9 — SegmentStart

Без Context: `archive_id:[u8;16], capture_session_id:[u8;16], clock_id:u32, previous_segment_no:u32, previous_segment_seal_record_no:u64, previous_segment_seal_frame_crc32:u32` (52 bytes).
Только offset=0 следующего segment. IDs/clock совпадают с ArchiveStart; header.segment_no=checked(previous+1); RecordNo продолжается без пропусков; previous seal существует, совпадает по RecordNo и **собственному frame CRC**, is_final_segment=false.
Definitions/config/tag state наследуются из проверенной предыдущей цепочки. Отдельный segment без начала архива/зависимых definitions не считается самодостаточным replay archive.

### 10 — ArchiveSeal

Без Context: `expected_segment_count:u32, prior_frame_count:u64, total_prefix_physical_bytes:u64, prefix_crc32:u32, prior_record_no:u64, input_quality:u8` (33 bytes).
Quality: NoKnownLoss=1, GapsRecorded=2, Unknown=3. NoKnownLoss запрещён при любом Gap; GapsRecorded требует хотя бы один Gap; Unknown не подменяется NoKnownLoss. NoKnownLoss означает отсутствие **записанной известной** потери, не доказанную exchange continuity.
ArchiveSeal идёт непосредственно после SegmentSeal(is_final_segment=true), в том же final segment. Это единственный разрешённый frame после final SegmentSeal; ни bytes, ни дополнительные segments после ArchiveSeal не разрешены.
Prefix охватывает все segments в порядке, все frames до ArchiveSeal, включая SegmentStart/SegmentSeal. Count/physical bytes включают индивидуальные trailers; aggregate CRC снова охватывает только header || payload каждого prefix frame. `prior_record_no = ArchiveSeal.RecordNo-1`, expected_segment_count=final_segment_no+1 с checked arithmetic.
Все значения сверяются с реально разобранной цепочкой, а не принимаются как доверенная декларация completeness.

## 5. Recovery result и запрет magic scanning

Результат разделяет `physical_completion` (Incomplete / Complete), `input_quality` (NoKnownLoss / GapsRecorded / Unknown) и применимость данных. Complete+GapsRecorded возможен; это **не пригодная без resync книга**.
Recovery сообщает segment_no, local/absolute `last_good_offset`, последний принятый RecordNo и причину остановки. last_good_offset указывает конец последнего frame, прошедшего framing, CRC, payload schema и reference/order validation. Только framing scanner обязан называться иначе и не выдавать свой offset за semantic acceptance.

| Наблюдение | Результат |
|---|---|
| Пустой input | NoArchive, Incomplete, last_good_offset=0 |
| EOF до 32-byte header либо до полного payload/trailer | TruncatedTail на начале неполного frame; last_good_offset предыдущей границы |
| L выше cap / checked offset или count overflow | LengthError до allocation; остановка на предыдущей good границе |
| Bad magic/flags/reserved/checksum, неканоничный payload | Corrupt/InvalidPayload, Incomplete; не пропускать даже последний frame |
| Неизвестный frame/schema/kind/control version | Unsupported, prefix отдельно доступен для диагностики; не «успешный replay с пропуском» |
| Повтор/пропуск RecordNo, неверный SegmentNo или разорванная seal chain | OrderOrChainError, Incomplete |
| Валидный EOF на frame boundary без final ArchiveSeal | ValidPrefixIncomplete; последний целый frame не доказывает completion |
| Только валидный SegmentSeal | SegmentSealedArchiveIncomplete, пока не доказана вся archive chain |
| Все frames/seals/counts/lengths/CRCs/IDs согласованы и точный EOF после ArchiveSeal | Complete с отдельно указанным input_quality |
| Bytes/segments после ArchiveSeal | TrailingDataError, не Complete |

EOF после обещанной header длины — наблюдаемый truncated tail, не доказательство причины (это также может быть испорченная length). Никаких forensic/crash guarantees из одной классификации.
При corruption между good frames следующий похожий magic **не** точка восстановления. Не сканировать и не склеивать suffix молча. Prefix разрешён как явный incomplete diagnostic input; восстановление полного архива требует отдельного решения, не auto repair.
Удаление целого последнего raw/control frame вместе с seal, самого ArchiveSeal или final segment не даёт Complete. Полностью отсутствующий архив нельзя обнаружить по его отсутствующим bytes без внешнего inventory; такой гарантии нет.

## 6. Watermarks, loss и durability

Watermarks — Option<RecordNo> непрерывного prefix; None означает неизвестное/ещё отсутствующее подтверждение, не запись 0. В одной session при известных значениях:
`durable <= flushed <= written <= appended <= accepted`.

| Frontier | Значение, но не более сильная гарантия |
|---|---|
| Accepted | input атомарно принят bounded owner, ему присвоен RecordNo в выбранном admission order |
| Appended | полный canonical frame сформирован в логическом append buffer |
| Written | writer принял все bytes frame; они ещё могут находиться в userspace buffer |
| Flushed | userspace buffers переданы OS; это не гарантия пережить power loss |
| Durable | выполнен и успешно подтверждён platform-specific sync protocol для этого prefix и необходимых metadata updates |

До admission потеря отражается CaptureAttemptNo и будущим Gap; непринятый input не получает RecordNo. После присвоения RecordNo потерять frame и продолжить журнал с дыркой нельзя: recorder Failed/архив Incomplete. Нельзя задним числом заменить payload уже принятой identity на Gap. При утрате всего volatile хвоста отсутствие completion seal не должно выдавать prefix за полный архив.
Partial write, flush или sync error не продвигает соответствующий frontier. Более сильный frontier не «догадывается» по более слабому или CRC-readback. При restart старые in-memory watermarks не переносятся как факты; начинается новый архив.

Mode Buffered=1: допустима публикация после Written только при явной ослабленной recording policy; возможная потеря при process/OS/power failure объявляется. Mode GroupSynced=2: sync выполняется группами, Durable продвигается только после успеха; зависимая работа с Durable gate ждёт его, а не произвольное время. Mode SyncBeforePublish=3: публикация state/effect требует durable prefix его raw/control causal basis. Это рекомендуемый кандидат безопасного capture profile, не обещание latency.
Durability observations и DataHealth evidence не могут удостоверять сами себя. Ack собственного record не разрешён. Как writer предоставляет проверяемый durable acknowledgement и как recorder публикует ready state без циклического ожидания — обязательный integration review REC-001; модель SPEC-001 проверяет только порядок/bounds acknowledgements, не физический storage.

## 7. Rotation, finalization и границы доказательств

Rotation: остановить admission в закрываемый segment, записать принятый prefix, SegmentSeal(final=false), выполнить выбранный flush/sync policy, затем новый SegmentStart с проверенной ссылкой. Данные не пропускаются ради смены файла. Не объявлять новый segment durable до требуемой platform metadata persistence.
Finalization: прекратить admission, дождаться полного drain accepted inputs, записать final SegmentSeal и ArchiveSeal, flush/sync их bytes и необходимые файловые metadata; только после успешного принятого platform protocol рекламировать archive Complete. Ошибка на любом шаге оставляет incomplete/unknown result, даже если ранние frames валидны.
Порядок file creation/rename/directory sync, atomic publication/manifest, crash recovery и power-loss tests относятся к REC-001 и проверяемой OS. Этот proposal требует их evidence, но не утверждает, что вызов одной абстрактной `flush` обеспечивает durability на Windows/Linux.
Повторное открытие для append после process restart не поддерживается v1; новый архив с previous link сохраняет факт разрыва clock/session. Merge архива, repair, compression, encryption/authentication, persisted normalized projection и произвольные extensions требуют новой принятой revision.

## 8. Independently checked golden frame

Fixture W01: origin=synthetic, frame/schema=1, ArchiveId=16 bytes 0x01, CaptureSessionId=16 bytes 0x02, ClockId=1, mode=SyncBeforePublish(3), previous=None; RecordNo=1, SegmentNo=0. L=38, frame=74 bytes. Это один ArchiveStart, **не Complete archive**.

```text
0000: 50 53 52 57 01 00 01 00 01 00 00 00 26 00 00 00
0010: 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
0020: 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01
0030: 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02
0040: 01 00 00 00 03 00 13 c4 02 9e
```

Header+payload CRC = **0x9E02C413**; trailer little-endian `13 c4 02 9e`.
Reference vectors: ASCII `123456789` -> **0xCBF43926**; empty bytes -> **0x00000000**.
На КП1 эти значения и 32/38/74-byte размеры реально сверены offline: Python 3.13.5, zlib build/runtime 1.3.1 и отдельно написанный bit-at-a-time loop с reflected polynomial. Это два расчёта документационного golden, **не запуск Rust codec, decoder или recovery test**. CRC всего готового frame с trailer = 0x2144DF1C; поэтому aggregate CRC выше не включает trailers.
Expected bytes должны храниться независимо от проверяемого codec; runtime round-trip не заменяет golden assertion. Полный synthetic raw/control/Gap/seal набор и all-offset truncation assertions планируются в [matrix](../domain/test-matrix-v1.md) после approval.

Review D5/D6: raw/control-only формат, one-session/restart policy, seal chain, completion vs input quality, modes/ack boundary, CRC и caps. Всё остаётся **PROPOSED**; filesystem durability и Windows execution **NOT_RUN**.
