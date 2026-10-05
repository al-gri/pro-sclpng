# SPEC-001 — proposed positive / negative test matrix

Status: **PROPOSED**. Контрольная точка DESIGN REVIEW, не отчёт о готовых domain tests.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Контракты: [types](types-v1.md), [events](../market-data/events-v1.md), [DataHealth](../market-data/data-health-v1.md), [WAL](../recording/wal-v1.md), [ADR](../../docs/adr/0002-domain-event-wal-contracts.md).

## 1. Статус и fixture protocol

На КП1 **нет новой Rust реализации типов, валидаторов, reference model/codec или executable contract tests**. Все строки ниже — test design; их Rust execution **NOT_RUN / NOT_IMPLEMENTED** до явного design approval в PR. Исключение по выполненной работе: arithmetic иллюстрации и W01 CRC/length рассчитаны offline для проверки текста; это не считается выполнением соответствующих Rust assertions.
Все данные synthetic, без network, secrets, live clock и real Bitget parameters. Каждая будущая fixture несёт `id`, `origin=synthetic`, `schema_version=1`, `policy_version` (конкретная version либо not_applicable), `input`, `expected_result_or_error`, `rationale`. Один schema tag без expected outcome не является тестом.
Для health vectors отдельный профиль `synthetic-1`: snapshot/delta verification задаётся тестовым evidence, не Bitget mapping. Пример policy H-A: config=1, normalizer=1, provenance=Synthetic, freshness deadline=10 ns, StaleAfterDeadline, warmup_min_updates=Some(2), warmup_min_elapsed_ns=Some(5), allow_quiet_with_proof=false, require_two_sided_snapshot=true, recording_gate=Durable. Эти маленькие числа — **только test values**, не рекомендации для feed.

Future integration tests: `crates/domain/tests/**`; memory-only codec helpers: `crates/domain/tests/support/**`; fixtures: `tests/fixtures/domain/**`, подключаются manifest-relative из domain tests. Не оставлять Rust tests лишь в корневом tests/ виртуального workspace. Не добавлять dependencies, features, build scripts или файловый recorder.

## 2. Exact decimals, units и checked arithmetic

| ID | Input (synthetic) | Expected assertion / rationale |
|---|---|---|
| N01 | tick=0.05, price=100.10 | PriceTicks=2002; reverse canonical decimal `100.1`; проверяется count и unit/spec |
| N02 | step=0.001, qty=1.234 | QuantitySteps=1234; exact reverse `1.234` |
| N03 | `000100.1000`, increment `0.0500` | canonical (1001,1)/(5,2), 2002 ticks; insignificant zeros не меняют grid |
| N04 | price=100.11, tick=0.05 | OffGrid, без rounded price |
| N05 | qty=1.2345, step=0.001 | OffGrid, не 1234 и не 1235 |
| N06 | empty, `.5`, `1.`, `1e2`, `NaN`, `inf`, `1,2`, space/tab/newline, Unicode digits | InvalidSyntax отдельно для каждого input |
| N07 | `+1`, `-1`, `-0` | InvalidSign; нет permissive sign parsing |
| N08 | 96 ASCII bytes незначащих нулей; затем 97 | первый input canonical zero (допустим численно); второй InputTooLong до разбора; ZeroPrice проверяется отдельно |
| N09 | `1.0000000000000000000`; `0.0000000000000000001` | первый canonical (1,0), второй ScaleTooLarge (значащая scale=19) |
| N10 | counts 1 и u64::MAX, increment=1 | min/max PriceTicks проходят; canonical обратное преобразование точное |
| N11 | decimal 18446744073709551616, increment=1 | CountOutOfRange (u64::MAX+1), без narrowing |
| N12 | u128::MAX coefficient; decimal u128::MAX+1 | MAX parsing проходит как ExactDecimal; +1 Overflow(CoefficientAdd); conversion в u64 отдельно может не пройти |
| N13 | coefficient append, для которого acc*10 > u128::MAX | Overflow(CoefficientMultiply), не panic/wrap |
| N14 | zero increment; coefficient с scale=19 в direct metadata constructor; noncanonical decimal | InvalidIncrement / ScaleTooLarge / noncanonical metadata error согласно месту validation; ни один не достигает деления на 0 |
| N15 | X=u128::MAX scale0, increment=(1,18) | Overflow при alignment X*10^18; не saturation, не математическое сравнение через float |
| N16 | count=u64::MAX, increment coefficient=u128::MAX | reverse conversion Overflow(Multiply), даже если count сам валиден |
| N17 | positive quantity multiplier=0.01 base/contract, step=1 contract, steps=123 | exact base amount 1.23, сохранены units и SpecRef |
| N18 | multiplier=None / zero / units mismatch / nonlinear conversion | UnknownMultiplier / InvalidMultiplier / UnitMismatch / UnsupportedConversion соответственно; None не 1 |
| N19 | checked steps*increment*multiplier overflow; scale sum=36 без удаления до <=18 | точная ошибка operation/ScaleTooLarge, без rounding промежуточного значения |
| N20 | quantity=0 в numeric value, SetLevel, snapshot level, Trade и DeleteLevel | numeric 0 допустим; Set/snapshot/trade отвергаются; Delete имеет price и вообще не несёт qty; parser не угадывает deletion |
| N21 | InstrumentRef или SpecVersion отличается при том же count | IdentityMismatch / SpecMismatch до применения цены или размера |

При реализации ошибки direct constructor должны быть согласованы с parser errors в types-v1; не объединять разные обязательные ошибки в assertion «is_err». Точная variant nomenclature подтверждается design review, не выдумывается тестом отдельно от API.
Bounded exhaustive plan: coefficient 0..=200, scale 0..=3, положительные increment coefficients 1..=20, steps 0..=100. Для grid cases assert exact count/reverse; off-grid assert OffGrid. Отдельные ручные MAX/intermediate boundaries выше необходимы: малый loop не покрывает u128 overflow. Никаких proptest/dependency additions. Эти loops пока **не запускались**.

## 3. Identities, order, time и UNKNOWN

| ID | Input | Expected assertion |
|---|---|---|
| E01 | одинаковый symbol, venue/namespace; Spot vs Perpetual | InstrumentRef различны; slots не сливаются по symbol |
| E02 | Normal и Rpi BookRef одного instrument | разные BookId/epoch ownership, independent state; равный epoch integer не даёт equality |
| E03 | чужой stream/BookId/spec при применении input | IdentityMismatch/SpecMismatch; ни чужая, ни текущая книга не становятся Usable |
| E04 | RecordNo=10/exchange_ts=200, затем 11/100 | cursor order строго 10 -> 11; равные/Unknown timestamps тоже не меняют порядок |
| E05 | один RawFrameId с двумя results | sub-index 0,1; два разных EventId; replay дважды даёт идентичный ordered result |
| E06 | тот же EventId + identical canonical fields; затем conflicting payload | consumer duplicate no-op; conflict IdentityConflict, не второй event и не silent overwrite |
| E07 | пропуск/повтор RecordNo в WAL; sub-index 0,2 или перестановка outputs | RecordOrderError / SubEventOrderError с точной первой ошибочной позицией |
| E08 | current time/random отличаются между двумя test runs | canonical IDs/output не меняются: clock/random не inputs reducer; применение незаписанных inputs запрещено |
| E09 | timer/config change между raw inputs | меняет только последующие outputs/availability; current ConfigDefinition Context отражает прежнюю config, новая действует со следующего RecordNo |
| E10 | отсутствующий config/normalizer/feed profile либо version reused с иными bytes | UnknownConfiguration/MissingVerificationProfile/IdentityConflict; нет fallback на latest config |
| E11 | monotonic values одинаковы, session или clock различаются | IncomparableClock, ни elapsed, ни fake continuation после restart |
| E12 | receive time поздно доставленного input меньше evaluation time; Unix jump | RecordNo сохраняется; evaluation maximum не откатывается, freshness не становится свежей от одного late arrival |
| E13 | as_of > available_at; future raw/config reference; future sub-index | FutureCausalReference/UnknownDefinition; no look-ahead |
| E14 | unknown source timestamp / aggressor / trade-book link / RPI flag | сохраняются Unknown; не 0/local time/Buy/Sell/Proven по соседству timestamps |
| E15 | новый live capture и replay прежнего capture | новый ArchiveId допустим; replay прежнего сохраняет IDs; EventId не выдаётся за exchange dedup key |
| E16 | epoch next<=current; exhausted u64/u32 IDs | EpochRollback/Overflow с сохранением current state, никакого wrap-to-zero |
| E17 | snapshot/update >4096 entries; duplicate(side,price) в update | EventTooLarge / duplicate-level validation error, не silent coalescing/last-write-wins |

## 4. DataHealth transition vectors

Все healthy witnesses ссылаются на существующий current-tag anchor и известный synthetic profile. Expected state проверяется по четырём осям и guards, а не только одному bool.

| ID | Sequence / starting state | Expected |
|---|---|---|
| H01 | definition -> transport Up -> heartbeat, snapshot отсутствует | T=Up, B=NoSnapshot, F не Fresh из heartbeat, usable_data=false |
| H02 | current verified two-sided snapshot t=0 -> verified updates t=2 и t=5 -> truthful WarmupEvidence | B=Warming до witness, затем Usable; H-A counters=2/elapsed=5; F Fresh; recording guard проверяется отдельно |
| H03 | H02 + Healthy recording ack через causal frontier | capture_usable=true только при достаточном выбранном Durable frontier |
| H04 | H02 + Gap/overflow | B=Invalid, F=Unknown, anchor/counters cleared; следующий delta/heartbeat не восстанавливает usable |
| H05 | H04 -> новый current verified snapshot -> новый warm-up | восстановление возможно только от нового anchor и witness, не старых counters |
| H06 | epoch advance из Usable -> old snapshot/warm-up evidence | новый tag остаётся NoSnapshot/not usable; old input даёт diagnostic, не rollback |
| H07 | ConnEpoch advance при двух связанных streams и одном постороннем | оба связанных invalidated, посторонний сохраняется; Sub/BookEpoch advance затрагивает только свои dependencies |
| H08 | Normal snapshot/warm-up при пустой Rpi generation | Normal может стать Usable; Rpi остаётся NoSnapshot |
| H09 | t=16 после last_valid_data t=5 при H-A; только heartbeat | age=11>10, F=Stale, usable=false; heartbeat не reset last valid time |
| H10 | silence с UnknownOnSilence и без применимого quiet proof | F=Unknown, не выдуманный disconnect/verified resync/QuietVerified |
| H11 | allow_quiet=true + применимый current policy/profile quiet proof; затем missing/old proof | только первый случай допускает QuietVerified, второй не восстанавливает usable |
| H12 | ложные warm-up count/elapsed, чужой anchor/config/tag/profile | witness отвергнут, B не Usable; проверяется конкретная violated guard |
| H13 | SpecActivate/config/normalizer change из Usable | old evidence очищено; старые qualified ticks и policy не используются |
| H14 | Transport Down -> Up | B остаётся Invalid до нового snapshot/warm-up; Up сам не лечит книгу |
| H15 | recorder Failed, Gap невозможно записать | R=Failed, capture_usable=false, archive incomplete/unknown; никакого assertion «durable Gap exists» |
| H16 | recording Healthy после Gap без book resync | R может стать Healthy, B остаётся Invalid |
| H17 | recording ack собственного/будущего RecordNo, durable>written, неизвестный frontier | reference/watermark error, capture_usable=false |
| H18 | startup/restart с новым ArchiveId/clock и старым state | NoSnapshot/Unknown; старые elapsed/usable не переносятся |
| H19 | unverified real Bitget profile при структурно корректном snapshot | BLOCKED_BY_MD_001, не synthetic Verified; generic synthetic vectors независимы |

## 5. WAL golden, limits, corruption, recovery

W01 bytes приведены независимо в [wal-v1](../recording/wal-v1.md), не производятся проверяемым codec в test runtime. W02 — план маленькой последовательности: ArchiveStart -> InstrumentSpec -> StreamDefinition -> ConfigDefinition -> RawInput -> Timer/Control -> Gap -> final SegmentSeal -> ArchiveSeal. Source bytes synthetic; нет network capture.

| ID | Input / mutation | Expected assertion |
|---|---|---|
| W00 | ASCII `123456789`; empty input в standalone CRC function | CRC 0xCBF43926 / 0x00000000, primary algorithm и независимая проверка описаны в WAL |
| W01 | exact 74-byte ArchiveStart golden | header=32, payload=38, RecordNo=1, SegmentNo=0, CRC=0x9E02C413, bytes match; last_good_offset=74, ValidPrefixIncomplete, не Complete |
| W02 | согласованная small raw/control/Gap/seal chain | все offsets/decoded fields/counts/CRCs совпадают с независимо записанными expected bytes; Complete + GapsRecorded, не NoKnownLoss |
| W03 | schema/frame version/kind/control tag неизвестен | Unsupported на конкретном offset; suffix не применяется |
| W04 | wrong magic/flags/reserved; bad option/bool tag; noncanonical decimal; лишние payload bytes | точная Corrupt/InvalidPayload ошибка, не permissive decode |
| W05 | protected byte flip либо trailer byte flip у good frame | ChecksumMismatch; last_good_offset до повреждённого frame |
| W06 | L=1_048_577 либо u32::MAX; nested raw_len/count > validated remaining | LengthError до allocation по указанной длине; allocation instrumentation не видит oversized reserve |
| W07 | absolute offset near u64::MAX; checked count/length overflow | Overflow/LengthError до addition wrap/allocation; checked usize conversion тоже проверяется без зависимости от host usize |
| W08 | W01[:k] для каждого k=0..73 | k=0 NoArchive, прочие TruncatedTail, last_good_offset=0; никакого частичного принятого ArchiveStart |
| W09 | full W02 prefix cut на каждом byte offset каждого header/payload/trailer | точная предыдущая good boundary и RecordNo; EOF на frame boundary unsealed=Incomplete, не ошибка доказанной source sequence |
| W10 | good frame A -> corrupted B -> good C с magic | stop перед B, C не применяется; запрещён magic rescan/skip |
| W11 | удалить целый последний frame/ArchiveSeal/final segment | не Complete даже при корректном оставшемся frame boundary |
| W12 | seal count/physical length/aggregate CRC/previous seal reference не совпадает; собственный frame CRC пересчитан | semantic seal/chain error; собственный CRC не отменяет неверный manifest |
| W13 | nested segment start с чужими ArchiveId/session/clock или SegmentNo | OrderOrChainError/IdentityMismatch, archive Incomplete |
| W14 | корректный SegmentSeal без ArchiveSeal; bytes после ArchiveSeal | SegmentSealedArchiveIncomplete / TrailingDataError соответственно |
| W15 | loss_count=None, range=None; count=0; range overflow | Unknown сохраняется; known 0 запрещён; overflow отвергнут; None не zero |
| W16 | RawInput attempt sequence 1,3 без Gap; та же последовательность с явным Gap attempt=2 | первая continuity diagnostic/error согласно input validation, вторая loss наблюдаема и health invalid; local attempt не exchange seq |
| W17 | Accepted input потерян после присвоения RecordNo | продолжение с дыркой запрещено; архив Incomplete/Failed, не replacement payload с тем же ID |
| W18 | модели partial write/flush/sync failure | соответствующий frontier не продвигается; ordering D<=F<=W<=A сохраняется (A здесь Appended; Accepted проверяется отдельным верхним frontier) |
| W19 | mode Buffered/GroupSynced/SyncBeforePublish с разными watermarks | publication guard соответствует выбранной mode/policy; CRC/readback/flush не выдают durable guarantee |
| W20 | aggregate CRC ошибочно включает готовые trailers | test oracle отклоняет этот метод; prefix CRC считается по header+payload каждого frame, individual trailers исключены |

Для W02 нужны independent full golden bytes после согласования layout; на КП1 они **ещё не вычислены и не заявляются проверенными**. W01 не заменяет этот обязательный последующий набор. Model scenarios partial write не являются filesystem crash/power-loss tests.
Обязательные assertions: конкретный error variant/field, consumed/last_good offsets, frontier/axis state, exact ID/order и запрет обработанного suffix. «Не упало»/round-trip alone недостаточны.

## 6. Реальная верификация и последующий gate

КП1 offline arithmetic check: 10010/5=2002, off-grid remainder 10011%5=1 и quantity example; это integer calculation иллюстраций, не decimal parser test.
W00/W01 calculations реально сверены Python 3.13.5, zlib 1.3.1 и отдельным reflected bit-loop; header/payload/frame lengths=32/38/74. Все будущие Rust rows выше остаются NOT_RUN.
На final PR head применимы существующие build/fmt/clippy/workspace tests. Они пока проверяют BOOT-001 code (15 CLI tests на Linux, domain без новых tests), **не корректность предложенного wire format**. Финальные фактические результаты и exact SHA публикуются в PR после commit; base push-CI не подставляется вместо них.

После явного design approval — реализация только согласованных value types/checked conversions/pure validators/reference models и перечисленных vectors в той же ветке/PR. Затем на точном head:

```text
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test -p domain --locked
```

Каждая команда требует отдельного факта запуска/exit code; workspace test не выдаётся за выполненный standalone `cargo test -p domain`. Недоступные команды NOT_RUN, failed — FAIL. Проверить initial/final clean checkout и неизменный root Cargo.lock. Windows 11 x64/PowerShell 5.1 отдельно NOT_RUN до owner evidence; Linux CI не является Windows test.
Независимый QA-001 review на том же SHA обязателен. Этот checkpoint не закрывает Issue #3 или #6 и не завершает acceptance SPEC-001.
