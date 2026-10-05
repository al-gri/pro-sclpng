# ADR-0002 — Exact domain, causal events, DataHealth and bounded WAL

Status: **PROPOSED — DESIGN REVIEW REQUIRED**.
Date: 2026-10-05.
Task: [SPEC-001 / Issue #3](https://github.com/al-gri/pro-sclpng/issues/3).
Packet: [5994665559](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559).
Worker claim: [5995282901](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901).
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Branch: `feat/SPEC-001-domain-contracts` -> `main`.

## Context

BOOT-001 уже предоставляет std-only workspace и закреплённый Rust 1.98.1, но domain crate намеренно не экспортирует market types. До network/storage реализации требуется согласованная первая поверхность: exact values, идентичности, causal inputs, health и проверяемый WAL framing. Accepted baseline ADR-0001 не изменяется; этот новый ADR не объявляет accepted собственные схемы.

Ни прочтение исходников, ни зелёный bootstrap CI не подтверждают correctness ещё не реализованных контрактов. Эта поставка — первая контрольная точка **только design proposal**. После неё worker останавливается до явного Architecture/Integrator ответа в PR на конкретной ревизии.

## Предлагаемые решения и альтернативы

| ID | Proposal | Обоснование и отвергнутая на этом этапе альтернатива |
|---|---|---|
| D1 | PriceTicks/QuantitySteps — разные u64 value types с SpecRef; ExactDecimal u128 coefficient, canonical scale 0..18; immutable versions и market identity | Исключить float rounding и смешение spot/futures/spec. Signed/negative-price support не вводится молча; первой вертикали предлагаются положительные цены, отдельный quantity zero contract |
| D2 | ASCII decimal parser, 96-byte bound, checked intermediates, off-grid error, точный linear multiplier с units/Unknown | Не округлять off-grid и не использовать arbitrary precision dependency. Conservative intermediate overflow выбран вместо сложной cancellation/big-integer арифметики; это намеренное ограничение кандидата |
| D3 | Archive/cause/sub-index/normalizer-version EventId, recorded admission order, availability/as-of, записанные control/config inputs | Не использовать текущие clock/random/exchange_ts как identity/order. EventId не exchange dedup key. Raw/control-only WAL воспроизводит normalized DTO; отдельная persisted projection потребует нового решения |
| D4 | Независимые stream/book identities и composite epochs; DataHealth по transport/freshness/book/recording; verified snapshot + recorded warm-up | `connected=true`, heartbeat, старый snapshot или UNKNOWN profile не разрешают usable. One writer-stream per BookRef в архиве v1; rebind на другой StreamId требует нового capture, same-stream resubscription использует epoch |
| D5 | One-session archive, 32-byte header, bounded canonical payload, CRC32, explicit GAP, segment/archive seals; completion отдельно от quality и durability | Не принимать EOF на good boundary за complete, не magic-scan после corruption, не приравнивать flush к durable. Restart создаёт новый архив; file append recovery не прячется в generic domain |
| D6 | Предельные размеры, known-tag-only versioning, independent golden bytes и exhaustive negative matrix | Payload<=1 MiB, book event<=4096 entries, explicit GAP targets<=256 — engineering caps, не exchange limits. Нет skip unknown records, Rust memory-layout serialization, runtime-generated expected golden или нового dependency |

Ни одна строка таблицы не является approval. Полные кандидаты:
[types-v1](../../specs/domain/types-v1.md),
[events-v1](../../specs/market-data/events-v1.md),
[data-health-v1](../../specs/market-data/data-health-v1.md),
[wal-v1](../../specs/recording/wal-v1.md),
[test-matrix-v1](../../specs/domain/test-matrix-v1.md).

## Consequences / trade-offs

Числа всегда qualified соответствующим instrument/spec; это добавляет явные проверки, но предотвращает совпадение численно одинаковых ticks разных рынков. Неизвестный multiplier блокирует только base-unit conversion, а не уже определённую price/quantity grid.

RecordNo фиксирует выбранный capture admission order, а не недоказуемый глобальный биржевой порядок. Receive monotonic samples разных queued streams могут прибывать в журнал не по возрастающему времени; evaluation time не откатывается. Replay требует доступных immutable config/normalizer/feed-profile definitions; отсутствующая version не заменяется latest. Рестарт с новым архивом упрощает identity/clock correctness, но не обещает бесшовное межсессионное продолжение.

DataHealth usable — пригодность данных, не trading eligibility. Для требующей durable capture policy публикация ждёт подтверждения causal input prefix. RecordingEvidence не может подтверждать собственный frame. Конкретный storage acknowledgement/finalization protocol, OS sync/rename semantics и crash correctness остаются обязательной downstream работой REC-001, не доказанной memory model.

CRC32 ловит случайную порчу согласно предложенному алгоритму, но не даёт cryptographic integrity/authentication. Aggregate seal CRC исключает индивидуальные frame CRC trailers; counts/length/chain тоже проверяются. Complete+GapsRecorded допустим как физически завершённый архив с явной потерей; использовать его как gap-free input запрещено. Невозможность записать GAP не оправдывает фиктивный durable GAP.

Std-only и фиксированный Rust 1.98.1 сохраняются без изменения root Cargo/lockfile/toolchain/CI. На КП1 crates/domain вообще не меняется. После review допустимы лишь согласованные types, conversions, pure validators/test models; не production market-data/storage/replay.

## Unknowns и границы зависимостей

**BLOCKED_BY_MD_001 только для real-feed mapping:** точный Bitget market/product identity; metadata units/increments/multiplier; source timestamp units; sequence/pseq/duplicate/reset bridge; zero-quantity interpretation; aggressor/RPI semantics; trade-book causality; snapshot validity, quiet/no-change evidence и реальные timeout/warm-up policy. Неподтверждённый profile не выдаётся за Verified; REST/WS stitching не разрешается.

Независимы от этих unknowns: bounded ASCII parsing, checked arithmetic, разные identities/epochs, recorded order и IDs, generic health guards, bounded binary framing/checksum, truncated/corrupt/complete distinctions и synthetic test vectors. Они не блокируются только потому, что MD-001 ещё не выполнен.

**Не входит:** Bitget adapter/live capture, production book/queues/supervisor, file recorder/replay engine, стратегия/уровни/TradePlan, Telegram, private API/execution, performance claims и настройки репозитория.

## Вопросы Architecture / Integrator на этой ревизии

1. **D1/D2:** принять ли u64 counts, canonical scale<=18, input<=96, положительные цены и conservative intermediate overflow? Нужен ли более широкий numeric domain до реализации, а не после публикации API? Принимается ли Unknown linear multiplier без блокировки независимых grid conversions?
2. **D3:** принять ли EventId с normalizer version, raw/control-only archive и one-session/new-archive-on-restart модель? Достаточны ли доступные immutable revision registries или требуется хранить дополнительный content fingerprint/bundle до freeze?
3. **D4:** принять ли composite epoch fan-out, current verified snapshot + explicit warm-up witness, quiet-proof policy и independent health axes? Подтвердить минимальную policy для UnknownOnSilence и запрет same-archive writer rebind; MD-001 должен дать применимые verification proofs, а не worker assumptions.
4. **D5:** принять ли header/payload tags, CRC coverage и seal chain/completion model? Подтвердить предложенные durability modes и границу между чистыми watermark guards SPEC-001 и обязательным OS/storage acknowledgement protocol REC-001; никакой durability implementation acceptance здесь нет.
5. **D6:** принять ли 1 MiB / 4096 entries / 256 GAP targets caps и fail-on-unsupported policy? Для превышения нужен явный loss/error outcome, не silent truncation. Нужен ли иной integrity algorithm до golden freeze?
6. **QA-001:** проверить полноту positive/negative matrix, точные error/offset assertions и будущие independent multi-frame golden bytes. Особенно old epoch, ложное warm-up evidence, missing config, loss целого final frame и невозможность записать GAP.

## Approval и stop gate

Для продолжения нужен явный design review Architecture/Integrator **в единственном Draft PR этой ветки**, с указанием проверенного head и ответами/исправлениями по обязательным решениям выше. Role prompt и этот ADR не заменяют такой review. Worker не делает self-approval и не объявляет API/wire format accepted.

После согласования дизайн остаётся PROPOSED до принятия в установленном workflow; реализация согласованных value types/validators/vectors продолжается в той же ветке/PR. Полная acceptance дополнительно требует реально выполненных checks на final head, independent QA и owner acceptance/merge. Issue #6 не закрывается этой работой.

На КП1: один independently checked ArchiveStart golden и test design; **Rust contract tests NOT_IMPLEMENTED/NOT_RUN**. Матрица не объявляется пройденной. Handoff и post-commit PR evidence сохраняют фактические SHA, scope, проверки и ограничения без самоссылочного будущего commit SHA.
