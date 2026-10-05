# Definition of Done

## Любая задача

- Scope и allowed paths соблюдены.
- Нет неутверждённого изменения публичного контракта.
- Результат сохранён в commit/PR, handoff содержит base/head SHA.
- Проверки перечислены как PASS/FAIL/NOT_RUN; отсутствующие команды не выдуманы.
- Нет секретов, приватных исходников и крупных данных в diff.
- Открытые риски/отклонения вынесены явно.
- Review относится к текущему head; после значимых изменений проверка повторяется.
- Владелец принимает merge.

## Docs-only

Проверить внутренние ссылки, согласованность статусов и provenance; явно указать, что runtime/CI не тестировались. Не требовать property tests для текста README.

## Rust code после BOOT-001

Реальные fmt/clippy/tests на зафиксированном toolchain; Cargo.lock для workspace; negative tests для ошибок ввода. Никаких недокументированных unbounded queues, unchecked numeric conversions или panic на внешних данных. Test-only unwrap допустим с понятным failure; blanket-запрет всех unwrap не заменяет обработку production errors.

## Market-data code

Fixtures для snapshot/delta/gap/duplicate/reset/old epoch; bounded-queue failure; receive-order/replay parity; corrupted/truncated WAL. Live-network smoke test запускается отдельно от offline CI и не является unit test.

## Strategy code

No-look-ahead, frozen level revision, episode IDs, trigger vs outcome, UNKNOWN policy, expiry/revocation, conflict/duplicate guards. Все thresholds имеют происхождение. Microstructure proxies имеют quality/uncertainty.

## Performance claim

Только вместе с machine/toolchain/dataset/config, warm-up, sample size и p50/p95/p99/queue age. Не устанавливать жёсткий latency gate на непостоянном shared CI runner без отдельного benchmark protocol.
