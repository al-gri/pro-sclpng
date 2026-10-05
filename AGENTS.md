# Instructions for all project agents

## Scope and first read

Работай только с `al-gri/pro-sclpng`. Не просматривай другие репозитории пользователя и не переноси оттуда файлы. Внешнюю техническую документацию читать можно; не подменять ей авторские торговые правила.

Начинай с этого файла, `docs/PROJECT_STATE.md`, `docs/ARCHITECTURE.md`, `docs/INVARIANTS.md`, `docs/WORKFLOW.md`, затем текущего Issue и релевантных specs/ADR. Читай фактический GitHub-контекст, не полагайся на память чата.

До изменений проверь репозиторий, актуальный base SHA, чистоту рабочего дерева и доступные инструменты. При отсутствии shell, Rust или права записи явно запиши BLOCKED/NOT_RUN. Никогда не выдумывай запуск тестов, commit, PR, benchmark или содержимое недоступного файла.

## Work unit

Один worker = один Issue = одна короткая ветка = один PR. Разрешённые пути и out-of-scope заданы Issue. Не исправляй соседние модули заодно. Не меняй публичные контракты/торговую семантику молча; предложи ADR и останови только зависимую часть работы.

Не писать напрямую в main, не force-push, не merge/auto-merge, не менять visibility/access/billing/secrets без отдельного разрешения владельца. Разовое создание README в пустом репозитории уже выполнено для первого PR.

## Non-negotiable constraints

- MVP — recorder/replay/shadow alerts, не торговый бот.
- Rust critical path; Python только offline. Никаких NATS/Redis/HTTP между tick и decision в MVP.
- Integer ticks/quantity steps и checked arithmetic для исполнимых цен/объёмов. Floats допустимы для исследовательских признаков при явно заданной finite/rounding policy.
- Single writer per instrument/market state; bounded queues; потеря критического события не скрывается.
- Book gap/epoch change/overflow инвалидируют зависимые решения до resync/warm-up.
- REST не используется для склейки WS-deltas без доказанного общего sequence contract.
- Spot/futures/RPI имеют независимые книги и epochs.
- Exchange time не заменяет receive order; таймеры, конфигурация и внешние inputs тоже воспроизводимы.
- Уровень нельзя использовать до valid_from. Не переписывать прошлое после подтверждения pivot.
- TradePlan — immutable intent; revocation/expiry отдельны; старые epochs и повторная доставка не дают повторных действий.
- Не выдавать spoof/iceberg/large-player proxy за установленную личность или истинный размер скрытой заявки.
- Source-derived правило, инженерный порог и гипотеза имеют разные provenance.
- Не публиковать закрытые материалы, API-ключи, cookies, аккаунтные данные или raw market archives.

## Verification

На bootstrap Rust-команды ещё не существуют. BOOT-001 создаёт workspace и фиксирует toolchain. После этого выполнить и указать результаты:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Дополнить релевантными negative/property/golden tests по Issue. Фиксировать commit, команды, exit codes и ограничения. Не называть тест, не выполненный в текущей среде или CI, пройденным. CI зелёный не означает доказанную корректность стратегии.

Заверши работу PR и `docs/handoffs/TASK-ID.md` по шаблону. Передай base/head SHA, список файлов, реальные проверки, deviations и blockers. Не закрывай чужие задачи. Чат можно удалить после сохранения всех результатов и приёмки handoff.
