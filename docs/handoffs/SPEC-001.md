# Handoff: SPEC-001 — первая контрольная точка

Status: **PARTIAL / DESIGN_REVIEW_REQUIRED**. Новые контракты и ADR: **PROPOSED**.
Issue: https://github.com/al-gri/pro-sclpng/issues/3
PR: https://github.com/al-gri/pro-sclpng/pull/10 — **Draft**, target main.
Claim: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901
Packet: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559
Role/session: WORK — SPEC-001, один worker, только al-gri/pro-sclpng.
Base SHA: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Branch: `feat/SPEC-001-domain-contracts`.
Proposal source до добавления этого handoff: `91933ff248b42f386f9b8891c6f305157a5f30fa`.
Tested final head SHA: **post-commit evidence в PR #10**; будущий hash содержащего этот файл commit сюда не вписывается. Локальные Rust tests NOT_RUN. Таблица ниже — состояние при написании handoff, не подстановка base CI вместо проверки последнего PR head.

## Что сделано

Прочитан pinned GitHub context: AGENTS.md, все перечисленные packet docs, root manifests/lock/toolchain и весь текущий crates/domain, затем весь Issue #3 с комментариями и Issues #4/#6. Перед claim и перед публикацией повторно проверены main и наличие чужой работы. Main совпал с expected base; PR #9 действительно merged в этот SHA; отдельный push-run 37307781420 completed/success. Чужой claim, рабочая ветка или PR до claim не обнаружены; после claim найден только свой комментарий. ADR-0002 был свободен.

Подготовлены четыре согласуемых проекта контрактов, ADR и test matrix. Статусы PROPOSED, источники инженерных параметров/UNKNOWN обозначены. Запрос Architecture/Integrator design review опубликован в Draft PR #10; отдельного принятого review на момент handoff **нет**.

**На этой контрольной точке реализация остановлена.** Нет изменений Rust types/validators, reference codec/model или executable contract tests. Production network/book/recorder/replay и дальнейшие торговые компоненты не реализуются. SPEC-001 не объявляется DONE, Issue #6 не закрывается.

## Изменённые файлы

Ровно семь новых документов:

```text
specs/domain/types-v1.md
specs/domain/test-matrix-v1.md
specs/market-data/events-v1.md
specs/market-data/data-health-v1.md
specs/recording/wal-v1.md
docs/adr/0002-domain-event-wal-contracts.md
docs/handoffs/SPEC-001.md
```

Все внутри allowed paths packet. Root Cargo.toml/Cargo.lock, rust-toolchain.toml, CI, crates/domain, apps/radar, README, PROJECT_STATE, specs/README.md, прежние ADR/handoffs не изменяются. std-only, Rust 1.98.1 и исходный lockfile сохранены.

## Проверки

| Command/check | PASS/FAIL/NOT_RUN | Environment / exit code / evidence |
|---|---|---|
| Pinned reads, main SHA, PR #9, branch/PR/claim checks | PASS | GitHub API, не shell execution; evidence в claim и PR; база точно 6c520237d35865c79dba9e74fa64bd4c2c9e419f |
| Shell/Git availability | PASS | Linux x86_64, Bash 5.2.37, Git 2.47.3; Rust/Cargo/rustup отсутствуют |
| `git clone --no-checkout` только разрешённого repo | FAIL | exit 128: Could not resolve host: github.com; локальный checkout не создан |
| Локальные checkout SHA/cleanliness | NOT_RUN | Нет checkout; GitHub API objects/local staging не выдаются за checkout |
| Ручная сверка внутренних относительных links/status/provenance и allowed paths proposal | PASS | Проверка текста/путей, не компиляция и не независимый QA; окончательный diff сверяется в PR |
| Offline integer illustrations и CRC/golden length calculation | PASS | exit 0; Python 3.13.5, zlib build/runtime 1.3.1 + отдельный bit-at-a-time loop; W01 header/payload/frame=32/38/74, CRC=9E02C413; не Rust codec test |
| `cargo build --workspace --locked` локально | NOT_RUN | Rust/Cargo и checkout недоступны |
| `cargo fmt --all -- --check` локально | NOT_RUN | То же |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` локально | NOT_RUN | То же |
| `cargo test --workspace --locked` локально | NOT_RUN | То же |
| `cargo test -p domain --locked` отдельной командой | NOT_RUN | Локально Cargo отсутствует; existing workflow содержит workspace test, не эту отдельную команду |
| Fresh final-head PR CI | NOT_RUN в момент написания этого файла | Итоговые run/check/step/decoded-log evidence и exact SHA публикуются в PR после последнего commit; требуются rust-fmt, rust-clippy, rust-tests и actual checkout SHA |
| Новые Rust contract vectors, reference model/codec | NOT_RUN / NOT_IMPLEMENTED | Ожидают design approval; matrix не является исполнимыми tests |
| Windows 11 x64 / PowerShell 5.1 checks | NOT_RUN | Среда владельца, evidence не получено; Linux CI не Windows |
| Filesystem crash/fsync/power-loss correctness | NOT_RUN | OUT_OF_SCOPE SPEC-001; downstream REC-001 |

Preflight CI evidence **только базы**: [push-run 37307781420](https://github.com/al-gri/pro-sclpng/actions/runs/37307781420), event push/main, exact base, attempt 1, completed/success. Проверены все job/step summaries; прочитан полный [rust-tests log 111755538555](https://github.com/al-gri/pro-sclpng/actions/runs/37307781420/job/111755538555). Там actual checkout SHA=base, Ubuntu 24.04.5 LTS x86_64, Rust/Cargo 1.98.1, build/test/lockfile/cleanliness success, 15 CLI tests и 0 новых domain tests. Полные base fmt/clippy logs этим worker не перечитывались; их status/steps проверены. Это не запуск нового CI worker и не проверка новых спецификаций.

Existing CI делает отдельный реальный clean checkout exact PR head, устанавливает и проверяет pin, выполняет обязательные steps и проверяет неизменность tracked files. Post-commit PR evidence должен различать PR event/synthetic merge SHA и реально tested source head; старый opening head не подменяет final после handoff. Зелёные BOOT-001 tests не принимают proposed API/wire format.

## Контракты / ADR

[ADR-0002](../adr/0002-domain-event-wal-contracts.md) — PROPOSED, с D1–D6 decision/review questions.
[Types](../../specs/domain/types-v1.md), [events](../../specs/market-data/events-v1.md), [health](../../specs/market-data/data-health-v1.md), [WAL](../../specs/recording/wal-v1.md), [matrix](../../specs/domain/test-matrix-v1.md) — PROPOSED.

Ключевые кандидаты: exact qualified u64 values/u128 conversion; immutable identity/spec/epoch; recorded causal order и stable sub-event IDs; четыре оси health; raw/control-only bounded WAL; отдельные completion/quality/durability; fail-closed loss/unknown/version handling.
Единственный 74-byte ArchiveStart golden проверен как документационное число/bytes; full multi-frame golden, negative assertions и bounded exhaustive Rust loops ещё не реализованы. Статус accepted требует установленного review/owner workflow, не заявления worker.

## Deviations и известные ограничения

Локальное выполнение Git clone заблокировано DNS, Rust отсутствует. Proposal создан через GitHub API; локального checkout нет. Доступный путь реального checkout/build/test — существующий CI, без изменения workflow. Это явное ограничение среды, не «локальный PASS». Недоступность локального инструмента не блокирует независимый docs-only proposal.

Optional `expected_sha` у update_ref отклонён connector на argument binding. Branch head повторно прочитан и совпал с expected base, затем использован обычный fast-forward, force=false. No force-push, merge/auto-merge, settings changes или прямой push main.

В packet отмечено OWNER_ACTION_REQUIRED для required checks/up-to-date protection. Эта сессия не проводила новый полный аудит защиты и не меняет настройки. Это inherited integration/owner вопрос до merge, не основание выдавать незапущенный check за PASS.

## Blockers / открытые Issues

**DESIGN_REVIEW_REQUIRED:** Architecture/Integrator должен явно рассмотреть D1–D6 на указанном в PR head. Открыты numeric bounds/overflow policy; normalizer/profile revision identity/bundle; quiet/verification proof policy и epoch scope; writer rebind restriction; WAL tags/caps/CRC/seals и storage acknowledgement boundary. Worker не продолжает Rust API до согласования.

**BLOCKED_BY_MD_001 (#4)** для непроверенных реальных Bitget identity/units/increments, sequence bridge, zero-size mapping, aggressor/RPI, timestamp/quiet/resync/timeout evidence. Generic numeric/order/health/WAL synthetic contracts независимы.

**QA-001 (#6)** остаётся open: нужен независимый negative design review и затем проверка конкретного implementation SHA. Этот worker не считается независимым QA своего proposal. REC-001 обязан реализовать/доказать storage/OS behavior отдельно.

## PowerShell 5.1 — команды владельцу, NOT_RUN

Из отдельного чистого checkout final SHA из PR, с установленными Git, Rustup toolchain 1.98.1 (rustfmt/clippy) и platform linker. Заменить только ExpectedHead; failure останавливает script. `$LASTEXITCODE` проверяется немедленно после каждой native-команды; Bash export и `&&` не используются.
Справка: [Microsoft PowerShell 5.1 automatic variables](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_automatic_variables?view=powershell-5.1).

```powershell
$ErrorActionPreference = 'Stop'
$env:RUSTUP_TOOLCHAIN = '1.98.1'
$env:CARGO_NET_OFFLINE = 'true'
$ExpectedHead = 'COPY_FINAL_HEAD_FROM_PR'
if ($ExpectedHead -notmatch '^[0-9a-f]{40}$') { throw 'Set exact final PR SHA first' }

$actualHead = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'git rev-parse failed' }
if ($actualHead -ne $ExpectedHead) { throw 'Wrong checkout SHA' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'git status failed' }
if ($status) { throw 'Checkout is not clean' }

$active = rustup show active-toolchain
if ($LASTEXITCODE -ne 0) { throw 'rustup active-toolchain failed' }
Write-Output $active
if ($active -notmatch '^1\.98\.1-') { throw 'Wrong active toolchain' }
$compiler = rustc --version
if ($LASTEXITCODE -ne 0) { throw 'rustc version failed' }
Write-Output $compiler
if ($compiler -notmatch '^rustc 1\.98\.1(\s|$)') { throw 'Wrong rustc release' }
rustc --version --verbose
if ($LASTEXITCODE -ne 0) { throw 'rustc verbose version failed' }
cargo --version
if ($LASTEXITCODE -ne 0) { throw 'cargo version failed' }

cargo build --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw 'cargo fmt failed' }
cargo clippy --workspace --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { throw 'cargo clippy failed' }
cargo test --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'workspace tests failed' }
cargo test -p domain --locked
if ($LASTEXITCODE -ne 0) { throw 'domain tests failed' }

git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw 'Cargo.lock changed' }
git diff --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Tracked files changed' }
git diff --cached --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Index changed' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'Final git status failed' }
if ($status) { throw 'Final checkout is not clean' }
```

Эти команды не запускались на Windows. При передаче owner evidence нужны exact SHA, OS/target/toolchain, exit codes и logs; отсутствие выводов не заменяет проверку.

## Артефакты

Все уникальные design решения и golden bytes сохранены в семи Git-документах и PR #10; claim в Issue #3 содержит preflight evidence. Нет секретов, live feed archives или материалов других репозиториев. Final SHA и свежие CI URLs/результаты — post-commit PR comment.

## Следующий шаг

**Architecture/Integrator: выполнить design review D1–D6 в Draft PR #10 на final head и явно разрешить согласованную реализацию либо перечислить изменения.** До этого worker остановлен на КП1; API/wire format не приняты, merge не запрошен.

## Данные для замены чата

Читать AGENTS, packet/claim Issue #3, PR #10 (особенно final-head evidence/review), затем ADR/specs/matrix и этот handoff. Непринятые решения не превращать в accepted при переносе чата. Незакоммиченной реализации нет; handoff принимает Integrator/владелец, не его автор.
