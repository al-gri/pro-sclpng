# Instructions for all project agents

## Scope and first read

Работай только с `al-gri/pro-sclpng`. Не просматривай другие репозитории пользователя. Внешние технические источники не заменяют авторские торговые правила.

GitHub — единственный источник истины. Прочитай [WORKFLOW](docs/WORKFLOW.md), [PROJECT_STATE](docs/PROJECT_STATE.md), [ARCHITECTURE](docs/ARCHITECTURE.md), [INVARIANTS](docs/INVARIANTS.md), текущий Issue/PR и релевантные принятые specs/ADR. Приоритет имеет явно принятое owner scope/security решение, затем accepted contracts и действующие инструкции; proposed/Draft не равны accepted.

До изменений зафиксируй настоящий base, scope, чистоту checkout и фактический способ сборки. Полный environment preflight повторяй при смене executor/toolchain/доступа, не после каждого docs commit. Отсутствующие проверки — NOT_RUN, выполненные с ошибкой — FAIL. Не выдумывай команды, commit, PR, benchmark или PASS.

## Последовательная работа и приёмка

ONE ACTIVE TASK / ONE ACTIVE IMPLEMENTATION EXECUTOR / ONE BRANCH / ONE PR. Integrator owns orchestration and integration. Implementation is normally delegated to exactly one ephemeral Worker; independent ephemeral QA проверяет source после IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / exact FINAL_SHA/TREE. Integrator — постоянный orchestrator, а не default feature executor. Самостоятельные shared/governance/integration edits разрешены только по WORKFLOW с явно записанными EXECUTOR_EXCEPTION=INTEGRATOR, REASON и SCOPE; их выполняют последовательно после остановки Worker.

QA не исправляет source. FAIL -> Integrator corrective packet -> один Worker, same Issue/branch/PR -> новый head -> review/freeze -> QA заново. Во время QA source branch не изменяется; любой новый commit отменяет final QA acceptance для нового SHA, включая docs/lock/CI. Итоговый verdict только PASS/FAIL/BLOCKED. Другую задачу начинать после принятия либо явного откладывания текущей с GitHub resume handoff. Исторические Draft PR/backlog не дают разрешения на параллельное исполнение.

Dependency/handshake/allocation probes входят в implementation, а не требуют готовой реализации до создания ветки. Их обязательные acceptance требования, численные бюджеты и контрактные stops сохраняются. Отсутствие live evidence блокирует live acceptance, но не независимую разрешённую offline-часть.

Scope и файлы заданы Issue. Не исправляй соседние модули заодно. Публичные контракты/торговую семантику меняй только после принятого ADR; останови зависимую часть. Все final candidates получают отдельный QA после exact freeze, с применимыми проверками по WORKFLOW; критические WAL/sequence/epochs/quantity/ownership/transport/лимиты и milestone сохраняют дополнительные независимые gates. Самопроверка автора не является независимым QA.

Не писать напрямую в main, не merge/auto-merge, не force-push, не менять visibility/access/billing/secrets без отдельного разрешения владельца. Merge остаётся за владельцем.

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

## Verification and handoff

Для Rust использовать зафиксированный toolchain и применимые команды:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Фиксируй actual head, среду, команды/exit codes или CI refs. Новые зависимости требуют подготовки locked cache до offline checks; Cargo.lock/workspace/CI меняет Integrator последовательно. Не переносить evidence между изменёнными runtime bytes или executors без проверки применимости.

PR report — Handoff завершённой задачи. Отдельный [Handoff](docs/templates/HANDOFF.md) нужен для прерывания/сложной передачи/milestone либо если прямо задан packet. Исторические packets/handoffs/receipts сохранять; restrictions на закрытие tracking Issues действуют.

## Обязательная автоматическая передача

При каждом переходе/завершении/blocker Integrator сам сохраняет в GitHub один bounded packet и полный copy-ready prompt: Issue/PR, actual base/head/tree, branch/executor/environment, inputs/contracts, files_allowed, acceptance/evidence, stops и результат. При доступном subagent orchestration автоматически запускает одного Worker и затем, после freeze, отдельного QA. Если spawn действительно недоступен, NEXT_EXECUTOR=WORKER (или QA) / AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT и полный prompt; нельзя выдумывать запуск. Worker/QA context disposable; уникальные решения/result/CI receipts/Handoff сохраняются в GitHub до удаления. Не ждать просьбы владельца и не запускать будущие роли одновременно. Это не scheduled automation и не разрешение merge. Полное правило находится в [WORKFLOW](docs/WORKFLOW.md#автоматическая-подготовка-следующего-шага-и-передачи).
