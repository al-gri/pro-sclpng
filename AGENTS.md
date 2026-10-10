# Instructions for all project agents

## Scope and first read

Работай только с `al-gri/pro-sclpng`; другие репозитории пользователя не просматривай. Внешние технические источники не заменяют авторские торговые правила.

GitHub — каноническая память. Начни с этого файла, канонического указателя текущего Issue, актуального packet/PR и нужных разделов [WORKFLOW](docs/WORKFLOW.md). [PROJECT_STATE](docs/PROJECT_STATE.md) — карта, не замена actual refs. Читай ARCHITECTURE/INVARIANTS и accepted specs/ADR по затронутому scope; всю историю на каждом шаге загружать не нужно. Явно принятое owner scope/security решение имеет приоритет, затем accepted contracts и действующие инструкции; proposed/Draft не равны accepted. Условия активации этой редакции — в WORKFLOW; containing commit не объявляет собственное принятие.

Перед start/resume/передачей/freeze/merge сверь actual START/HEAD/TARGET refs, clean checkout либо сохранённые dirty files, scope/authority и единственного автора. Полный environment preflight повторяй при смене executor/toolchain/access; внутри этапа проверяй изменившиеся предпосылки. NOT_RUN не PASS; не выдумывай команды, commit, PR, benchmark или результаты.

## Последовательная работа и приёмка

Одна active delivery task, один implementation executor, один автор рабочей копии, одна основная branch/PR. Изолированные read-only/review snapshots допустимы; параллельная feature delivery не разрешена. Integrator — заменяемая роль, восстанавливаемая по Issue checkpoint. Обычный feature пишет Worker; Integrator — только документированный LOW/integration exception с REASON/SCOPE/WORKSPACE/WRITER. Shared/lock/CI files имеют одного явно назначенного владельца в bounded packet, существующий claim сохраняется до передачи.

- LOW — только опечатки/несемантическое оформление: один автор, diff и подходящие checks.
- STANDARD — поведенческие изменения: Worker, авторские checks, Integrator и свежий независимый целевой review.
- STRICT — governance/specs/contracts/security и критические boundaries: independent QA и все accepted gates. Governance/spec/security/изменения статуса gate никогда не LOW, даже docs-only. #48/PR49 требует documentary QA и existing CI; #45/M1 gates неизменны.

Перед review/QA — exact HEAD_SHA (FINAL_SHA)/TREE/TARGET_SHA, известная среда и freeze; source writers остановлены. Любой source commit требует нового candidate и нового итога по риску. QA не меняет поставляемую ветку; временные reproductions допустимы в своей изолированной копии. Source-evidence finding допустим с честным runtime NOT_RUN. Verdict PASS/FAIL/BLOCKED отделён от check-level результатов.

FAIL → Integrator packet → прежний Worker по умолчанию, same Issue/branch/PR → review/freeze и новая проверка. После двух циклов той же ошибочной гипотезы изменить подход/передать спор Architect. Не начинать другую delivery задачу до принятия либо явного откладывания текущей с resume checkpoint.

Не расширяй Issue scope. Public contracts/торговую семантику меняй только после принятого ADR; зависимую часть останови. Dependency/handshake/allocation probes входят в implementation; их acceptance budgets/stops сохраняются. Нет live evidence → нет live acceptance; разрешённая offline-часть может продолжаться. Самопроверка не заменяет независимый review или M1 QA.

Не писать напрямую в main, не merge/auto-merge/force-push/settings/access/billing/secrets без отдельного owner разрешения. Merge для конкретного готового результата остаётся за владельцем; этот процесс не делегирует будущие merges.

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

Для Rust применять pinned toolchain и проверки по scope:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Фиксируй actual candidate/target, среду, команды/exit codes/CI refs. New dependencies требуют locked cache preparation до offline checks. Existing mandatory CI и technical gates не отменяются уровнем риска.

В Issue один canonical pointer; PR report — результат/Handoff. Полный уникальный versioned packet/copy-ready prompt нужен при фактической передаче роли; микродействия используют delta и ссылки без повторения полных prompts. After-freeze receipts публикуются comments: commit не может содержать собственный окончательный SHA. Исторические packets/handoffs/receipts и tracking Issue restrictions сохранять.

Integrator сам готовит следующий разрешённый шаг, сохраняет checkpoint и запускает роли через доступные subagents по risk route. Свежий review/QA без Worker transcript: текущий механизм spawn_agent требует `fork_turns="none"`; механизм другой среды проверить. При unavailable spawn — полный packet, NEXT_EXECUTOR и честный AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT. Новый пользовательский чат/сообщение другому чату требуют соответствующей явной пользовательской авторизации. Goal/расписание — только по отдельному запросу, не обещание фоновой работы и не расширение scope/merge rights.

Подробнее: [автоматическая передача](docs/WORKFLOW.md#автоматическая-подготовка-следующего-шага-и-передачи), [короткий запуск](docs/DAILY_WORKFLOW.md), [Handoff](docs/templates/HANDOFF.md).
