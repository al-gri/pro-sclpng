# ProScalping Radar

Bitget → recorder → replay → горизонтальные уровни → два сетапа → shadow-алерты.

**Начать здесь: [ежедневный маршрут и готовые задания](docs/DAILY_WORKFLOW.md); [точка продолжения](docs/START_HERE.md).**

## Текущий статус

M0 принят; M1 в работе. Workspace/CI, decoder, WAL, DataHealth и supervisor library приняты; F-1/F-2 приняты только в partial diagnostic/synthetic scopes. Реальный capture и canonical local book ещё не приняты, usable_data=false. Актуальные accepted baseline и blockers — в [PROJECT_STATE](docs/PROJECT_STATE.md) и GitHub Issues.

Разработка последовательная: одна delivery task, один implementation executor/автор workspace, одна основная ветка и PR. Заменяемый Integrator восстанавливает работу по canonical Issue pointer; [WORKFLOW](docs/WORKFLOW.md) задаёт LOW/STANDARD/STRICT с обязательными technical/CI gates. Редакция GOV-ORCH-001 активируется после owner acceptance, merged [PR49](https://github.com/al-gri/pro-sclpng/pull/49) и canonical receipt [#48](https://github.com/al-gri/pro-sclpng/issues/48); actual статус проверяется по этим записям. Датированный checkpoint 2026-10-10 сохраняет #45/PR47 paused и выполненный E1-CORR-01: следующий технический шаг после активации и проверки authority — existing E1 independent Architecture/source re-review по [packet](docs/task-packets/GOV-ORCH-001.md), без restart.

## Источники истины

- [AGENTS.md](AGENTS.md) — обязательные инструкции каждому рабочему чату.
- [Текущее состояние](docs/PROJECT_STATE.md) — что принято, что отсутствует, что делать дальше.
- [Архитектура](docs/ARCHITECTURE.md) и [инварианты](docs/INVARIANTS.md).
- [Roadmap](docs/ROADMAP.md), [процесс работы](docs/WORKFLOW.md), [Definition of Done](docs/DEFINITION_OF_DONE.md).
- [Контракты](specs/README.md), [политика источников](docs/SOURCE_POLICY.md), [реестр правил](rules/README.md).
- [Промпт Integrator](docs/prompts/INTEGRATOR.md), [Worker](docs/prompts/WORKER.md), [QA](docs/prompts/QA.md).

## Границы проекта

Первая версия — Rust modular monolith и публичные market data. Только horizontal TrueBreakout/FalseBreakout после проверки recorder/replay. Нет Private API, выставления ордеров или обещаний доходности. Полный робот — отдельный будущий этап.

Разрешённый репозиторий: **только `al-gri/pro-sclpng`**. Чаты не являются долговременной памятью или разными GitHub-пользователями. Состояние сохраняется в Git, Issues, PR и handoff; merge выполняет владелец.

Репозиторий публичный. Не публиковать API-ключи, приватную переписку, полные тексты курса, персональные данные и большие raw datasets. Для больших данных — внешний архив и проверяемый manifest в Git.
