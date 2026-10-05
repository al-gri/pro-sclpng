# ProScalping Radar

Bitget → recorder → replay → горизонтальные уровни → два сетапа → shadow-алерты.

**Начать здесь: [пошаговый запуск](docs/START_HERE.md).**

## Текущий статус

M0: организационная и архитектурная база. Исполняемый скринер, Cargo workspace и CI пока НЕ реализованы. Этот bootstrap предложен на review; слияние владельцем принимает baseline. Первая задача с кодом — [BOOT-001 / #2](https://github.com/al-gri/pro-sclpng/issues/2).

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
