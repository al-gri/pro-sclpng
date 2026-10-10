# Продолжить разработку ProScalping

M0 принят; M1 в работе. Начни с [AGENTS](../AGENTS.md), [PROJECT_STATE](PROJECT_STATE.md), [последовательного WORKFLOW](WORKFLOW.md), [ARCHITECTURE](ARCHITECTURE.md), [INVARIANTS](INVARIANTS.md) и текущего GitHub Issue/PR. BOOT-001 и partial F-1/F-2 повторно не запускать.

## Сейчас

1. Владелец принимает governance [PR #43](https://github.com/al-gri/pro-sclpng/pull/43). До merge новая process migration в Issues ожидает принятия в main.
2. Один Integrator читает актуальную базу и [свой prompt](prompts/INTEGRATOR.md).
3. Следующая единственная implementation — существующая [#45](https://github.com/al-gri/pro-sclpng/issues/45): способ сборки, probes внутри реализации, real capture → WAL → diagnostic replay.
4. Независимый QA подключается последовательно к готовому критическому head. Owner acceptance этого ограниченного результата не завершает M1.
5. Дальнейшие последовательные checkpoints — в [ROADMAP](ROADMAP.md); actual assignments/blockers — в Issues.

Integrator сам выдаёт полный copy-ready prompt и packet при нужной передаче. Владельцу не требуется каждый раз просить материалы или запускать несколько чатов. Для простой задачи Integrator может быть исполнителем. Один активный Issue/исполнитель/PR; completed Draft/backlog не означают параллельное исполнение.

## Границы

Merge остаётся за владельцем; настройки доступа/CI protection, force-push и execution этим процессом не разрешаются. Repo public: без ключей, приватных источников и raw archives. UNKNOWN, canonical applicability, artifacts/proofs и usable_data gates сохраняются. Новые policy/ADR не считаются принятыми только потому, что появились в Draft PR.
