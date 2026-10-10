# Продолжить разработку ProScalping

Начни с [короткого ежедневного маршрута и готовых заданий](DAILY_WORKFLOW.md). Для исполнителя: [AGENTS](../AGENTS.md) → канонический указатель текущего Issue → актуальный packet/PR и нужные разделы [WORKFLOW](WORKFLOW.md). [PROJECT_STATE](PROJECT_STATE.md) — карта milestone, [ROADMAP](ROADMAP.md) — зависимости. ARCHITECTURE/INVARIANTS/specs/ADR читать по scope, не всю историю каждый раз.

Integrator — заменяемая роль; восстановление идёт по GitHub checkpoint. Одна delivery task/primary branch/PR, один implementation executor и один автор workspace. Обычная реализация — Worker; независимая проверка по LOW/STANDARD/STRICT. Read-only snapshots допустимы. Полный уникальный packet сохраняется при передаче роли; микрообновления — короткие ссылки. [Подробный prompt Integrator](prompts/INTEGRATOR.md).

## Активация и сохранённая точка перехода

PR #43 — принятая последовательная база. Эта редакция действует после owner acceptance, merge [PR49](https://github.com/al-gri/pro-sclpng/pull/49) и canonical receipt [#48](https://github.com/al-gri/pro-sclpng/issues/48), как определено WORKFLOW. До этого действует база main и разрешённый governance scope. Текущий статус брать из canonical Issue pointer/actual refs; содержащий этот текст commit не объявляет себя принятым.

Snapshot 2026-10-10: M0 принят, M1 в работе; #45/Draft PR47 paused ради #48. E1-CORR-01 уже доставлен, не повторять. [Packet с revision appendix](task-packets/GOV-ORCH-001.md) задаёт следующий технический шаг после активации и сверки authority: sequential independent Architecture/source re-review existing corrected E1. Эта governance-ревизия #45 не возобновляет; незавершённому production scope не запускать full final QA.

При продолжении #45 читать принятую governance policy из actual main + #48/#49 receipt, даже если его branch process docs старые. Не менять/rebase его source ради инструкций перед review. Runtime/spec inputs — exact review head и принятые решения. Дальнейшие corrections — один Worker same Issue/branch/PR по новому packet.

## Границы

Governance/specs/contracts/security и изменение gate не LOW, даже docs-only. #48/PR49 требует independent documentary QA и existing CI; #45/M1 сохраняют STRICT gates. QA не пишет delivery branch, временные reproductions допустимы в своей копии. Новый candidate требует нового итога. Merge — отдельное owner решение; ADR/contracts/milestone — Owner/Architecture authority.

BOOT-001 и partial F1/F2 не перезапускать. Repo public: без ключей, приватных источников и raw archives. UNKNOWN, applicability/artifacts/proofs, budgets и usable_data gates сохраняются. Governance не принимает ADR0004/U1–U7/full production/M1. Proposed/Draft/CI PASS не равны acceptance.
