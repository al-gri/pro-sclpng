# Продолжить разработку ProScalping

M0 принят; M1 в работе. Прочитай [AGENTS](../AGENTS.md), [PROJECT_STATE](PROJECT_STATE.md), [WORKFLOW](WORKFLOW.md), [ARCHITECTURE](ARCHITECTURE.md), [INVARIANTS](INVARIANTS.md) и actual текущие Issue/PR. BOOT-001 и partial F1/F2 не перезапускать.

## Сейчас

1. Последовательная база PR #43 уже принята в main. GOV-ORCH-001 [#48](https://github.com/al-gri/pro-sclpng/issues/48) — отдельное изменение ролей, ожидающее owner acceptance/merge своего governance PR; technical gates сохраняются.
2. Постоянный Integrator читает actual refs и [свой prompt](prompts/INTEGRATOR.md). Он управляет и интегрирует; ordinary implementation делает один ephemeral Worker, separate QA проверяет exact frozen final SHA/tree.
3. Existing [#45](https://github.com/al-gri/pro-sclpng/issues/45) / Draft PR47 приостановлены на время единственной governance задачи. E1-CORR-01 уже доставлен, не повторять. [Migration/resume packet](task-packets/GOV-ORCH-001.md) задаёт продолжение с sequential Architecture/source re-review existing corrected E1 после governance acceptance.
4. При новых corrective findings Integrator запускает одного Worker в той же #45/branch/PR. Final independent QA незавершённой production implementation не запускается до IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / exact FINAL_SHA/TREE.
5. Дальнейшие checkpoints — [ROADMAP](ROADMAP.md); actual assignments/blockers — Issues. Ограниченная приёмка #45 сама по себе не завершает M1.

ONE ACTIVE TASK / ONE ACTIVE IMPLEMENTATION EXECUTOR / ONE BRANCH / ONE PR. Integrator сам сохраняет bounded prompt/packet и при доступном orchestration запускает Worker/QA последовательно. При unavailable spawn — полный copy-ready prompt и честный AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT. Не требовать Owner как курьера при доступном spawn. Integrator authorship допускается только с объявленным WORKFLOW exception; Worker/QA context disposable после durable GitHub handoff.

## Границы

Merge — отдельное решение владельца; ADR/contracts/milestone — Owner/Architecture authority. QA source не исправляет; FAIL -> Integrator -> corrective Worker -> новый freeze/QA. Во время QA source writes запрещены; новый commit отменяет final QA для нового SHA.

Repo public: без ключей, приватных источников и raw archives. UNKNOWN, applicability/artifacts/proofs, budgets и usable_data gates сохраняются. Governance не принимает ADR0004, U1–U7, full fork/production или M1; proposed/Draft/CI PASS не равны acceptance.
