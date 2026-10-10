## Проблема и результат

TASK / Issue:
STATUS:
BASE_SHA:
FINAL_SHA / TREE:
Branch:
Worker / Integrator / independent QA identities:
EXECUTOR_EXCEPTION / REASON / SCOPE (если Integrator автор):
Governance: [WORKFLOW](../docs/WORKFLOW.md)

## Scope / accepted contracts

CHANGED_PATHS; accepted inputs/ADR/invariants; scope deviations.
KNOWN_UNKNOWNS_PRESERVED:
HANDOFF_PATH = этот canonical PR report либо отдельный файл:

## Проверки и evidence

| Check | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | Executor / command / exit / CI or log URL |
|---|---|---|

Developer checks, independent QA и CI отдельно. Документальные checks по scope; runtime/live evidence не выдумывается и имеет explicit applicability.

## Freeze / независимый QA

IMPLEMENTATION_COMPLETE:
SOURCE_FROZEN receipt / exact FINAL_SHA / TREE:
QA_EXECUTOR / independence:
FINAL_VERDICT = PASS | FAIL | BLOCKED:
FINDINGS / report URL:

После freeze source branch не меняется; any source commit -> new review/freeze/QA. After-freeze receipts — comments, не source commits. FAIL исправляет Worker через Integrator, same Issue/branch/PR; QA source не пишет.

## Ограничения / blockers / один следующий шаг

Missing evidence / preserved UNKNOWN / real-synthetic provenance:
Cause / action / resume condition:
NEXT_EXECUTOR:
Actual AUTOSPAWN / fallback reason:
Canonical saved bounded packet / full copy-ready prompt URL:

Контекст Worker/QA disposable после durable сохранения. QA PASS не принимает ADR/milestone и не разрешает automatic merge.

## Перед merge

- [ ] Scope/contracts/technical gates сохранены; секретов/private sources/raw archives нет.
- [ ] Применимые checks реальны; обязательное missing evidence отмечено.
- [ ] Независимый QA PASS и требуемые review/CI относятся к exact frozen FINAL_SHA/TREE.
- [ ] Acceptance/integration receipt сохранён, следующий шаг/packet подготовлен.
- [ ] Owner/Architecture decisions для scope/ADR/milestone получены где требуются.
- [ ] Владелец отдельно принимает merge; auto-merge не включено.
