# Handoff: TASK-ID

Обычный PR report может быть Handoff; отдельный файл нужен при сложной/прерванной передаче/milestone или explicit deliverable. After-freeze Worker/QA/integration receipts сохранять в Issue/PR comments без нового source commit. Все уникальные объекты должны быть durable в GitHub; executor context disposable.

```text
TASK / STATUS / ISSUE / PR
ROLE / ACTUAL_EXECUTOR
EXECUTOR_EXCEPTION / REASON / SCOPE (если Integrator пишет source)
BASE_SHA / FINAL_SHA / TREE
BRANCH / RETAINED_PATCH
CHANGED_PATHS
KNOWN_UNKNOWNS_PRESERVED
HANDOFF_PATH = файл либо canonical PR report URL
```

## Результат и границы

Bounded objective/result; accepted inputs/contracts/ADR; deviations/limitations. Не принимать ADR/milestone или расширять scope результатом Worker/QA.

## Реальные проверки

| Check | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | Executor / command / exit / log or CI URL |
|---|---|---|

Developer checks / independent QA / CI отдельно. Не переносить PASS на новые bytes/executor; applicability прежнего evidence явная.

## Freeze и QA

IMPLEMENTATION_COMPLETE:
SOURCE_FROZEN receipt:
FINAL_SHA / TREE:
QA_EXECUTOR / independence:
FINAL_VERDICT = PASS | FAIL | BLOCKED:
FINDINGS / reproduction:
QA report URL:

Любой source commit после freeze требует нового review/freeze/QA. FAIL -> Integrator corrective packet -> один Worker, same Issue/branch/PR; QA source не исправляет. PASS -> Integrator receipt -> owner acceptance; merge/Architecture/milestone не автоматические.

## Blocker / один следующий шаг

Cause / owner action / resume condition; current task явно отложена до другой implementation.
NEXT_EXECUTOR:
AUTOSPAWN = фактический spawn/agent identity либо UNAVAILABLE_IN_CURRENT_ENVIRONMENT:
Canonical bounded Task Packet / полный copy-ready prompt URL:

Передачу готовит и запускает Integrator без напоминания при доступном orchestration; fallback сохраняет полный prompt. Исторические записи сохранять; уникального контекста только в чате нет.
