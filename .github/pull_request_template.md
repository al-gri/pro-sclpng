## Проблема и результат

TASK / canonical Issue pointer / PACKET_VERSION:
UI_STAGE / DETAILED_STATUS:
START_SHA / HEAD_SHA (FINAL_SHA) / TREE:
TARGET_BRANCH / TARGET_SHA / MERGE_SHA_IF_TESTED:
BRANCH / WORKSPACE / WRITER / SHARED_FILES_OWNER:
RISK / REASON / VERIFICATION_TYPE / REQUIRED_GATES:
EXECUTOR_EXCEPTION / REASON / SCOPE (если Integrator автор):
Governance: [WORKFLOW](../docs/WORKFLOW.md)

## Scope / accepted contracts

CHANGED_PATHS / accepted inputs/ADR/invariants / deviations:
KNOWN_UNKNOWNS_PRESERVED / authority:
HANDOFF_PATH = этот PR report либо explicit отдельный файл:

## Проверки и evidence

| Criterion | Procedure | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | Executor / environment / exit / CI or log URL |
|---|---|---|---|

Авторские checks, independent review/QA и CI отдельно. Runtime/live не выдумывать; reuse evidence имеет applicability. LOW только опечатки/несемантическое оформление; governance/specs/contracts/security/изменения gate не LOW. STANDARD требует fresh independent targeted review, STRICT — все accepted QA gates; обязательный CI сохраняется.

## Freeze / review

IMPLEMENTATION_COMPLETE / SOURCE_FROZEN receipt / exact HEAD_SHA / TREE / TARGET_SHA:
CLEAN_STATUS / REVIEW_EXECUTOR / fresh context and independence:
VERDICT_SCOPE / FINAL_VERDICT = PASS | FAIL | BLOCKED:
FINDINGS / source evidence or reproduction / runtime status / report URL:

LOW без independent review указывает AUTHOR_CHECK и не выдумывает QA_PASS. Source frozen до итога либо явной отмены; любой commit → новый итог по риску. Receipts в comments, не содержащий собственный SHA commit. QA временные reproductions только в своей копии; fixes — назначенный автор через Integrator, same Issue/branch/PR.

## Recovery / один следующий шаг

Blocker / action owner / resume condition / dirty files or retained patch:
NEXT_EXECUTOR / actual agent status / unavailable spawn reason:
Canonical checkpoint / versioned full role-transfer packet URL:

Не копировать полный packet на каждое микродействие. Контекст исполнителя заменяем после durable result. QA PASS не принимает ADR/milestone и не разрешает merge.

## Перед merge

- [ ] Scope/contracts/technical gates сохранены; секретов/private sources/raw archives нет.
- [ ] Риск обоснован, его review/QA и required CI выполнены на actual candidate; missing evidence раскрыто.
- [ ] HEAD/TARGET и up-to-date protections перепроверены, blocking findings отсутствуют.
- [ ] Receipt/checkpoint/следующий шаг сохранены; required Architecture/owner решения получены.
- [ ] Владелец отдельно разрешил merge этого готового candidate; future auto-merge не разрешён.
