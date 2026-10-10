# Handoff: TASK-ID

Обычный PR report достаточен. Отдельный файл — для сложной/прерванной передачи/milestone либо explicit deliverable. Issue хранит один canonical pointer, остальные места ссылаются на result. Полный уникальный packet нужен при реальной смене роли; микрообновление не повторяет его.

```text
TASK / PACKET_VERSION / UI_STAGE / DETAILED_STATUS / ISSUE / PR
LAST_AUTHORIZED_TRANSITION / AUTHORITY
ROLE / ACTUAL_EXECUTOR / WORKSPACE / WRITER / SHARED_FILES_OWNER
EXECUTOR_EXCEPTION / REASON / SCOPE (если Integrator автор)
START_SHA / HEAD_SHA (FINAL_SHA) / TREE / TARGET_BRANCH / TARGET_SHA
MERGE_SHA_IF_TESTED = фактический либо NOT_CREATED
BRANCH / CLEAN_STATUS_OR_DIRTY_FILES / RETAINED_PATCH
RISK / REASON / VERIFICATION_TYPE / REQUIRED_GATES
CHANGED_PATHS / KNOWN_UNKNOWNS_PRESERVED / HANDOFF_PATH
```

## Результат и границы

Objective/result; criterion IDs; accepted inputs/contracts/ADR; deviations/limitations. Worker/reviewer не принимают ADR/milestone и не расширяют scope.

## Реальные проверки

| Criterion | Procedure | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | Executor / environment / command / exit / evidence URL |
|---|---|---|---|

Авторские checks / independent review/QA / CI отдельно. Reused evidence имеет applicability. LOW — только опечатка/несемантическое оформление; governance/specs/contracts/security/изменение gate не LOW. STANDARD требует fresh independent targeted review; STRICT — все accepted QA gates. Required CI сохраняется.

## Freeze / review

IMPLEMENTATION_COMPLETE / SOURCE_FROZEN receipt:
HEAD_SHA / TREE / TARGET_SHA / clean snapshot:
REVIEW_EXECUTOR / fresh context / independence:
VERDICT_SCOPE / FINAL_VERDICT = PASS | FAIL | BLOCKED:
FINDINGS (criterion/path/expected/actual/impact/source evidence или reproduction; runtime NOT_RUN если не запускался):
REPORT_URL / UNVERIFIED_ITEMS:

LOW без независимого review записывает AUTHOR_CHECK, не выдумывает QA. Новый source commit требует нового review/freeze/итога по риску. QA не пишет delivery branch; временные reproductions в своей копии отражаются отдельно. FAIL → Integrator correction прежнему Worker по умолчанию, same Issue/branch/PR. После двух циклов той же ошибочной гипотезы изменить подход.

Commit не содержит собственного окончательного SHA: receipt публикуется после commit в Issue/PR comment/CI. Внутри branch Handoff ссылается на предшествующую проверенную версию; содержащий его commit проверяется отдельно. After-freeze receipts не меняют source.

## Recovery checkpoint / следующий шаг

LAST_AUTHORIZED_TRANSITION / packet version / authority / active agents:
FREEZE / completed criteria and evidence / findings/blockers:
Cause / owner action / retained refs/patch / resume condition:
NEXT_EXECUTOR / one next authorized action:
AUTOSPAWN = actual agent identity/status либо UNAVAILABLE_IN_CURRENT_ENVIRONMENT:
Canonical full role-transfer packet / copy-ready prompt URL:

Новый Integrator сверяет refs/checkout/ownership/authority, не повторяет завершённый шаг. Следующая delivery task — после принятия/явного откладывания текущей и в пределах bounded authority. Merge/Architecture/milestone остаются отдельными решениями; Goal/расписание не подразумеваются Handoff.
