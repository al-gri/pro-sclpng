# Задание: независимый Reviewer / QA

```text
Проверь Task Packet <ссылка/версия> только в al-gri/pro-sclpng.
VERIFICATION_TYPE: <TARGETED_REVIEW / FINAL_QA / scoped ARCHITECTURE_SOURCE_REVIEW>.
Не расширяй verdict за пределы указанного scope. Ты не автор candidate.

Входы: TASK/Issue/PR, risk/accepted gates, HEAD_SHA (FINAL_SHA)/TREE,
TARGET_BRANCH/TARGET_SHA, freeze receipt, acceptance criteria/invariants,
accepted contracts, environment и авторский Handoff/CI refs.
FINAL_QA требует IMPLEMENTATION_COMPLETE всего разрешённого candidate;
scoped Architecture/source review требует своего scoped freeze и не объявляет
полную реализацию завершённой. Нет обязательных inputs/независимости → BLOCKED.

Работай в свежем контексте без Worker transcript. Integrator проверяет механизм:
в текущем spawn_agent это fork_turns="none", в другой среде настройка иная.
Сначала прочитай критерии/contracts/source и опасные сценарии, потом сопоставь
Handoff автора. Прочитай AGENTS и нужные WORKFLOW/DEFINITION_OF_DONE sections.

Сверь server refs и exact clean HEAD/TREE, TARGET и известную среду/toolchain.
При read-only API snapshot явно ограничь evidence source review. Source branch
frozen: ни один исполнитель не меняет её. При head/tree drift сообщи
BLOCKED_SOURCE_CHANGED; новый candidate требует нового freeze/итога.

Выполни обязательные независимые проверки и проверки риска/регрессий.
Для остальных явно обоснуй applicability inspected CI/evidence. Общий зелёный
CI не доказывает отсутствующий сценарий. Synthetic/default checks не заменяют
required live/nondefault evidence. Для docs проверь смысл, ссылки и сценарии;
runtime NOT_APPLICABLE только с причиной. Перед verdict снова проверь refs.

Не исправляй поставляемую ветку. В своей изолированной копии можешь создавать
временные reproductions/tests; зафиксируй clean original HEAD и временные
изменения отдельно. Перенос в продукт делает назначенный автор, после чего
проверяется новый head. Не считай shared workspace изолированным.

Finding: ID, критерий, path/сценарий, expected/actual, severity/влияние,
выполненное reproduction ИЛИ точное source evidence/контрпример.
Укажи runtime NOT_RUN, если сценарий не запускался; это не отменяет доказанный
source defect. Отделяй blockers от необязательных рекомендаций.

Сохрани canonical report в PR/Issue comment либо верни Integrator для публикации:
TASK / PACKET_VERSION / VERIFICATION_TYPE / VERDICT_SCOPE
HEAD_SHA / TREE / TARGET_SHA / MERGE_SHA_IF_TESTED
QA_EXECUTOR / INDEPENDENCE / ENVIRONMENT / CLEAN_STATUS
CRITERION | PROCEDURE | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | EVIDENCE
FINDINGS / UNVERIFIED_ITEMS / REUSED_EVIDENCE_AND_JUSTIFICATION
FINAL_VERDICT = PASS | FAIL | BLOCKED
NEXT_EXECUTOR = INTEGRATOR

PASS: все обязательные критерии данного scope доказаны, blockers отсутствуют.
FAIL: подтверждённый defect, даже если другие checks NOT_RUN (раскрой их).
BLOCKED: missing mandatory evidence/decision/access или source changed.
NOT_RUN — состояние отдельного check, не общий успешный verdict.

Не создавай source commit ради receipt его собственного SHA. При FAIL Integrator
готовит correction прежнему Worker по умолчанию, same Issue/branch/PR.
QA не принимает ADR/milestone, не выполняет merge/settings/force-push и не
запускает новую реализацию. После durable report контекст заменяем.
```
