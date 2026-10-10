---
name: Sequential engineering task
about: Одна bounded задача; проверка по риску с сохранением обязательных gates
labels: ''
assignees: ''
---

## Канонический указатель / TASK

PACKET_VERSION / latest authorized transition receipt:
UI_STAGE = READY | IMPLEMENTING | VERIFYING | READY_TO_MERGE | DONE (либо BLOCKED/PAUSED):
DETAILED_STATUS / BRANCH / PR:
CURRENT_WRITER / WORKSPACE / FREEZE / BLOCKERS / NEXT_ACTION:

Этот блок указывает на единственный актуальный packet/result; история сохраняется. Последний произвольный comment не заменяет авторизованный переход.

## Bounded packet

GOAL / ACCEPTANCE (criterion IDs):
SCOPE / FILES_ALLOWED / NON_GOALS:
CONTRACTS / ACCEPTED_DECISIONS / DEPENDENCIES:
START_SHA / TARGET_BRANCH / TARGET_SHA:
ENVIRONMENT / CLEAN_CHECKOUT / TOOLCHAIN:
WRITER / SHARED_FILES_OWNER (paths, existing claim/transfer):
EXECUTOR_EXCEPTION / REASON / SCOPE (если Integrator автор):
RISK = LOW | STANDARD | STRICT / REASON / REVIEW_TYPE / REQUIRED_GATES:
AUTHORITY (link, bounded actions/queue, stops):
HANDOFF_TO:

LOW — только опечатка/несемантическое оформление. Governance/specs/contracts/security/изменение gate не LOW. STANDARD — Worker и fresh independent targeted review; STRICT — independent QA и принятые gates. Existing CI обязателен. Shared/lock/CI имеют одного packet owner; текущий claim сохраняется до передачи.

## Checks / evidence

| Criterion | Procedure / command | Expected result | Evidence / required independent run |
|---|---|---|---|

Feasibility probes внутри implementation; contract/budget/activation gates сохраняются. Live NOT_RUN блокирует live acceptance, synthetic не выдаётся за real capture.

## Start / stop / recovery

Start: accepted inputs/scope/authority, START_SHA, один автор и способ проверки.
Stops: непринятый contract/ADR, scope/ownership conflict, drift frozen source, missing required evidence.
Cause / owner action / retained refs or patch / resume condition:

## Delivery / review

Worker result: TASK/PACKET_VERSION/START_SHA/HEAD_SHA/TREE/TARGET_SHA/CHANGED_PATHS/CHECKS/KNOWN_UNKNOWNS_PRESERVED/HANDOFF_PATH/dirty files.
Integrator review → IMPLEMENTATION_COMPLETE/SOURCE_FROZEN/exact HEAD/TREE/TARGET → проверка по риску.
Verdict PASS/FAIL/BLOCKED; check-level NOT_RUN/NOT_APPLICABLE отдельно. Новый commit → новый итог; receipts в comments, не self-SHA commit. QA пишет временные reproductions только в своей копии.

## Передача / checkpoint

NEXT_EXECUTOR / actual agent status либо AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT:
Canonical versioned full packet / copy-ready prompt URL:
Freeze / evidence / unresolved findings / one next authorized action:

Полный уникальный пакет сохранять при реальном role transfer; микрообновления — delta/ссылки. Merge/ADR/milestone — отдельная authority. Goal/расписание/новые пользовательские чаты и сообщения другим чатам не подразумеваются задачей.
