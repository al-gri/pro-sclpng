---
name: Sequential engineering task
about: Один проверяемый результат для одного текущего исполнителя
labels: ''
assignees: ''
---

## Task ID / цель и результат

## Dependencies / base / assignment
Фактический base при claim, один executor/role, branch; risk и нужен ли independent QA.

## Scope и contracts
Allowed modules/paths; ссылки на accepted specs/ADR; out of scope. Shared files — Integrator.

## Acceptance / evidence
Применимые positive/negative/replay checks; real vs synthetic; обязательное evidence.
Feasibility probes выполняются внутри implementation. Не требовать готового adapter до старта ветки.

## Start / stop / blocker
Start: scope, available input contracts, один исполнитель и способ сборки.
Stop: непринятый contract/ADR, scope violation, конкретное небезопасное поведение.
Недоступный live блокирует live acceptance. При откладывании сохранить branch/patch, owner/action/resume condition.

## Delivery и автоматическая передача
Один PR report/Handoff с actual head/checks/limitations; отдельный Handoff при необходимости.
Integrator сам выдаёт один следующий шаг и полный copy-ready prompt/packet нужной роли; уникальные материалы сохраняются в GitHub. Не ждать просьбы владельца. Merge — владелец.
