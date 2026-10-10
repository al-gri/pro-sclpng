---
name: Sequential engineering task
about: Одна bounded задача; ephemeral Worker и независимый QA после freeze
labels: ''
assignees: ''
---

## TASK / objective / result

## Lifecycle / dependencies / assignment

STATUS = READY_FOR_WORKER | WORKER_ACTIVE | IMPLEMENTATION_READY | INTEGRATOR_REVIEW | SOURCE_FROZEN | QA_ACTIVE | QA_FAILED | CORRECTION_READY | QA_PASSED | OWNER_ACCEPTANCE | ACCEPTED_IN_MAIN | BLOCKED_*
BASE_SHA:
BRANCH / PR:
Integrator / one Worker / separate QA identity:
EXECUTOR_EXCEPTION / REASON / SCOPE (только если Integrator пишет):
Environment / pinned toolchain / actual spawn capability:

## Bounded inputs / scope / contracts

Accepted specs/ADR/invariants; files_allowed; forbidden scope. Root Cargo.toml/Cargo.lock/workspace/CI — sequential Integrator ownership; никаких concurrent writers.

## Acceptance commands / evidence

Applicable positive/negative/replay checks; actual/synthetic provenance; required evidence.
Feasibility probes внутри implementation; contract/budget/activation gates сохраняются.

## Start / stop / blocker

Start: accepted inputs/scope, exact base, один implementation executor и способ проверок.
Stop: непринятый contract/ADR, scope violation, source changed during QA, unsafe behavior.
Live NOT_RUN блокирует live acceptance. Cause/action/retained refs/resume condition сохраняются перед откладыванием.

## Delivery / freeze / independent QA

Worker result: TASK / BASE_SHA / FINAL_SHA / TREE / PR / CHANGED_PATHS / CHECKS / KNOWN_UNKNOWNS_PRESERVED / HANDOFF_PATH.
Integrator review -> IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / FINAL_SHA / TREE -> separate QA packet.
QA final PASS/FAIL/BLOCKED; FAIL -> Integrator bounded corrective Worker, same Issue/branch/PR, new freeze/QA. Source writes during QA запрещены.

## Durable handoff / один следующий шаг

Canonical packet/result/CI/QA/acceptance receipts в GitHub; контекст Worker/QA disposable.
NEXT_EXECUTOR / actual AUTOSPAWN or UNAVAILABLE_IN_CURRENT_ENVIRONMENT:
Full saved copy-ready prompt / packet URL:
Owner/Architecture authority и отдельный merge сохраняются.
