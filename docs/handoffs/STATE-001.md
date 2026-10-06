# Handoff: STATE-001

Status: **READY_FOR_REVIEW**

Issue: https://github.com/al-gri/pro-sclpng/issues/11
Branch: `docs/STATE-001-project-state`
Base SHA: `8d9d6ada6309542822e4e38dd064f1e3467f990b`
Head SHA: recorded by the branch/PR after this file is committed; the handoff does not self-reference its containing commit.

## Что сделано

- Closed SPEC-001 Issue #3 as completed after verified squash merge and green push-to-main CI.
- Updated `docs/PROJECT_STATE.md` from bootstrap-era state to the accepted post-SPEC-001 baseline.
- Changed current SPEC-001 ADR/spec status/provenance from pending design review to `ACCEPTED` without changing contract semantics.
- Updated the SPEC-001 handoff header from partial/pending to `DONE / ACCEPTED_IN_MAIN` while preserving historical evidence.
- Left QA coordination Issue #6 open.
- Did not start MD-001/REC-001 implementation.

## Изменённые файлы

- `docs/PROJECT_STATE.md`
- `docs/adr/0002-domain-event-wal-contracts.md`
- `docs/handoffs/SPEC-001.md`
- `specs/domain/types-v1.md`
- `specs/domain/artifacts-v1.md`
- `specs/domain/test-matrix-v1.md`
- `specs/domain/review-vectors-v2.md`
- `specs/market-data/events-v1.md`
- `specs/market-data/data-health-v1.md`
- `specs/recording/wal-v1.md`
- `docs/handoffs/STATE-001.md`

## Проверки

| Check | Result | Evidence |
|---|---|---|
| Base main | PASS | `8d9d6ada6309542822e4e38dd064f1e3467f990b` |
| SPEC-001 post-merge CI baseline | PASS | run 37416467117 on `8d9d6ada6309542822e4e38dd064f1e3467f990b` |
| Issue states | PASS | #1/#2/#3 closed; #4/#5/#6/#7/#11 open as expected |
| Scope review | PASS | docs/status/provenance only; no Rust/Cargo/workflow/fixture changes intended |
| Runtime Rust commands for STATE-001 diff | NOT_APPLICABLE locally | status-only docs diff; PR CI remains required |

## Контракты / ADR

No contract field, wire layout, error taxonomy, test vector semantics or Rust API is changed. STATE-001 only records that the already-reviewed SPEC-001 revision became accepted through owner merge of PR #10.

The synthetic AF fixture Markdown remains untouched even though it records its historical proposal origin; fixtures are outside STATE-001 scope.

## Следующий шаг

Review the exact STATE-001 PR head and its CI. After owner merge, MD-001 (#4) is the next critical-path task; REC-001 (#5) remains blocked only by MD-001.
