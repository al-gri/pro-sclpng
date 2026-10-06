# Project state

Обновлено: 2026-10-06.

## Baseline

- Repository: `al-gri/pro-sclpng`, public, default branch `main`.
- GOV-001 (#1): **DONE**.
- BOOT-001 (#2): **DONE**.
- SPEC-001 (#3): **DONE / ACCEPTED_IN_MAIN**.
- Accepted SPEC-001 source head: `9f351334d82e07be03694682d219d343471630c6`.
- Verified SPEC-001 main baseline: `8d9d6ada6309542822e4e38dd064f1e3467f990b` — squash merge of [PR #10](https://github.com/al-gri/pro-sclpng/pull/10); post-merge push CI [37416467117](https://github.com/al-gri/pro-sclpng/actions/runs/37416467117) completed successfully on this exact commit.
- Current phase: **M0 complete; M1 prerequisite verification in progress**.
- Workspace/toolchain: Rust **1.98.1**, workspace and offline/shadow CLI established by BOOT-001.
- Domain status: exact value types, identities, event/causal contracts, DataHealth reference contracts, artifact relations and bounded WAL contracts are implemented/verified within the accepted SPEC-001 scope.
- Runtime status: production Bitget connector, live book engine, filesystem recorder/replay, production artifact loader/verifier, strategy and execution are **NOT_IMPLEMENTED**.
- CI: **CONFIGURED / GREEN**. Required jobs are `rust-fmt`, `rust-clippy`, `rust-tests`; the post-merge run above passed on the accepted SPEC-001 main baseline.
- Main protection: `protect-main` is active with PR workflow, review-thread resolution and strict required checks; no bypass actors were accepted for SPEC-001.
- Live trading: **OUT_OF_SCOPE / DISABLED**.
- Imported course/matrix: **NOT_IMPORTED**; provenance work remains RULE-001 (#7).

The embedded SPEC-001 baseline SHA is a verified release/reference point, not a claim that `refs/heads/main` can never advance after later governance/docs merges.

## Очередь

| ID | Issue | Current state | Следующее действие |
|---|---|---|---|
| GOV-001 | #1 | **DONE** | — |
| BOOT-001 | #2 | **DONE** | — |
| SPEC-001 | #3 | **DONE / ACCEPTED_IN_MAIN** | Downstream code must reuse accepted contracts rather than invent replacements |
| MD-001 | #4 | **NEXT / CRITICAL_PATH** | Verify the current official Bitget public feed contract and prepare bounded fixtures |
| REC-001 | #5 | **BLOCKED_BY_MD_001** | After #4, Integrator decomposes M1 into small decoder/book/WAL/replay/supervisor/integration PRs |
| QA-001 | #6 | **OPEN / ONGOING** | Independent review/evidence for concrete M1 PR SHAs |
| RULE-001 | #7 | **PARALLEL / NON_BLOCKING_M1** | Validate provenance for the two strategy setups without delaying recorder/replay |

BOOT-001 and SPEC-001 no longer block REC-001. The remaining prerequisite for starting M1 implementation is MD-001.

## Управление

Владелец: al-gri. Главный чат — Architecture; один Integrator; workers краткоживущие; QA проверяет конкретные SHAs независимо от worker reasoning.

Источник истины: принятые документы и код в `main`, затем AGENTS/INVARIANTS, Issue scope и конкретный reviewed SHA. Старые чаты и промежуточные failed SHAs сохраняют audit history, но не переопределяют accepted contracts.

Не поддерживать второй ручной backlog здесь: динамические назначения, PR и blockers смотреть в Issues. После milestone обновлять этот файл отдельным bounded governance change; не вписывать SHA будущего содержащего commit как самоссылку.

## Следующая практическая поставка

**MD-001 (#4)** — подтвердить по актуальным официальным источникам Bitget public feed semantics и добавить малые versioned fixtures без API-ключей/private data.

После принятия MD-001 Integrator декомпозирует REC-001 (#5). Первый функциональный M1 результат остаётся прежним: небольшой публичный поток → локальная книга → WAL → детерминированный replay без скрытых gaps. Сигналы/TradePlan/execution в M1 не входят.
