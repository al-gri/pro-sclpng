# Project state

Обновлено: 2026-10-06.

## Baseline

- Repository: `al-gri/pro-sclpng`, public, default branch `main`.
- GOV-001 (#1): **DONE**.
- BOOT-001 (#2): **DONE**.
- SPEC-001 (#3): **DONE / ACCEPTED_IN_MAIN**.
- Accepted SPEC-001 source head: `9f351334d82e07be03694682d219d343471630c6`.
- Verified SPEC-001 main baseline: `8d9d6ada6309542822e4e38dd064f1e3467f990b` — squash merge of [PR #10](https://github.com/al-gri/pro-sclpng/pull/10); post-merge push CI [37416467117](https://github.com/al-gri/pro-sclpng/actions/runs/37416467117) completed successfully on this exact commit.
- MD-001 (#4): **DONE / ACCEPTED_IN_MAIN**.
- Accepted MD-001 reviewed head: `76ca481ed9221289e001d120baf4baf8170c205a`.
- Accepted MD-001 main baseline: `da3c1cd1086b42d4a06ec4a0c90010b012099557` — squash merge of [PR #13](https://github.com/al-gri/pro-sclpng/pull/13); post-merge push CI [37440205546](https://github.com/al-gri/pro-sclpng/actions/runs/37440205546) completed successfully on this exact commit.
- Current phase: **M1 implementation planning / REC-001 decomposition ready**.
- Workspace/toolchain: Rust **1.98.1**, workspace and offline/shadow CLI established by BOOT-001.
- Domain status: exact value types, identities, event/causal contracts, DataHealth reference contracts, artifact relations and bounded WAL contracts are implemented/verified within the accepted SPEC-001 scope.
- Exchange contract status: current Bitget public-feed baseline and bounded synthetic fixtures are accepted from MD-001; unresolved UNKNOWN/BLOCKED/CONFLICT items remain hard downstream constraints rather than implementation assumptions.
- Runtime status: production Bitget connector, live book engine, filesystem recorder/replay, production artifact loader/verifier, strategy and execution are **NOT_IMPLEMENTED**.
- CI: **CONFIGURED / GREEN**. Required jobs are `rust-fmt`, `rust-clippy`, `rust-tests`; the latest accepted main baseline above passed all three on push.
- Main protection: `protect-main` is active with PR workflow, review-thread resolution and strict required checks.
- Live trading: **OUT_OF_SCOPE / DISABLED**.
- Imported course/matrix: **NOT_IMPORTED**; provenance work remains RULE-001 (#7).

The embedded accepted baseline SHAs are release/reference points, not claims that `refs/heads/main` can never advance after later bounded governance changes.

## Очередь

| ID | Issue | Current state | Следующее действие |
|---|---|---|---|
| GOV-001 | #1 | **DONE** | — |
| BOOT-001 | #2 | **DONE** | — |
| SPEC-001 | #3 | **DONE / ACCEPTED_IN_MAIN** | Downstream code must reuse accepted contracts rather than invent replacements |
| MD-001 | #4 | **DONE / ACCEPTED_IN_MAIN** | Preserve unresolved U/C constraints in every dependent task |
| REC-001 | #5 | **READY_FOR_DECOMPOSITION** | Integrator splits M1 into small dependency-ordered implementation Issues/PRs; do not assign the whole epic to one worker |
| QA-001 | #6 | **OPEN / ONGOING** | Independent review/evidence for concrete M1 PR SHAs |
| RULE-001 | #7 | **PARALLEL / NON_BLOCKING_M1** | Validate provenance for the two strategy setups without delaying recorder/replay |
| STATE-002 | #14 | **IN_PROGRESS / GOVERNANCE_ONLY** | Synchronize this state after accepted MD-001; no runtime semantics |

All M1 prerequisites GOV-001, BOOT-001, SPEC-001 and MD-001 are now accepted. This unblocks **decomposition** of REC-001, not a monolithic implementation.

## REC-001 downstream constraints

At minimum, every relevant REC-001 task packet must carry these accepted MD-001 constraints:

- **U-09 UNKNOWN / BLOCKED** — regular `books50` quantity asset/unit is not proven. No worker may invent a canonical `quantity_unit`.
- **U-10 UNKNOWN / BLOCKED** — regular `qty=0` / deletion semantics are not proven. No production reducer may silently map zero to `DeleteLevel`.
- **U-20 NOT_PROVEN / FORBIDDEN** — no documented REST↔WS causal/sequence bridge. REST snapshots must not be used to stitch/heal a WS gap by timestamp, price or level coincidence.
- **C-01 CONTRACT_CONFLICT / BLOCKED** — RPI depth has two quantity components while the accepted canonical Book level has one. RPI normalization requires a separate accepted ADR/contract change before implementation.
- **C-03 DOC_CONFLICT / UNKNOWN** — official docs conflict between `/api/v3/market/instruments` and `/api/v3/public/instruments`. MD-001 uses `/market/instruments` by recorded evidence precedence, but implementation workers may not declare the other route alias/deprecated/invalid without new evidence.

The remaining U-01…U-21/C-02 limitations in `docs/exchange/bitget-public-feed.md` also remain authoritative. In particular Spot exact increments (U-06/U-07) and aggressor semantics (U-18) must stay unresolved unless a later accepted task provides evidence.

## Управление

Владелец: al-gri. Главный чат — Architecture; один Integrator; workers краткоживущие; QA проверяет конкретные SHAs независимо от worker reasoning.

Источник истины: принятые документы и код в `main`, затем AGENTS/INVARIANTS, Issue scope и конкретный reviewed SHA. Старые чаты и промежуточные failed SHAs сохраняют audit history, но не переопределяют accepted contracts.

Не поддерживать второй ручной backlog здесь: динамические назначения, PR и blockers смотреть в Issues. После milestone обновлять этот файл отдельным bounded governance change; не вписывать SHA будущего содержащего commit как самоссылку.

## Следующая практическая поставка

**REC-001 (#5) decomposition** — создать dependency-ordered bounded implementation Issues до начала кода. Первый worker не должен одновременно реализовывать live WebSocket, canonical book mutation, WAL и replay.

Первый functional M1 target остаётся прежним: небольшой публичный поток → validated local state/data health → WAL → deterministic replay без скрытых gaps. Сигналы/TradePlan/execution в M1 не входят.
