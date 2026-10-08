# Project state

Обновлено: 2026-10-08. Проверенный main: `b283eface02e5b6671d02fc78d07a3fabf49d46b`.

Это docs-only синхронизация после приёмки REC-001C, MD-002 и REC-001D. SHA выше — проверенная исходная ревизия, не SHA будущего commit с этим файлом.

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
- Accepted post-MD state sync: `94b391c6f2ce3a86fdbdd3eb2bca503b38808069` — squash merge of [PR #15](https://github.com/al-gri/pro-sclpng/pull/15); post-merge push CI [37440668685](https://github.com/al-gri/pro-sclpng/actions/runs/37440668685) completed successfully on this exact commit.
- REC-001A (#16): **DONE / ACCEPTED_IN_MAIN**.
- Accepted REC-001A reviewed head: `edeae3608c369a5060798c19c3505d1c847b51db`; QA verdict record: PR #25 review `5426726490`.
- Accepted REC-001A main baseline: `943c5d8f36015c65eda542717c2d61477dae7621` — squash merge of [PR #25](https://github.com/al-gri/pro-sclpng/pull/25); post-merge push CI [37446766415](https://github.com/al-gri/pro-sclpng/actions/runs/37446766415) completed successfully on this exact commit.
- REC-001B (#17): **DONE / ACCEPTED_IN_MAIN**.
- Accepted REC-001B reviewed head: `cb8ae11fce986531fc1a4473edfafc90cc547a5b`; independent re-QA + Integrator gate provenance: [PR #28 comment 6016099061](https://github.com/al-gri/pro-sclpng/pull/28#issuecomment-6016099061).
- Accepted REC-001B main baseline: `a7c825316c21ed2841da79e06fb8fa8f62b97e6f` — squash merge of [PR #28](https://github.com/al-gri/pro-sclpng/pull/28); post-merge push CI [37462581887](https://github.com/al-gri/pro-sclpng/actions/runs/37462581887) completed successfully on this exact commit.
- REC-001C (#18): **DONE / ACCEPTED_IN_MAIN**. [PR #32](https://github.com/al-gri/pro-sclpng/pull/32), accepted head `6785605076335271249faf98b07451ce6ccdfa38`, main merge `c9c23f707299ab35c051783b75f645e592213822`. [QA + Integrator receipt](https://github.com/al-gri/pro-sclpng/pull/32#issuecomment-6024057261); [exact main push CI 37520822675](https://github.com/al-gri/pro-sclpng/actions/runs/37520822675) SUCCESS.
- MD-002 (#19): **DONE / ACCEPTED_IN_MAIN**, evidence scope only. [PR #33](https://github.com/al-gri/pro-sclpng/pull/33), accepted head `4a98ae7ee02870b17cee4be17acdad47f450a755`, main merge `39ff0dba797eb010586238ef06fb80e996340401`. [Owner acceptance](https://github.com/al-gri/pro-sclpng/issues/19#issuecomment-6024780127); [exact main push CI 37526098000](https://github.com/al-gri/pro-sclpng/actions/runs/37526098000) SUCCESS. U-09/U-10 and regular snapshot zero semantics remain UNKNOWN; closing the evidence task does not unblock REC-001E.
- REC-001D (#20): **DONE / ACCEPTED_IN_MAIN in the approved bounded library scope**. [PR #34](https://github.com/al-gri/pro-sclpng/pull/34) merged 2026-10-08T20:00:12Z. Accepted head `156ae16021f062b332856f9d61255005142ac599`; actual main merge `b283eface02e5b6671d02fc78d07a3fabf49d46b`; integrated tree `1b7809b4d6ff125f130afc4b8428f86686013e24`. [Final acceptance](https://github.com/al-gri/pro-sclpng/issues/20#issuecomment-6068045032); [exact main push CI 37836151426](https://github.com/al-gri/pro-sclpng/actions/runs/37836151426) SUCCESS. Its logs show 536 PASS (525 runtime + 11 expected compile-fail), 19 suites, 0 failed/ignored. Accepted independent QA record: 47/47 PASS_SCOPED; debug/release evidence belongs to accepted source and is not a fresh release run on the merge commit.
- Current phase: **M1 IN_PROGRESS / reduced REC-001F first part selected / implementation NOT_STARTED**.
- Workspace/toolchain: Rust **1.98.1**; members: domain, market-data, recording, radar.
- Runtime implemented: pure Bitget JSON decoder/continuity classifier; bounded filesystem WAL reader/writer/recovery; deterministic DataHealth reducer; bounded public WS supervisor/capture/session ownership library, approved Timer A and finalization/diagnostic-close boundaries.
- Application runtime absent: recorder/replay apps, concrete WebSocket/TLS adapter, live clock sampling/scheduler, signal-driven shutdown and production publication composition. `apps/radar` remains the bootstrap offline/shadow CLI. Library ports and durable WAL operations do not constitute a runnable live capture application.
- Production artifact loader/verifier is absent. Frame/warm-up proof capabilities cannot be created by an external app from parsed WAL references. Unknown/Unverified artifacts block canonical applicability; no implicit VERIFIED/VALID/Usable fallback.
- Canonical regular local book: **BLOCKED / NOT_IMPLEMENTED** under REC-001E. Strategy/TradePlan/execution remain **NOT_IMPLEMENTED / OUT_OF_SCOPE**.
- Live public socket/TLS/live-clock smoke: **NOT_RUN / DEFERRED** in the accepted REC-001D scope. No live-capture parity is newly claimed.
- CI: **CONFIGURED / GREEN on the verified main SHA above**. `rust-fmt`, `rust-clippy`, `rust-tests` are SUCCESS; all three exact-checkout logs were read. Local Rust/Cargo were unavailable in this state-sync session; local runtime checks are NOT_RUN.
- Main protection/settings are not changed by this state update.
- Live trading: **OUT_OF_SCOPE / DISABLED**.
- Imported course/matrix: **NOT_IMPORTED**; provenance work remains RULE-001 (#7).

The embedded accepted baseline SHAs are release/reference points, not claims that `refs/heads/main` can never advance after later bounded changes.

## Очередь

| ID | Issue | Current state | Следующее действие |
|---|---|---|---|
| GOV-001 | #1 | **DONE** | — |
| BOOT-001 | #2 | **DONE** | — |
| SPEC-001 | #3 | **DONE / ACCEPTED_IN_MAIN** | Downstream code must reuse accepted contracts rather than invent replacements |
| MD-001 | #4 | **DONE / ACCEPTED_IN_MAIN** | Preserve unresolved U/C constraints in every dependent task |
| REC-001 | #5 | **IN_PROGRESS / A+B+C+D ACCEPTED IN THEIR SCOPES / FULL M1 NOT_DONE** | One reduced REC-001F worker after preflight; retain local-book and live-application gates |
| MD-002 | #19 | **DONE / EVIDENCE ACCEPTED; U-09/U-10 UNRESOLVED** | New normative evidence or separately accepted engineering policy/ADR needed for REC-001E |
| REC-001E | #21 | **BLOCKED / NOT_STARTED** | Do not infer unit/delete semantics from #19 closure |
| REC-001F | #22 | **REDUCED SCOPE SELECTED / NOT_STARTED / FULL TASK NOT_DONE** | [REC-001F-1 Task Packet](task-packets/REC-001F-1.md): offline diagnostic replay first |
| QA-001 | #6 | **OPEN / ONGOING** | Independent review/evidence for concrete M1 PR SHAs |
| RULE-001 | #7 | **PARALLEL / NON_BLOCKING_M1** | Validate source provenance for two setups |

REC-001 child Issues are #16–#22. Dynamic assignment and blockers live in Issues; this file does not duplicate their full status graph. The accepted decomposition is recorded in [Issue #5 comment 6013088690](https://github.com/al-gri/pro-sclpng/issues/5#issuecomment-6013088690).

## REC-001 downstream constraints

At minimum, every relevant REC-001 task packet must carry these accepted MD-001 constraints:

- **U-09 UNKNOWN / BLOCKED** — regular `books50` quantity asset/unit is not proven. No worker may invent a canonical `quantity_unit`.
- **U-10 UNKNOWN / BLOCKED** — regular `qty=0` / deletion semantics are not proven. No production reducer may silently map zero to `DeleteLevel`.
- **U-20 NOT_PROVEN / FORBIDDEN** — no documented REST↔WS causal/sequence bridge. REST snapshots must not be used to stitch/heal a WS gap by timestamp, price or level coincidence.
- **C-01 CONTRACT_CONFLICT / BLOCKED** — RPI depth has two quantity components while the accepted canonical Book level has one. RPI normalization requires a separate accepted ADR/contract change before implementation.
- **C-03 DOC_CONFLICT / UNKNOWN** — official docs conflict between `/api/v3/market/instruments` and `/api/v3/public/instruments`. MD-001 uses `/market/instruments` by recorded evidence precedence, but implementation workers may not declare the other route alias/deprecated/invalid without new evidence.

The remaining U-01…U-21/C-02 limitations in `docs/exchange/bitget-public-feed.md` also remain authoritative. In particular Spot exact increments (U-06/U-07) and aggressor semantics (U-18) must stay unresolved unless a later accepted task provides evidence.

## REC-001 decomposition boundary

- **#16 REC-001A** — pure regular JSON decoder + continuity classifier. **DONE / ACCEPTED_IN_MAIN.**
- **#17 REC-001B** — bounded WAL writer/reader/recovery. **DONE / ACCEPTED_IN_MAIN.**
- **#18 REC-001C** — DataHealth runtime reducer without socket/book mutation. **DONE / ACCEPTED_IN_MAIN via PR #32.**
- **#19 MD-002** — unit/delete source evidence. **DONE via PR #33; U-09/U-10 remain UNKNOWN / BLOCKED.**
- **#20 REC-001D** — bounded public supervisor/capture/session library. **DONE via PR #34 only in its accepted library scope; live smoke NOT_RUN.**
- **#21 REC-001E** — canonical regular local-book reducer. **BLOCKED.** [Blocker reaffirmation](https://github.com/al-gri/pro-sclpng/issues/21#issuecomment-6024795633) remains effective.
- **#22 REC-001F** — recorder/replay composition. **Reduced scope selected by owner; not implemented or accepted.**

## Selected reduced REC-001F scope and remaining M1 gates

The owner selected this bounded integration scope on 2026-10-08: raw messages and existing contracted control inputs in the existing WAL; offline replay through the existing Rust decoder and DataHealth reducer; two runs of one fixture with identical canonical diagnostic output; commands, checks and handoff. Canonical local book is excluded while REC-001E remains blocked.

First delivery **REC-001F-1** is the offline part: an additional `radar-replay` binary in the existing radar package, a small saved synthetic WAL and deterministic diagnostic replay. This avoids a new workspace member and does not implement live capture. Its fixture writer uses the existing WAL codec/writer; it is not a production recorder. The exact worker scope is [the packet](task-packets/REC-001F-1.md) and Issue #22. Shared root Cargo.lock remains Integrator-owned and must be integrated sequentially before final locked checks.

Here canonical output means a fixed, deterministic serialization of physical recovery, lexical inputs and diagnostic health projection. It is **not** a canonical local book or proof of canonical applicability. `CanonicalStatus::NotEvaluated`, missing artifact evidence, UNKNOWN quality and `usable_data=false` remain explicit. Supported synthetic policy/cardinality never become Bitget guarantees. Unsupported state-changing/proof controls cannot be silently skipped and called fully replayed.

Partial REC-001F acceptance does not close #22, parent #5 or full M1. Remaining gates include concrete public socket/TLS transport, recorded live clock/timer and shutdown composition, actual public capture/replay evidence, production artifact verification for canonical applicability, REC-001E unblock/implementation/QA, and final M1 QA. A separate explicit milestone decision is required to change those gates. No M2/M3 work follows automatically from deterministic diagnostic output.

## Управление

Владелец: al-gri. Главный чат — Architecture; один Integrator; workers краткоживущие; QA проверяет конкретные SHAs независимо от worker reasoning.

Источник истины: принятые документы и код в `main`, затем AGENTS/INVARIANTS, Issue scope и конкретный reviewed SHA. Старые чаты и промежуточные failed SHAs сохраняют audit history, но не переопределяют accepted contracts.

Не поддерживать второй ручной backlog здесь: динамические назначения, PR и blockers смотреть в Issues. После milestone обновлять этот файл отдельным bounded governance change; не вписывать SHA будущего содержащего commit как самоссылку.

## Следующая практическая поставка

**One WORK — REC-001F-1** using the [complete packet](task-packets/REC-001F-1.md), after fresh base/Issue/branch/PR preflight and claim. Implementation is launched in a separate worker chat; this governance PR implements no Rust code. No merge/auto-merge or Issue closure is authorized here.
