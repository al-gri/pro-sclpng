# Project state

Обновлено: 2026-10-09. Проверенный main для подготовки REC-001F-2: `8b251e1ef09354a1a205fa152ce7726ea5f5749b`; accepted implementation baseline: `5dbba8b895601db42adfadd27bab2ae98265d195`.

Это docs-only синхронизация после приёмки REC-001C, MD-002, bounded REC-001D и частичной REC-001F-1. Владелец принял диагностический scope и явно разрешил прежние ручные integration действия от его имени; PR #36 merged. Reviewed source head, implementation baseline и актуальный main учитываются отдельно. Оба SHA выше относятся к проверенным baseline до этой docs подготовки, а не к будущему commit с этим файлом.

PR #35 также merged; актуальный main/tree проверены отдельно при подготовке следующей задачи. REC-001F-2 **PREPARED / IMPLEMENTATION NOT_STARTED**, [child Issue #37](https://github.com/al-gri/pro-sclpng/issues/37), [один Task Packet](task-packets/REC-001F-2.md). Это owner-bound scripted capture driver в существующем radar package: synthetic session, fake synchronous transport, controlled ReceiveStamp и реальный filesystem WAL round-trip. Реализация worker сейчас не запускается; live WS/TLS и полная REC-001F/M1 приёмка остаются отдельными gates.

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
- REC-001F-1 (#22): **ACCEPTED_IN_MAIN / PARTIAL_DIAGNOSTIC_SCOPE**. [PR #36](https://github.com/al-gri/pro-sclpng/pull/36), exact reviewed head/Integrator lock commit `7295966fe945279c8d8ed722d974be4d7c1f0288`, accepted implementation main `5dbba8b895601db42adfadd27bab2ae98265d195`, tree `74f15999df55e0752aecaf34b5b5c75408e15a86`; original base `b283eface02e5b6671d02fc78d07a3fabf49d46b`. [Owner-authorized acceptance](https://github.com/al-gri/pro-sclpng/pull/36#issuecomment-6077826720), [final worker receipt](https://github.com/al-gri/pro-sclpng/pull/36#issuecomment-6071161148), [independent QA](https://github.com/al-gri/pro-sclpng/pull/36#issuecomment-6071920852), [Integrator gate](https://github.com/al-gri/pro-sclpng/pull/36#issuecomment-6075958056), [source-head CI 37858980472](https://github.com/al-gri/pro-sclpng/actions/runs/37858980472) and [exact implementation main push CI 37908798577](https://github.com/al-gri/pro-sclpng/actions/runs/37908798577) SUCCESS. Merge tree equals the reviewed source tree; no new implementation bytes were introduced.
- Accepted state synchronization [PR #35](https://github.com/al-gri/pro-sclpng/pull/35) merged as `8b251e1ef09354a1a205fa152ce7726ea5f5749b`, tree `f946e0ab89edb6d6a4d18fdfa28fcaaae1f6f9c1`. Freshly checked [exact main push CI 37909422049](https://github.com/al-gri/pro-sclpng/actions/runs/37909422049): fmt/clippy/tests SUCCESS; decoded logs verify exact checkout, pinned Rust/Cargo 1.98.1, lock/build/clean and 560 PASS (549 runtime + 11 compile-fail; 24 replay included). This is baseline evidence, not REC-001F-2 runtime evidence.
- REC-001F-2 (#37): **PREPARED / IMPLEMENTATION NOT_STARTED**. [Task Packet](task-packets/REC-001F-2.md), base `8b251e1ef09354a1a205fa152ce7726ea5f5749b`, planned worker branch `feat/REC-001F-2-owner-bound-capture-driver`. Existing owner/supervisor/Timer A APIs support this synthetic slice without an identified public contract change; runtime confirmation belongs to the future worker. No Cargo delta expected. GitHub read/write, shell/Git checked; local Rust/Cargo/rustup/gh absent, local runtime/release/new-driver checks and independent QA **NOT_RUN**. No conflicting open PR or F-2 assignment found during preparation; worker must refresh before claim.
- Current phase: **M1 IN_PROGRESS / REC-001F PARTIAL_ACCEPTED / FULL REC-001F NOT_DONE**.
- Workspace/toolchain: Rust **1.98.1**; members: domain, market-data, recording, radar.
- Runtime accepted in main: pure Bitget JSON decoder/continuity classifier; bounded filesystem WAL reader/writer/recovery; deterministic DataHealth reducer; bounded public WS supervisor/capture/session ownership library, approved Timer A and finalization/diagnostic-close boundaries; diagnostic offline `radar-replay` in the existing radar package.
- Accepted partial application: PR #36 adds bounded offline `radar-replay`, a synthetic writer-built fixture, deterministic diagnostic output and 24 replay integration tests in the existing radar package. The original radar offline/shadow behavior is preserved. This acceptance does not establish a live recorder or canonical local book.
- Application runtime absent in main: live recorder/capture app. Concrete WebSocket/TLS adapter, live clock sampling/scheduler, signal-driven shutdown and production publication composition remain absent. Library ports and durable WAL operations do not constitute a runnable live capture application.
- Production artifact loader/verifier is absent. Frame/warm-up proof capabilities cannot be created by an external app from parsed WAL references. Unknown/Unverified artifacts block canonical applicability; no implicit VERIFIED/VALID/Usable fallback.
- Canonical regular local book: **BLOCKED / NOT_IMPLEMENTED** under REC-001E. Strategy/TradePlan/execution remain **NOT_IMPLEMENTED / OUT_OF_SCOPE**.
- Live public socket/TLS/live-clock smoke: **NOT_RUN / DEFERRED** in the accepted REC-001D scope. No live-capture parity is newly claimed.
- CI: **CONFIGURED / GREEN on verified main `8b251e1…`**, exact push CI 37909422049 as recorded above; accepted implementation push CI 37908798577 remains separate evidence for `5dbba8b…`. No fresh local Rust/runtime or release run was performed in this preparation session: NOT_RUN. Future F-2 implementation needs fresh checks/CI on its actual final head.
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
| REC-001 | #5 | **IN_PROGRESS / A+B+C+D AND PARTIAL F ACCEPTED / FULL M1 NOT_DONE** | Implement bounded REC-001F-2 separately; retain local-book and live-application gates |
| MD-002 | #19 | **DONE / EVIDENCE ACCEPTED; U-09/U-10 UNRESOLVED** | New normative evidence or separately accepted engineering policy/ADR needed for REC-001E |
| REC-001E | #21 | **BLOCKED / NOT_STARTED** | Do not infer unit/delete semantics from #19 closure |
| REC-001F | #22 | **PARTIAL REC-001F-1 ACCEPTED_IN_MAIN / FULL TASK NOT_DONE** | [REC-001F-2 packet](task-packets/REC-001F-2.md) prepared; original F-1 packet is completed scope/history |
| REC-001F-2 | #37 | **PREPARED / IMPLEMENTATION NOT_STARTED** | One worker: owner-bound synthetic driver, controlled observations/fake effects, WAL readback and lifecycle negatives |
| QA-001 | #6 | **OPEN / ONGOING; PARTIAL REC-001F-1 ACCEPTED AFTER EXACT-HEAD QA** | Final source QA at `7295966…` remains applicable to the identical merge tree; new implementation heads and final M1 need their own gates |
| RULE-001 | #7 | **PARALLEL / NON_BLOCKING_M1** | Validate source provenance for two setups |

REC-001 decomposition Issues are #16–#22; REC-001F-2 #37 has explicit Parent #22 and reciprocal issue links (native GitHub sub-issue relationship is not set by the available connector). Dynamic assignment and blockers live in Issues; this file does not duplicate their full status graph. The accepted decomposition is recorded in [Issue #5 comment 6013088690](https://github.com/al-gri/pro-sclpng/issues/5#issuecomment-6013088690).

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
- **#22 REC-001F** — recorder/replay composition. **Partial REC-001F-1 ACCEPTED_IN_MAIN via PR #36; full task NOT_DONE.**

## Selected reduced REC-001F scope and remaining M1 gates

The owner selected this bounded integration scope on 2026-10-08: raw messages and existing contracted control inputs in the existing WAL; offline replay through the existing Rust decoder and DataHealth reducer; two runs of one fixture with identical canonical diagnostic output; commands, checks and handoff. Canonical local book is excluded while REC-001E remains blocked.

First delivery **REC-001F-1** is the offline part: an additional `radar-replay` binary in the existing radar package, a small saved synthetic WAL and deterministic diagnostic replay. This is accepted in main via PR #36, avoids a new workspace member and does not implement live capture. Its fixture writer uses the existing WAL codec/writer; it is not a production recorder. The exact worker scope is [the original packet](task-packets/REC-001F-1.md) and Issue #22. Integrator already committed the root Cargo.lock delta sequentially as `7295966fe945279c8d8ed722d974be4d7c1f0288`: only 5 added lock lines over worker parent `0329039c88a616124994cbd9f2d792a9dc629f4f`; all 144 non-lock files unchanged. The original packet and committed worker handoff retain preparation/authorship history; final SHA-bound receipts establish current status.

Here canonical output means a fixed, deterministic serialization of physical recovery, lexical inputs and diagnostic health projection. It is **not** a canonical local book or proof of canonical applicability. `CanonicalStatus::NotEvaluated`, missing artifact evidence, UNKNOWN quality and `usable_data=false` remain explicit. Supported synthetic policy/cardinality never become Bitget guarantees. Unsupported state-changing/proof controls cannot be silently skipped and called fully replayed.

### Accepted partial-delivery evidence

- Actual implementation head `7295966…`: worker Linux Rust/Cargo 1.98.1 captured 32 successful commands. Independent QA executed its own build/fmt/clippy, 560 workspace tests (549 runtime + 11 expected compile-fail; the 24 replay tests are included), fixture builder/two processes/golden comparisons and additional 45 WAL CLI, 44 accepted-reader and 8 path checks. Fresh CI 37858980472 has all three jobs SUCCESS on the same exact head; lock equality/build/clean steps and logs were checked separately.
- QA first reported BLOCKED because the required worker ZIP was missing in its session. [QA RESUME-B1](https://github.com/al-gri/pro-sclpng/pull/36#issuecomment-6071920852) verified the actual archive: 104 entries, 103 checksums, 102 manifest records, 145 source identities, raw command logs and fresh bytes. **B1 RESOLVED/PASS; no blocking findings.** Previous independent runtime remains applicable to unchanged SHA/bytes; resume did not rerun runtime. Integrator checked the supplied QA supplement and source/contract scope; fresh local Integrator Cargo execution is NOT_RUN because Rust/Cargo are unavailable.
- **O1 OPEN / nonblocking P3:** `git diff --check` is **FAIL / exit 2**, not PASS. Cargo.toml has mixed CRLF/LF; Handoff line 130 is an intentional blank context marker inside its fenced unified diff. No whitespace configuration override or source cleanup was performed. Accepted runtime/CI gates passed; no separate nonwaivable whitespace gate was established.
- **Owner partial acceptance and merge authorization were issued; PR #36 is merged.** Exact implementation main `5dbba8b895601db42adfadd27bab2ae98265d195` has the same tree as reviewed source `7295966…`; post-merge push CI 37908798577 is separately SUCCESS. O1 remains OPEN/nonblocking P3; accepting this partial scope does not establish data usability or full M1 completion. The original packet and preparation handoff remain frozen history, not instructions to launch another REC-001F-1 worker. Further changes require their own compatibility/check-applicability assessment.

Partial REC-001F acceptance does not close #22, parent #5 or full M1. Remaining gates include concrete public socket/TLS transport, recorded live clock/timer and shutdown composition, actual public capture/replay evidence, production artifact verification for canonical applicability, REC-001E unblock/implementation/QA, and final M1 QA. A separate explicit milestone decision is required to change those gates. No M2/M3 work follows automatically from deterministic diagnostic output.

## Управление

Владелец: al-gri. Главный чат — Architecture; один Integrator; workers краткоживущие; QA проверяет конкретные SHAs независимо от worker reasoning.

Источник истины: принятые документы и код в `main`, затем AGENTS/INVARIANTS, Issue scope и конкретный reviewed SHA. Старые чаты и промежуточные failed SHAs сохраняют audit history, но не переопределяют accepted contracts.

Не поддерживать второй ручной backlog здесь: динамические назначения, PR и blockers смотреть в Issues. После milestone обновлять этот файл отдельным bounded governance change; не вписывать SHA будущего содержащего commit как самоссылку.

## Следующая практическая поставка

**REC-001F-2 — owner-bound capture driver**, [Issue #37](https://github.com/al-gri/pro-sclpng/issues/37), [full packet](task-packets/REC-001F-2.md). One worker starts in a separate chat after fresh claim/base/conflict/tool checks. It composes CaptureSessionOwner/PublicWsSupervisor/Timer A with bounded synthetic script and synchronous fake effects, validates actual filesystem WAL through WalReader, deterministic processes and healthy/failed shutdown. No implementation is launched in this preparation session. Partial REC-001F-1 remains accepted and frozen.

Real WS/TLS still needs bounded transport/effect completion integration (owner.dispatch callback is synchronous), physical clock sampling/live queue_tick wake-ups, serialized owner actor, signal shutdown/Close policy and real public capture/live-smoke/QA. Public trusted AuthenticatedClosure producer is absent. F-2 profile/ACK differs from accepted F-1 replay; this slice promises WalReader readback, not full F-1 replay compatibility. Canonical artifact verification and E unit/delete gates are separate from recording raw diagnostic inputs. Public-contract gaps require ACR and separately accepted ADR.

The prior owner-authorized PR #36/#35 integration is complete. This new docs preparation PR is separate and does not inherit merge authorization. No merge/auto-merge, force-push, repository-settings change or Issue closure is performed.

REC-001E, unresolved U/C constraints, `canonical_status=NotEvaluated`, `canonical_applicability=BLOCKED_UNVERIFIED`, `usable_data=false` and all remaining M1 gates remain effective. #22, #5 and full M1 stay open.
