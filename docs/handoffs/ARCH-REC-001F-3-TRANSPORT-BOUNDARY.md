# Handoff: ARCH-REC-001F-3-TRANSPORT-BOUNDARY

Status: **DOCS_REVIEW_COMPLETED / PUBLICATION_AND_INDEPENDENT_QA_PENDING**.
Verdict: **API_COMPATIBLE_WITH_EXPLICIT_LIMITATIONS** for the one single-shot
numeric-peer Text-only diagnostic slice; conditional ACR gates remain explicit.

- Issue: [#40](https://github.com/al-gri/pro-sclpng/issues/40); Parent
  [#22](https://github.com/al-gri/pro-sclpng/issues/22).
- Assignment: one Architecture reviewer; implementation not started.
- Existing packet: [6095975783](https://github.com/al-gri/pro-sclpng/issues/22#issuecomment-6095975783).
- PR: published by Integrator; exact final link/head/tree/check receipt belongs
  in mutable Issue/PR metadata after commit, not a future self-SHA here.
- Base / source-inspected SHA: `8baf610b4ac15122dfcdbfe07730bc30c6558d73`.
- Base tree: `ef5ae29b1ab546641c50110c9053834ad2891219`.
- Branch: `docs/ARCH-REC-001F-3-transport-boundary`.

## Delivered work

Source-level assessment of synchronous dispatch, sole Rc/SessionTurn owner,
effect success versus enqueue/Unknown, real local Close settlement and retained
epoch identity, original Timer A clocks/deadlines, bounded I/O, native protocol
side effects, memory accounting, failure/shutdown and diagnostic replay.
One later implementation recommendation is in the report; no duplicate packet
or transport code was created.

Selected raw frame/manual TLS route can avoid unleased automatic WebSocket
responses. It accepts bounded final Text only; native control/binary/fragment
input causes explicit unsuccessful stop. Existing text Pong is contracted
TransportUp, not raw transcript bytes. DNS is not implemented: only an externally
prepared numeric peer with preserved SNI/certificate hostname is admitted.
Live acceptance needs independently bounded address-preparation evidence.
Five-second cooperative shutdown is conditional on synchronous disk calls
returning; it is not a hard filesystem deadline.

Read-only replay subreview and independent API challenge confirmed the narrow
existing-API alternative and exposed ACK/Pong/profile limitations. These are
separate reasoning sessions, not Rust execution or final-head independent QA.

## Changed files

```text
docs/architecture/REC-001F-3-transport-boundary.md
docs/handoffs/ARCH-REC-001F-3-TRANSPORT-BOUNDARY.md
```

All accepted code/Cargo/lock/workflow/spec/ADR/F-1/F-2 fixtures/profiles/history
and original packets are unchanged. Final committed path/tree/blob audit is
Integrator-owned and must be recorded in the final mutable receipt.

## Checks and environment

| Command / check | Result | Environment / actual evidence |
| --- | --- | --- |
| `git rev-parse HEAD` | PASS, exit 0 | Isolated Linux/Bash worktree: exact base SHA above. |
| `git rev-parse 'HEAD^{tree}'` | PASS, exit 0 | Exact base tree above. |
| `git status --porcelain` before edits | PASS, exit 0 / empty | Clean assigned worktree. |
| `git remote get-url origin` | PASS, exit 0 | Only `https://github.com/al-gri/pro-sclpng.git`. |
| `command -v` preflight | PASS as capability check | Git/Python3 available; rustc/cargo/rustup ABSENT. |
| Governance/spec/API/acceptance receipt reading | PASS, source review | Exact-base files, original packet, final PR #38/#39 receipts; no other repository inspected. |
| Official networking documentation | PASS, documentation retrieval | Publisher versions: tungstenite 0.30.0, rustls 0.23.45, webpki-roots 1.0.9, signal-hook 0.4.5. Installation, chosen features and compatibility are NOT_RUN. |
| Strict UTF-8/NUL/fences/relative-links/LF/whitespace audit | PASS, Python3 exit 0 | Both candidate files checked in this worktree; final committed-head audit remains Integrator-owned. |
| Final base-to-head clean/frozen-packet/tree audit | PENDING final Integrator receipt | Final head does not yet exist in this handoff. |
| Base CI 38039982818 | PASS, separately attributed | Integrator decoded exact-main three-job logs: Rust/Cargo 1.98.1; 577=566+11, zero failed/ignored. Baseline only. |
| Candidate CI / independent final-head QA | NOT_RUN at handoff creation | Awaiting published immutable candidate. |
| Local build/fmt/clippy/test/release / dependency fetch | NOT_RUN | Rust/Cargo absent. No runtime code changed. |
| Real DNS / socket / TLS / live-clock smoke / capture/replay | NOT_RUN | Docs-only review; no connection performed. |

## Contracts and conditional ACR

No accepted API/spec/ADR/wire/Timer A delta. Same-owner synchronous dispatch and
truthful idempotent local cessation suffice for selected scope; missing public
AuthenticatedClosure constructor alone is not a blocker.

Unsupported stronger traces require separately accepted ADR before dependent
code: native Ping->Pong continuation; typed full raw control/frame transcript;
asynchronous post-callback completion; hard total shutdown under stalled fsync;
independent typed replay ACK classification through the accepted private parser.
Report §11 names minimal proposals, preserving alternatives, migration/bounds/
invariant/test implications. These proposals are not accepted decisions.

## Limitations and remaining gates

New captured diagnostic profile is required; frozen F-1 rejects different IDs/
clock/bootstrap and ACK. ACK capture acceptance/readback is tested through the
existing supervisor; replay preserves its opaque raw bytes and steps Noop, with
subscription semantics NotReconstructed. Recorded controls/book inputs use the
accepted DataHealth/continuity reducers and recorded time, never fresh clocks.
Physical Complete/Durable cannot prove peer delivery or usability.

U-09/U-10/REC-001E stay blocked, U-20 healing forbidden, C-01 blocked and C-03
unknown. Numeric/artifact bootstrap remains synthetic/unverified while actual
wire bytes and clock samples are real and separately labeled. Every result keeps
NotEvaluated/BLOCKED_UNVERIFIED/usable_data=false. Full REC-001F/M1 is incomplete;
M2 #29 remains blocked. Existing #37/#22/#5 are not closed.

## Next step / handover

Integrator publishes exactly this two-file docs candidate, verifies exact
head/tree/checks and obtains independent source/docs QA. Then publish one
bounded implementation Task Packet/Issue for REC-001F-3-PUBLIC-TEXT-CAPTURE,
with the report's limitations, sequential Integrator Cargo.lock ownership,
clean-runner dependency fetch before offline/locked gates, real-WAL replay
acceptance and NOT_RUN live conditions. No worker code starts through this
handoff alone; any strengthened requirement first follows the conditional ADR
gate. Owner retains merge/acceptance authority.

Unique findings are saved in the two report/handoff files; publication receipts
will preserve exact containing SHA and real validation. No secrets, large raw
archives, native settings changes, merge/auto-merge or issue closure occurred.
