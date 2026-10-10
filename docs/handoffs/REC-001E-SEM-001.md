# Handoff: REC-001E-SEM-001

Status: **BLOCKED_INSUFFICIENT_EVIDENCE / READY_FOR_INTEGRATOR_PUBLICATION**.
Issue: [#42](https://github.com/al-gri/pro-sclpng/issues/42).
PR: **PENDING_INTEGRATOR_PUBLICATION**; final head and exact-head check receipts belong in mutable PR/Issue metadata after commit.
Role/session: sole docs/source worker `semantics42`; Integrator performs publication; separate QA reviews the final head.
Base SHA: `8baf610b4ac15122dfcdbfe07730bc30c6558d73`.
Checked working-tree revision: base SHA plus the two new files below; no containing future self-SHA is claimed.
Branch: `docs/REC-001E-SEM-001-u09-u10-resolution`.
Source verification date: **2026-10-10**.

## What was done

Freshly read accepted source reports/MD-002 receipt and applicable numeric/event/artifact/health/WAL/ADR boundaries; independently checked current GitHub main, #42/#21/#19 comments, open PRs/branches and assignment. No competing #42 claim/branch/PR existed at preflight; the Integrator then published [this claim](https://github.com/al-gri/pro-sclpng/issues/42#issuecomment-6096873314).

Opened current official English/Chinese depth, Best Practices, REST Market Data, Order Management, RPI, SBE and current/legacy rollout previews. U-09/U-10 and source snapshot zero remain UNKNOWN. The new current preview specifies a November 5, 2026 planned live rollout of incremental books50; current depth text already describes increments and legacy preview gives end October. The report records this live-applicability CONFLICT and keeps actual live observation NOT_RUN.

Delivered a separate PROPOSED engineering-policy appendix `REC-SEM-UNKNOWN-001 v1`: preserve diagnostic source data, withhold uncertain unit/delete effects, reject canonical snapshot zero, preserve epochs/proof/ownership barriers. It does not choose base units or zero deletion and does not unblock #21. This is not an accepted ADR or an implementation packet.

## Changed files

Exactly these two new files:

- `docs/exchange/REC-001E-U09-U10-resolution.md`
- `docs/handoffs/REC-001E-SEM-001.md`

Existing MD-001/MD-002 reports/handoffs, PROJECT_STATE, accepted specs/ADR, production code, fixtures, Cargo files and workflows are unchanged by this work unit.

## Actual checks

Environment: Linux x86_64, shell/Git/Python3 plus GitHub read connector and web documentation reader. Worker did not assume Windows/Docker execution results or Rust availability.

| Command/check | Result | Actual result / applicability |
|---|---|---|
| GitHub `git/ref/heads/main` GET | PASS | Exact main remained `8baf610b4ac15122dfcdbfe07730bc30c6558d73` at worker preflight; a changed main requires renewed compatibility review. |
| GitHub #42/#21/#19 plus comments, open PRs and branches GET | PASS | #19 owner acceptance retains UNKNOWN; #21 explicitly BLOCKED; no competing task branch/PR/claim. Sole Integrator claim subsequently reread. |
| `git rev-parse HEAD`; `git status --short` before work | PASS / exit 0 | Clean exact-base checkout. Separate worktree created at `/workspace/scratch/a86080fd97c8/semantics42`. |
| `git worktree add -b docs/REC-001E-SEM-001-u09-u10-resolution ... <base>` | PASS / exit 0 | Isolated requested branch and worktree; no shared Cargo/CI mutation. |
| Official source opens / locator and applicability inspection | PASS | Nine official source locators in report reached through the web reader on 2026-10-10. Current preview obtained by following current Changelog link; quantity/delete UNKNOWN is bounded to the inspected set. |
| Direct Python HTTPS fetch of English depth HTML | FAIL / HTTP 403 | Full original response bytes/hash unavailable in this executor; source text accessible through web reader. Does not fabricate a full-page hash. |
| Excerpt fingerprint recomputation, Python SHA-256 | PASS / exit 0 | Nine JSON-decoded UTF-8/LF bounded excerpt hashes verified; these are excerpt fingerprints, not whole-source version hashes. |
| Two-file path audit, question-status vocabulary, required policy/contract markers, relative-link resolution | PASS / exit 0 | Actual working-tree docs audit; exact final-head independent QA remains separate. |
| `git diff --no-index --check /dev/null <each new file>` | NO_WHITESPACE_DIAGNOSTICS / exit 1 | Both outputs empty; `--no-index` implies difference exit status and each new file differs from `/dev/null`. This is not recorded as exit 0. |
| Temporary `GIT_INDEX_FILE`: `git read-tree HEAD`; `git add -- <two allowed files>`; `git diff --cached --check` | PASS / all exit 0 | Both new documents checked through an isolated scratch index; the task worktree index is untouched and no commit/push was made. Final containing SHA not yet available at authorship. |
| Rust fmt/clippy/tests | NOT_RUN / NOT_APPLICABLE | Docs-only diff; no Rust/runtime test result claimed. |
| Public WS capture / source observation | NOT_RUN / OUT_OF_SCOPE | No second transport/capture worker and no raw public archive added. |
| Independent source/contract QA on final PR head | NOT_RUN / PENDING | Integrator must publish exact head and appoint an independent session. |
| Final-head applicable CI | NOT_RUN / PENDING | Existing historical CI does not transfer to the future docs head. |

## Contracts / ADR

No accepted contract changed. `SetLevel(qty>0)` and quantity-free `DeleteLevel`, exact numeric grids/checked arithmetic, immutable artifact activation/closure, current-scope proofs, epochs/barriers, and owner-bound terminal capture/storage rules remain in force. Policy appendix is visibly PROPOSED and recommends retaining the canonical blocker. Its later adoption requires explicit Architecture/owner receipt; any canonical mapping or schema delta needs its own separately scoped accepted decision.

## Deviations and limitations

The deliverable follows the insufficient-evidence branch permitted by #42. New official rollout evidence is relevant to feed-profile applicability but does not solve U-09 or U-10. Absence in the bounded inspected source set is not a claim that no possible official clarification exists anywhere. Search-cache/old-route text is not used as current normative depth evidence.

Full HTML source-body hash unavailable (HTTP 403); bounded excerpts have reproducible hashes and precise public locators instead. No live observation was performed and there are no OBSERVED_ONLY results. Semantic test matrix is a prospective plan, not executed production tests. Independent QA/CI and final SHA receipts are pending root publication; worker neither committed nor pushed nor mutated GitHub.

## Blockers / next decision

Verdict: **BLOCKED_INSUFFICIENT_EVIDENCE**. U-09a/b, U-10a/b and snapshot zero remain UNKNOWN; production rollout applicability has an explicit documentation CONFLICT. [#21](https://github.com/al-gri/pro-sclpng/issues/21) remains BLOCKED. Accepted policy/source decisions, applicable artifacts, implementation and QA remain distinct future gates.

Integrator's next step: publish one draft PR containing exactly the allowed files, record concrete head SHA/check evidence, and request independent source/contract QA. Architecture/owner then records a decision receipt; no automatic M2, merge, settings change or Issue closure.

## Artifacts and replacing this worker

The two permitted files contain all durable conclusions, official URLs, excerpt fingerprints, inference boundaries, proposed decisions and semantic test plan. No raw market archive, account/private source, key or contact with third parties is included. Intermediate web-reader response references identify the worker's extraction but are not substituted for public URLs. Root publication/QA receipts supersede only this handoff's pending publication state; historical UNKNOWN findings remain unchanged.
