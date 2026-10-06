# STATE-002 handoff

## Status

**READY_FOR_REVIEW / GOVERNANCE_ONLY**

Issue: #14  
Branch: `docs/STATE-002-md-accepted`  
Base main SHA: `da3c1cd1086b42d4a06ec4a0c90010b012099557`

The exact final PR head is recorded in PR metadata after this file is committed; this file does not attempt to self-reference the SHA of its containing commit.

## Purpose

Synchronize the repository source-of-truth after accepted MD-001 and make it explicit that REC-001 is ready for decomposition, not monolithic implementation.

## Accepted MD-001 evidence

- PR #13 reviewed head: `76ca481ed9221289e001d120baf4baf8170c205a`
- Independent QA review: `5425727400` — `READY_FOR_OWNER_REVIEW`
- Owner squash merge: `da3c1cd1086b42d4a06ec4a0c90010b012099557`
- Post-merge push CI: run `37440205546` — `completed / success`
- Issue #4: closed as completed with acceptance record comment `6012966234`

## Changed paths

Only:
- `docs/PROJECT_STATE.md`
- `docs/handoffs/STATE-002.md`

No Rust, Cargo, workflows, specs, ADR semantics, exchange fixtures, runtime implementation or repository settings are changed.

## State transition

- MD-001: `DONE / ACCEPTED_IN_MAIN`
- REC-001: `READY_FOR_DECOMPOSITION`
- Runtime connector/book/recorder/replay: still `NOT_IMPLEMENTED`
- Live trading: still `OUT_OF_SCOPE / DISABLED`

## Mandatory downstream constraints

REC-001 decomposition must preserve at least:
- U-09 UNKNOWN/BLOCKED — regular books50 quantity unit
- U-10 UNKNOWN/BLOCKED — zero/delete mapping
- U-20 NOT_PROVEN/FORBIDDEN — REST↔WS stitching
- C-01 CONTRACT_CONFLICT/BLOCKED — RPI canonical normalization
- C-03 DOC_CONFLICT/UNKNOWN — instruments endpoint relationship

Other U/C constraints from the accepted MD-001 document remain authoritative as well.

## Verification

- accepted MD-001 main SHA re-read before branch creation: PASS
- allowed-path scope: PASS by construction
- status/provenance consistency review: PASS
- runtime Rust commands: NOT_APPLICABLE to the docs-only content change
- PR CI: required after PR creation; record exact-head result in PR/Issue, not as a guessed value here

## Next step after owner acceptance

Integrator decomposes REC-001 (#5) into dependency-ordered bounded Issues/PRs. Do not assign the whole epic to one worker and do not let an implementation worker resolve U-09/U-10/U-20/C-01/C-03 by assumption.
