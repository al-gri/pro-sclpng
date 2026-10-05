# Handoff: BOOT-001

Status: PARTIAL — implementation prepared; CI evidence and generated lockfile pending.
Issue: https://github.com/al-gri/pro-sclpng/issues/2
PR: not opened at this snapshot; final PR link and head will be recorded in Issue #2.
Role/session: WORK — BOOT-001
Base SHA: f6c4af55a79e61078a6c3f9d97295c8fd32f831e
Tested head SHA: NONE — Rust checks have not run at this snapshot.
Branch: feat/BOOT-001-workspace-ci

## What is implemented

Exactly two dependency-free workspace members; documented empty domain library;
strict offline/shadow CLI; 15 integration tests on Linux invoking the real binary
and checking positive, invalid, repeated, trailing, and non-Unicode arguments.
All accepted modes disable execution and terminate without network or signals.
Pinned stable Rust 1.98.1; three run-only CI jobs with read-only permissions,
exact event-head checkout/version verification, and clean-tree gates.

## Changed files

`Cargo.toml`, `rust-toolchain.toml`, `crates/domain/Cargo.toml`,
`crates/domain/src/lib.rs`, `apps/radar/Cargo.toml`, `apps/radar/src/main.rs`,
`apps/radar/tests/cli.rs`, `.github/workflows/ci.yml`, `docs/DEVELOPMENT.md`,
`docs/handoffs/BOOT-001.md`. `Cargo.lock` is pending actual Cargo generation in CI.
No baseline, specs, ADR, README, .gitignore, or other paths are changed.

## Checks

| Command/check | PASS/FAIL/NOT_RUN | Environment/exit code/log |
|---|---|---|
| Base, PR #8, scope/claim preflight | PASS | GitHub connector reads; base matches; no earlier worker claim |
| `git ls-remote ... refs/heads/main` | FAIL | Debian 13.3 x86_64; exit 128: Could not resolve host: github.com |
| Local checkout cleanliness | NOT_RUN | No local checkout; source staging is not a checkout |
| Local Rust toolchain/build/fmt/clippy/test | NOT_RUN | Rust tools absent from PATH |
| CI Rust installation and lockfile generation | NOT_RUN | Planned, no run at this snapshot |
| `cargo build --workspace --locked` | NOT_RUN | Planned in rust-tests |
| `cargo fmt --all -- --check` | NOT_RUN | Planned in rust-fmt |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | NOT_RUN | Planned in rust-clippy |
| `cargo test --workspace --locked` | NOT_RUN | Planned in rust-tests |

## Contracts / ADR

No domain contracts or architecture changes. CLI mode parsing is private to radar.
SPEC-001 remains responsible for the first domain APIs. No live execution path,
market connection, level calculation, signal generation, or future milestone code.

## Deviations and limitations

GitHub API writes replace local checkout work because DNS fails and Rust is absent.
Real runtime verification must take place in CI. The first bootstrap CI run will
generate and print Cargo.lock and fail the tracked-lockfile gate until those exact
bytes are committed. That run is exploratory, not an acceptance PASS.
The actual runner image/toolchain and final tested head must be taken from logs,
not assumed. The announcement confirms release existence, not local installation.
No performance or strategy claim. Source review is not an enforced network sandbox.

## Blockers / open issues

Until generated Cargo.lock is committed and all three checks pass on the final PR
head, BOOT-001 is not ready for QA acceptance. Workflow write/run capability is
unproven at this document snapshot. If it fails, preserve the exact workflow patch
and error in the PR/handoff without attempting a permissions bypass.

## Artifacts

Implementation and workflow live in the task branch. Final commit/CI log links
will be recorded in the PR after they exist. No invented self-referential head SHA.

## Next step

Run the first PR CI, commit Cargo's generated lockfile, and rerun all three checks
on the final head before the separate QA review. Owner alone decides merge.

## Replacement-session data

Task scope and worker claim are in Issue #2; code and checks are in the task branch.
Final results belong in this handoff and the PR. No secret or private data included.
Integrator/owner acceptance of this handoff has not occurred.
