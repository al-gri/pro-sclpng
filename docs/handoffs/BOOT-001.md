# Handoff: BOOT-001

Status: DONE — worker implementation verified; independent QA/owner acceptance pending.
Issue: https://github.com/al-gri/pro-sclpng/issues/2
PR: https://github.com/al-gri/pro-sclpng/pull/9
Role/session: WORK — BOOT-001
Base SHA: f6c4af55a79e61078a6c3f9d97295c8fd32f831e
Tested head SHA: b2bf5470bbd2d15d96f4e08c881fd81f85f68214
Branch: feat/BOOT-001-workspace-ci

This is the evidence snapshot before the documentation-only commit containing
this handoff. That commit must run all checks again. The final head SHA and its
fresh results are recorded in PR #9 after they exist; the earlier green run is
not proof for a later SHA. No self-referential future commit hash is invented.

## What is implemented

Exactly two dependency-free members: documented empty domain library and strict
offline/shadow radar binary. Fifteen Linux integration tests execute the real
binary and check success/invalid/live/repeated/trailing/non-Unicode arguments,
exit code, stdout and stderr. Execution remains disabled; no network or signals.
Stable Rust 1.98.1 is pinned and its installation is proved in CI.
Three independent run-only CI jobs verify exact event-head checkout, compiler,
required Cargo commands, lockfile reproducibility, and clean Git status.

## Changed files

`Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `crates/domain/Cargo.toml`,
`crates/domain/src/lib.rs`, `apps/radar/Cargo.toml`, `apps/radar/src/main.rs`,
`apps/radar/tests/cli.rs`, `.github/workflows/ci.yml`, `docs/DEVELOPMENT.md`,
`docs/handoffs/BOOT-001.md`. No other paths changed; no custom rustfmt config.

## Checks

Successful CI run: https://github.com/al-gri/pro-sclpng/actions/runs/37299608362 .
The CI results in this table refer to source SHA
`b2bf5470bbd2d15d96f4e08c881fd81f85f68214`.

| Command/check | PASS/FAIL/NOT_RUN | Environment/exit code/log |
|---|---|---|
| Base/PR #8/claim/scope reads and GitHub writes | PASS | GitHub connector; only allowed repository/paths |
| TOML/YAML parse, embedded shell `bash -n` | PASS | Local staged source, Python/Bash exit 0; not Rust execution |
| `git ls-remote ... refs/heads/main` | FAIL | Debian 13.3 x86_64, Git 2.47.3; exit 128: Could not resolve host: github.com |
| Local checkout and all local Rust commands | NOT_RUN | No checkout; Rust tools absent |
| CI exact SHA checkout and Rust install/selection | PASS | All three jobs, exit 0; actual versions below |
| `cargo generate-lockfile --offline` and tracked lockfile comparison | PASS | rust-tests, exit 0; generated file equals committed Cargo.lock |
| `cargo fmt --all -- --check` | PASS | rust-fmt job 111728937919, exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | PASS | rust-clippy job 111728937631, exit 0 |
| `cargo build --workspace --locked` | PASS | rust-tests job 111728937910, exit 0 |
| `cargo test --workspace --locked` | PASS | rust-tests job 111728937910, exit 0; 15 CLI tests passed, 0 failed/ignored |
| Final `git diff`, cached diff, `git status --porcelain` gates | PASS | All three jobs, exit 0; empty status, no tracked-file changes |

Actual CI environment: Ubuntu 24.04.5 LTS x86_64; runner image 20260927.320.1;
Git 2.55.0; active `1.98.1-x86_64-unknown-linux-gnu`; rustc 1.98.1
(48a229cea 2026-09-01), Cargo 1.98.1 (797e8a9bc 2026-08-05), rustfmt
1.9.0-stable, Clippy 0.1.98, LLVM 22.1.8.

Successful job logs:
- https://github.com/al-gri/pro-sclpng/actions/runs/37299608362/job/111728937919
- https://github.com/al-gri/pro-sclpng/actions/runs/37299608362/job/111728937631
- https://github.com/al-gri/pro-sclpng/actions/runs/37299608362/job/111728937910

### Initial failed run and correction history

Run https://github.com/al-gri/pro-sclpng/actions/runs/37299372900 tested
`10c42763c25539c990cf21df3367a745a533fba1`. Toolchain installation and actual
`cargo generate-lockfile --offline` passed (exit 0). The following tracked-lockfile
gate failed (exit 1), because Cargo.lock was not yet committed. Build and tests
were skipped, therefore NOT_RUN. Clippy failed (exit 101) due to the missing
lockfile under --locked. Formatting failed (exit 1) with one multiline writeln
diff. These failures are not relabeled PASS.

The exact lockfile bytes printed by Cargo 1.98.1 in job 111728177941 and the
formatter's actual diff were committed in `b2bf5470bbd2d15d96f4e08c881fd81f85f68214`.
No gate was weakened. Cargo.lock SHA256:
`48417f69201a3c88df1a833ae5a63b2a52fb0374fdd232b34632a085430755c2`.

Initial job logs:
- https://github.com/al-gri/pro-sclpng/actions/runs/37299372900/job/111728177941
- https://github.com/al-gri/pro-sclpng/actions/runs/37299372900/job/111728178330
- https://github.com/al-gri/pro-sclpng/actions/runs/37299372900/job/111728178234

## Contracts / ADR

No accepted contract or architecture changes. CLI internals stay in radar.
SPEC-001 owns future domain APIs. No Bitget, levels, trading signals, strategy,
recording/replay, credentials, or order execution were added. The domain library
has no artificial tests or domain API pretending to implement SPEC-001.

## Deviations and limitations

GitHub API writes replace local checkout work because DNS fails and Rust is absent.
CI provides the actual clean checkouts and Rust execution. Local Rust remains
NOT_RUN and must not be inferred from CI success. Source staging is not a checkout.
The first exploratory CI generated Cargo.lock; no hand-written resolution was
substituted. API update_ref rejected optional expected_sha at argument binding;
the branch was reread unchanged and updated with ordinary fast-forward force=false.
No force push, settings change, credential, other repository, or external Action
was involved. Workflow write, execution, and decoded job-log reads all worked.

Cargo offline mode is not a network sandbox; source and dependency review also
check the absence of runtime/test network calls. Network use for fetching this
repository and Rust is CI setup only. Runtime has no networking or live path.
The CI event tested so far is pull_request. The push-to-main event can only be
observed after an owner merge; it is configured but its execution is NOT_RUN.
No cross-platform, performance, latency, profitability, or strategy claim.

## Blockers / open issues

No implementation blocker remains at the tested head listed above. For a later
head, require fresh success from all three checks in PR #9. Independent QA and
owner acceptance remain pending; CI is not approval and no merge is authorized.
Required-check repository settings are unchanged.

## Artifacts

PR #9 contains all 11 changed files, generated lockfile, workflow, source tests,
development commands, and CI history. Final head and fresh job URLs belong in
that PR after the last commit. No secret, private material, or raw dataset added.

## Next step

After the final-head CI gate, hand PR #9 and its exact tested head to a separate
QA — BOOT-001 session. Owner alone decides merge after review.

## Replacement-session data

Issue #2 contains the packet and worker claim. PR #9 contains implementation,
failed and successful CI evidence, and final SHA. This handoff records the
verified snapshot and outstanding QA/owner decision. No unique implementation
remains only in chat or local staging. Integrator/owner has not accepted handoff.
