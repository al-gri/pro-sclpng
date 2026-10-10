# REC-001F-3 implementation pre-allocation stop

Status: **BLOCKED_ALLOCATION_GATE / PARTIAL_IMPLEMENTATION / NOT_ACCEPTED**.
GitHub Issue #45 is authoritative. This is a stopped experimental increment,
not the intended capture/WAL/replay result and not independent QA.

## Authority and executor

Accepted base `273bfac01bc7a7954644e5270eb96cc99d787fab`, tree
`abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`; related governance diff is docs-only.
Architecture reviewed at `830f8953dca540e751fbd5645d0532735b8c6527`.
Sole Integrator implementing sequentially, claim
https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097721380.
One branch `feat/REC-001F-3-public-text-capture`; final head/tree and command
receipts are pinned in its one Draft PR and #45 after all commits.

Actual host: Windows 10.0.26200 x64, PowerShell 7.6.5, Git 2.55.0.windows.3,
gh 2.97.0; discovery used Get-Command. Docker Desktop 4.68.0, Engine 29.3.1,
desktop-linux, Debian 12 amd64 / WSL2 kernel 6.6.87.2.
Image `rust:1.98.1-bookworm`, image/RepoDigest
`sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`.
Container `gate45-impl-20261010`, ID
`a29ad0b138348f8a8a2d3052d96c3dc75f772df72230f4fe3bde3cab42bba108`.
Rust 1.98.1 `48a229ceaefd4985c50990b14116b6d856af0985`, Cargo 1.98.1
`797e8a9bca276c1c9f9f738d2a20f484fa4eea9d`, Linux GNU target.
New `CARGO_HOME=/cargo` bind cache had zero files before locked preparation;
target directory `/evidence/target` is outside the checkout.

## Implemented and tested scope

Unactivated transport primitives validate passive server Text and produce a
fresh ring secure_random mask per outgoing frame. The send helper enqueues once
and flushes that same frame against the original deadline and atomic stop flag.
Its caller must supply counted authority, prepaid allocation and bounded I/O;
no owner callback, Ready proof or live capture entry point is provided.

Linux tests include the actual helpers and exact locked library sources:
partial writes/WouldBlock/physical flush, cancellation after three bytes,
expired deadline before writes, unsupported native opcodes without automatic
writes, fragments/RSV/masked server/binary/UTF8 refusal, oversized advertised
length before reserve, real numeric TCP slow drip with the unchanged first
progress +2s deadline, TLS12/13 final drain and TLS101 + first Text in one TLS
record, invalid Accept/redirect/extension refusals.

All loopback certificates, keys, fault inputs and clocks are explicitly
synthetic. The embedded localhost key is not an operational secret. The live
route mode is opt-in through GATE45_PROBE_MODE=live and an externally prepared
GATE45_NUMERIC_ADDRESS. Ordinary CI never selects it. It obtains only the first
Text, retains its bytes privately and closes by dropping the probe socket; it
does not claim recording-owner Close, ACK acceptance, Books50, WAL or replay.

## Exact stop and minimum continuation

rustls 0.23.45 `set_buffer_limit(65536)` controls application buffers, not the
decoded certificate list. `msgs/codec.rs` Vec::read uses infallible push before
certificate validation. TLS12/13 structurally accepted synthetic flights cause
decoded storage realloc from 393216 to 786432 bytes. Reserving old+new before
System allocation requires 1241088–1246007 bytes beyond the diagnostic baseline.
This is a conservative moving-realloc envelope, **not a measured retained
1MiB breach**, and the diagnostic baseline includes fixtures/harness.

The probe guard denies before System. Both guarded library calls terminate with
SIGABRT (6), shell equivalent 134. They cannot return an error through the
owner callback. Those outcomes are KILLED/Unknown/incomplete, never successful
Close or seal. The guard is diagnostic, not a production allocator: it is not
category-complete TLS lifetime accounting or whole-app ownership accounting.
Measured peak, baseline-plus-allowance and a passing expected-abort test do not
establish the required production bounds.

Consequently TLS/certificate pre-allocation enforcement is **BLOCKED** and the
absolute whole-application 8MiB composition remains **NOT_RUN**. Following #45's
failed-allocation-verification stop, the dependent owner-bound capture, ACK
binding, real WAL, two replay processes and live acceptance were not enabled.
This does not assert that every possible adapter is impossible.

The one next step is to continue this same branch/PR with a source-backed,
checked production allocation boundary: count retained TLS objects and
existing application ownership, reserve conservative realloc coexistence,
refuse before the over-budget allocator call, and expose a lawful error or
honest KILLED/Unknown/incomplete outcome without fabricating owner completion.
Prove the fixed 1MiB TLS allowance and total 8MiB under normal and adversarial
TLS12/13 inputs before enabling their dependent path. If that requires new
input limits, patched/substituted dependencies, public APIs or paths, first
obtain the explicit scope/ADR decision required by #45; do not relax budgets
or remove TLS12 silently. Then complete the remaining authorized implementation
and evidence in the existing PR. QA starts only after a real final implementation
head exists; its packet must bind that head/tree and private WAL receipt.

## Dependency and validation interpretation

Four exact candidates only: tungstenite0.30.0 handshake/no defaults,
rustls0.23.45 std/ring/tls12/no defaults, webpki-roots1.0.9,
signal-hook0.4.5/no defaults. Sole Integrator generated the root lock and added
explicit online locked fetch before offline checks in each of the three CI
jobs; no parallel root-file writer or unrelated upgrade.

Current cache audit verifies all 57 registry archives, sparse-index/lock
checksums, 2733 extracted files, licenses/provenance and resolved features.
43 registry packages are active on Linux. Maximum declared dependency MSRV
is 1.85; five undeclared MSRVs remain unknown. Compilation with pinned1.98.1
proves the active Linux closure's compatibility, not all-platform/minimum-MSRV
proof. Source inventory SHA256:
`936be4d5c22a3fc27639feba1bd0d8415fb91f3b2dc7fedce852eaaf1efa152c`.

Actual test totals and final source/log hashes are reported in the GitHub
receipt; 577 historical tests are not reused as a new-head result. Frozen
F-1/F-2, crate APIs/specs/ADRs and root toolchain remain byte-identical to base.
Missing captured WAL/evidence/replay digests are explicitly NOT_RUN, not empty
or synthetic replacements. usable_data remains false; M1 is not accepted,
#21 is not launched, M2 remains BLOCKED, merge/acceptance remain with the owner.
