# Handoff: SPEC-001 — implementation continuation

Status: **PARTIAL / WINDOWS_RETEST_REQUIRED**. All new contracts and ADR remain **PROPOSED**.
Repository: `al-gri/pro-sclpng` only. Same Issue #3, branch `feat/SPEC-001-domain-contracts`, Draft PR #10.
[Packet](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559) · [Implementation approval](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5418412674).
Base/main remains `6c520237d35865c79dba9e74fa64bd4c2c9e419f`. Approved design revision: `272f6ec50cd0df3630f37ef99cb8b3bb54a967d7`.
This file deliberately does not embed its own containing commit SHA; exact final head and CI are recorded post-commit in PR #10.

## Evidence correction retained

The earlier PARTIAL report incorrectly cited run `37363552027`; a fresh API read returned 404. The historical report remains visible. The corrected exact-`c393439d...` run is [37363320459](https://github.com/al-gri/pro-sclpng/actions/runs/37363320459): workflow conclusion `failure`; rust-tests job `111953533746` completed/success; rust-clippy `111953532296` and rust-fmt `111953532573` were cancelled with `steps=null`. Cancelled is **NOT_RUN**, neither PASS nor demonstrated lint failure.

The full rust-tests log was re-read in this continuation and confirms Expected SHA = Checked out SHA = `c393439dc90ec381479889303f415d8725da68d8`, Ubuntu 24.04.5 LTS/Linux x86_64, Rust/Cargo 1.98.1. `cargo build --workspace --locked` and `cargo test --workspace --locked` succeeded; 68 contracts + 9 identity + 18 numeric = **95 domain integration tests passed**, separately **15 BOOT-001 CLI tests passed**, zero failed/ignored. Cargo.lock comparison and final checkout cleanliness passed. This is historical input-head evidence, not final-head lint/test evidence.

At preflight for this continuation, PR head was newer than c393: `9b3ee661b81a035c7acbfaf7ca2e463b0cf407f5`, parented by `f1f1209dd180ecc69bd69fc923929308d4ff0aec`. It already contained the memory-only WAL/recovery implementation and independent binary fixtures; no rollback was performed.

Fresh local environment: Linux x86_64, Git 2.47.3, Python 3.13.5; rustc/cargo/rustup/rustfmt/clippy unavailable. `git ls-remote https://github.com/al-gri/pro-sclpng.git HEAD` failed DNS with exit 128. There is no local checkout; API authoring is not reported as one. Worker-local Rust commands remain **NOT_RUN**; the later owner Windows standalone result is recorded below.

## Implemented scope

Production `crates/domain/src/**` remains limited to approved value types/DTOs/pure guards: exact numeric conversions, qualified identities/versions/epochs, event/source/application/control identity and causal guards, UNKNOWN, policy tags, ArtifactRef parsing/identity metadata, and recorded-input shapes. No production byte codec, filesystem storage, hash implementation, loader/verifier, connector, book engine, queues, publisher, replay engine, strategy or execution was added.

Test-local `crates/domain/tests/support/**` now additionally contains:
- bounded memory-only WAL framing/payload codec, exact CRC-32/ISO-HDLC, checked lengths/caps/offsets and exact EOF;
- definitions/reference/order validation, local/source GAP accounting, segment/archive seal checks and recovery classification;
- frozen independent single- and multi-segment raw/control/GAP/seal fixtures under `tests/fixtures/domain/wal-frames-v1.txt`;
- PSAD/PSAM typed memory encodings, typed artifact bodies and PSCO commitment bytes; no SHA-256 implementation or production verifier;
- frozen six policy variants with valid descriptor/body claims and WAL CRCs;
- every-byte truncation, middle corruption, whole-frame/seal loss and trailing-data recovery assertions;
- V2 positive/reversed GAP frames with independent literal CRCs and exact scope offset 56.

This continuation also adds executable AF/PSAM/PSCO/policy-byte assertions and direct E15/H13/H16/H18 matrix coverage. These are synthetic tests only.

## Matrix / vector → executable test mapping

Execution result for rows added or changed by the containing commit is **NOT_RUN on the containing head until exact-head CI completes**. Rows already present at c393 have historical PASS on c393 only; final-head status must come from the post-commit evidence.

### Numeric N01–N21

| Vector IDs | Test file → function | Checked outcome |
|---|---|---|
| N01,N03 | `numeric.rs::n01_n03_price_grid_and_canonical_reverse`; `identity.rs::n01_n17_qualified_conversions_preserve_reference_and_units` | exact ticks/canonical reverse/reference |
| N02 | `numeric.rs::n02_quantity_grid_and_reverse` | exact steps/reverse |
| N04,N05 | `numeric.rs::n04_n05_off_grid_never_rounds` | exact OffGrid, no rounding |
| N06 | `numeric.rs::n06_invalid_ascii_grammar` | InvalidSyntax cases |
| N07 | `numeric.rs::n07_signs_are_rejected` | InvalidSign |
| N08 | `numeric.rs::n08_input_bound_and_error_priority` | 96/97-byte priority, ZeroPrice |
| N09 | `numeric.rs::n09_significant_scale_only` | canonical zeros / ScaleTooLarge |
| N10,N11 | `numeric.rs::n10_n11_count_boundaries` | u64 MAX / CountOutOfRange |
| N12 | `numeric.rs::n12_coefficient_max_and_add_overflow` | u128 boundary/add overflow |
| N13 | `numeric.rs::n13_coefficient_multiply_overflow` | multiply overflow |
| N14 | `numeric.rs::n14_metadata_does_not_repair_noncanonical_values` | invalid increment/noncanonical |
| N15 | `numeric.rs::n15_alignment_overflow_is_not_cancelled` | checked alignment overflow |
| N16 | `numeric.rs::n16_reverse_multiply_overflow` | reverse multiply overflow |
| N17 | `numeric.rs::n17_constant_multiplier_round_trip`; qualified identity test | exact base conversion |
| N18 | `numeric.rs::n18_unknown_and_invalid_multiplier`; `identity.rs::n18_units_unknown_nonlinear_and_metadata_guards` | Unknown/Invalid/Unit/Unsupported |
| N19 | `numeric.rs::n19_intermediate_overflow_and_final_scale` | intermediate overflow / final scale |
| N20 | `numeric.rs::n20_numerical_zero_is_not_trade_or_level`; `events.rs::e17_n20_level_structure_and_zero_deletion_distinction` | numeric zero vs event zero/delete |
| N21 | `identity.rs::n21_identity_and_spec_mismatch_precede_parsing_and_comparison` | IdentityMismatch/SpecMismatch |
| bounded exhaustive | `numeric.rs::n_exhaustive_small_grids_and_reverse` | coefficient/scale/increment/step loops |

### Event/order E01–E17

| IDs | File → function | Checked outcome |
|---|---|---|
| E01,E02 | `identity.rs::e01_e02_market_and_book_identities_are_distinct` | spot/futures, Normal/RPI separation |
| E03 | `identity.rs::e03_channel_scope_and_slot_conflicts`; `events.rs::e03_missing_raw_source_index_and_epoch_are_rejected` | owner/spec/scope failures |
| E04,E05,E08 | `events.rs::e04_e05_e08_source_application_ids_and_replay_are_distinct` | recorded order, source/apply IDs, deterministic replay |
| E06 | `events.rs::e06_redelivery_checks_all_canonical_fields` | no-op vs IdentityConflict |
| E07 | `events.rs::e07_record_and_output_order_retain_prefix_on_error` | exact order/sub-index failure |
| E09 | `events.rs::e09_v_r4_timeline_same_normalizer_then_new_revision` | activation timeline/cursors |
| E10 | `artifacts.rs::v_r4_missing_dependency_has_no_partial_success`; `v_r4_same_norm_reuse_early_loading_and_rebinding` | missing/rebind/profile closure |
| E11,E12 | `events.rs::e11_e12_clocks_units_and_unix_jumps_are_not_order` | incomparable clock / original time |
| E13 | `events.rs::e13_causal_frontiers_references_and_availability`; `same_step_earlier_effect_is_available_but_later_is_not` | future refs, cursor/available_at |
| E14 | `events.rs::e14_unknown_is_preserved_and_payload_reinterpretation_rejected` | UNKNOWN preservation |
| E15 | `events.rs::e15_new_capture_namespace_changes_ids_while_same_archive_replay_is_stable` | same archive stable; new ArchiveId distinct |
| E16 | `identity.rs::e16_positive_ids_and_checked_exhaustion`; `e16_epoch_expected_and_rollback` | rollback/exhaustion |
| E17 | `events.rs::e17_n20_level_structure_and_zero_deletion_distinction`; `v_r1_mixed_and_invalid_whole_frame_have_no_partial_acceptance` | caps/duplicate/atomic rejection |

### DataHealth H01–H19 and named R/C vectors

| IDs / named vectors | File → function(s) | Checked outcome |
|---|---|---|
| H01,H12 | `health.rs::h01_h12_snapshot_warmup_and_false_witness_guards` | no snapshot / truthful vs false witness |
| H02,C2 | `v_c2_progress_caps_per_output_and_freezes_after_usable`; `v_c2_max_is_a_supplied_boundary_state_not_billions_of_updates` | warm-up count/cap/freeze |
| H03,R2-FINITE/NO-FENCE | `publication.rs::v_r2_finite_receipt_then_final_fence_includes_receipt_in_basis`; `v_r2_missing_behind_weak_wrong_scope_future_and_unverified_fences` | candidate/fence boundary |
| H04,H05,R1-BARRIER/RESYNC | `health.rs::v_r1_barrier_and_post_barrier_resync`; pending bounds test | invalidation/new resync |
| H06 | `shared.rs::v_r6_shared_registration_down_and_epoch_preserve_other_connection`; `health.rs::v_r1_down_up_and_config_change_do_not_reuse_pending_snapshot` | epoch/barrier old proof does not restore |
| H07,R6 | `shared.rs::v_r6_shared_registration_down_and_epoch_preserve_other_connection` | connection fan-out isolation |
| H08 | same R6 test | RPI new stream remains NoSnapshot while Normal stays usable |
| H09,H10 | `health.rs::v_r3_freshness_before_equal_after_none_and_overflow` | exact expiry/None |
| H11 | `v_r3_quiet_original_observation_bounds_and_expiry`; `v_r3_quiet_future_is_not_automatically_activated_by_timer`; `v_r3_old_quiet_proof_does_not_poison_new_scope` | bounded quiet rules |
| H13 | `v_r1_down_up_and_config_change_do_not_reuse_pending_snapshot`; `h13_spec_activation_invalidates_only_after_declared_new_spec` | config/normalizer/spec activation invalidation |
| H14 | `v_r1_down_up_and_config_change_do_not_reuse_pending_snapshot` | Up does not clear barrier |
| H15 | `publication.rs::v_r2_revoked_candidates_cannot_be_resurrected_by_late_fence`; recording-order test | Failed recording revokes permit; no production GAP write is simulated |
| H16 | `health.rs::h16_recording_health_recovery_does_not_resync_invalid_book` | recorder Healthy does not resync book |
| H17 | `publication.rs::v_r2_self_future_none_and_regressing_receipts_retain_prefix`; `v_r2_recording_order_partial_failure_and_weaker_evidence` | invalid acknowledgements retain prefix |
| H18 | `health.rs::h18_restart_has_new_archive_clock_and_no_inherited_ready_state` | no inherited definitions/readiness/candidate |
| H19 | `health.rs::h19_missing_current_artifact_blocks_without_advancing_state` | BLOCKED, no synthetic-as-live fallback |
| R1 reorder/dup/conflict/atomic/deadline | corresponding `health.rs::v_r1_*` functions | exact IDs/cursors/retained state |
| R2 mode/gate/revoked/superseded | `policy.rs::v_r2_mode_gate_all_nine_cells`; `publication.rs::v_r2_*` | nine cells + permit diagnostics |
| R3 | all `health.rs::v_r3_*` | freshness/quiet boundaries |
| R4 | `artifacts.rs::v_r4_*`; event timeline | parse/missing/rebind/caps/activation |
| R5 | `accounting.rs::v_r5_*` plus WAL W15/W16 | local/source loss semantics |
| C1 | `identity.rs::v_c1_writer_and_shared_connection_guards` | one writer/book/archive |
| C2 | C2 tests above | progress semantics |

### WAL W00–W20 and targeted binary vectors

| IDs | File → function | Checked outcome |
|---|---|---|
| W00 | `wal.rs::w00_crc_independent_known_vectors_and_streaming` | independent CRC constants |
| W01 | `w01_encoder_and_decoder_match_independent_start_golden` | 74-byte W01, checksum, prefix |
| W02 | `w02_full_single_golden_exact_dtos_seals_and_quality`; `w02_full_multi_segment_golden_chain_and_inherited_state` | independent full single/multi-segment goldens |
| W03 | `w03_unknown_header_control_and_encoding_tags_report_exact_offsets` | unsupported versions/kinds/tags/offsets |
| W04 | `w04_magic_flags_reserved_options_bools_tokens_and_trailing_bytes` | canonical payload failures |
| W05,W10 | `w05_w10_middle_corruption_does_not_scan_for_following_magic` | checksum/middle stop/no magic rejoin |
| W06,W07 | `w06_w07_caps_and_checked_offsets_precede_allocation` | cap/overflow before allocation |
| W08 | `w08_every_start_byte_cut_is_incomplete_at_zero` | every W01 byte cut |
| W09 | both `w09_every_byte_cut_*` | every byte cut across complete and segment boundary |
| W11,W17 | `w11_whole_raw_final_seal_or_segment_loss_never_means_complete` | missing assigned frame/seal/final segment cannot look complete |
| W12,W20 | `w12_w20_forged_seal_fields_and_wrong_aggregate_exclude_no_checks` | recomputed own CRC cannot authorize false chain/aggregate |
| W13 | `w13_wrong_segment_link_or_detached_segment_fails_at_boundary` | identity/link/segment boundary |
| W14 | `w14_trailing_bytes_after_archive_seal_are_not_ignored` | exact EOF / trailing data |
| W15 | `w15_unresolved_local_loss_requires_unknown_quality_even_when_sealed` | unresolved count not zero; quality Unknown |
| W16 | `w16_local_gap_covers_attempts_but_source_gap_does_not` | local/source GAP separation |
| W18 | `publication.rs::v_r2_recording_order_partial_failure_and_weaker_evidence` | partial/weak watermark does not advance stronger completion |
| W19 | `policy.rs::v_r2_mode_gate_all_nine_cells`; publication fence tests | mode×gate and receipt/fence |
| V2-WIRE-GAP-ORDER | `wal.rs::v2_gap_order_bytes_and_exact_decode_before_transition` | bytes 56..59, CRC, state/cursors |
| V2-WIRE-GAP-REVERSED | `wal.rs::v2_gap_reversed_has_own_valid_crc_and_preserves_prefix_and_state` | valid own CRC, Unsupported scope at 56, retained state |
| AF/PSAD/PSAM | `artifact_wire.rs::af_psad_bodies_and_psam_match_frozen_bytes_without_hashing` | exact frozen bytes/lengths; no hashing claim |
| PSCO | `artifact_wire.rs::psco_snapshot_and_update_goldens_decode_to_exact_outputs` | exact commitment bytes and operation error |
| warm-up/quiet artifact bodies | `artifact_wire.rs::warmup_and_quiet_body_goldens_bind_scope_time_and_basis_records` | scope/time/basis bytes |
| V2 policy all tags/match | `artifact_wire.rs::v2_policy_binary_all_tags_match_wal_and_descriptor` | all six valid tag pairs and exact offsets |
| V2 policy unsupported/mismatch/no ordinal cast | `artifact_wire.rs::v2_policy_binary_unsupported_and_supported_mismatch_reach_target_guards` | 0/255, supported mismatch, gate3 != watermark3 |

## Current verification state

The first current-head run for `9b3ee661...`, [37369820809](https://github.com/al-gri/pro-sclpng/actions/runs/37369820809), initially completed/failure with all three jobs cancelled and `steps=null`; therefore it supplied **no** fmt/Clippy/test verdict. A rerun request was accepted for rust-clippy and the run became queued; additional per-job rerun requests were refused while the run was active. No PASS is inferred from this.

The containing commit adds substantive tests/handoff work rather than an empty CI trigger. Its exact-head CI results are intentionally not guessed here and must be attached to PR #10 after publication. If actual fmt/Clippy diagnostics appear, fix them without allows/ignored tests or semantic changes. Existing workflow is not modified.

Required final commands remain:
`cargo build --workspace --locked`;
`cargo fmt --all -- --check`;
`cargo clippy --workspace --all-targets --locked -- -D warnings`;
`cargo test --workspace --locked`;
and separately `cargo test -p domain --locked`.

The existing workflow executes the first four through three jobs but does **not** execute the separate `cargo test -p domain --locked`. With no local Rust checkout, that standalone command is currently **NOT_RUN / verification blocker** unless another authorized environment executes it. Linux CI is not Windows evidence.

## Boundaries / next action

std-only and Rust 1.98.1 retained; no dependency/crate/feature/build-script changes. Root Cargo.toml/Cargo.lock/toolchain/CI, apps/radar, PROJECT_STATE, specs registry, accepted ADR-0001 and other handoffs remain unchanged.
No production connector/book/queues/supervisor/publisher/filesystem recorder/replay/artifact loader/hash/verifier/storage-fence producer/strategy/TradePlan/Telegram/execution.
Real Bitget feed semantics remain UNKNOWN/BLOCKED_BY_MD_001. Physical fsync/crash/power-loss and external delivery remain OUT_OF_SCOPE/NOT_RUN.

Next: obtain exact-head CI for this implementation, repair only demonstrated formatting/Clippy/test failures, then update this handoff once more with actual results. Final exact SHA remains a post-commit PR comment. No merge/auto-merge/force-push/settings changes or Issue #3/#6 closure.

## Pre-final exact-head evidence before formatter correction

Exact-head CI on `a38d5dcf24e04bdcd7766e4cfc33e557bfab9039`: [run 37374378766](https://github.com/al-gri/pro-sclpng/actions/runs/37374378766), event pull_request, completed/failure solely because the formatter check failed. The jobs were actually acquired and executed:
- rust-tests job `111979126078`: **PASS**. Expected SHA = checked out SHA = `a38d5dcf24e04bdcd7766e4cfc33e557bfab9039`; `cargo build --workspace --locked` and `cargo test --workspace --locked` passed; Cargo.lock comparison and final cleanliness passed. Executed domain integration binaries: 73 contracts + 9 identity + 18 numeric + 26 wire = **126 domain tests passed**, zero failed/ignored; separately **15 BOOT-001 CLI tests passed**.
- rust-clippy job `111979125739`: **PASS**, including `cargo clippy --workspace --all-targets --locked -- -D warnings`, clean final checkout.
- rust-fmt job `111979126135`: **FAIL**, with actual `cargo fmt --all -- --check` diff. This is a demonstrated formatting defect, not a cancelled/queued inference. The containing commit applies that exact formatter output across the allowed `crates/domain/**` files without semantic changes.

The containing commit therefore requires fresh exact-head CI before any QA-ready claim. Its final SHA is intentionally recorded only in the PR after commit. The existing workflow still does not execute the separate mandatory `cargo test -p domain --locked`; with no local Rust toolchain or checkout, that command remains **NOT_RUN / verification blocker** unless an authorized environment runs it. Linux CI is not Windows evidence.


## Windows standalone portability failure and fix

Owner verification on exact SHA `dc7af8369e9c30c6877aae1b13cd23901c765cb3`, Windows 11 x64 / PowerShell 5.1 / `1.98.1-x86_64-pc-windows-msvc`, executed the mandatory standalone command `cargo test -p domain --locked` and **FAILED**. The `wire` binary reported **10 passed, 16 failed, 0 ignored**. The shown failures converged at `crates/domain/tests/support/binary_fixtures.rs:211:49` while locating an AF Markdown heading.

The confirmed cause is test-fixture portability, not contract semantics: `AF_MD` is loaded with `include_str!`, while the frozen artifact parser searched LF-only delimiters such as `"\\n## {id}\\n"`, `"Descriptor:\\n\`\`\`text\\n"` and `"\\nBody:\\n\`\`\`text\\n"`. A Windows checkout may materialize the Markdown wrapper with CRLF, so the parser could not find the heading.

The containing fix normalizes only CRLF→LF in the **textual Markdown wrapper before heading/fence lookup**. Literal hex is decoded only after that normalization; descriptor bytes, body bytes, claimed digests, WAL bytes, CRCs and contract semantics are unchanged. No production API/dependency/hash/verifier is added and no normative golden is rewritten for Windows.

Regression test: `artifact_wire.rs::af_markdown_wrapper_is_crlf_portable_without_changing_fixture_bytes` constructs a synthetic CRLF wrapper and proves identical descriptor/body bytes versus the LF wrapper for every Markdown-backed frozen AF ID used by the helper: AF-N1, AF-B1, AF-C1, AF-F1 and AF-V1. AF-I1 is sourced from the line-oriented WAL fixture and does not use the Markdown parser.

The other textual fixture readers in `binary_fixtures.rs` (`wal-frames-v1.txt` and `policy-variants-v1.txt`) parse through `str::lines()`, which accepts LF and CRLF line endings; no analogous exact-delimiter defect was found, so they are unchanged.

This containing commit requires fresh exact-head Linux CI. The exact new final SHA and its CI evidence are recorded post-commit in PR #10 rather than self-referencing this file. After green Linux CI, the required next owner action is to repeat exactly `cargo test -p domain --locked` on Windows 11 x64 / PowerShell 5.1 / Rust 1.98.1. Until that succeeds, status remains **PARTIAL / WINDOWS_RETEST_REQUIRED**, not READY_FOR_QA.


## Linux verification of the CRLF portability fix

The portability-fix implementation head `0af9c882fb099ccf23e7e42c864b82c2ac40fb77` was checked by [Rust CI run 37378699179](https://github.com/al-gri/pro-sclpng/actions/runs/37378699179), event `pull_request`, attempt 1, **completed/success**. Every job checked out exactly `0af9c882fb099ccf23e7e42c864b82c2ac40fb77` on Ubuntu 24.04.5 LTS / Linux x86_64 with Rust/Cargo 1.98.1:

- rust-fmt job `111994653056`: `cargo fmt --all -- --check` **PASS**;
- rust-clippy job `111994652646`: `cargo clippy --workspace --all-targets --locked -- -D warnings` **PASS**;
- rust-tests job `111994653029`: Cargo.lock regeneration/comparison, `cargo build --workspace --locked`, `cargo test --workspace --locked` and final clean-checkout checks **PASS**.

Workspace test counts on that exact SHA: **127 domain tests passed** = 73 contracts + 9 identity + 18 numeric + 27 wire, zero failed/ignored; separately **15 BOOT-001 CLI tests passed**, zero failed/ignored. The new wire count includes `artifact_wire.rs::af_markdown_wrapper_is_crlf_portable_without_changing_fixture_bytes`, which checks all five Markdown-backed AF fixtures under both LF and synthetic CRLF wrappers.

This Linux PASS does not overwrite the historical Windows FAIL at `dc7af8369e9c30c6877aae1b13cd23901c765cb3` and is not Windows evidence. The required next verification is a repeated owner-side `cargo test -p domain --locked` on Windows 11 x64 / PowerShell 5.1 / Rust 1.98.1 against the new final PR head. The containing handoff-only commit receives its own exact-head CI before handoff; its SHA is recorded in the PR post-commit rather than self-referenced here.


## Second Windows standalone result: regression-construction defect

Owner retest on exact SHA `387bdc624fbbae54785665374d0d8c790c7776a6`, Windows 11 x64 / PowerShell 5.1 / `1.98.1-x86_64-pc-windows-msvc`, again executed `cargo test -p domain --locked` and **FAILED**, but it confirmed the parser portability fix itself.

Exact observed counts:
- contracts: **73/73 PASS**;
- identity: **9/9 PASS**;
- numeric: **18/18 PASS**;
- wire: **26 PASS / 1 FAIL / 0 ignored**.

The only failure was `artifact_wire::af_markdown_wrapper_is_crlf_portable_without_changing_fixture_bytes`, at `crates/domain/tests/support/binary_fixtures.rs:185:50` with `AF heading`. All previously failing AF/WAL coverage, including `af_psad_bodies_and_psam_match_frozen_bytes_without_hashing`, W01/W02/W05/W08/W09/W11-W16 and targeted V2 GAP tests, passed on Windows. Therefore `markdown_artifact_bytes` CRLF→LF normalization is confirmed effective for native Windows fixture materialization.

The remaining failure was created by the regression itself. At `387bdc624fbbae54785665374d0d8c790c7776a6` the test used `frozen::AF_MD.replace('\n', "\r\n")`. When `AF_MD` was already CRLF on Windows, that transformed each `\r\n` into `\r\r\n`; the unchanged parser then normalized only `\r\n`→`\n`, intentionally leaving the extra `\r`, so the synthetic wrapper no longer represented valid CRLF text.

The containing correction changes only regression construction:
1. canonical LF wrapper = `frozen::AF_MD.replace("\r\n", "\n")`;
2. synthetic CRLF wrapper = canonical LF with `\n` replaced by `\r\n`;
3. the same `markdown_artifact_bytes` path parses native platform representation, canonical LF and synthetic CRLF;
4. for AF-N1/B1/C1/F1/V1, all three representations must yield identical descriptor/body bytes and match the frozen originals.

`markdown_artifact_bytes` semantics are unchanged. Normative fixtures, digests, WAL bytes, CRCs and contracts are unchanged. No dependency or production API change is introduced.

This containing test-support correction requires fresh exact-head Linux fmt/Clippy/build/workspace CI. After that green SHA, the required next owner action is another `cargo test -p domain --locked` on Windows. Both Windows FAILs — `dc7af836…` (parser portability) and `387bdc624fbbae54785665374d0d8c790c7776a6` (regression construction only) — remain part of the evidence and are not rewritten as PASS.
