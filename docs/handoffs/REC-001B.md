# REC-001B handoff

## Task

- Task ID: **REC-001B**
- Issue: #17 — Bounded WAL file writer/reader + crash/tail recovery
- Parent epic: #5 REC-001
- Exact base SHA: `a9b55f34c459794601c16d8ead9c417812b9eb11`
- Branch: `feat/REC-001B-wal`
- PR: #28
- Pre-handoff implementation head: `70941b681853d51026f80e4b80fb0ca4bbf4326b`
- Final head SHA: intentionally not embedded in this committed file because a commit cannot reliably contain its own SHA without self-reference. The exact post-handoff final head and its exact-head CI run are recorded after this file is committed in mutable PR #28 / Issue #17 metadata, following the repository precedent used by REC-001A.

## Accepted contract

Implementation targets the accepted SPEC-001 WAL contract:

- `specs/recording/wal-v1.md` — **ACCEPTED**, proposal revision **2**;
- frame version: **1**;
- record schema version: **1**;
- magic: `PSRW`;
- frame: fixed 32-byte header + bounded payload + 4-byte CRC;
- `MAX_PAYLOAD = 1_048_576`;
- CRC-32/ISO-HDLC exactly as specified;
- authoritative replay order is dense recorded `RecordNo` / append order, never exchange/source timestamp order;
- accepted domain/event/WAL semantics remain owned by SPEC-001 and ADR-0002; this PR does not change them.

## Delivered scope

A production-facing std-only filesystem WAL implementation in `crates/recording` provides:

1. exact WAL v1 encode/decode for the accepted record kinds and control/GAP payloads;
2. append-only `WalWriter` with `create_new` files and no reopen-for-append path;
3. bounded streaming `WalReader` for one or multiple ordered segment paths;
4. physical archive validation across RecordNo/SegmentNo, definitions/references, segment seals, archive seal, aggregate CRCs, and GAP/local-attempt accounting;
5. typed corruption/truncation/unsupported/validation/I/O outcomes;
6. explicit physical archive status:
   - `NoArchive`
   - `ValidPrefixIncomplete`
   - `SegmentSealedArchiveIncomplete`
   - `TruncatedTail`
   - `Corrupt`
   - `Unsupported`
   - `Invalid`
   - `Complete`;
7. separate `CanonicalStatus::NotEvaluated`; physical `Complete` is not claimed to mean reducer/artifact applicability;
8. storage watermarks for Accepted/Appended/Written/Flushed/Durable;
9. validation of the accepted durability-mode/recording-gate matrix;
10. deterministic repeated read without timestamp sorting or suffix magic scanning.

No accepted wire/spec/ADR semantics were modified.

## Changed files

Exact diff from base, including this handoff:

- `Cargo.toml`
- `Cargo.lock`
- `crates/recording/Cargo.toml`
- `crates/recording/src/binary.rs`
- `crates/recording/src/codec.rs`
- `crates/recording/src/file.rs`
- `crates/recording/src/lib.rs`
- `crates/recording/src/recovery.rs`
- `crates/recording/src/wire.rs`
- `crates/recording/tests/wal.rs`
- `docs/handoffs/REC-001B.md`

No `crates/domain/**`, `crates/market-data/**`, `apps/**`, `specs/**`, ADR, workflow, CI, Bitget fixture, or `docs/PROJECT_STATE.md` file is changed.

## Error / corruption / recovery model

External WAL bytes are treated as malformed/untrusted input.

- Header/version/kind/flags/reserved/length are validated before payload-sized allocation.
- Declared payload length above `MAX_PAYLOAD`, checked frame arithmetic failure, or checked absolute-offset overflow returns a typed length error before allocation.
- Nested GAP target counts are capped and remaining-byte checked before allocation.
- CRC mismatch is explicit `ChecksumMismatch`; it is never skipped or converted into clean EOF.
- EOF inside header, payload, or checksum is `TruncatedTail`.
- Unsupported frame/schema/record/control tags are typed `Unsupported`.
- Structural noncanonical values are typed corruption/invalid payload/validation failures.
- Reader stops at the first bad frame. It never scans later bytes for another `PSRW` magic and never rejoins a suffix.
- A valid prefix remains available diagnostically, but the archive is not `Complete`.
- `Complete` requires a valid final segment seal, valid archive seal, all count/length/CRC/link checks, and exact EOF.
- Extra bytes after ArchiveSeal are invalid trailing data.
- Deleting a final frame/seal/segment cannot promote an archive to `Complete`.
- Reader yields records in stored order only; no exchange/source timestamp sorting exists.
- Canonical artifact/reducer applicability is intentionally not evaluated in REC-001B.

## Durability behavior

The implementation exposes only the accepted storage-frontier meanings:

- **Accepted/Appended**: logical record accepted by the bounded owner/validator.
- **Written**: `BufWriter::write_all` accepted the complete encoded frame; bytes may still be userspace-buffered.
- **Flushed**: userspace buffer successfully flushed to the OS; this is **not** a power-loss guarantee.
- **Durable**: successful `flush` + `File::sync_all` + required creation-metadata protocol in this implementation.

Mode/gate validation:

- `Buffered` accepts Written, Flushed, or Durable recording gates.
- `GroupSynced` requires Durable.
- `SyncBeforePublish` requires Durable.

The writer does not fabricate a `StorageFence` WAL record. The accepted StorageFence remains an external typed storage-operation completion for downstream publication gating.

## Filesystem / OS assumptions

- WAL segment files are created with `OpenOptions::create_new(true)`; existing files are not reopened for append.
- Restart is expected to create a new archive per the accepted contract.
- `BufWriter` means a successful append/write frontier is not equivalent to OS flush or durable media.
- `flush()` only claims successful delivery from Rust userspace buffering to the OS interface.
- `sync_all()` uses Rust `File::sync_all`; actual persistence semantics depend on the OS, filesystem, mount options, storage stack, controller/cache behavior, and platform implementation.
- On Unix, the first durable sync for each newly created segment also opens and `sync_all`s its parent directory to cover advertised creation metadata.
- On non-Unix targets, parent-directory metadata sync is deliberately reported as `Unsupported`; the writer does not advertise a Durable frontier from that path.
- No filesystem-independent guarantee of survival across arbitrary power loss is claimed.
- No real power-cut/crash-harness test was run; crash/tail behavior is verified through deterministic truncated/corrupt byte cases.
- Archive inventory and ordered segment-path discovery are external responsibilities; the reader validates the paths it is given.

## Dependencies

No external dependency was added.

`recording` depends only on the existing workspace crate:

- `domain = { path = "../domain" }`

No async runtime, WebSocket, HTTP client, database, Redis, NATS, Python, or Parquet dependency is present. `Cargo.lock` drift is limited to the new workspace package and its `domain` dependency.

## Tests

The REC-001B test suite covers, among other cases:

- W00/W01 accepted exact vectors and CRC;
- one-record exact round-trip;
- multiple records preserve recorded order;
- deterministic repeated read;
- maximum bounded payload;
- control and GAP records;
- checksum corruption;
- every cut of W01 and every byte cut of a complete multi-record archive;
- partial header/body/final checksum;
- truncated final record with valid prefix;
- oversized declared length and absolute-offset overflow;
- unsupported frame/schema/record/control/GAP tags;
- malformed structural fields/options;
- valid prefix + corrupt final record;
- valid prefix + truncated final record;
- no timestamp sorting;
- bounded arbitrary external bytes do not panic;
- no suffix rejoin after middle corruption;
- missing/deleted seals do not become complete;
- wrong seal/aggregate CRC and trailing data;
- multi-segment chain validation;
- local GAP accounting and one-use loss windows;
- durability mode / recording-gate matrix;
- truthful storage watermarks;
- existing WAL file cannot be reopened for append.

## Verification evidence

### Local worker shell

This continuation session has no local Rust shell execution evidence. Therefore:

- `cargo fmt --all -- --check` — **NOT_RUN locally**
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**
- `cargo test --workspace --locked` — **NOT_RUN locally**

### Exact-head CI before this handoff commit

GitHub Actions run **37453053651** ran on exact pre-handoff implementation head
`70941b681853d51026f80e4b80fb0ca4bbf4326b` and completed **success**:

- rust-fmt / `cargo fmt --all -- --check` — **PASS**
- rust-clippy / `cargo clippy --workspace --all-targets --locked -- -D warnings` — **PASS**
- rust-tests / lockfile verification + workspace build + `cargo test --workspace --locked` — **PASS**

These PASS results apply only to `70941b...` and are **not** transferred to the post-handoff head.

### Final post-handoff exact-head CI

At the instant this committed handoff file is authored, the containing commit SHA does not yet exist. Therefore final-head CI is **NOT_RUN at authorship time**. After this handoff commit, PR #28 must receive a new exact-head CI run; its final SHA, run ID, and PASS/FAIL result are recorded in the mutable PR #28 / Issue #17 metadata. PR #28 must not be marked ready for independent QA unless that exact final-head CI succeeds.

## Self-review / scope audit

Self-review was performed against exact base
`a9b55f34c459794601c16d8ead9c417812b9eb11`.

Findings:

- changed paths are within the Issue #17 allowed scope;
- root Cargo changes only register the recording crate / workspace dependency;
- no network, Bitget, HTTP/REST, WebSocket, local-book, DataHealth runtime, strategy, execution, private API, Parquet, telemetry, or uploader code exists in the diff;
- no accepted spec/ADR/domain/market-data semantics are changed;
- no external dependency drift exists;
- no `unsafe` code was introduced;
- production code does not use panic/unwrap/expect recovery for malformed WAL input;
- input-derived record lengths are bounded and checked before payload allocation;
- the reader never sorts by source/exchange timestamp and never silently skips corruption.

No blocker defect was identified during this worker self-review. This is not independent QA.

## Known limitations

- Artifact resolution and canonical market reducer applicability are intentionally `NotEvaluated`.
- This crate does not implement recorder/replay applications or network capture.
- Caller code must construct accepted `RecordFrame` values and append required segment/archive seals; the writer does not invent missing semantic records.
- Filesystem durability beyond the successful OS calls described above is not proven.
- Non-Unix durable creation-metadata sync is unsupported by this implementation.
- No physical power-loss laboratory test, filesystem matrix, benchmark, latency percentile, or throughput claim is included.
- A completely absent archive still requires external inventory knowledge; a reader can only classify files/paths supplied to it.
- CRC protects accidental corruption detection; it is not authentication or malicious tamper resistance.

## Explicitly OUT_OF_SCOPE

- Bitget WebSocket or REST;
- market-data supervisor;
- canonical local order book mutation;
- DataHealth runtime reducer/FSM;
- recorder CLI;
- replay application/reducer wiring;
- Parquet/export;
- strategy, levels, OFI/MLOFI, signals, TradePlan;
- execution/private API/API keys;
- live capture;
- background upload/telemetry;
- changes to accepted specs, ADR semantics, domain contracts, or market-data semantics.

## Downstream implications

### REC-001D

Public WS/raw-capture integration should:

- preserve receive/admission order when constructing and appending WAL records;
- never substitute exchange timestamp ordering for `RecordNo`;
- explicitly record accepted GAP/control events instead of hiding queue/source loss;
- honor the configured storage gate using the writer's real Written/Flushed/Durable frontiers;
- treat StorageFence as external verified storage-operation completion, not a parsed WAL field;
- create a new archive after restart rather than reopening an old WAL for append.

### REC-001F

Recorder/replay integration should:

- consume `WalReader` in stored order;
- distinguish `Complete` from all incomplete/corrupt/unsupported/invalid statuses;
- never replay a truncated/corrupt archive as complete;
- use valid prefixes only with an explicit incomplete diagnostic policy;
- perform downstream artifact resolution and canonical reducer applicability separately from physical WAL validation;
- call the same reducers used by live processing;
- not infer historical sync/send success from replayed WAL bytes or CRCs.

## Independent QA finding and R5 correction

Independent QA reviewed exact head
`3b3222bd8171421e9f7df075daeb12446e2cb58c` and returned
**CHANGES_REQUIRED** for one MEDIUM R5 recovery defect:

- an unknown-range local QueueOverflow window could remain open through final seals;
- `ArchiveSeal(input_quality=Unknown)` could validly preserve a physical `Complete` chain;
- however `PhysicalReport` had no non-fatal typed way to surface the accepted
  `UnresolvedLossWindow` diagnostic.

The correction is intentionally runtime-only and does not change WAL bytes, tags,
accepted specs, domain contracts, or physical completion semantics:

- `PhysicalReport` now carries ordered `RecoveryDiagnostic` values;
- a loss diagnostic identifies the affected `StreamId` and typed `LossError`;
- terminal EOF/finalization derives unresolved-window diagnostics from validator
  state in deterministic `BTreeMap<StreamId, ...>` order;
- an unresolved local window reports `LossError::UnresolvedLossWindow`;
- terminal input quality is `Unknown` while such a window remains open;
- valid `ArchiveSeal(input_quality=Unknown)` + exact EOF remains physical
  `ArchiveStatus::Complete` with `failure == None`;
- stronger `NoKnownLoss` and `GapsRecorded` seal claims remain rejected;
- ordinary EOF before ArchiveSeal remains physically incomplete while exposing the
  same non-fatal diagnostic.

New production regression coverage in `crates/recording/tests/wal.rs` includes:

- accepted vector `V-R5-UNRESOLVED`: Complete + Unknown + non-fatal
  `UnresolvedLossWindow`;
- rejection of NoKnownLoss and GapsRecorded on an unresolved window;
- ordinary EOF before ArchiveSeal with Unknown + diagnostic;
- two unresolved streams with deterministic ascending StreamId diagnostic order,
  no duplicates, and repeated-read equality.

The previous exact-head CI run `37455560628` applies only to the QA-rejected
head `3b3222bd...` and is historical after this correction. Local Rust commands
remain **NOT_RUN** in this connector-only continuation session. The post-fix exact
head and its new exact-head CI run are recorded in mutable PR #28 / Issue #17
metadata after this commit; no PASS is transferred from the rejected head.

## Next gate

After the handoff commit:

1. verify GitHub Actions on the exact new PR #28 head;
2. record the exact final head and exact-head CI run in PR #28;
3. update stale PR metadata/handoff status;
4. only if all required checks pass and no blocker defect appears, convert PR #28 from Draft to Ready for review;
5. independent QA is performed by a separate chat. This worker does not self-approve or merge.
