# REC-001A handoff

## Task

- Task ID: **REC-001A**
- Issue: #16
- Parent epic: #5
- Exact base SHA: `1617a3622e83c36c3104e1bc437f7d16a1f279a4`
- Branch: `feat/REC-001A-bitget-json-decoder`
- PR: #25
- Pre-handoff implementation head: `438e4d82e96e3f438bf6e5beb2d98bafce11f2a1`
- Final head: intentionally not embedded in this committed file because that would require a self-referential commit SHA. The exact final head and its exact-head CI run are recorded after the last commit in mutable PR #25 / Issue #16 metadata.

## Delivered scope

A minimal std-only `market-data` crate implements:

1. bounded offline JSON parsing of external bytes;
2. typed regular Bitget `books50` snapshot/update decoding;
3. typed `publicTrade` decoding;
4. source lexical preservation for book price/quantity and trade price/size;
5. checked `seq`/`pseq` and millisecond timestamp parsing;
6. deterministic regular-books50 continuity classification only.

It does **not** implement a local order book, canonical market events, networking, REST, WAL, runtime supervision, strategy, alerts, or execution.

## Changed files

- `Cargo.toml`
- `Cargo.lock`
- `crates/market-data/Cargo.toml`
- `crates/market-data/src/lib.rs`
- `crates/market-data/src/json.rs`
- `crates/market-data/src/decoder.rs`
- `crates/market-data/src/continuity.rs`
- `crates/market-data/tests/accepted_fixtures.rs`
- `crates/market-data/tests/input_safety.rs`
- `crates/market-data/tests/architecture.rs`
- `docs/handoffs/REC-001A.md`

No accepted Bitget fixture, `crates/domain/**`, accepted spec, ADR, workflow, app, or `docs/PROJECT_STATE.md` was changed.

## Dependencies

No external Rust dependency was added. `market-data` is std-only. There is no async runtime, WebSocket library, HTTP/REST client, or filesystem dependency.

## Public API / types

Exported from crate `market_data`:

- decoding functions:
  - `decode_message(bytes: &[u8]) -> Result<BitgetMessage, DecodeError>`
  - `decode_message_with_limits(bytes: &[u8], limits: DecodeLimits) -> Result<BitgetMessage, DecodeError>`
- input/safety:
  - `DecodeLimits`
  - `DecodeError`
  - `JsonError`
  - `JsonErrorKind`
  - `BOOKS50_MAX_LEVELS`
- wire identity/enums:
  - `Category`
  - `Topic`
  - `Action`
  - `FillSide`
  - `RpiFlag`
- wire values/messages:
  - `LexicalValue`
  - `WireTimestampMs`
  - `WireLevel`
  - `Books50Frame`
  - `WireTrade`
  - `PublicTradeFrame`
  - `BitgetMessage`
- continuity:
  - `ContinuityClassifier`
  - `ContinuityOutcome`
  - `ContinuityRule`

The decoder intentionally does not export or construct canonical `BookUpdate`, `DeleteLevel`, or canonical `Aggressor` effects.

## Continuity behavior

- initial regular `books50` snapshot -> `AnchorCandidate`;
- snapshot -> first update -> documented interval rule `current.pseq <= snapshot.seq <= current.seq`;
- update -> update -> `previous.seq == current.pseq`;
- mismatch -> explicit `Gap` or conservative `ResetOrDiscontinuity`;
- reset classification does not make `pseq=0` an exhaustive reset contract;
- exact duplicate fixture -> project-only `DuplicateDiagnostic`;
- gap/reset invalidates classifier state and a later update returns `NeedsSnapshot`;
- classifier never fetches or stitches a REST snapshot;
- a valid visible-push pseq relation can span a larger seq change; the classifier does not assume every intermediate book event/state is observable.

## Accepted fixtures

All 9 accepted Bitget fixtures are consumed read-only in tests:

- `books50-snapshot.json`
- `books50-update.json`
- `books50-gap.json`
- `books50-duplicate.json`
- `books50-reset.json`
- `books50-empty-levels.json`
- `public-trades.json`
- `books50-zero-quantity-unknown.json`
- `rpi-books50-snapshot.json`

They remain synthetic fixtures from MD-001; no claim is made that they were captured live.

## Tests / evidence

At pre-handoff implementation head `438e4d82e96e3f438bf6e5beb2d98bafce11f2a1`, GitHub Actions Rust CI run `37444770088` passed all three exact-head jobs:

- `rust-fmt` -> PASS (`cargo fmt --all -- --check`)
- `rust-clippy` -> PASS (`cargo clippy --workspace --all-targets --locked -- -D warnings`)
- `rust-tests` -> PASS, including Cargo-generated lockfile equality, workspace build, and `cargo test --workspace --locked`

The final post-handoff commit is required to receive its own exact-head CI. That final run is recorded in mutable PR #25 / Issue #16 metadata after this file is committed.

Test coverage includes:

- all accepted fixture cases;
- malformed JSON;
- oversized message;
- too many books50 levels;
- bounded trade count;
- excessive nesting/container size;
- wrong topic;
- invalid action;
- invalid and overflowing `seq`/`pseq`;
- invalid and overflowing timestamps;
- invalid field type;
- malformed external bytes do not panic;
- source zero quantity remains lexical `"0"`;
- publicTrade fill side remains wire `FillSide`;
- RPI two-quantity profile is rejected rather than collapsed;
- gap does not auto-recover;
- no network/REST/WAL/file-I/O dependency or source path;
- no canonical BookUpdate/DeleteLevel/Aggressor construction;
- no assumption that every intermediate source book event is observable.

Local shell execution in this connector-only worker session: **NOT_RUN**. The required commands are nevertheless executed by the repository exact-head CI; only exact-head CI success is counted as PASS.

## Preserved UNKNOWN / BLOCKED / FORBIDDEN boundaries

- **U-09 UNKNOWN / BLOCKED** — regular `books50` quantity asset/unit remains unknown. No canonical `quantity_unit` is invented.
- **U-10 UNKNOWN / BLOCKED** — source `qty=0` remains source lexical data and is not converted to `DeleteLevel`.
- **U-20 NOT_PROVEN / FORBIDDEN** — no REST↔WS bridge or healing exists.
- **C-01 CONTRACT_CONFLICT / BLOCKED** — `rpi-books50` two-quantity levels are rejected at this boundary; there is no one-quantity RPI normalization.
- **C-03 DOC_CONFLICT / UNKNOWN** — no instrument endpoint selection, alias, or fallback is implemented.
- **U-18 UNKNOWN** — `publicTrade.S` is preserved only as `FillSide`; no canonical aggressor mapping is made.
- **U-19 UNKNOWN** — no trade↔book sequence/timestamp/ID linkage is inferred.

## Deviations

No scope/contract deviation. The implementation uses a small in-crate std-only JSON parser rather than adding a dependency because repository CI is offline and REC-001A requires only bounded accepted-profile parsing.

The first CI attempt exposed an escaped byte-literal transport error and rustfmt differences. Those heads are historical failures only; they were corrected before handoff and are not counted as PASS.

## Limitations

- This is an offline decoder/classifier, not a production WebSocket client.
- Engineering parser bounds are safety policy, not claimed Bitget transport maxima.
- No latency, allocation, throughput, or p99 claim is made; no benchmark was run.
- No canonical price/quantity grid normalization is performed.
- No local book reducer is implemented.
- No RPI normalization is implemented.
- No REST, WAL, replay, strategy, alert, or execution behavior is present.

## Next recommended task

Immediate next action: independent QA review of the exact final PR #25 head.

After REC-001A is independently accepted in main, the decomposition identifies **REC-001B / Issue #17** as the next implementation task; REC-001E remains blocked by U-09/U-10 evidence work.
