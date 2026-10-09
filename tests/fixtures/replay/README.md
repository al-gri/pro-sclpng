# REC-001F-1 synthetic diagnostic replay fixture

`synthetic-v1.wal` is a deterministic, single-segment synthetic archive for
`radar-replay --wal tests/fixtures/replay/synthetic-v1.wal --profile synthetic-rec001f1-v1`.
`synthetic-v1.expected.txt` fixes the diagnostic serialization. `SHA256SUMS`
covers the saved deliverables; `manifest.json` records construction provenance.

| Deliverable | Bytes | SHA-256 |
| --- | ---: | --- |
| `synthetic-v1.wal` | 8543 | `e967d2cd86a539a7f5b5f03a8767606c4dee3519d36cb73f31732fdca42c511d` |
| `synthetic-v1.expected.txt` | 52054 | `3ed7b44898dcfcf6523f100614342215b2e65c4fcce9fa4302ff8cc81a111bc6` |

The `synthetic-rec001f1-v1` diagnostic serialization pins the current Rust Debug
representations of accepted DTOs, lexical inputs, controls, structural records,
recorded contexts and reducer results, with fixed field/line order. The golden
contains no runtime path, process identifier, wall-clock sample or container
capacity. A serialization change requires a reviewed format/golden update.

Rebuild to a new destination using the accepted low-level synthetic WAL writer:

```sh
replay_tmp="$(mktemp -d)"
cargo run --locked -p radar --example build_replay_fixture -- \
  --output "$replay_tmp/synthetic-v1.wal"
cmp tests/fixtures/replay/synthetic-v1.wal "$replay_tmp/synthetic-v1.wal"
sha256sum -c tests/fixtures/replay/SHA256SUMS

cargo run --locked -p radar --bin radar-replay -- \
  --wal tests/fixtures/replay/synthetic-v1.wal --profile synthetic-rec001f1-v1 \
  > "$replay_tmp/replay-1.txt"
cargo run --locked -p radar --bin radar-replay -- \
  --wal tests/fixtures/replay/synthetic-v1.wal --profile synthetic-rec001f1-v1 \
  > "$replay_tmp/replay-2.txt"
cmp "$replay_tmp/replay-1.txt" "$replay_tmp/replay-2.txt"
cmp tests/fixtures/replay/synthetic-v1.expected.txt "$replay_tmp/replay-1.txt"
sha256sum "$replay_tmp/replay-1.txt" "$replay_tmp/replay-2.txt"
```

The builder uses `WalWriter::create` (`create_new`, no overwrite), `append`,
`prefix_summary`, explicit final `SegmentSeal` and `ArchiveSeal`, then `finish`.
It samples no clock and reads no environment fallback. Linux supports the
writer's final file and parent-directory synchronization. On Windows the
existing `finish` API reports unsupported parent-directory synchronization;
the builder returns an error and makes no successful finalization claim.
Its resulting partial output must not be mistaken for a successful builder run.

## Provenance and limits

All receive stamps, identities, configuration, epoch changes, control records,
attempt numbers and archive metadata are synthetic. Archive/session IDs are
respectively sixteen `f1`/`c1` bytes, clock ID is `1`, and recorded Unix samples
are `1770000000000000000 + monotonic_sample_ns`. These are fixed samples, not
live time or empirical timing thresholds.

Raw payloads are included byte-for-byte from the accepted synthetic fixtures in
`tests/fixtures/bitget`, whose manifest remains authoritative for their shape
and source provenance. Reusing them does not promote their synthetic values or
edge cases to exchange guarantees. No live market archive is included.

The profile fixes all external `HealthPolicy` fields: unknown on silence,
freshness deadline 50 ns, warmup minimum one update and zero elapsed ns,
two-sided snapshot required, no quiet proof policy, recording gate `Written`,
pending frame/output limits `3`, pending raw-byte limit `16384`, pending wait
`100` ns, and no quiet lifetime. Decoder limits are 4096 message bytes, 16
trades, 256 container items, nesting depth 16, and 128 string bytes. Application
admission limits are 8 MiB, 256 records and two declared streams; this profile
supports exactly its pinned stream. Saved fixture size is at most 64 KiB.
All these caps and thresholds are versioned engineering choices.

Each supported homogeneous decoded book frame contributes exactly one
snapshot candidate or one update candidate. Number of levels is not number of
outputs. Quantity/price unit tokens and unit increments in the synthetic
descriptor are placeholders. `quantity_unit=SYNTHETIC_UNKNOWN` and the absent
base multiplier preserve the unresolved quantity-unit boundary. The replay
does not use these placeholders to normalize quantities or mutate levels.
All descriptor references are the unresolved all-zero SHA-256 reference;
no artifact body or applicability has been verified.

The profile also recognizes one optional exact inactive InstrumentSpec version
`2`, with bootstrap sample `15`, the same instrument/numeric fields and the
same unresolved provenance. Negative scenarios may define it before the stream
and initial configuration. This definition does not activate version `2` and
does not supply evidence: a subsequent SpecActivate remains unsupported and
blocks dependent projection. The saved 29-record fixture omits this descriptor
and contains no SpecActivate.

## Fixed record order

Offsets are zero-based frame starts in the saved archive, obtained by the
accepted `WalReader` and `encode_frame`; the end offset is `8543`. They locate
mutation tests and are specific to these exact saved bytes.

| Record | Start byte | Sample ns | Input and diagnostic role |
| --- | ---: | ---: | --- |
| 1 | 0 | 0 | ArchiveStart, Buffered; structural Noop |
| 2 | 74 | 10 | Synthetic InstrumentSpec; structural Noop |
| 3 | 342 | 20 | Pinned stream definition; RegisterStream |
| 4 | 525 | 30 | Pinned initial ConfigDefinition; structural Noop |
| 5 | 693 | 40 | Transport Up |
| 6 | 767 | 100 | Snapshot, attempt 1; pending anchor candidate |
| 7 | 1390 | 110 | Update, attempt 2; pending continuity candidate |
| 8 | 1896 | 120 | Byte-identical duplicate, attempt 3; no extra pending candidate |
| 9 | 2402 | 130 | Empty-level update, attempt 4; third pending candidate |
| 10 | 2778 | 140 | Zero-quantity lexeme, attempt 5; pending frame overflow |
| 11 | 3214 | 200 | Book epoch 1 to 2 |
| 12 | 3297 | 300 | Snapshot in book epoch 2, attempt 6 |
| 13 | 3920 | 400 | Timer ID 1, deadline/sample 400; exact pending-deadline timeout |
| 14 | 4001 | 410 | Transport Down |
| 15 | 4075 | 420 | Transport Up; no readiness recovery |
| 16 | 4149 | 430 | Book epoch 2 to 3 |
| 17 | 4232 | 440 | Old book epoch 2 raw snapshot, attempt 7; obsolete scope |
| 18 | 4855 | 450 | Current book epoch 3 snapshot, attempt 8 |
| 19 | 5478 | 460 | Update, attempt 9 |
| 20 | 5984 | 470 | Reset-signature update, attempt 10 |
| 21 | 6485 | 480 | Book epoch 3 to 4 |
| 22 | 6568 | 490 | Current book epoch 4 snapshot, attempt 11 |
| 23 | 7191 | 500 | Update, attempt 12 |
| 24 | 7697 | 510 | Sequence-gap update, attempt 13 |
| 25 | 8138 | 520 | Explicit critical QueueOverflow GAP, lost attempts 14–15, count 2 |
| 26 | 8262 | 530 | Observed Recording Failed, Written through record 25 |
| 27 | 8335 | 540 | Observed Recording Healthy, Written through record 26; Failed remains latched |
| 28 | 8408 | 540 | Final SegmentSeal; Noop repeats last recorded sample |
| 29 | 8474 | 540 | ArchiveSeal with GapsRecorded; Noop repeats last recorded sample |

Raw attempts are ordered 1–13; the explicit gap accounts for 14–15 without an
open loss window. Record numbers are dense. Physical Complete and GapsRecorded
describe storage and loss accounting, never canonical market-data readiness.

No frame proof, warmup proof or production artifact verifier is available, so
frames remain pending or invalid and usable data remains false. The recording
observations are preserved diagnostic inputs, not trusted StorageFence values,
publication permits, historical sync receipts, or external delivery receipts.
Recorded Timer fields reproduce only the existing health timer observation;
they do not reconstruct Timer A scheduler authorization or physical Ping/Close.

U-09, U-10 and regular snapshot-zero semantics remain UNKNOWN/BLOCKED. Zero is
preserved as a lexeme and is not interpreted as deletion. U-20 forbids REST/WS
healing; C-01 blocks RPI normalization; C-03 remains UNKNOWN. Canonical books,
signals, plans, execution, and live capture are outside this fixture.

The test-local helper exposes mutable `Vec<RecordFrame>` scenarios and reseals
them using the accepted codec for negative tests. It is not a replacement WAL
reader/recovery scanner or a live capture ownership path.
