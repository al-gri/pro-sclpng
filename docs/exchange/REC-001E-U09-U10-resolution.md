# REC-001E-SEM-001 — U-09/U-10 evidence and proposed handling

Verdict: **BLOCKED_INSUFFICIENT_EVIDENCE**. Policy appendix: **PROPOSED ENGINEERING_POLICY / NOT_ACCEPTED**.
Issue: [#42](https://github.com/al-gri/pro-sclpng/issues/42); dependent implementation [#21](https://github.com/al-gri/pro-sclpng/issues/21) remains **BLOCKED**.
Verified base: `8baf610b4ac15122dfcdbfe07730bc30c6558d73`. Fresh source verification: **2026-10-10**.
Target: production UTA v3 public **regular JSON**, `books50`, `usdt-futures`, `BTCUSDT` only.

## 1. Result and historical boundary

Freshly checked depth-specific sources still do not establish U-09 quantity asset/unit, its relationship to the quantity increment/base conversion, U-10 deletion encoding, or whether regular snapshot quantity zero is permitted. These questions remain **UNKNOWN**. The historical [MD-002 report](bitget-books50-semantics.md) and [handoff](../handoffs/MD-002.md) are preserved, including their 2026-10-06 conclusions and [owner receipt](https://github.com/al-gri/pro-sclpng/issues/19#issuecomment-6024780127).

There is new, independently checkable **rollout/applicability evidence**: the current Update Preview gives a November 5, 2026 live date for incremental `books50` while the depth reference already describes the incremental model. The legacy preview gives a different planned date. This does not resolve quantity/deletion semantics. Section 4 records the conflict without rewriting accepted MD-001 history or selecting live behavior by assumption.

The proposed fail-closed policy in section 6 does not authorize canonical effects. There is insufficient evidence to recommend a base-coin unit or zero-to-delete rule safely. A future accepted engineering policy could authorize explicitly bounded project behavior, but must describe residual uncertainty and cannot turn an exchange UNKNOWN into a Bitget guarantee. This report's publication or merge cannot unblock #21.

## 2. Fresh sources and provenance

All sources below were opened and inspected through the web documentation reader on **2026-10-10**. The regular JSON reference has no exposed immutable global schema version. The dates below identify retrieval, not guaranteed deployment. Section/field anchors are authoritative locators; reader line ranges identify this particular extraction and may change.

| ID | Official URL and inspected locator | Applicability | Bounded finding and inference boundary |
|---|---|---|---|
| D-EN | [Depth Channel](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel), Description; Request/Push Parameters; extracted lines 20–44, 65–76, 147–165 | UTA v3 regular JSON; listed regular categories | `books50` is described as first snapshot then updates. Level component 1 is labeled quantity; the inspected page does not assign an asset/unit or define deletion/zero handling. Positive examples do not prove zero impossible. |
| D-ZH | [Chinese Depth reference](https://www.bitget.com/zh-CN/docs/uta/websocket/public/Order-Book-Channel), Description and Push Parameters; lines 15–39, 147–155 | Same current regular JSON reference in Chinese | The current translation likewise labels quantity without a unit/delete rule; it describes incremental `books50`. Translation agreement cannot supply a missing rule. A search-cache result for the old `/api-doc/` route was stale; opening that route redirected to Tickers, so it is not depth evidence. |
| BEST | [Best Practices](https://www.bitget.com/docs/uta/best-practices-guide), Market Data; lines 26–45 | UTA regular depth guidance | Describes incremental `books50` and final-state/coalescing behavior. Coalescing does not define the removal encoding. |
| MARKET | [Market Data](https://www.bitget.com/docs/catalog/market/market-data), Get Instruments quantityPrecision/quantityMultiplier; Get Order Book response; lines 207–232, 775–871 | REST metadata and REST depth, adjacent interfaces | Metadata documents futures order quantity constraints; REST depth labels quantity. Neither inspected section binds those units/increments to WS depth. No REST↔WS sequence bridge is established here. |
| ORDER | [Order Management](https://www.bitget.com/docs/catalog/trading/order-management), Place Order request qty; lines 114–125 | Order requests, not market depth | Assigns category-dependent request units, including base coin for USDT/USDC futures. Applying that to depth would be an unsupported cross-interface inference. No account/private request was performed. |
| RPI | [RPI Depth](https://www.bitget.com/docs/uta/websocket/public/RPI-OrderBook-Channel), Description and Push Parameters; lines 20–38, 162–173 | RPI JSON only | Separate quantity components and sequence rules. An RPI component zero cannot establish a regular JSON deletion rule. |
| SBE | [SBE Introduction](https://www.bitget.com/docs/uta/websocket/sbe/sbe-intro), Core Features and XML header; lines 21–27, 52–59 | Versioned binary profile; displayed schema 5 / semanticVersion 1.0.0 | Independent profile/version boundary; no regular JSON unit/delete guarantee follows from the binary schema or its published future version. |
| PREVIEW | [Current Update Preview](https://www.bitget.com/docs/uta/update-preview), books50 Depth Channel: Incremental Push; lines 18–31 | Planned regular JSON/SBE change; demo distinguished from live | Gives November 5, 2026 live date and says demo already uses the change. It describes a move from every-push snapshots to first snapshot plus updates. It supplies no unit/delete statement and no proof of actual production deployment. |
| LEGACY | [Legacy UTA changelog](https://www.bitget.com/legacy-docs/uta/changelog), September 29 Preview; lines 55–68 | Older official preview, explicitly legacy | Gives end-October 2026 planned effect with demo already effective. Keep it as conflicting planning history; do not silently prefer it to the current preview or treat either as observed live behavior. |

### Reproducible bounded fingerprints

The following SHA-256 values hash only the listed bounded excerpts, **not full HTML pages**. Each input is a JSON string: decode it, then hash its UTF-8 bytes, including the displayed LF (`\n`) endings, without trimming or normalization. Two nonadjacent field labels are joined by LF where indicated. These fingerprints support excerpt auditing; they do not authenticate a source, pin a whole documentation revision, or prove absence elsewhere.

| ID | Exact fingerprint input (JSON string) | SHA-256 |
|---|---|---|
| D-EN | `"Sell quantity\nBuy quantity\n"` | `fb8bb1a4f64b956c38d097001d6292559494cd8ccf2a6c75d5365961bf1ca500` |
| D-ZH | `"卖一量\n买一量\n"` | `4c4a35cf6decf3a08cff0947ba143f06b2b153985b872ce4ba3b1566d0d0a47e` |
| BEST | `"The system pushes the latest state of the order book.\n"` | `81f840bf007f188b2c7f616815022d164bcfe3a8e1b7ce8b769b90dc9adde815` |
| MARKET | `"Each entry: [price, quantity]\nQuantity multiplier, used for futures orders together with quantityPrecision\n"` | `3720b9f53083e16c3f2438fb3f272414423beb8b6c7b70e9cd12c8f8e404a778` |
| ORDER | `"USDT/USDC-Futures\nThe unit is base coin\nCOIN-Futures\nThe unit is quote coin\n"` | `75b8e208e3a935c65ef11107319789bf354a70aabae894bb24e30fa2553363a7` |
| RPI | `"Ask non-RPI quantity\nAsk RPI quantity\n"` | `75350144f9fad4dd2e6ec8590faaf39721b28877bcad1134acd8474339a3652a` |
| SBE | `"version=\"5\"\nsemanticVersion=\"1.0.0\"\n"` | `234f10f183e4340fd58acce3c9fc1c27703235a2bdf2dfdee963c2f1271479ed` |
| PREVIEW | `"Live date: November 5, 2026\nCurrently effective in demo trading.\n"` | `b030e00622b559f30f7b84c523ee15909b3a41b2f9295d1377d88ae9b3fac1f1` |
| LEGACY | `"Effective date: by the end of October 2026\nCurrently effective in demo trading.\n"` | `d5bd51fb161f1a45b5887b37d5867d1177fe844be3807555337383ba9ecbe59c` |

A direct Python HTTPS fetch of D-EN returned **HTTP 403** in this Linux executor. Consequently a fresh full-response-byte hash is **NOT_RUN / UNAVAILABLE**, not replaced with an invented digest. The official page text remained accessible through the web reader. Independent source QA must reopen the URLs, locate the text, and verify applicability; rehashing this report alone is insufficient source reproduction.

## 3. Exchange-question matrix

Each row has exactly one exchange-evidence status. Policy acceptance is separate.

| ID | Exchange question | Status | Scope and remaining evidence requirement |
|---|---|---|---|
| U-09a | Which asset/unit is regular `usdt-futures` / `BTCUSDT` books50 quantity? | **UNKNOWN** | D-EN/D-ZH do not state it. Metadata, order qty, trades, SDK conventions and numeric observations cannot establish depth units. |
| U-09b | Does depth quantity use the metadata quantity increment/grid and a constant base multiplier? | **UNKNOWN** | No inspected depth-specific binding. Quantity precision alone is not an increment; checked arithmetic cannot verify a dimension. |
| U-10a | Which regular incremental operation removes a price level? | **UNKNOWN** | No deletion encoding in inspected current depth/guidance sources. |
| U-10b | Is zero quantity a delete marker; for which regular channel/category/version? | **UNKNOWN** | Premise unresolved; no category/version may be assigned this rule. |
| U-11 | Can source zero occur in a regular snapshot? | **UNKNOWN** | Positive examples do not prove prohibition, and RPI examples do not prove permission. Canonical zero rejection is a project contract. |
| CAT-UNIT | Are units/deletion the same across other regular categories? | **UNKNOWN** | `spot`, `coin-futures`, `usdc-futures` remain unproved; no rule from this target expands to them. |
| PROFILE | Are regular JSON, RPI JSON and SBE distinct profiles? | **VERIFIED** | D-EN/RPI/SBE establish independent wire shapes/profile boundaries. This does not settle any regular quantity rule. |
| LIVE-ROLLOUT | Is incremental books50 already guaranteed on the production endpoint at this date? | **CONFLICT** | Current depth text describes it; current preview schedules it for November 5; legacy preview has another date. Live observation is NOT_RUN and neither preview proves current rollout. |

There are no **OBSERVED_ONLY** claims: this worker did not perform live capture or receive a privately verifiable archive. Future captured quantities cannot prove units; zero followed by price absence cannot prove general deletion because bounded depth, truncation, coalescing, intervening updates and missing inputs are alternatives. Lack of zero cannot prove impossibility.

## 4. Rollout applicability and diagnostic capture

PREVIEW is newly checked evidence absent from the MD-002 source set, rather than a new quantity/deletion guarantee. Its planned live date must be distinguished from the current-reference wire model and actual observed endpoint behavior. The legacy date is historical/conflicting, not authority to mark a rollout complete. Accepted MD-001 continuity conclusions remain historical facts about its checked sources; this report does not edit them.

For a future profile decision, separately record endpoint, public production versus demo, category, symbol, channel, actual `action` and sequence fields, capture time/order, source bytes/hash and applicable source revision. A sampled snapshot/update remains **OBSERVED_ONLY**, with no silent promotion to a guarantee. Choosing demo instead of the scoped production endpoint, switching to `books`, or assuming updates are required for a bounded diagnostic capture changes the packet and requires Integrator review.

Diagnostic capture can preserve actual source text and honest decoder results while canonical applicability remains blocked. If actual payload/action/sequence shape is unsupported by the accepted decoder or immutable selected diagnostic profile, stop the dependent path with the concrete mismatch; do not reclassify a snapshot as a delta, add defaults, switch endpoint/profile, or loosen its parser silently. This source task does not authorize transport implementation or another capture worker.

## 5. Compatibility with accepted contracts

The [numeric contract](../../specs/domain/types-v1.md) requires explicit quantity_unit/increment and checked exact conversion; None multiplier means UNKNOWN. A lexically positive source quantity cannot become a dimensioned QuantitySteps without applicable metadata. Labeling an opaque wire unit as BTC, setting multiplier=1, or inferring the increment from decimal precision would invent missing semantics.

The [event contract](../../specs/market-data/events-v1.md) retains `SetLevel(qty>0)` and quantity-free `DeleteLevel`. Snapshot levels must be strictly positive and unique/sorted. Source zero is a separate unresolved mapping; dropping it, emitting `SetLevel(0)` or choosing deletion would violate the accepted boundary. A frame is applied atomically only after full current-scope verification; structural parsing alone does not grant effects.

[ADR-0002](../adr/0002-domain-event-wal-contracts.md), [artifacts](../../specs/domain/artifacts-v1.md), [DataHealth](../../specs/market-data/data-health-v1.md) and [WAL](../../specs/recording/wal-v1.md) retain exact identity, recorded activation, applicable artifact closure, proof commitment, source continuity, barrier/epoch ownership, fresh resync and warm-up gates. Up/ACK/pong, a parsed hash, a complete WAL, two equal diagnostic digests or accepted source prose cannot force Usable. Wrong scope, contradictory current proof or required artifact failure stays fail closed; pre-barrier/old-epoch data cannot restore or poison a recovered generation.

The approved [ADR-0003](../adr/0003-ws-capture-saturation.md) boundary remains owner-bound admission/drain/leases, irreversible terminal capture failure and bounded diagnostic drain. Its unresolved-source exclusions remain effective; its design-proposed extensions are not accepted by this report. No public API, WAL format, canonical event schema, cap or owner contract is changed.

## 6. PROPOSED ENGINEERING_POLICY appendix — explicit fail-closed boundary

Prospective ID/version: **REC-SEM-UNKNOWN-001 v1**. Status: **PROPOSED / NOT_ACCEPTED**. This is an engineering decision proposal, not EXCHANGE_SPEC or an implementation packet. Architecture/owner must issue an explicit SHA-bound receipt before any policy is called accepted. It deliberately **retains #21's blocker** and authorizes no unsupported canonical behavior.

### Independent decisions

| Decision | Proposed behavior | Alternatives and rationale |
|---|---|---|
| P-U09: unit/grid/conversion unknown | Preserve bounded source quantity lexically for diagnostics; withhold real canonical quantity_unit, QuantitySteps effects and base conversion until each depth-specific mapping is accepted. | Assuming base BTC + multiplier 1; inventing a wire-unit token; copying order metadata would enable effects with unproved dimensions. Reject those choices. An explicitly accepted future assumed mapping needs a different prospective policy/version and residual-risk review. |
| P-U10: removal/zero unknown | Preserve zero as unresolved source data. Withhold DeleteLevel and all dependent canonical regular-book mutation; a zero-bearing current frame has no partial canonical effects. | Mapping zero to delete, ignoring zero, or treating absent top-50 levels as delete risks silently wrong state. None is selected. Nonzero-only deltas also cannot establish a correct persistent book when removal encoding is missing. |
| P-SNAPSHOT-ZERO | Preserve raw zero diagnostically; reject canonical snapshot conversion/anchoring for the entire frame. Do not prune zero levels to manufacture a valid snapshot. | Empty canonical snapshots can be structurally representable under accepted rules; removing an unknown zero is not equivalent. Wire permission remains UNKNOWN despite canonical rejection. |
| P-UNKNOWN/CONTRADICTORY | Unknown mapping/profile blocks applicability. A contradicted adopted assumption revokes its dependent applicability and stops effects at the offending input; scope-local barriers/pending/candidates follow existing contracts. Preserve typed failure and source provenance. | Continuing using last good assumptions or falling back to other categories/profiles hides the conflict. A new policy/version and recorded activation are required for changed meaning. |
| P-RESYNC | Gap/reset/overflow/relevant context change invalidates dependent state; current-scope verified post-barrier WS snapshot, accepted artifacts/proofs and warm-up remain required. Semantic unknown is not cleared by resubscription alone. | REST stitching and ACK/Up-based readiness violate existing gates; zero fallback and historical proof reuse cannot recover a generation. |

### Exact failure behavior and semantic test plan

The following are **proposed tests, NOT_RUN**. They require a later separately authorized implementation packet. Entries describe required behavior, not new Rust error spellings. The unresolved-source block takes precedence over converting a real unverified payload into canonical effects; existing numeric/structural failure outcomes can be tested independently with valid synthetic metadata.

| Case | Required proposed outcome / accepted-contract constraint |
|---|---|
| Positive levels, correct lexical syntax, source mapping still unknown | Diagnostic raw preserved; applicability Blocked; no SetLevel, anchor, proof success or usable_data. |
| Positive levels with later explicitly accepted exact unit/grid mapping | Checked exact count conversion only; positive SetLevel requires accepted removal/profile decision and complete frame proof. Passing U-09 alone does not clear U-10. |
| Zero in update | Current source status UNKNOWN; no inferred DeleteLevel, skip or SetLevel(0); no partial frame effects. Later accepted deletion mapping must emit quantity-free DeleteLevel and prove applicability. |
| Zero in snapshot | Reject entire canonical snapshot; no pruning, implicit deletion or usable anchor. Raw diagnostic remains auditable. |
| Duplicate (side,price), snapshot or update | Existing duplicate guard; reject frame atomically, never last-write-wins. |
| Off-grid price or quantity under valid accepted/synthetic metadata | OffGrid; no rounding, narrowed count or partial effect. Unverified real metadata first remains Blocked. |
| Quantity coefficient/intermediate/u64 count overflow | Existing checked Overflow/CountOutOfRange; no saturation/wrap or partial effect. |
| Invalid syntax, precision-only increment, unknown multiplier | Existing syntax/grid/metadata constraints; unknown conversion never becomes multiplier 1. |
| Wrong category, symbol, regular channel, RPI or SBE profile | No cross-profile unit/delete reuse or alias; block/reject dependent interpretation. |
| Gap, reset, local loss or terminal capture failure | Existing current-scope barrier/owner failure behavior; no readiness or resumed canonical effect by Up alone. |
| Old epoch/pre-barrier raw or proof after recovery | Diagnostic-only; cannot mutate, restore or invalidate the new generation. |
| Contradicted unit/delete assumption or conflicting current proof | Stop current dependent semantics; revoke pending/publication applicability under existing barriers; retain exact causal source/failure. |
| Fresh snapshot after a barrier while unit/delete still unknown | Structurally checked diagnostics only; canonical application/usability remain Blocked. |
| Missing/mismatched/unverified artifacts, wrong scope or absent proof | Existing artifact/proof block; no fabricated verification or implicit latest/network fallback. |
| Equal replay outputs or physical Complete/Durable WAL | No change to semantic evidence status, canonical applicability or publication authority. |

### Minimum future ADR/spec delta and acceptance gate

For this fail-closed clarification, accepted types/events/WAL schemas need **no delta**. If Architecture wants a recorded normative project decision, create a separately scoped ADR that adopts the policy ID/version, bounded applicability, failure order and denied canonical effects; this appendix itself is not that accepted ADR.

To authorize #21 later, the minimum decision must independently resolve U-09 (unit, exact increment relationship, base conversion/multiplier and provenance), U-10 (complete removal mapping), source snapshot/update zero handling, and actual production feed model/version applicability. Record the official depth evidence or visibly assumed ENGINEERING_POLICY mapping, risks, counterexamples and prospective test expected values. Accept and bind immutable InstrumentSpec/FeedProfile/Normalizer/Basis artifacts under existing contracts. Artifact production verification, reducer implementation and independent QA remain separate gates even after source/policy acceptance.

Adding Unknown/Opaque units to canonical event effects, weakening proof applicability, allowing SetLevel(0), changing DeleteLevel or sharing regular/RPI/SBE state would require a separately reviewed spec/ADR change. This report recommends **retaining the blocker**, rather than selecting those changes just to start implementation. There are no new numeric thresholds or calibration values in this proposal.

## 7. Performed checks and next decision

Worker performed fresh GitHub main/Issue #42/#21/#19/branch/PR/claim checks, official source opens, excerpt SHA-256 recomputation, two-path/relative-link/status/provenance checks and `git diff --cached --check` using an isolated temporary index. Detailed commands/results are in [handoff](../handoffs/REC-001E-SEM-001.md). Local Rust/runtime/live checks are **NOT_RUN / NOT_APPLICABLE** for this docs-only diff. Independent final-head source/contract QA and CI are **NOT_RUN / PENDING_INTEGRATOR_PUBLICATION** at authorship.

Next result: Integrator publishes one docs-only draft PR, records its exact final head in mutable PR/Issue metadata, and obtains independent source/contract QA on that head. Architecture/owner then explicitly accepts/rejects the proposed fail-closed clarification and chooses the next evidence/policy task. Until a separate receipt closes each required semantic decision, **#21 remains BLOCKED**. No milestone or Issue closure follows from this report.
