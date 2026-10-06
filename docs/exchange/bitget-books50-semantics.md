# Bitget regular books50 quantity / deletion evidence — MD-002

Status: **SOURCE EVIDENCE COMPLETE; U-09/U-10 REMAIN BLOCKED**  
Task: MD-002 / Issue #19  
Verification date: **2026-10-06**  
Repository base: `c9c23f707299ab35c051783b75f645e592213822`  
Primary target: current Bitget UTA v3 **regular JSON** public `books50`, especially `usdt-futures`.

This document is a bounded follow-up to `docs/exchange/bitget-public-feed.md`. It records only what current official Bitget sources establish. It does not change SPEC-001 and it does not define a production reducer policy.

## 1. Evidence policy and scope

Normative status `EXCHANGE_SPEC` is granted only to current official Bitget documentation checked on 2026-10-06. No blog, forum, Reddit, StackOverflow, third-party SDK, Classic API convention, or generic order-book convention is used as proof.

Public unauthenticated live observation was **NOT_RUN**. Therefore this task contains no `OBSERVED_ONLY` claim and no raw capture/fixture.

Important inference boundary: an official Bitget field on one interface does not automatically define the semantics of a different interface. In particular, category-specific order-placement `qty` units are not silently transferred to WebSocket depth-level quantity.

## 2. Current official sources

| ID | Current official URL | Evidence used here |
|---|---|---|
| BG-DEPTH | https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel | Regular JSON `books50` categories, snapshot/update model, level shape `[price, quantity]`, seq/pseq |
| BG-BEST | https://www.bitget.com/docs/uta/best-practices-guide | Regular depth sends first snapshot then incremental updates and publishes latest/final state after short-period coalescing |
| BG-MARKET | https://www.bitget.com/docs/catalog/market/market-data | Current standard REST order-book shape and instrument metadata; useful as an adjacent-interface boundary, not WS-delete proof |
| BG-ORDER | https://www.bitget.com/docs/catalog/trading/order-management | Current order-request `qty` units differ by category/order type; this is explicitly not treated as depth-level unit proof |
| BG-RPI | https://www.bitget.com/docs/uta/websocket/public/RPI-OrderBook-Channel | Separate RPI JSON depth profile with three level components and independent sequence behavior |
| BG-SBE | https://www.bitget.com/docs/uta/websocket/sbe/sbe-intro | Separate versioned SBE binary profile; current page exposes schema version 5 / semanticVersion 1.0.0 |

No historical Classic endpoint is used to establish current UTA v3 regular JSON semantics.

## 3. Evidence excerpts / paraphrases and applicability

### BG-DEPTH — regular JSON `books50`

Applicability: public UTA v3 regular JSON; `books50`; categories `spot`, `usdt-futures`, `coin-futures`, `usdc-futures`.

The current page states that:

- `books50` first pushes a full `snapshot`, then incremental `update` messages;
- every ask level is represented as `[sell price, sell quantity]`;
- every bid level is represented as `[buy price, buy quantity]`;
- the documented category list is the same four categories above;
- sequence continuity is expressed through `seq` and `pseq`.

The checked schema does **not** state:

- which asset or unit the depth quantity represents;
- a deletion operation or delete marker for an incremental level;
- that `quantity="0"` means deletion;
- whether zero quantity is allowed or forbidden in a regular snapshot.

The positive-quantity examples on the page do not prove that zero is impossible.

### BG-BEST — incremental/final-state behavior

Applicability: current UTA regular order-book guidance.

The page states that after the first snapshot subsequent order-book data is incremental, and that the system pushes the latest order-book state. It explicitly gives a short-period `A -> B -> A` example where the pushed update reflects the final state.

This establishes final-state/coalescing behavior. It does **not** specify how removal of a price level is encoded, and it does not define `qty=0` as that operation.

### BG-MARKET — adjacent standard REST order book and instruments

Applicability: current UTA REST market data, not the regular JSON WS update encoding.

The current standard REST order-book schema likewise labels the two level components price and quantity. It does not add a depth quantity asset/unit or a deletion encoding.

The instrument schema exposes category/product metadata and futures quantity constraints/multipliers. Those fields do not state that WebSocket depth quantity uses the same unit.

Therefore BG-MARKET does not close U-09 or U-10.

### BG-ORDER — category-specific order-request quantity

Applicability: order placement requests, **not** market-depth levels.

Current official order-management documentation assigns different `qty` meanings depending on product/order type. In particular, it describes USDT/USDC futures order quantity in base coin and COIN futures order quantity in quote coin; Spot has order-type/side-specific request semantics.

This is useful negative-boundary evidence: Bitget quantity fields are interface/category sensitive. Copying those units into the depth feed without a depth-specific statement would be a cross-interface inference. This task therefore does **not** promote those units to regular `books50`.

### BG-RPI — separate JSON depth profile

Applicability: RPI channels only; not regular `books50`.

The current RPI depth schema represents each level with three components: price, non-RPI quantity, and RPI quantity. Standard and RPI order books have separate sequence behavior, and `rpi-books50` is not the regular `books50` profile.

A zero in one RPI quantity component cannot be used as evidence that regular two-component `books50` uses zero as deletion.

### BG-SBE — separate versioned binary profile

Applicability: UTA SBE public WebSocket only; not regular JSON.

The current SBE introduction identifies a separate binary schema (current page: schema version 5 / semanticVersion 1.0.0) with its own depth template and versioned fields. Its profile/version boundary must not be imported into regular JSON.

No checked SBE material used here establishes regular JSON `qty=0` deletion or regular JSON depth quantity units.

## 4. Required question matrix

Each research question has exactly one final status from the MD-002 vocabulary.

| # | Question | Final status | Result / applicability |
|---|---|---|---|
| 1 | What asset/unit does quantity represent in regular JSON `books50`, especially `usdt-futures`? | **UNKNOWN** | BG-DEPTH names only “quantity”. No current depth-specific asset/unit statement was found for `spot`, `usdt-futures`, `coin-futures`, or `usdc-futures`. BG-ORDER units are a different interface and are not transferred. |
| 2 | What is the official incremental deletion operation? | **UNKNOWN** | BG-DEPTH/BG-BEST establish incremental updates/final state, but do not define a remove-level operation or marker for regular JSON `books50`. |
| 3 | Does regular JSON `qty="0"` mean delete? | **UNKNOWN** | No checked current official regular JSON depth source states this mapping. |
| 4 | If zero means delete, for which channel/profile/category/version is it proven? | **UNKNOWN** | The premise is not proven for current regular JSON, so no regular `books50` category/version scope can be asserted. RPI/SBE are separate profiles and do not fill this gap. |
| 5 | Can zero quantity appear in a regular JSON snapshot? | **UNKNOWN** | The schema/examples do not document either permission or prohibition. RPI component-zero behavior is not transferable to regular `books50`. |
| 6 | Are there category/profile differences requiring different normalizer contracts? | **VERIFIED** | Profile separation is proven: regular JSON levels are two-component; RPI JSON levels are three-component with separate sequence/profile behavior; SBE is a separate versioned binary schema. Within **regular JSON**, category-specific depth quantity-unit differences remain UNKNOWN rather than inferred from order APIs. |

No question is classified `CONFLICT`: the checked current official sources do not make contradictory regular-JSON zero/delete claims; they leave the semantics unstated.

## 5. U-09 conclusion — regular books50 quantity unit

**U-09 remains UNKNOWN / BLOCKED.**

For current regular JSON `books50`, including `usdt-futures`, the depth page does not identify the asset/unit of the second level component.

A future REC-001E must not:

- label regular depth quantity as base coin merely because USDT/USDC futures order requests use base-coin `qty`;
- label COIN-futures regular depth quantity as quote coin merely because order requests/public trades use category-specific units;
- infer Spot depth units from order-side/order-type request semantics;
- construct a canonical `quantity_unit` from convention.

The lexical source quantity can continue to be preserved by the accepted decoder boundary, but canonical quantity-unit conversion remains blocked pending accepted depth-specific evidence or an explicit separately reviewed engineering/ADR policy.

## 6. U-10 conclusion — regular qty=0 / deletion

**U-10 remains UNKNOWN / BLOCKED.**

Current official regular JSON documentation does not define `qty="0"` as `DeleteLevel`, nor does it document another incremental deletion marker.

A future REC-001E must not map source zero to the accepted SPEC-001 `DeleteLevel` by convention. Because the source delete encoding is unresolved, canonical application of regular incremental depth mutations remains blocked where a removal must be represented.

This task does not weaken SPEC-001's existing event boundary: `SetLevel(qty>0)` versus `DeleteLevel` without quantity remains unchanged.

## 7. What can be handed to REC-001E

REC-001E may rely on the following source/evidence boundaries:

- current regular JSON `books50` has two-component levels `[price, quantity]`;
- first push is snapshot and subsequent pushes are incremental updates;
- the four documented regular categories are `spot`, `usdt-futures`, `coin-futures`, `usdc-futures`;
- regular JSON, RPI JSON, and SBE are distinct profiles and must not share one implicit normalizer contract;
- RPI's three-component quantity shape and any component-zero observation/example do not define regular JSON semantics;
- category-specific order-request quantity units are not depth-level unit proof;
- U-09 and U-10 remain hard blockers for canonical regular-book quantity/deletion effects.

## 8. Still-forbidden assumptions

Until later accepted evidence or an explicit contract/ADR change says otherwise, it remains forbidden to assume:

- regular `books50` quantity is base coin, quote coin, contracts, lots, or any other specific unit;
- `usdt-futures` depth quantity inherits the order-placement `qty` unit;
- regular JSON `qty="0"` means remove/delete;
- zero can or cannot occur in a regular snapshot;
- RPI zero-component semantics apply to regular JSON;
- SBE semantics or schema versioning apply to regular JSON;
- different regular categories share an undocumented unit merely because their wire level shape is identical.

If an engineering policy is later chosen to bridge any of these gaps, it requires explicit review/ADR/contract treatment; MD-002 does not implement such a policy.

## 9. Checks / limitations

PASS:

- exact base `refs/heads/main` verified before claim: `c9c23f707299ab35c051783b75f645e592213822`;
- issue/claim/branch preflight: no prior Issue #19 comment, no dedicated MD-002 branch, no dedicated MD-002 PR at claim time;
- source-domain audit: normative evidence above uses current official `bitget.com` documentation only;
- question-status audit: all six required questions have exactly one of `VERIFIED`, `OBSERVED_ONLY`, `UNKNOWN`, `CONFLICT`;
- contract-boundary audit: no SPEC-001 change is proposed or performed.

NOT_RUN:

- public live WebSocket observation / capture — **NOT_RUN**;
- local `cargo fmt --all -- --check` — **NOT_RUN / NOT_APPLICABLE** (docs-only source task);
- local `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN / NOT_APPLICABLE**;
- local `cargo test --workspace --locked` — **NOT_RUN / NOT_APPLICABLE**;
- production reducer/runtime validation — **NOT_RUN / OUT_OF_SCOPE**.

No fixture or raw market-data capture is added by MD-002.
