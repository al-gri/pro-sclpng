# Bitget public feed contract — MD-001

Status: **VERIFIED WITH EXPLICIT UNKNOWN / CONFLICT / BLOCKED ITEMS**  
Task: MD-001 / Issue #4  
Verification date: **2026-10-06**  
Verification session start: 2026-10-06T08:01+02:00 (Europe/Warsaw)  
Repository base: 27084cd2708cd0fb50eb447ef176baf412c6bd56

This document records only Bitget facts supported by current official sources. It is input to REC-001, not a production WebSocket connector. Anything not established by a cited Bitget source is explicitly UNKNOWN, NOT_PROVEN, CONFLICT, or BLOCKED.

## 1. Verification metadata and official sources

The regular JSON UTA pages do not expose one global immutable documentation version. Pages below were checked on 2026-10-06. The SBE intro currently embeds schema version 5 / semanticVersion 1.0.0; that SBE version does not version the regular JSON pages.

| ID | Official Bitget URL | What it establishes |
|---|---|---|
| BG-QUICK | https://www.bitget.com/docs/uta/quick-start | Current UTA v3 production/demo WS endpoints, public WS limits, ping/pong |
| BG-DEPTH | https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel | Regular JSON books50 categories, snapshot/update model, 20 ms push, level order/shape, seq/pseq |
| BG-TRADES | https://www.bitget.com/docs/uta/websocket/public/New-Trades-Channel | publicTrade schema, fill side, IDs, timestamp, isRPI, size unit |
| BG-RPI | https://www.bitget.com/docs/uta/websocket/public/RPI-OrderBook-Channel | RPI channels, 100 ms rpi-books50 snapshots, two quantity components, independent sequence rules |
| BG-MARKET | https://www.bitget.com/docs/catalog/market/market-data | GET /api/v3/market/instruments, standard/RPI REST books, instrument fields and futures multipliers |
| BG-BEST | https://www.bitget.com/docs/uta/best-practices-guide | Regular JSON depth frequency/snapshot-update guidance and unchanged-book behavior |
| BG-CHANGE | https://www.bitget.com/legacy-docs/uta/changelog | Checksum removal, SBE launch history and feed changes |
| BG-DEMO | https://www.bitget.com/docs/uta/demo-trading/websocket | Demo WS address and Demo API Key requirement |
| BG-SBE | https://www.bitget.com/docs/uta/websocket/sbe/sbe-intro | Separate SBE profile and current schema version |
| BG-PUBLIC-AUTH | https://www.bitget.com/docs/uta/rest-api | Generic statement that public interfaces can be used without authentication; its older domain table is not used for current v3 endpoint selection |
| BG-TICK-CHANGE | https://www.bitget.com/support/articles/12560603894868 | 2026-09-11 official spot tick-size change notice; proves spot tick size changes operationally |
| BG-UPGRADE | https://www.bitget.com/docs/classic/uta-api-upgrade-guide | v3 migration context and books50 “up to 50 levels” statement |

Current endpoint/channel semantics are taken from current v3 pages BG-QUICK, BG-DEPTH, BG-TRADES, BG-RPI and BG-MARKET. Historical/generic pages are used only for the specific statements named above.

## 2. Scope

Primary REC-001 profile verified here:

- production UTA **regular JSON v3** public WebSocket;
- categories spot, usdt-futures, coin-futures, usdc-futures;
- regular books50 and publicTrade;
- REST instrument metadata and REST order book only to answer metadata/stitching questions;
- RPI public depth only far enough to define isolation and accepted-contract conflicts;
- SBE only far enough to prevent accidental cross-profile assumptions.

Not established as the REC-001 implementation profile: private/account/order APIs, Classic v2, VIP/Lo-La, Reality/Stock+, SBE decoder, Margin public-WS mapping, execution/order placement, latency/performance.

## 3. Endpoint, category and subscription matrix

Source: BG-QUICK, BG-DEMO, BG-PUBLIC-AUTH, BG-DEPTH, BG-TRADES.

| Item | Verified result |
|---|---|
| Regular production public WS | wss://ws.bitget.com/v3/ws/public |
| Production SBE public WS | wss://ws.bitget.com/v3/ws/public/sbe |
| Demo public WS | wss://wspap.bitget.com/v3/ws/public |
| Demo auth | Demo docs require creating/using a Demo API Key |
| Production public channel auth | Public interface docs say public requests can be used without authentication; current public channel examples subscribe without a login frame |
| books50/publicTrade categories | spot, usdt-futures, coin-futures, usdc-futures |
| REST instrument categories | SPOT, MARGIN, USDT-FUTURES, COIN-FUTURES, USDC-FUTURES |
| MARGIN to public WS category | **UNKNOWN / NOT_PROVEN**; do not silently reuse spot |
| Subscribe shape | JSON op=subscribe with args entries containing instType, topic, symbol |
| Unsubscribe | Same request family with op=unsubscribe |
| Ack | event=subscribe plus arg and connId; schemas expose code/msg on errors |
| Max args per one subscribe request | **UNKNOWN** in the checked current public-channel/Quick Start pages |

Do not transfer assumptions between categories when a Bitget page does not state they share semantics.

### Mapping to accepted InstrumentRef

Safe project mapping:

- spot / SPOT → MarketKind::Spot.
- Futures require metadata type: perpetual → MarketKind::Perpetual; delivery → MarketKind::DatedFuture.
- Preserve the Bitget product line in product_namespace so equal native symbols in different product lines never alias.
- Exact product_namespace token spelling is project normalization, not a Bitget wire guarantee.
- MARGIN remains outside the initial REC-001 profile because no distinct checked public-WS category mapping was established.

## 4. Instrument metadata mapping

Source: BG-MARKET, BG-TICK-CHANGE.

Endpoint: GET https://api.bitget.com/api/v3/market/instruments

Relevant documented fields:

| Need | Bitget field(s) | Verified interpretation |
|---|---|---|
| Symbol identity | symbol, category, baseCoin, quoteCoin | Source identity/product line |
| Futures kind | type | perpetual or delivery |
| Trading state | status | listed, online, limit_open, limit_close, offline, restrictedAPI |
| Price decimal constraint | pricePrecision | Decimal places allowed |
| Quantity decimal constraint | quantityPrecision | Decimal places allowed |
| Futures price step | priceMultiplier | Futures only; order price must be a multiple and satisfy precision |
| Futures quantity step | quantityMultiplier | Futures only; used with quantityPrecision |
| Min/max quantity | minOrderQty, maxOrderQty, maxMarketOrderQty | Futures order constraints; maxOrderQty=0 means no limit |
| Minimum amount | minOrderAmount | Unit USDT per current docs |
| Lifecycle | offTime, limitOpenTime, deliveryTime, deliveryStartTime, maintainTime | Product/status timing where applicable |

### Exact increments required by SPEC-001

**Futures:** priceMultiplier and quantityMultiplier are explicit source grid multipliers. They may be parsed as positive ExactDecimal candidates and validated together with their precision constraints.

**Spot/Margin:** the checked instrument schema gives precision but does **not** expose priceMultiplier/quantityMultiplier. Treating 10^-pricePrecision or 10^-quantityPrecision as the authoritative exchange increment would be an inference. Therefore:

- spot price_increment mapping = **NOT_PROVEN / BLOCKED**;
- spot quantity_increment mapping = **NOT_PROVEN / BLOCKED**;
- same for MARGIN.

BG-TICK-CHANGE independently proves that Bitget changes spot tick sizes and defines tick size as the minimum unit price change, but does not provide a machine-readable metadata version/cursor or prove that pricePrecision alone is the tick contract.

### Metadata changes/versioning

The checked instruments response exposes no metadata revision, effective sequence, ETag-like source version, or WS activation cursor.

Bitget metadata version/change cursor = **UNKNOWN / NOT_DOCUMENTED**.

A project SpecVersion transition may be an engineering policy, but must not be described as Bitget-native version semantics.

## 5. Regular JSON books50 contract

Source: BG-DEPTH; corroboration BG-BEST and BG-UPGRADE.

Documented facts:

- topic: books50;
- categories: spot, usdt-futures, coin-futures, usdc-futures;
- 50-level / “up to 50 levels” tier;
- push frequency: **20 ms** for Spot and Futures;
- first push after subscription: action=snapshot;
- subsequent pushes: incremental action=update;
- asks a are ascending by price;
- bids b are descending by price;
- each level is [price, quantity], encoded as strings in the schema;
- maxDepth is documented for full books, not books50;
- seq increments when the book is updated and is intended for out-of-order detection;
- pseq is previous-push sequence for books/books50 and is intended for packet-loss detection;
- checksum was removed from depth channels on 2026-05-19; current guidance uses seq/pseq.

### Quantity meaning

The regular depth page labels the second level component only as sell/buy quantity; it does not state its asset/unit.

regular books50 quantity unit = **UNKNOWN / BLOCKED_FOR_CANONICAL_UNIT_MAPPING**.

Do not copy the publicTrade size-unit rule into order-book levels without an official source.

### Zero/deletion semantics

The checked current regular depth page does not define:

- whether an update level can contain quantity "0";
- whether "0" means delete the price level;
- whether a snapshot can contain zero quantity;
- an alternate explicit delete marker.

Therefore:

regular books50 zero → DeleteLevel mapping = **UNKNOWN / BLOCKED**.

This blocks a correct production incremental book reducer under accepted SPEC-001, which requires explicit SetLevel(qty>0) versus DeleteLevel and forbids guessing source-zero semantics.

### Ordering and duplicates

Price ordering of ask/bid arrays is documented. Behavior for duplicate equal-price entries within one payload is **UNKNOWN**. Accepted SPEC-001 already rejects duplicate (side,price) in one normalized update; MD-001 does not weaken it.

## 6. publicTrade contract

Source: BG-TRADES and BG-MARKET.

Verified schema/facts:

- topic publicTrade;
- categories spot, usdt-futures, coin-futures, usdc-futures;
- real-time push;
- p = fill price;
- v = fill size;
- i = execution ID;
- L = execution correlation ID;
- S = **Fill side** (buy/sell);
- T = Unix-millisecond fill timestamp;
- isRPI = whether the fill is an RPI fill, yes/no;
- size unit: COIN-FUTURES = quote coin; all other listed categories = base coin.

The regular JSON page calls S only “Fill side”. It does not say taker/aggressor/initiator side.

JSON publicTrade S ↔ accepted aggressor = **UNKNOWN**. Preserve Aggressor::Unknown unless a separate official source proves the mapping.

No checked source links L or another trade field to a book sequence. trade ↔ book sequence link = **UNKNOWN**.

The page does not say zero-size fills are possible or impossible. publicTrade zero quantity = **UNKNOWN**; accepted canonical Trade still rejects zero.

## 7. Snapshot/update/continuity matrix

Source: BG-DEPTH.

| Case | Official contract | MD-001 consequence |
|---|---|---|
| New successful books50 subscription | First push is snapshot | Snapshot is the only documented initial anchor |
| Update → update | Previous update seq must equal next update pseq | Mismatch is a documented continuity failure/gap signal |
| Update monotonicity | seq should increase except during symbol maintenance | Non-increase must not be silently normalized |
| Snapshot → first update | snapshot seq should lie within first update [pseq, seq] | Validate the interval; do **not** require equality snapshot.seq=first.pseq |
| Release/restart | Sequence may reset; users will “most likely” get pseq=0 | Reset exists; pseq=0 is likely, **not an exhaustive guarantee** |
| Messages after reset | Bitget says sequence continues normally | Project still requires a new trusted snapshot before canonical reuse |
| Gap recovery/retransmit | No retransmission/replay repair procedure documented | **UNKNOWN**; invalidate and resubscribe rather than invent recovery |
| Duplicate/replayed WS message | Not defined | **UNKNOWN**; fixture is synthetic negative/idempotency test only |
| Repeated seq | Not defined as legal duplicate behavior | **UNKNOWN** |
| Repeated snapshot in one live epoch | Not defined | **UNKNOWN** |
| Resubscribe edge behavior beyond first-push rule | No separate state-machine guarantee found | **UNKNOWN** |
| Reconnect continuity | Reconnect itself does not prove continuity | New connection/subscription generation and fresh snapshot required by project policy |

### Proven gap detector

For update→update:

previous.seq != current.pseq

is a documented packet-loss/continuity failure. It is sufficient to invalidate the current book. It is not a documented automatic recovery algorithm.

For snapshot→first update, the documented check is:

first.pseq <= snapshot.seq <= first.seq

## 8. Heartbeat and transport limits

Source: BG-QUICK; unchanged-book observation corroborated by BG-BEST.

Heartbeat:

- client should send text "ping" every **30 seconds**;
- server responds text "pong";
- if no pong is received Bitget says reconnect;
- server disconnects if no ping is received for **2 minutes**;
- an exact client-side “wait N milliseconds for pong” timeout is not specified.

Transport heartbeat is not market-stream freshness. BG-BEST says no new book snapshot is sent when the order book has not changed, so absence of market updates is not proof of a broken socket. This preserves INV-11.

Current documented limits:

| Limit | Value/scope |
|---|---|
| Connection requests | 300 / IP / 5 minutes |
| Concurrent connections | max 100 / IP |
| Subscription requests | 240 / hour / connection |
| Channel subscriptions | max 1000 / connection |
| Client messages | max 10 messages / second / connection, including ping/login/subscribe/unsubscribe |
| Limit violation | Connection may be disconnected; repeatedly disconnected IPs may be blocked |
| Recommended channels | fewer than 50 / connection; recommendation, not the 1000 hard maximum |
| Reconnect rate | covered by 300 connection requests / IP / 5 minutes |
| Topics/args in one subscribe request | **UNKNOWN** |
| Unsubscribe-specific hourly quota | **UNKNOWN**; 10 msg/s still applies |
| Maximum WS frame/message size | **UNKNOWN / NOT_DOCUMENTED** in checked pages |

## 9. REST snapshot and stitching conclusion

Source: BG-MARKET.

Standard REST order book:

GET /api/v3/market/orderbook?category=...&symbol=...

Documented response:

- default limit 5, maximum 1000;
- asks ascending, bids descending;
- level [price, quantity];
- response has ts in milliseconds;
- **no seq or pseq field is documented**.

No checked official source defines a common sequence domain or causal stitching algorithm between this REST snapshot and regular WS books50.

**REST ↔ WS stitching = NOT_PROVEN / FORBIDDEN.**

Consequences preserving INV-06:

- do not stitch by timestamp;
- do not infer continuity from matching prices/levels;
- do not use REST to heal a WS gap;
- the trusted regular-book resync anchor is a new raw WS books50 snapshot that passes documented sequence checks and project proof gates.

RPI REST order book likewise has timestamp but no documented sequence bridge to RPI WS.

## 10. RPI semantics and contract boundary

Source: BG-RPI, BG-MARKET and BG-TRADES.

RPI = **Retail Price Improvement** in current Bitget documentation.

Verified facts:

- separate channels exist: rpi-books, rpi-books1, rpi-books5, rpi-books50;
- standard and RPI order books have **independent sequence rules**, calculated separately;
- rpi-books50 sends a **snapshot on every push**, at **100 ms**;
- pseq is documented as meaningful only for the full rpi-books channel, not rpi-books50;
- each RPI depth level is [price, non-RPI quantity, RPI quantity];
- official examples include a level whose non-RPI component is 0 while RPI quantity is nonzero;
- publicTrade.isRPI=yes/no identifies whether a fill is an RPI fill;
- REST provides RPI symbol/order-book endpoints.

Normal books50 and rpi-books50 must not be joined merely because category+symbol matches. Their independent sequence domains reinforce accepted independent Normal/RPI BookRef/epochs.

### CONTRACT_CONFLICT: RPI level shape

Accepted SPEC-001 BookSnapshot/BookUpdate carries one canonical quantity per (side,price), with SetLevel versus DeleteLevel. Bitget RPI depth carries **two independent quantities per price**: non-RPI and RPI.

No official source authorizes summing them, dropping one component, or substituting one for the other.

**RPI depth → current one-quantity canonical Book level = CONTRACT_CONFLICT / BLOCKED.**

MD-001 does not modify SPEC-001. A separate ADR/change task is required before RPI depth normalization.

## 11. MBP observability and limitations

Source: BG-DEPTH.

Regular books50 exposes price levels [price, quantity]. It exposes no individual order ID, queue position, participant ID, or per-order timestamp. Its observable model is therefore price-level aggregate / Market By Price, not Market By Order.

Observable limitations:

- finite tier: up to 50 levels;
- no individual order IDs;
- FIFO/order-level queue position is not observable;
- underlying orders at the same price cannot be separated from this payload;
- participant/order-owner identity is not observable;
- full-book liquidity outside the tier is not observable.

Do **not** infer true iceberg hidden size, spoof intent, participant identity/“large player”, exact queue position, or order-level FIFO from books50 alone.

Whether Bitget internally coalesces multiple source events into each 20 ms update beyond the visible price-level aggregation is **UNKNOWN**.

## 12. Resync and warm-up specification

Exchange facts:

- fresh books50 subscription begins with a snapshot;
- update→update continuity is previous.seq == next.pseq;
- snapshot→first-update uses the documented interval check;
- sequences may reset around releases/restarts;
- no REST bridge and no retransmission recovery algorithm is documented;
- heartbeat does not establish book continuity.

Project policy required to preserve accepted SPEC-001/INVARIANTS:

The current book becomes unusable when a seq/pseq continuity check fails, snapshot→first-update interval check fails, a reset/maintenance discontinuity invalidates the old chain, connection/subscription/book generation changes, local critical loss/overflow occurs, or source semantics cannot be proven.

After invalidation:

1. retain old data for audit if useful but do not apply stale pre-gap deltas to the new generation;
2. reconnect/resubscribe as needed;
3. require a **new raw WS books50 snapshot**;
4. validate its first incremental update using Bitget interval rules;
5. only then allow accepted DataHealth verification/warm-up policy to progress.

Transport Up or "pong" alone never makes the book usable. Stale/pre-gap events cannot restore continuity.

**Bitget-defined warm-up: UNKNOWN / not specified.**  
Any warm-up threshold is accepted project engineering policy, not a Bitget guarantee.

## 13. Mapping to accepted ProScalping contracts/invariants

| Accepted requirement | MD-001 result |
|---|---|
| Exact price/quantity grids | Futures multipliers documented; Spot/Margin exact increment mapping NOT_PROVEN; regular book quantity unit UNKNOWN |
| Explicit SetLevel vs DeleteLevel | Regular JSON zero/delete mapping UNKNOWN / BLOCKED |
| Aggressor unknown preservation | JSON trade S is only “Fill side”; keep Aggressor::Unknown |
| RPI identity separation | Strengthened by Bitget independent RPI sequence rules |
| One-quantity RPI book payload | CONTRACT_CONFLICT / BLOCKED |
| INV-05 gap invalidation | Supported by seq/pseq continuity detector; recovery remains project policy |
| INV-06 no unproven REST stitching | REST has no common sequence field: NOT_PROVEN / FORBIDDEN |
| INV-08 unknown causality | Trade↔book link and JSON aggressor stay UNKNOWN |
| INV-11 heartbeat != freshness | Directly preserved |
| Warm-up | No Bitget warm-up guarantee found |

## 14. UNKNOWN / CONFLICT / BLOCKED table

| ID | Status | Exact unresolved item | REC-001 consequence |
|---|---|---|---|
| U-01 | UNKNOWN | Separate generic unauthenticated public testnet endpoint beyond documented Demo | Do not invent testnet URL |
| U-02 | UNKNOWN | Maximum args/topics in one subscribe request | Batch conservatively; enforce known connection/request limits |
| U-03 | UNKNOWN | Exact client-side pong timeout | Supervisor timeout is engineering policy |
| U-04 | UNKNOWN | Maximum WS frame/message size | Parser bound must be engineering policy with provenance |
| U-05 | UNKNOWN | MARGIN → public WS category mapping | Exclude MARGIN from initial profile |
| U-06 | NOT_PROVEN / BLOCKED | Spot/Margin exact price increment from pricePrecision alone | Cannot emit exchange-proven price_increment |
| U-07 | NOT_PROVEN / BLOCKED | Spot/Margin exact quantity increment from quantityPrecision alone | Cannot emit exchange-proven quantity_increment |
| U-08 | UNKNOWN | Bitget metadata version/change sequence | Project SpecVersion transitions remain policy |
| U-09 | UNKNOWN / BLOCKED | Regular books50 quantity asset/unit | Canonical quantity_unit mapping cannot be claimed |
| U-10 | UNKNOWN / BLOCKED | Regular delta qty=0 and deletion semantics | Production incremental reducer must not guess DeleteLevel |
| U-11 | UNKNOWN | Regular snapshot zero quantity | Zero fixture is negative/unknown test only |
| U-12 | UNKNOWN | Trade zero quantity possibility | Canonical zero Trade remains rejected |
| U-13 | UNKNOWN | Duplicate equal-price entries within one source payload | Keep accepted duplicate-level rejection |
| U-14 | UNKNOWN | Duplicate/replayed WS-message semantics and repeated seq guarantee | Idempotency test is project policy, not exchange guarantee |
| U-15 | UNKNOWN | Repeated snapshot inside one subscription epoch | Treat unexpected snapshot conservatively as resync input/new-anchor policy |
| U-16 | UNKNOWN | Resubscribe edge semantics beyond first-push rule | New successful subscription requires new trusted snapshot |
| U-17 | UNKNOWN | Exchange retransmission/gap-repair protocol | Invalidate/resync; no replay assumption |
| U-18 | UNKNOWN | JSON publicTrade.S aggressor/taker semantics | Aggressor::Unknown |
| U-19 | UNKNOWN | Trade L relationship to book sequence | trade_book_link=Unknown |
| U-20 | NOT_PROVEN / FORBIDDEN | REST↔WS common causal/sequence bridge | Never stitch by timestamp/price |
| C-01 | CONTRACT_CONFLICT / BLOCKED | RPI level [price, non-RPI qty, RPI qty] vs accepted one-qty Book level | Separate ADR/change task before RPI normalization |
| C-02 | CONFLICT / PROFILE-SEPARATION | 2026-04-21 SBE launch changelog described SBE books50 full snapshots every 20 ms, while current SBE schema v5 has depthAction Snapshot/Update and pseq since v5 | Do not infer SBE behavior from old launch text; any SBE implementation needs its own version-aware task |
| U-21 | UNKNOWN | Internal event coalescing/aggregation within 20 ms regular feed beyond visible price-level aggregation | Do not infer order-event completeness or FIFO |

These blockers are findings of MD-001, not permission to alter accepted SPEC-001 in this task.

## 15. Fixtures manifest

Fixtures live only under tests/fixtures/bitget/ and are intentionally bounded. They contain no API key, cookie, account/private data, or raw market archive.

All fixtures are **origin=synthetic**, generated from official schemas checked on 2026-10-06. Synthetic edge cases do not claim Bitget emitted those exact values live.

tests/fixtures/bitget/manifest.json records exact source basis, expected interpretation, guarantee scope and SHA-256 for:

- books50-snapshot.json;
- books50-update.json;
- books50-gap.json;
- books50-duplicate.json;
- books50-reset.json;
- books50-empty-levels.json;
- public-trades.json;
- books50-zero-quantity-unknown.json;
- rpi-books50-snapshot.json.

Required snapshot/update/gap/duplicate/reset/empty-levels/trades scenarios are present. Zero and RPI bounded cases preserve explicitly unknown/conflicting semantics instead of fabricating guarantees.

## 16. REC-001 gate and out-of-scope confirmation

MD-001 is complete as a source-verification task, but REC-001 must respect these blockers:

1. Regular JSON books50 production local-book application is blocked on official deletion/zero mapping.
2. Canonical regular-book quantity unit is not established by the checked depth docs.
3. Spot exact tick/quantity-step derivation is not proven by the current instruments schema.
4. RPI normalization conflicts with the accepted one-quantity level model and requires separate ADR/change work.

Futures metadata multipliers, regular JSON sequence continuity, heartbeat/limits, publicTrade field parsing and REST non-stitching are sufficiently documented to build bounded parsing/supervision scaffolding **without** inventing the blocked canonical effects.

No production connector, decoder, local book, supervisor, recorder/replay runtime, private API, strategy, execution, performance benchmark, merge, or auto-merge is implemented by MD-001.
