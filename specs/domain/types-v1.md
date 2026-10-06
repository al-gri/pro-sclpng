# Domain types v1 — проект

Status: **ACCEPTED**. Proposal revision **2**, accepted by owner squash merge of [PR #10](https://github.com/al-gri/pro-sclpng/pull/10); verified main baseline `8d9d6ada6309542822e4e38dd064f1e3467f990b`.
Design base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`; post-merge CI: [37416467117](https://github.com/al-gri/pro-sclpng/actions/runs/37416467117).
Historical numeric D1/D2 review provenance: [Integrator](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5415922496) and [Architecture](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5416219284). Final implementation/QA/owner acceptance is recorded in PR #10.
Решения: [ADR-0002](../../docs/adr/0002-domain-event-wal-contracts.md); проверки: [matrix](test-matrix-v1.md), [review vectors](review-vectors-v2.md); зависимости: [artifacts](artifacts-v1.md).

## 1. Идентичность и единицы

`InstrumentRef = (venue, market_kind, product_namespace, native_symbol)`.
Сравнение всех компонент побайтовое, без case folding, alias resolution или склейки по символу.
MarketKind: Spot=1, Perpetual=2, DatedFuture=3. Неизвестный вид не подставляется как Spot.
venue/product_namespace — Token32, native_symbol — Token64.
TokenN: 1..N ASCII bytes, алфавит `[A-Za-z0-9._:/-]`; N — предел, не длина Rust array. Пробелы, NUL, Unicode, пустая строка запрещены. Реальная неизвестная exchange identity блокирует регистрацию, а не подменяется synthetic.
Dated/inverse/product partition определяет проверенный metadata profile; Bitget mapping — BLOCKED_BY_MD_001.

InstrumentSlot(u32) — локальная ссылка в ArchiveId на один InstrumentRef, не exchange ID. Другую identity нельзя привязать к существующему slot. Разные slot для одной identity также запрещены как неоднозначная регистрация.
SpecRef=(InstrumentRef,SpecVersion); версия неизменяемая. Новые параметры требуют нового определения и записанной активации, не перезаписи прошлого.

BookRef=(InstrumentRef,BookClass), Normal=1/Rpi=2. BookId(u32) связывается с BookRef один раз в архиве. Normal/RPI не разделяют snapshot, sequence или BookEpoch.
StreamId(u32) — зарегистрированный source/channel; mapping включает instrument, ConnectionId, Channel и optional BookId. Channels: BookNormal=1,BookRpi=2,Trades=3. Book channel требует свой BookId; Trades его не имеет.
**C1: один зарегистрированный writer-stream на BookRef за весь архив.** Смена StreamId требует нового ArchiveId: WriterRebindRequiresNewArchive. Same-stream resubscription выражается EpochAdvance. Reset/rebind на другой StreamId внутри архива не поддерживается; совпадение символа не объединяет состояния.
Connection transport хранится один раз на (ConnectionId,ConnectionEpoch); новый stream с тем же owner не сбрасывает его. Freshness/book/pending принадлежат своим stream/book scopes.

## 2. Представления и границы

| Значение | Представление | Диапазон / scope |
|---|---|---|
| PriceTicks | отдельный u64 newtype | 1..=18446744073709551615; zero/negative запрещены в v1 |
| QuantitySteps | отдельный u64 newtype | 0..=18446744073709551615; событие определяет допустимость 0 |
| Decimal coefficient | u128 | 0..=340282366920938463463374607431768211455 |
| Decimal scale | u8 | 0..=18 после канонизации |
| SpecVersion,ConfigVersion,NormalizerVersion,FeedProfileVersion | разные u32 newtypes | 1..=u32::MAX; нулевые обычные версии НЕДОПУСТИМЫ |
| InstrumentSlot,StreamId,BookId,ConnectionId,ClockId | разные u32 newtypes | 1..=u32::MAX, archive-qualified |
| ConnectionEpoch | u64 | 1..=MAX, owner ConnectionId |
| SubscriptionEpoch | u64 | 1..=MAX, owner StreamId |
| BookEpoch | u64 | 1..=MAX, owner BookRef; None для Trades |
| ArchiveId,CaptureSessionId | разные opaque [u8;16] | не все нули; записаны при setup, не создаются replay |
| RecordNo | u64 | 1..=MAX, dense archive admission order |
| CaptureAttemptNo | u64 | 1..=MAX, per StreamId за архив; starts1, epochs не сбрасывают |
| RawSubIndex,OutputIndex | разные логические u32 индексы | 0..=MAX; индекс source output и индекс apply-step output не взаимозаменяемы |
| SegmentNo | u32 | 0..=MAX, starts0 |
| LocalUnixNs | i64 | весь signed range; не критерий порядка |
| MonotonicNs,DurationNs | u64 | включая0; scope CaptureSessionId/ClockId |

BootstrapContext — отдельный вариант WAL Context, декодируемый из пары (0,0) только для разрешённых administrative inputs до первой config. Это НЕ constructors ConfigVersion(0)/NormalizerVersion(0). Смешанные zero/nonzero пары и bootstrap market/control inputs отвергаются. Frontier sentinel0 допустим только для ещё пустого accounting/invalidation frontier, не как RecordNo.

RawFrameId, SourceCandidateKey, SourceApplicationKey, EventId, EventCursor и CausalBasis определены в [events sections1–2](../market-data/events-v1.md). Source raw identity не меняется при позднем proof; apply identity получает текущий RecordNo. EventCursor сравним через записанные normalizer activations в одном ArchiveId. Control/definition references имеют RecordRef, не фиктивный market EventCursor.
ArtifactRef — отдельный constrained Token128: ровно `sha256:` +64 lowercase hex digits. Parsed ref не доказывает проверку bytes; namespace/resolution/descriptor policy в artifacts-v1. Числовой revision без content binding не разрешает canonical replay.

Никакого wraparound: checked increments, exhaustion ID/epoch/order заканчивается ошибкой. Численно равные разные newtypes не взаимозаменяемы.
PricedValue=(SpecRef,PriceTicks),SizedValue=(SpecRef,QuantitySteps) qualified. Сравнение/применение требует той же identity/spec; голый u64 не переносится на другую книгу.
Unchecked constructors будущего Rust API скрыты; точные имена/варианты ошибок согласуются с этим spec и review.

## 3. InstrumentSpec и multiplier

InstrumentSpec: SpecRef; price_quote_unit,price_basis_unit,quantity_unit,base_asset(Token32); positive ExactDecimal price_increment/quantity_increment; quantity_to_base_multiplier(Option positive ExactDecimal); provenance.
В canonical WAL provenance — ArtifactRef на InstrumentSpec descriptor с точной копией числовых полей/units и отдельным human/source provenance, не произвольная строка (явное A3 изменение). Независимая synthetic numeric conversion не требует реализации loader/hash.
Цена имеет units price_quote_unit/price_basis_unit. Quantity выражено в quantity_unit, не автоматически base asset.
Multiplier имеет units base_asset/quantity_unit и разрешён только для проверенной постоянной линейной конверсии. При quantity_unit=base_asset он Some(1). None=UNKNOWN, не1. Inverse/nonlinear valuation,notional/PnL не выводятся из multiplier: UnsupportedConversion/UnknownMultiplier блокируют только зависимое преобразование. Grid conversions при известных increments независимы.
Изменение increment/units/multiplier нельзя скрыть под старой SpecVersion.

ExactDecimal(c,s)=c/10^s; canonical: c=0 =>s=0; при s>0 последняя цифра c не0. Increments/multiplier требуют c>0. Tick0.05=(5,2), не просто decimal places2:100.11 off-grid.

## 4. ASCII parsing и checked conversion — D1/D2 без изменения алгоритма

Input<=96 bytes до разбора. Полная grammar `[0-9]+(\.[0-9]+)?`. Leading integer zeros и trailing fractional zeros допустимы:000100.1000→(1001,1). Перед coefficient accumulation удалить эти незначащие нули; ноль отдельно canonical(0,0). Затем scale<=18 и checked acc*10+digit в u128. Bound96 действует и на незначащие нули.
1.0000000000000000000 допустимо после canonicalization;0.0000000000000000001 значащая scale19 запрещена. Sign(+/- включая-0),exponent,NaN/inf,comma,whitespace,.5,1.,Unicode digits запрещены. Float/locale parsing отсутствует.
Ошибка priority: InputTooLong -> InvalidSign/InvalidSyntax -> ScaleTooLarge -> Overflow(CoefficientMultiply/CoefficientAdd). Сначала metadata identity/version/units/validity, потом conversion; invalid metadata не становится OffGrid.

Для x=(cx,sx),increment d=(cd,sd),cd>0:

```text
s=max(sx,sd)
X=checked_mul(cx,checked_pow10(s-sx))  // u128
D=checked_mul(cd,checked_pow10(s-sd))  // u128 > 0
X % D != 0 => OffGrid                 // не rounding
n=X/D
checked_u64(n), затем zero policy
```

Intermediate overflow возвращает ошибку даже когда big-integer/cancellation формула могла бы получить маленький итог. Это согласованное консервативное ограничение; no saturation/narrowing.
Reverse: checked_mul(u128(n),cd),scale=sd,canonicalization. ASCII formatting без exponent, минимальная дробная часть, zero='0'; исходные незначащие нули не восстанавливаются.

QuantitySteps→base: a=checked_mul(steps,step.coefficient);b=checked_mul(a,multiplier.coefficient);scale=checked_add(sd,sm)(0..36), затем убрать fractional zeros и потребовать итогscale<=18. На каждом multiply u128. Непредставимый итог ScaleTooLarge/Overflow, без rounding. Обратная base conversion требует exact divisibility/units, не implicit cast.

Ошибки: IdentityMismatch,SpecMismatch,UnitMismatch,InvalidIncrement,InvalidMultiplier,UnknownMultiplier,UnsupportedConversion,InputTooLong,InvalidSign,InvalidSyntax,ScaleTooLarge,Overflow(operation),CountOutOfRange,OffGrid,ZeroPrice,ZeroTradeQuantity. Diagnostic указывает поле/operation, не требует panic или публикации raw source целиком. Exact constructor error naming остаётся предметом code review независимого numeric slice.

## 5. Zero и event boundary

QuantitySteps(0) допустим численно, но не live level/trade. SetLevel(side,price>0,qty>0) либо DeleteLevel(side,price>0) БЕЗ qty. Snapshot zero-level отвергается, не deletion. Trade zero=>ZeroTradeQuantity. Bitget source-zero mapping BLOCKED_BY_MD_001; generic parser не угадывает его.
Empty snapshot структурно представим, но usability требует verified profile/policy. Book effects применяются только frame-wide по events A1; numeric parsing само по себе не подтверждает snapshot.

## 6. Примеры и граница разрешения

Synthetic:price100.10/tick0.05→2002;100.11→OffGrid. Qty1.234/step0.001→1234;1.2345→OffGrid. Tick1 допускает maxPriceTicks,MAX+1→CountOutOfRange. Spot/Perpetual BTCUSDT различны; SpecVersion mismatch запрещает применение равных counts.
D1/D2 уже согласованы только для изолированной реализации в этой же ветке/PR. Они не переоткрываются как неопределённый вопрос и не принимают остальные контракты. На этой docs-only ревизии Rust-код не изменяется. Event/health/WAL/artifact relations остаются DESIGN_REVIEW_REQUIRED; все новые документы PROPOSED до принятия workflow.
