# Domain types v1 — проект

Status: **PROPOSED**. SPEC-001, контрольная точка DESIGN REVIEW.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Это кандидат контракта, не принятый Rust API. Код типов/валидаторов отсутствует на этой контрольной точке.
Решение и альтернативы: [ADR-0002](../../docs/adr/0002-domain-event-wal-contracts.md).
Проверки: [test matrix](test-matrix-v1.md).

## 1. Идентичность и единицы

`InstrumentRef = (venue, market_kind, product_namespace, native_symbol)`.
Сравнение всех компонент побайтовое, без case folding, alias resolution или склейки по символу.
`market_kind`: Spot=1, Perpetual=2, DatedFuture=3. Неизвестный вид не подставляется как Spot.
`venue` и `product_namespace` — Token32, `native_symbol` — Token64.
TokenN: от 1 до N ASCII bytes, алфавит `[A-Za-z0-9._:/-]`; N — верхняя граница, не длина Rust array.
Пробелы, NUL, Unicode и пустая строка запрещены. Неизвестная реальная exchange identity блокирует её регистрацию, а не подменяется синтетической.
Идентичность dated/inverse/product partition определяет проверенный metadata profile; его Bitget mapping — **BLOCKED_BY_MD_001**.

`InstrumentSlot(u32)` — только локальная ссылка на зарегистрированный InstrumentRef в одном ArchiveId; slot не является биржевым ID.
Одному slot нельзя приписать другую identity. Разные slot, ссылающиеся на одну identity, отклоняются как неоднозначная регистрация.
`SpecRef = (InstrumentRef, SpecVersion)`; версия неизменяемая, непустая, новые параметры требуют новой версии.
Активация версии является записанным input, не перезаписью metadata в прошлом.

`BookRef = (InstrumentRef, BookClass)`; Normal=1, Rpi=2.
`BookId(u32)` связывается с BookRef один раз в архиве. Обычная и RPI книги не имеют общих snapshot, sequence или BookEpoch.
`StreamId(u32)` обозначает зарегистрированный источник/канал; mapping содержит instrument, ConnectionId, Channel и опциональный BookId.
Channel: BookNormal=1, BookRpi=2, Trades=3. У book-channel обязателен соответствующий BookId; у Trades его нет.
Несколько потоков не становятся одной книгой из-за равного символа. Для BookRef допускается один активный writer; замена stream — явный reset/rebind, не параллельная запись.

## 2. Представления и границы

| Значение | Предлагаемое представление | Диапазон / scope |
|---|---|---|
| PriceTicks | отдельный newtype над u64 | 1..=18446744073709551615; нулевая/отрицательная цена запрещена в v1 |
| QuantitySteps | отдельный newtype над u64 | 0..=18446744073709551615; допустимость 0 определяет событие |
| Decimal coefficient | u128 | 0..=340282366920938463463374607431768211455 |
| Decimal scale | u8 | 0..=18 после канонизации |
| SpecVersion, ConfigVersion, NormalizerVersion, FeedProfileVersion | отдельные u32 newtypes | 1..=u32::MAX; 0 только как явно описанный bootstrap marker WAL Context |
| InstrumentSlot, StreamId, BookId, ConnectionId, ClockId | отдельные u32 newtypes | 1..=u32::MAX; уникальность в ArchiveId |
| ConnectionEpoch | u64 | 1..=u64::MAX, принадлежит ConnectionId |
| SubscriptionEpoch | u64 | 1..=u64::MAX, принадлежит StreamId |
| BookEpoch | u64 | 1..=u64::MAX, принадлежит BookRef; None у Trades |
| ArchiveId, CaptureSessionId | разные opaque [u8;16] | не все нули; фиксируются до записи, не генерируются при replay |
| RecordNo | u64 | 1..=u64::MAX, единый выбранный ingest order в архиве |
| CaptureAttemptNo | u64 | 1..=u64::MAX, отдельный локальный счётчик попыток приёма для StreamId |
| SubEventIndex | u32 | 0..=u32::MAX; последовательный индекс результата одного cause input |
| SegmentNo | u32 | 0..=u32::MAX; начинается с 0 |
| LocalUnixNs | i64 | весь i64, signed Unix nanoseconds; не критерий порядка |
| MonotonicNs, DurationNs | u64 | весь u64, ноль допустим; scope CaptureSessionId + ClockId |

Никакой wraparound: checked increment, исчерпание ID/epoch/order завершает зависимую операцию ошибкой; архив не продолжает нумерацию с нуля.
Численно равные значения разных newtypes не взаимозаменяемы.
`PricedValue = (SpecRef, PriceTicks)` и `SizedValue = (SpecRef, QuantitySteps)` — квалифицированные значения.
Операции сравнения/применения требуют совпадения InstrumentRef и SpecVersion; голые u64 нельзя применять к другой книге или спецификации.
Будущий Rust API должен скрывать unchecked constructors; точные имена методов согласуются в PR до реализации.

## 3. InstrumentSpec и multiplier

InstrumentSpec содержит в указанном смысловом порядке: SpecRef; `price_quote_unit`, `price_basis_unit`, `quantity_unit`, `base_asset` (Token32); `price_increment`, `quantity_increment` (положительные ExactDecimal); `quantity_to_base_multiplier` (Option положительного ExactDecimal); provenance (Token128).
Цена имеет единицы `price_quote_unit / price_basis_unit`.
Quantity выражено в `quantity_unit`; для контракта это contract units, а не автоматически base asset.
Multiplier имеет единицы `base_asset / quantity_unit` и разрешён только при проверенной **постоянной** линейной конверсии.
При quantity_unit=base_asset multiplier должен быть Some(1). Отсутствующий multiplier — UNKNOWN, а не 1.
Inverse/nonlinear valuation и notional/PnL не выводятся из этого поля; зависимая base conversion возвращает UnsupportedConversion/UnknownMultiplier. Сами exact ticks/steps при известных increments при этом разрешены.
Изменение increment, units, multiplier или identity нельзя спрятать под прежней SpecVersion.

`ExactDecimal(c,s) = c / 10^s`. Каноническая форма: c=0 => s=0; при s>0 последняя десятичная цифра c не ноль.
Increment/multiplier требуют c>0. Например increment 0.05 = (5,2), не произвольный decimal places=2: число 100.11 имеет допустимую scale, но не лежит на сетке 0.05.
Все пределы здесь — **engineering proposal**, не утверждение о параметрах Bitget.

## 4. ASCII parsing и точные преобразования

Вход ограничен **96 bytes до разбора**. Грамматика целой строки: `[0-9]+(\.[0-9]+)?`.
Принимаются leading integer zeros и trailing fractional zeros: `000100.1000` => (1001,1).
Перед накоплением коэффициента удалить ведущие integer zeros и конечные fractional zeros; ноль канонизировать отдельно.
Затем проверить scale <=18 и checked-накопление u128 по цифрам (`acc*10 + digit`).
Ограничение 96 действует и на незначащие нули: оно предотвращает неограниченный разбор.
`1.0000000000000000000` допустимо после канонизации; `0.0000000000000000001` имеет значащую scale 19 и отклоняется.
Запрещены `+`, `-` (включая -0), exponent, NaN/inf, comma, whitespace, `.5`, `1.`, Unicode digits. Никакого float и locale parsing.

Порядок ошибок: InputTooLong -> InvalidSign/InvalidSyntax -> ScaleTooLarge -> Overflow(CoefficientMultiply/CoefficientAdd).
Для operation со SpecRef сначала проверить identity/version/units и валидность metadata, затем numeric conversion; невалидное metadata не становится OffGrid.

Для x=(cx,sx), increment d=(cd,sd), cd>0:

```text
s = max(sx, sd)
X = checked_mul(cx, checked_pow10(s - sx))  // u128
D = checked_mul(cd, checked_pow10(s - sd))  // u128, D > 0
X % D != 0 => OffGrid                     // НЕ округлять
n = X / D
checked_u64(n), затем правило zero для целевого типа/события
```

Это консервативный ограниченный алгоритм: intermediate overflow возвращает ошибку даже если альтернативная big-integer/cancellation формула могла бы дать маленький ответ.
Никакого saturating arithmetic и implicit narrowing.
Обратное преобразование n -> decimal: `checked_mul(u128(n), cd)`, scale=sd, затем канонизация.
Форматирование — ASCII без exponent, минимальная дробная часть, ноль `0`; оно не восстанавливает исходное количество незначащих нулей.

QuantitySteps -> base quantity при известном multiplier m=(cm,sm):
`a = checked_mul(steps, quantity_increment.coefficient)`; `b = checked_mul(a, cm)`; scale=`checked_add(sd,sm)` (0..36), затем удалить fractional zeros и потребовать итоговую scale<=18.
Непредставимый итог => ScaleTooLarge/Overflow, не округление. На каждом шаге u128; итог также ExactDecimal.
Обратная base conversion требует такой же exact divisibility и корректных units; не является операцией implicit cast.

Минимальный набор ошибок: IdentityMismatch, SpecMismatch, UnitMismatch, InvalidIncrement, InvalidMultiplier, UnknownMultiplier, UnsupportedConversion, InputTooLong, InvalidSign, InvalidSyntax, ScaleTooLarge, Overflow(operation), CountOutOfRange, OffGrid, ZeroPrice, ZeroTradeQuantity.
Ошибка содержит поле/operation, но не требует panic или включения raw source целиком в diagnostics.

## 5. Zero, deletion и события

QuantitySteps(0) допустим как числовое значение, но не как trade quantity или live book level.
Normalized delta использует `SetLevel(side, price>0, quantity>0)` либо `DeleteLevel(side, price>0)`; Delete не имеет quantity-поля.
В snapshot нулевой level отклоняется, а не превращается в удаление. Trade с нулём отклоняется как ZeroTradeQuantity.
Реальное значение source zero в Bitget update — **BLOCKED_BY_MD_001**: adapter обязан доказать mapping, generic parser сам его не выбирает.
Пустой snapshot структурно представим, но не свидетельствует о пригодной книге без соответствующего verified profile/policy.

## 6. Примеры и review gate

Synthetic: price=100.10, tick=0.05 => 2002; price=100.11 => OffGrid.
Quantity=1.234, step=0.001 => 1234; 1.2345 => OffGrid.
При tick=1 максимальный PriceTicks представим; MAX+1 => CountOutOfRange.
Одинаковый `BTCUSDT` при Spot и Perpetual даёт разные InstrumentRef; одинаковое число ticks под другой SpecVersion неприменимо.

Вопросы D1/D2: достаточны ли u64 counts, u128 intermediates, canonical scale<=18 и 96-byte input bound; принимается ли fail-on-intermediate-overflow вместо более сложного cancellation; принимаются ли только положительные цены первой вертикали и явный UNKNOWN multiplier?
До явного ответа Architecture/Integrator это **PROPOSED**, без exports в crates/domain.
