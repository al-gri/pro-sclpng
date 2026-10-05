# Architecture baseline — proposed for M0

## Цель

Создать radar/shadow-систему Bitget с воспроизводимой причинной логикой, затем при отдельном решении подключить execution. Сейчас не заявляем HFT, субмиллисекундный SLA или торговую готовность.

## Первая вертикаль

```text
Public REST instrument metadata + WS market events
    → decoder / per-stream supervisor
    → per-market single-writer state + DataHealth
    → append-only recorded inputs
    → deterministic replay of the same reducers
```

Следующие вертикали:

```text
closed bars / market state
    → confirmed horizontal zones + approach episodes
    → flow/context features + setup arbiter
    → TrueBreakout / FalseBreakout FSM
    → immutable TradePlan events + evidence
    → shadow alert consumer
```

Hot/warm/cold universe ranking и UI добавляются после проверки первого небольшого потока. Разделение модулей логическое, не требование создавать отдельный crate для каждого понятия.

## Границы модулей

| Модуль | Ответственность | Запрет |
|---|---|---|
| domain | exact numeric types, IDs, envelope | сеть/аккаунт |
| market-data | Bitget adapter, book, health, pairing | торговые решения |
| recording | WAL и replay-inputs | скрыто терять события |
| analysis | уровни/признаки | прямые сетевые вызовы |
| strategy | arbiter/FSM/rules/intent | ключи, account risk, Telegram |
| consumers | UI/alerts | самостоятельно менять смысл сигнала |
| execution (later) | account risk, orders, reconciliation | ослаблять plan guards |

Будущий граф зависимостей: adapters/recording/analysis/strategy/consumers импортируют domain; consumers импортируют intent contracts; strategy не импортирует exchange adapters или UI. Composition root собирает компоненты. Replay вызывает те же reducers, а не отдельную Python-копию логики.

## Данные и временная модель

Spot, futures и RPI не сливаются в одну книгу. Epoch принадлежит конкретной stream/book generation; paired context несёт freshness каждого входа. Loss/overflow не превращаются в очередное валидное обновление.

Хранить raw input, receive order, timestamps, версии и управляющие события. Exchange timestamp не доказывает общий порядок независимых каналов. Таймеры, рестарты, конфигурация, leader inputs должны быть воспроизводимы. Parquet — последующий аналитический экспорт, не write-ahead log.

Single-writer упрощает владение состоянием, но не доказывает отсутствие deadlock/latency spikes. Bounded queue требует явной overflow policy. Ticker/UI допускают coalescing; критические book/trade gaps инвалидируют зависимые признаки.

## Trade Intent boundary

До триггера публикуются SetupStateEvent. На подтверждённом событии — immutable TradePlanCreated с instrument/side, IDs, source epochs, schema/strategy/config versions, causal as-of, TTL, price bounds, invalidation, objective target и evidence reference. Revoked/Expired/Superseded — отдельные события. Повторная доставка дедуплицируется.

В MVP plan только shadow; отсутствие обязательного доказательства не превращается в AutoEligible. Telegram/человек могут получить карточку уже после TTL: показывать срок/expired явно; предупреждение ARMED и короткоживущий trigger — разные продукты. Автоисполнение этим не разрешено.

Позже consumer заново проверяет аккаунтный риск, доступную цену, expiry и epochs. Revocation — требование отмены, не гарантия отсутствия racing fill; gateway обязан reconcile partial/late fills. TradePlan не содержит обещания fill. Объём исполнения и fees зависят от аккаунта и instrument spec. Execution/position FSM потребуют отдельного проекта.

## Scope MVP

Rust modular monolith, стандартный сетевой стек/JSON как исходный вариант; публичные данные, recorder/replay, горизонтальные зоны, два сетапа, диагностические flow/leader признаки, shadow alerts.

## Отложено

Private API/live orders; сложная геометрия; точная идентификация spoof/iceberg/игрока; ML ranking; Hawkes/PCA; mandatory CPCV; DPDK/kernel bypass; обязательный SBE; конкретный AWS region; микросервисы и broker в critical path. Это возможные исследования по доказанной необходимости, а не критерии первого старта.

## Источники и параметры

Это инженерное резюме принятых направлений переписки, не независимая проверка курса или API. Факты Bitget подтверждаются MD-001; торговые первоисточники — RULE-001. Числа 0.3/0.6 ATR, TTL 2000ms и latency 1–2ms не утверждены как универсальные константы. Порог либо подтверждён автором, либо отмечен engineering calibration. Unknowns не скрываются.
