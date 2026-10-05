# Проверяемые инварианты

Статус: policy baseline; тестовая реализация появится по соответствующим Issues. Этот список не означает, что тесты уже проходят.

| ID | Инвариант | Где доказать |
|---|---|---|
| INV-01 | Только публичный read-only radar; live orders отсутствуют в MVP | BOOT-001/CI |
| INV-02 | Цены/количества исполнения — exact ticks/steps с проверкой scale и overflow | SPEC-001 |
| INV-03 | Один владелец каждого состояния instrument/market | REC-001 |
| INV-04 | Очереди ограничены; overflow не теряется незаметно | REC-001/QA-001 |
| INV-05 | Gap/epoch mismatch запрещает использование книги до verified resync | MD-001/REC-001 |
| INV-06 | REST не склеивает WS sequence без доказанного bridge | MD-001 |
| INV-07 | Spot/futures/RPI sequence независимы | SPEC-001 |
| INV-08 | Unknown aggressor/causality сохраняется как UNKNOWN | MD-001/SPEC-001 |
| INV-09 | Live и replay используют одни reducers, записанный receive order, timer/config inputs | REC-001 |
| INV-10 | Raw persistence gap/коррупция/обрезанный tail явно обнаруживаются | REC-001/QA-001 |
| INV-11 | Socket heartbeat не равен свежести каждого market stream | MD-001/REC-001 |
| INV-12 | Уровни доступны только после valid_from; без retroactive signals | M2 |
| INV-13 | Level revision фиксируется для approach episode; новая ревизия не переписывает старый подход | M2 |
| INV-14 | Schmitt thresholds/dwell/reset предотвращают alert chatter | M2/M3 |
| INV-15 | Конфликтующие гипотезы → AMBIGUOUS, не противоположные executable планы | M3 |
| INV-16 | Старый epoch/expired/revoked plan не допускается к новому действию | M3 и future execution |
| INV-17 | Duplicate delivery не создаёт второе логическое действие; нужен durable consumer state при restart | M3 и future execution |
| INV-18 | Trading core не импортирует exchange/private API/alerts/account state | архитектурные тесты |
| INV-19 | Авторские правила, инженерные параметры и гипотезы различаются provenance | RULE-001 |
| INV-20 | Proxy не выдаётся за скрытую заявку, identity участника или фактическую карту стопов | M3/UI |
| INV-21 | Секреты, платный курс, приватные данные и большие datasets не публикуются | каждый PR |
| INV-22 | NOT_RUN никогда не отчётен как PASS | каждый handoff |

Требования позже для execution: bounded price, account limits, no averaging, no stop widening, order reconciliation, partial/racing fills, protective policy. Они сохраняются как обязательный release gate будущего робота, но не требуют private-ключа для разработки радара.
