# Контракты: реестр статусов

Bootstrap не замораживает Rust API или JSON schema молча. Ни одна схема ниже ещё не реализована/протестирована. SPEC-001 создаёт первый набор accepted contracts.

| Contract | Status | Владелец/задача |
|---|---|---|
| InstrumentRef, exact numeric types | DRAFT_REQUIRED | SPEC-001 |
| NormalizedMarketEvent + causal envelope | DRAFT_REQUIRED | SPEC-001/MD-001 |
| DataHealth + epochs + warm-up | DRAFT_REQUIRED | SPEC-001 |
| WAL frame + recovery/durability | DRAFT_REQUIRED | SPEC-001 |
| Level/ApproachEpisode + valid_from | PLANNED_M2 | Quant + Architect |
| SetupStateEvent + conflict policy | PLANNED_M3 | Quant + Architect |
| TradePlanCreated/Revoked/Expired/Superseded | PLANNED_M3 | Architect |
| ExecutionReport/PositionDirective | DEFERRED | separate execution stage |

Требования к intent: instrument/spec version и side, stable IDs, evidence/config/version refs, fresh source epochs, causal as-of, TTL, inclusive entry bounds, protective invalidation и объективные targets. Runtime plan не содержит аккаунтный баланс; exact sizing выполняет будущий RiskGovernor. Значения price/qty без float. Не путать входной min-RR threshold с рассчитанным RR при реальном fill.

После freeze изменение — отдельный ADR/spec revision + compatibility tests. Предложенный контракт не должен выдаваться за компилируемый Rust struct или гарантированно безопасную схему.
