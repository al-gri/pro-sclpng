# Roadmap и порядок зависимостей

Не календарь обещаний: переход определяется доказательствами, а не количеством чатов.

| Milestone | Результат | Gate |
|---|---|---|
| M0 | repo governance, workspace/CI, минимальные domain/feed/WAL contracts | bootstrap принят, CI реально зелёный, specs reviewed |
| M1 | recorder + books50/trades + replay на малой выборке | gaps обнаруживаются, replay совпадает, raw provenance сохранена |
| M2 | horizontal levels + valid_from + approach episodes | positive/negative/no-look-ahead golden tests |
| M3 | TrueBreakout/FalseBreakout + arbiter + TradePlan lifecycle | deterministic plans, stale/duplicate/conflict tests |
| M4 | shadow radar, UI/alerts, warm/hot ranking | наблюдения/метрики, не доказанная доходность |
| M5 | labelled episodes и ограниченная calibration | out-of-sample/reproducibility; explicit parameter budget |
| M6 | demo/paper execution (отдельное разрешение) | account risk, fills, reconciliation, stop/kill policies |
| M7 | ограниченный live (отдельное разрешение) | независимый release review |

## Созданные задачи

- [GOV-001 #1](https://github.com/al-gri/pro-sclpng/issues/1): принять bootstrap.
- [BOOT-001 #2](https://github.com/al-gri/pro-sclpng/issues/2): первый код workspace/CI.
- [SPEC-001 #3](https://github.com/al-gri/pro-sclpng/issues/3): domain/event/WAL/DataHealth.
- [MD-001 #4](https://github.com/al-gri/pro-sclpng/issues/4): официальная feed specification и fixtures.
- [REC-001 #5](https://github.com/al-gri/pro-sclpng/issues/5): M1 integration-эпик.
- [QA-001 #6](https://github.com/al-gri/pro-sclpng/issues/6): независимая проверка конкретных SHA.
- [RULE-001 #7](https://github.com/al-gri/pro-sclpng/issues/7): source validation и минимальный rule registry.

## Порядок сейчас

```text
GOV-001 → BOOT-001 → domain implementation
       ↘ SPEC-001 ────────────────┐
       ↘ MD-001 ─────────────────┼→ REC-001 → QA M1 gate
       ↘ QA design ──────────────┘
       ↘ RULE-001 → M2/M3 (не блокирует чистый recorder)
```

После BOOT-001 допускаются максимум два активных implementation PR с непересекающимися файлами. Specs/QA чтение могут идти параллельно. Общие workspace/schema-файлы меняет только Integrator. REC-001 разбить на decoder/book, WAL/replay, WS supervisor, integration; не запускать их вслепую до contracts.
