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

## Текущая последовательность M1

Одна активная delivery task/implementation executor/основная ветка/PR и один автор workspace по [WORKFLOW](WORKFLOW.md); заменяемый Integrator организует маршрут LOW/STANDARD/STRICT. Для #45 и M1 обязательные independent QA gates сохраняются. Backlog хранится в GitHub Issues; этот файл описывает checkpoints, не копирует оперативные назначения.

| Очередь | Проверяемый результат |
|---|---|
| 1 | PR #43 принят. Governance GOV-ORCH-001 активируется после owner acceptance, merged PR49 и canonical receipt #48. Затем по authority/actual refs — independent Architecture/source re-review existing corrected E1 #45; не Worker restart и не full final QA |
| 2 | #45: real bounded capture → WAL → два diagnostic replay; probes внутри implementation |
| 3 | Независимый QA критического #45 и owner acceptance его ограниченного scope |
| 4 | Отдельный bounded book-engine child #21 на accepted types и synthetic metadata/fixtures |
| 5 | Конкретное принятое Bitget normalization/applicability решение и его реализация |
| 6 | Canonical book + artifacts/proofs + recovery + replay integration |
| 7 | Independent integrated QA и owner acceptance полного M1 |

Checkpoint 2 не даёт usable_data и не закрывает M1. Engine не конвертирует реальные неизвестные quantities или source zero в canonical effects. #46 сохраняет U09/U10/snapshot-zero/profile blockers; принятие его proposed fail-closed policy само по себе не разрешает normalizer. Публичный API/ADR и numerical budget изменения требуют отдельного принятия.

M2 [#29](https://github.com/al-gri/pro-sclpng/issues/29) остаётся BLOCKED до M1 acceptance. RULE-001 [#7](https://github.com/al-gri/pro-sclpng/issues/7) выполняется только как одна явно выбранная задача с предоставленными источниками; параллельного research/implementation нет.

Если текущая задача blocked, Integrator сохраняет resume packet и явно откладывает её до выбора одной замены. Shared Cargo.lock/workspace/CI интегрируются последовательно. Integrator сохраняет canonical checkpoint и следующий разрешённый шаг; полный уникальный versioned packet/copy-ready prompt нужен при фактической смене роли, для микродействий достаточно delta/ссылки. Исполнение следующей задачи требует bounded authority; текущая governance-ревизия #45 не запускает.
