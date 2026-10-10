# Definition of Done

Применять [последовательный WORKFLOW](WORKFLOW.md). Начало реализации, готовность PR и принятие milestone — разные решения.

## Любая задача

- Scope соблюдён; отсутствуют неутверждённые изменения contracts/семантики.
- Код/документ и результат сохранены в GitHub. PR report содержит actual base/head и выполняет роль Handoff.
- Применимые checks имеют PASS/FAIL/NOT_RUN, executor и exit code/log/CI reference. NOT_APPLICABLE допустим с причиной для проверки вне scope.
- IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / exact FINAL_SHA/TREE сохранены после Integrator review/shared edits. Final independent QA PASS и требуемые review/CI относятся к этому candidate; любой новый source commit требует нового review/freeze/QA. Применимость прошлого специального evidence указана явно.
- Отдельный ephemeral QA после freeze возвращает PASS/FAIL/BLOCKED; documentary scope не выдумывает runtime tests. Риск определён по последствиям; критические boundaries сохраняют дополнительные independent review/negative gates. Самопроверка автора отмечается отдельно. QA FAIL исправляет один Worker по corrective packet Integrator, same Issue/branch/PR; QA source не пишет.
- Нет секретов, приватных источников и больших raw archives; limitations/blockers/evidence refs явны.
- Владелец принимает merge. DONE не означает только Draft PR или зелёный CI.
- Integrator автоматически сохраняет один следующий bounded packet/full prompt и при доступном orchestration запускает Worker/QA последовательно. При unavailable spawn честный fallback. Worker/QA result содержит exact refs/tree/checks/preserved unknowns/Handoff; их contexts disposable, GitHub — durable context. Owner/Architecture принятие ADR/milestone и merge остаются отдельными.

## Docs-only

Проверить scope, ссылки, статусы, provenance и отсутствие противоречий действующим правилам. Runtime локально обычно NOT_APPLICABLE; если CI реально запустил Rust, указать его отдельно и честно. Новые source/semantic policies и архитектурные решения требуют содержательного независимого review/owner acceptance по риску.

## Rust code

Реальные fmt/Clippy/tests на pinned toolchain, Cargo.lock и negative tests по scope. Без скрытых unbounded queues, unchecked numeric conversions и panic на внешних данных. Test-only unwrap допустим с понятным failure. Source/feature/allocator/transport probes выполняются в implementation и закрываются до соответствующей приёмки, не до написания кода.

## Market data и стратегия

Market-data cases: snapshot/delta/gap/duplicate/reset/old epoch, overflow, receive order/replay, corrupted/truncated WAL и storage failure по scope. Реальный smoke отделён от synthetic tests; live NOT_RUN блокирует live acceptance.

Strategy cases: no-look-ahead, level revision/valid_from, episode IDs, UNKNOWN, expiry/revocation, conflict/duplicates и provenance thresholds. CI не доказывает прибыльность.

## Milestone, performance и передача

Milestone требует завершённого интеграционного контура, независимого QA и owner acceptance. Диагностический capture/synthetic engine не заменяет canonical applicability/proofs/recovery.

Performance claim: machine/toolchain/dataset/config, warm-up/sample size, p50/p95/p99/queue age; без жёсткого latency gate на непостоянном CI runner без принятого protocol.

Отдельный Handoff нужен для прерванной/сложной работы, milestone или прямого deliverable packet; обычный PR report достаточен. Не дублировать одинаковые receipts в нескольких местах и не переписывать историю.
