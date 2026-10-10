# Definition of Done

Применять [WORKFLOW](WORKFLOW.md) и его условия активации. Начало реализации, готовность PR и milestone acceptance — разные решения.

## Любая задача

- Scope/accepted contracts соблюдены; риск и причина записаны в canonical packet. Один автор/workspace, shared owner и authority известны.
- Результат сохранён в GitHub; Issue содержит canonical pointer, PR — Handoff со START/HEAD/TREE/TARGET и средой/clean status. MERGE_SHA только фактический, если проверен.
- Критерии связаны с checks/evidence. Check-level PASS/FAIL/NOT_RUN/NOT_APPLICABLE (с причиной) отделены от общего verdict PASS/FAIL/BLOCKED.
- После integration edits зафиксированы IMPLEMENTATION_COMPLETE/SOURCE_FROZEN/exact candidate. LOW: diff и подходящие checks; STANDARD: Worker и свежий independent targeted review; STRICT: independent QA и все accepted gates. Все обязательные CI сохраняются, включая для LOW. Новый commit получает новый итог по риску; reuse evidence обоснован.
- Самопроверка автора обозначена отдельно. Review/QA идёт в свежем контексте без inherited Worker transcript и на чистом закреплённом snapshot/известной среде. QA не пишет delivery branch; временные reproductions в своей копии описаны отдельно. Finding может иметь точное source evidence при runtime NOT_RUN.
- FAIL возвращается прежнему Worker по умолчанию через bounded packet Integrator, same Issue/branch/PR; два цикла той же ошибочной гипотезы требуют смены подхода. BLOCKED имеет cause/action/resume condition.
- Нет секретов/private sources/raw archives; limitations/unknowns и evidence refs явны. Исторические receipts не переписаны.
- Перед merge actual HEAD/TARGET, required checks/up-to-date protection и findings перепроверены; есть отдельное owner решение. DONE — подтверждённый результат в main с required integration checks, не Draft/CI PASS. ADR/milestone authority остаётся отдельной.
- Checkpoint и один следующий разрешённый шаг сохранены. Полный уникальный packet/copy-ready prompt нужен для реальной передачи роли; микродействия не дублируют его. Spawn по risk route при доступном orchestration; иначе честный fallback. Новая задача — только в границах bounded authority после принятия/явного откладывания текущей.

## Docs-only

Проверить scope, ссылки, статусы, provenance и смысловую согласованность сценариев. Только опечатка/несемантическое оформление может быть LOW. Governance/specs/contracts/security и изменение gate исключены из LOW; эта ревизия #48/PR49 требует independent documentary QA и existing CI. Runtime локально обычно NOT_APPLICABLE с причиной; реально выполненный CI указать отдельно. Source/semantic policy/архитектурные решения требуют принятия по authority, даже без Rust diff.

## Rust code

Реальные fmt/Clippy/tests на pinned toolchain, Cargo.lock и negative tests по scope. Без скрытых unbounded queues, unchecked numeric conversions и panic на внешних данных. Test-only unwrap допустим с понятным failure. Source/feature/allocator/transport probes выполняются в implementation и закрываются до соответствующей приёмки, не до написания кода.

## Market data и стратегия

Market-data cases: snapshot/delta/gap/duplicate/reset/old epoch, overflow, receive order/replay, corrupted/truncated WAL и storage failure по scope. Реальный smoke отделён от synthetic tests; live NOT_RUN блокирует live acceptance.

Strategy cases: no-look-ahead, level revision/valid_from, episode IDs, UNKNOWN, expiry/revocation, conflict/duplicates и provenance thresholds. CI не доказывает прибыльность.

## Milestone, performance и передача

Milestone требует завершённого интеграционного контура, независимого QA и owner acceptance. Диагностический capture/synthetic engine не заменяет canonical applicability/proofs/recovery.

Performance claim: machine/toolchain/dataset/config, warm-up/sample size, p50/p95/p99/queue age; без жёсткого latency gate на непостоянном CI runner без принятого protocol.

Отдельный Handoff нужен для прерванной/сложной работы, milestone или прямого deliverable packet; обычный PR report достаточен. Не дублировать одинаковые receipts в нескольких местах и не переписывать историю.
