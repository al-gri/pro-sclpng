## Проблема и результат

Issue / Task ID:
Base SHA:
Current head / tested refs:
Risk / author / reviewer:
Governance/workflow: [WORKFLOW](../docs/WORKFLOW.md)

## Scope / contracts

Changed modules; applicable accepted contracts/ADR; scope deviations.

## Проверки и evidence

| Check | PASS/FAIL/NOT_RUN/NOT_APPLICABLE | Executor / exit / CI or evidence URL |
|---|---|---|

Самопроверка автора, independent QA и CI указываются отдельно. QA требуется по риску, а не для каждого микрошагa. Runtime/live evidence привязан к actual refs; new runtime bytes требуют affected checks. Docs-only не придумывает runtime PASS.

## Ограничения / blockers

Real/synthetic provenance, неизвестные outcomes, отсутствующее обязательное evidence.
Large raw archives вне public Git; здесь manifest/digest/reference.

## Handoff и следующий один шаг

Этот PR report — основной Handoff. Отдельный файл только при необходимой сложной/прерванной передаче или прямом packet deliverable.
Integrator автоматически выдаёт copy-ready prompt/packet следующей нужной роли: refs/branch/executor/scope/acceptance/stops/result. Ссылка на сохранённый GitHub packet:

## Перед merge

- [ ] Scope и contracts соблюдены; секретов/private sources/raw archives нет.
- [ ] Обязательные checks реальны; отсутствующие проверки отмечены.
- [ ] Требуемый по риску review/QA относится к current head.
- [ ] Следующий шаг и нужные transfer materials подготовлены без просьбы владельца.
- [ ] Владелец разрешил merge; auto-merge не включено.
