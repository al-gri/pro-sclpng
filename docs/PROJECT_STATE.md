# Project state

Обновлено: 2026-10-05. Состояние ниже относится к bootstrap, не к будущему завершению Issues.

## Baseline

- Repository: `al-gri/pro-sclpng`, public, default branch main.
- Initial seed: `62c225cf7e4dc28faa7d038f5e03eccab603a710` (только README).
- Bootstrap branch: `docs/GOV-001-project-bootstrap`.
- Baseline status: PROPOSED до owner merge; после merge приняты документы этой ревизии.
- Current phase: M0 — старт организации и интерфейсов.
- Executable code: NOT_IMPLEMENTED.
- CI: NOT_CONFIGURED.
- Branch/ruleset enforcement: OWNER_ACTION_REQUIRED, не включено этим PR.
- Rust/toolchain/OS: выбираются и проверяются в BOOT-001, не предполагаются.
- Live trading: OUT_OF_SCOPE / DISABLED.
- Imported course/matrix: NOT_IMPORTED, происхождение проверяется в RULE-001.

## Очередь

| ID | Issue | Состояние на bootstrap | Следующее действие |
|---|---|---|---|
| GOV-001 | #1 | IN_REVIEW | Владелец принимает bootstrap PR |
| BOOT-001 | #2 | READY_AFTER_GOV | Первый workspace/CI PR |
| SPEC-001 | #3 | READY_AFTER_GOV | Contracts proposal, затем код типов |
| MD-001 | #4 | READY_AFTER_GOV | Official feed verification/fixtures |
| REC-001 | #5 | BLOCKED_BY_2_3_4 | Integrator декомпозирует M1 |
| QA-001 | #6 | READY_AFTER_GOV | Spec review, затем тесты конкретных PR |
| RULE-001 | #7 | SOURCE_VALIDATION_REQUIRED | Проверить доступные источники |

## Управление

Владелец: al-gri. Главный чат — Architecture; один Lead/Integrator; workers краткоживущие. Чаты не могут считаться назначенными или запущенными от самого факта появления файла с промптом. В момент bootstrap новые рабочие чаты ещё должен создать владелец.

Не поддерживать второй ручной backlog здесь: динамические назначения/PR/блокеры смотреть в Issues. После milestone Integrator обновляет этот файл и фиксирует проверенный release SHA; не вписывать SHA будущего коммита в самого себя.

## Следующая практическая поставка

BOOT-001: минимальный workspace и CI. Функциональный milestone M1: записать и воспроизвести небольшой поток без скрытых gaps. Прибыльность, latency и покрытие всех правил не оценивались.
