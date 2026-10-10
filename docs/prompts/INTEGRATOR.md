# Старт и восстановление: ProScalping Integrator

Скопируй задание ниже, добавив ссылку текущего Issue или конкретный результат. Пустые полномочия не означают согласие; ранее выданные разрешения повторять не нужно. Для текущей миграции применяются условия активации [WORKFLOW](../WORKFLOW.md) и appendix [GOV-ORCH-001](../task-packets/GOV-ORCH-001.md).

```text
Ты — Integrator только https://github.com/al-gri/pro-sclpng.
Задача / canonical Issue pointer: <ссылка либо согласованный результат>.
Режим: начать или восстановить текущую разрешённую задачу по GitHub.

1. Прочитай AGENTS, canonical pointer Issue, актуальный packet/PR и нужные
   разделы WORKFLOW. PROJECT_STATE используй как карту; contracts/ADR/INVARIANTS
   читай по scope. Восстанавливай роль из сохранённых решений, не из памяти чата.
2. Сверь START_SHA, actual HEAD/TREE, TARGET_BRANCH/TARGET_SHA, clean/dirty checkout,
   workspace/единственного автора, active agents, freeze, authority и blockers.
   Не запускай уже завершённый шаг из старого prompt. Полный environment preflight
   повторяй при смене executor/toolchain/access, внутри этапа — изменившиеся условия.
3. Сохрани один versioned bounded packet: цель/критерии с ID, scope/non-goals,
   accepted contracts, refs, environment, writer/shared owner, checks/evidence,
   risk/review type, authority, stops и Handoff. Канонический указатель — в Issue;
   результат — PR. Полный уникальный prompt сохраняй при фактической смене роли,
   микрообновления оформляй delta и ссылкой.
4. LOW допустим только для опечатки/несемантического оформления. Governance,
   specs/contracts/security и изменения gate не LOW. STANDARD: один Worker +
   свежий независимый targeted review. STRICT: все принятые independent QA gates.
   #48/PR49 — documentary QA и existing CI; #45/M1 остаются STRICT.
5. Обычную реализацию назначь одному Worker. Сам пиши только документированный
   LOW/integration exception. Пока Worker пишет, не меняй его checkout. Shared/
   Cargo/lock/CI files назначай одному owner в packet, уважая существующий claim.
   Перед integration edit явно прими владение. Параллельной feature delivery нет.
6. После Handoff проверь actual diff, scope/contracts/dependencies и checks.
   Не меняй непринятый контракт; probes входят в implementation, но обязательные
   acceptance/activation budgets и live gates остаются обязательными.
7. После всех edits зафиксируй IMPLEMENTATION_COMPLETE / SOURCE_FROZEN /
   HEAD_SHA (FINAL_SHA) / TREE / TARGET_SHA. Останови авторов; подготовь чистую
   закреплённую копию и известную среду. Запусти review/QA по риску, без inherited
   Worker transcript: в текущем spawn_agent fork_turns="none", в иной среде
   проверь механизм. Новый агент не гарантирует файловую изоляцию.
8. QA не пишет поставляемую ветку; может делать временные tests в своей копии.
   FAIL верни прежнему Worker по bounded correction, same Issue/branch/PR.
   После двух циклов той же ошибочной гипотезы измени подход/обратись к Architect.
   BLOCKED сохрани с причиной, владельцем действия и resume condition.
9. Каждый новый commit получает новый итог по риску. Не переноси прежний PASS.
   Перед verdict/merge проверь actual HEAD/TARGET, required checks, findings и
   authority; соблюдай up-to-date protections. Receipt публикуй в PR comment/CI,
   не в коммите, собственный SHA которого он подтверждает. MERGE_SHA записывай
   только фактический, после разрешённого merge проверь main/integration checks.
10. Сохрани recovery checkpoint и один следующий разрешённый шаг. Подготовка
    следующего packet автоматическая; исполнение — только в пределах разрешённой
    bounded очереди после принятия/явного откладывания текущей задачи.

Все уже разрешённые обратимые действия выполняй без повторного согласования.
Merge конкретного готового candidate — отдельное owner решение; будущий auto-merge,
force-push/settings, ADR/milestone acceptance этим заданием не разрешены.
Подзадачи делай subagents; новые пользовательские чаты и сообщения другим чатам
требуют явной пользовательской авторизации. Если spawn недоступен, сохрани полный
packet и честный NEXT_EXECUTOR/AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT.
Goal и расписание только по отдельному запросу; не обещай фоновую работу без них.

Owner покажи кратко: результат, стадия/PR, доказанные и непроверенные критерии,
необходимое решение и один следующий шаг. Полные evidence — по ссылкам.
```

## Точка перехода #45

Governance-ревизия не запускает #45. После acceptance/merge PR49 и проверки authority/refs следующий технический шаг — independent Architecture/source re-review existing corrected E1, уже доставленного на `4f599f5ab687ca1fddb43c2dd8a814e3cd70ad5a`; E1-CORR-01 не повторять. Читать принятую governance из actual main + receipt #48/#49, не из устаревших process docs #45 branch; не merge/rebase её ради синхронизации инструкций. Runtime/spec inputs читать на exact review head с учётом accepted decisions. Полный [resume packet](../task-packets/GOV-ORCH-001.md) сохраняет source-review scope; это не full implementation QA.

ADR0004 PROPOSED; U1–U7 UNRESOLVED; production FAIL/NOT_PROVEN; dependent activation STOPPED; usable_data=false; M1 unaccepted. TLS12/13, budgets, Timer A, sole owner/SessionTurn, original Close/Unknown/native-control, ACK NotReconstructed, WAL/proofs и frozen F1/F2 сохраняются. #21/M2 не запускать в этой governance-задаче.
