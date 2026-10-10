# Последовательная разработка ProScalping

## Решение и область действия

Последовательная база принята в main через PR #43. Эта редакция GOV-ORCH-001 действует после owner acceptance, merge [PR #49](https://github.com/al-gri/pro-sclpng/pull/49) и канонического acceptance receipt в [Issue #48](https://github.com/al-gri/pro-sclpng/issues/48). До выполнения условий действует принятая база main и явно разрешённый scope [ревизии](https://github.com/al-gri/pro-sclpng/issues/48#issuecomment-6102286926). Текст в ветке не означает принятие. После merge проверять receipt/actual refs, не трактовать датированный pending snapshot как вечный статус. Исторические Issues/packets/handoffs/receipts сохраняются.

**Одна активная задача поставки, один implementation executor, один автор каждой рабочей копии, одна основная delivery branch и один PR.** Read-only исследование и проверочная копия закреплённого commit допустимы; это не вторая feature delivery. Два автора source, параллельные feature-задачи и запись поставляемой ветки во время review/QA запрещены. Draft PR/backlog/tracking Issues не являются активными задачами.

Короткий путь: [AGENTS](../AGENTS.md) → канонический указатель текущего Issue → актуальный packet/PR и нужные разделы WORKFLOW. [PROJECT_STATE](PROJECT_STATE.md) — карта проекта. ARCHITECTURE, INVARIANTS, specs/ADR и capability reports читать по scope, не всю историю на каждом шаге. Для нового технического scope затронутые принятые контракты и инварианты обязательны. Пользовательский маршрут — [DAILY_WORKFLOW](DAILY_WORKFLOW.md).

## Роли и владение

| Роль | Ответственность |
|---|---|
| Владелец al-gri | Приоритеты, существенный scope, политики, merge и milestone |
| Architect / Architecture | Контракты, ADR, инварианты и межмодульные решения; требуемое отдельное принятие |
| Integrator | Одна задача, packet, назначение автора, запуск ролей, scope/integration review, freeze и receipt |
| Один Worker | Bounded implementation и авторские проверки в принятом scope |
| Независимый Reviewer / QA | Целевой review STANDARD либо обязательный QA STRICT на exact candidate |

Постоянна роль Integrator, а не чат. Каждый исполнитель заменяем после сохранения checkpoint в GitHub. Самопроверка автора не является независимым review; два чата под одним GitHub login не создают две approving identities.

### Integrator как автор и shared files

Integrator может выполнить LOW либо документированное исключение: INTEGRATION_ONLY, shared integration glue, конфликт после Worker, governance/project-state/task-packet, root Cargo/workspace/lock/CI или обоснованный минимальный emergency fix. До записи packet содержит `EXECUTOR_EXCEPTION=INTEGRATOR`, конкретные `REASON`, `SCOPE` (exact paths), `WORKSPACE/WRITER`. Малый размер feature/runtime diff не делает его LOW или исключением. Обычные features выполняет Worker. Перед integration edits Worker прекращает запись и передаёт владение; авторство Integrator не отменяет gates STANDARD/STRICT.

Для root Cargo.toml/Cargo.lock/workspace/CI и общих файлов packet явно назначает одного `SHARED_FILES_OWNER` и paths; по умолчанию это Integrator. Worker может получить их только явным bounded packet после передачи существующего claim. Отсутствие нового назначения не отменяет текущего владельца. Concurrent edits запрещены; lock генерируется закреплённым Cargo. Существующий claim #45 и его ограничения эта ревизия не меняет.

## GitHub — каноническая память

| Место | Что хранится |
|---|---|
| main и accepted specs/ADR | Принятый код и контракты |
| Issue | Один канонический указатель: версия packet, последний авторизованный переход, автор/workspace, candidate, blockers и следующий шаг |
| PR | Результат/diff, head, checks/review/QA, Handoff и acceptance receipts |
| PROJECT_STATE | Карта milestone, датированные snapshots и ссылки |

Issue ссылается на PR/result вместо копии полного отчёта; PR — на указатель Issue. Последний комментарий не обязательно является последним авторизованным решением. Owner decisions имеют дату, scope и статус. Proposed/Draft, CI PASS и наблюдаемый payload не равны accepted contract/milestone. Уникальных решений только в чате быть не должно.

Процесс не меняет цены/quantity, Timer A, ownership/completion, WAL, proof/applicability, UNKNOWN или usability. Публичный контракт требует отдельно принятого решения/ADR; зависимый код до этого остановлен. Raw archives остаются вне публичного Git; в GitHub — manifest/digest и доступная проверяющему evidence-ссылка. Секреты и приватные источники не публикуются.

## Review по риску

Уровень и причина записываются в packet по последствиям ошибки. Принятые task-specific, technical/M1 gates и существующие обязательные CI имеют приоритет; общая классификация их не снижает.

| Уровень | Scope | Минимальный маршрут |
|---|---|---|
| LOW | Только опечатка или несемантическое оформление без изменения поведения/требований | Один назначенный автор → просмотр diff и подходящая лёгкая проверка; обязательный CI сохраняется; отдельный review только если его требует действующий gate |
| STANDARD | Ограниченное изменение поведения внутри принятых контрактов, без STRICT boundary | Worker → авторские checks → Integrator → свежий независимый целевой review frozen candidate → применимые CI |
| STRICT | Governance, specs/контракты/инварианты, security, публичный API, concurrency/unsafe, сохранность данных, миграция, сложная FSM; sequence/epochs, WAL/recovery, quantity/delete, ownership, transport/TLS, критические лимиты, milestone | Принятое решение где требуется → Worker либо допустимое exception → integration review → независимый QA и все принятые дополнительные gates |

Governance, specs, contracts, security и изменения статуса gate **никогда не LOW, даже docs-only**. Если отсутствие семантического влияния не доказано, LOW не выбирать. Эта ревизия #48/PR49 проходит independent documentary QA и существующий CI; #45 и M1 остаются STRICT. Architecture/owner acceptance контрактов, ADR и milestone не заменяются review/QA.

STANDARD reviewer проверяет затронутое поведение, негативные случаи и evidence, а не повторяет механически весь CI. STRICT packet перечисляет обязательные независимые запуски и специальные gates. Docs-only правка не требует нового live capture при неизменных runtime/profile/inputs/assumptions; применимость старого evidence объясняется. Это не перенос PASS на новые runtime bytes или неизвестную среду.

## Один цикл и пять стадий

1. Integrator сверяет actual refs, scope/authority, блокеры и автора; сохраняет один версионированный packet и выбранный риск.
2. Один Worker (или документированный Integrator exception) подтверждает START_SHA/checkout/environment, реализует scope, выполняет авторские checks и передаёт результат; запись прекращается.
3. Integrator проверяет diff/contracts/paths/shared ownership, последовательно завершает integration edits, фиксирует candidate и freeze.
4. LOW получает назначенную лёгкую проверку; STANDARD — отдельный свежий целевой review; STRICT — независимый QA с обязательными gates. Branch frozen до результата или явной отмены freeze.
5. FAIL → bounded correction прежнему Worker по умолчанию, same Issue/branch/PR → новый candidate/review/freeze и проверка по риску. BLOCKED → причина, ответственный и resume condition.
6. Подтверждённый результат → integration receipt → отдельное owner решение о merge. Перед merge проверить actual HEAD, target, findings, required checks и authority; после merge — actual main/MERGE_SHA и обязательные integration checks.
7. Сохранить checkpoint и подготовить один следующий шаг. Следующую задачу выполнять только в пределах заданной bounded очереди/полномочий и после принятия либо явного откладывания текущей.

| Стадия для владельца | Существующие детальные события |
|---|---|
| READY | READY_FOR_WORKER; готовый packet для LOW/exception |
| IMPLEMENTING | WORKER_ACTIVE, IMPLEMENTATION_READY, INTEGRATOR_REVIEW; QA_FAILED → CORRECTION_READY перед повторной реализацией |
| VERIFYING | SOURCE_FROZEN, QA_ACTIVE; тип STANDARD — TARGETED_REVIEW, LOW — AUTHOR_CHECK |
| READY_TO_MERGE | QA_PASSED для независимого review/QA, OWNER_ACCEPTANCE после gates; LOW хранит check receipt без выдуманного QA |
| DONE | ACCEPTED_IN_MAIN после проверки результата и required integration checks |

BLOCKED/PAUSED содержат причину, ответственного, retained refs и resume condition; детальные BLOCKED_* сохраняются. BLOCKED_QA не равен QA_FAILED. M1 IN_PROGRESS — aggregate milestone. Исторические labels/receipts и restrictions на закрытие tracking Issues не переименовываются. Для LOW/exception ненужные роли пропускаются явно; запуск Worker/QA не выдумывается.

## Идентичность кандидата и freeze

```text
START_SHA = состояние начала работы исполнителя (historical BASE_SHA может быть alias)
HEAD_SHA = полный SHA проверяемого кандидата (FINAL_SHA в прежних receipts)
TREE = tree кандидата
TARGET_BRANCH / TARGET_SHA = целевая ветка и проверенное состояние совместимости
MERGE_SHA = проверенный результат объединения, если создан; иначе NOT_CREATED
```

Не смешивать START_SHA с merge-base, HEAD с target или GitHub test merge SHA. Review/QA получает HEAD/TARGET, чистый checkout HEAD_SHA либо проверенный read-only API snapshot и известную среду: OS/toolchain/lock/features/существенные flags. API source review не доказывает runtime checks. SHA не описывает dirty files или внешнюю среду.

После разрешённой реализации Integrator публикует IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / exact HEAD_SHA (FINAL_SHA) / TREE / TARGET_SHA, критерии и Handoff. SOURCE_FROZEN — запись, не техническая блокировка: авторы действительно остановлены, проверка читает закреплённую копию. Refs сверяются перед verdict и merge. Никто не меняет поставляемую ветку до конца проверки либо явной отмены freeze.

Любой новый source commit, включая docs/lock/CI, требует нового candidate, review/freeze и нового итога по риску. Старый verdict historical/superseded для нового SHA; evidence переиспользуется только с applicability и без пропуска gates. Head/tree drift → BLOCKED_SOURCE_CHANGED. Изменение main требует анализа diff/совместимости и affected integration checks; само по себе не требует ритуального rebase. Required up-to-date checks и реальные repository protections исполняются. Force-push без отдельного разрешения запрещён.

Commit не может содержать собственный окончательный SHA без изменения этого SHA. Сначала фиксируются source/документы, затем receipt публикуется в Issue/PR comment или CI. Handoff-файл в branch описывает предшествующий проверенный commit; содержащий его новый commit получает собственный итог. After-freeze receipts не создают source commit.

Architecture/source review isolated accepted scope — отдельный тип проверки. Он не означает IMPLEMENTATION_COMPLETE всего PR и не заменяет final implementation QA. Scoped freeze всегда называет границы проверки.

## Независимая проверка и corrections

Reviewer/QA не автор candidate; получает критерии/contracts/scope, самостоятельно читает source и опасные сценарии, затем сверяет Handoff автора. Для STANDARD и STRICT нужен свежий контекст без inherited Worker transcript. В текущем доступном механизме spawn_agent это `fork_turns="none"`; в иной среде проверить её механизм. Новый агент сам по себе не гарантирует чистую историю или filesystem isolation. Если fresh/independence gate нельзя обеспечить, раскрыть ограничение и вернуть BLOCKED.

QA не исправляет поставляемую ветку. В своей изолированной проверочной копии допустимы временные tests/reproductions: записать исходный clean HEAD, состав временных файлов и результаты отдельно от candidate. Нужный продукту тест переносит назначенный автор, затем проверяется новый head. Shared workspace не является изоляцией; нужны закреплённая копия и учёт shared ports/services/caches.

| Verdict | Значение |
|---|---|
| PASS | Все обязательные критерии данного verification scope доказаны, blocking findings отсутствуют |
| FAIL | Подтверждено нарушение критерия/инварианта |
| BLOCKED | Обязательное evidence/решение/доступ отсутствует либо source изменился |

Check-level PASS/FAIL/NOT_RUN/NOT_APPLICABLE (последнее с причиной) отделены от verdict PASS/FAIL/BLOCKED. Отсутствие найденных дефектов не доказывает непроверенный критерий. Подтверждённый defect даёт FAIL даже при других NOT_RUN, которые также раскрываются.

Finding содержит ID, критерий, path/сценарий, ожидаемое/фактическое, severity/влияние и воспроизведение **либо точное source evidence/контрпример**. Runtime NOT_RUN раскрывается и не обесценивает доказанный source defect. Рекомендации отделены от blockers; стилистическое пожелание само по себе не запускает correction.

Для локальных findings по умолчанию возобновляется прежний Worker. После двух циклов с той же ошибочной гипотезой Integrator меняет подход: уточняет контракт, назначает другого исполнителя либо передаёт конкретный спор Architect. QA завершается до возобновления автора; same Issue/branch/PR — default.

## Packet, checkpoint и передача

Packet может быть секцией Issue или версионированным comment. Полный уникальный bounded packet для **каждой фактической смены роли** сохраняется в GitHub с PACKET_VERSION, ссылками и copy-ready заданием; историю чата не переносить. Для микродействий внутри той же роли достаточно status/evidence delta и ссылки — полный prompt не дублировать.

Минимум packet: TASK/Issue/PR, PACKET_VERSION, GOAL, ACCEPTANCE с ID, SCOPE/files_allowed/NON_GOALS, accepted CONTRACTS, START_SHA, TARGET_BRANCH/TARGET_SHA, BRANCH, WORKSPACE/WRITER/SHARED_FILES_OWNER, ENVIRONMENT, RISK/REVIEW_TYPE/gates, CHECKS (критерий → процедура → ожидаемый результат), AUTHORITY, STOP_CONDITIONS, HANDOFF_TO. Review/QA дополнительно получает HEAD_SHA/TREE/freeze и авторский result. Критерии не заменять голым списком команд.

Worker result: TASK/PACKET_VERSION, START_SHA, HEAD_SHA/TREE, TARGET_SHA, PR, CHANGED_PATHS, CHECKS/evidence/exit codes, dirty files либо clean status, limitations/KNOWN_UNKNOWNS_PRESERVED, HANDOFF_PATH. PR report достаточен. Отдельный [Handoff](templates/HANDOFF.md) нужен для прерывания/сложной передачи/milestone или explicit packet deliverable.

Recovery checkpoint в каноническом Issue: последний авторизованный переход/receipt, packet version/authority, actual branch/START/HEAD/TREE/TARGET, writer/workspace/agent status, freeze, завершённые критерии/evidence, findings/blockers, retained patch/dirty files и **один следующий разрешённый шаг**. Сохранять при смене роли, stop/resume, завершении и замене Integrator. PROJECT_STATE обновлять при milestone/существенном решении/главном blocker, не после микродействий.

Новый Integrator читает указатель Issue, AGENTS и нужные rules/inputs; сверяет refs, dirty files, фактических авторов и authority. Не запускает завершённый шаг из старого prompt. Конфликт решает по authority/evidence, не по «самому новому тексту» или памяти чата.

## Начать, слить PR и принять milestone

| Решение | Достаточные условия |
|---|---|
| Начать | Понятный scope, доступные входные контракты, единственный исполнитель и фактический способ сборки |
| Готовность к merge | Реализация, применимые tests/CI и требуемый review/QA |
| Принятие milestone | Законченный интеграционный контур, независимый QA и решение владельца |

Dependency/features/MSRV, handshake/partial-write, allocator и shutdown probes являются первой частью implementation. Разрешены экспериментальный код и ветка до их PASS; соответствующее поведение не принимается и не активируется до закрытия обязательных проверок. Зависимый код на непринятом публичном контракте/ADR остаётся заблокированным.

Недоступный live endpoint блокирует live acceptance, но разрешённая offline-часть той же задачи может продолжаться. Не скрывать FAIL/NOT_RUN и не называть synthetic fixture реальным capture. Численные бюджеты #45, включая 8 MiB и ограничения TLS, сохраняются до отдельного утверждённого изменения; RSS/container limit не доказывает pre-allocation bound.

## Среда и блокировки

Среда настраивается один раз с командами/identity. Полный preflight повторять при смене executor/toolchain/access; refs/checkout/ownership — при start/resume/передаче/freeze/merge. Внутри этапа проверять изменившиеся предпосылки. Не переносить PASS между неизвестными средами/caches. Новые dependencies требуют verified versions/features/provenance, Cargo-generated lock и clean-runner fetch --locked либо verified vendoring до offline checks.

При blocker сохранить причину, завершённую часть, branch/commit/patch, владельца действия и resume condition. Для другой delivery задачи текущую явно отложить, остановить автора и выдать один новый packet. Параллельная feature delivery не разрешена.

## Автоматическая подготовка следующего шага и передачи

Integrator сам сохраняет следующий шаг и выполняет уже разрешённые обратимые действия. При доступном orchestration запускает одного Worker и затем требуемого Reviewer/QA последовательно по риску. Следит за actual identities/statuses, ownership/freeze. Read-only Researcher/Architecture reviewer не становится вторым автором.

Если spawn действительно недоступен, сохранить полный уникальный packet/copy-ready prompt и честно указать NEXT_EXECUTOR=WORKER (либо REVIEWER/QA), AUTOSPAWN=UNAVAILABLE_IN_CURRENT_ENVIRONMENT. Подготовленный prompt не означает запущенного агента. Orchestration не доказывает shell/Rust/network/live access; missing mandatory evidence → BLOCKED.

Подзадачи текущего запуска выполнять subagents. Новый пользовательский чат создаётся только по явному запросу пользователя; отправка сообщения другому пользовательскому чату требует явной пользовательской авторизации. Наличие инструмента/соседнего чата не выдаёт такое право. Внутренний subagent и отдельный пользовательский чат — разные механизмы.

## Полномочия и продолжение

Один раз определить bounded задачу/очередь, разрешённые edit/check/commit/push/Draft PR/report действия, архитектурную authority и stops. Уже выданное разрешение повторно не запрашивать. Новый scope/непринятый контракт/действие вне мандата требует конкретного решения после подготовки reviewable результата.

Merge остаётся отдельным решением владельца для конкретного готового candidate. Процесс не разрешает будущий automatic merge/auto-merge, force-push, settings/access/billing/secrets, private API или live execution. QA PASS/receipt сами по себе merge не разрешают. Restrictions на закрытие #37/#22/#5 сохраняются.

Обычный активный запуск, Goal и расписание различны. Goal создаётся только по явному запросу для измеримого результата и не расширяет authority. Возврат позже/мониторинг требует отдельно запрошенного доступного расписания; локальному исполнению нужны включённый компьютер, работающее приложение и доступный проект. Не обещать работу после активного запуска без механизма продолжения. Монитор сообщает о существенном изменении/готовности/сбое/необходимом решении, не о каждом неизменном опросе.

## Миграция текущей #45 / PR #47

[Сохранённый packet и revision appendix](task-packets/GOV-ORCH-001.md) и указатель [#48](https://github.com/al-gri/pro-sclpng/issues/48) фиксируют переход. Snapshot 2026-10-10: #45 приостановлен для governance; existing branch/Draft PR/claim/source/history сохранены. E1-CORR-01 доставлен на `4f599f5ab687ca1fddb43c2dd8a814e3cd70ad5a`; не выполнять его снова по старому prompt.

После governance acceptance/merge и сверки authority/refs следующий технический шаг — sequential independent Architecture/source re-review **existing corrected E1** в accepted isolated scope. Это не запуск Worker и не full final QA; данная governance-ревизия #45 не возобновляет. Читать действующую governance policy из actual main и receipt #48/#49, даже если сохранённая #45 branch содержит старые process docs. Не merge/rebase #45 ради обновления инструкций перед pending review; runtime/spec inputs проверять на exact #45 head с учётом отдельно принятых решений.

При новых findings — один corrective Worker same #45/branch/PR по новому packet. Full implementation QA только после полной разрешённой реализации и нового exact freeze. ADR0004 PROPOSED, U1–U7 UNRESOLVED, production FAIL/NOT_PROVEN, M1 unaccepted и все technical stops сохраняются.

## Последовательные результаты M1

1. Работающий build executor и implementation-integrated probes #45.
2. Реальный bounded capture → WAL → диагностический replay.
3. Независимый QA критического PR и owner acceptance его ограниченного scope.
4. Отдельный book engine на принятых типах и явно synthetic metadata/fixtures.
5. Отдельное принятое решение по Bitget normalizer/applicability и его реализация.
6. Интеграция canonical book, artifacts/proofs, recovery и replay.
7. Итоговый QA и owner acceptance полного M1.

Integrator оформляет engine как отдельный bounded child #21 только при достижении этого шага. Engine не нормализует реальные Bitget quantities, не создаёт production proofs и не устанавливает usable_data. U-09/U-10/snapshot-zero и profile applicability блокируют live canonical portion, а не исследование алгоритма на принятых нормализованных inputs. Если принятый API не позволяет выбранный engine boundary, требуется ADR до зависимого кода.

Policy в PR #46 сохраняет blocker; её принятие не разрешает normalizer. Изменение семантики требует конкретного evidence/policy решения. Diagnostic capture и synthetic book tests не закрывают M1. M2 #29 остаётся BLOCKED до отдельного принятия M1; RULE-001 выполняется только как выбранная единственная задача при наличии источников.
