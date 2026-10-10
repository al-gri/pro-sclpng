# Последовательная разработка ProScalping

## Решение и область действия

Последовательная база принята в main через PR #43. GOV-ORCH-001 ([#48](https://github.com/al-gri/pro-sclpng/issues/48)) меняет контракт ролей: Integrator управляет, ephemeral Worker реализует, отдельный ephemeral QA проверяет frozen source. Изменение процесса вступает в силу после owner acceptance/merge governance PR; до этого сохраняется принятая база main и явно разрешённый governance scope. Исторические Issues/PR/packets/handoffs/receipts не переписываются.

Обязательные инварианты: **ONE ACTIVE TASK / ONE ACTIVE IMPLEMENTATION EXECUTOR / ONE BRANCH / ONE PR**. Постоянный Integrator не является вторым implementation executor, пока только управляет и проверяет scope. Реализацию обычно выполняет ровно один ephemeral Worker. Integrator integration edits, независимый QA и corrections идут последовательно: два Worker, две implementation branches или запись source во время QA запрещены. Сохранённые Draft PR/backlog/tracking Issues не являются активными задачами.

## Роли

| Роль | Ответственность |
|---|---|
| Владелец al-gri | Приоритеты, существенный scope, принятые политики, merge и milestone |
| Architecture | Контракты, ADR, инварианты и межмодульные решения; отдельное требуемое принятие |
| Постоянный Integrator | Выбор одной задачи, Issue/Task Packet, запуск ролей, scope/integration review, shared edits, freeze, corrective packets и acceptance receipt |
| Один ephemeral Worker | Bounded implementation и developer checks; без расширения scope/контрактов, merge или milestone acceptance |
| Отдельный ephemeral QA | Независимая проверка exact frozen FINAL_SHA/TREE; PASS/FAIL/BLOCKED с findings; source не исправляет |

Integrator не погружается в feature implementation, если её можно безопасно делегировать. Он автоматически запускает одного Worker и затем отдельного QA, когда orchestration доступен. Контекст Worker/QA disposable после сохранения результата в GitHub. Самопроверка автора не является независимым QA; разные чаты под одним GitHub login не дают разных GitHub approving identities.

### Исключение: Integrator пишет сам

Допустимы только явно классифицированные INTEGRATION_ONLY, root Cargo.toml/Cargo.lock, CI/workflow, неделегируемый shared integration glue, conflict resolution после Worker, governance/project-state/task-packet changes либо отдельно обоснованный минимальный emergency fix. Перед записью зафиксировать в Issue/packet:

```text
EXECUTOR_EXCEPTION = INTEGRATOR
REASON = конкретная причина и классификация
SCOPE = exact allowed paths и действие
```

Integrator edits выполняются после остановки Worker и до freeze. Обычная feature/runtime задача не становится исключением из-за малого размера. Если сам Integrator был implementation author, final QA всё равно выполняет независимый исполнитель. Governance #48 использует явно записанное исключение, не разрешает Integrator снова стать feature executor.

## GitHub — единственный источник истины

| Место | Что хранится |
|---|---|
| main и принятые specs/ADR | Принятый код, контракты и архитектура |
| Issue | Scope, назначение, зависимости, состояние и blocker |
| PR | Конкретный результат, head, CI/review/QA и принятие |
| PROJECT_STATE | Краткий снимок milestone и ссылки на активную работу |

Новые owner decisions сохраняются в GitHub с датой, scope и явным статусом. Чат не переопределяет accepted contract без такой записи и требуемого ADR. Proposed policy, Draft PR, зелёный CI и наблюдаемый payload не равны принятому контракту или milestone.

В действующей задаче ссылки на accepted contracts и явно утверждённый scope обязательны. Процесс не меняет цены/quantity, Timer A, ownership/completion, WAL, proof/applicability, UNKNOWN или usability. Публичный контракт меняется только через отдельно принятое решение/ADR. Исторические packets/handoffs/receipts не переписываются.

Большие raw market archives остаются вне публичного Git; в GitHub сохраняются manifest/digest и проверяемая evidence-ссылка. Секреты не включаются в чаты или Git.

## Один цикл

1. Integrator проверяет фактический main, blockers/dependencies и accepted inputs; выбирает одну задачу, сохраняет Task Packet и полный Worker prompt в GitHub.
2. Запускается один Worker. Он подтверждает actual base/checkout/environment, выполняет bounded implementation и developer checks, сохраняет exact SHA/tree и Handoff, затем прекращает source edits.
3. Integrator сверяет base/head/tree, changed paths, scope, public-contract compatibility, shared workspace/Cargo.lock/CI. Integration-owned edits выполняет только он, последовательно и с объявленным scope.
4. После полной реализации Integrator публикует IMPLEMENTATION_COMPLETE, SOURCE_FROZEN, FINAL_SHA и TREE. На этом exact source запускается отдельный независимый QA.
5. QA возвращает PASS/FAIL/BLOCKED. FAIL -> Integrator готовит bounded corrective packet -> новый или возобновлённый Worker в том же Issue/branch/PR -> новый SHA -> review/freeze -> QA заново. QA сам не исправляет.
6. PASS -> Integrator сохраняет acceptance/integration receipt для exact SHA/tree -> owner acceptance/merge. Ни QA PASS, ни receipt не принимают ADR или milestone автоматически.
7. После принятия/merge Integrator сверяет actual main и scope, сохраняет состояние и автоматически готовит ровно одну следующую задачу. При blocker сначала явно откладывает текущую с resume packet; будущий Worker не запускается одновременно.

### Lifecycle

| Статус | Условие перехода / следующий исполнитель |
|---|---|
| READY_FOR_WORKER | Packet/inputs/base готовы; следующий Worker |
| WORKER_ACTIVE | Ровно один назначенный Worker пишет allowed source |
| IMPLEMENTATION_READY | Worker закончил, сохранил result/handoff; Integrator |
| INTEGRATOR_REVIEW | Scope/compatibility и последовательная shared integration |
| SOURCE_FROZEN | IMPLEMENTATION_COMPLETE и exact FINAL_SHA/TREE сохранены; source writes остановлены |
| QA_ACTIVE | Отдельный QA проверяет только frozen candidate |
| QA_FAILED | QA FAIL, findings сохранены; Integrator |
| CORRECTION_READY | Integrator сохранил bounded findings packet; один Worker в той же задаче |
| QA_PASSED | QA PASS на exact SHA/tree; Integrator receipt |
| OWNER_ACCEPTANCE | Требуемые review/QA/CI есть; решение владельца |
| ACCEPTED_IN_MAIN | Принятый scope в проверенном actual main; следующая задача может готовиться |

BLOCKED_* содержит конкретную причину, ответственного, сохранённые refs и resume condition. BLOCKED_QA не равен QA_FAILED и не оправдывает выдуманный PASS. BACKLOG допустим для неактивной работы. Не использовать общий IN_PROGRESS, если известна стадия. Старые статусы исторических receipts/packets сохраняются; M1 IN_PROGRESS остаётся aggregate milestone, а не статусом executor. Явные restrictions на закрытие tracking Issues сохраняются. Записанное Integrator exception может пропустить Worker stages, но не review/freeze/independent QA.

### Exact source freeze

QA packet требует IMPLEMENTATION_COMPLETE / SOURCE_FROZEN / FINAL_SHA / TREE / PR / acceptance criteria / invariants / Worker Handoff (либо явно объявленный Integrator exception result). QA сверяет server ref и локальный source/tree либо проверенный API snapshot. Никто не меняет implementation branch до окончания QA. Metadata/QA receipts публикуются в Issue/PR comments и не требуют нового source commit.

Любой новый source commit, включая docs/lock/CI/integration change, отменяет final QA acceptance для нового SHA. Integrator останавливает QA либо помечает его verdict superseded, выполняет необходимые affected checks, заново фиксирует exact SHA/tree и запускает QA. Прежний PASS не переносится автоматически. Допустимое прежнее runtime/live evidence можно сохранить как историческое с явной applicability; новый QA verdict всё равно привязан к новому candidate. Rebase/merge-base/source-tree changes также требуют сверки inputs и нового freeze.

Architecture/source review может быть отдельной последовательной стадией accepted isolated scope. Она не означает IMPLEMENTATION_COMPLETE всего PR и не заменяет final implementation QA. QA не заменяет требуемое Architecture/owner принятие контракта/ADR.

## Начать, слить PR и принять milestone

| Решение | Достаточные условия |
|---|---|
| Начать | Понятный scope, доступные входные контракты, единственный исполнитель и фактический способ сборки |
| Готовность к merge | Реализация, применимые tests/CI и требуемый review/QA |
| Принятие milestone | Законченный интеграционный контур, независимый QA и решение владельца |

Dependency/features/MSRV, handshake/partial-write, allocator и shutdown probes являются первой частью implementation. Разрешены экспериментальный код и ветка до их PASS; соответствующее поведение не принимается и не активируется до закрытия обязательных проверок. Зависимый код на непринятом публичном контракте/ADR остаётся заблокированным.

Недоступный live endpoint блокирует live acceptance, но разрешённая offline-часть той же задачи может продолжаться. Не скрывать FAIL/NOT_RUN и не называть synthetic fixture реальным capture. Численные бюджеты #45, включая 8 MiB и ограничения TLS, сохраняются до отдельного утверждённого изменения; RSS/container limit не доказывает pre-allocation bound.

## Review по риску

| Изменение | Проверка |
|---|---|
| Обычные docs/статус | Документальные проверки, Integrator scope review, отдельный final QA по documentary scope |
| Простая внутренняя реализация | Worker developer checks, tests/CI, Integrator review и отдельный final QA после freeze |
| Sequence/epochs, WAL/recovery, quantity/delete, ownership, transport/TLS, критические лимиты | Дополнительно независимый review и целевые QA-проверки конкретного PR |
| Контракты, инварианты, политики неизвестных данных, scope milestone | Architecture и принятие владельцем |
| Завершённый milestone | Независимый интеграционный QA и принятие владельцем |

Риск определяется последствиями дефекта, включая содержание документа. Source/semantic policy не становится обычным docs PR только из-за отсутствия Rust diff. Быстрые автоматические suites сохраняются. QA ищет дополнительные failure cases, не дублирует механически все прошлые команды.

CI относится к текущему final head. После изменений повторить затронутые проверки и явно обосновать применимость прежнего evidence. Документальная правка не требует повторного live capture, если код, profile, inputs/evidence и assumptions не изменились; это не перенос runtime PASS на новые runtime bytes.

## Короткий packet и один Handoff

Packet: TASK/Issue/PR; exact base/dependencies; branch и executor; bounded inputs/accepted contracts; files_allowed; forbidden scope; acceptance commands/evidence; starts/stops; expected Handoff. Не передавать всю историю чата. Технические спецификации передаются ссылками.

Минимальный Worker result: TASK, BASE_SHA, FINAL_SHA, TREE, PR, CHANGED_PATHS, CHECKS с PASS/FAIL/NOT_RUN, KNOWN_UNKNOWNS_PRESERVED, HANDOFF_PATH (файл или canonical PR report URL). Минимальный QA input: TASK, PR, FINAL_SHA, TREE, ACCEPTANCE_CRITERIA, INVARIANTS, WORKER_HANDOFF и freeze receipt. QA final verdict только PASS/FAIL/BLOCKED; check-level NOT_RUN/NOT_APPLICABLE имеют конкретную причину.

Канонические durable objects: Issue, Task Packet, PR, commit SHA/tree, CI receipts, docs/handoffs/**, accepted ADR/specs и PROJECT_STATE milestone snapshots. Уникальных решений/файлов только в чате быть не должно. Новый Worker/QA восстанавливается из этих объектов без истории предшественника.

PR report выполняет роль Handoff завершённой задачи: base/head, файлы, реальные проверки, ограничения, blockers/evidence и следующий шаг. Issue ссылается на PR вместо одинаковых длинных receipts. Отдельный Handoff нужен для сложной передачи, прерывания или milestone, либо если он явно остаётся deliverable активного packet. Исторические Handoff-файлы сохраняются.

PROJECT_STATE обновляется при milestone, существенном решении или изменении главного blocker; отдельный state-sync PR после каждого микрошагa не требуется. Связанные текущие docs могут обновляться в том же PR в разрешённом scope.

## Среда, main и блокировки

Рабочую среду настроить один раз, сохранить воспроизводимые команды и её identity/toolchain. Полный preflight повторяется при смене среды/toolchain/доступа; каждый новый код получает свои применимые tests. Не переносить PASS между executors или неизвестными caches. Root Cargo.lock, workspace и CI интегрирует только Integrator, последовательно; при исполнении им же отдельная передача этих файлов не нужна.

Изменение main само по себе не перезапускает задачу. Integrator проверяет относящийся diff; совместимые изменения позволяют продолжать, конфликт разрешается с affected tests, изменение входного контракта требует решения. Обновить фактический base/интеграционные refs в PR. Не требовать rebase как ритуал; force-push без отдельного разрешения запрещён.

При blocker сохранить в Issue причину, завершённую часть, branch/commit/patch, владельца действия и условие продолжения. Можно продолжать независимую разрешённую часть этой же задачи. Чтобы перейти к другой, Integrator явно откладывает текущую задачу, прекращает её исполнение и выдаёт один новый packet. Одновременная работа запрещена.

## Автоматическая подготовка следующего шага и передачи

Не ждать просьбы владельца о prompt/packet. На каждой границе задачи, смене роли или blocker Integrator сам сохраняет один следующий шаг и bounded copy-ready prompt в GitHub: Issue/PR, exact refs/branch, executor/environment, inputs/accepted contracts, files_allowed/shared ownership, acceptance/evidence, starts/stops и expected result.

Если доступен реальный subagent orchestration, Integrator автоматически запускает ровно одного Worker, передаёт bounded packet и после review/freeze запускает отдельного независимого QA. Не передавать весь постоянный чат. Отслеживать фактические agent identities/statuses; никакого второго implementation writer или source edits во время QA. При QA FAIL сначала закончить QA, затем сформировать corrective packet и запустить одного Worker в том же Issue/branch/PR.

Если spawn действительно недоступен, сохранить уникальный packet/Handoff в GitHub, выдать полный copy-ready prompt и честно указать:

```text
NEXT_EXECUTOR = WORKER
AUTOSPAWN = UNAVAILABLE_IN_CURRENT_ENVIRONMENT
```

Для QA fallback NEXT_EXECUTOR=QA; тот же freeze/lifecycle. Не утверждать, что агент создан, если только подготовлен prompt. При доступном orchestration ручной fallback не используется. Orchestration capability не доказывает shell/Rust/network/toolchain/exchange access: отсутствующее обязательное evidence -> BLOCKED_*.

Read-only Researcher/Architecture reviewer может помогать без второго implementation executor; он не пишет branch. Architecture review не заменяет final QA, QA не принимает ADR/milestone. Для архитектурной передачи сохраняется точная область решения и требуемая authority.

Это выполнение роли при текущей передаче, не scheduled automation, не автоматическое merge и не расширение scope. Owner не работает курьером при доступном spawn. Нужные prompts/packets остаются сохранёнными, даже если текущий executor disposable.

## Миграция текущей #45 / PR #47

[Canonical governance/resume packet](task-packets/GOV-ORCH-001.md) и [#48](https://github.com/al-gri/pro-sclpng/issues/48) фиксируют текущую точку. #45 приостановлен для единственной governance задачи; существующие branch/Draft PR/claim/source и история сохраняются. E1-CORR-01 уже доставлен на 4f599f5ab687ca1fddb43c2dd8a814e3cd70ad5a; не запускать его повторно по устаревшему промпту.

После governance acceptance Integrator перечитывает actual refs и возобновляет последовательный Architecture/source re-review existing corrected E1. При новых findings следующий implementation executor — один Worker с corrective packet в той же #45/ветке/PR, если нет отдельно записанного допустимого exception. Full implementation final QA начинается только после завершения разрешённой реализации всего кандидата и нового exact freeze. ADR0004 PROPOSED, U1–U7 UNRESOLVED, production FAIL/NOT_PROVEN, M1 unaccepted и все technical stops сохраняются.

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

## Полномочия и заменяемость

Merge остаётся за владельцем. Этот процесс не делегирует merge/auto-merge, settings/access/billing/secrets, force-push, private API или live execution. Нужное owner approval запрашивается только для конкретного готового результата.

Чат заменяем, когда в GitHub сохранены код/ветка или patch, результаты/refs, решения, ограничения и следующий шаг. Новый участник читает AGENTS, PROJECT_STATE, accepted contracts и текущий Issue/PR; уникальной памяти только в переписке нет.
