# Начать разработку

## 1. Принять bootstrap

Открыть PR из ветки `docs/GOV-001-project-bootstrap` в main. Прочитать README, ARCHITECTURE, INVARIANTS и этот файл. После review владелец выполняет Squash and merge. Это принимает базу M0, но НЕ означает готовность кода или CI.

До merge рабочие чаты могут читать ветку для review, но feature-ветки создают от main после merge. Не использовать начальный README commit как постоянную базу всех будущих задач.

## 2. Ограничить доступ и защитить main

Разрешить рабочему инструменту только этот repo. Не давать Bitget trading keys, не покупать сервер и не запускать Private API на старте. Не менять visibility автоматически: репозиторий сейчас public.

В GitHub Settings → Rules → Rulesets → New branch ruleset (точные названия UI могут отличаться):
- name: `protect-main`, enforcement: Active, target: main/default branch;
- require a pull request before merging;
- block force pushes и restrict deletions;
- require conversation resolution;
- не добавлять blanket bypass для app.

Если все чаты действуют под одним GitHub login, начать с 0 обязательных approvals: автор PR не может одобрить собственный PR как независимый reviewer. QA-отчёт всё равно обязателен, merge делает владелец. При появлении второго доверенного GitHub reviewer включить 1 approval.

После BOOT-001, когда реально прошли `rust-fmt`, `rust-clippy`, `rust-tests`, добавить именно эти status checks и проверку актуальности ветки. До появления CI не считать `0 checks` успешной проверкой. Не включать неизвестные required checks, которые никогда не запускались.

Эти настройки не включаются файлом CODEOWNERS. В bootstrap они не применены; владелец проверяет их в Settings.

Официальные справки (проверены 2026-10-05):
- https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository
- https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches
- https://docs.github.com/en/pull-requests/how-tos/review-pull-requests/approving-a-pull-request-with-required-reviews

## 3. Создать только один новый постоянный чат

Название: `01 — ProScalping Integrator`. Передать ему `docs/prompts/INTEGRATOR.md` и [Issue #2](https://github.com/al-gri/pro-sclpng/issues/2). Текущий главный чат остаётся архитектурным, а Integrator отвечает за код и последовательность PR.

Интегратор должен прочитать repo, определить SHA/возможности среды и выпустить task packet. Он может реализовать первую маленькую задачу сам или передать её одному worker. Не создавать сразу семь кодерских чатов.

## 4. Первый worker

Название: `WORK — BOOT-001`. Передать `docs/prompts/WORKER.md` и Issue #2. Первая поставка — Rust workspace, безопасный offline/shadow skeleton, CI и документация команд. Ни Bitget, ни стратегия пока не пишутся.

Если среда поддерживает shell и Git, использовать отдельный checkout/worktree только этого repo. Если она умеет лишь GitHub reads/writes, коммиты возможны, но Rust-проверки должны выполниться в CI или отдельной dev-среде. Не объявлять проверки пройденными без запуска.

## 5. Отдельная проверка

Создать `QA — BOOT-001`, когда есть PR. Передать `docs/prompts/QA.md`, Issue #6 и точный PR. QA возвращает проверенный SHA, результаты и defects, не переписывая реализацию молча. Владелец принимает merge после исправления блокеров.

## 6. Следующие работы

После первого merge: SPEC-001 (#3) + проверка Bitget MD-001 (#4); затем REC-001 (#5), независимо QA-001 (#6). RULE-001 (#7) можно готовить отдельно, но он не задерживает базовый recorder.

Первый вертикальный результат: небольшой публичный поток → local book → WAL → воспроизводимый replay. Только затем горизонтальные уровни и shadow-сигналы.

## Что вернуть главному чату

Номер PR, tested head SHA, handoff и конкретный вопрос/блокер. Не пересылать весь диалог кодера. Чат можно удалить, когда код/артефакты закоммичены, тесты задокументированы, незакрытые вопросы вынесены в Issues, а handoff принят.
