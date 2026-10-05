# Handoff: SPEC-001 — targeted wire and link correction

Status: **PARTIAL / TARGETED_DESIGN_REVIEW_REQUIRED**. All new contracts and ADR remain **PROPOSED**, proposal revision2.
Issue: https://github.com/al-gri/pro-sclpng/issues/3
Same Draft PR: https://github.com/al-gri/pro-sclpng/pull/10
Existing claim: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901
Packet: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559
Role/session: WORK — SPEC-001, same worker, branch and PR; only al-gri/pro-sclpng.
Base/main: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Reviewed/input head for this iteration: `8758b3c2a8896146a14d397bd65dc18ac5a49415`.
Branch: `feat/SPEC-001-domain-contracts`.
Final head / tested source: **post-commit evidence and targeted review request in PR #10**. The future containing-commit SHA is deliberately not written into this file.

## Что сделано в этой итерации

Перечитаны AGENTS.md, полный packet и [последний Integrator review5417294202](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5417294202), фактические main/head и обсуждение PR/Issue3. SHA совпали с ожидаемыми; более новой работы не найдено. Новый claim, ветка или PR не создаются. Подготовлен один ограниченный docs-only commit поверх reviewed/input head, без переписывания истории.

Integrator уже отметил R1–R6/C1/C2 RESOLVED **на уровне дизайна**. Это решение reviewer, не worker и не результат Rust tests. A1–A3, artifact schemas, scope proof, activation timeline, loss accounting и девять mode/gate решений не перепроектированы. Исторические подробности revision2 и её проверки сохранены в [предыдущем checkpoint](https://github.com/al-gri/pro-sclpng/pull/10#issuecomment-5997558404) и Git history. Этот handoff заменяет прежний следующий шаг на targeted review трёх текущих findings.

V2-WIRE-01: восстановлен нормативный Gap body после Context: scope_kind:u8, reason:u8, target_count:u16, targets. Ошибочная перестановка в8758b3c исправлена, а не объявлена намеренной новой wire schema. Header32 + Context24 дают offsets56/57/58..59; ExplicitTargets1/QueueOverflow4/count1 кодируется01 04 01 00. Положительный и переставленный отрицательный полные100-byte frames имеют каждый собственный корректный CRC, поэтому отрицательный вектор доходит до Unsupported Gap.scope_kind4 на offset56.

V2-WIRE-02: единая нормативная таблица policy tags восстановлена в DataHealth2.1, ссылки добавлены из WAL ConfigDefinition и PSAD Config. Unsupported, включая0/255, не становится default/bootstrap policy. Поддерживаемые, но несовпадающие WAL/descriptor values отклоняются до activation. RecordingGate и RecordingEvidence.watermark_kind явно различены: Durable gate3 не равен Written watermark3. watermark_kind не перенумерован; девять mode/gate решений не меняются.

V2-DOC-01: три ссылки из specs/domain исправлены на ../../tests/fixtures/domain/artifacts-v1.md. Разрешение проверяется от каталога каждого документа. Все новые named vectors — ожидаемые результаты design review, не новая исполнимая модель.

## Finding → исправленный раздел → named vector

| Finding | Исправленный раздел | Named vector |
|---|---|---|
| V2-WIRE-01 | [WAL Gap](../../specs/recording/wal-v1.md#7--gap), WAL1; [ADR-0002](../adr/0002-domain-event-wal-contracts.md) targeted correction; этот handoff | V2-WIRE-GAP-ORDER; V2-WIRE-GAP-REVERSED |
| V2-WIRE-02 | [DataHealth2.1](../../specs/market-data/data-health-v1.md#21-policy-byte-tags), WAL ConfigDefinition; [PSAD Config](../../specs/domain/artifacts-v1.md#3-definition-bodies-and-mandatory-wal-bindings) | V2-POLICY-SILENCE-ALL; V2-POLICY-GATE-ALL; V2-POLICY-SILENCE-UNSUPPORTED; V2-POLICY-GATE-UNSUPPORTED; V2-POLICY-WAL-PSAD-MATCH; V2-POLICY-WAL-PSAD-MISMATCH; V2-POLICY-NO-ORDINAL-CAST |
| V2-DOC-01 | AF fixture links in artifacts-v1, test-matrix-v1 and review-vectors-v2 | V2-DOC-AF-LINKS |

Полные bytes, offsets, поля, ошибки, state/cursor/ID/available_at expectations: [targeted vectors and mapping](../../specs/domain/review-vectors-v2.md#9-targeted-wire-and-link-vectors). Статус текущих трёх findings — SUBMITTED_FOR_REVIEW, не CLOSED автором.

## Scope / изменённые файлы

Семь существующих Markdown-документов, без новых файлов или кода:

```text
specs/recording/wal-v1.md
specs/market-data/data-health-v1.md
specs/domain/artifacts-v1.md
specs/domain/review-vectors-v2.md
specs/domain/test-matrix-v1.md
docs/adr/0002-domain-event-wal-contracts.md
docs/handoffs/SPEC-001.md
```

Только allowed paths. Root Cargo.toml/Cargo.lock, Rust1.98.1/toolchain, CI, crates/domain, apps/radar, README, PROJECT_STATE, specs/README, accepted ADR-0001 и чужие handoffs не изменяются. Types/events и [AF fixture bytes](../../tests/fixtures/domain/artifacts-v1.md) также не меняются в этой итерации. std-only сохранён. D1/D2 permission остаётся отдельным; numeric code не начат, зависимые event/DataHealth/WAL types/validators/models не реализованы.

WAL header, record/control tags, CRC coverage и W01 bytes/checksum сохранены. PSAD/PSAM/PSCO schemas и AF-C1 bytes/hashes не меняются. Новые полный Gap positive/negative примеры используют уже существующую раскладку, а policy table восстанавливает исчезнувшие определения, не добавляет новых вариантов enum.

## Реальные проверки и границы evidence

| Check / command | Status | Evidence / environment |
|---|---|---|
| AGENTS, packet, latest full review, discussion and main/head reads | PASS (read) | GitHub connector, не запуск кода |
| Shell/Git/Python availability | PASS | Linux x86_64, Python3.13.5, Bash5.2.37, Git2.47.3; Cargo/rustc/rustup отсутствуют |
| git ls-remote permitted repository refs | FAIL | exit128, Could not resolve host github.com; локального checkout нет |
| Gap literal layout/length/CRC calculations | PASS (calculation), exit0 | Python struct/zlib1.3.1 плюс отдельный reflected bit-loop; оба frames100 bytes, payload64; positive CRC E5F41926, negative35EDEBF2 |
| Policy tag/mirror/offset calculations | PASS (calculation), exit0 | 512 single-u8 membership checks, включая0/255; шесть matching field-pairs, два supported-tag mismatch примера, semantic gate/watermark mapping и девять mode/gate cells; не parser/model tests |
| AF-C1 body and W01 | PASS (calculation), exit0 | AF-C1 body135 bytes/hash неизменен; body policy offsets83/109, соответствующие WAL offsets137/163; W01 length74/CRC9E02C413 |
| Relative-link path resolution | PASS (path calculation), exit0 | 61 distinct document/href pairs из ручного inventory семи подготовленных документов нормализованы Python posixpath до API-read repository paths; три исправленных AF links не выходят из repo. Fragment headings сверены отдельно по тексту |
| Local cargo build/fmt/clippy/workspace test/standalone domain test | NOT_RUN | Нет Rust/Cargo/checkout; API objects/staging не являются checkout |
| Fresh final-head CI | NOT_RUN на момент записи этого handoff | После commit в PR публикуются новый SHA, run/check/job/step/actual checkout evidence; прежний CI не подставляется |
| New Rust contract assertions/models/codec/resolver | NOT_IMPLEMENTED / NOT_RUN | Только documentary byte/link correction, не общее разрешение реализации |
| Windows11 x64 / PowerShell5.1 | NOT_RUN | Owner evidence не получено; Linux CI не Windows |
| Full multi-frame/multi-segment/all-offset recovery assertions | NOT_IMPLEMENTED / NOT_RUN | Остаются обязательны после design approval; два Gap frames не заменяют W02 |
| Production loader/verifier, filesystem fsync/crash/power-loss/delivery | NOT_RUN / OUT_OF_SCOPE | Нет production integration и доказательств физической durability |

Byte calculations используют буквальные documentary bytes и обычные integer/CRC операции. Они не доказывают выполнение отсутствующего Rust decoder/state model. В частности, ожидаемые Unsupported/PolicyRepresentationMismatch и отсутствие activation — специфицированные assertions для будущих tests, а не уже исполненный repository validator. Для отрицательного полного Gap checksum действительно пересчитан и согласован двумя методами; он не маскирует scope error.

Link-check inventory составлен по прочитанным и подготовленным текстам; это не автоматический Markdown parser, не checkout и не crawler внешних URL. Существование target paths сверено с GitHub reads/tree, fragment headings — с соответствующими разделами. Попытка files.materialize для GitHub response не дала файл; отсутствующий container copy не выдан за скачанный checkout.

Исторический CI на reviewed/input8758b3c: run37332206907, 15 BOOT-001 CLI tests/domain0. Он НЕ проверяет новый commit. Новое CI evidence фиксируется только после последнего commit в PR, без self-referential hash в handoff. Existing workspace CI проверяет bootstrap, а не корректность новых документированных контрактов.

## PowerShell 5.1 — owner commands, NOT_RUN

Из отдельного чистого checkout final SHA из PR, с установленными Git/Rustup1.98.1,rustfmt/clippy и linker. $LASTEXITCODE проверяется сразу после каждой native-команды; Bash export/&& не используются.

```powershell
$ErrorActionPreference = 'Stop'
$env:RUSTUP_TOOLCHAIN = '1.98.1'
$env:CARGO_NET_OFFLINE = 'true'
$ExpectedHead = 'COPY_FINAL_HEAD_FROM_PR'
if ($ExpectedHead -notmatch '^[0-9a-f]{40}$') { throw 'Set exact PR SHA' }
$actual = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'git rev-parse failed' }
if ($actual -ne $ExpectedHead) { throw 'Wrong SHA' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'git status failed' }
if ($status) { throw 'Dirty checkout' }
$active = rustup show active-toolchain
if ($LASTEXITCODE -ne 0) { throw 'rustup failed' }
Write-Output $active
if ($active -notmatch '^1\.98\.1-') { throw 'Wrong toolchain' }
$compiler = rustc --version
if ($LASTEXITCODE -ne 0) { throw 'rustc failed' }
Write-Output $compiler
if ($compiler -notmatch '^rustc 1\.98\.1(\s|$)') { throw 'Wrong compiler' }
rustc --version --verbose
if ($LASTEXITCODE -ne 0) { throw 'rustc verbose failed' }
cargo --version
if ($LASTEXITCODE -ne 0) { throw 'cargo version failed' }
cargo build --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'build failed' }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw 'fmt failed' }
cargo clippy --workspace --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { throw 'clippy failed' }
cargo test --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'workspace tests failed' }
cargo test -p domain --locked
if ($LASTEXITCODE -ne 0) { throw 'domain tests failed' }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw 'Lockfile changed' }
git diff --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Tracked files changed' }
git diff --cached --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Index changed' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'Final status failed' }
if ($status) { throw 'Final checkout dirty' }
```

Команды не запускались на Windows. Нужны exact SHA/OS/target/toolchain/exit codes/logs, а не отсутствие сообщения об ошибке.

## Открытые вопросы / следующий один шаг / замена чата

Требуется targeted SHA-bound review Integrator только по V2-WIRE-01, V2-WIRE-02 и V2-DOC-01 в том же Draft PR10. Нового межмодульного вопроса A1–A3 не предлагается. До явного design approval зависимая реализация остановлена. D1/D2 permission не расширять на остальной API.

MD-001(#4) по-прежнему отвечает за реальные Bitget meanings/profiles; непроверенное UNKNOWN/BLOCKED_BY_MD_001 не заменено synthetic evidence. Полный QA конкретной будущей реализации и owner acceptance остаются отдельными gates. Настройки/required-check concern из packet — owner/integration responsibility, не задача этого commit.

Все нужные literal vectors, schema corrections, mapping и результаты сохранены в Git/PR; локальные расчёты не единственный источник bytes. Новый worker читает AGENTS,packet,review5417294202,последний targeted request/final-head CI,ADR/specs и этот handoff. Никаких секретов, иных репозиториев, production loader/recorder/connector/book/replay/strategy/execution. Issues3/6 не закрываются; merge,auto-merge,force-push и изменение настроек не выполняются. Handoff принимает Integrator/owner, не его автор.
