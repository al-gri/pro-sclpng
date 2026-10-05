# Handoff: SPEC-001 — revised design checkpoint

Status: **PARTIAL / DESIGN_REVIEW_REQUIRED**. All new contracts and ADR: **PROPOSED**, proposal revision2.
Issue: https://github.com/al-gri/pro-sclpng/issues/3
Same Draft PR: https://github.com/al-gri/pro-sclpng/pull/10
Existing claim: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901
Packet: https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559
Role/session: WORK — SPEC-001,continuation of the same worker/branch/PR,only al-gri/pro-sclpng.
Base/main: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Reviewed/input head: `98ecd7484f8345d6c5f162a85ff3effbd6a46a68`.
Branch: `feat/SPEC-001-domain-contracts`.
Final revision head / tested source: **post-commit evidence and renewed review request in PR #10**. Do not insert a future self-referential containing-commit SHA here.

## Что сделано

Перечитаны AGENTS,packet,оба полных review и обсуждение PR/Issue3. Проверены фактические main и PR head:совпали с указанными SHA,новой numeric implementation не обнаружено. Review sources: [Integrator5415922496](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5415922496) и [Architecture5416219284](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5416219284). Никакой новый claim/branch/PR не создан,работа поверх более новой ревизии не откатывалась.

Architecture direction ARCHITECTURE_DIRECTION_SET / DESIGN_REVISION_REQUIRED перенесена в согласуемые specs/ADR/matrix. D1/D2 сохраняют отдельное APPROVED_FOR_ISOLATED_IMPLEMENTATION;этот commit docs-only,код numeric slice НЕ НАЧАТ и не смешан с изменением дизайна. Полная event/DataHealth/WAL реализация не разрешена этими review и не выполнялась.

A1: source candidate отдельно от apply effect;whole-frame proof,ordered per-stream release,current availability,barrier,at-most-once source application,bounded pending и exact negative traces.
A2: все mode/gate сочетания,детерминированный immutable candidate,receipt в causal prefix и конечный non-WAL StorageFence без ack-рекурсии;предикат не разрешает публикацию по одному receipt.
A3: typed content-bound ArtifactRef,точные external PSAD/PSAM/PSCO schemas,descriptor/body hashes,полная dependency closure,namespace и fail-closed loader boundary;canonical activation timeline и BootstrapContext.
R3/R5/R6/C1/C2: finite freshness/quiet intervals,None semantics,one-use local loss windows,source-vs-local loss,shared transport initialization/fan-out,no writer rebind,capped per-output progress.

Все изменения предъявлены на повторный review,НЕ объявлены закрытыми findings. Новых Rust types/validators/models/codec/production runtime нет. Матрица ожидаемых outcomes не названа выполненными tests.

## Изменённые файлы

Семь существующих proposal-документов обновлены и три Markdown-документа добавлены:

```text
specs/domain/types-v1.md
specs/domain/test-matrix-v1.md
specs/domain/artifacts-v1.md                 # new
specs/domain/review-vectors-v2.md            # new
specs/market-data/events-v1.md
specs/market-data/data-health-v1.md
specs/recording/wal-v1.md
tests/fixtures/domain/artifacts-v1.md        # new,hex fixtures as documentation
docs/adr/0002-domain-event-wal-contracts.md
docs/handoffs/SPEC-001.md
```

Только allowed paths. Root Cargo.toml/Cargo.lock,toolchain1.98.1,CI,crates/domain,apps/radar,README,PROJECT_STATE,specs/README,accepted ADR-0001 и чужие handoffs не меняются. std-only сохранён. GitHub compare после commit фиксирует окончательный scope и parentage;API objects/staging не выдаются за checkout.

## Finding mapping

Подробная таблица и точные варианты: [mapping](../../specs/domain/review-vectors-v2.md#8-finding-to-change-mapping-and-remaining-review).

| Finding | Изменённые разделы | Векторы | Остаётся |
|---|---|---|---|
| A1/R1 | events1–4;health1/3/4;WAL Control;artifacts4;ADR A1 | V-R1-* | Повторный review application/proof/frontier semantics;исполнение NOT_RUN |
| A2/R2 | health2/5;WAL6/RecordingEvidence;ADR A2 | V-R2-* | Review candidate/fence pure boundary;реальная storage completion REC-001 |
| R3 | health2–4;artifacts4;events7 | V-R3-* | Review exact lifetime/error rules;real-feed bounds MD-001 |
| A3/R4 | artifacts1–5;events1/2/6;types2/3;WAL1/3/4 | V-R4-*,AF-N1/B1/C1/F1/V1 | Review exact descriptor/PSCO formats/caps;production verifier absent |
| R5 | WAL Gap/4.1/5/ArchiveSeal;events4 | V-R5-* | Review one-window/block-on-scope-change restriction;no queue implementation |
| R6 | health1/4;types1;WAL StreamDefinition | V-R6-* | Model execution NOT_RUN |
| C1/C2 | types1;health1/3;events3;WAL warm-up | V-C1-WRITER,V-C2-PROGRESS/MAX | Review explicit one-writer/progress limits |
| C3 | retained N/E/H/W matrix plus named catalog | baseline + all new named vectors | Full multi-frame/segment golden and executable assertions still pending |

## Явные semantic / wire changes

WAL header/kinds/field order/CRC и W01 golden НЕ изменены. Изменён смысл Token128 provenance/evidence/proof:теперь обязательные typed ArtifactRefs,не прежний prose token. Required pending/quiet fields находятся в Config descriptor,без новых WAL полей;его proposal_revision=2. Новые external PSAD descriptor,optional PSAM manifest и PSCO whole-frame output commitment описаны побайтово.
SourceCandidateKey не EventId;effects получают apply cursor,available_at не переносится назад;as_of=CausalBasis с conservative recorded prefix. Administrative inputs имеют RecordRef,не фиктивный market EventCursor. Activation использует old Context/new-next и единый timeline через norm revisions.
StorageFence не WAL input и не clock tick. Scope proof включает owners через immutable mappings,tag/config/norm/profile/barrier,anchor/basis и temporal bounds. GAP accounting explicit,без новых wire fields;консервативные ограничения вынесены в ADR для review,не спрятаны в codec.

## Реальные проверки этой revision-сессии

| Check / command | Status | Evidence / environment |
|---|---|---|
| AGENTS,packet,both full reviews,new discussion,main/head reads | PASS (read) | GitHub connector;не исполнение кода/тестов |
| Shell/Git/Python availability | PASS | Linux x86_64,Python3.13.5,Bash5.2.37,Git2.47.3;Cargo/rustc/rustup отсутствуют |
| git ls-remote только разрешённого repo/main/work branch | FAIL | exit128:Could not resolve host github.com;локального checkout нет |
| Ручная междокументная сверка правил,links,status/provenance | PERFORMED | Найденные неоднозначности dependency list/candidate creation уточнены;не независимый QA и не автоматический link checker |
| AF descriptor/body byte lengths + hashlib/OpenSSL SHA256 | PASS (calculation) | 5 descriptor+5 body digests согласились;Python3.13.5/OpenSSL3.5.5,native exit0;вне будущего Rust codec |
| W01 literal bytes/length/zlib CRC | PASS (calculation) | 74bytes,header32/payload38,CRC9E02C413,без изменения golden;не recovery test |
| Локальные cargo build/fmt/clippy/workspace test/domain test | NOT_RUN | Нет Cargo/Rust/checkout;API authoring не локальная сборка |
| New final-head CI | NOT_RUN на момент написания handoff | Итоговые run/checks/steps/actual SHA/logs публикуются ПОСЛЕ commit в PR,не подменяются прежним run |
| Новые numeric/event/health/WAL contract assertions/models | NOT_IMPLEMENTED / NOT_RUN | Только design revision;нет runtime approvals для зависимой реализации |
| Windows11 x64 / PowerShell5.1 | NOT_RUN | Нет owner evidence;Linux CI не Windows |
| Filesystem fsync/crash/power-loss,production artifact loader | NOT_RUN / OUT_OF_SCOPE | Семантика описана,но реализация/доказательства downstream |

AF descriptor/body lengths:86/23,91/27,154/135,228/172,307/245;PSCO snapshot commitment49bytes. [Fixture document](../../tests/fixtures/domain/artifacts-v1.md) сохраняет literal hex,refs,hashes и ограничения. Hashlib/OpenSSL cross-tool agreement не выдаётся за независимые криптографические реализации или proof of resolver correctness. Synthetic artifact label не нормализует production data.

Исторический КП1 CI:run37319070087 на reviewed98ecd748...,15 BOOT-001 CLI tests/domain0,Ubuntu24.04.5/Rust1.98.1. Это прежнее evidence,НЕ новый run этой ревизии. Push-run37307781420 относится только к base. Новые фактические проверки/точный SHA фиксируются в PR после последнего commit,без self-hash в handoff.

## Deviations / ограничения / открытые вопросы

Локальный GitHub DNS недоступен;authoring через GitHub objects и обычное fast-forward обновление ТОЛЬКО рабочей ветки. Отдельный clean checkout и Rust execution предоставляет existing CI. Никаких изменений workflow ради docs-only check. Недоступные локальные/Windows проверки не заменяются чтением CI metadata.

Review нужен по точной новой поверхности,а не только по совпадению терминов A1–A3:projection/control identities,CausalBasis,frame-release atomicity/equivalence/pending caps;детерминированное создание/supersession candidates и final-fence diagnostics;binary descriptor/PSCO schemas/implicit scoped dependencies;one unresolved loss window и ошибка при scope change до правой границы. Findings не закрыты самостоятельно.

MD-001(#4):real Bitget identity/units/increments/sequence/zero/aggressor/RPI/time/resync/quiet/timeout proofs остаются UNKNOWN/BLOCKED_BY_MD_001. Это не блокирует независимые generic contracts. QA-001(#6):independent review/implementation acceptance ещё впереди;не закрывать. Required-check/settings concern из packet остаётся owner/integration responsibility,настройки не менялись.

## PowerShell 5.1 — owner commands, NOT_RUN

Из отдельного чистого checkout final SHA из PR,с установленными Git/Rustup1.98.1,rustfmt/clippy и linker. $LASTEXITCODE проверяется сразу после каждой native-команды;Bash export/&& не используются.

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

Команды не запускались на Windows. Нужны exact SHA/OS/target/toolchain/exit codes/logs,а не отсутствие сообщения об ошибке.

## Артефакты / следующий один шаг / замена чата

Все design decisions,точные vectors/fixture bytes и remaining questions находятся в перечисленных Git-docs и PR10. Локальный каталог расчётов не считается checkout и не является единственным источником fixture bytes. Никаких секретов или иных репозиториев.

**Следующий bounded шаг: Architecture/Integrator повторно проверяет revision2 на новом head PR10 и явно разрешает согласованный дизайн либо задаёт дальнейшие исправления. До этого event/DataHealth/WAL implementation остановлена.** Merge не запрошен/не выполнен,Issue3/6 остаются open.

Новый worker читает AGENTS,packet,оба review,final-head PR evidence,ADR,specs,matrix/review-vectors и этот handoff. D1/D2 разрешение не расширять на остальной API. Handoff принимает Integrator/owner,не его автор. Отдельного numeric code/незакоммиченной реализации для переноса нет.
