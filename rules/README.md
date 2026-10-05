# Canonical rules — not imported yet

Задача [RULE-001](https://github.com/al-gri/pro-sclpng/issues/7) формирует минимальный реестр двух сетапов из реально доступных источников. Старые 271 строки не скопированы и не проверены в этом repo.

Required fields: canonical_rule_id, aliases, source_refs/source_status, applicability, role, dependencies, inputs, unknown_policy, parameter_origin, test_vectors.

Roles: REQUIRED / CONFIRMATION / INVALIDATION / CONTEXT / MANAGEMENT.
Evaluation: AUTOMATIC / MANUAL / EXTERNAL / NOT_PORTABLE / UNRESOLVED.
Results: PASS / FAIL / UNKNOWN / NOT_APPLICABLE / STALE_DATA.

Критический UNKNOWN не допускает автоматическое действие. Диагностический WATCH может показывать отсутствующий input. Резервные числа не подставляются вместо отсутствующих данных. Дубли и aliases не дают независимых голосов.

Только verified source-derived формулировки маркируются правилами автора. В bootstrap нет заявления о полном покрытии курса и нет исполняемого реестра.
