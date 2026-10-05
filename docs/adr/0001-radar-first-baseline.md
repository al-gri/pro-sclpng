# ADR-0001 — Radar first, Rust modular monolith

Status: PROPOSED; accepted только после owner merge bootstrap PR.
Date: 2026-10-05.

## Context
Нужно перейти от объёмной дискуссии к проверяемому продукту и не переписывать детекторы при будущем подключении робота.

## Decision
Начать с публичного recorder/replay. Затем только горизонтальные зоны и TrueBreakout/FalseBreakout в shadow-mode. Rust modular monolith, single-writer state, bounded queues. Python offline, без broker на tick-to-decision пути. Минимальный workspace вместо множества пустых crates.

Сигналы отделены immutable Trade Intent Protocol; точные схемы и тесты утверждаются отдельными specs. Execution не входит в стартовый код. API-профиль Bitget перепроверяется перед реализацией.

## Consequences
Быстрое доказательство корректности данных; часть расширенных функций отложена. Нет обещания, что future execution потребует лишь одного consumer: account/order/position FSM и race handling остаются отдельной работой.

## Rejected for MVP
Микросервисы, kernel bypass, обязательный binary feed, ML/Hawkes/PCA, фиксированный облачный регион и не измеренные SLA. Возвращаться к ним только через измерение bottleneck и новое ADR.
