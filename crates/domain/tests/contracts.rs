//! Executable SPEC-001 contracts. All inputs are synthetic and memory-only.
//! Public support modules are local to this test binary, not domain's API.

pub mod support;

#[path = "cases/accounting.rs"]
mod accounting;
#[path = "cases/artifacts.rs"]
mod artifacts;
#[path = "cases/events.rs"]
mod events;
#[path = "cases/health.rs"]
mod health;
#[path = "cases/policy.rs"]
mod policy;
#[path = "cases/publication.rs"]
mod publication;
#[path = "cases/shared.rs"]
mod shared;
