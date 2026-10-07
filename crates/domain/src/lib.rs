//! Pure contracts for SPEC-001. No network, storage or execution runtime.
//!
//! Implementation direction: PR #10 review 5418412674, design SHA 272f6ec.
//! The associated contracts remain PROPOSED until the owner workflow accepts them.

#![forbid(unsafe_code)]

pub mod artifact;
pub mod capture_session;
pub mod event;
pub mod identity;
pub mod numeric;
pub mod policy;
pub mod qualified;
pub mod record;
