//! Sequential Linux implementation probes; not independent QA or acceptance.
#![cfg(target_os = "linux")]
#[path = "support/public_capture/mod.rs"]
mod support;
#[path = "../src/public_capture/transport.rs"]
mod transport;

#[test]
fn implementation_integrated_probes() {
    support::run();
}
