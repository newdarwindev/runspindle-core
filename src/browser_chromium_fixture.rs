//! Explicit synthetic Chromium fixture bytes, never workflow selected.
pub(crate) const HELPER: &[u8] = include_bytes!("../tests/fixtures/browser_chromium_peer.py");
pub(crate) const PIN: &[u8] = include_bytes!("../tests/fixtures/browser_chromium_source_pin.json");
