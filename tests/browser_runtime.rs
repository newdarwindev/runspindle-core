#![cfg(feature = "artifact-store")]
use runspindle_core::{
    browser_effect_wire::{Binding, Target},
    browser_runtime::*,
    durable::digest,
};
use rusqlite::Connection;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "browser-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn target() -> Target {
    Target {
        binding: Binding {
            principal: "principal".into(),
            account: "account".into(),
            browser: "browser".into(),
            tab: "tab".into(),
            intent: "intent".into(),
            revision: 1,
            operation: "operation".into(),
            epoch: 1,
            origin: "https://example.invalid".into(),
        },
        native_session: "session".into(),
        frame: "frame".into(),
        frame_origin: "https://example.invalid".into(),
    }
}
fn script() -> BrowserScript {
    BrowserScript {
        target: target(),
        expected_sha256: digest(b"x"),
        dispatch_authorized: true,
        reconcile_authorized: true,
        not_after_unix_ms: u64::MAX,
        crash_before_effect: false,
        crash_after_effect: false,
        lost_reply: false,
    }
}
fn save(t: &Temp, s: &BrowserScript) {
    std::fs::write(
        t.0.join("browser-script.json"),
        serde_json::to_vec(s).unwrap(),
    )
    .unwrap();
}
fn setup() -> (Temp, BrowserPorts) {
    let temp = Temp::new();
    save(&temp, &script());
    std::fs::write(temp.0.join("browser-payload.bin"), b"x").unwrap();
    let ports = BrowserPorts {
        directory: Some(temp.0.clone()),
        pinned_fixture: false,
    };
    (temp, ports)
}
fn call(reconcile: bool) -> BrowserCall {
    BrowserCall {
        target: target(),
        reconcile,
    }
}
fn counts(t: &Temp) -> (u64, u64, u64) {
    let db = Connection::open(t.0.join("browser-site.sqlite")).unwrap();
    (
        db.query_row(
            "SELECT count(*) FROM calls WHERE action='dispatch'",
            [],
            |r| r.get(0),
        )
        .unwrap(),
        db.query_row(
            "SELECT count(*) FROM calls WHERE action='reconcile'",
            [],
            |r| r.get(0),
        )
        .unwrap(),
        db.query_row("SELECT count(*) FROM effects", [], |r| r.get(0))
            .unwrap(),
    )
}
#[test]
fn default_missing_expired_and_changed_grants_deny_without_peer() {
    assert!(BrowserPorts {
        directory: None,
        pinned_fixture: false
    }
    .invoke(call(false))
    .is_err());
    for mode in 0..5 {
        let (temp, ports) = setup();
        let mut s = script();
        match mode {
            0 => s.dispatch_authorized = false,
            1 => s.not_after_unix_ms = 0,
            2 => s.target.binding.account = "other".into(),
            3 => s.expected_sha256 = digest(b"y"),
            _ => {
                std::fs::remove_file(temp.0.join("browser-script.json")).unwrap();
            }
        }
        if mode < 4 {
            save(&temp, &s);
        }
        let err = ports.invoke(call(false)).unwrap_err();
        assert_eq!(err.to_string(), "browser activity denied");
        assert_eq!(err.chain().count(), 1);
        assert!(!temp.0.join("browser-site.sqlite").exists());
    }
}
#[test]
fn dispatch_ack_is_unknown_duplicate_no_peer_explicit_reconcile_confirms() {
    let (temp, ports) = setup();
    assert_eq!(ports.invoke(call(false)).unwrap().outcome, "unknown");
    assert_eq!(counts(&temp), (1, 0, 1));
    assert_eq!(ports.invoke(call(false)).unwrap().outcome, "unknown");
    assert_eq!(counts(&temp), (1, 0, 1));
    assert_eq!(ports.invoke(call(true)).unwrap().outcome, "confirmed");
    assert_eq!(counts(&temp), (1, 1, 1));
    assert_eq!(ports.invoke(call(false)).unwrap().outcome, "unknown");
    assert_eq!(counts(&temp), (1, 1, 1));
}
#[test]
fn revoked_reconcile_does_not_reuse_confirmation_and_restored_grant_can_read() {
    let (temp, ports) = setup();
    ports.invoke(call(false)).unwrap();
    ports.invoke(call(true)).unwrap();
    let mut s = script();
    s.reconcile_authorized = false;
    save(&temp, &s);
    assert!(ports.invoke(call(true)).is_err());
    assert_eq!(counts(&temp), (1, 1, 1));
    s.reconcile_authorized = true;
    save(&temp, &s);
    assert_eq!(ports.invoke(call(true)).unwrap().outcome, "confirmed");
    assert_eq!(counts(&temp), (1, 2, 1));
}
#[test]
fn lost_reply_and_reconstructed_ports_preserve_one_independent_effect() {
    let (temp, ports) = setup();
    let mut s = script();
    s.lost_reply = true;
    save(&temp, &s);
    assert_eq!(ports.invoke(call(false)).unwrap().outcome, "unknown");
    drop(ports);
    let ports = BrowserPorts {
        directory: Some(temp.0.clone()),
        pinned_fixture: false,
    };
    assert_eq!(ports.invoke(call(false)).unwrap().outcome, "unknown");
    assert_eq!(ports.invoke(call(true)).unwrap().outcome, "confirmed");
    assert_eq!(counts(&temp), (1, 1, 1));
    let db = Connection::open(temp.0.join("browser-site.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT payload FROM effects", [], |r| r
            .get::<_, Vec<u8>>(0))
            .unwrap(),
        b"x"
    );
}
#[test]
fn workflow_identity_ignores_replacement_operation_but_binds_revision_and_principal() {
    let t = target();
    let id = workflow_id(&t).unwrap();
    let mut changed = t.clone();
    changed.binding.operation = "replacement".into();
    assert_eq!(workflow_id(&changed).unwrap(), id);
    changed.binding.revision += 1;
    assert_ne!(workflow_id(&changed).unwrap(), id);
    changed = t;
    changed.binding.principal = "other".into();
    assert_ne!(workflow_id(&changed).unwrap(), id);
}
#[test]
fn bounded_explicit_requests_reject_changed_target_pending_and_exhausted_budget() {
    let mut state = BrowserState::new(target());
    let mut other = target();
    other.binding.account = "other".into();
    assert!(!state.request_reconcile(&other));
    assert!(state.request_reconcile(&target()));
    assert!(!state.request_reconcile(&target()));
    state.pending = false;
    state.completed_reconciles = MAX_RECONCILES;
    assert!(!state.request_reconcile(&target()));
    state.completed_reconciles = 0;
    state.finished = true;
    assert!(!state.request_reconcile(&target()));
    assert!(state.historical.is_none());
    assert_eq!(state.attempts, 0);
    assert!(!state.dispatch_finished);
}

#[test]
fn pinned_fixture_is_explicit_fixed_root_and_never_falls_back_to_synthetic() {
    assert!(BrowserPorts {
        directory: None,
        pinned_fixture: true
    }
    .invoke(call(false))
    .is_err());
    let (temp, _) = setup();
    let ports = BrowserPorts {
        directory: Some(temp.0.clone()),
        pinned_fixture: true,
    };
    assert!(ports.invoke(call(false)).is_err());
    assert!(!temp.0.join("browser-registry.sqlite").exists());
    assert!(!temp.0.join("browser-site.sqlite").exists());
}
