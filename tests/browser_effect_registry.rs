#![cfg(feature = "artifact-store")]
use runspindle_core::browser_effect_registry::{BrowserRegistry, CAPACITY};
use runspindle_core::browser_effect_wire::*;
use runspindle_core::durable::digest;
use rusqlite::Connection;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "browser-registry-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn db(&self) -> PathBuf {
        self.0.join("registry.sqlite")
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
struct Allow;
impl Authority for Allow {
    fn verify(&self, _: &Target, _: &str, _: Action) -> bool {
        true
    }
}
struct Spy {
    calls: usize,
    outcome: &'static str,
    actions: Vec<String>,
}
impl Spy {
    fn new(outcome: &'static str) -> Self {
        Self {
            calls: 0,
            outcome,
            actions: vec![],
        }
    }
}
impl Peer for Spy {
    fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.calls += 1;
        let request: serde_json::Value = serde_json::from_slice(raw)?;
        self.actions
            .push(request["action"].as_str().unwrap().into());
        Ok(serde_json::to_vec(
            &json!({"version":VERSION,"request_sha256":digest(raw),"evidence_class":EVIDENCE_CLASS,"outcome":self.outcome}),
        )?)
    }
}
#[test]
fn reservation_reopen_duplicate_never_resends_or_returns_stale_confirmation() {
    let temp = Temp::new();
    let t = target();
    let mut peer = Spy::new("confirmed");
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    assert_eq!(
        registry
            .dispatch(t.clone(), b"x", &Allow, &mut peer)
            .unwrap(),
        Outcome::Confirmed
    );
    drop(registry);
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    assert_eq!(
        registry
            .historical_snapshot(&t)
            .unwrap()
            .unwrap()
            .observation,
        Outcome::Confirmed
    );
    assert_eq!(
        registry
            .dispatch(t.clone(), b"x", &Allow, &mut peer)
            .unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls, 1);
    assert!(registry
        .dispatch(t.clone(), b"x", &Deny, &mut peer)
        .is_err());
    assert!(registry.reconcile(&t, &Deny, &mut peer).is_err());
    assert_eq!(peer.calls, 1);
    assert_eq!(
        registry.reconcile(&t, &Allow, &mut peer).unwrap(),
        Outcome::Confirmed
    );
    assert_eq!(peer.actions, vec!["dispatch", "reconcile"]);
}
#[test]
fn changed_operation_target_payload_and_reused_operation_deny_before_peer() {
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("unknown");
    registry
        .dispatch(target(), b"x", &Allow, &mut peer)
        .unwrap();
    for mode in 0..6 {
        let mut t = target();
        match mode {
            0 => t.binding.operation = "different".into(),
            1 => t.binding.intent = "different".into(),
            2 => t.binding.account = "different".into(),
            3 => t.binding.epoch += 1,
            4 => t.frame = "different".into(),
            _ => t.binding.principal = "different".into(),
        }
        assert!(
            registry
                .dispatch(t.clone(), b"x", &Allow, &mut peer)
                .is_err(),
            "mode {mode}"
        );
        assert!(
            registry.reconcile(&t, &Allow, &mut peer).is_err(),
            "mode {mode}"
        );
    }
    assert!(registry
        .dispatch(target(), b"y", &Allow, &mut peer)
        .is_err());
    assert_eq!(peer.calls, 1);
}
#[test]
fn absent_reconciliation_never_creates_a_reservation_or_peer_effect() {
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("confirmed");
    assert!(registry.reconcile(&target(), &Allow, &mut peer).is_err());
    assert!(registry.historical_snapshot(&target()).unwrap().is_none());
    assert_eq!(peer.calls, 0);
}
#[test]
fn unknown_does_not_erase_terminal_and_contradiction_is_sticky_quarantine() {
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("confirmed");
    registry
        .dispatch(target(), b"x", &Allow, &mut peer)
        .unwrap();
    peer.outcome = "unknown";
    assert_eq!(
        registry.reconcile(&target(), &Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(
        registry
            .historical_snapshot(&target())
            .unwrap()
            .unwrap()
            .observation,
        Outcome::Confirmed
    );
    peer.outcome = "rejected";
    assert_eq!(
        registry.reconcile(&target(), &Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    let saved = registry.historical_snapshot(&target()).unwrap().unwrap();
    assert!(saved.conflict);
    assert_eq!(saved.generation, 3);
    peer.outcome = "confirmed";
    assert_eq!(
        registry.reconcile(&target(), &Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert!(
        registry
            .historical_snapshot(&target())
            .unwrap()
            .unwrap()
            .conflict
    );
}
#[test]
fn concurrent_connections_only_one_committed_dispatch_reaches_peer() {
    struct Count(Arc<AtomicUsize>);
    impl Peer for Count {
        fn exchange(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(b"{}".to_vec())
        }
    }
    let temp = Temp::new();
    drop(BrowserRegistry::open(temp.db()).unwrap());
    let barrier = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let handles = (0..2)
        .map(|_| {
            let path = temp.db();
            let barrier = barrier.clone();
            let calls = calls.clone();
            std::thread::spawn(move || {
                let mut registry = BrowserRegistry::open(path).unwrap();
                barrier.wait();
                registry
                    .dispatch(target(), b"x", &Allow, &mut Count(calls))
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results, vec![Outcome::Unknown; 2]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[test]
fn restored_schema_cannot_hide_corrupt_bytes_keys_or_digest() {
    for assignment in [
        "payload=X'79'",
        "identity='wrong'",
        "identity='wrong',operation='wrong'",
        "operation='wrong'",
        "digest='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        "target='{}'",
        "payload=zeroblob(1048577)",
        "target=replace(hex(zeroblob(8193)),'00','x')",
    ] {
        let temp = Temp::new();
        let mut registry = BrowserRegistry::open(temp.db()).unwrap();
        let mut peer = Spy::new("unknown");
        registry
            .dispatch(target(), b"x", &Allow, &mut peer)
            .unwrap();
        let db = Connection::open(temp.db()).unwrap();
        let trigger: String = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='browser_binding_immutable'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        db.execute_batch(&format!("PRAGMA ignore_check_constraints=ON;DROP TRIGGER browser_binding_immutable;UPDATE browser_operations SET {assignment};{trigger};PRAGMA ignore_check_constraints=OFF;")).unwrap();
        let before: Vec<u8> = db
            .query_row("SELECT payload FROM browser_operations", [], |r| r.get(0))
            .unwrap();
        assert!(
            registry.historical_snapshot(&target()).is_err(),
            "{assignment}"
        );
        assert!(
            registry.reconcile(&target(), &Allow, &mut peer).is_err(),
            "{assignment}"
        );
        assert!(
            registry
                .dispatch(target(), b"x", &Allow, &mut peer)
                .is_err(),
            "{assignment}"
        );
        assert_eq!(peer.calls, 1);
        assert_eq!(
            db.query_row("SELECT payload FROM browser_operations", [], |r| r
                .get::<_, Vec<u8>>(0))
                .unwrap(),
            before
        );
    }
}
#[test]
fn missing_schema_is_not_repaired_and_nonregistry_is_not_adopted() {
    for name in ["browser_binding_immutable", "browser_operations_retained"] {
        let temp = Temp::new();
        let mut registry = BrowserRegistry::open(temp.db()).unwrap();
        let mut peer = Spy::new("unknown");
        registry
            .dispatch(target(), b"x", &Allow, &mut peer)
            .unwrap();
        let db = Connection::open(temp.db()).unwrap();
        db.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
        assert!(BrowserRegistry::open(temp.db()).is_err());
        assert!(registry
            .dispatch(target(), b"x", &Allow, &mut peer)
            .is_err());
        assert!(registry.reconcile(&target(), &Allow, &mut peer).is_err());
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name=?1",
                [name],
                |r| r.get::<_, usize>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(peer.calls, 1);
    }
    let temp = Temp::new();
    let db = Connection::open(temp.db()).unwrap();
    db.execute_batch("CREATE TABLE unrelated(x)").unwrap();
    assert!(BrowserRegistry::open(temp.db()).is_err());
    assert!(BrowserRegistry::open(":memory:").is_err());
    assert!(BrowserRegistry::open("file:registry?mode=memory").is_err());
}
#[test]
fn capacity_denies_new_operations_but_preserves_exact_duplicate() {
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("unknown");
    for i in 0..CAPACITY {
        let mut t = target();
        t.binding.intent = format!("intent{i}");
        t.binding.operation = format!("operation{i}");
        registry.dispatch(t, b"x", &Allow, &mut peer).unwrap();
    }
    assert!(registry
        .dispatch(target(), b"x", &Allow, &mut peer)
        .is_err());
    let mut t = target();
    t.binding.intent = "intent0".into();
    t.binding.operation = "operation0".into();
    assert_eq!(
        registry.dispatch(t, b"x", &Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls, CAPACITY as usize);
}

#[test]
fn committed_reservation_visible_during_callback_and_nested_readback_conflicts() {
    struct Nested {
        path: PathBuf,
    }
    impl Peer for Nested {
        fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
            let mut other = BrowserRegistry::open(&self.path)?;
            let saved = other.historical_snapshot(&target())?.unwrap();
            assert_eq!(saved.observation, Outcome::Unknown);
            assert_eq!(saved.generation, 0);
            let mut peer = Spy::new("rejected");
            assert_eq!(
                other.reconcile(&target(), &Allow, &mut peer)?,
                Outcome::Rejected
            );
            assert_eq!(peer.actions, vec!["reconcile"]);
            Ok(serde_json::to_vec(
                &json!({"version":VERSION,"request_sha256":digest(raw),"evidence_class":EVIDENCE_CLASS,"outcome":"confirmed"}),
            )?)
        }
    }
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    assert_eq!(
        registry
            .dispatch(target(), b"x", &Allow, &mut Nested { path: temp.db() })
            .unwrap(),
        Outcome::Unknown
    );
    let saved = registry.historical_snapshot(&target()).unwrap().unwrap();
    assert!(saved.conflict);
    assert_eq!(saved.observation, Outcome::Rejected);
}
#[test]
fn authority_loss_at_final_persistence_leaves_only_unknown_reservation() {
    struct Revoked {
        checks: AtomicUsize,
    }
    impl Authority for Revoked {
        fn verify(&self, _: &Target, _: &str, _: Action) -> bool {
            self.checks.fetch_add(1, Ordering::SeqCst) < 5
        }
    }
    let temp = Temp::new();
    let mut registry = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("confirmed");
    let authority = Revoked {
        checks: AtomicUsize::new(0),
    };
    assert!(registry
        .dispatch(target(), b"x", &authority, &mut peer)
        .is_err());
    assert_eq!(peer.calls, 1);
    let saved = registry.historical_snapshot(&target()).unwrap().unwrap();
    assert_eq!(saved.observation, Outcome::Unknown);
    assert_eq!(saved.generation, 0);
    assert_eq!(
        registry
            .dispatch(target(), b"x", &Allow, &mut peer)
            .unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls, 1);
}
