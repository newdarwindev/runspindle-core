#![cfg(feature = "artifact-store")]
use runspindle_core::browser_control as control;
use runspindle_core::browser_effect_registry::BrowserRegistry;
use runspindle_core::browser_effect_wire::*;
use runspindle_core::durable::digest;
use rusqlite::Connection;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
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

struct ControlGrant {
    upgrade: bool,
    proof: bool,
    release: bool,
    allow: bool,
}
impl control::Authority for ControlGrant {
    fn allow_schema_upgrade(&self) -> bool {
        self.upgrade
    }
    fn verify(&self, _: &control::Transition, _: Action) -> bool {
        self.allow
    }
    fn initialize_proven(&self, _: &control::Transition) -> bool {
        self.proof
    }
    fn released(&self, _: &control::Transition) -> bool {
        self.release
    }
}
impl control::InputAuthority for ControlGrant {
    fn verify(&self, _: &Target, _: &control::Holder) -> bool {
        self.allow
    }
}
fn grant() -> ControlGrant {
    ControlGrant {
        upgrade: true,
        proof: true,
        release: true,
        allow: true,
    }
}
fn initial() -> control::Transition {
    let mut t = target();
    t.binding.intent = control::intent(&t).unwrap();
    control::Transition {
        target: t,
        data: control::TransitionData {
            version: control::DOMAIN.into(),
            action: control::TransferKind::Initialize,
            source: None,
            destination: control::Holder {
                kind: control::HolderKind::Agent,
                identity: "agent".into(),
            },
            predecessor: None,
        },
    }
}
fn next(registry: &BrowserRegistry, old: &control::Transition) -> control::Transition {
    let snapshot = registry.historical_control(&old.target).unwrap().unwrap();
    let mut t = old.target.clone();
    t.binding.epoch += 1;
    t.binding.revision += 1;
    t.binding.operation = format!("operation-{}", t.binding.epoch);
    let takeover = old.data.destination.kind == control::HolderKind::Agent;
    control::Transition {
        target: t,
        data: control::TransitionData {
            version: control::DOMAIN.into(),
            action: if takeover {
                control::TransferKind::Takeover
            } else {
                control::TransferKind::Handback
            },
            source: Some(old.data.destination.clone()),
            destination: control::Holder {
                kind: if takeover {
                    control::HolderKind::Human
                } else {
                    control::HolderKind::Agent
                },
                identity: if takeover { "human" } else { "agent" }.into(),
            },
            predecessor: Some(control::Predecessor {
                operation: old.target.binding.operation.clone(),
                payload_digest: snapshot.payload_digest,
                generation: snapshot.generation,
            }),
        },
    }
}
#[test]
fn explicit_upgrade_preserves_existing_rows_and_reopens() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("confirmed");
    r.dispatch(target(), b"existing", &Allow, &mut peer)
        .unwrap();
    let mut denied = grant();
    denied.upgrade = false;
    assert!(r.upgrade_control_schema(&denied).is_err());
    r.upgrade_control_schema(&grant()).unwrap();
    drop(r);
    let r = BrowserRegistry::open(temp.db()).unwrap();
    assert_eq!(
        r.historical_snapshot(&target())
            .unwrap()
            .unwrap()
            .observation,
        Outcome::Confirmed
    );
    assert!(r.historical_control(&initial().target).unwrap().is_none());
}
#[test]
fn unknown_survives_reopen_no_refence_then_exact_readback() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    assert_eq!(
        r.dispatch_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls, 1);
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || Ok(()))
        .is_err());
    drop(r);
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    assert_eq!(peer.calls, 1);
    assert_eq!(
        r.reconcile_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Confirmed
    );
    assert_eq!(peer.actions, vec!["dispatch", "reconcile"]);
    r.with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || Ok(()))
        .unwrap();
}
#[test]
fn control_namespace_is_closed_to_generic_apis() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    let p = t.payload().unwrap();
    assert!(r.dispatch(t.target.clone(), &p, &Allow, &mut peer).is_err());
    assert!(r
        .execute(t.target.clone(), &p, Action::Dispatch, &Allow, &mut peer)
        .is_err());
    assert!(r.reconcile(&t.target, &Allow, &mut peer).is_err());
    assert!(r.historical_snapshot(&t.target).is_err());
    assert_eq!(peer.calls, 0);
}
#[test]
fn proof_release_and_pending_head_deny_admission() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    let mut denied = grant();
    denied.proof = false;
    assert!(r.dispatch_control(&t, &denied, &mut peer).is_err());
    assert_eq!(peer.calls, 0);
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    let pending = next(&r, &t);
    assert!(r.dispatch_control(&pending, &grant(), &mut peer).is_err());
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    let human = next(&r, &t);
    r.dispatch_control(&human, &grant(), &mut peer).unwrap();
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || Ok(()))
        .is_err());
    assert!(r.reconcile_control(&t, &grant(), &mut peer).is_err());
    r.reconcile_control(&human, &grant(), &mut peer).unwrap();
    let agent = next(&r, &human);
    denied = grant();
    denied.release = false;
    let calls = peer.calls;
    assert!(r.dispatch_control(&agent, &denied, &mut peer).is_err());
    assert_eq!(calls, peer.calls);
    r.dispatch_control(&agent, &grant(), &mut peer).unwrap();
    r.reconcile_control(&agent, &grant(), &mut peer).unwrap();
    r.with_control_input::<_, ()>(&agent.target, &agent.data.destination, &grant(), || Ok(()))
        .unwrap();
}
#[test]
fn predecessor_mutation_quarantines_entire_chain() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    let human = next(&r, &t);
    r.dispatch_control(&human, &grant(), &mut peer).unwrap();
    let db = Connection::open(temp.db()).unwrap();
    db.execute(
        "UPDATE browser_operations SET generation=generation+1 WHERE operation=?1",
        [&t.target.binding.operation],
    )
    .unwrap();
    let calls = peer.calls;
    assert!(r.reconcile_control(&human, &grant(), &mut peer).is_err());
    assert_eq!(calls, peer.calls);
    assert!(r.historical_control(&human.target).is_err());
}
#[test]
fn conflicting_readback_is_sticky_and_denies_input() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    peer.outcome = "rejected";
    assert_eq!(
        r.reconcile_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || Ok(()))
        .is_err());
    let calls = peer.calls;
    peer.outcome = "confirmed";
    assert_eq!(
        r.reconcile_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(calls, peer.calls);
}

#[test]
fn conflicting_cas_race_has_exactly_one_native_fence() {
    use std::sync::{Arc, Barrier};
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|index| {
            let path = temp.db();
            let barrier = barrier.clone();
            let mut t = t.clone();
            t.target.binding.operation = format!("racer-{index}");
            std::thread::spawn(move || {
                let mut r = BrowserRegistry::open(path).unwrap();
                let mut peer = Spy::new("confirmed");
                barrier.wait();
                let result = r.dispatch_control(&t, &grant(), &mut peer);
                (result.is_ok(), peer.calls)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.0).count(), 1);
    assert_eq!(results.iter().map(|r| r.1).sum::<usize>(), 1);
}
#[test]
fn input_writer_fence_serializes_transfer_reservation() {
    use std::sync::{Arc, Barrier};
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    let human = next(&r, &t);
    let barrier = Arc::new(Barrier::new(2));
    let gate = barrier.clone();
    let path = temp.db();
    // Open the second connection before the first takes its writer fence.
    let second = BrowserRegistry::open(path).unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut second = second;
        gate.wait();
        let mut peer = Spy::new("confirmed");
        let result = second.dispatch_control(&human, &grant(), &mut peer);
        send.send((result.is_ok(), peer.calls)).unwrap();
    });
    r.with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || {
        barrier.wait();
        assert!(recv
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err());
        Ok(())
    })
    .unwrap();
    assert_eq!(
        recv.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap(),
        (true, 1)
    );
    worker.join().unwrap();
}
#[test]
fn schema_upgrade_lost_authority_rolls_back_without_head_table() {
    use std::cell::Cell;
    struct Vanishing(Cell<u32>);
    impl control::Authority for Vanishing {
        fn allow_schema_upgrade(&self) -> bool {
            let n = self.0.get();
            self.0.set(n + 1);
            n < 2
        }
        fn verify(&self, _: &control::Transition, _: Action) -> bool {
            false
        }
    }
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    assert!(r.upgrade_control_schema(&Vanishing(Cell::new(0))).is_err());
    let db = Connection::open(temp.db()).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='browser_control_heads'",
            [],
            |r| r.get::<_, u32>(0)
        )
        .unwrap(),
        0
    );
}
#[test]
fn wrong_scope_holder_epoch_and_authority_never_run_input() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    let mut changed = t.target.clone();
    changed.frame = "other".into();
    assert!(r
        .with_control_input::<_, ()>(&changed, &t.data.destination, &grant(), || panic!(
            "must not run"
        ))
        .is_err());
    changed = t.target.clone();
    changed.binding.epoch += 1;
    assert!(r
        .with_control_input::<_, ()>(&changed, &t.data.destination, &grant(), || panic!(
            "must not run"
        ))
        .is_err());
    let mut holder = t.data.destination.clone();
    holder.identity = "imposter".into();
    assert!(r
        .with_control_input::<_, ()>(&t.target, &holder, &grant(), || panic!("must not run"))
        .is_err());
    let mut denied = grant();
    denied.allow = false;
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &denied, || panic!(
            "must not run"
        ))
        .is_err());
}
#[test]
fn reserved_control_capacity_is_bounded_separately() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let mut peer = Spy::new("confirmed");
    for index in 0..control::CAPACITY {
        let mut t = initial();
        t.target.binding.browser = format!("browser-{index}");
        t.target.binding.operation = format!("operation-{index}");
        t.target.binding.intent = control::intent(&t.target).unwrap();
        r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    }
    let mut over = initial();
    over.target.binding.browser = "overflow".into();
    over.target.binding.intent = control::intent(&over.target).unwrap();
    let calls = peer.calls;
    assert!(r.dispatch_control(&over, &grant(), &mut peer).is_err());
    assert_eq!(peer.calls, calls);
    // Control reservation exhaustion does not consume ordinary effect capacity.
    r.dispatch(target(), b"ordinary", &Allow, &mut peer)
        .unwrap();
}

#[test]
fn unknown_after_confirmation_quarantines_historical_holder() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("confirmed");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    r.reconcile_control(&t, &grant(), &mut peer).unwrap();
    peer.outcome = "unknown";
    assert_eq!(
        r.reconcile_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    let snapshot = r.historical_control(&t.target).unwrap().unwrap();
    assert_eq!(snapshot.observation, Outcome::Confirmed);
    assert!(snapshot.conflict);
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || panic!(
            "historical evidence is not current authority"
        ))
        .is_err());
    assert!(r
        .dispatch_control(&next(&r, &t), &grant(), &mut peer)
        .is_err());
}
#[test]
fn rejected_head_never_admits_input_or_new_transition() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    r.upgrade_control_schema(&grant()).unwrap();
    let t = initial();
    let mut peer = Spy::new("rejected");
    r.dispatch_control(&t, &grant(), &mut peer).unwrap();
    assert_eq!(
        r.reconcile_control(&t, &grant(), &mut peer).unwrap(),
        Outcome::Rejected
    );
    assert!(r
        .with_control_input::<_, ()>(&t.target, &t.data.destination, &grant(), || panic!(
            "rejected"
        ))
        .is_err());
    assert!(r
        .dispatch_control(&next(&r, &t), &grant(), &mut peer)
        .is_err());
}
#[test]
fn malformed_v1_metadata_is_not_upgraded() {
    let temp = Temp::new();
    let mut r = BrowserRegistry::open(temp.db()).unwrap();
    let mut peer = Spy::new("confirmed");
    r.dispatch(target(), b"x", &Allow, &mut peer).unwrap();
    let db = Connection::open(temp.db()).unwrap();
    let trigger: String = db
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='browser_binding_immutable'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    db.execute_batch("DROP TRIGGER browser_binding_immutable;")
        .unwrap();
    db.execute("UPDATE browser_operations SET identity='corrupt'", [])
        .unwrap();
    db.execute_batch(&trigger).unwrap();
    assert!(r.upgrade_control_schema(&grant()).is_err());
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        1
    );
}
#[test]
fn oversized_control_pointer_and_payload_deny_before_peer() {
    for corrupt_payload in [false, true] {
        let temp = Temp::new();
        let mut r = BrowserRegistry::open(temp.db()).unwrap();
        r.upgrade_control_schema(&grant()).unwrap();
        let t = initial();
        let mut peer = Spy::new("confirmed");
        r.dispatch_control(&t, &grant(), &mut peer).unwrap();
        let db = Connection::open(temp.db()).unwrap();
        db.pragma_update(None, "foreign_keys", false).unwrap(); // Corrupt only this disposable fixture.
        if corrupt_payload {
            let trigger: String = db
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE name='browser_binding_immutable'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            db.execute_batch("DROP TRIGGER browser_binding_immutable;")
                .unwrap();
            db.execute("UPDATE browser_operations SET payload=zeroblob(16385)", [])
                .unwrap();
            db.execute_batch(&trigger).unwrap();
        } else {
            db.execute(
                "UPDATE browser_control_heads SET operation=?1",
                ["x".repeat(129)],
            )
            .unwrap();
        }
        let calls = peer.calls;
        assert!(r.reconcile_control(&t, &grant(), &mut peer).is_err());
        assert_eq!(calls, peer.calls);
    }
}
