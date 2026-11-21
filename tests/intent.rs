#![cfg(feature = "verified-authority")]
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use runspindle_core::{
    authenticated::AuthenticatedCall,
    authority::{self, Action, Assertion, LocalHelper},
    envelope::Envelope,
    intent_runtime::IntentPorts,
    runtime::{self, PortAction, PortCall, PortStatus, SyntheticScript},
};
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    directory: PathBuf,
    helper: LocalHelper,
    wire: String,
    key: Ed25519KeyPair,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "runspindle-intent-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&directory).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        let helper = LocalHelper {
            database: directory.join("authority.sqlite"),
            issuer: "synthetic-issuer".into(),
            actor: "Utest".into(),
            audience: "core-1".into(),
            allowed_actions: vec![
                Action::Admit,
                Action::Dispatch,
                Action::Reconcile,
                Action::Cancel,
            ],
        };
        let c = Connection::open(&helper.database).unwrap();
        authority::initialize_synthetic_store(&c).unwrap();
        let now = authority::now_ms().unwrap();
        c.execute(
            "INSERT INTO authority_state VALUES(1,?1,?2)",
            params![now, now + 60_000],
        )
        .unwrap();
        c.execute(
            "INSERT INTO trust_roots VALUES(?1,'key-1',?2,?3,?4,0)",
            params![helper.issuer, key.public_key().as_ref(), now, now + 60_000],
        )
        .unwrap();
        let vectors: Vec<Value> =
            serde_json::from_str(include_str!("../conformance/intent-v1/vectors.json")).unwrap();
        let wire =
            String::from_utf8(runspindle_core::envelope::canonical(&vectors[0]).unwrap()).unwrap();
        Self {
            directory,
            helper,
            wire,
            key,
        }
    }
    fn assertion(&self) -> Assertion {
        let a = test_admission(self.wire.as_bytes());
        let now = authority::now_ms().unwrap();
        Assertion {
            version: runspindle_core::intent::ASSERTION_VERSION.into(),
            issuer: self.helper.issuer.clone(),
            key_id: "key-1".into(),
            actor: self.helper.actor.clone(),
            audience: self.helper.audience.clone(),
            grant_id: a.grant_ref,
            request_digest: format!("{:x}", Sha256::digest(self.wire.as_bytes())),
            actions: self.helper.allowed_actions.clone(),
            policy_revision: a.binding.policy_revision.clone(),
            target: a.binding,
            not_before_ms: now,
            expires_at_ms: now + 60_000,
        }
    }
    fn insert(&self, a: &Assertion, bad_signature: bool) {
        let signing = a.signing_bytes().unwrap_or_else(|_| {
            let mut bytes = authority::DOMAIN.to_vec();
            bytes.extend(runspindle_core::envelope::canonical(a).unwrap());
            bytes
        });
        let mut sig = self.key.sign(&signing).as_ref().to_vec();
        if bad_signature {
            sig[0] ^= 1;
        }
        Connection::open(&self.helper.database)
            .unwrap()
            .execute(
                "INSERT INTO grants VALUES(?1,?2,?3)",
                params![a.grant_id, serde_json::to_string(a).unwrap(), sig],
            )
            .unwrap();
    }
    fn ports(&self) -> IntentPorts {
        let a = test_admission(self.wire.as_bytes());
        let scripts = vec![SyntheticScript {
            admission: a,
            authorized: true,
            online: true,
            native_session_id: "session-1".into(),
            launch_lost_reply: false,
            crash_after_launch: false,
            terminal: None,
            cancel_acknowledged: true,
            observe_barrier: false,
        }];
        std::fs::write(
            self.directory.join("script.json"),
            serde_json::to_vec(&scripts).unwrap(),
        )
        .unwrap();
        IntentPorts {
            helper: Some(self.helper.clone()),
            script_directory: Some(self.directory.clone()),
        }
    }
    fn call(&self, action: PortAction, permission: Action) -> AuthenticatedCall {
        AuthenticatedCall {
            wire: self.wire.clone(),
            call: PortCall {
                admission: test_admission(self.wire.as_bytes()),
                action,
                native_session_id: None,
            },
            action: permission,
        }
    }
    fn assert_zero_calls(&self) {
        assert!(!self.directory.join("calls.jsonl").exists());
        assert!(!self
            .directory
            .join(format!("effect-{}.json", runtime::child_id("task-test", 1)))
            .exists());
    }
}

use runspindle_core::{
    intent::{IntentAdmission, Kind},
    intent_runtime::{workflow_id, IntentState},
};
fn test_admission(wire: &[u8]) -> runtime::Admission {
    IntentAdmission::decode(wire).unwrap().admission().unwrap()
}
fn vectors() -> Vec<IntentAdmission> {
    serde_json::from_str(include_str!("../conformance/intent-v1/vectors.json")).unwrap()
}
#[test]
fn canonical_vectors_and_semantic_tampering() {
    for v in vectors() {
        let b = v.encode().unwrap();
        assert_eq!(IntentAdmission::decode(&b).unwrap(), v);
    }
    let v = vectors().remove(0);
    for case in 0..6 {
        let mut bad = v.clone();
        match case {
            0 => bad.event.kind = Kind::Delete,
            1 => bad.event.source.channel_id = "other".into(),
            2 => bad.event.content_sha256 = "0".repeat(64),
            3 => bad.event.revision = 2,
            4 => bad.scoped.destination.account_id = "other".into(),
            _ => bad.event.event_id = "other".into(),
        };
        assert!(bad.encode().is_err());
    }
}
#[test]
fn ordering_tombstone_and_conflicts_preserve_history() {
    let vs = vectors();
    let mut s = IntentState::default();
    assert_eq!(s.admit(vs[1].clone()), "missing_predecessor");
    assert_eq!(s.admit(vs[0].clone()), "admitted");
    assert_eq!(s.admit(vs[0].clone()), "duplicate");
    assert_eq!(s.admit(vs[1].clone()), "predecessor_active");
    s.terminal_revision = Some(1);
    assert_eq!(s.admit(vs[1].clone()), "admitted");
    assert_eq!(s.admit(vs[2].clone()), "admitted");
    assert!(s.tombstone);
    assert!(s.pending.is_none());
    assert_eq!(s.history.len(), 3);
    assert_eq!(s.admit(vs[2].clone()), "duplicate");
    assert_eq!(s.admit(vs[0].clone()), "duplicate");
    assert!(s.pending.is_none());
    assert_eq!(workflow_id(&vs[0]).unwrap(), workflow_id(&vs[2]).unwrap());
}
#[test]
fn signatures_scope_time_and_unavailable_trust_deny_before_effect() {
    for case in 0..8 {
        let f = Fixture::new();
        let mut a = f.assertion();
        match case {
            0 => a.actor = "forged".into(),
            1 => a.expires_at_ms = 0,
            2 => a.request_digest = "0".repeat(64),
            3 => a.version = authority::VERSION.into(),
            4 => a.target.operation_id = "wrong".into(),
            5 => a.actions = vec![Action::Admit],
            _ => {}
        }
        f.insert(&a, case == 6);
        let mut ports = f.ports();
        if case == 7 {
            ports.helper = None;
        }
        let observed = ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap();
        f.assert_zero_calls();
        assert_eq!(observed.status, PortStatus::Denied);
    }
}
#[test]
fn valid_dispatch_then_revocation_has_independent_effect_observation() {
    let f = Fixture::new();
    f.insert(&f.assertion(), false);
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Acknowledged
    );
    let before = std::fs::read(f.directory.join("calls.jsonl")).unwrap();
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('v3-grant-1')", [])
        .unwrap();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    assert_eq!(
        std::fs::read(f.directory.join("calls.jsonl")).unwrap(),
        before
    );
}
#[test]
fn delete_cannot_launch_even_with_dispatch_permission() {
    let mut f = Fixture::new();
    let v = vectors().remove(2);
    f.wire = String::from_utf8(v.encode().unwrap()).unwrap();
    let mut v = v;
    let mut admission = v.admission().unwrap();
    admission.grant_ref = "v3-grant-1".into();
    v.scoped.request = Envelope::admission(v.event.event_id.clone(), &admission).unwrap();
    f.wire = String::from_utf8(v.encode().unwrap()).unwrap();
    f.insert(&f.assertion(), false);
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    f.assert_zero_calls();
}

#[test]
fn current_delete_grant_cancels_exact_prior_even_when_original_is_revoked() {
    let mut f = Fixture::new();
    let original = test_admission(f.wire.as_bytes());
    f.insert(&f.assertion(), false);
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Acknowledged
    );
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('v3-grant-1')", [])
        .unwrap();
    let mut deleted = vectors().remove(1);
    deleted.event.kind = Kind::Delete;
    deleted.event.content_sha256 = runspindle_core::intent::digest(b"");
    let mut a = deleted.scoped.admission().unwrap();
    a.normalized_digest = runspindle_core::intent::digest(
        &runspindle_core::envelope::canonical(&deleted.event).unwrap(),
    );
    deleted.scoped.request = Envelope::admission(deleted.event.event_id.clone(), &a).unwrap();
    f.wire = String::from_utf8(deleted.encode().unwrap()).unwrap();
    let mut assertion = f.assertion();
    assertion.actions = vec![Action::Admit, Action::Reconcile, Action::Cancel];
    f.insert(&assertion, false);
    let mut input = AuthenticatedCall {
        wire: f.wire.clone(),
        call: PortCall {
            admission: original,
            action: PortAction::Cancel,
            native_session_id: Some("session-1".into()),
        },
        action: Action::Cancel,
    };
    assert_eq!(
        ports.invoke(&input).unwrap().status,
        PortStatus::Acknowledged
    );
    let before = std::fs::read(f.directory.join("calls.jsonl")).unwrap();
    input.call.admission.binding.operation_id = "other".into();
    assert_eq!(ports.invoke(&input).unwrap().status, PortStatus::Denied);
    assert_eq!(
        std::fs::read(f.directory.join("calls.jsonl")).unwrap(),
        before
    );
}
fn next(old: &IntentAdmission, kind: Kind) -> IntentAdmission {
    let mut value = old.clone();
    value.event.kind = kind;
    value.event.revision += 1;
    value.event.event_id = format!("event-{}", value.event.revision);
    value.event.previous = Some(runspindle_core::intent::Prior {
        request: old.scoped.request.clone(),
        wire_sha256: runspindle_core::intent::digest(&old.encode().unwrap()),
    });
    if kind == Kind::Delete {
        value.event.content_sha256 = runspindle_core::intent::digest(b"");
    }
    let mut a = old.admission().unwrap();
    a.binding.revision = value.event.revision;
    a.binding.operation_id = format!("op-{}", value.event.revision);
    a.delivery_id = value.event.event_id.clone();
    a.grant_ref = format!("grant-{}", value.event.revision);
    a.predecessor_revision = Some(old.event.revision);
    a.normalized_digest = runspindle_core::intent::digest(
        &runspindle_core::envelope::canonical(&value.event).unwrap(),
    );
    value.scoped.request = Envelope::admission(value.event.event_id.clone(), &a).unwrap();
    value
}
#[test]
fn full_capacity_reserves_terminal_delete_and_never_reopens() {
    let mut state = IntentState::default();
    let mut v = vectors().remove(0);
    assert_eq!(state.admit(v.clone()), "admitted");
    for _ in 1..64 {
        state.terminal_revision = Some(v.event.revision);
        v = next(&v, Kind::Edit);
        assert_eq!(state.admit(v.clone()), "admitted");
    }
    state.terminal_revision = Some(v.event.revision);
    assert_eq!(state.admit(next(&v, Kind::Edit)), "capacity");
    assert_eq!(state.admit(next(&v, Kind::Delete)), "admitted");
    assert_eq!(state.history.len(), 65);
    assert!(state.tombstone);
    assert!(state.pending.is_none());
}
#[test]
fn source_cannot_choose_parallel_task_or_actor_intake() {
    let v = vectors().remove(0);
    let mut other = v.clone();
    other.scoped.request.binding.task_id = "other-task".into();
    assert!(other.encode().is_err());
    let mut actor = v.clone();
    actor.event.source.actor_id = "other-actor".into();
    let mut a = actor.scoped.admission().unwrap();
    a.normalized_digest = runspindle_core::intent::digest(
        &runspindle_core::envelope::canonical(&actor.event).unwrap(),
    );
    actor.scoped.request = Envelope::admission(actor.event.event_id.clone(), &a).unwrap();
    assert_eq!(workflow_id(&v).unwrap(), workflow_id(&actor).unwrap());
    let mut state = IntentState::default();
    assert_eq!(state.admit(v), "admitted");
    assert_eq!(state.admit(actor), "delivery_conflict");
    assert_eq!(state.history.len(), 1);
}
fn batch(intents: Vec<IntentAdmission>) -> runspindle_core::intent::IntentBatch {
    runspindle_core::intent::IntentBatch {
        version: runspindle_core::intent::BATCH_VERSION.into(),
        intents,
    }
}

#[test]
fn single_delete_records_only_unstarted_pending_intent_as_superseded() {
    let create = vectors().remove(0);
    let delete = next(&create, Kind::Delete);
    for started in [false, true] {
        let mut state = IntentState::default();
        assert_eq!(state.admit(create.clone()), "admitted");
        assert!(state.pending.is_some());
        if started {
            // The workflow consumes pending before scheduling the operation.
            state.pending.take();
        }
        assert_eq!(state.admit(delete.clone()), "admitted");
        assert!(state.pending.is_none());
        assert!(state.tombstone);
        assert_eq!(state.terminal_revision, None);
        let expected = if started { vec![] } else { vec![1] };
        assert_eq!(state.superseded_without_execution, expected);
        assert_eq!(state.admit(delete.clone()), "duplicate");
        assert_eq!(state.superseded_without_execution, expected);
    }
}

#[test]
fn batch_delete_keeps_previously_queued_and_new_skipped_revisions_in_order() {
    let chain = vectors();
    let mut state = IntentState::default();
    assert_eq!(state.admit(chain[0].clone()), "admitted");
    assert_eq!(state.install_batch(batch(chain.clone()), false), "admitted");
    assert_eq!(state.superseded_without_execution, vec![1, 2]);
    assert!(state.pending.is_none());
    assert_eq!(state.terminal_revision, None);
    assert_eq!(state.install_batch(batch(chain), false), "duplicate");
    assert_eq!(state.superseded_without_execution, vec![1, 2]);
}

#[test]
fn atomic_chain_never_schedules_superseded_creates_or_edits() {
    let chain = vectors();
    let mut s = IntentState::default();
    assert_eq!(s.install_batch(batch(chain.clone()), false), "admitted");
    assert!(s.pending.is_none());
    assert!(s.tombstone);
    assert_eq!(s.superseded_without_execution, vec![1, 2]);
    assert_eq!(s.terminal_revision, None);
    assert_eq!(s.install_batch(batch(chain.clone()), false), "duplicate");
    assert!(s.pending.is_none());
    assert_eq!(
        s.install_batch(batch(vec![chain[0].clone()]), false),
        "stale"
    );
    let mut s = IntentState::default();
    assert_eq!(s.admit(chain[0].clone()), "admitted");
    s.pending = None;
    assert_eq!(s.install_batch(batch(chain.clone()), true), "admitted");
    assert_eq!(s.superseded_without_execution, vec![2]);
    assert!(s.tombstone);
}
#[test]
fn latest_edit_batch_executes_latest_only_and_rejects_active_or_bad_prefix() {
    let mut chain = vectors();
    chain.pop();
    let mut s = IntentState::default();
    assert_eq!(s.install_batch(batch(chain.clone()), false), "admitted");
    assert_eq!(
        IntentAdmission::decode(s.pending.as_ref().unwrap().as_bytes())
            .unwrap()
            .event
            .revision,
        2
    );
    assert_eq!(s.superseded_without_execution, vec![1]);
    let mut active = IntentState::default();
    assert_eq!(active.admit(chain[0].clone()), "admitted");
    let before = serde_json::to_string(&active).unwrap();
    assert_eq!(
        active.install_batch(batch(chain.clone()), true),
        "predecessor_active"
    );
    assert_eq!(serde_json::to_string(&active).unwrap(), before);
    chain[1].event.previous.as_mut().unwrap().wire_sha256 = "0".repeat(64);
    assert_eq!(active.install_batch(batch(chain), false), "invalid");
    assert_eq!(serde_json::to_string(&active).unwrap(), before);
}
#[test]
fn latest_delete_authorizes_only_proven_same_scope_active_ancestor() {
    let mut f = Fixture::new();
    let original = test_admission(f.wire.as_bytes());
    f.insert(&f.assertion(), false);
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Acknowledged
    );
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('v3-grant-1')", [])
        .unwrap();
    let chain = vectors();
    f.wire = String::from_utf8(chain[2].encode().unwrap()).unwrap();
    f.insert(&f.assertion(), false);
    let mut input = AuthenticatedCall {
        wire: String::from_utf8(batch(chain.clone()).encode().unwrap()).unwrap(),
        call: PortCall {
            admission: original,
            action: PortAction::Cancel,
            native_session_id: Some("session-1".into()),
        },
        action: Action::Cancel,
    };
    assert_eq!(
        ports.invoke(&input).unwrap().status,
        PortStatus::Acknowledged
    );
    let before = std::fs::read(f.directory.join("calls.jsonl")).unwrap();
    let mut forged = batch(chain);
    forged.intents[0].event.source.channel_id = "other".into();
    input.wire = String::from_utf8(runspindle_core::envelope::canonical(&forged).unwrap()).unwrap();
    assert_eq!(ports.invoke(&input).unwrap().status, PortStatus::Denied);
    assert_eq!(
        std::fs::read(f.directory.join("calls.jsonl")).unwrap(),
        before
    );
}
