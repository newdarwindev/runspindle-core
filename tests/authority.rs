#![cfg(feature = "verified-authority")]
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use runspindle_core::{
    authenticated::{AuthenticatedCall, AuthenticatedPorts},
    authority::{self, Action, Assertion, Authority, LocalHelper},
    envelope::Envelope,
    runtime::{self, PortAction, PortCall, PortStatus, RegistryState, SyntheticScript},
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
            "runspindle-auth-{}-{}",
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
            actor: "actor-1".into(),
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
            serde_json::from_str(include_str!("../conformance/envelope-v1/vectors.json")).unwrap();
        let wire = String::from_utf8(
            Envelope::decode(&serde_json::to_vec(&vectors[0]["envelope"]).unwrap())
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        Self {
            directory,
            helper,
            wire,
            key,
        }
    }
    fn assertion(&self) -> Assertion {
        let a = authority::request(self.wire.as_bytes()).unwrap();
        let now = authority::now_ms().unwrap();
        Assertion {
            version: authority::VERSION.into(),
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
        let mut sig = self.key.sign(&a.signing_bytes().unwrap()).as_ref().to_vec();
        if bad_signature {
            sig[0] ^= 1;
        }
        Connection::open(&self.helper.database)
            .unwrap()
            .execute(
                "INSERT INTO grants VALUES('grant-1',?1,?2)",
                params![serde_json::to_string(a).unwrap(), sig],
            )
            .unwrap();
    }
    fn ports(&self) -> AuthenticatedPorts {
        let a = authority::request(self.wire.as_bytes()).unwrap();
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
        AuthenticatedPorts {
            helper: Some(self.helper.clone()),
            script_directory: Some(self.directory.clone()),
        }
    }
    fn call(&self, action: PortAction, permission: Action) -> AuthenticatedCall {
        AuthenticatedCall {
            wire: self.wire.clone(),
            call: PortCall {
                admission: authority::request(self.wire.as_bytes()).unwrap(),
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
            .join(format!("effect-{}.json", runtime::child_id("task-1", 1)))
            .exists());
    }
}
#[test]
fn signatures_binding_scope_time_and_actions_fail_closed_before_adapter() {
    for case in 0..16 {
        let f = Fixture::new();
        let mut a = f.assertion();
        match case {
            0 => a.actor = "forged".into(),
            1 => a.audience = "other-core".into(),
            2 => a.issuer = "unknown".into(),
            3 => a.key_id = "unknown".into(),
            4 => a.target.lease_epoch += 1,
            5 => a.policy_revision = "other-policy".into(),
            6 => a.request_digest = "a".repeat(64),
            7 => a.grant_id = "other-grant".into(),
            8 => a.expires_at_ms = 1,
            9 => a.not_before_ms = a.expires_at_ms - 1,
            10 => a.actions = vec![Action::Admit],
            11 => a.actions.push(Action::Admit),
            12 => a.version = "future".into(),
            13 => a.target.host_id = "other-host".into(),
            14 => a.target.operation_id = "other-operation".into(),
            _ => (),
        }
        f.insert(&a, case == 15);
        assert!(
            !f.helper.verify(f.wire.as_bytes(), Action::Dispatch),
            "case {case}"
        );
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
}
#[test]
fn current_revocation_stale_missing_and_conflicting_authoritative_state_deny() {
    for case in 0..7 {
        let f = Fixture::new();
        f.insert(&f.assertion(), false);
        assert!(f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        let c = Connection::open(&f.helper.database).unwrap();
        match case {
            0 => {
                c.execute("INSERT INTO revocations VALUES('grant-1')", [])
                    .unwrap();
            }
            1 => {
                c.execute("UPDATE trust_roots SET revoked=1", []).unwrap();
            }
            2 => {
                c.execute("UPDATE authority_state SET valid_until_ms=0", [])
                    .unwrap();
            }
            3 => {
                c.execute(
                    "UPDATE authority_state SET issued_at_ms=9223372036854775807",
                    [],
                )
                .unwrap();
            }
            4 => {
                c.execute_batch("DROP TABLE revocations").unwrap();
            }
            5 => {
                c.execute_batch("PRAGMA user_version=2").unwrap();
            }
            _ => {
                drop(c);
                std::fs::rename(&f.helper.database, f.directory.join("unavailable.sqlite"))
                    .unwrap();
            }
        }
        assert!(!f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        assert_eq!(
            f.ports()
                .invoke(&f.call(PortAction::Launch, Action::Dispatch))
                .unwrap()
                .status,
            PortStatus::Denied
        );
        f.assert_zero_calls();
    }
}
#[test]
fn separate_actions_exact_wire_and_replayed_request_keep_identity() {
    let f = Fixture::new();
    let mut a = f.assertion();
    a.actions = vec![Action::Admit, Action::Dispatch];
    f.insert(&a, false);
    assert!(f.helper.verify(f.wire.as_bytes(), Action::Admit));
    assert!(!f.helper.verify(f.wire.as_bytes(), Action::Cancel));
    assert!(!f.helper.verify(f.wire.as_bytes(), Action::Reconcile));
    assert!(!authority::Deny.verify(f.wire.as_bytes(), Action::Dispatch));
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Cancel, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Poll, Action::Reconcile))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    let mut swapped = f.call(PortAction::Launch, Action::Dispatch);
    swapped.call.admission.binding.host_id = "other".into();
    assert_eq!(ports.invoke(&swapped).unwrap().status, PortStatus::Denied);
    let mut e = Envelope::decode(f.wire.as_bytes()).unwrap();
    e.message_id = "message-2".into();
    assert!(!f.helper.verify(&e.encode().unwrap(), Action::Dispatch));
    assert!(!f
        .helper
        .verify(format!("{} ", f.wire).as_bytes(), Action::Dispatch));
    f.assert_zero_calls();
    let mut state = RegistryState::default();
    let admission = authority::request(f.wire.as_bytes()).unwrap();
    let first = state.admit(admission.clone());
    let second = state.admit(admission);
    assert_eq!(first.job_id, second.job_id);
    assert_eq!(second.status, "duplicate_delivery");
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Acknowledged
    );
    assert!(f
        .directory
        .join(format!("effect-{}.json", runtime::child_id("task-1", 1)))
        .exists());
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('grant-1')", [])
        .unwrap();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    let lines = std::fs::read_to_string(f.directory.join("calls.jsonl")).unwrap();
    assert_eq!(lines.lines().count(), 1);
}
#[test]
fn assertion_is_immutable_and_revocation_is_retained() {
    let f = Fixture::new();
    f.insert(&f.assertion(), false);
    let c = Connection::open(&f.helper.database).unwrap();
    assert!(c
        .execute("UPDATE grants SET signature=zeroblob(64)", [])
        .is_err());
    assert!(c.execute("DELETE FROM grants", []).is_err());
    c.execute("INSERT INTO revocations VALUES('grant-1')", [])
        .unwrap();
    assert!(c.execute("DELETE FROM revocations", []).is_err());
}

#[test]
fn cancellation_permission_does_not_require_reconciliation_permission() {
    let f = Fixture::new();
    let mut a = f.assertion();
    a.actions = vec![Action::Admit, Action::Dispatch, Action::Cancel];
    f.insert(&a, false);
    let ports = f.ports();
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Acknowledged
    );
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Verify, Action::Cancel))
            .unwrap()
            .status,
        PortStatus::Verified
    );
    let mut cancel = f.call(PortAction::Cancel, Action::Cancel);
    cancel.call.native_session_id = Some("session-1".into());
    assert_eq!(
        ports.invoke(&cancel).unwrap().status,
        PortStatus::Acknowledged
    );
    assert_eq!(
        ports
            .invoke(&f.call(PortAction::Poll, Action::Reconcile))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    let lines = std::fs::read_to_string(f.directory.join("calls.jsonl")).unwrap();
    assert_eq!(lines.lines().count(), 3);
}

#[test]
fn altered_assertion_and_missing_domain_are_not_valid_signatures() {
    for missing_domain in [false, true] {
        let f = Fixture::new();
        let mut a = f.assertion();
        let signature = if missing_domain {
            f.key
                .sign(&runspindle_core::envelope::canonical(&a).unwrap())
        } else {
            f.key.sign(&a.signing_bytes().unwrap())
        };
        if !missing_domain {
            a.expires_at_ms += 1;
        }
        Connection::open(&f.helper.database)
            .unwrap()
            .execute(
                "INSERT INTO grants VALUES('grant-1',?1,?2)",
                params![serde_json::to_string(&a).unwrap(), signature.as_ref()],
            )
            .unwrap();
        assert!(!f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        assert_eq!(
            f.ports()
                .invoke(&f.call(PortAction::Launch, Action::Dispatch))
                .unwrap()
                .status,
            PortStatus::Denied
        );
        f.assert_zero_calls();
    }
}

#[test]
fn duplicate_authoritative_identities_deny_even_with_valid_first_row() {
    for table in ["authority_state", "grants", "trust_roots"] {
        let f = Fixture::new();
        f.insert(&f.assertion(), false);
        assert!(f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        let c = Connection::open(&f.helper.database).unwrap();
        // Simulate malformed store preparation that retains the version but
        // loses a uniqueness constraint. Both copies are otherwise valid.
        c.execute_batch(&format!(
            "ALTER TABLE {table} RENAME TO original_{table};
             CREATE TABLE {table} AS SELECT * FROM original_{table};
             INSERT INTO {table} SELECT * FROM original_{table};"
        ))
        .unwrap();
        assert!(!f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        assert_eq!(
            f.ports()
                .invoke(&f.call(PortAction::Launch, Action::Dispatch))
                .unwrap()
                .status,
            PortStatus::Denied
        );
        f.assert_zero_calls();
    }
}
