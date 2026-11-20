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
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
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
        let a = test_admission(self.wire.as_bytes());
        let now = authority::now_ms().unwrap();
        Assertion {
            version: if runspindle_core::scoped_authority::ScopedAdmission::decode(
                self.wire.as_bytes(),
            )
            .is_ok()
            {
                runspindle_core::scoped_authority::ASSERTION_VERSION
            } else {
                authority::VERSION
            }
            .into(),
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
                "INSERT INTO grants VALUES('grant-1',?1,?2)",
                params![serde_json::to_string(a).unwrap(), sig],
            )
            .unwrap();
    }
    fn ports(&self) -> AuthenticatedPorts {
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
        AuthenticatedPorts {
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
            .join(format!("effect-{}.json", runtime::child_id("task-1", 1)))
            .exists());
    }
}
#[test]
fn signatures_binding_scope_time_and_actions_fail_closed_before_adapter() {
    for (scoped, case) in (0..16).flat_map(|case| [(false, case), (true, case)]) {
        let f = if scoped {
            Fixture::scoped()
        } else {
            Fixture::new()
        };
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
            !(if scoped {
                f.helper.verify_scoped(f.wire.as_bytes(), Action::Dispatch)
            } else {
                f.helper.verify(f.wire.as_bytes(), Action::Dispatch)
            }),
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
fn authority_schema_reopen_child() {
    let Some(directory) = std::env::var_os("CORE_AUTHORITY_SCHEMA_TEST_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let ports = AuthenticatedPorts {
        helper: Some(LocalHelper::read(directory.join("helper.json")).unwrap()),
        script_directory: Some(directory.clone()),
    };
    let call =
        serde_json::from_slice(&std::fs::read(directory.join("call.json")).unwrap()).unwrap();
    let status = ports.invoke(&call).unwrap().status;
    std::fs::write(directory.join("result"), format!("{status:?}")).unwrap();
}

#[test]
fn invalid_revocation_schema_blocks_fresh_dispatch_without_effects() {
    for replacement in [
        "",
        "CREATE TRIGGER revocations_retained BEFORE DELETE ON revocations BEGIN SELECT 1;END;",
    ] {
        let f = Fixture::new();
        f.insert(&f.assertion(), false);
        assert!(f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        f.ports();
        std::fs::write(
            f.directory.join("helper.json"),
            serde_json::to_vec(&f.helper).unwrap(),
        )
        .unwrap();
        std::fs::write(
            f.directory.join("call.json"),
            serde_json::to_vec(&f.call(PortAction::Launch, Action::Dispatch)).unwrap(),
        )
        .unwrap();

        let db = Connection::open(&f.helper.database).unwrap();
        db.execute("INSERT INTO revocations VALUES('grant-1')", [])
            .unwrap();
        assert!(!f.helper.verify(f.wire.as_bytes(), Action::Dispatch));
        db.execute_batch("DROP TRIGGER revocations_retained;")
            .unwrap();
        db.execute_batch(replacement).unwrap();
        assert_eq!(db.execute("DELETE FROM revocations", []).unwrap(), 1);
        drop(db);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "authority_schema_reopen_child"])
            .env("CORE_AUTHORITY_SCHEMA_TEST_DIR", &f.directory)
            .spawn()
            .unwrap();
        let end = Instant::now() + Duration::from_secs(15);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < end => std::thread::sleep(Duration::from_millis(10)),
                other => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("authority schema child failed to finish: {other:?}");
                }
            }
        };
        assert!(status.success());
        f.assert_zero_calls();
        assert_eq!(
            std::fs::read_to_string(f.directory.join("result")).unwrap(),
            "Denied"
        );
        let db = Connection::open(&f.helper.database).unwrap();
        let retained: Option<String> = db
            .query_row(
                "SELECT (SELECT sql FROM sqlite_schema WHERE name='revocations_retained')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            retained.as_deref(),
            (!replacement.is_empty()).then_some(replacement.trim_end_matches(';'))
        );
    }
}

#[test]
fn absent_helper_cannot_use_script_authorized_flag() {
    let f = Fixture::new();
    f.insert(&f.assertion(), false);
    let mut ports = f.ports();
    ports.helper = None;
    for action in [PortAction::ObserveHost, PortAction::Launch] {
        assert_eq!(
            ports
                .invoke(&f.call(action, Action::Dispatch))
                .unwrap()
                .status,
            PortStatus::Denied
        );
    }
    f.assert_zero_calls();
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

fn test_admission(wire: &[u8]) -> runtime::Admission {
    authority::request(wire)
        .or_else(|_| {
            runspindle_core::scoped_authority::ScopedAdmission::decode(wire)
                .and_then(|s| s.admission())
        })
        .unwrap()
}
impl Fixture {
    fn scoped() -> Self {
        use runspindle_core::scoped_authority::*;
        let mut f = Self::new();
        f.helper.allowed_actions.push(Action::Deliver);
        f.wire = String::from_utf8(
            ScopedAdmission {
                version: VERSION.into(),
                request: Envelope::decode(f.wire.as_bytes()).unwrap(),
                destination: Destination {
                    account_id: "account-1".into(),
                    destination_id: "workspace-1.channel-1.thread-1".into(),
                    delivery_operation_id: "delivery-op-1".into(),
                },
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        f
    }
}
#[test]
fn scoped_exact_destination_and_version_cannot_be_changed() {
    use runspindle_core::scoped_authority::*;
    let f = Fixture::scoped();
    f.insert(&f.assertion(), false);
    assert!(f.helper.verify_scoped(f.wire.as_bytes(), Action::Admit));
    assert!(!f.helper.verify(f.wire.as_bytes(), Action::Admit));
    for field in ["account_id", "destination_id", "delivery_operation_id"] {
        let mut value: Value = serde_json::from_str(&f.wire).unwrap();
        value["destination"][field] = "other".into();
        let wire = runspindle_core::envelope::canonical(&value).unwrap();
        assert!(!f.helper.verify_scoped(&wire, Action::Dispatch));
        let mut call = f.call(PortAction::Launch, Action::Dispatch);
        call.wire = String::from_utf8(wire).unwrap();
        assert_eq!(f.ports().invoke(&call).unwrap().status, PortStatus::Denied);
    }
    let value = ScopedAdmission::decode(f.wire.as_bytes()).unwrap();
    assert!(!f
        .helper
        .verify(&value.request.encode().unwrap(), Action::Admit));
    f.assert_zero_calls();
}
#[test]
fn legacy_authority_never_grants_scoped_delivery() {
    let f = Fixture::new();
    f.insert(&f.assertion(), false);
    assert!(!f.helper.verify(f.wire.as_bytes(), Action::Deliver));
    assert!(!f.helper.verify_scoped(f.wire.as_bytes(), Action::Deliver));
}
#[test]
fn scoped_revocation_and_unavailable_helper_block_adapter_effects() {
    let f = Fixture::scoped();
    f.insert(&f.assertion(), false);
    assert!(f.helper.verify_scoped(f.wire.as_bytes(), Action::Dispatch));
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('grant-1')", [])
        .unwrap();
    assert_eq!(
        f.ports()
            .invoke(&f.call(PortAction::Launch, Action::Dispatch))
            .unwrap()
            .status,
        PortStatus::Denied
    );
    let mut ports = f.ports();
    ports.helper = None;
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
fn scoped_fresh_process_verification() {
    if let Ok(directory) = std::env::var("CORE16_SCOPED_TEST_DIRECTORY") {
        let directory = PathBuf::from(directory);
        let helper = LocalHelper::read(directory.join("helper.json")).unwrap();
        let wire = std::fs::read(directory.join("wire.json")).unwrap();
        let expected = std::env::var("CORE16_SCOPED_EXPECT").unwrap() == "true";
        assert_eq!(helper.verify_scoped(&wire, Action::Dispatch), expected);
        return;
    }
    let f = Fixture::scoped();
    f.insert(&f.assertion(), false);
    std::fs::write(
        f.directory.join("helper.json"),
        serde_json::to_vec(&f.helper).unwrap(),
    )
    .unwrap();
    std::fs::write(f.directory.join("wire.json"), &f.wire).unwrap();
    for expected in [true, false] {
        if !expected {
            Connection::open(&f.helper.database)
                .unwrap()
                .execute("INSERT INTO revocations VALUES('grant-1')", [])
                .unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "scoped_fresh_process_verification",
                "--nocapture",
            ])
            .env("CORE16_SCOPED_TEST_DIRECTORY", &f.directory)
            .env("CORE16_SCOPED_EXPECT", expected.to_string())
            .status()
            .unwrap();
        assert!(status.success());
    }
    f.assert_zero_calls();
}

#[test]
fn scoped_outbox_checks_exact_target_before_any_send_or_lookup() {
    use runspindle_core::{durable::*, outbox::*, scoped_authority::ScopedDeliveryAuthority};
    struct ValidatorOk;
    impl Validator for ValidatorOk {
        fn revision(&self) -> &str {
            "validator-1"
        }
        fn validate(&self, _: &ArtifactRef, bytes: &[u8], _: &str) -> bool {
            bytes == b"synthetic accepted bytes"
        }
    }
    struct AcceptOk;
    impl AcceptanceAuthority for AcceptOk {
        fn verify(&self, v: &Validation) -> bool {
            v.passed
        }
    }
    struct Peer {
        calls: usize,
    }
    impl Transport for Peer {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            bytes: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            self.calls += 1;
            assert_eq!(bytes, b"synthetic accepted bytes");
            Ok(Some(DeliveryReceipt {
                request: r.clone(),
                receipt_id: "receipt-1".into(),
            }))
        }
        fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            self.calls += 1;
            Ok(vec![DeliveryReceipt {
                request: r.clone(),
                receipt_id: "receipt-1".into(),
            }])
        }
    }
    let f = Fixture::scoped();
    f.insert(&f.assertion(), false);
    let path = f.directory.join("accepted.sqlite");
    let mut store = Store::open(&path).unwrap();
    let binding = test_admission(f.wire.as_bytes()).binding;
    let artifact = store
        .put(
            VERSION,
            binding,
            "artifact-1".into(),
            1,
            b"synthetic accepted bytes",
        )
        .unwrap();
    let validation = store
        .validate(VERSION, &artifact, "policy-1", &ValidatorOk)
        .unwrap();
    let accepted = store.accept(VERSION, &validation, &AcceptOk).unwrap();
    drop(store);
    let request = DeliveryRequest {
        version: VERSION.into(),
        accepted,
        account_id: "account-1".into(),
        destination_id: "workspace-1.channel-1.thread-1".into(),
        delivery_operation_id: "delivery-op-1".into(),
    };
    let grant = ScopedDeliveryAuthority {
        helper: &f.helper,
        wire: f.wire.as_bytes(),
    };
    let mut outbox = Outbox::open(&path).unwrap();
    let mut peer = Peer { calls: 0 };
    for field in 0..4 {
        let mut wrong = request.clone();
        match field {
            0 => wrong.account_id = "other".into(),
            1 => wrong.destination_id = "other".into(),
            2 => wrong.delivery_operation_id = "other".into(),
            _ => wrong.accepted.validation.artifact.binding.policy_revision = "other".into(),
        }
        assert!(outbox.deliver(&wrong, &grant, &mut peer).is_err());
        assert!(outbox.reconcile(&wrong, &grant, &mut peer).is_err());
    }
    assert_eq!(peer.calls, 0);
    let rows: i64 = Connection::open(&path)
        .unwrap()
        .query_row("SELECT count(*) FROM outbox", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 0);
    let delivered = outbox.deliver(&request, &grant, &mut peer).unwrap();
    assert_eq!(delivered.state, DeliveryState::Delivered);
    assert!(peer.calls > 0);
    let before = peer.calls;
    Connection::open(&f.helper.database)
        .unwrap()
        .execute("INSERT INTO revocations VALUES('grant-1')", [])
        .unwrap();
    assert!(outbox.deliver(&request, &grant, &mut peer).is_err());
    assert!(outbox.reconcile(&request, &grant, &mut peer).is_err());
    assert_eq!(peer.calls, before);
    drop(outbox);
    let reopened = Outbox::open(&path).unwrap();
    assert_eq!(reopened.snapshot(&request).unwrap().unwrap(), delivered);
}
