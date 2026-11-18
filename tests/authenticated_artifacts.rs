#![cfg(feature = "authenticated-artifacts")]
//! Public fixed seeds below are synthetic test fixtures, never issuer keys.
use ring::signature::{Ed25519KeyPair, KeyPair};
use runspindle_core::{authenticated_artifacts::*, durable, runtime::Binding};
use rusqlite::Connection;
use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

const BYTES: &[u8] = b"authenticated synthetic artifact\x00\xff\n";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    dir: PathBuf,
    cleanup: bool,
    scope: Scope,
}
fn pair(seed: u8) -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
struct FixedClock(u64);
impl Clock for FixedClock {
    fn now_ms(&self) -> Option<u64> {
        Some(self.0)
    }
}
#[derive(Clone)]
struct ExactTrust(TrustedState);
impl Trust for ExactTrust {
    fn current(&self, _: &Scope) -> Option<TrustedState> {
        Some(self.0.clone())
    }
}
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "core-artifact-evidence-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&dir).unwrap();
        Self::at(dir, true)
    }
    fn at(dir: PathBuf, cleanup: bool) -> Self {
        let mut store = durable::Store::open(dir.join("core.sqlite")).unwrap();
        let artifact = store
            .put(
                durable::VERSION,
                Binding {
                    task_id: "task".into(),
                    revision: 1,
                    operation_id: "operation".into(),
                    policy_revision: "policy".into(),
                    host_id: "host".into(),
                    workspace_id: "workspace".into(),
                    lease_epoch: 7,
                },
                "artifact".into(),
                1,
                BYTES,
            )
            .unwrap();
        Self {
            dir,
            cleanup,
            scope: Scope {
                artifact,
                byte_length: BYTES.len() as u64,
                producer: Producer {
                    principal: "native-producer".into(),
                    thread_id: "native-thread".into(),
                    turn_id: "native-turn".into(),
                    item_id: "native-item".into(),
                },
                validator: Validator {
                    principal: "validator-principal".into(),
                    revision: "validator-v1".into(),
                },
                authority_epoch: 11,
            },
        }
    }
    fn db(&self) -> Connection {
        Connection::open(self.dir.join("core.sqlite")).unwrap()
    }
    fn open(&self) -> AuthenticatedArtifacts {
        AuthenticatedArtifacts::open(self.dir.join("core.sqlite")).unwrap()
    }
    fn evidence(&self) -> Evidence {
        signed(self.scope.clone())
    }
    fn trust(&self) -> ExactTrust {
        trusted(self.scope.clone())
    }
    fn count(&self, table: &str) -> u64 {
        assert!([
            "validations",
            "acceptances",
            "authenticated_artifact_evidence",
            "artifacts"
        ]
        .contains(&table));
        self.db()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}
fn signer(key_id: &str) -> Signer {
    Signer {
        issuer: "synthetic-issuer".into(),
        key_id: key_id.into(),
        key_epoch: 3,
        audience: "core-fixture".into(),
        not_before_ms: 10,
        expires_at_ms: 1000,
    }
}
fn signed(scope: Scope) -> Evidence {
    let statement = ProducerStatement {
        version: VERSION.into(),
        signer: signer("producer-key"),
        scope: scope.clone(),
    };
    let producer = ProducerAttestation {
        signature: hex(pair(3).sign(&statement.signing_bytes().unwrap()).as_ref()),
        statement,
    };
    let statement = ValidatorStatement {
        version: VERSION.into(),
        signer: signer("validator-key"),
        scope,
        producer_sha256: producer.sha256().unwrap(),
        passed: true,
    };
    Evidence {
        producer,
        validator: ValidatorAttestation {
            signature: hex(pair(7).sign(&statement.signing_bytes().unwrap()).as_ref()),
            statement,
        },
    }
}
fn trusted(scope: Scope) -> ExactTrust {
    let key = |role, principal: String, name: &str, seed| TrustedKey {
        role,
        issuer: "synthetic-issuer".into(),
        key_id: name.into(),
        key_epoch: 3,
        principal,
        public_key: pair(seed).public_key().as_ref().try_into().unwrap(),
        not_before_ms: 10,
        expires_at_ms: 1000,
        revoked: false,
    };
    ExactTrust(TrustedState {
        producer: key(
            Role::Producer,
            scope.producer.principal.clone(),
            "producer-key",
            3,
        ),
        validator: key(
            Role::Validator,
            scope.validator.principal.clone(),
            "validator-key",
            7,
        ),
        scope,
        audience: "core-fixture".into(),
        not_before_ms: 10,
        expires_at_ms: 1000,
    })
}
fn resign_validator(e: &mut Evidence) {
    e.validator.statement.producer_sha256 = e.producer.sha256().unwrap();
    e.validator.signature = hex(pair(7)
        .sign(&e.validator.statement.signing_bytes().unwrap())
        .as_ref());
}
#[test]
fn signatures_bind_real_bytes_and_preserve_exact_acceptance() {
    let f = Fixture::new();
    let evidence = f.evidence();
    let mut store = f.open();
    assert!(store.accept(&evidence, &Deny, &FixedClock(100)).is_err());
    assert_eq!(f.count("acceptances"), 0);
    let accepted = store
        .accept(&evidence, &f.trust(), &FixedClock(100))
        .unwrap();
    assert_eq!(
        accepted.evidence_sha256(),
        durable::digest(&evidence.encode().unwrap())
    );
    let duplicate = store
        .accept(&evidence, &f.trust(), &FixedClock(100))
        .unwrap();
    assert_eq!(duplicate.accepted(), accepted.accepted());
    let (again, bytes) = store
        .read(accepted.accepted(), &f.trust(), &FixedClock(100))
        .unwrap();
    assert_eq!(again.evidence_sha256(), accepted.evidence_sha256());
    assert_eq!(bytes, BYTES);
    let db = f.db();
    let stored: Vec<u8> = db
        .query_row("SELECT bytes FROM artifacts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(stored, BYTES);
    // Independently calculated by Python hashlib over the literal fixture bytes.
    assert_eq!(
        evidence.producer.statement.scope.artifact.sha256,
        "1abd9939eb2ecfd5ea047c060e93e4b7bcfe6abf948d877247894d6dbac01594"
    );
    assert_eq!(
        durable::digest(&stored),
        evidence.producer.statement.scope.artifact.sha256
    );
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 1);
    }
    assert!(db
        .execute(
            "UPDATE authenticated_artifact_evidence SET evidence=x'00'",
            []
        )
        .is_err());
    assert!(db
        .execute("DELETE FROM authenticated_artifact_evidence", [])
        .is_err());
}
#[test]
fn forged_same_revision_validator_and_producer_signatures_are_rejected() {
    let f = Fixture::new();
    let mut store = f.open();
    let mut forged = f.evidence();
    forged.validator.signature = hex(pair(9)
        .sign(&forged.validator.statement.signing_bytes().unwrap())
        .as_ref());
    assert!(store.accept(&forged, &f.trust(), &FixedClock(100)).is_err());
    let mut forged = f.evidence();
    forged.producer.signature = hex(pair(9)
        .sign(&forged.producer.statement.signing_bytes().unwrap())
        .as_ref());
    resign_validator(&mut forged);
    assert!(store.accept(&forged, &f.trust(), &FixedClock(100)).is_err());
    // Correct validator key signing the wrong role domain remains invalid.
    let mut forged = f.evidence();
    let bytes = forged.validator.statement.signing_bytes().unwrap();
    let mut wrong_domain = PRODUCER_DOMAIN.to_vec();
    wrong_domain.extend(&bytes[VALIDATOR_DOMAIN.len()..]);
    forged.validator.signature = hex(pair(7).sign(&wrong_domain).as_ref());
    assert!(store.accept(&forged, &f.trust(), &FixedClock(100)).is_err());
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 0);
    }
}
#[test]
fn signed_native_identity_policy_principal_and_epoch_need_exact_authority() {
    let f = Fixture::new();
    let mut store = f.open();
    for change in 0..9 {
        let mut scope = f.scope.clone();
        match change {
            0 => scope.producer.thread_id = "other-thread".into(),
            1 => scope.producer.turn_id = "other-turn".into(),
            2 => scope.producer.item_id = "other-item".into(),
            3 => scope.producer.principal = "other-producer".into(),
            4 => scope.validator.principal = "other-validator".into(),
            5 => scope.validator.revision = "other-revision".into(),
            6 => scope.authority_epoch += 1,
            7 => scope.artifact.binding.policy_revision = "other-policy".into(),
            _ => scope.artifact.binding.lease_epoch += 1,
        }
        assert!(store
            .accept(&signed(scope), &f.trust(), &FixedClock(100))
            .is_err());
    }
    assert_eq!(f.count("acceptances"), 0);
}
#[test]
fn roles_epochs_revocation_audience_and_lifetimes_fail_closed() {
    let f = Fixture::new();
    let evidence = f.evidence();
    let mut store = f.open();
    for change in 0..8 {
        let mut trust = f.trust();
        match change {
            0 => trust.0.producer.role = Role::Validator,
            1 => trust.0.validator.role = Role::Producer,
            2 => trust.0.producer.key_epoch += 1,
            3 => trust.0.validator.revoked = true,
            4 => trust.0.audience = "other-core".into(),
            5 => trust.0.expires_at_ms = 100,
            6 => trust.0.producer.not_before_ms = 101,
            _ => trust.0.validator.expires_at_ms = 100,
        }
        assert!(store.accept(&evidence, &trust, &FixedClock(100)).is_err());
    }
    for time in [0, 9, 1000, 1001] {
        assert!(store
            .accept(&evidence, &f.trust(), &FixedClock(time))
            .is_err());
    }
    struct NoClock;
    impl Clock for NoClock {
        fn now_ms(&self) -> Option<u64> {
            None
        }
    }
    assert!(store.accept(&evidence, &f.trust(), &NoClock).is_err());
    assert_eq!(f.count("acceptances"), 0);
}
#[test]
fn byte_replacement_length_mismatch_and_failed_validation_never_accept() {
    let f = Fixture::new();
    let mut store = f.open();
    let mut scope = f.scope.clone();
    scope.byte_length += 1;
    assert!(store
        .accept(&signed(scope.clone()), &trusted(scope), &FixedClock(100))
        .is_err());
    let mut evidence = f.evidence();
    evidence.validator.statement.passed = false;
    resign_validator(&mut evidence);
    assert!(store
        .accept(&evidence, &f.trust(), &FixedClock(100))
        .is_err());
    f.db()
        .execute_batch("DROP TRIGGER artifacts_immutable;UPDATE artifacts SET bytes=x'00';")
        .unwrap();
    assert!(store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .is_err());
    assert_eq!(f.count("acceptances"), 0);
    assert_eq!(f.count("authenticated_artifact_evidence"), 0);
}
#[test]
fn legacy_acceptance_cannot_be_upgraded_or_inherit_authenticated_read() {
    struct Check;
    impl durable::Validator for Check {
        fn revision(&self) -> &str {
            "validator-v1"
        }
        fn validate(&self, _: &durable::ArtifactRef, _: &[u8], _: &str) -> bool {
            true
        }
    }
    struct Grant;
    impl durable::AcceptanceAuthority for Grant {
        fn verify(&self, _: &durable::Validation) -> bool {
            true
        }
    }
    let f = Fixture::new();
    let mut legacy = durable::Store::open(f.dir.join("core.sqlite")).unwrap();
    let validation = legacy
        .validate(durable::VERSION, &f.scope.artifact, "policy", &Check)
        .unwrap();
    let accepted = legacy
        .accept(durable::VERSION, &validation, &Grant)
        .unwrap();
    let mut store = f.open();
    assert!(store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .is_err());
    assert!(store.read(&accepted, &f.trust(), &FixedClock(100)).is_err());
    assert_eq!(legacy.accepted(&accepted).unwrap(), BYTES);
    assert_eq!(f.count("authenticated_artifact_evidence"), 0);
}
#[test]
fn evidence_failure_rolls_back_validation_and_acceptance_together() {
    let f = Fixture::new();
    let mut store = f.open();
    f.db().execute_batch("CREATE TRIGGER injected_evidence_failure BEFORE INSERT ON authenticated_artifact_evidence BEGIN SELECT RAISE(ABORT,'synthetic failure');END;").unwrap();
    assert!(store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .is_err());
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 0);
    }
    assert_eq!(f.count("artifacts"), 1);
    f.db()
        .execute_batch("DROP TRIGGER injected_evidence_failure;")
        .unwrap();
    store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
}
#[test]
fn revocation_at_final_commit_check_rolls_back_all_new_rows() {
    struct Revoke {
        state: TrustedState,
        calls: AtomicU64,
    }
    impl Trust for Revoke {
        fn current(&self, _: &Scope) -> Option<TrustedState> {
            (self.calls.fetch_add(1, Ordering::SeqCst) < 3).then(|| self.state.clone())
        }
    }
    let f = Fixture::new();
    let mut store = f.open();
    let trust = Revoke {
        state: f.trust().0,
        calls: AtomicU64::new(0),
    };
    assert!(store
        .accept(&f.evidence(), &trust, &FixedClock(100))
        .is_err());
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 0);
    }
}
#[test]
fn conflicting_authenticated_acceptances_race_without_mixed_evidence() {
    let f = Fixture::new();
    f.open();
    let mut scopes = [f.scope.clone(), f.scope.clone()];
    scopes[1].validator.revision = "validator-v2".into();
    // Open stores before any worker exists. Bounded start channels also prevent
    // a failed second spawn from leaving the first worker on an eternal barrier.
    let stores = [f.open(), f.open()];
    let workers = scopes
        .into_iter()
        .zip(stores)
        .map(|(scope, mut store)| {
            let (start, ready) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                ready.recv_timeout(std::time::Duration::from_secs(5))?;
                store.accept(&signed(scope.clone()), &trusted(scope), &FixedClock(100))
            });
            (start, worker)
        })
        .collect::<Vec<_>>();
    for (start, _) in &workers {
        start.send(()).unwrap();
    }
    let results = workers
        .into_iter()
        .map(|(_, h)| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 1);
    }
    let accepted = results.iter().find_map(|r| r.as_ref().ok()).unwrap();
    let wire: Vec<u8> = f
        .db()
        .query_row(
            "SELECT evidence FROM authenticated_artifact_evidence",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let scope = Evidence::decode(&wire).unwrap().producer.statement.scope;
    assert_eq!(
        accepted.accepted().validation.validator_revision,
        scope.validator.revision
    );
    assert_eq!(
        f.open()
            .read(accepted.accepted(), &trusted(scope), &FixedClock(100))
            .unwrap()
            .1,
        BYTES
    );
}
#[test]
fn authenticated_artifact_child() {
    let Ok(path) = std::env::var("CORE_AUTH_ARTIFACT_DIR") else {
        return;
    };
    let f = Fixture::at(PathBuf::from(path), false);
    let accepted = f
        .open()
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
    std::fs::write(
        f.dir.join("accepted.json"),
        serde_json::to_vec(accepted.accepted()).unwrap(),
    )
    .unwrap();
    std::process::exit(95); // Committed child exits without retaining a Store object.
}
struct BoundedChild(std::process::Child);
impl Drop for BoundedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn interrupted_acceptance_child() {
    let Ok(path) = std::env::var("CORE_AUTH_INTERRUPTED_DIR") else {
        return;
    };
    let mode = std::env::var("CORE_AUTH_INTERRUPTED_MODE").unwrap();
    if mode == "recover" {
        // Inspect before Fixture::at can put any bytes: recovery must not repair
        // a missing artifact or recreate acceptance state.
        let db = Connection::open(PathBuf::from(&path).join("core.sqlite")).unwrap();
        let count: u64 = db
            .query_row("SELECT count(*) FROM artifacts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let bytes: Vec<u8> = db
            .query_row("SELECT bytes FROM artifacts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(bytes, BYTES);
        for table in [
            "validations",
            "acceptances",
            "authenticated_artifact_evidence",
        ] {
            let count: u64 = db
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                count, 0,
                "uncommitted row survived before recovery: {table}"
            );
        }
    }
    let f = Fixture::at(PathBuf::from(path), false);
    if mode == "crash" {
        struct ExitBeforeCommit {
            state: TrustedState,
            calls: AtomicU64,
        }
        impl Trust for ExitBeforeCommit {
            fn current(&self, _: &Scope) -> Option<TrustedState> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 3 {
                    // No unwinding or Store/transaction destructor: SQLite must
                    // recover the uncommitted journal in the next process.
                    std::process::exit(96);
                }
                Some(self.state.clone())
            }
        }
        f.open()
            .accept(
                &f.evidence(),
                &ExitBeforeCommit {
                    state: f.trust().0,
                    calls: AtomicU64::new(0),
                },
                &FixedClock(100),
            )
            .unwrap();
        panic!("final precommit interruption was not reached");
    }
    let mut store = f.open();
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(
            f.count(table),
            0,
            "partial row survived process exit: {table}"
        );
    }
    assert_eq!(f.count("artifacts"), 1);
    let bytes: Vec<u8> = f
        .db()
        .query_row("SELECT bytes FROM artifacts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(bytes, BYTES);
    let accepted = store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
    let mut conflict = f.scope.clone();
    conflict.validator.revision = "validator-v2".into();
    assert!(store
        .accept(
            &signed(conflict.clone()),
            &trusted(conflict),
            &FixedClock(100)
        )
        .is_err());
    assert_eq!(
        store
            .read(accepted.accepted(), &f.trust(), &FixedClock(100))
            .unwrap()
            .1,
        BYTES
    );
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 1, "retry mixed evidence: {table}");
    }
    let wire: Vec<u8> = f
        .db()
        .query_row(
            "SELECT evidence FROM authenticated_artifact_evidence",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wire, f.evidence().encode().unwrap());
}
#[test]
fn fresh_process_precommit_exit_recovers_without_partial_acceptance() {
    let f = Fixture::new();
    for (mode, expected) in [("crash", 96), ("recover", 0)] {
        let mut child = BoundedChild(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "interrupted_acceptance_child", "--nocapture"])
                .env("CORE_AUTH_INTERRUPTED_DIR", &f.dir)
                .env("CORE_AUTH_INTERRUPTED_MODE", mode)
                // Keep the parent test-result line machine-readable. Child
                // assertion diagnostics remain on inherited stderr.
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(expected), "{mode}");
                break;
            }
            assert!(std::time::Instant::now() < end, "{mode} child timeout");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
#[test]
fn fresh_process_reopens_exact_evidence_and_rechecks_revocation() {
    let f = Fixture::new();
    let mut child = BoundedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "authenticated_artifact_child", "--nocapture"])
            .env("CORE_AUTH_ARTIFACT_DIR", &f.dir)
            .spawn()
            .unwrap(),
    );
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert_eq!(status.code(), Some(95));
            break;
        }
        assert!(std::time::Instant::now() < end, "child timeout");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let accepted: durable::Accepted =
        serde_json::from_slice(&std::fs::read(f.dir.join("accepted.json")).unwrap()).unwrap();
    let mut store = f.open();
    assert_eq!(
        store
            .read(&accepted, &f.trust(), &FixedClock(100))
            .unwrap()
            .1,
        BYTES
    );
    let mut revoked = f.trust();
    revoked.0.producer.revoked = true;
    assert!(store.read(&accepted, &revoked, &FixedClock(100)).is_err());
    assert!(store
        .read(&accepted, &f.trust(), &FixedClock(1000))
        .is_err());
    assert_eq!(f.count("acceptances"), 1);
    assert_eq!(f.count("authenticated_artifact_evidence"), 1);
}
#[test]
fn evidence_wire_and_missing_journal_fail_closed() {
    let f = Fixture::new();
    let evidence = f.evidence();
    let wire = evidence.encode().unwrap();
    assert_eq!(Evidence::decode(&wire).unwrap(), evidence);
    assert!(Evidence::decode(&[wire.as_slice(), b"\n"].concat()).is_err());
    let text = String::from_utf8(wire).unwrap();
    assert!(Evidence::decode(
        text.replacen(
            "\"authority_epoch\":11",
            "\"authority_epoch\":11,\"authority_epoch\":11",
            1
        )
        .as_bytes()
    )
    .is_err());
    for token in ["true", "11.0", "11e0", "-0"] {
        assert!(Evidence::decode(
            text.replacen(
                "\"authority_epoch\":11",
                &format!("\"authority_epoch\":{token}"),
                1
            )
            .as_bytes()
        )
        .is_err());
    }
    let mut value = serde_json::to_value(&evidence).unwrap();
    value["producer"]["statement"]["scope"]["producer"]["extra"] = serde_json::json!("untrusted");
    assert!(Evidence::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    f.open();
    f.db()
        .execute_batch("DROP TABLE authenticated_artifact_evidence;")
        .unwrap();
    assert!(AuthenticatedArtifacts::open(f.dir.join("core.sqlite")).is_err());
}
#[cfg(feature = "delivery-runtime")]
#[test]
fn authenticated_acceptance_is_not_a_delivery_grant() {
    use runspindle_core::outbox::{DeliveryReceipt, DeliveryRequest, Outbox, Transport};
    struct NoSend(u64);
    impl Transport for NoSend {
        fn send(
            &mut self,
            _: &DeliveryRequest,
            _: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            self.0 += 1;
            anyhow::bail!("forbidden send")
        }
        fn lookup(&mut self, _: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            self.0 += 1;
            anyhow::bail!("forbidden lookup")
        }
    }
    let f = Fixture::new();
    let accepted = f
        .open()
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
    let request = DeliveryRequest {
        version: durable::VERSION.into(),
        accepted: accepted.accepted().clone(),
        account_id: "account".into(),
        destination_id: "destination".into(),
        delivery_operation_id: "delivery-operation".into(),
    };
    let mut peer = NoSend(0);
    assert!(Outbox::open(f.dir.join("core.sqlite"))
        .unwrap()
        .deliver(&request, &durable::Deny, &mut peer)
        .is_err());
    assert_eq!(peer.0, 0);
    assert_eq!(
        f.db()
            .query_row("SELECT count(*) FROM outbox", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn conflicting_prior_validation_and_corrupt_retained_evidence_are_denied() {
    struct Failed;
    impl durable::Validator for Failed {
        fn revision(&self) -> &str {
            "validator-v1"
        }
        fn validate(&self, _: &durable::ArtifactRef, _: &[u8], _: &str) -> bool {
            false
        }
    }
    let f = Fixture::new();
    let mut legacy = durable::Store::open(f.dir.join("core.sqlite")).unwrap();
    legacy
        .validate(durable::VERSION, &f.scope.artifact, "policy", &Failed)
        .unwrap();
    assert!(f
        .open()
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .is_err());
    assert_eq!(f.count("acceptances"), 0);
    assert_eq!(f.count("validations"), 1);
    let f = Fixture::new();
    let mut store = f.open();
    let accepted = store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
    f.db().execute_batch("DROP TRIGGER authenticated_artifact_evidence_immutable;UPDATE authenticated_artifact_evidence SET evidence=x'7b7d';").unwrap();
    assert!(store
        .read(accepted.accepted(), &f.trust(), &FixedClock(100))
        .is_err());
    assert_eq!(f.count("acceptances"), 1);
}

#[test]
fn independent_literal_canonical_and_signature_vector() {
    let vector: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/authenticated-artifact-v1.json")).unwrap();
    let wire = vector["canonical_evidence"].as_str().unwrap().as_bytes();
    let evidence = Evidence::decode(wire).unwrap();
    let f = Fixture::new();
    assert_eq!(evidence, f.evidence());
    assert_eq!(evidence.encode().unwrap(), wire);
    assert_eq!(
        hex(&evidence.producer.statement.signing_bytes().unwrap()),
        vector["producer_signing_bytes_hex"].as_str().unwrap()
    );
    assert_eq!(
        hex(&evidence.validator.statement.signing_bytes().unwrap()),
        vector["validator_signing_bytes_hex"].as_str().unwrap()
    );
    assert_eq!(
        hex(&f.trust().0.producer.public_key),
        vector["producer_public_key_hex"].as_str().unwrap()
    );
    assert_eq!(
        hex(&f.trust().0.validator.public_key),
        vector["validator_public_key_hex"].as_str().unwrap()
    );
    assert_eq!(
        durable::digest(wire),
        vector["evidence_sha256"].as_str().unwrap()
    );
    assert_eq!(hex(BYTES), vector["artifact_bytes_hex"].as_str().unwrap());
    f.open()
        .accept(&evidence, &f.trust(), &FixedClock(100))
        .unwrap();
}
#[test]
fn duplicate_acceptance_denies_missing_or_corrupt_validation_without_repair() {
    for corruption in [
        "DROP TRIGGER validations_retained;DELETE FROM validations;",
        "DROP TRIGGER validations_immutable;UPDATE validations SET record='{}';",
    ] {
        let f = Fixture::new();
        let mut store = f.open();
        let accepted = store
            .accept(&f.evidence(), &f.trust(), &FixedClock(100))
            .unwrap();
        f.db().execute_batch(corruption).unwrap();
        let before: Vec<String> = f
            .db()
            .prepare("SELECT record FROM validations")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(store
            .accept(&f.evidence(), &f.trust(), &FixedClock(100))
            .is_err());
        assert!(store
            .read(accepted.accepted(), &f.trust(), &FixedClock(100))
            .is_err());
        let after: Vec<String> = f
            .db()
            .prepare("SELECT record FROM validations")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(before, after, "corrupt validation must not be repaired");
        assert_eq!(f.count("acceptances"), 1);
        let wire: Vec<u8> = f
            .db()
            .query_row(
                "SELECT evidence FROM authenticated_artifact_evidence",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(wire, f.evidence().encode().unwrap());
    }
}
#[test]
fn valid_changed_evidence_cannot_replace_an_existing_acceptance() {
    let f = Fixture::new();
    let mut store = f.open();
    let original = f.evidence();
    let accepted = store
        .accept(&original, &f.trust(), &FixedClock(100))
        .unwrap();
    let mut changed = original.clone();
    changed.producer.statement.signer.expires_at_ms = 999;
    changed.producer.signature = hex(pair(3)
        .sign(&changed.producer.statement.signing_bytes().unwrap())
        .as_ref());
    resign_validator(&mut changed);
    assert_ne!(changed.encode().unwrap(), original.encode().unwrap());
    let independent = Fixture::new();
    assert_eq!(independent.scope, f.scope);
    independent
        .open()
        .accept(&changed, &independent.trust(), &FixedClock(100))
        .unwrap();
    assert!(store
        .accept(&changed, &f.trust(), &FixedClock(100))
        .is_err());
    let (retained, bytes) = store
        .read(accepted.accepted(), &f.trust(), &FixedClock(100))
        .unwrap();
    assert_eq!(retained.evidence_sha256(), accepted.evidence_sha256());
    assert_eq!(bytes, BYTES);
    assert_eq!(f.count("authenticated_artifact_evidence"), 1);
}
#[test]
fn expiry_clock_rollback_and_post_read_authority_loss_fail_closed() {
    use std::{collections::VecDeque, sync::Mutex};
    struct SequenceClock(Mutex<VecDeque<Option<u64>>>);
    impl Clock for SequenceClock {
        fn now_ms(&self) -> Option<u64> {
            self.0.lock().unwrap().pop_front().flatten()
        }
    }
    for times in [
        vec![Some(100), Some(1000)],
        vec![Some(100), Some(99)],
        vec![Some(100), Some(100), Some(100), Some(100), None],
    ] {
        let f = Fixture::new();
        let mut store = f.open();
        let clock = SequenceClock(Mutex::new(times.into()));
        assert!(store.accept(&f.evidence(), &f.trust(), &clock).is_err());
        for table in [
            "validations",
            "acceptances",
            "authenticated_artifact_evidence",
        ] {
            assert_eq!(f.count(table), 0);
        }
    }
    struct RevokeAfterRead {
        state: TrustedState,
        calls: AtomicU64,
    }
    impl Trust for RevokeAfterRead {
        fn current(&self, _: &Scope) -> Option<TrustedState> {
            (self.calls.fetch_add(1, Ordering::SeqCst) < 2).then(|| self.state.clone())
        }
    }
    let f = Fixture::new();
    let mut store = f.open();
    let trust = RevokeAfterRead {
        state: f.trust().0,
        calls: AtomicU64::new(0),
    };
    assert!(store
        .accept(&f.evidence(), &trust, &FixedClock(100))
        .is_err());
    assert_eq!(trust.calls.load(Ordering::SeqCst), 3);
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 0);
    }
}

#[test]
fn retained_read_rechecks_authority_and_clock_after_byte_read() {
    use std::{collections::VecDeque, sync::Mutex};
    struct SequenceClock(Mutex<VecDeque<Option<u64>>>);
    impl Clock for SequenceClock {
        fn now_ms(&self) -> Option<u64> {
            self.0.lock().unwrap().pop_front().flatten()
        }
    }
    struct RevokeAfterRead {
        state: TrustedState,
        calls: AtomicU64,
    }
    impl Trust for RevokeAfterRead {
        fn current(&self, _: &Scope) -> Option<TrustedState> {
            (self.calls.fetch_add(1, Ordering::SeqCst) == 0).then(|| self.state.clone())
        }
    }
    let f = Fixture::new();
    let mut store = f.open();
    let accepted = store
        .accept(&f.evidence(), &f.trust(), &FixedClock(100))
        .unwrap();
    let trust = RevokeAfterRead {
        state: f.trust().0,
        calls: AtomicU64::new(0),
    };
    assert!(store
        .read(accepted.accepted(), &trust, &FixedClock(100))
        .is_err());
    assert_eq!(trust.calls.load(Ordering::SeqCst), 2);
    let clock = SequenceClock(Mutex::new(vec![Some(100), Some(100), None].into()));
    assert!(store.read(accepted.accepted(), &f.trust(), &clock).is_err());
    assert!(clock.0.lock().unwrap().is_empty());
    assert_eq!(
        store
            .read(accepted.accepted(), &f.trust(), &FixedClock(100))
            .unwrap()
            .1,
        BYTES
    );
    for table in [
        "validations",
        "acceptances",
        "authenticated_artifact_evidence",
    ] {
        assert_eq!(f.count(table), 1);
    }
}

#[cfg(feature = "delivery-runtime")]
mod delivery {
    use super::*;
    use runspindle_core::{authenticated_delivery::AuthenticatedOutbox, outbox::*};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    struct Live(Rc<RefCell<TrustedState>>);
    impl Trust for Live {
        fn current(&self, _: &Scope) -> Option<TrustedState> {
            Some(self.0.borrow().clone())
        }
    }
    struct Authority {
        calls: Cell<usize>,
        revoke_at: usize,
        live: Rc<RefCell<TrustedState>>,
        validator: bool,
    }
    impl DeliveryAuthority for Authority {
        fn verify(&self, _: &DeliveryRequest) -> bool {
            let n = self.calls.get() + 1;
            self.calls.set(n);
            if n == self.revoke_at {
                if self.validator {
                    self.live.borrow_mut().validator.revoked = true;
                } else {
                    self.live.borrow_mut().producer.revoked = true;
                }
            }
            true
        }
    }
    struct Grant;
    impl DeliveryAuthority for Grant {
        fn verify(&self, _: &DeliveryRequest) -> bool {
            true
        }
    }
    impl ResolutionAuthority for Grant {
        fn verify_resolution(&self, _: &ConflictResolution) -> bool {
            true
        }
    }
    struct Spy {
        sends: usize,
        reads: usize,
        live: Rc<RefCell<TrustedState>>,
        revoke_send: bool,
        revoke_read: bool,
        conflict: bool,
    }
    impl Spy {
        fn receipt(r: &DeliveryRequest) -> DeliveryReceipt {
            DeliveryReceipt {
                request: r.clone(),
                receipt_id: "receipt".into(),
            }
        }
    }
    impl Transport for Spy {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            b: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            assert_eq!(b, BYTES);
            self.sends += 1;
            if self.revoke_send {
                self.live.borrow_mut().producer.revoked = true;
            }
            Ok(Some(Self::receipt(r)))
        }
        fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            self.reads += 1;
            if self.revoke_read {
                self.live.borrow_mut().validator.revoked = true;
            }
            Ok(vec![Self::receipt(r)])
        }
        fn send_observation(
            &mut self,
            r: &DeliveryRequest,
            b: &[u8],
        ) -> anyhow::Result<SendObservation> {
            Ok(SendObservation {
                acknowledgement: self.send(r, b)?,
                conflict: self.conflict,
            })
        }
    }
    fn setup(f: &Fixture) -> (DeliveryRequest, Live, Spy) {
        let accepted = f
            .open()
            .accept(&f.evidence(), &f.trust(), &FixedClock(100))
            .unwrap();
        let live = Live(Rc::new(RefCell::new(f.trust().0)));
        let spy = Spy {
            sends: 0,
            reads: 0,
            live: live.0.clone(),
            revoke_send: false,
            revoke_read: false,
            conflict: false,
        };
        (
            DeliveryRequest {
                version: durable::VERSION.into(),
                accepted: accepted.accepted().clone(),
                account_id: "account".into(),
                destination_id: "destination".into(),
                delivery_operation_id: "delivery".into(),
            },
            live,
            spy,
        )
    }
    fn open(f: &Fixture) -> AuthenticatedOutbox {
        AuthenticatedOutbox::open(f.dir.join("core.sqlite")).unwrap()
    }
    fn snapshot(f: &Fixture, r: &DeliveryRequest) -> Option<DeliverySnapshot> {
        Outbox::open(f.dir.join("core.sqlite"))
            .unwrap()
            .snapshot(r)
            .unwrap()
    }
    #[test]
    fn authenticated_delivery_rechecks_each_callback_and_commit_without_nested_lock() {
        for validator in [false, true] {
            for at in 1..=6 {
                let f = Fixture::new();
                let (r, live, mut spy) = setup(&f);
                let a = Authority {
                    calls: Cell::new(0),
                    revoke_at: at,
                    live: live.0.clone(),
                    validator,
                };
                assert!(open(&f)
                    .deliver(&r, &a, &mut spy, &live, &FixedClock(100))
                    .is_err());
                assert_eq!(spy.sends, usize::from(at > 3));
                assert_eq!(spy.reads, usize::from(at > 4));
                assert!(snapshot(&f, &r).is_none_or(|s| s.state != DeliveryState::Delivered));
                // Restored authority can reconcile retained observations, never resend.
                *live.0.borrow_mut() = f.trust().0;
                if snapshot(&f, &r).is_some() {
                    open(&f)
                        .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                        .unwrap();
                    assert_eq!(spy.sends, usize::from(at > 3));
                }
            }
        }
        let f = Fixture::new();
        let (r, live, mut spy) = setup(&f);
        assert_eq!(
            open(&f)
                .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                .unwrap()
                .state,
            DeliveryState::Delivered
        );
        open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .unwrap();
        assert_eq!((spy.sends, spy.reads), (1, 2));
    }
    #[test]
    fn post_effect_revocation_preserves_unknown_and_never_resends() {
        for send in [false, true] {
            let f = Fixture::new();
            let (r, live, mut spy) = setup(&f);
            spy.revoke_send = send;
            spy.revoke_read = !send;
            assert!(open(&f)
                .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                .is_err());
            let s = snapshot(&f, &r).unwrap();
            assert_ne!(s.state, DeliveryState::Delivered);
            assert_eq!(spy.sends, 1);
            assert!(open(&f)
                .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                .is_err());
            assert_eq!(spy.sends, 1);
            *live.0.borrow_mut() = f.trust().0;
            spy.revoke_read = false;
            spy.revoke_send = false;
            open(&f)
                .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                .unwrap();
            assert_eq!(spy.sends, 1);
        }
    }
    #[test]
    fn legacy_evidence_loss_schema_loss_expiry_and_denied_delivery_never_call_transport() {
        for case in 0..5 {
            let f = Fixture::new();
            let (r, live, mut spy) = setup(&f);
            if case == 0 {
                f.db().execute_batch("DROP TRIGGER authenticated_artifact_evidence_retained; DELETE FROM authenticated_artifact_evidence;").unwrap();
            }
            if case == 1 {
                f.db()
                    .execute_batch("DROP TABLE authenticated_artifact_schema;")
                    .unwrap();
            }
            let mut out = open(&f);
            let result = if case == 2 {
                out.deliver(&r, &Grant, &mut spy, &live, &FixedClock(1000))
            } else if case == 3 {
                out.deliver(&r, &durable::Deny, &mut spy, &live, &FixedClock(100))
            } else if case == 4 {
                live.0.borrow_mut().validator.revoked = true;
                out.deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            } else {
                out.deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            };
            assert!(result.is_err());
            assert_eq!((spy.sends, spy.reads), (0, 0));
            assert!(snapshot(&f, &r).is_none());
        }
    }
    #[test]
    fn explicit_resolution_and_replay_require_current_artifact_and_separate_authority() {
        let f = Fixture::new();
        let (r, live, mut spy) = setup(&f);
        spy.conflict = true;
        assert!(open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .is_err());
        let resolution = ConflictResolution {
            prior: snapshot(&f, &r).unwrap(),
            receipt: Spy::receipt(&r),
            authorization_ref: "resolver".into(),
        };
        assert!(open(&f)
            .resolve(
                &resolution,
                &Grant,
                &durable::Deny,
                &mut spy,
                &live,
                &FixedClock(100)
            )
            .is_err());
        assert_eq!(spy.reads, 0);
        open(&f)
            .resolve(
                &resolution,
                &Grant,
                &Grant,
                &mut spy,
                &live,
                &FixedClock(100),
            )
            .unwrap();
        open(&f)
            .resolve(
                &resolution,
                &Grant,
                &Grant,
                &mut spy,
                &live,
                &FixedClock(100),
            )
            .unwrap();
        assert_eq!(spy.reads, 2);
        live.0.borrow_mut().producer.revoked = true;
        assert!(open(&f)
            .resolve(
                &resolution,
                &Grant,
                &Grant,
                &mut spy,
                &live,
                &FixedClock(100)
            )
            .is_err());
        assert_eq!((spy.sends, spy.reads), (1, 2));
    }

    struct NoClock;
    impl Clock for NoClock {
        fn now_ms(&self) -> Option<u64> {
            None
        }
    }
    #[test]
    fn absent_clock_and_missing_evidence_with_valid_schema_deny_without_effects() {
        let f = Fixture::new();
        let (r, live, mut spy) = setup(&f);
        assert!(open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &NoClock)
            .is_err());
        f.db().execute_batch("DROP TRIGGER authenticated_artifact_evidence_retained; DELETE FROM authenticated_artifact_evidence; CREATE TRIGGER authenticated_artifact_evidence_retained BEFORE DELETE ON authenticated_artifact_evidence BEGIN SELECT RAISE(ABORT,'retained artifact evidence');END").unwrap();
        assert!(open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .is_err());
        assert_eq!((spy.sends, spy.reads), (0, 0));
        assert!(snapshot(&f, &r).is_none());
    }
    struct RevokingResolver {
        calls: Cell<usize>,
        at: usize,
        live: Rc<RefCell<TrustedState>>,
    }
    impl ResolutionAuthority for RevokingResolver {
        fn verify_resolution(&self, _: &ConflictResolution) -> bool {
            let n = self.calls.get() + 1;
            self.calls.set(n);
            if n == self.at {
                self.live.borrow_mut().validator.revoked = true;
            }
            true
        }
    }
    #[test]
    fn resolver_callback_revocation_blocks_lookup_or_commit_including_exact_replay() {
        for replay in [false, true] {
            for at in 1..=4 {
                let f = Fixture::new();
                let (r, live, mut spy) = setup(&f);
                spy.conflict = true;
                assert!(open(&f)
                    .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                    .is_err());
                let resolution = ConflictResolution {
                    prior: snapshot(&f, &r).unwrap(),
                    receipt: Spy::receipt(&r),
                    authorization_ref: "resolver".into(),
                };
                if replay {
                    open(&f)
                        .resolve(
                            &resolution,
                            &Grant,
                            &Grant,
                            &mut spy,
                            &live,
                            &FixedClock(100),
                        )
                        .unwrap();
                }
                let before = snapshot(&f, &r).unwrap();
                let reads = spy.reads;
                let resolver = RevokingResolver {
                    calls: Cell::new(0),
                    at,
                    live: live.0.clone(),
                };
                assert!(open(&f)
                    .resolve(
                        &resolution,
                        &Grant,
                        &resolver,
                        &mut spy,
                        &live,
                        &FixedClock(100)
                    )
                    .is_err());
                assert_eq!(spy.reads - reads, usize::from(at > 2));
                assert_eq!(snapshot(&f, &r).unwrap(), before);
                assert_eq!(spy.sends, 1);
            }
        }
    }
    struct ResolutionPeer {
        calls: Rc<Cell<usize>>,
    }
    impl runspindle_core::tether_wire::Peer for ResolutionPeer {
        fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
            self.calls.set(self.calls.get() + 1);
            let v: serde_json::Value = serde_json::from_slice(raw)?;
            assert!(["resolve", "lookup_resolution"].contains(&v["action"].as_str().unwrap()));
            Ok(serde_json::to_vec(
                &serde_json::json!({"version":runspindle_core::tether_wire::RESOLUTION_VERSION,
                "request_sha256":durable::digest(raw),"decision_sha256":durable::digest(&serde_json::to_vec(&v["decision"])?),
                "evidence_class":"slack_wire_resolution_observation","state":"independently_reconciled_under_decision",
                "file_id":v["decision"]["file_id"],"message_id":v["decision"]["message_id"]}),
            )?)
        }
    }
    #[test]
    fn specialized_wire_resolution_first_call_is_guarded_and_repeat_reauthenticates() {
        use runspindle_core::tether_wire::{ResolutionBridge, ResolutionDecision};
        let f = Fixture::new();
        let (r, live, mut spy) = setup(&f);
        spy.conflict = true;
        assert!(open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .is_err());
        let pair = serde_json::to_vec(&("F123", "1700000000.000001")).unwrap();
        let resolution = ConflictResolution {
            prior: snapshot(&f, &r).unwrap(),
            receipt: DeliveryReceipt {
                request: r.clone(),
                receipt_id: format!("tether:{}", durable::digest(&pair)),
            },
            authorization_ref: "resolver".into(),
        };
        let decision = ResolutionDecision {
            version: "runspindle.wire-resolution.v1".into(),
            decision_id: "decision".into(),
            policy_ref: "policy".into(),
            authorization_ref: "resolver".into(),
            conflict_sha256: "a".repeat(64),
            core_resolution_sha256: durable::digest(
                &serde_json::to_vec(&serde_json::to_value(&resolution).unwrap()).unwrap(),
            ),
            file_id: "F123".into(),
            message_id: "1700000000.000001".into(),
        };
        let calls = Rc::new(Cell::new(0));
        let mut bridge = ResolutionBridge::new(
            &durable::Store::open(f.dir.join("core.sqlite")).unwrap(),
            resolution,
            decision,
            "file.txt",
            "text",
            ResolutionPeer {
                calls: calls.clone(),
            },
        )
        .unwrap();
        live.0.borrow_mut().producer.revoked = true;
        assert!(open(&f)
            .resolve_wire(&mut bridge, &Grant, &Grant, &live, &FixedClock(100))
            .is_err());
        assert_eq!(calls.get(), 0);
        *live.0.borrow_mut() = f.trust().0;
        open(&f)
            .resolve_wire(&mut bridge, &Grant, &Grant, &live, &FixedClock(100))
            .unwrap();
        assert_eq!(calls.get(), 2);
        open(&f)
            .resolve_wire(&mut bridge, &Grant, &Grant, &live, &FixedClock(100))
            .unwrap();
        assert_eq!(calls.get(), 4);
        let resolver = RevokingResolver {
            calls: Cell::new(0),
            at: 1,
            live: live.0.clone(),
        };
        assert!(open(&f)
            .resolve_wire(&mut bridge, &Grant, &resolver, &live, &FixedClock(100))
            .is_err());
        assert_eq!(calls.get(), 4);
    }

    #[test]
    fn authenticated_delivery_child() {
        let Ok(path) = std::env::var("CORE_AUTH_DELIVERY_DIR") else {
            return;
        };
        let f = Fixture::at(PathBuf::from(path), false);
        let (r, live, mut spy) = setup(&f);
        spy.revoke_send = true;
        assert!(open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .is_err());
        assert_eq!((spy.sends, spy.reads), (1, 0));
        std::fs::write(f.dir.join("effect-count"), b"1").unwrap();
        std::process::exit(95);
    }
    #[test]
    fn fresh_process_retains_post_effect_denial_without_resend() {
        let f = Fixture::new();
        let (r, live, mut spy) = setup(&f);
        let mut child = BoundedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "delivery::authenticated_delivery_child",
                    "--nocapture",
                ])
                .env("CORE_AUTH_DELIVERY_DIR", &f.dir)
                .spawn()
                .unwrap(),
        );
        let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(95));
                break;
            }
            assert!(std::time::Instant::now() < end, "child timeout");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(std::fs::read(f.dir.join("effect-count")).unwrap(), b"1");
        assert_ne!(snapshot(&f, &r).unwrap().state, DeliveryState::Delivered);
        open(&f)
            .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
            .unwrap();
        assert_eq!((spy.sends, spy.reads), (0, 1));
        assert_eq!(std::fs::read(f.dir.join("effect-count")).unwrap(), b"1");
    }

    struct Contradictory<'a> {
        live: &'a Live,
        reads: usize,
    }
    impl Transport for Contradictory<'_> {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            _: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            Ok(Some(Spy::receipt(r)))
        }
        fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            self.reads += 1;
            self.live.0.borrow_mut().producer.revoked = true;
            Ok(vec![DeliveryReceipt {
                request: r.clone(),
                receipt_id: "contradictory".into(),
            }])
        }
    }
    #[test]
    fn revoked_readback_still_retains_inferred_conflict_and_cannot_silently_clear() {
        for delivered in [false, true] {
            let f = Fixture::new();
            let (r, live, mut spy) = setup(&f);
            if delivered {
                open(&f)
                    .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                    .unwrap();
            }
            let mut wrong = Contradictory {
                live: &live,
                reads: 0,
            };
            assert!(open(&f)
                .deliver(&r, &Grant, &mut wrong, &live, &FixedClock(100))
                .is_err());
            assert_eq!(wrong.reads, 1);
            assert!(snapshot(&f, &r).unwrap().conflict);
            let evidence: i64 = f
                .db()
                .query_row(
                    "SELECT count(*) FROM outbox_evidence WHERE kind='conflict'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(evidence > 0);
            *live.0.borrow_mut() = f.trust().0;
            let reads = spy.reads;
            assert!(open(&f)
                .deliver(&r, &Grant, &mut spy, &live, &FixedClock(100))
                .is_err());
            assert_eq!(spy.reads, reads);
            assert!(snapshot(&f, &r).unwrap().conflict);
        }
    }
}

#[test]
fn malformed_evidence_diagnostics_do_not_echo_untrusted_members_or_values() {
    let f = Fixture::new();
    let evidence = f.evidence();
    let valid = serde_json::to_value(&evidence).unwrap();
    let canary = "synthetic-private-canary-DO-NOT-LOG";
    let mut cases = Vec::new();
    for pointer in [
        "",
        "/producer",
        "/producer/statement",
        "/producer/statement/scope",
        "/producer/statement/scope/producer",
        "/validator/statement/signer",
    ] {
        let mut value = valid.clone();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(canary.into(), serde_json::json!(canary));
        cases.push(serde_json::to_vec(&value).unwrap());
    }
    for pointer in [
        "/producer/statement/scope/byte_length",
        "/validator/statement/passed",
        "/producer/statement/signer/key_epoch",
    ] {
        let mut value = valid.clone();
        *value.pointer_mut(pointer).unwrap() = serde_json::json!(canary);
        cases.push(serde_json::to_vec(&value).unwrap());
    }
    cases.push(format!("{{\"{canary}\":").into_bytes());
    for bytes in cases {
        let error = Evidence::decode(&bytes).unwrap_err();
        assert_eq!(error.to_string(), "invalid artifact evidence encoding");
        assert_eq!(error.chain().count(), 1);
        assert!(!format!("{error:?}").contains(canary));
        assert!(!format!("{error:#}").contains(canary));
    }
    assert_eq!(
        Evidence::decode(&evidence.encode().unwrap()).unwrap(),
        evidence
    );
    assert_eq!(f.count("acceptances"), 0);
    assert_eq!(f.count("validations"), 0);
}
