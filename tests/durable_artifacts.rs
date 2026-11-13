#![cfg(feature = "artifact-store")]
use runspindle_core::{durable::*, runtime::Binding};
use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "core-artifact-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn db(&self) -> PathBuf {
        self.0.join("core.sqlite")
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn binding() -> Binding {
    Binding {
        task_id: "task".into(),
        revision: 1,
        operation_id: "operation".into(),
        policy_revision: "policy".into(),
        host_id: "host".into(),
        workspace_id: "workspace".into(),
        lease_epoch: 1,
    }
}
const BYTES: &[u8] = b"bounded synthetic artifact\x00\xff\n";
struct Check {
    revision: String,
    pass: bool,
}
impl Validator for Check {
    fn revision(&self) -> &str {
        &self.revision
    }
    fn validate(&self, a: &ArtifactRef, b: &[u8], p: &str) -> bool {
        assert_eq!(b, BYTES);
        assert_eq!(
            a.sha256,
            "a028cdc6d0533f81488b4303c1e83005a8844ca0dfcf09678037170a2456cb60"
        );
        assert_eq!(p, "policy");
        self.pass
    }
}
struct Grant;
impl AcceptanceAuthority for Grant {
    fn verify(&self, v: &Validation) -> bool {
        v.artifact.binding == binding()
            && v.validator_revision == "validator-1"
            && v.policy_revision == "policy"
            && v.passed
    }
}
fn prepare(s: &mut Store, id: &str) -> (ArtifactRef, Validation) {
    let a = s.put(VERSION, binding(), id.into(), 1, BYTES).unwrap();
    let v = s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "validator-1".into(),
                pass: true,
            },
        )
        .unwrap();
    (a, v)
}
#[test]
fn actual_bytes_are_hashed_and_changed_identity_or_bytes_conflict() {
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let a = s
        .put(VERSION, binding(), "artifact".into(), 1, BYTES)
        .unwrap();
    assert_eq!(
        a.sha256,
        "a028cdc6d0533f81488b4303c1e83005a8844ca0dfcf09678037170a2456cb60"
    );
    assert_eq!(s.bytes(&a).unwrap(), BYTES);
    assert_eq!(
        s.put(VERSION, binding(), "artifact".into(), 1, BYTES)
            .unwrap(),
        a
    );
    assert!(s
        .put(VERSION, binding(), "artifact".into(), 1, b"changed")
        .is_err());
    let mut b = binding();
    b.lease_epoch += 1;
    assert_ne!(s.put(VERSION, b, "artifact".into(), 1, BYTES).unwrap(), a);
    let mut changed = a;
    changed.sha256 = "a".repeat(64);
    assert!(s.bytes(&changed).is_err());
    assert!(s.put("future", binding(), "new".into(), 1, BYTES).is_err());
    assert!(s
        .put(
            VERSION,
            binding(),
            "large".into(),
            1,
            &vec![0; MAX_BYTES + 1]
        )
        .is_err());
}
#[test]
fn validation_cannot_be_skipped_forged_overwritten_or_inherited() {
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let a = s
        .put(VERSION, binding(), "artifact".into(), 1, BYTES)
        .unwrap();
    let forged = Validation {
        artifact: a.clone(),
        validator_revision: "validator-1".into(),
        policy_revision: "policy".into(),
        passed: true,
    };
    assert!(s.accept(VERSION, &forged, &Grant).is_err());
    let failed = s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "failed-validator".into(),
                pass: false,
            },
        )
        .unwrap();
    assert!(s.accept(VERSION, &failed, &Grant).is_err());
    let v = s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "validator-1".into(),
                pass: true,
            },
        )
        .unwrap();
    assert!(s.accept(VERSION, &v, &Deny).is_err());
    let accepted = s.accept(VERSION, &v, &Grant).unwrap();
    assert_eq!(s.accepted(&accepted).unwrap(), BYTES);
    assert!(s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "validator-1".into(),
                pass: false
            }
        )
        .is_err());
    let mut changed = v.clone();
    changed.validator_revision = "other".into();
    assert!(s.accept(VERSION, &changed, &Grant).is_err());
    changed = v.clone();
    changed.policy_revision = "other".into();
    assert!(s.accept(VERSION, &changed, &Grant).is_err());
    changed = v;
    changed.artifact.artifact_revision = 2;
    assert!(s.accept(VERSION, &changed, &Grant).is_err());
}
#[test]
fn conflicting_concurrent_acceptances_are_atomic() {
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let (_, a) = prepare(&mut s, "first");
    let (_, b) = prepare(&mut s, "second");
    drop(s);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [a, b]
        .into_iter()
        .map(|v| {
            let path = t.db();
            let bar = barrier.clone();
            std::thread::spawn(move || {
                let mut s = Store::open(path).unwrap();
                bar.wait();
                s.accept(VERSION, &v, &Grant)
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    let s = Store::open(t.db()).unwrap();
    assert_eq!(
        s.accepted(results.iter().find_map(|r| r.as_ref().ok()).unwrap())
            .unwrap(),
        BYTES
    );
}
#[test]
fn child() {
    let Ok(path) = std::env::var("ARTIFACT_DB") else {
        return;
    };
    let mut s = Store::open(path).unwrap();
    let (_, v) = prepare(&mut s, "artifact");
    let a = s.accept(VERSION, &v, &Grant).unwrap();
    std::fs::write(
        std::env::var("ARTIFACT_REF").unwrap(),
        serde_json::to_vec(&a).unwrap(),
    )
    .unwrap();
    std::process::exit(95);
}
#[test]
fn fresh_process_after_acceptance_commit_reuses_exact_bytes() {
    let t = Temp::new();
    let reference = t.0.join("accepted.json");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture"])
        .env("ARTIFACT_DB", t.db())
        .env("ARTIFACT_REF", &reference)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(95));
    let accepted: Accepted = serde_json::from_slice(&std::fs::read(reference).unwrap()).unwrap();
    let mut s = Store::open(t.db()).unwrap();
    assert_eq!(s.accepted(&accepted).unwrap(), BYTES);
    assert_eq!(
        s.accept(VERSION, &accepted.validation, &Grant).unwrap(),
        accepted
    );
    let db = rusqlite::Connection::open(t.db()).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM acceptances", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert!(db.execute("UPDATE artifacts SET bytes=x'00'", []).is_err());
}
#[test]
fn bounded_validations_reconcile_known_entries_at_capacity() {
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let a = s
        .put(VERSION, binding(), "artifact".into(), 1, BYTES)
        .unwrap();
    for i in 0..CAPACITY {
        s.validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: format!("validator-{i}"),
                pass: true,
            },
        )
        .unwrap();
    }
    assert!(s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "new-validator".into(),
                pass: true
            }
        )
        .is_err());
    assert!(s
        .validate(
            VERSION,
            &a,
            "policy",
            &Check {
                revision: "validator-1".into(),
                pass: true
            }
        )
        .is_ok());
}

#[test]
fn invalid_validation_metadata_never_invokes_validator() {
    struct Spy;
    impl Validator for Spy {
        fn revision(&self) -> &str {
            "validator"
        }
        fn validate(&self, _: &ArtifactRef, _: &[u8], _: &str) -> bool {
            panic!("invalid policy reached validation port")
        }
    }
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let a = s
        .put(VERSION, binding(), "artifact".into(), 1, BYTES)
        .unwrap();
    assert!(s.validate(VERSION, &a, "other-policy", &Spy).is_err());
}
#[test]
fn stored_byte_tampering_cannot_inherit_acceptance() {
    let t = Temp::new();
    let mut s = Store::open(t.db()).unwrap();
    let (_, v) = prepare(&mut s, "artifact");
    let accepted = s.accept(VERSION, &v, &Grant).unwrap();
    let db = rusqlite::Connection::open(t.db()).unwrap();
    db.execute_batch("DROP TRIGGER artifacts_immutable;UPDATE artifacts SET bytes=x'0001';")
        .unwrap();
    assert!(s.accepted(&accepted).is_err());
    assert!(s.accept(VERSION, &v, &Grant).is_err());
}
