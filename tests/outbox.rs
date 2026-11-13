#![cfg(feature = "delivery-runtime")]
use runspindle_core::{durable::*, outbox::*, runtime::Binding};
use rusqlite::{params, Connection};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
const BYTES: &[u8] = b"exact accepted synthetic bytes\x00\xff";
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "core-outbox-{}-{}",
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
struct Check;
impl Validator for Check {
    fn revision(&self) -> &str {
        "validator"
    }
    fn validate(&self, _: &ArtifactRef, b: &[u8], _: &str) -> bool {
        b == BYTES
    }
}
struct Accept;
impl AcceptanceAuthority for Accept {
    fn verify(&self, v: &Validation) -> bool {
        v.validator_revision == "validator" && v.passed && v.policy_revision == "policy"
    }
}
struct Grant(DeliveryRequest);
impl DeliveryAuthority for Grant {
    fn verify(&self, r: &DeliveryRequest) -> bool {
        r == &self.0
    }
}
fn request(t: &Temp) -> DeliveryRequest {
    let mut s = Store::open(t.db()).unwrap();
    let a = s
        .put(
            VERSION,
            Binding {
                task_id: "task".into(),
                revision: 1,
                operation_id: "native-operation".into(),
                policy_revision: "policy".into(),
                host_id: "host".into(),
                workspace_id: "workspace".into(),
                lease_epoch: 1,
            },
            "artifact".into(),
            1,
            BYTES,
        )
        .unwrap();
    let v = s.validate(VERSION, &a, "policy", &Check).unwrap();
    let accepted = s.accept(VERSION, &v, &Accept).unwrap();
    DeliveryRequest {
        version: VERSION.into(),
        accepted,
        account_id: "account".into(),
        destination_id: "destination".into(),
        delivery_operation_id: "delivery-operation".into(),
    }
}
struct Peer {
    db: Connection,
    lost: bool,
    before: bool,
    after: bool,
    ambiguous: bool,
    wrong: bool,
    receipt_override: Option<String>,
}
impl Peer {
    fn open(path: &Path) -> Self {
        let db = Connection::open(path).unwrap();
        db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY,action TEXT);CREATE TABLE IF NOT EXISTS receipts(operation TEXT PRIMARY KEY,receipt TEXT,bytes BLOB);").unwrap();
        Self {
            db,
            lost: false,
            before: false,
            after: false,
            ambiguous: false,
            wrong: false,
            receipt_override: None,
        }
    }
    fn count(&self, action: &str) -> u64 {
        self.db
            .query_row(
                "SELECT count(*) FROM calls WHERE action=?1",
                [action],
                |r| r.get(0),
            )
            .unwrap()
    }
}
impl Transport for Peer {
    fn send(&mut self, r: &DeliveryRequest, b: &[u8]) -> anyhow::Result<Option<DeliveryReceipt>> {
        if self.before {
            std::process::exit(92)
        }
        assert_eq!(b, BYTES);
        self.db
            .execute("INSERT INTO calls(action) VALUES('send')", [])?;
        let receipt = DeliveryReceipt {
            request: r.clone(),
            receipt_id: "external-observed-receipt".into(),
        };
        self.db.execute(
            "INSERT INTO receipts VALUES(?1,?2,?3)",
            params![r.delivery_operation_id, serde_json::to_string(&receipt)?, b],
        )?;
        if self.after {
            std::process::exit(94)
        }
        Ok(if self.lost { None } else { Some(receipt) })
    }
    fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        self.db
            .execute("INSERT INTO calls(action) VALUES('lookup')", [])?;
        let mut stmt = self
            .db
            .prepare("SELECT receipt FROM receipts WHERE operation=?1")?;
        let mut receipts = stmt
            .query_map([&r.delivery_operation_id], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str::<DeliveryReceipt>(&row?)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for receipt in &mut receipts {
            if self.wrong {
                receipt.request.destination_id = "other-destination".into()
            }
            if let Some(id) = &self.receipt_override {
                receipt.receipt_id = id.clone();
            }
        }
        if self.ambiguous {
            receipts.extend(receipts.clone());
        }
        Ok(receipts)
    }
}
#[test]
fn lost_reply_reconciles_exact_bytes_without_another_send() {
    let t = Temp::new();
    let r = request(&t);
    let mut p = Peer::open(&t.0.join("peer.sqlite"));
    p.lost = true;
    let mut o = Outbox::open(t.db()).unwrap();
    assert_eq!(
        o.deliver(&r, &Grant(r.clone()), &mut p).unwrap().state,
        DeliveryState::Unknown
    );
    assert_eq!(p.count("lookup"), 0);
    drop(o);
    let mut o = Outbox::open(t.db()).unwrap();
    let done = o.deliver(&r, &Grant(r.clone()), &mut p).unwrap();
    assert_eq!(done.state, DeliveryState::Delivered);
    assert_eq!(p.count("send"), 1);
    let bytes: Vec<u8> =
        p.db.query_row("SELECT bytes FROM receipts", [], |row| row.get(0))
            .unwrap();
    assert_eq!(bytes, BYTES);
    assert_eq!(digest(&bytes), r.accepted.artifact().sha256);
    assert_eq!(o.deliver(&r, &Grant(r.clone()), &mut p).unwrap(), done);
    assert_eq!(p.count("send"), 1);
}
#[test]
fn authority_version_destination_and_operation_bindings_fail_closed() {
    let t = Temp::new();
    let r = request(&t);
    let mut p = Peer::open(&t.0.join("peer.sqlite"));
    let mut o = Outbox::open(t.db()).unwrap();
    assert!(o.deliver(&r, &Deny, &mut p).is_err());
    assert!(o.snapshot(&r).unwrap().is_none());
    let mut reused = r.clone();
    reused.delivery_operation_id = reused.accepted.artifact().binding.operation_id.clone();
    assert!(!reused.valid());
    assert!(o.deliver(&reused, &Grant(reused.clone()), &mut p).is_err());
    let mut future = r.clone();
    future.version = "future".into();
    assert!(o.deliver(&future, &Grant(future.clone()), &mut p).is_err());
    let done = o.deliver(&r, &Grant(r.clone()), &mut p).unwrap();
    for field in 0..4 {
        let mut changed = r.clone();
        match field {
            0 => changed.account_id = "other".into(),
            1 => changed.destination_id = "other".into(),
            2 => changed.accepted.validation.validator_revision = "other".into(),
            _ => changed.accepted.validation.artifact.sha256 = "a".repeat(64),
        }
        assert!(o
            .deliver(&changed, &Grant(changed.clone()), &mut p)
            .is_err());
    }
    assert_eq!(o.snapshot(&r).unwrap().unwrap(), done);
    assert_eq!(p.count("send"), 1);
}
#[test]
fn ambiguous_wrong_and_conflicting_receipts_never_overwrite_delivery() {
    let t = Temp::new();
    let r = request(&t);
    let mut p = Peer::open(&t.0.join("peer.sqlite"));
    p.ambiguous = true;
    let mut o = Outbox::open(t.db()).unwrap();
    assert_eq!(
        o.deliver(&r, &Grant(r.clone()), &mut p).unwrap().state,
        DeliveryState::Unknown
    );
    assert!(o.reconcile(&r, &Grant(r.clone()), &mut p).is_err());
    p.ambiguous = false;
    p.wrong = true;
    assert!(o.reconcile(&r, &Grant(r.clone()), &mut p).is_err());
    p.wrong = false;
    let done = o.reconcile(&r, &Grant(r.clone()), &mut p).unwrap();
    p.receipt_override = Some("conflicting-external-receipt".into());
    assert!(o.reconcile(&r, &Grant(r.clone()), &mut p).is_err());
    let fenced = o.snapshot(&r).unwrap().unwrap();
    assert_eq!(fenced.receipt, done.receipt);
    assert_eq!(fenced.state, done.state);
    assert!(fenced.conflict);
    assert_eq!(p.count("send"), 1);
}
#[test]
fn fabricated_delivery_receipts_do_not_pass_readback() {
    struct Fabricated;
    impl Transport for Fabricated {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            _: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            Ok(Some(DeliveryReceipt {
                request: r.clone(),
                receipt_id: "fabricated".into(),
            }))
        }
        fn lookup(&mut self, _: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            Ok(vec![])
        }
    }
    let t = Temp::new();
    let r = request(&t);
    let mut o = Outbox::open(t.db()).unwrap();
    assert_eq!(
        o.deliver(&r, &Grant(r.clone()), &mut Fabricated)
            .unwrap()
            .state,
        DeliveryState::Unknown
    );
}
fn child(dir: &Path, mode: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_process", "--nocapture"])
        .env("OUTBOX_DIR", dir)
        .env("OUTBOX_MODE", mode)
        .spawn()
        .unwrap()
}
#[test]
fn child_process() {
    let Ok(dir) = std::env::var("OUTBOX_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let r: DeliveryRequest =
        serde_json::from_slice(&std::fs::read(dir.join("request.json")).unwrap()).unwrap();
    let mode = std::env::var("OUTBOX_MODE").unwrap();
    let mut p = Peer::open(&dir.join("peer.sqlite"));
    p.before = mode == "before";
    p.after = mode == "after";
    let mut o = Outbox::open(dir.join("core.sqlite")).unwrap();
    let _ = o.deliver(&r, &Grant(r.clone()), &mut p);
}
#[test]
fn fresh_process_reservation_and_lost_ack_never_resend() {
    for mode in ["before", "after"] {
        let t = Temp::new();
        let r = request(&t);
        std::fs::write(t.0.join("request.json"), serde_json::to_vec(&r).unwrap()).unwrap();
        assert_eq!(
            child(&t.0, mode).wait().unwrap().code(),
            Some(if mode == "before" { 92 } else { 94 })
        );
        assert!(child(&t.0, "restart").wait().unwrap().success());
        let mut o = Outbox::open(t.db()).unwrap();
        let mut p = Peer::open(&t.0.join("peer.sqlite"));
        if mode == "before" {
            assert!(o.reconcile(&r, &Grant(r.clone()), &mut p).is_err());
            assert_eq!(
                o.snapshot(&r).unwrap().unwrap().state,
                DeliveryState::Unknown
            );
            assert_eq!(p.count("send"), 0);
        } else {
            assert_eq!(
                o.reconcile(&r, &Grant(r.clone()), &mut p).unwrap().state,
                DeliveryState::Delivered
            );
            assert_eq!(p.count("send"), 1);
        }
        let s = Store::open(t.db()).unwrap();
        assert_eq!(s.accepted(&r.accepted).unwrap(), BYTES);
    }
}
#[test]
fn concurrent_process_owners_send_at_most_once() {
    let t = Temp::new();
    let r = request(&t);
    drop(Outbox::open(t.db()).unwrap());
    drop(Peer::open(&t.0.join("peer.sqlite")));
    std::fs::write(t.0.join("request.json"), serde_json::to_vec(&r).unwrap()).unwrap();
    let mut children = (0..6)
        .map(|_| child(&t.0, "concurrent"))
        .collect::<Vec<_>>();
    for c in &mut children {
        assert!(c.wait().unwrap().success());
    }
    let mut p = Peer::open(&t.0.join("peer.sqlite"));
    assert_eq!(p.count("send"), 1);
    assert_eq!(
        Outbox::open(t.db())
            .unwrap()
            .reconcile(&r, &Grant(r.clone()), &mut p)
            .unwrap()
            .state,
        DeliveryState::Delivered
    );
}

#[test]
fn bounded_outbox_preserves_unknown_work_and_duplicate_never_resends() {
    struct Counter(u64);
    impl Transport for Counter {
        fn send(
            &mut self,
            _: &DeliveryRequest,
            _: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            self.0 += 1;
            Ok(None)
        }
        fn lookup(&mut self, _: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            Ok(vec![])
        }
    }
    let t = Temp::new();
    let original = request(&t);
    let mut o = Outbox::open(t.db()).unwrap();
    let mut transport = Counter(0);
    for i in 0..CAPACITY {
        let mut r = original.clone();
        r.delivery_operation_id = format!("delivery-{i}");
        assert_eq!(
            o.deliver(&r, &Grant(r.clone()), &mut transport)
                .unwrap()
                .state,
            DeliveryState::Unknown
        );
    }
    assert!(o
        .deliver(&original, &Grant(original.clone()), &mut transport)
        .is_err());
    let mut duplicate = original.clone();
    duplicate.delivery_operation_id = "delivery-0".into();
    assert!(o
        .deliver(&duplicate, &Grant(duplicate.clone()), &mut transport)
        .is_err());
    assert_eq!(transport.0, CAPACITY);
    assert_eq!(
        o.snapshot(&duplicate).unwrap().unwrap().state,
        DeliveryState::Unknown
    );
}

#[test]
fn contradictory_acknowledgement_and_readback_are_fenced_across_restart() {
    struct Contradictory;
    impl Transport for Contradictory {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            _: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            Ok(Some(DeliveryReceipt {
                request: r.clone(),
                receipt_id: "receipt-A".into(),
            }))
        }
        fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            Ok(vec![DeliveryReceipt {
                request: r.clone(),
                receipt_id: "receipt-B".into(),
            }])
        }
    }
    let t = Temp::new();
    let r = request(&t);
    let mut o = Outbox::open(t.db()).unwrap();
    assert!(o
        .deliver(&r, &Grant(r.clone()), &mut Contradictory)
        .is_err());
    let prior = o.snapshot(&r).unwrap().unwrap();
    assert!(prior.conflict);
    assert_eq!(prior.state, DeliveryState::Unknown);
    assert_eq!(prior.acknowledgement.unwrap().receipt_id, "receipt-A");
    drop(o);
    let mut o = Outbox::open(t.db()).unwrap();
    assert!(o
        .reconcile(&r, &Grant(r.clone()), &mut Contradictory)
        .is_err());
    assert!(o.snapshot(&r).unwrap().unwrap().conflict);
}
