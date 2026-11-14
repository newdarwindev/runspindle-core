#![cfg(feature = "delivery-runtime")]
use runspindle_core::{durable::*, outbox::*, runtime::Binding};
use rusqlite::{params, Connection};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
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
    if mode == "resolve" || mode == "resolve-crash" {
        let resolution: ConflictResolution = serde_json::from_slice(
            &std::fs::read(dir.join("resolution.json")).unwrap()).unwrap();
        p.receipt_override = Some(resolution.receipt.receipt_id.clone());
        let resolver = ResolveGrant { expected: resolution.clone(), calls: AtomicU64::new(0),
            crash_after_lock: mode == "resolve-crash" };
        let _ = o.resolve(&resolution, &Grant(r.clone()), &resolver, &mut p);
        return;
    }
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

struct ResolveGrant {
    expected: ConflictResolution,
    calls: AtomicU64,
    crash_after_lock: bool,
}
impl ResolutionAuthority for ResolveGrant {
    fn verify_resolution(&self, resolution: &ConflictResolution) -> bool {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.crash_after_lock && call == 4 {
            std::process::exit(97);
        }
        resolution == &self.expected
    }
}
fn resolver(resolution: &ConflictResolution) -> ResolveGrant {
    ResolveGrant { expected: resolution.clone(), calls: AtomicU64::new(0), crash_after_lock: false }
}
fn quarantine(t: &Temp) -> (DeliveryRequest, ConflictResolution, Peer) {
    let r = request(t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    let mut outbox = Outbox::open(t.db()).unwrap();
    outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap();
    peer.receipt_override = Some("receipt-resolved".into());
    assert!(outbox.reconcile(&r, &Grant(r.clone()), &mut peer).is_err());
    let resolution = ConflictResolution {
        prior: outbox.snapshot(&r).unwrap().unwrap(),
        receipt: DeliveryReceipt { request: r.clone(), receipt_id: "receipt-resolved".into() },
        authorization_ref: "trusted-resolution-reference".into(),
    };
    assert!(resolution.prior.conflict);
    (r, resolution, peer)
}
struct FreshGrant<'a>(&'a AtomicBool);
impl DeliveryAuthority for FreshGrant<'_> {
    fn verify(&self, _: &DeliveryRequest) -> bool { self.0.load(Ordering::SeqCst) }
}
struct RevokingPeer<'a> {
    peer: Peer,
    authorized: &'a AtomicBool,
    after_send: bool,
}
impl Transport for RevokingPeer<'_> {
    fn send(&mut self, request: &DeliveryRequest, bytes: &[u8]) -> anyhow::Result<Option<DeliveryReceipt>> {
        let reply = self.peer.send(request, bytes)?;
        if self.after_send { self.authorized.store(false, Ordering::SeqCst); }
        Ok(reply)
    }
    fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        let result = self.peer.lookup(request)?;
        self.authorized.store(false, Ordering::SeqCst);
        Ok(result)
    }
}
#[test]
fn revocation_during_readback_cannot_settle_delivery() {
    for after_send in [false, true] {
        let t = Temp::new();
        let r = request(&t);
        let authorized = AtomicBool::new(true);
        let mut peer = RevokingPeer { peer: Peer::open(&t.0.join("peer.sqlite")), authorized: &authorized, after_send };
        let mut outbox = Outbox::open(t.db()).unwrap();
        assert!(outbox.deliver(&r, &FreshGrant(&authorized), &mut peer).is_err());
        let pending = outbox.snapshot(&r).unwrap().unwrap();
        assert_eq!(pending.state, DeliveryState::Unknown);
        assert!(pending.receipt.is_none());
        assert_eq!(peer.peer.count("send"), 1);
        assert_eq!(peer.peer.count("lookup"), if after_send { 0 } else { 1 });
        authorized.store(true, Ordering::SeqCst);
        assert!(outbox.reconcile(&r, &FreshGrant(&authorized), &mut peer).is_err());
        assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), pending);
        assert_eq!(peer.peer.count("send"), 1);
    }
}
#[test]
fn conflict_resolution_requires_separate_authority_and_exact_readback() {
    let t = Temp::new();
    let (r, resolution, mut peer) = quarantine(&t);
    let mut outbox = Outbox::open(t.db()).unwrap();
    let before_lookup = peer.count("lookup");
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &Deny, &mut peer).is_err());
    assert_eq!(peer.count("lookup"), before_lookup);
    let mut wrong = resolution.clone();
    wrong.receipt.request.destination_id = "wrong-thread".into();
    assert!(outbox.resolve(&wrong, &Grant(r.clone()), &resolver(&wrong), &mut peer).is_err());
    wrong = resolution.clone();
    wrong.prior.generation += 1;
    assert!(outbox.resolve(&wrong, &Grant(r.clone()), &resolver(&wrong), &mut peer).is_err());
    assert_eq!(peer.count("lookup"), before_lookup);
    peer.ambiguous = true;
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &resolver(&resolution), &mut peer).is_err());
    peer.ambiguous = false;
    peer.wrong = true;
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &resolver(&resolution), &mut peer).is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    assert_eq!(peer.count("send"), 1);
}
#[test]
fn resolution_retains_evidence_bytes_and_never_resends_after_restart() {
    let t = Temp::new();
    let (r, resolution, mut peer) = quarantine(&t);
    let mut outbox = Outbox::open(t.db()).unwrap();
    let done = outbox.resolve(&resolution, &Grant(r.clone()), &resolver(&resolution), &mut peer).unwrap();
    assert!(!done.conflict);
    assert_eq!(done.generation, resolution.prior.generation + 1);
    assert_eq!(done.receipt, Some(resolution.receipt.clone()));
    assert_eq!(done.acknowledgement, resolution.prior.acknowledgement);
    drop(outbox);
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &resolver(&resolution), &mut peer).is_err());
    assert_eq!(outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap(), done);
    assert_eq!(peer.count("send"), 1);
    assert_eq!(Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(), BYTES);
    let db = Connection::open(t.db()).unwrap();
    let records: Vec<(String,String)> = db.prepare("SELECT kind,record FROM outbox_evidence ORDER BY generation").unwrap()
        .query_map([], |row| Ok((row.get(0)?,row.get(1)?))).unwrap().map(Result::unwrap).collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, "conflict");
    assert!(records[0].1.contains("external-observed-receipt"));
    assert!(records[0].1.contains("receipt-resolved"));
    assert_eq!(serde_json::from_str::<ConflictResolution>(&records[1].1).unwrap(), resolution);
    assert!(db.execute("DELETE FROM outbox_evidence", []).is_err());
    assert!(db.execute("UPDATE outbox_evidence SET record='{}'", []).is_err());
}
#[test]
fn resolution_revocation_during_readback_leaves_quarantine_unchanged() {
    let t = Temp::new();
    let (r, resolution, peer) = quarantine(&t);
    let authorized = AtomicBool::new(true);
    let mut peer = RevokingPeer { peer, authorized: &authorized, after_send: false };
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox.resolve(&resolution, &FreshGrant(&authorized), &resolver(&resolution), &mut peer).is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    assert_eq!(peer.peer.count("send"), 1);
}
#[test]
fn failed_resolution_commit_rolls_back_audit_and_state() {
    let t = Temp::new();
    let (r, resolution, mut peer) = quarantine(&t);
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("CREATE TRIGGER fail_resolution BEFORE UPDATE ON outbox WHEN NEW.conflict=0 BEGIN SELECT RAISE(ABORT,'synthetic commit failure'); END;").unwrap();
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &resolver(&resolution), &mut peer).is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    assert_eq!(db.query_row("SELECT count(*) FROM outbox_evidence WHERE kind='resolution'", [], |r| r.get::<_,u64>(0)).unwrap(), 0);
    assert_eq!(peer.count("send"), 1);
}
#[test]
fn fresh_process_resolution_crash_and_racing_resolvers_never_send() {
    let t = Temp::new();
    let (r, resolution, peer) = quarantine(&t);
    std::fs::write(t.0.join("request.json"), serde_json::to_vec(&r).unwrap()).unwrap();
    std::fs::write(t.0.join("resolution.json"), serde_json::to_vec(&resolution).unwrap()).unwrap();
    assert_eq!(child(&t.0, "resolve-crash").wait().unwrap().code(), Some(97));
    assert_eq!(Outbox::open(t.db()).unwrap().snapshot(&r).unwrap().unwrap(), resolution.prior);
    let mut children = (0..4).map(|_| child(&t.0, "resolve")).collect::<Vec<_>>();
    for c in &mut children { assert!(c.wait().unwrap().success()); }
    let done = Outbox::open(t.db()).unwrap().snapshot(&r).unwrap().unwrap();
    assert!(!done.conflict);
    assert_eq!(done.generation, resolution.prior.generation + 1);
    let db = Connection::open(t.db()).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM outbox_evidence WHERE kind='resolution'", [], |row| row.get::<_,u64>(0)).unwrap(), 1);
    assert_eq!(peer.count("send"), 1);
}

#[test]
fn authority_rechecked_after_settlement_lock() {
    struct CountingGrant(AtomicU64);
    impl DeliveryAuthority for CountingGrant {
        fn verify(&self, _: &DeliveryRequest) -> bool {
            self.0.fetch_add(1, Ordering::SeqCst) < 4
        }
    }
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox.deliver(&r, &CountingGrant(AtomicU64::new(0)), &mut peer).is_err());
    let pending = outbox.snapshot(&r).unwrap().unwrap();
    assert_eq!(pending.state, DeliveryState::Unknown);
    assert!(pending.receipt.is_none());
    assert_eq!(peer.count("send"), 1);
}
#[test]
fn resolution_authority_rechecked_after_write_lock() {
    struct UntilLock(AtomicU64);
    impl ResolutionAuthority for UntilLock {
        fn verify_resolution(&self, _: &ConflictResolution) -> bool {
            self.0.fetch_add(1, Ordering::SeqCst) < 3
        }
    }
    let t = Temp::new();
    let (r, resolution, mut peer) = quarantine(&t);
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox.resolve(&resolution, &Grant(r.clone()), &UntilLock(AtomicU64::new(0)), &mut peer).is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    let db = Connection::open(t.db()).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM outbox_evidence WHERE kind='resolution'", [], |row| row.get::<_,u64>(0)).unwrap(), 0);
    assert_eq!(peer.count("send"), 1);
}

fn audit_rejection_retains_quarantine(full: bool) {
    struct ManyReceipts(Peer);
    impl Transport for ManyReceipts {
        fn send(&mut self, r: &DeliveryRequest, b: &[u8]) -> anyhow::Result<Option<DeliveryReceipt>> {
            self.0.send(r, b)
        }
        fn lookup(&mut self, r: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            let receipt = self.0.lookup(r)?.remove(0);
            let size = serde_json::to_string(&receipt)?.len();
            Ok(vec![receipt; MAX_BYTES / size + 1])
        }
    }
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    let mut outbox = Outbox::open(t.db()).unwrap();
    let delivered = outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap();
    assert_eq!(delivered.state, DeliveryState::Delivered);
    assert!(!delivered.conflict);
    let db = Connection::open(t.db()).unwrap();
    if full {
        for generation in 0..CAPACITY {
            db.execute("INSERT INTO outbox_evidence VALUES('capacity-fixture',?1,'conflict','{}')", [generation]).unwrap();
        }
    }
    peer.receipt_override = Some("contradictory-receipt".into());
    let error = if full {
        outbox.reconcile(&r, &Grant(r.clone()), &mut peer).unwrap_err()
    } else {
        let mut many = ManyReceipts(peer);
        let error = outbox.reconcile(&r, &Grant(r.clone()), &mut many).unwrap_err();
        peer = many.0;
        error
    };
    assert!(error.to_string().contains("durably fenced; evidence capacity exhausted"));
    let fenced = outbox.snapshot(&r).unwrap().unwrap();
    assert!(fenced.conflict);
    assert_eq!(fenced.generation, delivered.generation + 1);
    assert_eq!(fenced.receipt, delivered.receipt);
    assert_eq!(fenced.acknowledgement, delivered.acknowledgement);
    assert_eq!(db.query_row("SELECT count(*) FROM outbox_evidence", [], |row| row.get::<_, u64>(0)).unwrap(), if full { CAPACITY } else { 0 });
    drop(outbox);
    peer.receipt_override = None;
    let before_lookup = peer.count("lookup");
    let mut reopened = Outbox::open(t.db()).unwrap();
    assert!(reopened.reconcile(&r, &Grant(r.clone()), &mut peer).is_err());
    assert!(reopened.deliver(&r, &Grant(r.clone()), &mut peer).is_err());
    assert_eq!(reopened.snapshot(&r).unwrap().unwrap(), fenced);
    assert_eq!(peer.count("lookup"), before_lookup);
    assert_eq!(peer.count("send"), 1);
    assert_eq!(Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(), BYTES);
}
#[test]
fn full_evidence_table_cannot_erase_observed_conflict() {
    audit_rejection_retains_quarantine(true);
}
#[test]
fn oversized_evidence_cannot_erase_observed_conflict() {
    audit_rejection_retains_quarantine(false);
}
