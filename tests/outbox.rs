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
fn outbox_rejects_sqlite_aliases_before_resolving_a_retained_dispatch() {
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    peer.lost = true;
    let pending = Outbox::open(t.db())
        .unwrap()
        .deliver(&r, &Grant(r.clone()), &mut peer)
        .unwrap();
    assert_eq!(pending.state, DeliveryState::Unknown);
    let disk_uri = format!("file:{}?mode=memory", t.db().display());
    for alias in ["", ":memory:", "file::memory:?cache=shared", &disk_uri] {
        let error = Outbox::open(alias).err().expect("non-file path admitted");
        assert_eq!(
            error.to_string(),
            "core store requires an ordinary file path"
        );
    }
    assert_eq!(peer.count("send"), 1);
    assert_eq!(peer.count("lookup"), 0);

    let mut reopened = Outbox::open(t.db()).unwrap();
    assert_eq!(reopened.snapshot(&r).unwrap().unwrap(), pending);
    assert_eq!(
        reopened
            .deliver(&r, &Grant(r.clone()), &mut peer)
            .unwrap()
            .state,
        DeliveryState::Delivered
    );
    assert_eq!(peer.count("send"), 1);
    assert_eq!(peer.count("lookup"), 1);
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
        let resolution: ConflictResolution =
            serde_json::from_slice(&std::fs::read(dir.join("resolution.json")).unwrap()).unwrap();
        p.receipt_override = Some(resolution.receipt.receipt_id.clone());
        let resolver = ResolveGrant {
            expected: resolution.clone(),
            calls: AtomicU64::new(0),
            crash_after_lock: mode == "resolve-crash",
        };
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
    ResolveGrant {
        expected: resolution.clone(),
        calls: AtomicU64::new(0),
        crash_after_lock: false,
    }
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
        receipt: DeliveryReceipt {
            request: r.clone(),
            receipt_id: "receipt-resolved".into(),
        },
        authorization_ref: "trusted-resolution-reference".into(),
    };
    assert!(resolution.prior.conflict);
    (r, resolution, peer)
}
struct FreshGrant<'a>(&'a AtomicBool);
impl DeliveryAuthority for FreshGrant<'_> {
    fn verify(&self, _: &DeliveryRequest) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
struct RevokingPeer<'a> {
    peer: Peer,
    authorized: &'a AtomicBool,
    after_send: bool,
}
impl Transport for RevokingPeer<'_> {
    fn send(
        &mut self,
        request: &DeliveryRequest,
        bytes: &[u8],
    ) -> anyhow::Result<Option<DeliveryReceipt>> {
        let reply = self.peer.send(request, bytes)?;
        if self.after_send {
            self.authorized.store(false, Ordering::SeqCst);
        }
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
        let mut peer = RevokingPeer {
            peer: Peer::open(&t.0.join("peer.sqlite")),
            authorized: &authorized,
            after_send,
        };
        let mut outbox = Outbox::open(t.db()).unwrap();
        assert!(outbox
            .deliver(&r, &FreshGrant(&authorized), &mut peer)
            .is_err());
        let pending = outbox.snapshot(&r).unwrap().unwrap();
        assert_eq!(pending.state, DeliveryState::Unknown);
        assert!(pending.receipt.is_none());
        assert_eq!(peer.peer.count("send"), 1);
        assert_eq!(peer.peer.count("lookup"), if after_send { 0 } else { 1 });
        authorized.store(true, Ordering::SeqCst);
        assert!(outbox
            .reconcile(&r, &FreshGrant(&authorized), &mut peer)
            .is_err());
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
    assert!(outbox
        .resolve(&resolution, &Grant(r.clone()), &Deny, &mut peer)
        .is_err());
    assert_eq!(peer.count("lookup"), before_lookup);
    let mut wrong = resolution.clone();
    wrong.receipt.request.destination_id = "wrong-thread".into();
    assert!(outbox
        .resolve(&wrong, &Grant(r.clone()), &resolver(&wrong), &mut peer)
        .is_err());
    wrong = resolution.clone();
    wrong.prior.generation += 1;
    assert!(outbox
        .resolve(&wrong, &Grant(r.clone()), &resolver(&wrong), &mut peer)
        .is_err());
    assert_eq!(peer.count("lookup"), before_lookup);
    peer.ambiguous = true;
    assert!(outbox
        .resolve(
            &resolution,
            &Grant(r.clone()),
            &resolver(&resolution),
            &mut peer
        )
        .is_err());
    peer.ambiguous = false;
    peer.wrong = true;
    assert!(outbox
        .resolve(
            &resolution,
            &Grant(r.clone()),
            &resolver(&resolution),
            &mut peer
        )
        .is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    assert_eq!(peer.count("send"), 1);
}
#[test]
fn resolution_retains_evidence_bytes_and_never_resends_after_restart() {
    let t = Temp::new();
    let (r, resolution, mut peer) = quarantine(&t);
    let mut outbox = Outbox::open(t.db()).unwrap();
    let done = outbox
        .resolve(
            &resolution,
            &Grant(r.clone()),
            &resolver(&resolution),
            &mut peer,
        )
        .unwrap();
    assert!(!done.conflict);
    assert_eq!(done.generation, resolution.prior.generation + 1);
    assert_eq!(done.receipt, Some(resolution.receipt.clone()));
    assert_eq!(done.acknowledgement, resolution.prior.acknowledgement);
    drop(outbox);
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert_eq!(
        outbox
            .resolve(
                &resolution,
                &Grant(r.clone()),
                &resolver(&resolution),
                &mut peer
            )
            .unwrap(),
        done
    );
    let mut wrong_decision = resolution.clone();
    wrong_decision.authorization_ref = "other-resolution-grant".into();
    assert!(outbox
        .resolve(
            &wrong_decision,
            &Grant(r.clone()),
            &resolver(&wrong_decision),
            &mut peer
        )
        .is_err());
    assert_eq!(
        outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap(),
        done
    );
    assert_eq!(peer.count("send"), 1);
    assert_eq!(
        Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(),
        BYTES
    );
    let db = Connection::open(t.db()).unwrap();
    let records: Vec<(String, String)> = db
        .prepare("SELECT kind,record FROM outbox_evidence ORDER BY generation")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, "conflict");
    assert!(records[0].1.contains("external-observed-receipt"));
    assert!(records[0].1.contains("receipt-resolved"));
    assert_eq!(
        serde_json::from_str::<ConflictResolution>(&records[1].1).unwrap(),
        resolution
    );
    assert!(db.execute("DELETE FROM outbox_evidence", []).is_err());
    assert!(db
        .execute("UPDATE outbox_evidence SET record='{}'", [])
        .is_err());
}
#[test]
fn resolution_revocation_during_readback_leaves_quarantine_unchanged() {
    let t = Temp::new();
    let (r, resolution, peer) = quarantine(&t);
    let authorized = AtomicBool::new(true);
    let mut peer = RevokingPeer {
        peer,
        authorized: &authorized,
        after_send: false,
    };
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert!(outbox
        .resolve(
            &resolution,
            &FreshGrant(&authorized),
            &resolver(&resolution),
            &mut peer
        )
        .is_err());
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
    assert!(outbox
        .resolve(
            &resolution,
            &Grant(r.clone()),
            &resolver(&resolution),
            &mut peer
        )
        .is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM outbox_evidence WHERE kind='resolution'",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(peer.count("send"), 1);
}
#[test]
fn fresh_process_resolution_crash_and_racing_resolvers_never_send() {
    let t = Temp::new();
    let (r, resolution, peer) = quarantine(&t);
    std::fs::write(t.0.join("request.json"), serde_json::to_vec(&r).unwrap()).unwrap();
    std::fs::write(
        t.0.join("resolution.json"),
        serde_json::to_vec(&resolution).unwrap(),
    )
    .unwrap();
    assert_eq!(
        child(&t.0, "resolve-crash").wait().unwrap().code(),
        Some(97)
    );
    assert_eq!(
        Outbox::open(t.db()).unwrap().snapshot(&r).unwrap().unwrap(),
        resolution.prior
    );
    let mut children = (0..4).map(|_| child(&t.0, "resolve")).collect::<Vec<_>>();
    for c in &mut children {
        assert!(c.wait().unwrap().success());
    }
    let done = Outbox::open(t.db()).unwrap().snapshot(&r).unwrap().unwrap();
    assert!(!done.conflict);
    assert_eq!(done.generation, resolution.prior.generation + 1);
    let db = Connection::open(t.db()).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM outbox_evidence WHERE kind='resolution'",
            [],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
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
    assert!(outbox
        .deliver(&r, &CountingGrant(AtomicU64::new(0)), &mut peer)
        .is_err());
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
    assert!(outbox
        .resolve(
            &resolution,
            &Grant(r.clone()),
            &UntilLock(AtomicU64::new(0)),
            &mut peer
        )
        .is_err());
    assert_eq!(outbox.snapshot(&r).unwrap().unwrap(), resolution.prior);
    let db = Connection::open(t.db()).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM outbox_evidence WHERE kind='resolution'",
            [],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(peer.count("send"), 1);
}

fn audit_rejection_retains_quarantine(full: bool) {
    struct ManyReceipts(Peer);
    impl Transport for ManyReceipts {
        fn send(
            &mut self,
            r: &DeliveryRequest,
            b: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
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
            db.execute(
                "INSERT INTO outbox_evidence VALUES('capacity-fixture',?1,'conflict','{}')",
                [generation],
            )
            .unwrap();
        }
    }
    peer.receipt_override = Some("contradictory-receipt".into());
    let error = if full {
        outbox
            .reconcile(&r, &Grant(r.clone()), &mut peer)
            .unwrap_err()
    } else {
        let mut many = ManyReceipts(peer);
        let error = outbox
            .reconcile(&r, &Grant(r.clone()), &mut many)
            .unwrap_err();
        peer = many.0;
        error
    };
    assert!(error
        .to_string()
        .contains("durably fenced; evidence capacity exhausted"));
    let fenced = outbox.snapshot(&r).unwrap().unwrap();
    assert!(fenced.conflict);
    assert_eq!(fenced.generation, delivered.generation + 1);
    assert_eq!(fenced.receipt, delivered.receipt);
    assert_eq!(fenced.acknowledgement, delivered.acknowledgement);
    assert_eq!(
        db.query_row("SELECT count(*) FROM outbox_evidence", [], |row| row
            .get::<_, u64>(0))
            .unwrap(),
        if full { CAPACITY } else { 0 }
    );
    drop(outbox);
    peer.receipt_override = None;
    let before_lookup = peer.count("lookup");
    let mut reopened = Outbox::open(t.db()).unwrap();
    assert!(reopened
        .reconcile(&r, &Grant(r.clone()), &mut peer)
        .is_err());
    assert!(reopened.deliver(&r, &Grant(r.clone()), &mut peer).is_err());
    assert_eq!(reopened.snapshot(&r).unwrap().unwrap(), fenced);
    assert_eq!(peer.count("lookup"), before_lookup);
    assert_eq!(peer.count("send"), 1);
    assert_eq!(
        Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(),
        BYTES
    );
}
#[test]
fn full_evidence_table_cannot_erase_observed_conflict() {
    audit_rejection_retains_quarantine(true);
}
#[test]
fn oversized_evidence_cannot_erase_observed_conflict() {
    audit_rejection_retains_quarantine(false);
}

// Independent legacy fixture: the schema emitted before the v2 audit marker.
// Do not build this fixture through Outbox::open, which is the migration subject.
fn legacy_outbox(t: &Temp, r: &DeliveryRequest) {
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("CREATE TABLE outbox_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL);
      INSERT INTO outbox_schema VALUES(1,1);
      CREATE TABLE outbox(operation TEXT PRIMARY KEY,request TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','unknown','delivered')),receipt TEXT,generation INTEGER NOT NULL,acknowledgement TEXT,conflict INTEGER NOT NULL CHECK(conflict IN (0,1)));
      CREATE TRIGGER outbox_binding_immutable BEFORE UPDATE OF operation,request ON outbox BEGIN SELECT RAISE(ABORT,'immutable delivery binding');END;
      CREATE TRIGGER outbox_retained BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT,'retained delivery');END;").unwrap();
    db.execute(
        "INSERT INTO outbox VALUES(?1,?2,'unknown',NULL,4,NULL,0)",
        params![r.delivery_operation_id, serde_json::to_string(r).unwrap()],
    )
    .unwrap();
}
fn legacy_audit(db: &Connection) {
    db.execute_batch("CREATE TABLE outbox_evidence(operation TEXT NOT NULL,generation INTEGER NOT NULL,kind TEXT NOT NULL,record TEXT NOT NULL,PRIMARY KEY(operation,generation,kind));
      CREATE TRIGGER outbox_evidence_immutable BEFORE UPDATE ON outbox_evidence BEGIN SELECT RAISE(ABORT,'immutable delivery evidence');END;
      CREATE TRIGGER outbox_evidence_retained BEFORE DELETE ON outbox_evidence BEGIN SELECT RAISE(ABORT,'retained delivery evidence');END;").unwrap();
}
fn schema_version(db: &Connection) -> u64 {
    db.query_row("SELECT version FROM outbox_schema WHERE id=1", [], |r| {
        r.get(0)
    })
    .unwrap()
}
fn schema_count(db: &Connection, name: &str) -> u64 {
    db.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name=?1",
        [name],
        |r| r.get(0),
    )
    .unwrap()
}
fn retained_row(db: &Connection) -> String {
    db.query_row(
        "SELECT request || '|' || state || '|' || generation || '|' || conflict FROM outbox",
        [],
        |r| r.get(0),
    )
    .unwrap()
}
struct SchemaChild(std::process::Child);
impl SchemaChild {
    fn spawn(t: &Temp, mode: &str, result: &str) -> Self {
        if !t.0.join("request.json").exists() {
            std::fs::write(
                t.0.join("request.json"),
                serde_json::to_vec(&request(t)).unwrap(),
            )
            .unwrap();
        }
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "schema_reopen_child", "--nocapture"])
                .env("CORE_SCHEMA_DIR", &t.0)
                .env("CORE_SCHEMA_MODE", mode)
                .env("CORE_SCHEMA_RESULT", result)
                .spawn()
                .unwrap(),
        )
    }
    fn wait(&mut self) {
        let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(std::time::Instant::now() < end, "schema child timed out");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
impl Drop for SchemaChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn schema_reopen_child() {
    let Ok(directory) = std::env::var("CORE_SCHEMA_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let r: DeliveryRequest =
        serde_json::from_slice(&std::fs::read(directory.join("request.json")).unwrap()).unwrap();
    let outcome = match Outbox::open(directory.join("core.sqlite")) {
        Ok(mut outbox) => {
            if std::env::var("CORE_SCHEMA_MODE").unwrap() == "deliver" {
                let mut peer = Peer::open(&directory.join("peer.sqlite"));
                // A reopened journal may try to send even when the provider's
                // unique receipt rejects it. Count the attempted call separately.
                let _ = outbox.deliver(&r, &Grant(r.clone()), &mut peer);
            } else {
                assert!(outbox.snapshot(&r).unwrap().is_some());
            }
            "opened"
        }
        Err(_) => "blocked",
    };
    std::fs::write(
        directory.join(std::env::var("CORE_SCHEMA_RESULT").unwrap()),
        outcome,
    )
    .unwrap();
}
#[test]
fn missing_journal_blocks_fresh_process_without_resend() {
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    legacy_outbox(&t, &r);
    peer.send(&r, BYTES).unwrap();
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("DROP TABLE outbox;").unwrap();
    // The authoritative marker and accepted BLOB survive the missing journal.
    assert_eq!(schema_version(&db), 1);
    SchemaChild::spawn(&t, "deliver", "reopen.txt").wait();
    assert_eq!(
        peer.count("send"),
        1,
        "reopen must not attempt a second send"
    );
    assert_eq!(peer.count("lookup"), 0);
    assert_eq!(
        std::fs::read_to_string(t.0.join("reopen.txt")).unwrap(),
        "blocked"
    );
    assert_eq!(
        schema_count(&db, "outbox"),
        0,
        "missing journal must not be recreated"
    );
    assert_eq!(
        Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(),
        BYTES
    );
}
#[test]
fn lost_reservation_row_blocks_fresh_process_without_resend() {
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    peer.lost = true;
    let mut outbox = Outbox::open(t.db()).unwrap();
    outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap();
    drop(outbox);
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("DROP TRIGGER outbox_retained;DELETE FROM outbox;
        CREATE TRIGGER outbox_retained BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT,'retained delivery');END;").unwrap();
    assert_eq!(schema_version(&db), 3);
    assert_eq!(schema_count(&db, "outbox_retained"), 1);
    let retained: String = db
        .query_row("SELECT request FROM outbox_reservations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        serde_json::from_str::<DeliveryRequest>(&retained).unwrap(),
        r
    );
    SchemaChild::spawn(&t, "deliver", "missing-row.txt").wait();
    assert_eq!(
        peer.count("send"),
        1,
        "lost row must not trigger another send"
    );
    assert_eq!(peer.count("lookup"), 0);
    assert_eq!(
        std::fs::read_to_string(t.0.join("missing-row.txt")).unwrap(),
        "blocked"
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM outbox", [], |row| row
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(),
        BYTES
    );
}
#[test]
fn failed_reservation_record_write_rolls_back_before_transport() {
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    drop(Outbox::open(t.db()).unwrap());
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("CREATE TRIGGER fail_reservation BEFORE INSERT ON outbox_reservations BEGIN SELECT RAISE(ABORT,'synthetic reservation write fault');END;").unwrap();
    SchemaChild::spawn(&t, "deliver", "write-failed.txt").wait();
    assert_eq!(peer.count("send"), 0);
    assert_eq!(peer.count("lookup"), 0);
    assert!(Outbox::open(t.db())
        .unwrap()
        .snapshot(&r)
        .unwrap()
        .is_none());
    db.execute_batch("DROP TRIGGER fail_reservation;").unwrap();
    assert_eq!(
        Outbox::open(t.db())
            .unwrap()
            .deliver(&r, &Grant(r.clone()), &mut peer)
            .unwrap()
            .state,
        DeliveryState::Delivered
    );
    assert_eq!(peer.count("send"), 1);
    assert_eq!(peer.count("lookup"), 1);
}
#[test]
fn opened_outbox_checks_reservation_record_before_lookup() {
    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    peer.lost = true;
    let mut outbox = Outbox::open(t.db()).unwrap();
    outbox.deliver(&r, &Grant(r.clone()), &mut peer).unwrap();
    let db = Connection::open(t.db()).unwrap();
    db.execute_batch("DROP TRIGGER outbox_reservations_immutable;
        UPDATE outbox_reservations SET request='changed';
        CREATE TRIGGER outbox_reservations_immutable BEFORE UPDATE ON outbox_reservations BEGIN SELECT RAISE(ABORT,'immutable reservation');END;").unwrap();
    let outcome = outbox.reconcile(&r, &Grant(r.clone()), &mut peer);
    assert_eq!(peer.count("send"), 1);
    assert_eq!(peer.count("lookup"), 0);
    assert!(outcome.is_err());
    assert!(outbox.snapshot(&r).is_err());
    assert!(Outbox::open(t.db()).is_err());
}
#[test]
fn fresh_process_reconciliation_supersedes_pending_send() {
    struct ReconcileBeforeSend<'a>(&'a Temp, AtomicBool);
    impl DeliveryAuthority for ReconcileBeforeSend<'_> {
        fn verify(&self, request: &DeliveryRequest) -> bool {
            let db = Connection::open(self.0.db()).unwrap();
            let reserved: bool = db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM outbox WHERE operation=?1)",
                    [&request.delivery_operation_id],
                    |row| row.get(0),
                )
                .unwrap();
            if reserved && !self.1.swap(true, Ordering::SeqCst) {
                SchemaChild::spawn(self.0, "deliver", "reconciled.txt").wait();
            }
            true
        }
    }

    let t = Temp::new();
    let r = request(&t);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    let mut outbox = Outbox::open(t.db()).unwrap();
    let authority = ReconcileBeforeSend(&t, AtomicBool::new(false));
    let result = outbox.deliver(&r, &authority, &mut peer);

    assert!(authority.1.load(Ordering::SeqCst));
    assert_eq!(
        std::fs::read_to_string(t.0.join("reconciled.txt")).unwrap(),
        "opened"
    );
    assert_eq!(peer.count("send"), 0);
    assert_eq!(peer.count("lookup"), 1);
    assert_eq!(
        result.unwrap_err().to_string(),
        "dispatch reservation changed before send"
    );
    let retained = Outbox::open(t.db()).unwrap().snapshot(&r).unwrap().unwrap();
    assert_eq!(retained.state, DeliveryState::Unknown);
    assert_eq!(retained.generation, 1);
    assert!(retained.receipt.is_none());
}
#[test]
fn legacy_migration_preserves_reservation_across_concurrent_processes() {
    let t = Temp::new();
    let r = request(&t);
    legacy_outbox(&t, &r);
    let db = Connection::open(t.db()).unwrap();
    let before = retained_row(&db);
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    peer.send(&r, BYTES).unwrap(); // Existing remote effect, missing acknowledgement.
    let mut a = SchemaChild::spawn(&t, "snapshot", "a.txt");
    let mut b = SchemaChild::spawn(&t, "snapshot", "b.txt");
    a.wait();
    b.wait();
    for result in ["a.txt", "b.txt"] {
        assert_eq!(std::fs::read_to_string(t.0.join(result)).unwrap(), "opened");
    }
    assert_eq!(schema_version(&db), 3);
    assert_eq!(retained_row(&db), before);
    assert_eq!(schema_count(&db, "outbox_evidence"), 1);
    assert_eq!(schema_count(&db, "outbox_reservations"), 1);
    let mut outbox = Outbox::open(t.db()).unwrap();
    assert_eq!(
        outbox
            .deliver(&r, &Grant(r.clone()), &mut peer)
            .unwrap()
            .state,
        DeliveryState::Delivered
    );
    assert_eq!(peer.count("send"), 1);
    assert_eq!(peer.count("lookup"), 1);
    assert_eq!(
        Store::open(t.db()).unwrap().accepted(&r.accepted).unwrap(),
        BYTES
    );
}
#[test]
fn failed_schema_upgrade_rolls_back_before_fresh_process_retry() {
    let t = Temp::new();
    let r = request(&t);
    legacy_outbox(&t, &r);
    let db = Connection::open(t.db()).unwrap();
    let before = retained_row(&db);
    // Inject after audit DDL and before version advancement. All new schema
    // objects must disappear on failure while the old journal remains intact.
    db.execute_batch("CREATE TRIGGER fail_upgrade BEFORE UPDATE ON outbox_schema BEGIN SELECT RAISE(ABORT,'synthetic upgrade interruption');END;").unwrap();
    SchemaChild::spawn(&t, "deliver", "failed.txt").wait();
    assert_eq!(
        std::fs::read_to_string(t.0.join("failed.txt")).unwrap(),
        "blocked"
    );
    assert_eq!(schema_version(&db), 1);
    assert_eq!(retained_row(&db), before);
    for name in [
        "outbox_evidence",
        "outbox_evidence_immutable",
        "outbox_evidence_retained",
        "outbox_reservations",
        "outbox_reservations_immutable",
        "outbox_reservations_retained",
    ] {
        assert_eq!(schema_count(&db, name), 0);
    }
    assert!(
        !t.0.join("peer.sqlite").exists(),
        "blocked reopen reached transport"
    );
    db.execute_batch("DROP TRIGGER fail_upgrade;").unwrap();
    SchemaChild::spawn(&t, "snapshot", "retry.txt").wait();
    assert_eq!(
        std::fs::read_to_string(t.0.join("retry.txt")).unwrap(),
        "opened"
    );
    assert_eq!(schema_version(&db), 3);
    assert_eq!(retained_row(&db), before);
}
#[test]
fn incomplete_or_incompatible_outbox_schemas_are_never_repaired() {
    for corruption in [
        "DROP TABLE outbox_schema;",
        "DELETE FROM outbox_schema;",
        "UPDATE outbox_schema SET version=99;",
        "DROP TRIGGER outbox_binding_immutable;",
        "DROP TRIGGER outbox_retained;CREATE TRIGGER outbox_retained BEFORE DELETE ON outbox BEGIN SELECT 1;END;",
        "DROP TABLE outbox_evidence;",
        "DROP TRIGGER outbox_evidence_immutable;",
        "ALTER TABLE outbox_evidence ADD COLUMN unknown TEXT;",
        "DROP TABLE outbox_reservations;",
        "DROP TRIGGER outbox_reservations_immutable;",
    ] {
        let t = Temp::new();
        let r = request(&t);
        let mut peer = Peer::open(&t.0.join("peer.sqlite"));
        peer.lost = true;
        Outbox::open(t.db()).unwrap().deliver(&r, &Grant(r.clone()), &mut peer).unwrap();
        let db = Connection::open(t.db()).unwrap();
        db.execute_batch(corruption).unwrap();
        let before = retained_row(&db);
        assert!(Outbox::open(t.db()).is_err(), "accepted corruption: {corruption}");
        assert_eq!(retained_row(&db), before);
        assert_eq!(peer.count("send"), 1);
        assert_eq!(peer.count("lookup"), 0);
    }
}
#[test]
fn legacy_audit_survives_upgrade_and_partial_or_orphan_audit_blocks() {
    for version in [1, 2] {
        for damage in ["", "DROP TRIGGER outbox_evidence_retained;", "DROP TRIGGER outbox_retained;DELETE FROM outbox;CREATE TRIGGER outbox_retained BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT,'retained delivery');END;"] {
            check_legacy_audit_upgrade(version, damage);
        }
    }
}
fn check_legacy_audit_upgrade(version: u64, damage: &str) {
    let t = Temp::new();
    let r = request(&t);
    legacy_outbox(&t, &r);
    let db = Connection::open(t.db()).unwrap();
    legacy_audit(&db);
    db.execute("UPDATE outbox_schema SET version=?1", [version])
        .unwrap();
    db.execute(
        "INSERT INTO outbox_evidence VALUES(?1,0,'conflict','synthetic preserved audit')",
        [&r.delivery_operation_id],
    )
    .unwrap();
    db.execute_batch(damage).unwrap();
    let reopened = Outbox::open(t.db());
    assert_eq!(reopened.is_ok(), damage.is_empty());
    assert_eq!(
        schema_version(&db),
        if damage.is_empty() { 3 } else { version }
    );
    assert_eq!(
        schema_count(&db, "outbox_reservations"),
        u64::from(damage.is_empty())
    );
    let record: String = db
        .query_row("SELECT record FROM outbox_evidence", [], |r| r.get(0))
        .unwrap();
    assert_eq!(record, "synthetic preserved audit");
}

#[test]
fn missing_primary_key_token_boundary_blocks_outbox_reopen() {
    for declared_type in ["TEXT PRIMARYKEY", "TEXTPRIMARYKEY"] {
        let t = Temp::new();
        let r = request(&t);
        Outbox::open(t.db()).unwrap();
        let db = Connection::open(t.db()).unwrap();
        db.execute_batch(&format!("DROP TABLE outbox;
          CREATE TABLE outbox(operation {declared_type},request TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','unknown','delivered')),receipt TEXT,generation INTEGER NOT NULL,acknowledgement TEXT,conflict INTEGER NOT NULL CHECK(conflict IN (0,1)));
          CREATE TRIGGER outbox_binding_immutable BEFORE UPDATE OF operation,request ON outbox BEGIN SELECT RAISE(ABORT,'immutable delivery binding');END;
          CREATE TRIGGER outbox_retained BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT,'retained delivery');END;")).unwrap();
        let pk: u64 = db
            .query_row(
                "SELECT pk FROM pragma_table_info('outbox') WHERE name='operation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pk, 0, "fixture must actually lack a primary key");
        for _ in 0..2 {
            db.execute(
                "INSERT INTO outbox VALUES(?1,?2,'unknown',NULL,0,NULL,0)",
                params![r.delivery_operation_id, serde_json::to_string(&r).unwrap()],
            )
            .unwrap();
        }
        assert_eq!(
            db.query_row("SELECT count(*) FROM outbox", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        let peer = Peer::open(&t.0.join("peer.sqlite"));
        assert!(
            Outbox::open(t.db()).is_err(),
            "accepted missing operation constraint: {declared_type}"
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM outbox", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        assert_eq!(peer.count("send"), 0);
        assert_eq!(peer.count("lookup"), 0);
    }
}

struct WirePeer {
    mode: &'static str,
    calls: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
}
impl runspindle_core::tether_wire::Peer for WirePeer {
    fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
        let request: serde_json::Value = serde_json::from_slice(raw)?;
        self.calls
            .borrow_mut()
            .push(request["action"].as_str().unwrap().into());
        assert_eq!(
            request["payload"]["attachment"]["content_hex"],
            BYTES.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
        assert_eq!(
            request["envelope"]["binding"]["operation_id"],
            "native-operation"
        );
        let conflict = self.mode == "conflict";
        let unknown = conflict || self.mode == "unknown";
        Ok(serde_json::to_vec(&serde_json::json!({
            "version": runspindle_core::tether_wire::VERSION,
            "request_sha256": if self.mode == "wrong-binding" { "0".repeat(64) } else { digest(raw) },
            "evidence_class": if self.mode == "synthetic" { "synthetic" } else { "slack_wire_content_observation" },
            "state": if unknown { "unknown" } else { "content_confirmed" },
            "file_id": "F100", "message_id": if unknown { None } else { Some("1700000000.000002") },
            "conflict": conflict
        }))?)
    }
}
#[test]
fn tether_wire_exact_readback_and_reconstruction() {
    use runspindle_core::tether_wire::WireTransport;
    let temp = Temp::new();
    let request = request(&temp);
    let calls = std::rc::Rc::new(std::cell::RefCell::new(vec![]));
    let store = Store::open(temp.db()).unwrap();
    let make = || {
        WireTransport::new(
            &store,
            request.clone(),
            "result.bin",
            "accepted",
            WirePeer {
                mode: "confirmed",
                calls: calls.clone(),
            },
        )
        .unwrap()
    };
    let mut outbox = Outbox::open(temp.db()).unwrap();
    let first = outbox
        .deliver(&request, &Grant(request.clone()), &mut make())
        .unwrap();
    assert_eq!(first.state, DeliveryState::Delivered);
    drop(outbox);
    let mut reopened = Outbox::open(temp.db()).unwrap();
    let next = reopened
        .deliver(&request, &Grant(request.clone()), &mut make())
        .unwrap();
    assert_eq!(first.receipt, next.receipt);
    assert_eq!(*calls.borrow(), vec!["send", "lookup", "lookup"]);
}
#[test]
fn tether_wire_unbound_unknown_and_synthetic_never_deliver() {
    use runspindle_core::tether_wire::WireTransport;
    for mode in ["wrong-binding", "synthetic", "unknown", "conflict"] {
        let temp = Temp::new();
        let request = request(&temp);
        let calls = std::rc::Rc::new(std::cell::RefCell::new(vec![]));
        let mut adapter = WireTransport::new(
            &Store::open(temp.db()).unwrap(),
            request.clone(),
            "result.bin",
            "accepted",
            WirePeer {
                mode,
                calls: calls.clone(),
            },
        )
        .unwrap();
        let mut outbox = Outbox::open(temp.db()).unwrap();
        let _ = outbox.deliver(&request, &Grant(request.clone()), &mut adapter);
        let state = outbox.snapshot(&request).unwrap().unwrap();
        assert_ne!(state.state, DeliveryState::Delivered);
        assert!(state.receipt.is_none());
        assert_eq!(state.conflict, mode == "conflict");
        let _ = outbox.deliver(&request, &Grant(request.clone()), &mut adapter);
        assert_eq!(calls.borrow().iter().filter(|c| *c == "send").count(), 1);
    }
}

struct ExplicitConflict;
impl Transport for ExplicitConflict {
    fn send(&mut self, _: &DeliveryRequest, _: &[u8]) -> anyhow::Result<Option<DeliveryReceipt>> {
        Ok(None)
    }
    fn lookup(&mut self, _: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        Ok(vec![])
    }
    fn lookup_observation(&mut self, _: &DeliveryRequest) -> anyhow::Result<ReadbackObservation> {
        Ok(ReadbackObservation {
            receipts: vec![],
            conflict: true,
        })
    }
}
#[test]
fn explicit_readback_conflict_is_durable_without_fake_receipts() {
    let temp = Temp::new();
    let request = request(&temp);
    let mut outbox = Outbox::open(temp.db()).unwrap();
    let grant = Grant(request.clone());
    let first = outbox
        .deliver(&request, &grant, &mut ExplicitConflict)
        .unwrap();
    assert_eq!(first.state, DeliveryState::Unknown);
    assert!(outbox
        .reconcile(&request, &grant, &mut ExplicitConflict)
        .is_err());
    drop(outbox);
    let state = Outbox::open(temp.db())
        .unwrap()
        .snapshot(&request)
        .unwrap()
        .unwrap();
    assert!(state.conflict);
    assert!(state.receipt.is_none());
    assert!(state.acknowledgement.is_none());
}

/// This helper exists only in the integration test binary. Production transport
/// has no process launcher, no executable selection, and no ambient authority.
struct PythonWirePeer {
    source: PathBuf,
    root: PathBuf,
    expected: PathBuf,
}
impl runspindle_core::tether_wire::Peer for PythonWirePeer {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        use std::io::{Read, Write};
        use std::process::Stdio;
        let mut child = Command::new("python3")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tether_wire_peer.py"))
            .args([
                "--root",
                self.root.to_str().unwrap(),
                "--expected",
                self.expected.to_str().unwrap(),
            ])
            .current_dir(&self.source)
            .env("PYTHONPATH", &self.source)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        // Requests are bounded below OS pipe capacity in this small fixture.
        child.stdin.take().unwrap().write_all(request)?;
        let mut stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout
                .by_ref()
                .take(4097)
                .read_to_end(&mut bytes)
                .map(|_| bytes)
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill()?;
                child.wait()?;
                let _ = reader.join();
                anyhow::bail!("isolated wire peer deadline");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let bytes = reader
            .join()
            .map_err(|_| anyhow::anyhow!("peer reader failed"))??;
        anyhow::ensure!(
            status.success() && bytes.len() <= 4096,
            "isolated wire peer failed"
        );
        Ok(bytes)
    }
}
fn tether_request(temp: &Temp) -> DeliveryRequest {
    let mut store = Store::open(temp.db()).unwrap();
    let artifact = store
        .put(
            VERSION,
            Binding {
                task_id: "task-1".into(),
                revision: 1,
                operation_id: "operation-1".into(),
                policy_revision: "policy".into(),
                host_id: "host-1".into(),
                workspace_id: "T100".into(),
                lease_epoch: 1,
            },
            "artifact-1".into(),
            1,
            BYTES,
        )
        .unwrap();
    let validation = store
        .validate(VERSION, &artifact, "policy", &Check)
        .unwrap();
    DeliveryRequest {
        version: VERSION.into(),
        accepted: store.accept(VERSION, &validation, &Accept).unwrap(),
        account_id: "account-1".into(),
        destination_id: "destination-1".into(),
        delivery_operation_id: "delivery-1".into(),
    }
}
fn expected_wire(request: &DeliveryRequest) -> serde_json::Value {
    let a = request.accepted.artifact();
    // Independent fixture-side encoding, not obtained from the adapter/peer.
    serde_json::json!({"version":runspindle_core::tether_wire::VERSION,"action":"send",
        "envelope":{"version":"runspindle.envelope.v1","message_type":"delivery_request","message_id":request.delivery_operation_id,
        "binding":a.binding,"payload":{"artifact_id":a.artifact_id,"artifact_revision":a.artifact_revision,"sha256":a.sha256,"validator_revision":request.accepted.validation.validator_revision,
        "account_id":request.account_id,"destination_id":request.destination_id,"delivery_operation_id":request.delivery_operation_id}},
        "payload":{"text":"Synthetic accepted artifact","attachment":{"attachment_id":a.artifact_id,"filename":"result.bin","content_hex":BYTES.iter().map(|b| format!("{b:02x}")).collect::<String>()}}})
}
#[test]
fn wire_integration_child() {
    use runspindle_core::tether_wire::WireTransport;
    let Ok(root) = std::env::var("CORE_WIRE_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let source = PathBuf::from(std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").unwrap());
    let request: DeliveryRequest =
        serde_json::from_slice(&std::fs::read(root.join("request.json")).unwrap()).unwrap();
    let store = Store::open(root.join("core.sqlite")).unwrap();
    let mut transport = WireTransport::new(
        &store,
        request.clone(),
        "result.bin",
        "Synthetic accepted artifact",
        PythonWirePeer {
            source,
            root: root.clone(),
            expected: root.join("expected.json"),
        },
    )
    .unwrap();
    let mut outbox = Outbox::open(root.join("core.sqlite")).unwrap();
    let result = outbox.deliver(&request, &Grant(request.clone()), &mut transport);
    let snapshot = outbox.snapshot(&request).unwrap().unwrap();
    std::fs::write(
        root.join("result.json"),
        serde_json::to_vec(&serde_json::json!({"ok":result.is_ok(),"snapshot":snapshot})).unwrap(),
    )
    .unwrap();
}
fn wire_step(temp: &Temp, source: &Path) -> (bool, DeliverySnapshot) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "wire_integration_child", "--nocapture"])
        .env("CORE_WIRE_CHILD_ROOT", &temp.0)
        .env("RUNSPINDLE_TETHER_WIRE_SOURCE", source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.0.join("result.json")).unwrap()).unwrap();
    (
        result["ok"].as_bool().unwrap(),
        serde_json::from_value(result["snapshot"].clone()).unwrap(),
    )
}
fn wire_seed(temp: &Temp, request: &DeliveryRequest, fault: &str) {
    std::fs::write(
        temp.0.join("expected.json"),
        serde_json::to_vec(&expected_wire(request)).unwrap(),
    )
    .unwrap();
    std::fs::write(
        temp.0.join("request.json"),
        serde_json::to_vec(request).unwrap(),
    )
    .unwrap();
    std::fs::write(temp.0.join("fault.txt"), fault).unwrap();
}
fn wire_effect_counts(temp: &Temp) {
    let peer = Connection::open(temp.0.join("service.db")).unwrap();
    for action in ["allocate", "upload", "complete"] {
        let calls: u64 = peer
            .query_row(
                "SELECT count(*) FROM calls WHERE action=?1",
                [action],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(calls, 1, "{action} must never repeat");
    }
    let bytes: Vec<u8> = peer
        .query_row("SELECT content FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(bytes, BYTES);
}
#[test]
#[ignore = "requires explicitly pinned local Tether source, actual journals and isolated SQL HTTP fixture"]
fn actual_tether_journals_wire_readback_and_lost_ack() {
    let source = PathBuf::from(
        std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").expect("explicit Tether source required"),
    );
    for fault in ["", "lost:complete", "lost:bridge-response"] {
        let temp = Temp::new();
        let request = tether_request(&temp);
        wire_seed(&temp, &request, fault);
        let (ok, first) = wire_step(&temp, &source);
        assert!(ok);
        if fault != "lost:bridge-response" {
            assert_eq!(first.state, DeliveryState::Delivered);
        } else {
            assert_eq!(first.state, DeliveryState::Unknown);
        }
        std::fs::write(temp.0.join("fault.txt"), "").unwrap();
        let (ok, second) = wire_step(&temp, &source);
        assert!(ok);
        assert_eq!(second.state, DeliveryState::Delivered);
        wire_effect_counts(&temp);
    }
}
#[test]
#[ignore = "requires explicitly pinned local Tether source, actual journals and isolated SQL HTTP fixture"]
fn actual_tether_wrong_thread_and_conflict_never_retarget() {
    let source = PathBuf::from(
        std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").expect("explicit Tether source required"),
    );
    for field in ["thread", "message"] {
        let temp = Temp::new();
        let request = tether_request(&temp);
        wire_seed(&temp, &request, "lost:bridge-response");
        assert_eq!(wire_step(&temp, &source).1.state, DeliveryState::Unknown);
        std::fs::write(temp.0.join("fault.txt"), "").unwrap();
        let peer = Connection::open(temp.0.join("service.db")).unwrap();
        peer.execute(&format!("UPDATE files SET {field}='1700000999.000001'"), [])
            .unwrap();
        drop(peer);
        let (_, state) = wire_step(&temp, &source);
        assert_eq!(state.state, DeliveryState::Unknown);
        assert!(state.receipt.is_none());
        if field == "message" {
            assert!(state.conflict);
        }
        let (_, repeated) = wire_step(&temp, &source);
        assert!(repeated.receipt.is_none());
        wire_effect_counts(&temp);
    }
}

#[test]
#[ignore = "requires explicitly pinned local Tether source and isolated synthetic authority"]
fn actual_tether_revocation_after_lost_ack_cannot_deliver_or_retry() {
    let source = PathBuf::from(
        std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").expect("explicit Tether source required"),
    );
    let temp = Temp::new();
    let request = tether_request(&temp);
    wire_seed(&temp, &request, "lost:bridge-response");
    assert_eq!(wire_step(&temp, &source).1.state, DeliveryState::Unknown);
    std::fs::write(temp.0.join("fault.txt"), "").unwrap();
    std::fs::write(temp.0.join("revoked"), "synthetic authority revoked").unwrap();
    let (ok, state) = wire_step(&temp, &source);
    assert!(!ok);
    assert_eq!(state.state, DeliveryState::Unknown);
    assert!(state.receipt.is_none());
    wire_effect_counts(&temp);
}

struct ResolutionWirePeer {
    mode: &'static str,
    calls: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
}
impl runspindle_core::tether_wire::Peer for ResolutionWirePeer {
    fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
        let request: serde_json::Value = serde_json::from_slice(raw)?;
        let action = request["action"].as_str().unwrap();
        assert!(matches!(action, "resolve" | "lookup_resolution"));
        self.calls.borrow_mut().push(action.into());
        let unknown = self.mode == "unknown";
        let mut reply = serde_json::json!({"version":runspindle_core::tether_wire::RESOLUTION_VERSION,
            "request_sha256":digest(raw),"decision_sha256": if unknown { None } else { Some(digest(&serde_json::to_vec(&request["decision"])?)) },
            "evidence_class":"slack_wire_resolution_observation","state": if unknown { "unknown" } else { "independently_reconciled_under_decision" },
            "file_id":if unknown { serde_json::Value::Null } else { request["decision"]["file_id"].clone() },
            "message_id":if unknown { serde_json::Value::Null } else { request["decision"]["message_id"].clone() }});
        if self.mode == "wrong-decision" {
            reply["decision_sha256"] = serde_json::json!("0".repeat(64));
        }
        Ok(serde_json::to_vec(&reply)?)
    }
}
fn wire_decision(
    resolution: &mut ConflictResolution,
) -> runspindle_core::tether_wire::ResolutionDecision {
    let file = "F100";
    let message = "1700000002.000003";
    resolution.receipt.receipt_id = format!(
        "tether:{}",
        digest(&serde_json::to_vec(&(file, message)).unwrap())
    );
    runspindle_core::tether_wire::ResolutionDecision {
        version: "runspindle.wire-resolution.v1".into(),
        decision_id: "decision-1".into(),
        policy_ref: "policy-1".into(),
        authorization_ref: resolution.authorization_ref.clone(),
        conflict_sha256: "a".repeat(64),
        core_resolution_sha256: digest(
            &serde_json::to_vec(&serde_json::to_value(&*resolution).unwrap()).unwrap(),
        ),
        file_id: file.into(),
        message_id: message.into(),
    }
}
#[test]
fn resolution_bridge_requires_separate_authority_and_reobserves_exact_decision() {
    use runspindle_core::tether_wire::ResolutionBridge;
    let temp = Temp::new();
    let (request, mut resolution, _) = quarantine(&temp);
    let decision = wire_decision(&mut resolution);
    let store = Store::open(temp.db()).unwrap();
    let calls = std::rc::Rc::new(std::cell::RefCell::new(vec![]));
    let mut bridge = ResolutionBridge::new(
        &store,
        resolution.clone(),
        decision,
        "result.bin",
        "accepted",
        ResolutionWirePeer {
            mode: "confirmed",
            calls: calls.clone(),
        },
    )
    .unwrap();
    let mut outbox = Outbox::open(temp.db()).unwrap();
    assert!(bridge
        .resolve(&mut outbox, &Grant(request.clone()), &Deny)
        .is_err());
    assert!(calls.borrow().is_empty());
    let first = bridge
        .resolve(&mut outbox, &Grant(request.clone()), &resolver(&resolution))
        .unwrap();
    assert!(!first.conflict);
    assert_eq!(first.state, DeliveryState::Delivered);
    assert_eq!(*calls.borrow(), vec!["resolve", "lookup_resolution"]);
    drop(outbox);
    let mut outbox = Outbox::open(temp.db()).unwrap();
    assert!(bridge
        .resolve(&mut outbox, &Grant(request.clone()), &Deny)
        .is_err());
    assert_eq!(calls.borrow().len(), 2);
    let second = bridge
        .resolve(&mut outbox, &Grant(request), &resolver(&resolution))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        *calls.borrow(),
        vec![
            "resolve",
            "lookup_resolution",
            "resolve",
            "lookup_resolution"
        ]
    );
}
#[test]
fn resolution_bridge_rejects_unbound_or_unknown_evidence_without_clearing() {
    use runspindle_core::tether_wire::ResolutionBridge;
    for mode in ["unknown", "wrong-decision"] {
        let temp = Temp::new();
        let (request, mut resolution, _) = quarantine(&temp);
        let decision = wire_decision(&mut resolution);
        let mut bridge = ResolutionBridge::new(
            &Store::open(temp.db()).unwrap(),
            resolution.clone(),
            decision,
            "result.bin",
            "accepted",
            ResolutionWirePeer {
                mode,
                calls: Default::default(),
            },
        )
        .unwrap();
        let mut outbox = Outbox::open(temp.db()).unwrap();
        assert!(bridge
            .resolve(&mut outbox, &Grant(request.clone()), &resolver(&resolution))
            .is_err());
        assert_eq!(
            outbox.snapshot(&request).unwrap().unwrap(),
            resolution.prior
        );
    }
}

#[test]
fn wire_resolution_child() {
    use runspindle_core::tether_wire::{ResolutionBridge, ResolutionDecision};
    let Ok(root) = std::env::var("CORE_WIRE_RESOLUTION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let source = PathBuf::from(std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").unwrap());
    let resolution: ConflictResolution =
        serde_json::from_slice(&std::fs::read(root.join("core-resolution.json")).unwrap()).unwrap();
    let decision: ResolutionDecision =
        serde_json::from_slice(&std::fs::read(root.join("resolution-decision.json")).unwrap())
            .unwrap();
    let store = Store::open(root.join("core.sqlite")).unwrap();
    let mut bridge = ResolutionBridge::new(
        &store,
        resolution.clone(),
        decision,
        "result.bin",
        "Synthetic accepted artifact",
        PythonWirePeer {
            source,
            root: root.clone(),
            expected: root.join("expected.json"),
        },
    )
    .unwrap();
    let mut outbox = Outbox::open(root.join("core.sqlite")).unwrap();
    let result = if root.join("core-resolver-revoked").exists() {
        bridge.resolve(&mut outbox, &Grant(resolution.prior.request.clone()), &Deny)
    } else {
        bridge.resolve(
            &mut outbox,
            &Grant(resolution.prior.request.clone()),
            &resolver(&resolution),
        )
    };
    if result.is_ok() && root.join("core-resolution-lost-response").exists() {
        std::process::exit(32);
    }
    let snapshot = outbox.snapshot(&resolution.prior.request).unwrap().unwrap();
    std::fs::write(
        root.join("resolution-result.json"),
        serde_json::to_vec(&serde_json::json!({"ok":result.is_ok(),"snapshot":snapshot})).unwrap(),
    )
    .unwrap();
}
fn resolution_step(temp: &Temp, source: &Path) -> (bool, DeliverySnapshot) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "wire_resolution_child", "--nocapture"])
        .env("CORE_WIRE_RESOLUTION_ROOT", &temp.0)
        .env("RUNSPINDLE_TETHER_WIRE_SOURCE", source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.0.join("resolution-result.json")).unwrap())
            .unwrap();
    (
        result["ok"].as_bool().unwrap(),
        serde_json::from_value(result["snapshot"].clone()).unwrap(),
    )
}
fn resolution_seed(temp: &Temp, source: &Path) -> ConflictResolution {
    let request = tether_request(temp);
    wire_seed(temp, &request, "");
    assert_eq!(wire_step(temp, source).1.state, DeliveryState::Delivered);
    let peer = Connection::open(temp.0.join("service.db")).unwrap();
    peer.execute("UPDATE files SET message='1700000002.000003'", [])
        .unwrap();
    drop(peer);
    let (_, prior) = wire_step(temp, source);
    assert!(prior.conflict);
    let mut resolution = ConflictResolution {
        prior,
        receipt: DeliveryReceipt {
            request,
            receipt_id: "placeholder".into(),
        },
        authorization_ref: "resolver-1".into(),
    };
    let mut decision = wire_decision(&mut resolution);
    let snapshot = Command::new("python3")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tether_wire_peer.py"))
        .args([
            "--root",
            temp.0.to_str().unwrap(),
            "--expected",
            temp.0.join("expected.json").to_str().unwrap(),
            "--conflict-snapshot",
        ])
        .env("PYTHONPATH", source)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .current_dir(source)
        .output()
        .unwrap();
    assert!(
        snapshot.status.success(),
        "{}",
        String::from_utf8_lossy(&snapshot.stderr)
    );
    decision.conflict_sha256 = digest(&snapshot.stdout);
    std::fs::write(
        temp.0.join("core-resolution.json"),
        serde_json::to_vec(&resolution).unwrap(),
    )
    .unwrap();
    std::fs::write(
        temp.0.join("resolution-decision.json"),
        serde_json::to_vec(&decision).unwrap(),
    )
    .unwrap();
    resolution
}
fn retained_wire_conflicts(temp: &Temp) {
    let outer = Connection::open(temp.0.join("wire.db")).unwrap();
    let inner = Connection::open(temp.0.join("journal.db")).unwrap();
    assert_eq!(
        outer
            .query_row("SELECT conflict FROM wire_deliveries", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        inner
            .query_row("SELECT conflict FROM uploads", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        1
    );
    let decisions = Connection::open(temp.0.join("resolution.db")).unwrap();
    assert_eq!(
        decisions
            .query_row("SELECT count(*) FROM decisions", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        1
    );
    wire_effect_counts(temp);
}
#[test]
#[ignore = "requires exact Tether resolution source and isolated synthetic resolver authority"]
fn actual_tether_resolution_restarts_lost_replies_and_retained_conflicts() {
    let source = PathBuf::from(std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").unwrap());
    for lost in ["", "tether", "core"] {
        let temp = Temp::new();
        let resolution = resolution_seed(&temp, &source);
        if lost == "tether" {
            std::fs::write(temp.0.join("fault.txt"), "lost:resolution-response").unwrap();
        }
        if lost == "core" {
            std::fs::write(temp.0.join("core-resolution-lost-response"), "").unwrap();
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "wire_resolution_child"])
                .env("CORE_WIRE_RESOLUTION_ROOT", &temp.0)
                .env("RUNSPINDLE_TETHER_WIRE_SOURCE", &source)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(32));
            std::fs::remove_file(temp.0.join("core-resolution-lost-response")).unwrap();
        } else {
            let (ok, snapshot) = resolution_step(&temp, &source);
            if lost == "tether" {
                assert!(!ok);
                assert_eq!(snapshot, resolution.prior);
            } else {
                assert!(ok);
                assert!(!snapshot.conflict);
            }
        }
        std::fs::write(temp.0.join("fault.txt"), "").unwrap();
        let (ok, done) = resolution_step(&temp, &source);
        assert!(ok);
        assert!(!done.conflict);
        assert_eq!(done.receipt, Some(resolution.receipt.clone()));
        assert_eq!(resolution_step(&temp, &source), (true, done));
        retained_wire_conflicts(&temp);
    }
}
#[test]
#[ignore = "requires exact Tether resolution source and isolated synthetic resolver authority"]
fn actual_tether_resolution_revocation_and_changed_bytes_fail_closed() {
    let source = PathBuf::from(std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").unwrap());
    for reason in ["core-resolver-revoked", "resolver-revoked", "bytes"] {
        let temp = Temp::new();
        let resolution = resolution_seed(&temp, &source);
        if reason == "bytes" {
            Connection::open(temp.0.join("service.db"))
                .unwrap()
                .execute("UPDATE files SET content=x'00'", [])
                .unwrap();
        } else {
            std::fs::write(temp.0.join(reason), "").unwrap();
        }
        let (ok, state) = resolution_step(&temp, &source);
        assert!(!ok);
        assert_eq!(state, resolution.prior);
        assert_eq!(
            Connection::open(temp.db())
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM outbox_evidence WHERE kind='resolution'",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
    }
}

#[test]
#[ignore = "requires exact Tether resolution source and isolated synthetic resolver authority"]
fn actual_tether_concurrent_same_resolution_retains_one_decision() {
    let source = PathBuf::from(std::env::var("RUNSPINDLE_TETHER_WIRE_SOURCE").unwrap());
    let temp = Temp::new();
    let resolution = resolution_seed(&temp, &source);
    let spawn = || {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "wire_resolution_child"])
            .env("CORE_WIRE_RESOLUTION_ROOT", &temp.0)
            .env("RUNSPINDLE_TETHER_WIRE_SOURCE", &source)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap()
    };
    let mut first = spawn();
    let mut second = spawn();
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
    let (ok, state) = resolution_step(&temp, &source);
    assert!(ok);
    assert_eq!(state.receipt, Some(resolution.receipt));
    assert!(!state.conflict);
    let db = Connection::open(temp.db()).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM outbox_evidence WHERE kind='resolution'",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
    retained_wire_conflicts(&temp);
}

#[test]
fn resolution_bridge_rejects_oversized_refs_and_malformed_selected_identities() {
    use runspindle_core::tether_wire::ResolutionBridge;
    for field in ["decision", "policy", "file", "message", "long-message"] {
        let temp = Temp::new();
        let (_, mut resolution, _) = quarantine(&temp);
        let mut decision = wire_decision(&mut resolution);
        match field {
            "decision" => decision.decision_id = "x".repeat(129),
            "policy" => decision.policy_ref = "x".repeat(129),
            "file" => decision.file_id = "F_lowercase".into(),
            "message" => decision.message_id = "1700000002.00003".into(),
            "long-message" => decision.message_id = format!("{}.000003", "1".repeat(65)),
            _ => unreachable!(),
        }
        resolution.receipt.receipt_id = format!(
            "tether:{}",
            digest(&serde_json::to_vec(&(&decision.file_id, &decision.message_id)).unwrap())
        );
        decision.core_resolution_sha256 =
            digest(&serde_json::to_vec(&serde_json::to_value(&resolution).unwrap()).unwrap());
        let calls = std::rc::Rc::new(std::cell::RefCell::new(vec![]));
        assert!(ResolutionBridge::new(
            &Store::open(temp.db()).unwrap(),
            resolution,
            decision,
            "result.bin",
            "accepted",
            ResolutionWirePeer {
                mode: "confirmed",
                calls: calls.clone()
            }
        )
        .is_err());
        assert!(calls.borrow().is_empty());
    }
}
#[test]
fn matching_resolved_receipt_without_exact_audit_cannot_authorize_replay() {
    let temp = Temp::new();
    let (request, resolution, mut peer) = quarantine(&temp);
    let db = Connection::open(temp.db()).unwrap();
    db.execute(
        "UPDATE outbox SET state='delivered',conflict=0,receipt=?1,generation=?2",
        params![
            serde_json::to_string(&resolution.receipt).unwrap(),
            resolution.prior.generation + 1
        ],
    )
    .unwrap();
    let mut outbox = Outbox::open(temp.db()).unwrap();
    assert!(!outbox.resolution_applicable(&resolution).unwrap());
    let before = peer.count("lookup");
    assert!(outbox
        .resolve(
            &resolution,
            &Grant(request),
            &resolver(&resolution),
            &mut peer
        )
        .is_err());
    assert_eq!(peer.count("lookup"), before);
}
#[test]
fn tether_wire_errors_do_not_retain_peer_or_parser_content() {
    struct RejectedPeer(bool);
    impl runspindle_core::tether_wire::Peer for RejectedPeer {
        fn exchange(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
            if self.0 {
                Err(anyhow::anyhow!("synthetic-private-peer-canary").context("private-context"))
            } else {
                Ok(br#"{"synthetic-private-peer-canary":"private-value"}"#.to_vec())
            }
        }
    }
    for callback in [false, true] {
        let temp = Temp::new();
        let request = request(&temp);
        let mut adapter = runspindle_core::tether_wire::WireTransport::new(
            &Store::open(temp.db()).unwrap(),
            request.clone(),
            "result.bin",
            "accepted",
            RejectedPeer(callback),
        )
        .unwrap();
        let error = adapter.lookup(&request).unwrap_err();
        assert_eq!(error.chain().count(), 1);
        assert_eq!(
            error.to_string(),
            if callback {
                "wire peer exchange failed"
            } else {
                "invalid wire peer observation"
            }
        );
        assert!(!format!("{error:?}").contains("private"));
        assert!(Outbox::open(temp.db())
            .unwrap()
            .snapshot(&request)
            .unwrap()
            .is_none());
    }
}

#[test]
fn resolution_peer_rejections_are_fixed_and_preserve_quarantine() {
    struct RejectedPeer(bool);
    impl runspindle_core::tether_wire::Peer for RejectedPeer {
        fn exchange(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
            if self.0 {
                Err(anyhow::anyhow!("synthetic-private-resolution").context("private-context"))
            } else {
                Ok(br#"{"synthetic-private-resolution":"private-value"}"#.to_vec())
            }
        }
    }
    for callback in [false, true] {
        let temp = Temp::new();
        let (request, mut resolution, _) = quarantine(&temp);
        let decision = wire_decision(&mut resolution);
        let mut bridge = runspindle_core::tether_wire::ResolutionBridge::new(
            &Store::open(temp.db()).unwrap(),
            resolution.clone(),
            decision,
            "result.bin",
            "accepted",
            RejectedPeer(callback),
        )
        .unwrap();
        let mut outbox = Outbox::open(temp.db()).unwrap();
        let error = bridge
            .resolve(&mut outbox, &Grant(request.clone()), &resolver(&resolution))
            .unwrap_err();
        assert_eq!(error.chain().count(), 1);
        assert_eq!(
            error.to_string(),
            if callback {
                "resolution peer exchange failed"
            } else {
                "invalid resolution peer observation"
            }
        );
        assert!(!format!("{error:?}").contains("private"));
        assert_eq!(
            outbox.snapshot(&request).unwrap().unwrap(),
            resolution.prior
        );
    }
}

#[test]
fn changed_accepted_store_blocks_resolution_before_peer_effects() {
    for replay in [false, true] {
        let temp = Temp::new();
        let (request, mut resolution, _) = quarantine(&temp);
        let decision = wire_decision(&mut resolution);
        let calls = std::rc::Rc::new(std::cell::RefCell::new(vec![]));
        let mut bridge = runspindle_core::tether_wire::ResolutionBridge::new(
            &Store::open(temp.db()).unwrap(),
            resolution.clone(),
            decision,
            "result.bin",
            "accepted",
            ResolutionWirePeer {
                mode: "confirmed",
                calls: calls.clone(),
            },
        )
        .unwrap();
        let mut outbox = Outbox::open(temp.db()).unwrap();
        if replay {
            bridge
                .resolve(&mut outbox, &Grant(request.clone()), &resolver(&resolution))
                .unwrap();
        }
        let before = outbox.snapshot(&request).unwrap();
        let count = calls.borrow().len();
        let corrupt = rusqlite::Connection::open(temp.db()).unwrap();
        corrupt
            .execute_batch("DROP TRIGGER artifacts_immutable;")
            .unwrap();
        corrupt
            .execute("UPDATE artifacts SET bytes=?1", [b"corrupted".as_slice()])
            .unwrap();
        corrupt.execute_batch("CREATE TRIGGER artifacts_immutable BEFORE UPDATE ON artifacts BEGIN SELECT RAISE(ABORT,'immutable artifact');END;").unwrap();
        assert!(bridge
            .resolve(&mut outbox, &Grant(request.clone()), &resolver(&resolution))
            .is_err());
        assert_eq!(calls.borrow().len(), count);
        assert_eq!(outbox.snapshot(&request).unwrap(), before);
    }
}

#[test]
fn missing_retained_validation_blocks_delivery_without_transport() {
    let t = Temp::new();
    let r = request(&t);
    let db = Connection::open(t.db()).unwrap();
    let trigger: String = db
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='validations_retained'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.execute_batch("DROP TRIGGER validations_retained; DELETE FROM validations;")
        .unwrap();
    db.execute_batch(&trigger).unwrap();
    let mut outbox = Outbox::open(t.db()).unwrap();
    let mut peer = Peer::open(&t.0.join("peer.sqlite"));
    assert!(outbox.deliver(&r, &Grant(r.clone()), &mut peer).is_err());
    assert_eq!(peer.count("send"), 0);
    assert_eq!(peer.count("lookup"), 0);
    assert!(outbox.snapshot(&r).unwrap().is_none());
    assert_eq!(
        db.query_row("SELECT count(*) FROM validations", [], |row| row
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT bytes FROM artifacts", [], |row| row
            .get::<_, Vec<u8>>(0))
            .unwrap(),
        BYTES
    );
}

#[test]
fn forged_receipt_bindings_for_unchanged_request_never_establish_delivery() {
    struct Forged {
        peer: Peer,
        field: usize,
        stage: usize,
    }
    impl Forged {
        fn corrupt(&self, mut receipt: DeliveryReceipt) -> DeliveryReceipt {
            match self.field {
                0 => receipt.request.accepted.validation.artifact.sha256 = "a".repeat(64),
                1 => {
                    receipt
                        .request
                        .accepted
                        .validation
                        .artifact
                        .artifact_revision += 1
                }
                2 => {
                    receipt
                        .request
                        .accepted
                        .validation
                        .artifact
                        .binding
                        .revision += 1
                }
                3 => receipt.request.destination_id = "wrong-thread".into(),
                _ => receipt.request.account_id = "wrong-account".into(),
            }
            receipt
        }
    }
    impl Transport for Forged {
        fn send(
            &mut self,
            request: &DeliveryRequest,
            bytes: &[u8],
        ) -> anyhow::Result<Option<DeliveryReceipt>> {
            let reply = self.peer.send(request, bytes)?;
            Ok(reply.map(|r| if self.stage != 1 { self.corrupt(r) } else { r }))
        }
        fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
            let observed = self.peer.lookup(request)?;
            Ok(observed
                .into_iter()
                .map(|r| if self.stage != 0 { self.corrupt(r) } else { r })
                .collect())
        }
    }
    for field in 0..5 {
        // Corrupt send only, readback only, or both with the same forged binding.
        for stage in 0..3 {
            let t = Temp::new();
            let r = request(&t);
            let original = serde_json::to_vec(&r).unwrap();
            let mut peer = Forged {
                peer: Peer::open(&t.0.join("peer.sqlite")),
                field,
                stage,
            };
            let mut outbox = Outbox::open(t.db()).unwrap();
            let first = outbox.deliver(&r, &Grant(r.clone()), &mut peer);
            assert!(
                first.is_err() || first.unwrap().state != DeliveryState::Delivered,
                "forged receipt promoted for field {field}, stage {stage}"
            );
            assert_ne!(
                outbox.snapshot(&r).unwrap().unwrap().state,
                DeliveryState::Delivered
            );
            drop(outbox);
            let mut outbox = Outbox::open(t.db()).unwrap();
            let recovered = outbox.deliver(&r, &Grant(r.clone()), &mut peer);
            if stage == 0 {
                // A later independent *correct* readback can resolve an invalid ack.
                let recovered = recovered.unwrap();
                assert_eq!(recovered.state, DeliveryState::Delivered);
                assert_eq!(recovered.receipt.unwrap().request, r);
            } else {
                assert!(recovered.is_err());
                assert_ne!(
                    outbox.snapshot(&r).unwrap().unwrap().state,
                    DeliveryState::Delivered
                );
            }
            assert_eq!(peer.peer.count("send"), 1, "no resend after reopening");
            assert!(peer.peer.count("lookup") >= 1);
            let (receipt, bytes): (String, Vec<u8>) = peer
                .peer
                .db
                .query_row("SELECT receipt,bytes FROM receipts", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap();
            assert_eq!(
                serde_json::from_str::<DeliveryReceipt>(&receipt)
                    .unwrap()
                    .request,
                r
            );
            assert_eq!(bytes, BYTES);
            assert_eq!(digest(&bytes), r.accepted.artifact().sha256);
            let db = Connection::open(t.db()).unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM artifacts", [], |row| row
                    .get::<_, u64>(0))
                    .unwrap(),
                1
            );
            assert_eq!(
                db.query_row("SELECT bytes FROM artifacts", [], |row| row
                    .get::<_, Vec<u8>>(0))
                    .unwrap(),
                BYTES
            );
            assert_eq!(serde_json::to_vec(&r).unwrap(), original);
        }
    }
}
