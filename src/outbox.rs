//! Durable single-send delivery reservation. Reconciliation never resends.
use crate::durable::{self, Accepted, Store};
use crate::runtime::valid_id;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRequest {
    pub version: String,
    pub accepted: Accepted,
    pub account_id: String,
    pub destination_id: String,
    pub delivery_operation_id: String,
}
impl DeliveryRequest {
    pub fn valid(&self) -> bool {
        self.version == durable::VERSION
            && self.delivery_operation_id != self.accepted.artifact().binding.operation_id
            && self.accepted.validation.valid()
            && self.accepted.validation.passed
            && [
                &self.account_id,
                &self.destination_id,
                &self.delivery_operation_id,
            ]
            .iter()
            .all(|s| valid_id(s))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReceipt {
    pub request: DeliveryRequest,
    pub receipt_id: String,
}
impl DeliveryReceipt {
    fn matches(&self, request: &DeliveryRequest) -> bool {
        &self.request == request && valid_id(&self.receipt_id)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Reserved,
    Unknown,
    Delivered,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliverySnapshot {
    pub request: DeliveryRequest,
    pub state: DeliveryState,
    pub receipt: Option<DeliveryReceipt>,
    pub generation: u64,
    pub acknowledgement: Option<DeliveryReceipt>,
    pub conflict: bool,
}
/// A separately authorized decision, bound to an exact quarantined generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConflictResolution {
    pub prior: DeliverySnapshot,
    pub receipt: DeliveryReceipt,
    pub authorization_ref: String,
}
/// Trusted application seam; ordinary delivery/admission grants do not implement it.
pub trait ResolutionAuthority {
    fn verify_resolution(&self, resolution: &ConflictResolution) -> bool;
}
impl ResolutionAuthority for durable::Deny {
    fn verify_resolution(&self, _: &ConflictResolution) -> bool {
        false
    }
}
pub trait DeliveryAuthority {
    fn verify(&self, request: &DeliveryRequest) -> bool;
}
impl DeliveryAuthority for durable::Deny {
    fn verify(&self, _: &DeliveryRequest) -> bool {
        false
    }
}
/// Error/None after send means unknown; lookup is read-only and never sends.
pub trait Transport {
    fn send(
        &mut self,
        request: &DeliveryRequest,
        bytes: &[u8],
    ) -> anyhow::Result<Option<DeliveryReceipt>>;
    fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>>;
}
type StoredDelivery = (String, String, Option<String>, u64, Option<String>, bool);
pub struct Outbox {
    store: Store,
}
impl Outbox {
    pub fn open(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let mut store = Store::open(path)?;
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS outbox_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL);INSERT OR IGNORE INTO outbox_schema VALUES(1,1);
      CREATE TABLE IF NOT EXISTS outbox(operation TEXT PRIMARY KEY,request TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','unknown','delivered')),receipt TEXT,generation INTEGER NOT NULL,acknowledgement TEXT,conflict INTEGER NOT NULL CHECK(conflict IN (0,1)));
      CREATE TRIGGER IF NOT EXISTS outbox_binding_immutable BEFORE UPDATE OF operation,request ON outbox BEGIN SELECT RAISE(ABORT,'immutable delivery binding');END;
      CREATE TRIGGER IF NOT EXISTS outbox_retained BEFORE DELETE ON outbox BEGIN SELECT RAISE(ABORT,'retained delivery');END;")?;
        let version: u64 =
            tx.query_row("SELECT version FROM outbox_schema WHERE id=1", [], |r| {
                r.get(0)
            })?;
        anyhow::ensure!(version == 1, "unsupported outbox version");
        tx.execute_batch("CREATE TABLE IF NOT EXISTS outbox_evidence(operation TEXT NOT NULL,generation INTEGER NOT NULL,kind TEXT NOT NULL,record TEXT NOT NULL,PRIMARY KEY(operation,generation,kind));
          CREATE TRIGGER IF NOT EXISTS outbox_evidence_immutable BEFORE UPDATE ON outbox_evidence BEGIN SELECT RAISE(ABORT,'immutable delivery evidence');END;
          CREATE TRIGGER IF NOT EXISTS outbox_evidence_retained BEFORE DELETE ON outbox_evidence BEGIN SELECT RAISE(ABORT,'retained delivery evidence');END;")?;
        tx.commit()?;
        Ok(Self { store })
    }
    fn read(
        connection: &rusqlite::Connection,
        request: &DeliveryRequest,
    ) -> anyhow::Result<Option<DeliverySnapshot>> {
        let row: Option<StoredDelivery> = connection
            .query_row(
                "SELECT request,state,receipt,generation,acknowledgement,conflict FROM outbox WHERE operation=?1",
                [&request.delivery_operation_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,r.get(4)?,r.get(5)?)),
            )
            .optional()?;
        let Some((stored, state, receipt, generation, acknowledgement, conflict)) = row else {
            return Ok(None);
        };
        anyhow::ensure!(
            serde_json::from_str::<DeliveryRequest>(&stored)? == *request,
            "delivery operation binding conflict"
        );
        let state = match state.as_str() {
            "reserved" => DeliveryState::Reserved,
            "unknown" => DeliveryState::Unknown,
            "delivered" => DeliveryState::Delivered,
            _ => anyhow::bail!("unsupported delivery state"),
        };
        let receipt: Option<DeliveryReceipt> =
            receipt.map(|s| serde_json::from_str(&s)).transpose()?;
        let acknowledgement: Option<DeliveryReceipt> = acknowledgement
            .map(|s| serde_json::from_str(&s))
            .transpose()?;
        anyhow::ensure!(
            acknowledgement.as_ref().is_none_or(|r| r.matches(request)),
            "invalid acknowledgement binding"
        );
        anyhow::ensure!(
            (state == DeliveryState::Delivered) == receipt.is_some()
                && receipt.as_ref().is_none_or(|r| r.matches(request)),
            "invalid retained receipt"
        );
        Ok(Some(DeliverySnapshot {
            request: request.clone(),
            state,
            receipt,
            generation,
            acknowledgement,
            conflict,
        }))
    }
    pub fn snapshot(&self, request: &DeliveryRequest) -> anyhow::Result<Option<DeliverySnapshot>> {
        anyhow::ensure!(request.valid(), "invalid delivery version/binding");
        Self::read(&self.store.connection, request)
    }
    fn contradiction(old: &DeliverySnapshot, observed: &[DeliveryReceipt]) -> bool {
        let valid = observed
            .iter()
            .filter(|r| r.matches(&old.request))
            .collect::<Vec<_>>();
        // A separately resolved terminal receipt supersedes an old transport ack;
        // both remain retained in the immutable resolution evidence.
        let first = old
            .receipt
            .as_ref()
            .or(old.acknowledgement.as_ref())
            .or(valid.first().copied());
        first.is_some_and(|first| valid.iter().any(|receipt| *receipt != first))
    }
    fn remember(
        &mut self,
        request: &DeliveryRequest,
        acknowledgement: Option<DeliveryReceipt>,
        conflict: bool,
        observed: &[DeliveryReceipt],
    ) -> anyhow::Result<DeliverySnapshot> {
        if let Some(ref ack) = acknowledgement {
            anyhow::ensure!(ack.matches(request), "acknowledgement binding conflict");
        }
        let tx = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old =
            Self::read(&tx, request)?.ok_or_else(|| anyhow::anyhow!("missing reservation"))?;
        if old.conflict {
            return Ok(old);
        }
        let conflict = conflict
            || acknowledgement
                .as_ref()
                .is_some_and(|ack| Self::contradiction(&old, std::slice::from_ref(ack)));
        // Audit capacity must not roll back an observed contradiction. The
        // conflict flag itself is the durable fence when evidence is incomplete.
        let mut incomplete_evidence = false;
        if conflict {
            let record = serde_json::to_string(&(&old, &acknowledgement, observed))?;
            if Self::evidence_room(&tx, &record)? {
                Self::evidence(&tx, request, old.generation, "conflict", &record)?;
            } else {
                incomplete_evidence = true;
            }
        }
        let acknowledgement = old.acknowledgement.or(acknowledgement);
        let next = old
            .generation
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("delivery generation capacity"))?;
        let state = if old.state == DeliveryState::Delivered {
            DeliveryState::Delivered
        } else {
            DeliveryState::Unknown
        };
        let state_text = if state == DeliveryState::Delivered {
            "delivered"
        } else {
            "unknown"
        };
        anyhow::ensure!(tx.execute("UPDATE outbox SET state=?1,acknowledgement=?2,conflict=?3,generation=?4 WHERE operation=?5 AND generation=?6",params![state_text,acknowledgement.as_ref().map(serde_json::to_string).transpose()?,conflict,next,request.delivery_operation_id,old.generation])?==1,"delivery ownership conflict");
        tx.commit()?;
        anyhow::ensure!(!incomplete_evidence, "delivery conflict durably fenced; evidence capacity exhausted");
        Ok(DeliverySnapshot {
            request: request.clone(),
            state,
            receipt: old.receipt,
            generation: next,
            acknowledgement,
            conflict,
        })
    }
    fn settle<A: DeliveryAuthority>(
        &mut self,
        old: &DeliverySnapshot,
        receipt: Option<DeliveryReceipt>,
        authority: &A,
    ) -> anyhow::Result<DeliverySnapshot> {
        if let Some(ref receipt) = receipt {
            anyhow::ensure!(receipt.matches(&old.request), "receipt binding conflict");
        }
        let tx = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            Self::read(&tx, &old.request)?.ok_or_else(|| anyhow::anyhow!("missing reservation"))?;
        anyhow::ensure!(authority.verify(&old.request), "settlement authority unavailable");
        anyhow::ensure!(
            !current.conflict,
            "delivery receipt conflict is durably fenced"
        );
        if receipt
            .as_ref()
            .is_some_and(|r| Self::contradiction(&current, std::slice::from_ref(r)))
        {
            drop(tx);
            self.remember(&old.request, None, true, receipt.as_slice())?;
            anyhow::bail!("contradictory delivery receipt")
        }
        if current.state == DeliveryState::Delivered {
            anyhow::ensure!(
                receipt.as_ref() == current.receipt.as_ref(),
                "conflicting delivery receipt"
            );
            return Ok(current);
        }
        anyhow::ensure!(
            current.generation == old.generation,
            "stale delivery owner; reconcile"
        );
        let next = old
            .generation
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("delivery generation capacity"))?;
        let state = if receipt.is_some() {
            DeliveryState::Delivered
        } else {
            DeliveryState::Unknown
        };
        let state_text = if receipt.is_some() {
            "delivered"
        } else {
            "unknown"
        };
        let count=tx.execute("UPDATE outbox SET state=?1,receipt=?2,generation=?3 WHERE operation=?4 AND generation=?5",params![state_text,receipt.as_ref().map(serde_json::to_string).transpose()?,next,old.request.delivery_operation_id,old.generation])?;
        anyhow::ensure!(count == 1, "delivery ownership conflict");
        tx.commit()?;
        Ok(DeliverySnapshot {
            request: old.request.clone(),
            state,
            receipt,
            generation: next,
            acknowledgement: current.acknowledgement,
            conflict: false,
        })
    }
    fn evidence_room(tx: &rusqlite::Transaction<'_>, record: &str) -> anyhow::Result<bool> {
        if record.len() > durable::MAX_BYTES {
            return Ok(false);
        }
        let count: u64 = tx.query_row("SELECT count(*) FROM outbox_evidence", [], |r| r.get(0))?;
        Ok(count < durable::CAPACITY)
    }
    fn evidence(
        tx: &rusqlite::Transaction<'_>,
        request: &DeliveryRequest,
        generation: u64,
        kind: &str,
        record: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(Self::evidence_room(tx, record)?, "delivery evidence capacity");
        tx.execute("INSERT INTO outbox_evidence VALUES(?1,?2,?3,?4)", params![request.delivery_operation_id,generation,kind,record])?;
        Ok(())
    }
    /// Resolve quarantine using independent readback only. Never calls send.
    /// Authority callbacks must be fresh, bounded and non-reentrant; a remote
    /// provider must enforce its own atomic authority/route fence.
    pub fn resolve<A: DeliveryAuthority, R: ResolutionAuthority, T: Transport>(
        &mut self,
        resolution: &ConflictResolution,
        authority: &A,
        resolver: &R,
        transport: &mut T,
    ) -> anyhow::Result<DeliverySnapshot> {
        let prior = &resolution.prior;
        let request = &prior.request;
        anyhow::ensure!(request.valid() && prior.conflict && resolution.receipt.matches(request)
            && valid_id(&resolution.authorization_ref), "invalid conflict resolution");
        anyhow::ensure!(authority.verify(request) && resolver.verify_resolution(resolution), "resolution denied");
        self.store.accepted(&request.accepted)?;
        anyhow::ensure!(self.snapshot(request)?.as_ref() == Some(prior), "stale resolution generation");
        anyhow::ensure!(authority.verify(request) && resolver.verify_resolution(resolution), "resolution readback denied");
        let observed = transport.lookup(request)?;
        anyhow::ensure!(observed.as_slice() == std::slice::from_ref(&resolution.receipt), "resolution requires exact unique readback");
        anyhow::ensure!(authority.verify(request) && resolver.verify_resolution(resolution), "resolution authority changed");
        let tx = self.store.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        anyhow::ensure!(Self::read(&tx, request)?.as_ref() == Some(prior), "stale resolution owner");
        anyhow::ensure!(authority.verify(request) && resolver.verify_resolution(resolution), "resolution authority unavailable after lock");
        let next = prior.generation.checked_add(1).filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("delivery generation capacity"))?;
        Self::evidence(&tx, request, prior.generation, "resolution", &serde_json::to_string(resolution)?)?;
        anyhow::ensure!(tx.execute("UPDATE outbox SET state='delivered',receipt=?1,conflict=0,generation=?2 WHERE operation=?3 AND generation=?4 AND conflict=1",params![serde_json::to_string(&resolution.receipt)?,next,request.delivery_operation_id,prior.generation])? == 1, "resolution ownership conflict");
        tx.commit()?;
        Ok(DeliverySnapshot { request: request.clone(), state: DeliveryState::Delivered,
            receipt: Some(resolution.receipt.clone()), generation: next,
            acknowledgement: prior.acknowledgement.clone(), conflict: false })
    }
    pub fn deliver<A: DeliveryAuthority, T: Transport>(
        &mut self,
        request: &DeliveryRequest,
        authority: &A,
        transport: &mut T,
    ) -> anyhow::Result<DeliverySnapshot> {
        anyhow::ensure!(request.valid(), "invalid delivery version/binding");
        anyhow::ensure!(authority.verify(request), "delivery denied");
        self.store.accepted(&request.accepted)?;
        let tx = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if Self::read(&tx, request)?.is_some() {
            drop(tx);
            return self.reconcile(request, authority, transport);
        }
        durable::room(&tx, "outbox")?;
        tx.execute(
            "INSERT INTO outbox VALUES(?1,?2,'reserved',NULL,0,NULL,0)",
            params![
                request.delivery_operation_id,
                serde_json::to_string(request)?
            ],
        )?;
        tx.commit()?;
        let mut reserved = DeliverySnapshot {
            request: request.clone(),
            state: DeliveryState::Reserved,
            receipt: None,
            generation: 0,
            acknowledgement: None,
            conflict: false,
        };
        let bytes = self.store.accepted(&request.accepted)?;
        anyhow::ensure!(
            durable::digest(&bytes) == request.accepted.artifact().sha256,
            "accepted byte digest changed"
        );
        anyhow::ensure!(
            authority.verify(request),
            "delivery authority revoked after reservation"
        );
        // This call path is reachable only after a newly committed reservation. An
        // existing reservation can only reach read-only reconciliation above.
        let reply = transport.send(request, &bytes).ok().flatten();
        if let Some(reply) = reply {
            if !reply.matches(request) {
                self.settle(&reserved, None, authority)?;
                anyhow::bail!("send receipt mismatch")
            }
            reserved = self.remember(request, Some(reply.clone()), false, &[])?;
            anyhow::ensure!(
                !reserved.conflict,
                "acknowledgement contradicts retained receipt"
            );
            anyhow::ensure!(authority.verify(request), "readback authority unavailable");
            let observed = transport.lookup(request).unwrap_or_default();
            anyhow::ensure!(authority.verify(request), "readback authority changed");
            if Self::contradiction(&reserved, &observed) {
                self.remember(request, None, true, &observed)?;
                anyhow::bail!("send/readback receipt conflict")
            }

            if observed.len() == 1 && observed[0] == reply {
                return self.settle(&reserved, Some(reply), authority);
            }
        }
        self.settle(&reserved, None, authority)
    }
    pub fn reconcile<A: DeliveryAuthority, T: Transport>(
        &mut self,
        request: &DeliveryRequest,
        authority: &A,
        transport: &mut T,
    ) -> anyhow::Result<DeliverySnapshot> {
        anyhow::ensure!(
            request.valid() && authority.verify(request),
            "delivery reconciliation denied"
        );
        self.store.accepted(&request.accepted)?;
        let old = self
            .snapshot(request)?
            .ok_or_else(|| anyhow::anyhow!("missing reservation"))?;
        anyhow::ensure!(!old.conflict, "delivery receipt conflict is durably fenced");
        anyhow::ensure!(authority.verify(request), "readback authority unavailable");
        let observed = transport.lookup(request).unwrap_or_default();
        anyhow::ensure!(authority.verify(request), "readback authority changed");
        if Self::contradiction(&old, &observed) {
            self.remember(request, None, true, &observed)?;
            anyhow::bail!("contradictory readback receipts")
        }
        if observed.len() != 1 || !observed[0].matches(request) {
            if old.state == DeliveryState::Delivered {
                anyhow::bail!("retained delivery conflicts with readback")
            }
            self.settle(&old, None, authority)?;
            anyhow::bail!("delivery readback unknown or conflicting")
        }
        self.settle(&old, observed.into_iter().next(), authority)
    }
}
