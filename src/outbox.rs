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
        let first = old
            .acknowledgement
            .as_ref()
            .or(old.receipt.as_ref())
            .or(valid.first().copied());
        first.is_some_and(|first| valid.iter().any(|receipt| *receipt != first))
    }
    fn remember(
        &mut self,
        request: &DeliveryRequest,
        acknowledgement: Option<DeliveryReceipt>,
        conflict: bool,
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
        Ok(DeliverySnapshot {
            request: request.clone(),
            state,
            receipt: old.receipt,
            generation: next,
            acknowledgement,
            conflict,
        })
    }
    fn settle(
        &mut self,
        old: &DeliverySnapshot,
        receipt: Option<DeliveryReceipt>,
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
        anyhow::ensure!(
            !current.conflict,
            "delivery receipt conflict is durably fenced"
        );
        if receipt
            .as_ref()
            .is_some_and(|r| Self::contradiction(&current, std::slice::from_ref(r)))
        {
            drop(tx);
            self.remember(&old.request, None, true)?;
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
                self.settle(&reserved, None)?;
                anyhow::bail!("send receipt mismatch")
            }
            reserved = self.remember(request, Some(reply.clone()), false)?;
            anyhow::ensure!(
                !reserved.conflict,
                "acknowledgement contradicts retained receipt"
            );
            let observed = transport.lookup(request).unwrap_or_default();
            if Self::contradiction(&reserved, &observed) {
                self.remember(request, None, true)?;
                anyhow::bail!("send/readback receipt conflict")
            }

            if observed.len() == 1 && observed[0] == reply {
                return self.settle(&reserved, Some(reply));
            }
        }
        self.settle(&reserved, None)
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
        let observed = transport.lookup(request).unwrap_or_default();
        if Self::contradiction(&old, &observed) {
            self.remember(request, None, true)?;
            anyhow::bail!("contradictory readback receipts")
        }
        if observed.len() != 1 || !observed[0].matches(request) {
            if old.state == DeliveryState::Delivered {
                anyhow::bail!("retained delivery conflicts with readback")
            }
            self.settle(&old, None)?;
            anyhow::bail!("delivery readback unknown or conflicting")
        }
        self.settle(&old, observed.into_iter().next())
    }
}
