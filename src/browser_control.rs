//! Canonical Core control ownership in the existing BrowserRegistry database.
//! No Browser-owned store, default issuer, automatic cutover or native activation.
use crate::browser_effect_wire::{Action, Target};
use crate::durable::{digest, SchemaObject};
use serde::{Deserialize, Serialize};

pub const DOMAIN: &str = "runspindle.browser-control.v1";
pub const INTENT_PREFIX: &str = "browser-control:";
pub const CAPACITY: u64 = 128;
pub(crate) const SCHEMA: &[SchemaObject] = &[
    ("table", "browser_control_heads", "CREATE TABLE IF NOT EXISTS browser_control_heads(browser TEXT PRIMARY KEY,scope TEXT NOT NULL CHECK(length(scope)<=8192),operation TEXT NOT NULL UNIQUE REFERENCES browser_operations(operation));"),
    ("trigger", "browser_control_scope_immutable", "CREATE TRIGGER IF NOT EXISTS browser_control_scope_immutable BEFORE UPDATE OF browser,scope ON browser_control_heads BEGIN SELECT RAISE(ABORT,'immutable control scope');END;"),
    ("trigger", "browser_control_heads_retained", "CREATE TRIGGER IF NOT EXISTS browser_control_heads_retained BEFORE DELETE ON browser_control_heads BEGIN SELECT RAISE(ABORT,'retained control head');END;"),
];

fn identifier(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HolderKind {
    Agent,
    Human,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Holder {
    pub kind: HolderKind,
    pub identity: String,
}
impl Holder {
    pub fn valid(&self) -> bool {
        identifier(&self.identity)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferKind {
    Initialize,
    Takeover,
    Handback,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predecessor {
    pub operation: String,
    pub payload_digest: String,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionData {
    pub version: String,
    pub action: TransferKind,
    pub source: Option<Holder>,
    pub destination: Holder,
    pub predecessor: Option<Predecessor>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transition {
    pub target: Target,
    pub data: TransitionData,
}

/// Same conservative exact native/session/frame/account/origin scope as Browser;
/// no implicit widening across navigation, account change or session replacement.
pub fn scope(target: &Target) -> anyhow::Result<String> {
    anyhow::ensure!(target.valid(), "invalid control scope");
    let mut value = serde_json::to_value(target)?;
    let binding = value
        .get_mut("binding")
        .and_then(|v| v.as_object_mut())
        .ok_or_else(|| anyhow::anyhow!("invalid control scope"))?;
    for key in ["intent", "revision", "operation", "epoch"] {
        binding.remove(key);
    }
    let raw = serde_json::to_string(&value)?;
    anyhow::ensure!(raw.len() <= 8192, "control scope capacity");
    Ok(raw)
}
pub fn intent(target: &Target) -> anyhow::Result<String> {
    Ok(format!(
        "{INTENT_PREFIX}{}",
        digest(scope(target)?.as_bytes())
    ))
}
pub fn reserved(target: &Target) -> bool {
    target.binding.intent.starts_with(INTENT_PREFIX)
}
impl Transition {
    pub fn validate(&self) -> anyhow::Result<()> {
        let t = &self.target;
        let d = &self.data;
        anyhow::ensure!(
            t.valid()
                && d.version == DOMAIN
                && d.destination.valid()
                && t.binding.intent == intent(t)?
                && t.binding.revision == t.binding.epoch,
            "invalid control transition"
        );
        let valid = match (&d.action, &d.source, &d.predecessor) {
            (TransferKind::Initialize, None, None) => {
                t.binding.epoch == 1 && d.destination.kind == HolderKind::Agent
            }
            (TransferKind::Takeover, Some(source), Some(_)) => {
                t.binding.epoch > 1
                    && source.valid()
                    && source.kind == HolderKind::Agent
                    && d.destination.kind == HolderKind::Human
            }
            (TransferKind::Handback, Some(source), Some(_)) => {
                t.binding.epoch > 1
                    && source.valid()
                    && source.kind == HolderKind::Human
                    && d.destination.kind == HolderKind::Agent
            }
            _ => false,
        };
        anyhow::ensure!(valid, "invalid control holders");
        if let Some(previous) = &d.predecessor {
            anyhow::ensure!(
                identifier(&previous.operation)
                    && previous.operation != t.binding.operation
                    && previous.payload_digest.len() == 64
                    && previous
                        .payload_digest
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                    && previous.generation <= i64::MAX as u64,
                "invalid control predecessor"
            );
        }
        Ok(())
    }
    pub fn payload(&self) -> anyhow::Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(&self.data)?;
        anyhow::ensure!(bytes.len() <= 16384, "control transition capacity");
        Ok(bytes)
    }
}

/// Trusted current application/native fencing authority, never deserialized from
/// operation payloads. Initialization needs explicit current native proof;
/// handback needs authenticated release, not a generic effect grant.
pub trait Authority {
    fn allow_schema_upgrade(&self) -> bool {
        false
    }
    fn verify(&self, transition: &Transition, action: Action) -> bool;
    fn initialize_proven(&self, _transition: &Transition) -> bool {
        false
    }
    fn released(&self, _transition: &Transition) -> bool {
        false
    }
}
/// Caller holds the real native input fence throughout the callback. A historical
/// control-head snapshot by itself never grants input or credentials.
pub trait InputAuthority {
    fn verify(&self, target: &Target, holder: &Holder) -> bool;
}

impl crate::browser_effect_registry::BrowserRegistry {
    /// Explicit additive migration of the existing Core file. No legacy Browser
    /// file is opened, copied, retired or selected as fallback by this method.
    pub fn upgrade_control_schema<A: Authority>(&mut self, authority: &A) -> anyhow::Result<()> {
        anyhow::ensure!(
            authority.allow_schema_upgrade(),
            "control schema upgrade denied"
        );
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::browser_effect_registry::schema(&tx)?;
        anyhow::ensure!(
            authority.allow_schema_upgrade(),
            "control schema upgrade denied"
        );
        let version: u32 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version == 1 {
            {
                for target in crate::browser_effect_registry::validated_targets(&tx)? {
                    anyhow::ensure!(!reserved(&target), "legacy control namespace collision");
                }
            }
            crate::durable::create_schema(&tx, SCHEMA)?;
            tx.pragma_update(None, "user_version", 2)?;
        }
        crate::browser_effect_registry::schema(&tx)?;
        anyhow::ensure!(
            authority.allow_schema_upgrade(),
            "control schema upgrade denied"
        );
        tx.commit()?;
        Ok(())
    }
}

use crate::browser_effect_registry::{
    identity, read, schema, BrowserRegistry, HistoricalSnapshot, Record,
};
use crate::browser_effect_wire::{Client, Outcome, Peer};
use rusqlite::{params, Connection, TransactionBehavior};
use std::collections::{HashMap, HashSet};

fn admitted<A: Authority>(a: &A, t: &Transition, action: Action) -> bool {
    a.verify(t, action)
        && match t.data.action {
            TransferKind::Initialize => a.initialize_proven(t),
            TransferKind::Handback => a.released(t),
            TransferKind::Takeover => true,
        }
}
struct Grant<'a, A>(&'a A, &'a Transition);
impl<A: Authority> crate::browser_effect_wire::Authority for Grant<'_, A> {
    fn verify(&self, target: &Target, commitment: &str, action: Action) -> bool {
        target == &self.1.target
            && self
                .1
                .payload()
                .map(|p| digest(&p) == commitment)
                .unwrap_or(false)
            && admitted(self.0, self.1, action)
    }
}
fn transition(record: &Record) -> anyhow::Result<Transition> {
    anyhow::ensure!(record.payload.len() <= 16384, "control payload bound");
    let value = Transition {
        target: record.snapshot.target.clone(),
        data: serde_json::from_slice(&record.payload)?,
    };
    anyhow::ensure!(
        value.payload()? == record.payload,
        "noncanonical control payload"
    );
    Ok(value)
}
/// Validate every retained control record and pointer, not just the requested
/// head: orphaned reservations, forks and rewritten predecessor generations fail
/// closed. Holder state exists only in the transition payload, never the pointer.
fn heads(connection: &Connection) -> anyhow::Result<HashMap<String, Record>> {
    schema(connection)?;
    let version: u32 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    anyhow::ensure!(version == 2, "control schema not enabled");
    let mut records = HashMap::new();
    for target in crate::browser_effect_registry::validated_targets(connection)? {
        if reserved(&target) {
            let record = read(connection, &target)?
                .ok_or_else(|| anyhow::anyhow!("missing control record"))?;
            transition(&record)?;
            records.insert(target.binding.operation.clone(), record);
        }
    }
    anyhow::ensure!(records.len() <= CAPACITY as usize, "control capacity");
    let mut visited = HashSet::new();
    let mut result = HashMap::new();
    let mut statement =
        connection.prepare("SELECT CASE WHEN typeof(browser)='text' AND length(CAST(browser AS BLOB)) BETWEEN 1 AND 128 THEN browser END,CASE WHEN typeof(scope)='text' AND length(CAST(scope AS BLOB))<=8192 THEN scope END,CASE WHEN typeof(operation)='text' AND length(CAST(operation AS BLOB)) BETWEEN 1 AND 128 THEN operation END FROM browser_control_heads")?;
    for row in statement.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (browser, bound_scope, operation) = row?;
        let head = records
            .get(&operation)
            .ok_or_else(|| anyhow::anyhow!("missing control head"))?;
        let mut current = head;
        loop {
            let value = transition(current)?;
            anyhow::ensure!(
                value.target.binding.browser == browser
                    && scope(&value.target)? == bound_scope
                    && visited.insert(value.target.binding.operation.clone()),
                "invalid control chain"
            );
            match &value.data.predecessor {
                None => break,
                Some(previous) => {
                    let retained = records
                        .get(&previous.operation)
                        .ok_or_else(|| anyhow::anyhow!("missing predecessor"))?;
                    let prior = transition(retained)?;
                    anyhow::ensure!(
                        retained.snapshot.payload_digest == previous.payload_digest
                            && retained.snapshot.generation == previous.generation
                            && !retained.snapshot.conflict
                            && retained.snapshot.observation == Outcome::Confirmed
                            && prior.target.binding.epoch.checked_add(1)
                                == Some(value.target.binding.epoch)
                            && value.data.source.as_ref() == Some(&prior.data.destination),
                        "invalid control predecessor"
                    );
                    current = retained;
                }
            }
        }
        result.insert(browser, read(connection, &head.snapshot.target)?.unwrap());
    }
    anyhow::ensure!(visited.len() == records.len(), "orphaned control record");
    Ok(result)
}

impl BrowserRegistry {
    /// Persist reservation and head CAS before invoking the native fencing peer.
    /// Even a successful dispatch receipt is unknown until separate readback.
    pub fn dispatch_control<A: Authority, P: Peer>(
        &mut self,
        value: &Transition,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        let payload = value.payload()?;
        anyhow::ensure!(
            admitted(authority, value, Action::Dispatch),
            "control denied"
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = heads(&tx)?;
        if let Some(old) = read(&tx, &value.target)? {
            anyhow::ensure!(old.payload == payload, "control reservation conflict");
            return Ok(Outcome::Unknown);
        }
        let previous = current.get(&value.target.binding.browser);
        match previous {
            None => anyhow::ensure!(
                value.data.action == TransferKind::Initialize,
                "missing control initialization"
            ),
            Some(old) => {
                let predecessor = value
                    .data
                    .predecessor
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing predecessor"))?;
                let prior = transition(old)?;
                anyhow::ensure!(
                    scope(&value.target)? == scope(&prior.target)?
                        && !old.snapshot.conflict
                        && old.snapshot.observation == Outcome::Confirmed
                        && predecessor.operation == prior.target.binding.operation
                        && predecessor.payload_digest == old.snapshot.payload_digest
                        && predecessor.generation == old.snapshot.generation
                        && value.target.binding.epoch
                            == prior
                                .target
                                .binding
                                .epoch
                                .checked_add(1)
                                .ok_or_else(|| anyhow::anyhow!("epoch exhausted"))?
                        && value.data.source.as_ref() == Some(&prior.data.destination),
                    "control head conflict"
                );
            }
        }
        let count = crate::browser_effect_registry::validated_targets(&tx)?
            .iter()
            .filter(|t| reserved(t))
            .count() as u64;
        anyhow::ensure!(
            count < CAPACITY && admitted(authority, value, Action::Dispatch),
            "control admission denied"
        );
        tx.execute(
            "INSERT INTO browser_operations VALUES(?1,?2,?3,?4,?5,'unknown',0,0)",
            params![
                identity(&value.target)?,
                value.target.binding.operation,
                serde_json::to_string(&value.target)?,
                payload,
                digest(&payload)
            ],
        )?;
        match previous {
            None => {
                tx.execute(
                    "INSERT INTO browser_control_heads VALUES(?1,?2,?3)",
                    params![
                        value.target.binding.browser,
                        scope(&value.target)?,
                        value.target.binding.operation
                    ],
                )?;
            }
            Some(old) => {
                anyhow::ensure!(tx.execute("UPDATE browser_control_heads SET operation=?1 WHERE browser=?2 AND operation=?3", params![value.target.binding.operation, value.target.binding.browser, old.snapshot.target.binding.operation])? == 1, "control CAS failed");
            }
        }
        heads(&tx)?;
        anyhow::ensure!(
            admitted(authority, value, Action::Dispatch),
            "control denied"
        );
        tx.commit()?;
        // An exception/cancellation after commit cannot permit a second fence.
        let _ = Client::new(value.target.clone(), &payload)?
            .dispatch(&Grant(authority, value), peer)?;
        Ok(Outcome::Unknown)
    }
    /// Only the exact current head may collect readback; never re-fences.
    pub fn reconcile_control<A: Authority, P: Peer>(
        &mut self,
        value: &Transition,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        let payload = value.payload()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = heads(&tx)?;
        let old = current
            .get(&value.target.binding.browser)
            .ok_or_else(|| anyhow::anyhow!("missing control head"))?;
        anyhow::ensure!(
            old.snapshot.target == value.target
                && old.payload == payload
                && admitted(authority, value, Action::Reconcile),
            "control reconcile denied"
        );
        if old.snapshot.conflict {
            return Ok(Outcome::Unknown);
        }
        let outcome = Client::new(value.target.clone(), &payload)?
            .reconcile(&Grant(authority, value), peer)?;
        anyhow::ensure!(
            admitted(authority, value, Action::Reconcile),
            "control reconcile denied"
        );
        let conflict =
            old.snapshot.observation != Outcome::Unknown && old.snapshot.observation != outcome;
        let retained = if old.snapshot.observation == Outcome::Unknown {
            outcome
        } else {
            old.snapshot.observation
        };
        let label = match retained {
            Outcome::Unknown => "unknown",
            Outcome::Confirmed => "confirmed",
            Outcome::Rejected => "rejected",
        };
        let generation = old
            .snapshot
            .generation
            .checked_add(1)
            .filter(|g| *g <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("generation exhausted"))?;
        tx.execute("UPDATE browser_operations SET observation=?1,conflict=?2,generation=?3 WHERE operation=?4", params![label, conflict, generation, value.target.binding.operation])?;
        tx.commit()?;
        Ok(if conflict { Outcome::Unknown } else { outcome })
    }
    /// Passive evidence only. No lease/token is issued by reading a head.
    pub fn historical_control(
        &self,
        target: &Target,
    ) -> anyhow::Result<Option<HistoricalSnapshot>> {
        let tx = self.connection.unchecked_transaction()?;
        let current = heads(&tx)?;
        let result = current
            .get(&target.binding.browser)
            .map(|r| {
                anyhow::ensure!(
                    scope(target)? == scope(&r.snapshot.target)?,
                    "control scope mismatch"
                );
                Ok(r.snapshot.clone())
            })
            .transpose()?;
        tx.commit()?;
        Ok(result)
    }
    /// Serialize input with transfer reservations in the SAME Core database.
    /// The caller must additionally hold the real native fence; this does not
    /// manufacture an authenticated native holder token or credential grant.
    pub fn with_control_input<A: InputAuthority, T>(
        &mut self,
        target: &Target,
        holder: &Holder,
        authority: &A,
        run: impl FnOnce() -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = heads(&tx)?;
        let head = current
            .get(&target.binding.browser)
            .ok_or_else(|| anyhow::anyhow!("missing control head"))?;
        let value = transition(head)?;
        anyhow::ensure!(
            !head.snapshot.conflict
                && head.snapshot.observation == Outcome::Confirmed
                && scope(target)? == scope(&value.target)?
                && target.binding.epoch == value.target.binding.epoch
                && holder == &value.data.destination
                && authority.verify(target, holder),
            "control input denied"
        );
        let result = run()?;
        anyhow::ensure!(
            authority.verify(target, holder),
            "control input authority lost"
        );
        tx.commit()?;
        Ok(result)
    }
}
