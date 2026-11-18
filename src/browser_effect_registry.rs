//! Dedicated durable single-dispatch Browser registry, not a Temporal workflow.
//! Historical observations are never authority. A reservation can remain unknown
//! forever after a crash before send; neither restart nor reconciliation resends.
use crate::browser_effect_wire::{Action, Authority, Client, Outcome, Peer, Target, MAX_PAYLOAD};
use crate::durable::{create_schema, digest, require_schema, SchemaObject};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::{path::Path, time::Duration};

pub const CAPACITY: u64 = 1024;
pub(crate) const APPLICATION_ID: u32 = 0x42524547;
const SCHEMA: &[SchemaObject] = &[
    ("table", "browser_operations", "CREATE TABLE IF NOT EXISTS browser_operations(identity TEXT PRIMARY KEY,operation TEXT NOT NULL UNIQUE,target TEXT NOT NULL CHECK(length(target)<=8192),payload BLOB NOT NULL CHECK(length(payload)>0 AND length(payload)<=1048576),digest TEXT NOT NULL CHECK(length(digest)=64),observation TEXT NOT NULL CHECK(observation IN ('unknown','confirmed','rejected')),conflict INTEGER NOT NULL CHECK(conflict IN (0,1)),generation INTEGER NOT NULL CHECK(generation>=0));"),
    ("trigger", "browser_binding_immutable", "CREATE TRIGGER IF NOT EXISTS browser_binding_immutable BEFORE UPDATE OF identity,operation,target,payload,digest ON browser_operations BEGIN SELECT RAISE(ABORT,'immutable browser binding');END;"),
    ("trigger", "browser_operations_retained", "CREATE TRIGGER IF NOT EXISTS browser_operations_retained BEFORE DELETE ON browser_operations BEGIN SELECT RAISE(ABORT,'retained browser operation');END;"),
];

/// Historical local evidence only, not permission or a current site receipt.
#[derive(Clone, Debug)]
pub struct HistoricalSnapshot {
    pub target: Target,
    pub payload_digest: String,
    pub observation: Outcome,
    pub conflict: bool,
    pub generation: u64,
}
pub(crate) struct Record {
    pub(crate) snapshot: HistoricalSnapshot,
    pub(crate) payload: Vec<u8>,
}
pub struct BrowserRegistry {
    pub(crate) connection: Connection,
}
pub(crate) fn identity(target: &Target) -> anyhow::Result<String> {
    Ok(serde_json::to_string(&(
        &target.binding.principal,
        &target.binding.intent,
        target.binding.revision,
    ))?)
}
fn observation(value: &str) -> anyhow::Result<Outcome> {
    match value {
        "unknown" => Ok(Outcome::Unknown),
        "confirmed" => Ok(Outcome::Confirmed),
        "rejected" => Ok(Outcome::Rejected),
        _ => anyhow::bail!("invalid browser observation"),
    }
}
fn label(value: Outcome) -> &'static str {
    match value {
        Outcome::Unknown => "unknown",
        Outcome::Confirmed => "confirmed",
        Outcome::Rejected => "rejected",
    }
}
pub(crate) fn schema(connection: &Connection) -> anyhow::Result<()> {
    let app: u32 = connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: u32 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    anyhow::ensure!(
        app == APPLICATION_ID && (version == 1 || version == 2),
        "unsupported browser registry"
    );
    require_schema(connection, SCHEMA)?;
    if version == 2 {
        require_schema(connection, crate::browser_control::SCHEMA)?;
        let heads: u64 =
            connection.query_row("SELECT count(*) FROM browser_control_heads", [], |r| {
                r.get(0)
            })?;
        anyhow::ensure!(
            heads <= crate::browser_control::CAPACITY,
            "control head capacity"
        );
    }
    let objects: u64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    anyhow::ensure!(
        objects
            == (SCHEMA.len()
                + if version == 2 {
                    crate::browser_control::SCHEMA.len()
                } else {
                    0
                }) as u64,
        "unexpected browser registry schema"
    );
    let rows: u64 =
        connection.query_row("SELECT count(*) FROM browser_operations", [], |r| r.get(0))?;
    anyhow::ensure!(
        rows <= CAPACITY
            + if version == 2 {
                crate::browser_control::CAPACITY
            } else {
                0
            },
        "browser registry capacity"
    );
    Ok(())
}
fn empty(connection: &Connection) -> anyhow::Result<bool> {
    let app: u32 = connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: u32 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let objects: u64 =
        connection.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))?;
    Ok(app == 0 && version == 0 && objects == 0)
}
pub(crate) fn validated_targets(connection: &Connection) -> anyhow::Result<Vec<Target>> {
    schema(connection)?;
    // Scan only bounded metadata before trusting absence. Corrupting both SQL
    // keys must not hide an old reservation and permit a second dispatch.
    let mut metadata=connection.prepare("SELECT CASE WHEN typeof(identity)='text' AND length(CAST(identity AS BLOB))<=1024 THEN identity END,CASE WHEN typeof(operation)='text' AND length(CAST(operation AS BLOB))<=128 THEN operation END,CASE WHEN typeof(target)='text' AND length(CAST(target AS BLOB))<=8192 THEN target END FROM browser_operations")?;
    let rows = metadata.query_map([], |r| {
        Ok((
            r.get::<_, Option<String>>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut targets = Vec::new();
    for row in rows {
        let (Some(key), Some(op), Some(raw)) = row? else {
            anyhow::bail!("invalid browser metadata bounds")
        };
        let retained: Target =
            serde_json::from_str(&raw).map_err(|_| anyhow::anyhow!("invalid browser metadata"))?;
        anyhow::ensure!(
            retained.valid() && key == identity(&retained)? && op == retained.binding.operation,
            "invalid browser metadata binding"
        );
        targets.push(retained);
    }
    Ok(targets)
}
pub(crate) fn read(connection: &Connection, target: &Target) -> anyhow::Result<Option<Record>> {
    anyhow::ensure!(target.valid(), "invalid browser target");
    schema(connection)?;
    let key = identity(target)?;
    validated_targets(connection)?;
    let row=connection.query_row("SELECT CASE WHEN length(CAST(identity AS BLOB))<=1024 THEN identity END,CASE WHEN length(CAST(operation AS BLOB))<=128 THEN operation END,CASE WHEN length(CAST(target AS BLOB))<=8192 THEN target END,CASE WHEN typeof(payload)='blob' AND length(payload)<=?3 THEN payload END,CASE WHEN length(CAST(digest AS BLOB))=64 THEN digest END,CASE WHEN length(CAST(observation AS BLOB))<=9 THEN observation END,conflict,generation FROM browser_operations WHERE identity=?1 OR operation=?2",params![key,target.binding.operation,if crate::browser_control::reserved(target) {16384} else {1048576}],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<Vec<u8>>>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,i64>(6)?,r.get::<_,i64>(7)?))).optional()?;
    let Some(row) = row else { return Ok(None) };
    let (
        Some(stored_key),
        Some(op),
        Some(raw),
        Some(payload),
        Some(hash),
        Some(state),
        conflict,
        generation,
    ) = row
    else {
        anyhow::bail!("invalid browser record bounds")
    };
    anyhow::ensure!(
        raw.len() <= 8192 && !payload.is_empty() && payload.len() <= MAX_PAYLOAD,
        "invalid browser record bounds"
    );
    let stored: Target =
        serde_json::from_str(&raw).map_err(|_| anyhow::anyhow!("invalid browser record"))?;
    anyhow::ensure!(
        stored == *target
            && stored.valid()
            && stored_key == key
            && stored_key == identity(&stored)?
            && op == stored.binding.operation
            && hash == digest(&payload)
            && (conflict == 0 || conflict == 1)
            && generation >= 0,
        "invalid browser record binding"
    );
    let client = Client::new(stored.clone(), &payload)?;
    anyhow::ensure!(
        client.payload_digest() == hash,
        "invalid browser payload binding"
    );
    Ok(Some(Record {
        snapshot: HistoricalSnapshot {
            target: stored,
            payload_digest: hash,
            observation: observation(&state)?,
            conflict: conflict == 1,
            generation: generation as u64,
        },
        payload,
    }))
}
impl BrowserRegistry {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let name = path.to_string_lossy();
        anyhow::ensure!(
            !name.is_empty() && name != ":memory:" && !name.starts_with("file:"),
            "browser registry requires a file"
        );
        if path.exists() {
            let ro = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            if !empty(&ro)? {
                schema(&ro)?;
            }
        }
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        // Reject incompatible existing layouts before modifying journal pragmas.
        if !empty(&connection)? {
            schema(&connection)?;
        }
        connection.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;PRAGMA max_page_count=524288;")?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if empty(&tx)? {
            create_schema(&tx, SCHEMA)?;
            tx.pragma_update(None, "application_id", APPLICATION_ID)?;
            tx.pragma_update(None, "user_version", 1)?;
        }
        schema(&tx)?;
        tx.commit()?;
        Ok(Self { connection })
    }
    pub fn historical_snapshot(
        &self,
        target: &Target,
    ) -> anyhow::Result<Option<HistoricalSnapshot>> {
        anyhow::ensure!(
            !crate::browser_control::reserved(target),
            "control requires typed admission"
        );
        let tx = self.connection.unchecked_transaction()?;
        let result = read(&tx, target)?.map(|r| r.snapshot);
        tx.commit()?;
        Ok(result)
    }
    /// Application-owned entry point for a stateless Browser executor.
    /// The supplied operation is retained by the existing semantic identity map;
    /// neither a new operation nor changed bytes can retry an unknown intent.
    /// Reconciliation additionally binds the caller's current payload commitment
    /// before any peer call. This is not a new store or an authority grant.
    pub fn execute<A: Authority, P: Peer>(
        &mut self,
        target: Target,
        payload: &[u8],
        action: Action,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        anyhow::ensure!(
            !crate::browser_control::reserved(&target),
            "control requires typed admission"
        );
        let client = Client::new(target.clone(), payload)?;
        match action {
            Action::Dispatch => self.dispatch(target, payload, authority, peer),
            Action::Reconcile => {
                let retained = self
                    .historical_snapshot(&target)?
                    .ok_or_else(|| anyhow::anyhow!("missing browser reservation"))?;
                anyhow::ensure!(
                    retained.payload_digest == client.payload_digest(),
                    "browser payload conflict"
                );
                self.reconcile(&target, authority, peer)
            }
        }
    }
    pub fn dispatch<A: Authority, P: Peer>(
        &mut self,
        target: Target,
        payload: &[u8],
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        anyhow::ensure!(
            !crate::browser_control::reserved(&target),
            "control requires typed admission"
        );
        let client = Client::new(target.clone(), payload)?;
        anyhow::ensure!(
            authority.verify(&target, client.payload_digest(), Action::Dispatch),
            "browser dispatch denied"
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = read(&tx, &target)?;
        anyhow::ensure!(
            authority.verify(&target, client.payload_digest(), Action::Dispatch),
            "browser dispatch denied"
        );
        if let Some(old) = old {
            anyhow::ensure!(old.payload == payload, "browser payload conflict");
            tx.commit()?;
            return Ok(Outcome::Unknown);
        }
        let count = validated_targets(&tx)?
            .iter()
            .filter(|t| !crate::browser_control::reserved(t))
            .count() as u64;
        anyhow::ensure!(count < CAPACITY, "browser registry capacity");
        let raw = serde_json::to_string(&target)
            .map_err(|_| anyhow::anyhow!("invalid browser target"))?;
        anyhow::ensure!(raw.len() <= 8192, "browser target capacity");
        tx.execute(
            "INSERT INTO browser_operations VALUES(?1,?2,?3,?4,?5,'unknown',0,0)",
            params![
                identity(&target)?,
                target.binding.operation,
                raw,
                payload,
                client.payload_digest()
            ],
        )?;
        tx.commit()?;
        // Only this newly committed reservation may ever enter dispatch.
        let result = client.dispatch(authority, peer)?;
        self.remember(&target, payload, Action::Dispatch, result, authority)
    }
    pub fn reconcile<A: Authority, P: Peer>(
        &mut self,
        target: &Target,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        anyhow::ensure!(
            !crate::browser_control::reserved(target),
            "control requires typed admission"
        );
        let tx = self.connection.unchecked_transaction()?;
        let retained =
            read(&tx, target)?.ok_or_else(|| anyhow::anyhow!("missing browser reservation"))?;
        tx.commit()?;
        let client = Client::new(retained.snapshot.target, &retained.payload)?;
        let result = client.reconcile(authority, peer)?;
        self.remember(
            target,
            &retained.payload,
            Action::Reconcile,
            result,
            authority,
        )
    }
    fn remember<A: Authority>(
        &mut self,
        target: &Target,
        payload: &[u8],
        action: Action,
        result: Outcome,
        authority: &A,
    ) -> anyhow::Result<Outcome> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old =
            read(&tx, target)?.ok_or_else(|| anyhow::anyhow!("missing browser reservation"))?;
        anyhow::ensure!(old.payload == payload, "browser payload changed");
        anyhow::ensure!(
            authority.verify(target, &old.snapshot.payload_digest, action),
            "browser authority changed"
        );
        let previous = old.snapshot.observation;
        let conflict = old.snapshot.conflict
            || (previous != Outcome::Unknown && result != Outcome::Unknown && previous != result);
        let retained = if previous == Outcome::Unknown {
            result
        } else {
            previous
        };
        let generation = old
            .snapshot
            .generation
            .checked_add(1)
            .filter(|g| *g <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("browser generation exhausted"))?;
        tx.execute("UPDATE browser_operations SET observation=?1,conflict=?2,generation=?3 WHERE identity=?4",params![label(retained),conflict,generation,identity(target)?])?;
        anyhow::ensure!(
            authority.verify(target, &old.snapshot.payload_digest, action),
            "browser authority changed"
        );
        tx.commit()?;
        // Unknown input cannot promote historical confirmation into fresh proof.
        Ok(if conflict { Outcome::Unknown } else { result })
    }
}
