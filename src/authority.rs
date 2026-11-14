//! Offline assertion verification. Local helper configuration is trusted worker
//! input; requests cannot choose trust roots, actor, audience or grant contents.
use crate::{
    envelope::{self, Envelope, Message},
    runtime::{valid_id, Admission, Binding},
};
use ring::signature::{UnparsedPublicKey, ED25519};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const VERSION: &str = "runspindle.authority.v1";
pub const DOMAIN: &[u8] = b"runspindle.authority.v1\0ed25519\0";
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Admit,
    Dispatch,
    Reconcile,
    Cancel,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assertion {
    pub version: String,
    pub issuer: String,
    pub key_id: String,
    pub actor: String,
    pub audience: String,
    pub grant_id: String,
    pub request_digest: String,
    pub actions: Vec<Action>,
    pub policy_revision: String,
    pub target: Binding,
    pub not_before_ms: u64,
    pub expires_at_ms: u64,
}
impl Assertion {
    pub fn signing_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut bytes = DOMAIN.to_vec();
        bytes.extend(envelope::canonical(self)?);
        Ok(bytes)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalHelper {
    pub database: PathBuf,
    pub issuer: String,
    pub actor: String,
    pub audience: String,
    pub allowed_actions: Vec<Action>,
}
pub trait Authority {
    /// Verify fresh authoritative state on every invocation. No positive cache.
    fn verify(&self, wire: &[u8], action: Action) -> bool;
}
pub struct Deny;
impl Authority for Deny {
    fn verify(&self, _: &[u8], _: Action) -> bool {
        false
    }
}
pub fn now_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
pub fn request(wire: &[u8]) -> anyhow::Result<Admission> {
    let e = Envelope::decode(wire)?;
    anyhow::ensure!(e.encode()? == wire, "noncanonical authenticated envelope");
    match e.message()? {
        Message::Admission(a) => Ok(a),
        _ => anyhow::bail!("authority requires admission envelope"),
    }
}
impl Authority for LocalHelper {
    fn verify(&self, wire: &[u8], action: Action) -> bool {
        self.check(wire, action).is_ok()
    }
}
impl LocalHelper {
    pub fn read(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(1_048_577)
            .read_to_end(&mut bytes)?;
        Ok(serde_json::from_value(envelope::strict_json(&bytes)?)?)
    }
    fn check(&self, wire: &[u8], action: Action) -> anyhow::Result<()> {
        let admission = request(wire)?;
        anyhow::ensure!(
            [&self.issuer, &self.actor, &self.audience]
                .iter()
                .all(|v| valid_id(v))
                && self.database.is_absolute()
                && self.allowed_actions.contains(&action),
            "invalid helper scope"
        );
        // READ_ONLY never creates an empty database and never falls back to scripts.
        let mut connection = Connection::open_with_flags(
            &self.database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(1))?;
        let tx = connection.transaction()?;
        let schema: u64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
        anyhow::ensure!(schema == 1, "unknown authority schema");
        // A schema version alone does not establish that uniqueness constraints
        // survived store preparation. Ambiguous local state must fail closed.
        let state_rows: u64 = tx.query_row(
            "SELECT count(*) FROM authority_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        anyhow::ensure!(state_rows == 1, "ambiguous authority state");
        let grant_rows: u64 = tx.query_row(
            "SELECT count(*) FROM grants WHERE grant_id=?1",
            [&admission.grant_ref],
            |r| r.get(0),
        )?;
        anyhow::ensure!(grant_rows == 1, "ambiguous grant identity");
        let (issued, until): (u64, u64) = tx.query_row(
            "SELECT issued_at_ms,valid_until_ms FROM authority_state WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let (assertion, signature): (String, Vec<u8>) = tx.query_row(
            "SELECT assertion,signature FROM grants WHERE grant_id=?1",
            [&admission.grant_ref],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let a: Assertion = serde_json::from_value(envelope::strict_json(assertion.as_bytes())?)?;
        anyhow::ensure!(
            a.version == VERSION
                && a.issuer == self.issuer
                && a.actor == self.actor
                && a.audience == self.audience
                && valid_id(&a.key_id)
                && a.grant_id == admission.grant_ref
                && a.target == admission.binding
                && a.policy_revision == admission.binding.policy_revision
                && a.request_digest == format!("{:x}", Sha256::digest(wire)),
            "assertion binding"
        );
        anyhow::ensure!(
            !a.actions.is_empty()
                && a.actions.windows(2).all(|w| w[0] < w[1])
                && a.actions.iter().all(|v| self.allowed_actions.contains(v))
                && a.actions.contains(&action),
            "assertion actions"
        );
        let key_rows: u64 = tx.query_row(
            "SELECT count(*) FROM trust_roots WHERE issuer=?1 AND key_id=?2",
            [&a.issuer, &a.key_id],
            |r| r.get(0),
        )?;
        anyhow::ensure!(key_rows == 1, "ambiguous trust root identity");
        let (key,key_from,key_until,key_revoked):(Vec<u8>,u64,u64,bool)=tx.query_row("SELECT public_key,not_before_ms,expires_at_ms,revoked FROM trust_roots WHERE issuer=?1 AND key_id=?2",[&a.issuer,&a.key_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        let revoked: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM revocations WHERE grant_id=?1)",
            [&a.grant_id],
            |r| r.get(0),
        )?;
        anyhow::ensure!(
            !revoked && !key_revoked && key.len() == 32 && signature.len() == 64,
            "revoked or malformed key/signature"
        );
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&a.signing_bytes()?, &signature)
            .map_err(|_| anyhow::anyhow!("invalid signature"))?;
        // Time is sampled after store reads and cryptographic work, before returning.
        let now = now_ms()?;
        anyhow::ensure!(
            issued <= now
                && now < until
                && a.not_before_ms < a.expires_at_ms
                && a.not_before_ms <= now
                && now < a.expires_at_ms
                && key_from <= now
                && now < key_until,
            "stale authoritative state or grant"
        );
        Ok(())
    }
}

/// Store bootstrap is restricted to the explicitly invoked synthetic issuer.
/// The production verifier never calls this and never accepts a request path.
pub fn initialize_synthetic_store(connection: &Connection) -> anyhow::Result<()> {
    let version: u64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    anyhow::ensure!(version == 0, "store already initialized");
    let tables: u64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table'",
        [],
        |r| r.get(0),
    )?;
    anyhow::ensure!(tables == 0, "nonempty unversioned store");
    connection.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
    BEGIN IMMEDIATE;
    CREATE TABLE authority_state(id INTEGER PRIMARY KEY CHECK(id=1),issued_at_ms INTEGER NOT NULL,valid_until_ms INTEGER NOT NULL);
    CREATE TABLE trust_roots(issuer TEXT NOT NULL,key_id TEXT NOT NULL,public_key BLOB NOT NULL,not_before_ms INTEGER NOT NULL,expires_at_ms INTEGER NOT NULL,revoked INTEGER NOT NULL CHECK(revoked IN(0,1)),PRIMARY KEY(issuer,key_id));
    CREATE TABLE grants(grant_id TEXT PRIMARY KEY,assertion TEXT NOT NULL,signature BLOB NOT NULL);
    CREATE TABLE revocations(grant_id TEXT PRIMARY KEY);
    CREATE TRIGGER grants_immutable BEFORE UPDATE ON grants BEGIN SELECT RAISE(ABORT,'immutable grant');END;
    CREATE TRIGGER grants_retained BEFORE DELETE ON grants BEGIN SELECT RAISE(ABORT,'retained grant');END;
    CREATE TRIGGER revocations_retained BEFORE DELETE ON revocations BEGIN SELECT RAISE(ABORT,'retained revocation');END;
    CREATE TRIGGER revocations_immutable BEFORE UPDATE ON revocations BEGIN SELECT RAISE(ABORT,'immutable revocation');END;
    PRAGMA user_version=1;COMMIT;")?;
    Ok(())
}
