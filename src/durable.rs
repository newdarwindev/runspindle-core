//! Core-owned immutable byte store and explicit authorized acceptance.
use crate::runtime::{valid_id, Binding};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};
pub const VERSION: &str = "runspindle.integration.v1";
pub const CAPACITY: u64 = 1024;
pub const MAX_BYTES: usize = 1_048_576;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub binding: Binding,
    pub artifact_id: String,
    pub artifact_revision: u64,
    pub sha256: String,
}
impl ArtifactRef {
    pub fn valid(&self) -> bool {
        self.binding.valid()
            && valid_id(&self.artifact_id)
            && self.artifact_revision > 0
            && self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }
    fn key(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&(
            &self.binding,
            &self.artifact_id,
            self.artifact_revision,
        ))?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Validation {
    pub artifact: ArtifactRef,
    pub validator_revision: String,
    pub policy_revision: String,
    pub passed: bool,
}
impl Validation {
    pub fn valid(&self) -> bool {
        self.artifact.valid()
            && valid_id(&self.validator_revision)
            && self.policy_revision == self.artifact.binding.policy_revision
    }
    fn key(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&(
            &self.artifact,
            &self.validator_revision,
            &self.policy_revision,
        ))?)
    }
}
pub trait Validator {
    fn revision(&self) -> &str;
    fn validate(&self, artifact: &ArtifactRef, bytes: &[u8], policy_revision: &str) -> bool;
}
pub trait AcceptanceAuthority {
    fn verify(&self, validation: &Validation) -> bool;
}
pub struct Deny;
impl AcceptanceAuthority for Deny {
    fn verify(&self, _: &Validation) -> bool {
        false
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Accepted {
    pub validation: Validation,
}
impl Accepted {
    pub fn artifact(&self) -> &ArtifactRef {
        &self.validation.artifact
    }
    fn key(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&(
            &self.artifact().binding.task_id,
            self.artifact().binding.revision,
        ))?)
    }
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub struct Store {
    pub(crate) connection: Connection,
}
pub(crate) fn ensure_version(version: &str) -> anyhow::Result<()> {
    anyhow::ensure!(version == VERSION, "unsupported integration version");
    Ok(())
}
pub(crate) fn room(tx: &Transaction<'_>, table: &str) -> anyhow::Result<()> {
    let sql = match table {
        "artifacts" => "SELECT count(*) FROM artifacts",
        "validations" => "SELECT count(*) FROM validations",
        "acceptances" => "SELECT count(*) FROM acceptances",
        "outbox" => "SELECT count(*) FROM outbox",
        _ => anyhow::bail!("unsupported bounded table"),
    };
    let count: u64 = tx.query_row(sql, [], |r| r.get(0))?;
    anyhow::ensure!(count < CAPACITY, "store capacity");
    Ok(())
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let version: u64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
        anyhow::ensure!(
            version == 0 || version == 1,
            "unsupported core store schema"
        );
        connection.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;PRAGMA max_page_count=524288;")?;
        if version == 0 {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current: u64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
            anyhow::ensure!(
                current == 0 || current == 1,
                "unsupported concurrent core schema"
            );
            if current == 0 {
                let existing:u64=tx.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",[],|r|r.get(0))?;
                anyhow::ensure!(
                    existing == 0,
                    "unversioned nonempty database is not a core store"
                );
                tx.execute_batch("CREATE TABLE IF NOT EXISTS artifacts(identity TEXT PRIMARY KEY,reference TEXT NOT NULL,bytes BLOB NOT NULL CHECK(length(bytes)<=1048576));
        CREATE TABLE IF NOT EXISTS validations(identity TEXT PRIMARY KEY,record TEXT NOT NULL,artifact_identity TEXT NOT NULL REFERENCES artifacts(identity));
        CREATE TABLE IF NOT EXISTS acceptances(identity TEXT PRIMARY KEY,record TEXT NOT NULL,artifact_identity TEXT NOT NULL REFERENCES artifacts(identity));
        CREATE TRIGGER IF NOT EXISTS artifacts_immutable BEFORE UPDATE ON artifacts BEGIN SELECT RAISE(ABORT,'immutable artifact');END;
        CREATE TRIGGER IF NOT EXISTS artifacts_retained BEFORE DELETE ON artifacts BEGIN SELECT RAISE(ABORT,'retained artifact');END;
        CREATE TRIGGER IF NOT EXISTS validations_immutable BEFORE UPDATE ON validations BEGIN SELECT RAISE(ABORT,'immutable validation');END;
        CREATE TRIGGER IF NOT EXISTS validations_retained BEFORE DELETE ON validations BEGIN SELECT RAISE(ABORT,'retained validation');END;
        CREATE TRIGGER IF NOT EXISTS acceptances_immutable BEFORE UPDATE ON acceptances BEGIN SELECT RAISE(ABORT,'immutable acceptance');END;
        CREATE TRIGGER IF NOT EXISTS acceptances_retained BEFORE DELETE ON acceptances BEGIN SELECT RAISE(ABORT,'retained acceptance');END;
        PRAGMA user_version=1;")?;
            }
            tx.commit()?;
        }
        Ok(Self { connection })
    }
    pub fn put(
        &mut self,
        version: &str,
        binding: Binding,
        artifact_id: String,
        artifact_revision: u64,
        bytes: &[u8],
    ) -> anyhow::Result<ArtifactRef> {
        ensure_version(version)?;
        anyhow::ensure!(bytes.len() <= MAX_BYTES, "artifact byte capacity");
        let reference = ArtifactRef {
            binding,
            artifact_id,
            artifact_revision,
            sha256: digest(bytes),
        };
        anyhow::ensure!(reference.valid(), "invalid artifact identity");
        let key = reference.key()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<(String, Vec<u8>)> = tx
            .query_row(
                "SELECT reference,bytes FROM artifacts WHERE identity=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((old, data)) = old {
            anyhow::ensure!(
                serde_json::from_str::<ArtifactRef>(&old)? == reference && data == bytes,
                "immutable artifact conflict"
            );
            return Ok(reference);
        }
        room(&tx, "artifacts")?;
        tx.execute(
            "INSERT INTO artifacts VALUES(?1,?2,?3)",
            params![key, serde_json::to_string(&reference)?, bytes],
        )?;
        tx.commit()?;
        Ok(reference)
    }
    pub fn bytes(&self, reference: &ArtifactRef) -> anyhow::Result<Vec<u8>> {
        Self::read_bytes(&self.connection, reference)
    }
    pub(crate) fn read_bytes(
        connection: &Connection,
        reference: &ArtifactRef,
    ) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(reference.valid(), "invalid artifact reference");
        let (stored, bytes): (String, Vec<u8>) = connection.query_row(
            "SELECT reference,bytes FROM artifacts WHERE identity=?1",
            [reference.key()?],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<ArtifactRef>(&stored)? == *reference
                && bytes.len() <= MAX_BYTES
                && digest(&bytes) == reference.sha256,
            "artifact bytes/reference mismatch"
        );
        Ok(bytes)
    }
    pub fn validate<V: Validator>(
        &mut self,
        version: &str,
        artifact: &ArtifactRef,
        policy_revision: &str,
        validator: &V,
    ) -> anyhow::Result<Validation> {
        ensure_version(version)?;
        let validator_revision = validator.revision().to_owned();
        anyhow::ensure!(
            artifact.valid()
                && valid_id(&validator_revision)
                && policy_revision == artifact.binding.policy_revision,
            "invalid validation binding"
        );
        let bytes = self.bytes(artifact)?;
        let validation = Validation {
            artifact: artifact.clone(),
            validator_revision,
            policy_revision: policy_revision.into(),
            passed: validator.validate(artifact, &bytes, policy_revision),
        };
        anyhow::ensure!(validation.valid(), "invalid validation binding");
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::read_bytes(&tx, artifact)?;
        let old: Option<String> = tx
            .query_row(
                "SELECT record FROM validations WHERE identity=?1",
                [validation.key()?],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            anyhow::ensure!(
                serde_json::from_str::<Validation>(&old)? == validation,
                "immutable validation conflict"
            );
            return Ok(validation);
        }
        room(&tx, "validations")?;
        tx.execute(
            "INSERT INTO validations VALUES(?1,?2,?3)",
            params![
                validation.key()?,
                serde_json::to_string(&validation)?,
                artifact.key()?
            ],
        )?;
        tx.commit()?;
        Ok(validation)
    }
    pub fn accept<A: AcceptanceAuthority>(
        &mut self,
        version: &str,
        validation: &Validation,
        authority: &A,
    ) -> anyhow::Result<Accepted> {
        ensure_version(version)?;
        anyhow::ensure!(
            validation.valid() && validation.passed,
            "validation must pass"
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::read_bytes(&tx, &validation.artifact)?;
        let stored: String = tx.query_row(
            "SELECT record FROM validations WHERE identity=?1",
            [validation.key()?],
            |r| r.get(0),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<Validation>(&stored)? == *validation,
            "exact validation missing"
        );
        anyhow::ensure!(authority.verify(validation), "acceptance denied");
        let accepted = Accepted {
            validation: validation.clone(),
        };
        let key = accepted.key()?;
        let old: Option<String> = tx
            .query_row(
                "SELECT record FROM acceptances WHERE identity=?1",
                [&key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            anyhow::ensure!(
                serde_json::from_str::<Accepted>(&old)? == accepted,
                "accepted revision conflict"
            );
            return Ok(accepted);
        }
        room(&tx, "acceptances")?;
        tx.execute(
            "INSERT INTO acceptances VALUES(?1,?2,?3)",
            params![
                key,
                serde_json::to_string(&accepted)?,
                accepted.artifact().key()?
            ],
        )?;
        tx.commit()?;
        Ok(accepted)
    }
    pub fn accepted(&self, accepted: &Accepted) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            accepted.validation.valid() && accepted.validation.passed,
            "invalid acceptance"
        );
        let stored: String = self.connection.query_row(
            "SELECT record FROM acceptances WHERE identity=?1",
            [accepted.key()?],
            |r| r.get(0),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<Accepted>(&stored)? == *accepted,
            "accepted reference conflict"
        );
        self.bytes(accepted.artifact())
    }
}
