//! Opt-in authenticated artifact acceptance. No issuer, native adapter or delivery
//! grant is installed. Existing trusted-callback artifact APIs remain separate.
use crate::durable::{self, Accepted, ArtifactRef, Store, Validation};
use crate::runtime::valid_id;
use ring::signature::{UnparsedPublicKey, ED25519};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const VERSION: &str = "runspindle.artifact-evidence.v1";
pub const PRODUCER_DOMAIN: &[u8] = b"runspindle.artifact-evidence.v1\0producer\0ed25519\0";
pub const VALIDATOR_DOMAIN: &[u8] = b"runspindle.artifact-evidence.v1\0validator\0ed25519\0";
pub const MAX_EVIDENCE_BYTES: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Producer {
    pub principal: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Validator {
    pub principal: String,
    pub revision: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub artifact: ArtifactRef,
    pub byte_length: u64,
    pub producer: Producer,
    pub validator: Validator,
    pub authority_epoch: u64,
}
impl Scope {
    fn valid(&self) -> bool {
        self.artifact.valid()
            && self.byte_length <= durable::MAX_BYTES as u64
            && self.authority_epoch > 0
            && [
                &self.producer.principal,
                &self.producer.thread_id,
                &self.producer.turn_id,
                &self.producer.item_id,
                &self.validator.principal,
                &self.validator.revision,
            ]
            .iter()
            .all(|value| valid_id(value))
    }
    fn validation(&self) -> Validation {
        Validation {
            artifact: self.artifact.clone(),
            validator_revision: self.validator.revision.clone(),
            policy_revision: self.artifact.binding.policy_revision.clone(),
            passed: true,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signer {
    pub issuer: String,
    pub key_id: String,
    pub key_epoch: u64,
    pub audience: String,
    pub not_before_ms: u64,
    pub expires_at_ms: u64,
}
impl Signer {
    fn valid(&self) -> bool {
        self.key_epoch > 0
            && self.not_before_ms < self.expires_at_ms
            && [&self.issuer, &self.key_id, &self.audience]
                .iter()
                .all(|s| valid_id(s))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerStatement {
    pub version: String,
    pub signer: Signer,
    pub scope: Scope,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorStatement {
    pub version: String,
    pub signer: Signer,
    pub scope: Scope,
    pub producer_sha256: String,
    pub passed: bool,
}
fn canonical_value(value: &serde_json::Value, bytes: &mut Vec<u8>) -> anyhow::Result<()> {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            bytes.push(b'{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    bytes.push(b',');
                }
                bytes.extend(serde_json::to_vec(key)?);
                bytes.push(b':');
                canonical_value(value, bytes)?;
            }
            bytes.push(b'}');
        }
        serde_json::Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    bytes.push(b',');
                }
                canonical_value(value, bytes)?;
            }
            bytes.push(b']');
        }
        _ => bytes.extend(serde_json::to_vec(value)?),
    }
    Ok(())
}
fn canonical<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    // Sort every object explicitly, including when another dependency enables
    // serde_json/preserve_order through Cargo feature unification.
    let mut bytes = Vec::new();
    canonical_value(&serde_json::to_value(value)?, &mut bytes)?;
    Ok(bytes)
}
fn signing_bytes<T: Serialize>(domain: &[u8], value: &T) -> anyhow::Result<Vec<u8>> {
    let mut bytes = domain.to_vec();
    bytes.extend(canonical(value)?);
    Ok(bytes)
}
impl ProducerStatement {
    pub fn signing_bytes(&self) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            self.version == VERSION && self.signer.valid() && self.scope.valid(),
            "invalid producer statement"
        );
        signing_bytes(PRODUCER_DOMAIN, self)
    }
}
impl ValidatorStatement {
    pub fn signing_bytes(&self) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            self.version == VERSION
                && self.signer.valid()
                && self.scope.valid()
                && lowercase_hex(&self.producer_sha256, 64),
            "invalid validator statement"
        );
        signing_bytes(VALIDATOR_DOMAIN, self)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerAttestation {
    pub statement: ProducerStatement,
    pub signature: String,
}
impl ProducerAttestation {
    pub fn sha256(&self) -> anyhow::Result<String> {
        Ok(durable::digest(&canonical(self)?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorAttestation {
    pub statement: ValidatorStatement,
    pub signature: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub producer: ProducerAttestation,
    pub validator: ValidatorAttestation,
}
fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn signature(value: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(lowercase_hex(value, 128), "invalid attestation signature");
    Ok((0..128)
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).expect("validated hex"))
        .collect())
}
impl Evidence {
    fn validate(&self) -> anyhow::Result<()> {
        self.producer.statement.signing_bytes()?;
        self.validator.statement.signing_bytes()?;
        anyhow::ensure!(
            self.producer.statement.scope == self.validator.statement.scope
                && self.producer.statement.signer.audience
                    == self.validator.statement.signer.audience
                && self.validator.statement.passed
                && self.validator.statement.producer_sha256 == self.producer.sha256()?
                && lowercase_hex(&self.producer.signature, 128)
                && lowercase_hex(&self.validator.signature, 128),
            "artifact evidence binding mismatch"
        );
        Ok(())
    }
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        self.validate()?;
        let bytes = canonical(self)?;
        anyhow::ensure!(
            bytes.len() <= MAX_EVIDENCE_BYTES,
            "artifact evidence capacity"
        );
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= MAX_EVIDENCE_BYTES,
            "artifact evidence capacity"
        );
        // Typed deserialization rejects duplicate members and unknown fields at
        // every level; the canonical echo rejects numeric/encoding aliases.
        let evidence: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            evidence.encode()? == bytes,
            "noncanonical artifact evidence"
        );
        Ok(evidence)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Producer,
    Validator,
}
/// Trusted worker-owned values. These types cannot be decoded from attestations.
#[derive(Clone, Debug)]
pub struct TrustedKey {
    pub role: Role,
    pub issuer: String,
    pub key_id: String,
    pub key_epoch: u64,
    pub principal: String,
    pub public_key: [u8; 32],
    pub not_before_ms: u64,
    pub expires_at_ms: u64,
    pub revoked: bool,
}
#[derive(Clone, Debug)]
pub struct TrustedState {
    pub scope: Scope,
    pub audience: String,
    pub not_before_ms: u64,
    pub expires_at_ms: u64,
    pub producer: TrustedKey,
    pub validator: TrustedKey,
}
/// Fresh, bounded, non-reentrant trusted callback. None means unavailable/denied.
/// Requests cannot install keys, select a trust database or change this callback.
pub trait Trust {
    fn current(&self, scope: &Scope) -> Option<TrustedState>;
}
pub struct Deny;
impl Trust for Deny {
    fn current(&self, _: &Scope) -> Option<TrustedState> {
        None
    }
}
/// Trusted injected clock; unavailable or rolled-back clocks must return None.
pub trait Clock {
    fn now_ms(&self) -> Option<u64>;
}
fn within(now: u64, start: u64, end: u64) -> bool {
    start < end && start <= now && now < end
}
fn verify_key(
    key: &TrustedKey,
    signer: &Signer,
    role: Role,
    principal: &str,
    now: u64,
    bytes: &[u8],
    sig: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        key.role == role
            && !key.revoked
            && key.issuer == signer.issuer
            && key.key_id == signer.key_id
            && key.key_epoch == signer.key_epoch
            && key.principal == principal
            && key.key_epoch > 0
            && within(now, key.not_before_ms, key.expires_at_ms)
            && within(now, signer.not_before_ms, signer.expires_at_ms),
        "untrusted or stale artifact signer"
    );
    UnparsedPublicKey::new(&ED25519, key.public_key)
        .verify(bytes, &signature(sig)?)
        .map_err(|_| anyhow::anyhow!("invalid artifact attestation signature"))
}
fn verify(evidence: &Evidence, trust: &impl Trust, clock: &impl Clock) -> anyhow::Result<()> {
    evidence.validate()?;
    let producer = &evidence.producer.statement;
    let validator = &evidence.validator.statement;
    let state = trust
        .current(&producer.scope)
        .ok_or_else(|| anyhow::anyhow!("artifact authority unavailable"))?;
    let now = clock
        .now_ms()
        .ok_or_else(|| anyhow::anyhow!("artifact clock unavailable"))?;
    anyhow::ensure!(
        state.scope == producer.scope
            && state.audience == producer.signer.audience
            && within(now, state.not_before_ms, state.expires_at_ms),
        "artifact authority scope or epoch mismatch"
    );
    verify_key(
        &state.producer,
        &producer.signer,
        Role::Producer,
        &producer.scope.producer.principal,
        now,
        &producer.signing_bytes()?,
        &evidence.producer.signature,
    )?;
    verify_key(
        &state.validator,
        &validator.signer,
        Role::Validator,
        &validator.scope.validator.principal,
        now,
        &validator.signing_bytes()?,
        &evidence.validator.signature,
    )?;
    // Do not return an authority result that expired during cryptographic work.
    let after = clock
        .now_ms()
        .ok_or_else(|| anyhow::anyhow!("artifact clock unavailable"))?;
    anyhow::ensure!(
        after >= now
            && within(after, state.not_before_ms, state.expires_at_ms)
            && within(
                after,
                state.producer.not_before_ms,
                state.producer.expires_at_ms
            )
            && within(
                after,
                state.validator.not_before_ms,
                state.validator.expires_at_ms
            )
            && within(
                after,
                producer.signer.not_before_ms,
                producer.signer.expires_at_ms
            )
            && within(
                after,
                validator.signer.not_before_ms,
                validator.signer.expires_at_ms
            ),
        "artifact authority expired"
    );
    Ok(())
}

const SCHEMA: &[(&str, &str)] = &[
    ("authenticated_artifact_schema", "CREATE TABLE authenticated_artifact_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)"),
    ("authenticated_artifact_evidence", "CREATE TABLE authenticated_artifact_evidence(acceptance_identity TEXT PRIMARY KEY REFERENCES acceptances(identity),accepted_record TEXT NOT NULL,evidence BLOB NOT NULL CHECK(length(evidence)<=65536),evidence_sha256 TEXT NOT NULL)"),
    ("authenticated_artifact_evidence_immutable", "CREATE TRIGGER authenticated_artifact_evidence_immutable BEFORE UPDATE ON authenticated_artifact_evidence BEGIN SELECT RAISE(ABORT,'immutable artifact evidence');END"),
    ("authenticated_artifact_evidence_retained", "CREATE TRIGGER authenticated_artifact_evidence_retained BEFORE DELETE ON authenticated_artifact_evidence BEGIN SELECT RAISE(ABORT,'retained artifact evidence');END"),
];
fn schema_exists(connection: &Connection, name: &str) -> anyhow::Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
        [name],
        |r| r.get(0),
    )?)
}
/// Result is not Deserialize and does not implement any delivery authority.
#[derive(Clone, Debug)]
pub struct AuthenticatedAccepted {
    accepted: Accepted,
    evidence_sha256: String,
}
impl AuthenticatedAccepted {
    pub fn accepted(&self) -> &Accepted {
        &self.accepted
    }
    pub fn evidence_sha256(&self) -> &str {
        &self.evidence_sha256
    }
}
pub struct AuthenticatedArtifacts {
    store: Store,
}
impl AuthenticatedArtifacts {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut store = Store::open(path)?;
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !schema_exists(&tx, SCHEMA[0].0)? {
            for (name, _) in &SCHEMA[1..] {
                anyhow::ensure!(
                    !schema_exists(&tx, name)?,
                    "unversioned artifact evidence schema"
                );
            }
            for (_, ddl) in SCHEMA {
                tx.execute_batch(ddl)?;
            }
            tx.execute("INSERT INTO authenticated_artifact_schema VALUES(1,1)", [])?;
        }
        // Exact known DDL only; do not erase SQL token boundaries or repair loss.
        for (name, expected) in SCHEMA {
            let stored: String =
                tx.query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                    r.get(0)
                })?;
            anyhow::ensure!(stored == *expected, "incompatible artifact evidence schema");
        }
        let version: u64 = tx.query_row(
            "SELECT version FROM authenticated_artifact_schema WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        anyhow::ensure!(version == 1, "unsupported artifact evidence schema");
        tx.commit()?;
        Ok(Self { store })
    }
    pub fn accept(
        &mut self,
        evidence: &Evidence,
        trust: &impl Trust,
        clock: &impl Clock,
    ) -> anyhow::Result<AuthenticatedAccepted> {
        let wire = evidence.encode()?;
        verify(evidence, trust, clock)?;
        let scope = &evidence.producer.statement.scope;
        let accepted = Accepted {
            validation: scope.validation(),
        };
        let key = accepted.key()?;
        let evidence_sha256 = durable::digest(&wire);
        let tx = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        verify(evidence, trust, clock)?; // authority before protected byte read
        let bytes = Store::read_bytes(&tx, &scope.artifact)?;
        anyhow::ensure!(
            bytes.len() as u64 == scope.byte_length,
            "artifact byte length mismatch"
        );
        verify(evidence, trust, clock)?; // authority after SQLite lock and exact bytes
        let validation = &accepted.validation;
        let retained_validation: Option<String> = tx
            .query_row(
                "SELECT record FROM validations WHERE identity=?1",
                [validation.key()?],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(record) = &retained_validation {
            anyhow::ensure!(
                serde_json::from_str::<Validation>(record)? == *validation,
                "immutable validation conflict"
            );
        }
        let old: Option<String> = tx
            .query_row(
                "SELECT record FROM acceptances WHERE identity=?1",
                [&key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            anyhow::ensure!(retained_validation.is_some(), "retained validation missing");
            anyhow::ensure!(
                serde_json::from_str::<Accepted>(&old)? == accepted,
                "authenticated acceptance conflict"
            );
            let (record, prior, digest): (String, Vec<u8>, String) = tx.query_row(
                "SELECT accepted_record,evidence,evidence_sha256 FROM authenticated_artifact_evidence WHERE acceptance_identity=?1",
                [&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?; // Missing evidence is a legacy acceptance, never an upgrade.
            anyhow::ensure!(
                serde_json::from_str::<Accepted>(&record)? == accepted
                    && prior == wire
                    && digest == evidence_sha256,
                "authenticated evidence conflict"
            );
            verify(evidence, trust, clock)?; // fresh authority for duplicate return
            return Ok(AuthenticatedAccepted {
                accepted,
                evidence_sha256,
            });
        }
        let orphan: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM authenticated_artifact_evidence WHERE acceptance_identity=?1)", [&key], |r| r.get(0))?;
        anyhow::ensure!(!orphan, "orphaned artifact evidence");
        if retained_validation.is_none() {
            durable::room(&tx, "validations")?;
            tx.execute(
                "INSERT INTO validations VALUES(?1,?2,?3)",
                params![
                    validation.key()?,
                    serde_json::to_string(validation)?,
                    scope.artifact.key()?
                ],
            )?;
        }
        durable::room(&tx, "acceptances")?;
        let count: u64 = tx.query_row(
            "SELECT count(*) FROM authenticated_artifact_evidence",
            [],
            |r| r.get(0),
        )?;
        anyhow::ensure!(count < durable::CAPACITY, "artifact evidence capacity");
        tx.execute(
            "INSERT INTO acceptances VALUES(?1,?2,?3)",
            params![
                key,
                serde_json::to_string(&accepted)?,
                scope.artifact.key()?
            ],
        )?;
        tx.execute(
            "INSERT INTO authenticated_artifact_evidence VALUES(?1,?2,?3,?4)",
            params![
                key,
                serde_json::to_string(&accepted)?,
                wire,
                evidence_sha256
            ],
        )?;
        verify(evidence, trust, clock)?; // denial rolls back all three new rows
        tx.commit()?;
        Ok(AuthenticatedAccepted {
            accepted,
            evidence_sha256,
        })
    }
    pub fn read(
        &mut self,
        accepted: &Accepted,
        trust: &impl Trust,
        clock: &impl Clock,
    ) -> anyhow::Result<(AuthenticatedAccepted, Vec<u8>)> {
        let tx = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (record, wire, digest): (String, Vec<u8>, String) = tx.query_row(
            "SELECT accepted_record,evidence,evidence_sha256 FROM authenticated_artifact_evidence WHERE acceptance_identity=?1",
            [accepted.key()?], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<Accepted>(&record)? == *accepted
                && durable::digest(&wire) == digest,
            "retained artifact evidence mismatch"
        );
        let evidence = Evidence::decode(&wire)?;
        anyhow::ensure!(
            evidence.producer.statement.scope.validation() == accepted.validation,
            "retained validation mismatch"
        );
        let stored: String = tx.query_row(
            "SELECT record FROM acceptances WHERE identity=?1",
            [accepted.key()?],
            |r| r.get(0),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<Accepted>(&stored)? == *accepted,
            "retained acceptance mismatch"
        );
        let validation: String = tx.query_row(
            "SELECT record FROM validations WHERE identity=?1",
            [accepted.validation.key()?],
            |r| r.get(0),
        )?;
        anyhow::ensure!(
            serde_json::from_str::<Validation>(&validation)? == accepted.validation,
            "retained validation record mismatch"
        );
        verify(&evidence, trust, clock)?;
        let bytes = Store::read_bytes(&tx, accepted.artifact())?;
        anyhow::ensure!(
            bytes.len() as u64 == evidence.producer.statement.scope.byte_length,
            "artifact byte length mismatch"
        );
        verify(&evidence, trust, clock)?;
        tx.commit()?;
        Ok((
            AuthenticatedAccepted {
                accepted: accepted.clone(),
                evidence_sha256: digest,
            },
            bytes,
        ))
    }
}
