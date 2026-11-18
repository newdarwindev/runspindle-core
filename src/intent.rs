//! Version-isolated semantic admission. A deletion is never executable work.
use crate::{
    envelope::{self, Envelope},
    runtime::{valid_id, Admission},
    scoped_authority::ScopedAdmission,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const VERSION: &str = "runspindle.intent-admission.v1";
pub const ASSERTION_VERSION: &str = "runspindle.authority.v3";
pub const DOMAIN: &[u8] = b"runspindle.authority.v3\0ed25519\0";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub app_id: String,
    pub account_id: String,
    pub actor_id: String,
    pub workspace_id: String,
    pub channel_id: String,
    pub thread_id: String,
    pub message_id: String,
}
impl Source {
    pub fn task_id(&self) -> anyhow::Result<String> {
        Ok(format!(
            "tether-task:{}",
            digest(&envelope::canonical(&[
                &self.app_id,
                &self.account_id,
                &self.workspace_id,
                &self.channel_id,
                &self.thread_id,
                &self.message_id,
            ])?)
        ))
    }
    pub fn destination_id(&self) -> anyhow::Result<String> {
        Ok(format!(
            "tether:{}",
            digest(&envelope::canonical(&[
                &self.app_id,
                &self.account_id,
                &self.workspace_id,
                &self.channel_id,
                &self.thread_id,
            ])?)
        ))
    }
    fn valid(&self) -> bool {
        [
            &self.app_id,
            &self.account_id,
            &self.actor_id,
            &self.workspace_id,
            &self.channel_id,
            &self.thread_id,
            &self.message_id,
        ]
        .iter()
        .all(|s| valid_id(s))
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Create,
    Edit,
    Delete,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Prior {
    pub request: Envelope,
    pub wire_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub event_id: String,
    pub source: Source,
    pub revision: u64,
    pub kind: Kind,
    pub content_sha256: String,
    pub previous: Option<Prior>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IntentAdmission {
    pub version: String,
    pub scoped: ScopedAdmission,
    pub event: Event,
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl IntentAdmission {
    pub fn admission(&self) -> anyhow::Result<Admission> {
        let a = self.scoped.admission()?;
        let e = &self.event;
        anyhow::ensure!(
            a.binding.task_id == e.source.task_id()?,
            "deterministic source task identity"
        );
        anyhow::ensure!(
            self.version == VERSION && e.source.valid() && valid_id(&e.event_id),
            "intent version or source"
        );
        anyhow::ensure!(
            e.revision == a.binding.revision
                && e.event_id == self.scoped.request.message_id
                && e.event_id == a.delivery_id,
            "intent event identity"
        );
        anyhow::ensure!(
            e.source.workspace_id == a.binding.workspace_id
                && e.source.account_id == self.scoped.destination.account_id
                && e.source.destination_id()? == self.scoped.destination.destination_id,
            "intent destination"
        );
        anyhow::ensure!(
            is_digest(&e.content_sha256) && a.normalized_digest == digest(&envelope::canonical(e)?),
            "intent semantic digest"
        );
        match (&e.kind, &e.previous) {
            (Kind::Create, None) => anyhow::ensure!(e.revision == 1, "create revision"),
            (Kind::Edit | Kind::Delete, Some(prior)) => {
                let crate::envelope::Message::Admission(old) = prior.request.message()? else {
                    anyhow::bail!("prior admission required")
                };
                anyhow::ensure!(
                    is_digest(&prior.wire_sha256)
                        && old.binding.revision.checked_add(1) == Some(e.revision)
                        && old.binding.task_id == a.binding.task_id
                        && old.binding.operation_id != a.binding.operation_id
                        && old.binding.workspace_id == a.binding.workspace_id
                        && old.binding.host_id == a.binding.host_id
                        && old.binding.lease_epoch == a.binding.lease_epoch,
                    "prior intent binding"
                );
                anyhow::ensure!(
                    a.predecessor_revision == Some(old.binding.revision),
                    "intent predecessor"
                );
            }
            _ => anyhow::bail!("intent revision kind"),
        }
        anyhow::ensure!(
            e.kind != Kind::Delete || e.content_sha256 == digest(b""),
            "delete content"
        );
        Ok(a)
    }
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        self.admission()?;
        envelope::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let value: Self = envelope::typed_json(envelope::strict_json(bytes)?)?;
        anyhow::ensure!(value.encode()? == bytes, "noncanonical intent");
        Ok(value)
    }
}

pub const BATCH_VERSION: &str = "runspindle.intent-batch.v1";
pub const EXECUTABLE_CAPACITY: usize = 64;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IntentBatch {
    pub version: String,
    pub intents: Vec<IntentAdmission>,
}
impl IntentBatch {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.version == BATCH_VERSION
                && !self.intents.is_empty()
                && self.intents.len() <= EXECUTABLE_CAPACITY + 1,
            "intent batch bounds"
        );
        let first = &self.intents[0];
        let binding = first.admission()?.binding;
        anyhow::ensure!(
            first.event.kind == Kind::Create && first.event.revision == 1,
            "intent batch root"
        );
        let mut events = std::collections::BTreeSet::new();
        let mut operations = std::collections::BTreeSet::new();
        for (index, value) in self.intents.iter().enumerate() {
            let a = value.admission()?;
            anyhow::ensure!(
                value.event.source == first.event.source
                    && a.binding.task_id == binding.task_id
                    && a.binding.host_id == binding.host_id
                    && a.binding.workspace_id == binding.workspace_id
                    && a.binding.lease_epoch == binding.lease_epoch,
                "intent batch scope"
            );
            anyhow::ensure!(
                events.insert(&value.event.event_id)
                    && operations.insert(a.binding.operation_id.clone()),
                "intent batch duplicate identity"
            );
            anyhow::ensure!(
                value.event.kind != Kind::Delete || index + 1 == self.intents.len(),
                "intent batch resurrection"
            );
            if index > 0 {
                let previous = &self.intents[index - 1];
                let p = value
                    .event
                    .previous
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("batch predecessor"))?;
                anyhow::ensure!(
                    p.request == previous.scoped.request
                        && p.wire_sha256 == digest(&previous.encode()?),
                    "intent batch chain"
                );
            }
        }
        anyhow::ensure!(
            self.intents.len() <= EXECUTABLE_CAPACITY
                || self.intents.last().expect("nonempty").event.kind == Kind::Delete,
            "intent batch terminal reserve"
        );
        Ok(())
    }
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        self.validate()?;
        let bytes = envelope::canonical(self)?;
        anyhow::ensure!(bytes.len() <= envelope::MAX_BYTES, "intent batch size");
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let value: Self = envelope::typed_json(envelope::strict_json(bytes)?)?;
        anyhow::ensure!(value.encode()? == bytes, "noncanonical intent batch");
        Ok(value)
    }
}
