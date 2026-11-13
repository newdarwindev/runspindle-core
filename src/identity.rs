//! Request-delivery deduplication and explicit host-pin comparison.

use std::collections::BTreeMap;

/// An opaque reference, not a secret, URL, user message or source of authority.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidId;

impl Id {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidId> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
        {
            return Err(InvalidId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The exact 32-byte SHA-256 value supplied by a trusted hashing adapter.
/// This type does not compute a hash or verify that a value came from any bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A semantic intent has an explicit task ID. Revisions are distinct requests.
/// This slice neither infers intent from text nor implements revision supersession.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IntentKey {
    pub task_id: Id,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPin {
    pub host_id: Id,
    pub workspace_id: Id,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentSpec {
    pub key: IntentKey,
    pub semantic_digest: Sha256Digest,
    pub policy_revision: Id,
    pub host_pin: HostPin,
}

/// Scope separates upstream namespaces, such as different synthetic channels.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestDeliveryKey {
    pub scope: Id,
    pub event_id: Id,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    NewIntent,
    NewRevision,
    ExistingRevision,
    DuplicateDelivery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    DeliveryConflict,
    IntentConflict,
}

/// A process-local ledger. An admission result grants no execution authority.
#[derive(Debug, Default)]
pub struct AdmissionLedger {
    deliveries: BTreeMap<RequestDeliveryKey, IntentSpec>,
    intents: BTreeMap<IntentKey, IntentSpec>,
}

impl AdmissionLedger {
    pub fn admit(
        &mut self,
        delivery: RequestDeliveryKey,
        intent: IntentSpec,
    ) -> Result<Admission, AdmissionError> {
        if let Some(existing) = self.deliveries.get(&delivery) {
            return if existing == &intent {
                Ok(Admission::DuplicateDelivery)
            } else {
                Err(AdmissionError::DeliveryConflict)
            };
        }
        if let Some(existing) = self.intents.get(&intent.key) {
            if existing != &intent {
                return Err(AdmissionError::IntentConflict);
            }
            self.deliveries.insert(delivery, intent);
            return Ok(Admission::ExistingRevision);
        }
        let known_intent = self
            .intents
            .keys()
            .any(|key| key.task_id == intent.key.task_id);
        self.intents.insert(intent.key.clone(), intent.clone());
        self.deliveries.insert(delivery, intent);
        Ok(if known_intent {
            Admission::NewRevision
        } else {
            Admission::NewIntent
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    Online,
    Offline,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostObservation {
    pub host: HostPin,
    pub availability: Availability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostDecision {
    Eligible,
    QueueForPinnedHost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostError {
    HostMismatch,
    WorkspaceMismatch,
    AvailabilityUnknown,
}

/// Compares supplied references only. Eligibility does not launch or authorize work.
pub fn check_host(pin: &HostPin, observation: &HostObservation) -> Result<HostDecision, HostError> {
    if pin.host_id != observation.host.host_id {
        return Err(HostError::HostMismatch);
    }
    if pin.workspace_id != observation.host.workspace_id {
        return Err(HostError::WorkspaceMismatch);
    }
    match observation.availability {
        Availability::Online => Ok(HostDecision::Eligible),
        Availability::Offline => Ok(HostDecision::QueueForPinnedHost),
        Availability::Unknown => Err(HostError::AvailabilityUnknown),
    }
}
