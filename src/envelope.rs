//! Explicit wire mappings. Existing component/history serializers remain separate.
use crate::{
    durable::{Accepted, ArtifactRef, Validation},
    integration::host_binding,
    outbox::{DeliveryReceipt, DeliveryRequest},
    runtime::{self, valid_id, Admission, Binding},
};
use runspindle_host::lifecycle::{Observation, Terminal};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use std::fmt;

pub const VERSION: &str = "runspindle.envelope.v1";
pub const MAX_BYTES: usize = 1_048_576;

/// Retain JSON's structure while refusing duplicate keys recursively, before any
/// typed deserializer can discard evidence or flatten repeated bindings.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("unique-key JSON")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Unique, M::Error> {
                let mut out = serde_json::Map::new();
                while let Some((key, Unique(value))) = map.next_entry::<String, Unique>()? {
                    if out.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(Unique(Value::Object(out)))
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Unique, S::Error> {
                let mut out = Vec::new();
                while let Some(Unique(value)) = seq.next_element()? {
                    out.push(value);
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Unique, E> {
                Ok(Unique(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Unique, E> {
                Ok(Unique(Value::String(v)))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Unique, E> {
                Ok(Unique(Value::Bool(v)))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Unique, E> {
                if v < 0 {
                    return Err(E::custom("negative integer"));
                }
                Ok(Unique(Value::from(v as u64)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Unique, E> {
                Ok(Unique(Value::from(v)))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Unique, E> {
                Err(E::custom("float/out-of-range integer"))
            }
        }
        d.deserialize_any(V)
    }
}
pub fn strict_json(bytes: &[u8]) -> anyhow::Result<Value> {
    anyhow::ensure!(bytes.len() <= MAX_BYTES, "oversized JSON");
    let Unique(value) = serde_json::from_slice(bytes)?;
    Ok(value)
}
pub fn canonical<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::to_value(value)?)?)
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionPayload {
    pub delivery_namespace: String,
    pub delivery_id: String,
    // Value makes an explicit null mandatory; Option would accept a missing key.
    pub predecessor_revision: Value,
    pub normalized_digest: String,
    pub grant_ref: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObservationPayload {
    pub native_session_id: String,
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub state: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPayload {
    pub artifact_id: String,
    pub artifact_revision: u64,
    pub sha256: String,
    pub validator_revision: String,
    pub account_id: String,
    pub destination_id: String,
    pub delivery_operation_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptPayload {
    pub request: DeliveryPayload,
    pub receipt_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub version: String,
    pub message_type: String,
    pub message_id: String,
    pub binding: Binding,
    pub payload: Value,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Admission(Admission),
    Observation(Observation),
    Delivery(DeliveryRequest),
    Receipt(DeliveryReceipt),
}
impl DeliveryPayload {
    fn mapped(&self, binding: &Binding) -> anyhow::Result<DeliveryRequest> {
        let request = DeliveryRequest {
            version: crate::durable::VERSION.into(),
            accepted: Accepted {
                validation: Validation {
                    artifact: ArtifactRef {
                        binding: binding.clone(),
                        artifact_id: self.artifact_id.clone(),
                        artifact_revision: self.artifact_revision,
                        sha256: self.sha256.clone(),
                    },
                    validator_revision: self.validator_revision.clone(),
                    policy_revision: binding.policy_revision.clone(),
                    passed: true,
                },
            },
            account_id: self.account_id.clone(),
            destination_id: self.destination_id.clone(),
            delivery_operation_id: self.delivery_operation_id.clone(),
        };
        anyhow::ensure!(request.valid(), "invalid delivery");
        Ok(request)
    }
    fn from_component(r: &DeliveryRequest) -> anyhow::Result<Self> {
        anyhow::ensure!(r.valid(), "unrepresentable delivery");
        let a = r.accepted.artifact();
        Ok(Self {
            artifact_id: a.artifact_id.clone(),
            artifact_revision: a.artifact_revision,
            sha256: a.sha256.clone(),
            validator_revision: r.accepted.validation.validator_revision.clone(),
            account_id: r.account_id.clone(),
            destination_id: r.destination_id.clone(),
            delivery_operation_id: r.delivery_operation_id.clone(),
        })
    }
}
impl Envelope {
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let e: Self = serde_json::from_value(strict_json(bytes)?)?;
        e.message()?;
        Ok(e)
    }
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        self.message()?;
        canonical(self)
    }
    pub fn message(&self) -> anyhow::Result<Message> {
        anyhow::ensure!(
            self.version == VERSION && valid_id(&self.message_id) && self.binding.valid(),
            "invalid envelope header"
        );
        Ok(match self.message_type.as_str() {
            "admission_request" => {
                let p: AdmissionPayload = serde_json::from_value(self.payload.clone())?;
                let predecessor_revision = if p.predecessor_revision.is_null() {
                    None
                } else {
                    Some(
                        p.predecessor_revision
                            .as_u64()
                            .ok_or_else(|| anyhow::anyhow!("invalid predecessor"))?,
                    )
                };
                let a = Admission {
                    version: runtime::VERSION.into(),
                    binding: self.binding.clone(),
                    delivery_namespace: p.delivery_namespace,
                    delivery_id: p.delivery_id,
                    predecessor_revision,
                    normalized_digest: p.normalized_digest,
                    grant_ref: p.grant_ref,
                };
                anyhow::ensure!(a.valid(), "invalid admission");
                Message::Admission(a)
            }
            "host_observation" => {
                let p: ObservationPayload = serde_json::from_value(self.payload.clone())?;
                anyhow::ensure!(
                    valid_id(&p.native_session_id) && p.sequence > 0,
                    "invalid observation"
                );
                let terminal = match p.state.as_str() {
                    "running" => None,
                    "completed" => Some(Terminal::Completed),
                    "failed" => Some(Terminal::Failed),
                    "cancelled" => Some(Terminal::Cancelled),
                    _ => anyhow::bail!("unknown observation state"),
                };
                Message::Observation(Observation {
                    binding: host_binding(&self.binding),
                    native_session_id: p.native_session_id,
                    sequence: p.sequence,
                    observed_at_ms: p.observed_at_ms,
                    terminal,
                })
            }
            "delivery_request" => Message::Delivery(
                serde_json::from_value::<DeliveryPayload>(self.payload.clone())?
                    .mapped(&self.binding)?,
            ),
            "delivery_receipt" => {
                let p: ReceiptPayload = serde_json::from_value(self.payload.clone())?;
                anyhow::ensure!(valid_id(&p.receipt_id), "invalid receipt");
                Message::Receipt(DeliveryReceipt {
                    request: p.request.mapped(&self.binding)?,
                    receipt_id: p.receipt_id,
                })
            }
            _ => anyhow::bail!("unsupported envelope type"),
        })
    }
    pub fn admission(message_id: String, a: &Admission) -> anyhow::Result<Self> {
        anyhow::ensure!(a.valid(), "invalid admission");
        Self::new(
            message_id,
            a.binding.clone(),
            "admission_request",
            AdmissionPayload {
                delivery_namespace: a.delivery_namespace.clone(),
                delivery_id: a.delivery_id.clone(),
                predecessor_revision: serde_json::to_value(a.predecessor_revision)?,
                normalized_digest: a.normalized_digest.clone(),
                grant_ref: a.grant_ref.clone(),
            },
        )
    }
    pub fn observation(message_id: String, o: &Observation) -> anyhow::Result<Self> {
        let b = &o.binding;
        let binding = Binding {
            task_id: b.task_id.clone(),
            revision: b.revision,
            operation_id: b.operation_id.clone(),
            policy_revision: b.policy_revision.clone(),
            host_id: b.host_id.clone(),
            workspace_id: b.workspace_id.clone(),
            lease_epoch: b.lease_epoch,
        };
        Self::new(
            message_id,
            binding,
            "host_observation",
            ObservationPayload {
                native_session_id: o.native_session_id.clone(),
                sequence: o.sequence,
                observed_at_ms: o.observed_at_ms,
                state: match o.terminal {
                    None => "running",
                    Some(Terminal::Completed) => "completed",
                    Some(Terminal::Failed) => "failed",
                    Some(Terminal::Cancelled) => "cancelled",
                }
                .into(),
            },
        )
    }
    pub fn delivery(message_id: String, r: &DeliveryRequest) -> anyhow::Result<Self> {
        Self::new(
            message_id,
            r.accepted.artifact().binding.clone(),
            "delivery_request",
            DeliveryPayload::from_component(r)?,
        )
    }
    pub fn receipt(message_id: String, r: &DeliveryReceipt) -> anyhow::Result<Self> {
        Self::new(
            message_id,
            r.request.accepted.artifact().binding.clone(),
            "delivery_receipt",
            ReceiptPayload {
                request: DeliveryPayload::from_component(&r.request)?,
                receipt_id: r.receipt_id.clone(),
            },
        )
    }
    fn new(
        message_id: String,
        binding: Binding,
        message_type: &str,
        payload: impl Serialize,
    ) -> anyhow::Result<Self> {
        let e = Self {
            version: VERSION.into(),
            message_type: message_type.into(),
            message_id,
            binding,
            payload: serde_json::to_value(payload)?,
        };
        e.message()?;
        Ok(e)
    }
    /// Trusted receiver context, never populated from the received payload itself.
    pub fn checked_message(&self, expected: &Expected<'_>) -> anyhow::Result<Message> {
        let message = self.message()?;
        anyhow::ensure!(&self.binding == expected.binding, "binding mismatch");
        match &message {
            Message::Observation(o) => anyhow::ensure!(
                Some(o.native_session_id.as_str()) == expected.session
                    && o.sequence > expected.last_sequence,
                "stale session/sequence"
            ),
            Message::Delivery(r) | Message::Receipt(DeliveryReceipt { request: r, .. }) => {
                anyhow::ensure!(
                    expected.destination
                        == Some((r.account_id.as_str(), r.destination_id.as_str())),
                    "destination mismatch"
                )
            }
            _ => (),
        }
        Ok(message)
    }
}
pub struct Expected<'a> {
    pub binding: &'a Binding,
    pub session: Option<&'a str>,
    pub last_sequence: u64,
    pub destination: Option<(&'a str, &'a str)>,
}
