//! Separately versioned exact-destination admission. V1 remains host-only.
use crate::{
    authority::{Action, LocalHelper},
    envelope::{self, Envelope, Message},
    outbox::{DeliveryAuthority, DeliveryRequest},
    runtime::{valid_id, Admission},
};
use serde::{Deserialize, Serialize};

pub const VERSION: &str = "runspindle.scoped-admission.v1";
pub const ASSERTION_VERSION: &str = "runspindle.authority.v2";
pub const DOMAIN: &[u8] = b"runspindle.authority.v2\0ed25519\0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    pub account_id: String,
    pub destination_id: String,
    pub delivery_operation_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScopedAdmission {
    pub version: String,
    pub request: Envelope,
    pub destination: Destination,
}
impl ScopedAdmission {
    pub fn admission(&self) -> anyhow::Result<Admission> {
        anyhow::ensure!(self.version == VERSION, "unknown scoped admission version");
        let Message::Admission(a) = self.request.message()? else {
            anyhow::bail!("admission required")
        };
        anyhow::ensure!(
            [
                &self.destination.account_id,
                &self.destination.destination_id,
                &self.destination.delivery_operation_id
            ]
            .iter()
            .all(|s| valid_id(s))
                && self.destination.delivery_operation_id != a.binding.operation_id,
            "invalid exact destination"
        );
        Ok(a)
    }
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        self.admission()?;
        envelope::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let value: Self = envelope::typed_json(envelope::strict_json(bytes)?)?;
        anyhow::ensure!(value.encode()? == bytes, "noncanonical scoped admission");
        Ok(value)
    }
}
impl LocalHelper {
    pub fn verify_scoped(&self, wire: &[u8], action: Action) -> bool {
        self.check_bound(wire, action, true).is_ok()
    }
}
/// Outbox checks this on reservation, immediately before send and at readback.
/// A task grant cannot choose another delivery operation or destination. This
/// grants neither artifact acceptance nor quarantined receipt resolution.
pub struct ScopedDeliveryAuthority<'a> {
    pub helper: &'a LocalHelper,
    pub wire: &'a [u8],
}
impl DeliveryAuthority for ScopedDeliveryAuthority<'_> {
    fn verify(&self, request: &DeliveryRequest) -> bool {
        let Ok(scoped) = ScopedAdmission::decode(self.wire) else {
            return false;
        };
        let Ok(admission) = scoped.admission() else {
            return false;
        };
        request.valid()
            && request.accepted.artifact().binding == admission.binding
            && request.account_id == scoped.destination.account_id
            && request.destination_id == scoped.destination.destination_id
            && request.delivery_operation_id == scoped.destination.delivery_operation_id
            && self.helper.verify_scoped(self.wire, Action::Deliver)
    }
}
