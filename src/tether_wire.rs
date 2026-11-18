//! Exact-byte Tether wire bridge. Peer is a trusted local adapter, not an authority.
//! No endpoint, credentials, subprocess, retry, or production activation is supplied.
use crate::{
    durable::{self, Store},
    outbox::*,
};
use serde::Deserialize;
use serde_json::{json, Value};
pub const VERSION: &str = "runspindle.tether-wire-bridge.v1";
pub trait Peer {
    /// Bounded one-shot exchange. An error after send is an unknown outcome.
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: String,
    request_sha256: String,
    evidence_class: String,
    state: String,
    file_id: Option<String>,
    message_id: Option<String>,
    conflict: bool,
}
/// One immutable delivery binding. Fresh instances recover bytes from the actual
/// accepted Store; callers may not substitute a digest-only artifact or new target.
pub struct WireTransport<P> {
    request: DeliveryRequest,
    bytes: Vec<u8>,
    envelope: Value,
    payload: Value,
    peer: P,
}
impl<P: Peer> WireTransport<P> {
    pub fn new(
        store: &Store,
        request: DeliveryRequest,
        filename: &str,
        text: &str,
        peer: P,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(request.valid(), "invalid bridge delivery");
        anyhow::ensure!(
            !filename.is_empty()
                && filename.len() <= 255
                && !filename.chars().any(char::is_control)
                && text.len() <= durable::MAX_BYTES,
            "invalid presentation"
        );
        let bytes = store.accepted(&request.accepted)?;
        let artifact = request.accepted.artifact();
        anyhow::ensure!(
            durable::digest(&bytes) == artifact.sha256,
            "artifact digest mismatch"
        );
        let envelope = json!({"version":"runspindle.envelope.v1", "message_type":"delivery_request", "message_id":request.delivery_operation_id,
            "binding":artifact.binding, "payload": {"artifact_id":artifact.artifact_id,"artifact_revision":artifact.artifact_revision,"sha256":artifact.sha256,
                "validator_revision":request.accepted.validation.validator_revision,"account_id":request.account_id,"destination_id":request.destination_id,"delivery_operation_id":request.delivery_operation_id}});
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        let payload = json!({"text":text,"attachment":{"attachment_id":artifact.artifact_id,"filename":filename,"content_hex":hex}});
        Ok(Self {
            request,
            bytes,
            envelope,
            payload,
            peer,
        })
    }
    fn observe(
        &mut self,
        request: &DeliveryRequest,
        action: &str,
    ) -> anyhow::Result<ReadbackObservation> {
        anyhow::ensure!(request == &self.request, "bridge binding changed");
        // Value maps use sorted keys; this is the canonical JSON consumed by Tether.
        let wire = serde_json::to_vec(
            &json!({"version":VERSION,"action":action,"envelope":self.envelope,"payload":self.payload}),
        )?;
        let raw = self
            .peer
            .exchange(&wire)
            .map_err(|_| anyhow::anyhow!("wire peer exchange failed"))?;
        anyhow::ensure!(raw.len() <= 4096, "oversized bridge observation");
        let observation: Response = serde_json::from_slice(&raw)
            .map_err(|_| anyhow::anyhow!("invalid wire peer observation"))?;
        anyhow::ensure!(
            observation.version == VERSION
                && observation.request_sha256 == durable::digest(&wire)
                && observation.evidence_class == "slack_wire_content_observation",
            "unbound bridge observation"
        );
        anyhow::ensure!(
            matches!(observation.state.as_str(), "content_confirmed" | "unknown"),
            "unsupported bridge state"
        );
        if observation.conflict {
            anyhow::ensure!(
                observation.state == "unknown" && observation.message_id.is_none(),
                "invalid conflict observation"
            );
            return Ok(ReadbackObservation {
                receipts: vec![],
                conflict: true,
            });
        }
        if observation.state == "unknown" {
            return Ok(ReadbackObservation::default());
        }
        let file = observation
            .file_id
            .ok_or_else(|| anyhow::anyhow!("missing file identity"))?;
        let message = observation
            .message_id
            .ok_or_else(|| anyhow::anyhow!("missing message identity"))?;
        anyhow::ensure!(
            crate::runtime::valid_id(&file) && crate::runtime::valid_id(&message),
            "invalid wire identities"
        );
        // Stable exact provider pair, not an upload acknowledgement or invented
        // synthetic receipt. Provider lookup must independently confirm it again.
        let pair = serde_json::to_vec(&(file, message))?;
        let receipt_id = format!("tether:{}", durable::digest(&pair));
        Ok(ReadbackObservation {
            receipts: vec![DeliveryReceipt {
                request: request.clone(),
                receipt_id,
            }],
            conflict: false,
        })
    }
}
impl<P: Peer> Transport for WireTransport<P> {
    fn send(
        &mut self,
        request: &DeliveryRequest,
        bytes: &[u8],
    ) -> anyhow::Result<Option<DeliveryReceipt>> {
        let result = self.send_observation(request, bytes)?;
        anyhow::ensure!(!result.conflict, "wire conflict");
        Ok(result.acknowledgement)
    }
    fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        let result = self.lookup_observation(request)?;
        anyhow::ensure!(!result.conflict, "wire conflict");
        Ok(result.receipts)
    }
    fn send_observation(
        &mut self,
        request: &DeliveryRequest,
        bytes: &[u8],
    ) -> anyhow::Result<SendObservation> {
        anyhow::ensure!(bytes == self.bytes, "bridge bytes changed");
        let result = self.observe(request, "send")?;
        Ok(SendObservation {
            acknowledgement: result.receipts.into_iter().next(),
            conflict: result.conflict,
        })
    }
    fn lookup_observation(
        &mut self,
        request: &DeliveryRequest,
    ) -> anyhow::Result<ReadbackObservation> {
        self.observe(request, "lookup")
    }
}
