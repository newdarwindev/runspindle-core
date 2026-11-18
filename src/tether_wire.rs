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

pub const RESOLUTION_VERSION: &str = "runspindle.tether-wire-resolution-bridge.v1";
#[derive(Clone, Debug, serde::Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolutionDecision {
    pub version: String,
    pub decision_id: String,
    pub policy_ref: String,
    pub authorization_ref: String,
    pub conflict_sha256: String,
    pub core_resolution_sha256: String,
    pub file_id: String,
    pub message_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolutionResponse {
    version: String,
    request_sha256: String,
    decision_sha256: Option<String>,
    evidence_class: String,
    state: String,
    file_id: Option<String>,
    message_id: Option<String>,
}
fn canonical<T: serde::Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::to_value(value)?)?)
}
/// Deliberately NOT a public Transport: ordinary delivery reconciliation cannot
/// consume resolver evidence without the separate current Core authority.
pub struct ResolutionBridge<P> {
    wire: WireTransport<P>,
    resolution: ConflictResolution,
    decision: ResolutionDecision,
}
impl<P: Peer> ResolutionBridge<P> {
    pub fn new(
        store: &Store,
        resolution: ConflictResolution,
        decision: ResolutionDecision,
        filename: &str,
        text: &str,
        peer: P,
    ) -> anyhow::Result<Self> {
        let request = &resolution.prior.request;
        let digest_valid = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        anyhow::ensure!(
            resolution.prior.conflict
                && resolution.receipt.request == *request
                && decision.version == "runspindle.wire-resolution.v1"
                && [
                    &decision.decision_id,
                    &decision.policy_ref,
                    &decision.authorization_ref,
                    &decision.file_id,
                    &decision.message_id
                ]
                .iter()
                .all(|s| crate::runtime::valid_id(s))
                && decision.authorization_ref == resolution.authorization_ref
                && digest_valid(&decision.conflict_sha256)
                && decision.core_resolution_sha256 == durable::digest(&canonical(&resolution)?),
            "invalid resolution binding"
        );
        anyhow::ensure!(
            decision.file_id.starts_with('F')
                && decision.file_id.len() > 1
                && decision.file_id[1..]
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                && decision.message_id.len() <= 64
                && decision
                    .message_id
                    .split_once('.')
                    .is_some_and(|(seconds, fraction)| !seconds.is_empty()
                        && seconds.bytes().all(|b| b.is_ascii_digit())
                        && fraction.len() == 6
                        && fraction.bytes().all(|b| b.is_ascii_digit())),
            "invalid selected Slack file/message identity"
        );
        let pair = serde_json::to_vec(&(&decision.file_id, &decision.message_id))?;
        anyhow::ensure!(
            resolution.receipt.receipt_id == format!("tether:{}", durable::digest(&pair)),
            "selected receipt differs from resolver decision"
        );
        let wire = WireTransport::new(store, request.clone(), filename, text, peer)?;
        Ok(Self {
            wire,
            resolution,
            decision,
        })
    }
    fn observe(&mut self, action: &str) -> anyhow::Result<Option<DeliveryReceipt>> {
        let wire = canonical(&json!({"version":RESOLUTION_VERSION,"action":action,
            "envelope":self.wire.envelope,"payload":self.wire.payload,"decision":self.decision}))?;
        let raw = self
            .wire
            .peer
            .exchange(&wire)
            .map_err(|_| anyhow::anyhow!("resolution peer exchange failed"))?;
        anyhow::ensure!(raw.len() <= 4096, "oversized resolution observation");
        let response: ResolutionResponse = serde_json::from_slice(&raw)
            .map_err(|_| anyhow::anyhow!("invalid resolution peer observation"))?;
        anyhow::ensure!(
            response.version == RESOLUTION_VERSION
                && response.request_sha256 == durable::digest(&wire)
                && response.evidence_class == "slack_wire_resolution_observation",
            "unbound resolution observation"
        );
        if response.state == "unknown" {
            anyhow::ensure!(
                response.file_id.is_none()
                    && response.message_id.is_none()
                    && response.decision_sha256.is_none(),
                "unknown resolution includes unsupported evidence"
            );
            return Ok(None);
        }
        anyhow::ensure!(
            response.state == "independently_reconciled_under_decision"
                && response.decision_sha256.as_deref()
                    == Some(durable::digest(&canonical(&self.decision)?).as_str())
                && response.file_id.as_ref() == Some(&self.decision.file_id)
                && response.message_id.as_ref() == Some(&self.decision.message_id),
            "resolution evidence mismatch"
        );
        Ok(Some(self.resolution.receipt.clone()))
    }
    /// Explicitly authorized append-only Tether decision, followed by a separate
    /// fresh lookup inside Core's transactional resolution fence. No resend.
    pub fn resolve<A: DeliveryAuthority, R: ResolutionAuthority>(
        &mut self,
        outbox: &mut Outbox,
        authority: &A,
        resolver: &R,
    ) -> anyhow::Result<DeliverySnapshot> {
        self.resolve_guarded(outbox, authority, resolver, None)
    }
    pub(crate) fn resolve_guarded<A: DeliveryAuthority, R: ResolutionAuthority>(
        &mut self,
        outbox: &mut Outbox,
        authority: &A,
        resolver: &R,
        guard: Option<&dyn crate::outbox::ArtifactGuard>,
    ) -> anyhow::Result<DeliverySnapshot> {
        anyhow::ensure!(
            authority.verify(&self.resolution.prior.request)
                && resolver.verify_resolution(&self.resolution),
            "dedicated resolution denied"
        );
        anyhow::ensure!(
            outbox.resolution_applicable(&self.resolution)?,
            "stale resolution before peer call"
        );
        outbox.check_artifact_guard(&self.resolution.prior.request, guard)?;
        anyhow::ensure!(
            self.observe("resolve")?.as_ref() == Some(&self.resolution.receipt),
            "resolution not independently observed"
        );
        anyhow::ensure!(
            authority.verify(&self.resolution.prior.request)
                && resolver.verify_resolution(&self.resolution),
            "resolution authority changed"
        );
        let resolution = self.resolution.clone();
        outbox.check_artifact_guard(&self.resolution.prior.request, guard)?;
        outbox.resolve_guarded(
            &resolution,
            authority,
            resolver,
            &mut ResolutionLookup(self),
            guard,
        )
    }
}
struct ResolutionLookup<'a, P>(&'a mut ResolutionBridge<P>);
impl<P: Peer> Transport for ResolutionLookup<'_, P> {
    fn send(&mut self, _: &DeliveryRequest, _: &[u8]) -> anyhow::Result<Option<DeliveryReceipt>> {
        anyhow::bail!("resolution cannot send")
    }
    fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        anyhow::ensure!(
            request == &self.0.resolution.prior.request,
            "resolution lookup request changed"
        );
        Ok(self.0.observe("lookup_resolution")?.into_iter().collect())
    }
}
