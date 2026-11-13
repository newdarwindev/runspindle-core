//! Exact-reference artifact progression and a single-target delivery contract.

use crate::identity::{Id, IntentKey, Sha256Digest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRef {
    pub intent: IntentKey,
    pub artifact_id: Id,
    pub digest: Sha256Digest,
    pub policy_revision: Id,
}

/// Supplied by a trusted validator. Evidence references are not authenticated here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationEvidence {
    pub artifact: ArtifactRef,
    pub evidence_id: Id,
    pub validator: Id,
}

/// Supplied by an authorized acceptance adapter, for this exact validation record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptanceEvidence {
    pub artifact: ArtifactRef,
    pub validation_evidence_id: Id,
    pub evidence_id: Id,
    pub approver: Id,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactState {
    Produced,
    Validated,
    Accepted,
    Delivered,
}

/// A single effect identity and destination. No connector is invoked by this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryRequest {
    pub artifact: ArtifactRef,
    pub delivery_id: Id,
    pub destination: Id,
}

/// An observation provided by a trusted adapter, not proof verified by this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub request: DeliveryRequest,
    pub external_receipt_id: Id,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryState {
    NotStarted,
    Pending(DeliveryRequest),
    Uncertain(DeliveryRequest),
    Delivered(DeliveryReceipt),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryDecision {
    Started,
    AlreadyPending,
    ReconcileRequired,
    AlreadyDelivered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactError {
    ArtifactMismatch,
    InvalidTransition,
    ValidationEvidenceMismatch,
    NotAccepted,
    DeliveryConflict,
    DeliveryNotStarted,
    ReceiptConflict,
}

/// Private fields prevent a caller from skipping transitions by mutating state.
/// There is no persistence, deserialization, hashing, approval or I/O adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Artifact {
    reference: ArtifactRef,
    state: ArtifactState,
    validation: Option<ValidationEvidence>,
    acceptance: Option<AcceptanceEvidence>,
    delivery: DeliveryState,
}

impl Artifact {
    pub fn produced(reference: ArtifactRef) -> Self {
        Self {
            reference,
            state: ArtifactState::Produced,
            validation: None,
            acceptance: None,
            delivery: DeliveryState::NotStarted,
        }
    }

    pub fn reference(&self) -> &ArtifactRef {
        &self.reference
    }

    pub fn state(&self) -> ArtifactState {
        self.state
    }

    pub fn validation(&self) -> Option<&ValidationEvidence> {
        self.validation.as_ref()
    }

    pub fn acceptance(&self) -> Option<&AcceptanceEvidence> {
        self.acceptance.as_ref()
    }

    pub fn delivery_state(&self) -> &DeliveryState {
        &self.delivery
    }

    fn require_artifact(&self, reference: &ArtifactRef) -> Result<(), ArtifactError> {
        if reference == &self.reference {
            Ok(())
        } else {
            Err(ArtifactError::ArtifactMismatch)
        }
    }

    /// Records a trusted validator's exact-artifact evidence; does not validate bytes.
    pub fn record_validation(&mut self, evidence: ValidationEvidence) -> Result<(), ArtifactError> {
        self.require_artifact(&evidence.artifact)?;
        if self.state != ArtifactState::Produced {
            return Err(ArtifactError::InvalidTransition);
        }
        self.validation = Some(evidence);
        self.state = ArtifactState::Validated;
        Ok(())
    }

    /// Records an authorized acceptance; this library does not authenticate the actor.
    pub fn record_acceptance(&mut self, evidence: AcceptanceEvidence) -> Result<(), ArtifactError> {
        self.require_artifact(&evidence.artifact)?;
        if self.state != ArtifactState::Validated {
            return Err(ArtifactError::InvalidTransition);
        }
        if self.validation.as_ref().map(|v| &v.evidence_id)
            != Some(&evidence.validation_evidence_id)
        {
            return Err(ArtifactError::ValidationEvidenceMismatch);
        }
        self.acceptance = Some(evidence);
        self.state = ArtifactState::Accepted;
        Ok(())
    }

    /// Reserves the one delivery identity. Only `Started` is a fresh reservation.
    /// It is not permission to perform an external effect or a durable outbox entry.
    pub fn begin_delivery(
        &mut self,
        request: DeliveryRequest,
    ) -> Result<DeliveryDecision, ArtifactError> {
        self.require_artifact(&request.artifact)?;
        if !matches!(
            self.state,
            ArtifactState::Accepted | ArtifactState::Delivered
        ) {
            return Err(ArtifactError::NotAccepted);
        }
        let decision = match &self.delivery {
            DeliveryState::NotStarted => DeliveryDecision::Started,
            DeliveryState::Pending(existing) if existing == &request => {
                DeliveryDecision::AlreadyPending
            }
            DeliveryState::Uncertain(existing) if existing == &request => {
                DeliveryDecision::ReconcileRequired
            }
            DeliveryState::Delivered(receipt) if receipt.request == request => {
                DeliveryDecision::AlreadyDelivered
            }
            _ => return Err(ArtifactError::DeliveryConflict),
        };
        if decision == DeliveryDecision::Started {
            self.delivery = DeliveryState::Pending(request);
        }
        Ok(decision)
    }

    /// A lost acknowledgement never resets the record to a retryable new effect.
    pub fn mark_delivery_uncertain(
        &mut self,
        request: &DeliveryRequest,
    ) -> Result<(), ArtifactError> {
        self.require_artifact(&request.artifact)?;
        match &self.delivery {
            DeliveryState::Pending(existing) | DeliveryState::Uncertain(existing)
                if existing == request =>
            {
                self.delivery = DeliveryState::Uncertain(request.clone());
                Ok(())
            }
            DeliveryState::NotStarted => Err(ArtifactError::DeliveryNotStarted),
            _ => Err(ArtifactError::DeliveryConflict),
        }
    }

    /// Records observed delivery or a positive reconciliation, never submission alone.
    /// Repeating the identical receipt is idempotent; conflicting receipts are rejected.
    pub fn record_observed_delivery(
        &mut self,
        receipt: DeliveryReceipt,
    ) -> Result<(), ArtifactError> {
        self.require_artifact(&receipt.request.artifact)?;
        if !matches!(
            self.state,
            ArtifactState::Accepted | ArtifactState::Delivered
        ) {
            return Err(ArtifactError::NotAccepted);
        }
        match &self.delivery {
            DeliveryState::Pending(request) | DeliveryState::Uncertain(request)
                if request == &receipt.request => {}
            DeliveryState::Delivered(existing) if existing == &receipt => return Ok(()),
            DeliveryState::Delivered(existing) if existing.request == receipt.request => {
                return Err(ArtifactError::ReceiptConflict);
            }
            DeliveryState::NotStarted => return Err(ArtifactError::DeliveryNotStarted),
            _ => return Err(ArtifactError::DeliveryConflict),
        }
        self.delivery = DeliveryState::Delivered(receipt);
        self.state = ArtifactState::Delivered;
        Ok(())
    }
}
