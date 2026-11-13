//! Synthetic component contracts only. No native session, network or real effects.

use runspindle_core::artifact::*;
use runspindle_core::identity::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn digest(byte: u8) -> Sha256Digest {
    Sha256Digest::from_bytes([byte; 32])
}

fn pin() -> HostPin {
    HostPin {
        host_id: id("synthetic-host-a"),
        workspace_id: id("synthetic-workspace-a"),
    }
}

fn intent() -> IntentSpec {
    IntentSpec {
        key: IntentKey {
            task_id: id("synthetic-task-a"),
            revision: 1,
        },
        semantic_digest: digest(1),
        policy_revision: id("synthetic-policy-1"),
        host_pin: pin(),
    }
}

fn transport(event: &str) -> RequestDeliveryKey {
    RequestDeliveryKey {
        scope: id("synthetic-source-a"),
        event_id: id(event),
    }
}

fn reference() -> ArtifactRef {
    ArtifactRef {
        intent: intent().key,
        artifact_id: id("synthetic-artifact-a"),
        digest: digest(2),
        policy_revision: id("synthetic-policy-1"),
    }
}

fn validation() -> ValidationEvidence {
    ValidationEvidence {
        artifact: reference(),
        evidence_id: id("synthetic-validation-1"),
        validator: id("synthetic-validator"),
    }
}

fn acceptance() -> AcceptanceEvidence {
    AcceptanceEvidence {
        artifact: reference(),
        validation_evidence_id: validation().evidence_id,
        evidence_id: id("synthetic-acceptance-1"),
        approver: id("synthetic-approver"),
    }
}

fn request() -> DeliveryRequest {
    DeliveryRequest {
        artifact: reference(),
        delivery_id: id("synthetic-delivery-1"),
        destination: id("synthetic-destination-a"),
    }
}

fn receipt() -> DeliveryReceipt {
    DeliveryReceipt {
        request: request(),
        external_receipt_id: id("synthetic-observation-1"),
    }
}

fn at(state: ArtifactState) -> Artifact {
    let mut artifact = Artifact::produced(reference());
    if state != ArtifactState::Produced {
        artifact.record_validation(validation()).unwrap();
    }
    if matches!(state, ArtifactState::Accepted | ArtifactState::Delivered) {
        artifact.record_acceptance(acceptance()).unwrap();
    }
    if state == ArtifactState::Delivered {
        artifact.begin_delivery(request()).unwrap();
        artifact.record_observed_delivery(receipt()).unwrap();
    }
    artifact
}

#[test]
fn opaque_ids_reject_empty_whitespace_urls_and_overlong_values() {
    for invalid in [
        "",
        "two words",
        "line\nbreak",
        "https://example.invalid/a",
        "é",
    ] {
        assert_eq!(Id::new(invalid), Err(InvalidId));
    }
    assert_eq!(Id::new("a".repeat(129)), Err(InvalidId));
    assert_eq!(id("source:host-a_1.v2").as_str(), "source:host-a_1.v2");
    assert_eq!(digest(2).as_bytes(), &[2; 32]);
}

#[test]
fn admission_distinguishes_transport_replay_revision_and_intent() {
    let mut ledger = AdmissionLedger::default();
    assert_eq!(
        ledger.admit(transport("event-1"), intent()),
        Ok(Admission::NewIntent)
    );
    assert_eq!(
        ledger.admit(transport("event-1"), intent()),
        Ok(Admission::DuplicateDelivery)
    );
    assert_eq!(
        ledger.admit(transport("event-2"), intent()),
        Ok(Admission::ExistingRevision)
    );
    assert_eq!(
        ledger.admit(transport("event-2"), intent()),
        Ok(Admission::DuplicateDelivery)
    );
    let mut revised = intent();
    revised.key.revision = 2;
    assert_eq!(
        ledger.admit(transport("event-3"), revised),
        Ok(Admission::NewRevision)
    );
    let mut another = intent();
    another.key.task_id = id("synthetic-task-b");
    assert_eq!(
        ledger.admit(transport("event-4"), another),
        Ok(Admission::NewIntent)
    );
}

#[test]
fn transport_namespaces_do_not_alias_event_ids() {
    let mut ledger = AdmissionLedger::default();
    ledger.admit(transport("event-1"), intent()).unwrap();
    let mut other_source = transport("event-1");
    other_source.scope = id("synthetic-source-b");
    assert_eq!(
        ledger.admit(other_source, intent()),
        Ok(Admission::ExistingRevision)
    );
}

#[test]
fn conflicting_payload_host_workspace_policy_and_revision_reuse_are_rejected() {
    let mut conflicts = Vec::new();
    let mut changed = intent();
    changed.semantic_digest = digest(9);
    conflicts.push(changed);
    let mut changed = intent();
    changed.host_pin.host_id = id("synthetic-host-b");
    conflicts.push(changed);
    let mut changed = intent();
    changed.host_pin.workspace_id = id("synthetic-workspace-b");
    conflicts.push(changed);
    let mut changed = intent();
    changed.policy_revision = id("synthetic-policy-2");
    conflicts.push(changed);
    for changed in conflicts {
        let mut ledger = AdmissionLedger::default();
        ledger.admit(transport("event-1"), intent()).unwrap();
        assert_eq!(
            ledger.admit(transport("event-1"), changed.clone()),
            Err(AdmissionError::DeliveryConflict)
        );
        assert_eq!(
            ledger.admit(transport("event-2"), changed),
            Err(AdmissionError::IntentConflict)
        );
        // Neither failed registration consumes event-2 or alters the original.
        assert_eq!(
            ledger.admit(transport("event-2"), intent()),
            Ok(Admission::ExistingRevision)
        );
        assert_eq!(
            ledger.admit(transport("event-1"), intent()),
            Ok(Admission::DuplicateDelivery)
        );
    }
    let mut ledger = AdmissionLedger::default();
    ledger.admit(transport("event-1"), intent()).unwrap();
    let mut revised = intent();
    revised.key.revision = 2;
    assert_eq!(
        ledger.admit(transport("event-1"), revised),
        Err(AdmissionError::DeliveryConflict)
    );
}

#[test]
fn host_pin_never_fails_over_and_unknown_is_not_eligible() {
    for (availability, expected) in [
        (Availability::Online, Ok(HostDecision::Eligible)),
        (Availability::Offline, Ok(HostDecision::QueueForPinnedHost)),
        (Availability::Unknown, Err(HostError::AvailabilityUnknown)),
    ] {
        let observed = HostObservation {
            host: pin(),
            availability,
        };
        assert_eq!(check_host(&pin(), &observed), expected);
        let mut wrong_host = observed.clone();
        wrong_host.host.host_id = id("synthetic-host-b");
        assert_eq!(
            check_host(&pin(), &wrong_host),
            Err(HostError::HostMismatch)
        );
        let mut wrong_workspace = observed;
        wrong_workspace.host.workspace_id = id("synthetic-workspace-b");
        assert_eq!(
            check_host(&pin(), &wrong_workspace),
            Err(HostError::WorkspaceMismatch)
        );
    }
}

#[test]
fn lifecycle_requires_validation_acceptance_and_observed_receipt() {
    let mut artifact = at(ArtifactState::Produced);
    assert_eq!(artifact.reference(), &reference());
    assert_eq!(artifact.validation(), None);
    assert_eq!(artifact.acceptance(), None);
    artifact.record_validation(validation()).unwrap();
    assert_eq!(artifact.state(), ArtifactState::Validated);
    assert_eq!(artifact.validation(), Some(&validation()));
    artifact.record_acceptance(acceptance()).unwrap();
    assert_eq!(artifact.state(), ArtifactState::Accepted);
    assert_eq!(artifact.acceptance(), Some(&acceptance()));
    assert_eq!(
        artifact.begin_delivery(request()),
        Ok(DeliveryDecision::Started)
    );
    assert_eq!(artifact.state(), ArtifactState::Accepted);
    assert_eq!(
        artifact.delivery_state(),
        &DeliveryState::Pending(request())
    );
    artifact.record_observed_delivery(receipt()).unwrap();
    assert_eq!(artifact.state(), ArtifactState::Delivered);
    assert_eq!(
        artifact.delivery_state(),
        &DeliveryState::Delivered(receipt())
    );
}

#[test]
fn delivery_before_acceptance_rejects_without_mutation() {
    for state in [ArtifactState::Produced, ArtifactState::Validated] {
        let mut artifact = at(state);
        let before = artifact.clone();
        assert_eq!(
            artifact.begin_delivery(request()),
            Err(ArtifactError::NotAccepted)
        );
        assert_eq!(
            artifact.record_observed_delivery(receipt()),
            Err(ArtifactError::NotAccepted)
        );
        assert_eq!(artifact, before);
    }
}

#[test]
fn state_transition_matrix_rejects_skip_repeat_and_regression() {
    for state in [
        ArtifactState::Produced,
        ArtifactState::Validated,
        ArtifactState::Accepted,
        ArtifactState::Delivered,
    ] {
        let mut artifact = at(state);
        let before = artifact.clone();
        if state != ArtifactState::Produced {
            assert_eq!(
                artifact.record_validation(validation()),
                Err(ArtifactError::InvalidTransition)
            );
            assert_eq!(artifact, before);
        }
        if state != ArtifactState::Validated {
            assert_eq!(
                artifact.record_acceptance(acceptance()),
                Err(ArtifactError::InvalidTransition)
            );
            assert_eq!(artifact, before);
        }
    }
}

#[test]
fn all_transitions_reject_changed_digest_identity_revision_or_policy() {
    let mut mismatches = Vec::new();
    let mut changed = reference();
    changed.digest = digest(9);
    mismatches.push(changed);
    let mut changed = reference();
    changed.intent.task_id = id("synthetic-task-b");
    mismatches.push(changed);
    let mut changed = reference();
    changed.intent.revision = 2;
    mismatches.push(changed);
    let mut changed = reference();
    changed.artifact_id = id("synthetic-artifact-b");
    mismatches.push(changed);
    let mut changed = reference();
    changed.policy_revision = id("synthetic-policy-2");
    mismatches.push(changed);
    for changed in mismatches {
        let mut produced = at(ArtifactState::Produced);
        let before = produced.clone();
        let bad = ValidationEvidence {
            artifact: changed.clone(),
            ..validation()
        };
        assert_eq!(
            produced.record_validation(bad),
            Err(ArtifactError::ArtifactMismatch)
        );
        assert_eq!(produced, before);
        let mut validated = at(ArtifactState::Validated);
        let before = validated.clone();
        let bad = AcceptanceEvidence {
            artifact: changed.clone(),
            ..acceptance()
        };
        assert_eq!(
            validated.record_acceptance(bad),
            Err(ArtifactError::ArtifactMismatch)
        );
        assert_eq!(validated, before);
        let mut accepted = at(ArtifactState::Accepted);
        let before = accepted.clone();
        let bad = DeliveryRequest {
            artifact: changed,
            ..request()
        };
        assert_eq!(
            accepted.begin_delivery(bad.clone()),
            Err(ArtifactError::ArtifactMismatch)
        );
        assert_eq!(accepted, before);
        accepted.begin_delivery(request()).unwrap();
        let before = accepted.clone();
        assert_eq!(
            accepted.mark_delivery_uncertain(&bad),
            Err(ArtifactError::ArtifactMismatch)
        );
        let bad_receipt = DeliveryReceipt {
            request: bad,
            ..receipt()
        };
        assert_eq!(
            accepted.record_observed_delivery(bad_receipt),
            Err(ArtifactError::ArtifactMismatch)
        );
        assert_eq!(accepted, before);
    }
}

#[test]
fn acceptance_of_a_previous_validation_is_rejected() {
    let mut artifact = at(ArtifactState::Validated);
    let before = artifact.clone();
    let mut stale = acceptance();
    stale.validation_evidence_id = id("synthetic-previous-validation");
    assert_eq!(
        artifact.record_acceptance(stale),
        Err(ArtifactError::ValidationEvidenceMismatch)
    );
    assert_eq!(artifact, before);
}

#[test]
fn an_old_acceptance_cannot_accept_a_new_revision_with_identical_bytes() {
    let mut next_ref = reference();
    next_ref.intent.revision = 2;
    let mut artifact = Artifact::produced(next_ref.clone());
    artifact
        .record_validation(ValidationEvidence {
            artifact: next_ref,
            ..validation()
        })
        .unwrap();
    let before = artifact.clone();
    assert_eq!(
        artifact.record_acceptance(acceptance()),
        Err(ArtifactError::ArtifactMismatch)
    );
    assert_eq!(artifact, before);
}

#[test]
fn duplicate_delivery_is_not_a_new_reservation() {
    let mut artifact = at(ArtifactState::Accepted);
    artifact.begin_delivery(request()).unwrap();
    let pending = artifact.clone();
    assert_eq!(
        artifact.begin_delivery(request()),
        Ok(DeliveryDecision::AlreadyPending)
    );
    assert_eq!(artifact, pending);
    artifact.record_observed_delivery(receipt()).unwrap();
    let delivered = artifact.clone();
    assert_eq!(
        artifact.begin_delivery(request()),
        Ok(DeliveryDecision::AlreadyDelivered)
    );
    assert_eq!(artifact.record_observed_delivery(receipt()), Ok(()));
    assert_eq!(artifact, delivered);
}

#[test]
fn uncertain_delivery_requires_reconciliation_not_blind_retry() {
    let mut artifact = at(ArtifactState::Accepted);
    artifact.begin_delivery(request()).unwrap();
    artifact.mark_delivery_uncertain(&request()).unwrap();
    let uncertain = artifact.clone();
    assert_eq!(
        artifact.begin_delivery(request()),
        Ok(DeliveryDecision::ReconcileRequired)
    );
    assert_eq!(artifact.mark_delivery_uncertain(&request()), Ok(()));
    assert_eq!(artifact, uncertain);
    assert_eq!(artifact.state(), ArtifactState::Accepted);
    assert_eq!(
        artifact.delivery_state(),
        &DeliveryState::Uncertain(request())
    );
    artifact.record_observed_delivery(receipt()).unwrap();
    assert_eq!(artifact.state(), ArtifactState::Delivered);
}

#[test]
fn receipt_or_uncertainty_before_reservation_is_rejected() {
    let mut artifact = at(ArtifactState::Accepted);
    let before = artifact.clone();
    assert_eq!(
        artifact.record_observed_delivery(receipt()),
        Err(ArtifactError::DeliveryNotStarted)
    );
    assert_eq!(
        artifact.mark_delivery_uncertain(&request()),
        Err(ArtifactError::DeliveryNotStarted)
    );
    assert_eq!(artifact, before);
}

#[test]
fn delivery_id_and_destination_conflicts_reject_in_every_reserved_state() {
    for stage in 0..3 {
        let mut artifact = at(ArtifactState::Accepted);
        artifact.begin_delivery(request()).unwrap();
        if stage == 1 {
            artifact.mark_delivery_uncertain(&request()).unwrap();
        } else if stage == 2 {
            artifact.record_observed_delivery(receipt()).unwrap();
        }
        let before = artifact.clone();
        for change_destination in [false, true] {
            let mut bad = request();
            if change_destination {
                bad.destination = id("synthetic-destination-b");
            } else {
                bad.delivery_id = id("synthetic-delivery-2");
            }
            assert_eq!(
                artifact.begin_delivery(bad.clone()),
                Err(ArtifactError::DeliveryConflict)
            );
            assert_eq!(
                artifact.mark_delivery_uncertain(&bad),
                Err(ArtifactError::DeliveryConflict)
            );
            let bad_receipt = DeliveryReceipt {
                request: bad,
                ..receipt()
            };
            assert_eq!(
                artifact.record_observed_delivery(bad_receipt),
                Err(ArtifactError::DeliveryConflict)
            );
            assert_eq!(artifact, before);
        }
    }
}

#[test]
fn a_delivered_receipt_cannot_be_replaced_or_regressed_to_uncertain() {
    let mut artifact = at(ArtifactState::Delivered);
    let before = artifact.clone();
    let mut conflicting = receipt();
    conflicting.external_receipt_id = id("synthetic-observation-2");
    assert_eq!(
        artifact.record_observed_delivery(conflicting),
        Err(ArtifactError::ReceiptConflict)
    );
    assert_eq!(
        artifact.mark_delivery_uncertain(&request()),
        Err(ArtifactError::DeliveryConflict)
    );
    assert_eq!(artifact, before);
}
