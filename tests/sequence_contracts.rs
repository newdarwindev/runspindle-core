//! Bounded exhaustive local-model checks; no adapter, persistence or authority proof.

use runspindle_core::artifact::*;
use runspindle_core::identity::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn intent(revision: u64) -> IntentSpec {
    IntentSpec {
        key: IntentKey {
            task_id: id("synthetic-task"),
            revision,
        },
        semantic_digest: Sha256Digest::from_bytes([7; 32]),
        policy_revision: id("synthetic-policy"),
        host_pin: HostPin {
            host_id: id("synthetic-host"),
            workspace_id: id("synthetic-workspace"),
        },
    }
}

fn event(value: &str) -> RequestDeliveryKey {
    RequestDeliveryKey {
        scope: id("synthetic-source"),
        event_id: id(value),
    }
}

#[test]
fn id_alphabet_and_length_boundaries_are_exhaustive_for_ascii() {
    // Explicit contract alphabet, independent of the implementation predicate.
    let allowed = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:";
    for byte in 0..=127_u8 {
        for length in [1, 128] {
            let value = String::from_utf8(vec![byte; length]).unwrap();
            assert_eq!(
                Id::new(value).is_ok(),
                allowed.contains(&byte),
                "ASCII byte {byte}, length {length}"
            );
        }
    }
    for length in [0, 129, 256] {
        assert_eq!(Id::new("a".repeat(length)), Err(InvalidId));
    }
    for invalid in ['\u{80}', '\u{7ff}', '\u{ffff}', '\u{10ffff}'] {
        assert_eq!(Id::new(invalid.to_string()), Err(InvalidId));
    }
}

#[test]
fn failed_delivery_reuse_does_not_register_a_new_task_or_revision() {
    // The local model deliberately accepts unordered numeric revisions. Test the
    // entire u64 boundary, without inventing an agreed supersession policy.
    for revision in [0, 2, u64::MAX] {
        let mut ledger = AdmissionLedger::default();
        ledger.admit(event("original"), intent(1)).unwrap();
        let revised = intent(revision);
        assert_eq!(
            ledger.admit(event("original"), revised.clone()),
            Err(AdmissionError::DeliveryConflict)
        );
        assert_eq!(
            ledger.admit(event("revision"), revised.clone()),
            Ok(Admission::NewRevision)
        );
        assert_eq!(
            ledger.admit(event("revision"), revised),
            Ok(Admission::DuplicateDelivery)
        );
        let mut another_task = intent(revision);
        another_task.key.task_id = id("synthetic-other-task");
        assert_eq!(
            ledger.admit(event("original"), another_task.clone()),
            Err(AdmissionError::DeliveryConflict)
        );
        assert_eq!(
            ledger.admit(event("other-task"), another_task),
            Ok(Admission::NewIntent)
        );
        assert_eq!(
            ledger.admit(event("original"), intent(1)),
            Ok(Admission::DuplicateDelivery)
        );
    }
}

// This table specifies legal transitions independently of Artifact methods.
// Columns: validate, accept, begin, uncertain, observe. Rows: produced,
// validated, accepted/unreserved, pending, uncertain, delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Unit,
    Delivery(DeliveryDecision),
    Error(ArtifactError),
}

use ArtifactError::{DeliveryConflict, DeliveryNotStarted, InvalidTransition, NotAccepted};
use DeliveryDecision::{AlreadyDelivered, AlreadyPending, ReconcileRequired, Started};
use Outcome::{Delivery, Error, Unit};

const RULES: [[(usize, Outcome); 5]; 6] = [
    [
        (1, Unit),
        (0, Error(InvalidTransition)),
        (0, Error(NotAccepted)),
        (0, Error(DeliveryNotStarted)),
        (0, Error(NotAccepted)),
    ],
    [
        (1, Error(InvalidTransition)),
        (2, Unit),
        (1, Error(NotAccepted)),
        (1, Error(DeliveryNotStarted)),
        (1, Error(NotAccepted)),
    ],
    [
        (2, Error(InvalidTransition)),
        (2, Error(InvalidTransition)),
        (3, Delivery(Started)),
        (2, Error(DeliveryNotStarted)),
        (2, Error(DeliveryNotStarted)),
    ],
    [
        (3, Error(InvalidTransition)),
        (3, Error(InvalidTransition)),
        (3, Delivery(AlreadyPending)),
        (4, Unit),
        (5, Unit),
    ],
    [
        (4, Error(InvalidTransition)),
        (4, Error(InvalidTransition)),
        (4, Delivery(ReconcileRequired)),
        (4, Unit),
        (5, Unit),
    ],
    [
        (5, Error(InvalidTransition)),
        (5, Error(InvalidTransition)),
        (5, Delivery(AlreadyDelivered)),
        (5, Error(DeliveryConflict)),
        (5, Unit),
    ],
];

fn check_trace(artifact: &Artifact, row: usize, remaining: usize, trace: &mut Vec<usize>) -> usize {
    if remaining == 0 {
        return 0;
    }
    let mut count = 0;
    for (action, &(next_row, expected)) in RULES[row].iter().enumerate() {
        let mut next = artifact.clone();
        let reference = artifact.reference().clone();
        let request = DeliveryRequest {
            artifact: reference.clone(),
            delivery_id: id("synthetic-delivery"),
            destination: id("synthetic-destination"),
        };
        let expected_request = request.clone();
        let unit = |result: Result<(), ArtifactError>| result.map_or_else(Error, |_| Unit);
        let actual = match action {
            0 => unit(next.record_validation(ValidationEvidence {
                artifact: reference.clone(),
                evidence_id: id("synthetic-validation"),
                validator: id("synthetic-validator"),
            })),
            1 => unit(next.record_acceptance(AcceptanceEvidence {
                artifact: reference,
                validation_evidence_id: id("synthetic-validation"),
                evidence_id: id("synthetic-acceptance"),
                approver: id("synthetic-approver"),
            })),
            2 => next.begin_delivery(request).map_or_else(Error, Delivery),
            3 => unit(next.mark_delivery_uncertain(&request)),
            4 => unit(next.record_observed_delivery(DeliveryReceipt {
                request,
                external_receipt_id: id("synthetic-observation"),
            })),
            _ => unreachable!(),
        };
        trace.push(action);
        assert_eq!(actual, expected, "trace {trace:?}");
        assert_eq!(next.reference(), artifact.reference(), "trace {trace:?}");
        if matches!(expected, Error(_)) || next_row == row {
            assert_eq!(next, *artifact, "non-transition mutated state: {trace:?}");
        }
        let expected_state = [
            ArtifactState::Produced,
            ArtifactState::Validated,
            ArtifactState::Accepted,
            ArtifactState::Accepted,
            ArtifactState::Accepted,
            ArtifactState::Delivered,
        ][next_row];
        assert_eq!(next.state(), expected_state, "trace {trace:?}");
        let expected_delivery = match next_row {
            0..=2 => DeliveryState::NotStarted,
            3 => DeliveryState::Pending(expected_request),
            4 => DeliveryState::Uncertain(expected_request),
            5 => DeliveryState::Delivered(DeliveryReceipt {
                request: expected_request,
                external_receipt_id: id("synthetic-observation"),
            }),
            _ => unreachable!(),
        };
        assert_eq!(next.delivery_state(), &expected_delivery, "trace {trace:?}");
        assert_eq!(next.validation().is_some(), next_row >= 1);
        assert_eq!(next.acceptance().is_some(), next_row >= 2);
        count += 1 + check_trace(&next, next_row, remaining - 1, trace);
        trace.pop();
    }
    count
}

#[test]
fn every_artifact_action_trace_through_length_seven_matches_transition_table() {
    let artifact = Artifact::produced(ArtifactRef {
        intent: intent(1).key,
        artifact_id: id("synthetic-artifact"),
        digest: Sha256Digest::from_bytes([9; 32]),
        policy_revision: id("synthetic-policy"),
    });
    // 5 + 5^2 + ... + 5^7. Includes success, rejected interleavings, repeat
    // reservations, repeated uncertainty, reconciliation and delivered replays.
    assert_eq!(check_trace(&artifact, 0, 7, &mut Vec::new()), 97_655);
}
