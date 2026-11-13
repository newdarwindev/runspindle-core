#![cfg(feature = "temporal-runtime")]
use runspindle_core::runtime::*;

fn request(task: &str, delivery: &str) -> Admission {
    Admission {
        version: VERSION.into(),
        delivery_namespace: "synthetic".into(),
        delivery_id: delivery.into(),
        binding: Binding {
            task_id: task.into(),
            revision: 1,
            operation_id: format!("op-{task}"),
            policy_revision: "policy-1".into(),
            host_id: "host-1".into(),
            workspace_id: "workspace-1".into(),
            lease_epoch: 1,
        },
        predecessor_revision: None,
        normalized_digest: "a".repeat(64),
        grant_ref: "grant-1".into(),
    }
}
#[test]
fn wire_roundtrip_rejects_malformed_integer_and_unknown_fields() {
    let a = request("task", "delivery");
    let value = serde_json::to_value(&a).unwrap();
    assert_eq!(value["task_id"], "task");
    assert_eq!(
        serde_json::from_value::<Admission>(value.clone()).unwrap(),
        a
    );
    for bad in [
        serde_json::json!(true),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("1"),
    ] {
        let mut v = value.clone();
        v["revision"] = bad;
        assert!(serde_json::from_value::<Admission>(v).is_err());
    }
    let mut v = value;
    v["unexpected"] = serde_json::json!("x");
    assert!(serde_json::from_value::<Admission>(v).is_err());
}
#[test]
fn known_duplicates_reconcile_at_capacity_without_eviction_or_new_delivery() {
    let mut state = RegistryState::default();
    let first = request("task-0", "delivery-0");
    for i in 0..CAPACITY {
        let a = request(&format!("task-{i}"), &format!("delivery-{i}"));
        assert_eq!(state.admit(a).status, "admitted");
    }
    let before = serde_json::to_value(&state).unwrap();
    assert_eq!(state.admit(first.clone()).status, "duplicate_delivery");
    let mut next = first.clone();
    next.delivery_id = "extra".into();
    assert_eq!(state.admit(next).status, "capacity");
    assert_eq!(state.admit(request("extra", "new")).status, "capacity");
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
}
#[test]
fn conflicts_revisions_and_malformed_inputs_preserve_state() {
    let mut state = RegistryState::default();
    let a = request("task", "delivery");
    let original = state.admit(a.clone());
    assert_eq!(original.job_id, Some(child_id("task", 1)));
    let before = serde_json::to_value(&state).unwrap();
    let mut conflict = a.clone();
    conflict.binding.host_id = "alternate-host".into();
    assert_eq!(state.admit(conflict).status, "delivery_conflict");
    let mut revision = a.clone();
    revision.delivery_id = "next".into();
    revision.binding.revision = 2;
    revision.predecessor_revision = Some(1);
    revision.binding.operation_id = "new-operation".into();
    assert_eq!(state.admit(revision).status, "predecessor_active");
    let mut skip = a.clone();
    skip.delivery_id = "skip".into();
    skip.binding.revision = 3;
    skip.predecessor_revision = Some(2);
    assert_eq!(state.admit(skip).status, "revision_conflict");
    let mut malformed = a.clone();
    malformed.version = "unreviewed".into();
    assert_eq!(state.admit(malformed).status, "invalid");
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    let mut another_delivery = a.clone();
    another_delivery.delivery_id = "another".into();
    assert_eq!(state.admit(another_delivery).job_id, original.job_id);
    let mut changed = a;
    changed.delivery_id = "change".into();
    changed.normalized_digest = "b".repeat(64);
    assert_eq!(state.admit(changed).status, "intent_conflict");
}
#[test]
fn default_verifier_denies_every_port() {
    let ports = Ports {
        script_directory: None,
    };
    for action in [
        PortAction::Verify,
        PortAction::ObserveHost,
        PortAction::Launch,
        PortAction::Discover,
        PortAction::Cancel,
        PortAction::Poll,
    ] {
        let reply = ports
            .invoke(&PortCall {
                admission: request("task", "delivery"),
                action,
                native_session_id: None,
            })
            .unwrap();
        assert_eq!(reply.status, PortStatus::Denied);
        assert_eq!(reply.native_session_id, None);
    }
}
#[test]
fn ids_and_digest_canonicalization_are_bounded() {
    assert!(valid_id("A:a.1_-"));
    for s in ["", "-first", "has\nnewline", "é", &"a".repeat(129)] {
        assert!(!valid_id(s));
    }
    let mut a = request("task", "delivery");
    a.normalized_digest = "A".repeat(64);
    assert!(!a.valid());
    a.normalized_digest = "a".repeat(64);
    a.binding.lease_epoch = 0;
    assert!(!a.valid());
    assert_ne!(child_id("task", 1), child_id("task", 2));
    assert_eq!(child_id("task", 1).len(), 68);
}

#[test]
fn parallel_port_calls_write_complete_process_safe_spy_records() {
    let directory = std::env::temp_dir().join(format!("core-port-spy-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let a = request("task", "delivery");
    let script = SyntheticScript {
        admission: a.clone(),
        authorized: true,
        online: true,
        native_session_id: "native-session".into(),
        launch_lost_reply: false,
        crash_after_launch: false,
        terminal: None,
        cancel_acknowledged: true,
        observe_delay_ms: 0,
    };
    std::fs::write(
        directory.join("script.json"),
        serde_json::to_vec(&vec![script]).unwrap(),
    )
    .unwrap();
    let ports = std::sync::Arc::new(Ports {
        script_directory: Some(directory.clone()),
    });
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let ports = ports.clone();
            let a = a.clone();
            std::thread::spawn(move || {
                for _ in 0..25 {
                    assert_eq!(
                        ports
                            .invoke(&PortCall {
                                admission: a.clone(),
                                action: PortAction::Verify,
                                native_session_id: None
                            })
                            .unwrap()
                            .status,
                        PortStatus::Verified
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let spy = std::fs::read_to_string(directory.join("calls.jsonl")).unwrap();
    assert_eq!(spy.lines().count(), 200);
    for line in spy.lines() {
        let call: PortCall = serde_json::from_str(line).unwrap();
        assert_eq!(call.admission, a);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn any_prior_operation_id_reuse_is_rejected_after_terminal_revisions() {
    let mut state = RegistryState::default();
    let mut a = request("task", "delivery-1");
    assert_eq!(state.admit(a.clone()).status, "admitted");
    // Synthetic state reconstruction tests transition rules, not store durability.
    fn terminal(state: RegistryState, a: &Admission) -> RegistryState {
        let mut json = serde_json::to_value(state).unwrap();
        json["terminal"][child_id(&a.binding.task_id, a.binding.revision)] =
            serde_json::to_value(OperationSnapshot {
                binding: a.binding.clone(),
                state: OperationState::ObservedCompleted,
                cancellation: CancellationState::NotRequested,
                native_session_id: Some("observed-native".into()),
            })
            .unwrap();
        serde_json::from_value(json).unwrap()
    }
    state = terminal(state, &a);
    a.binding.revision = 2;
    a.binding.operation_id = "op-2".into();
    a.delivery_id = "delivery-2".into();
    a.predecessor_revision = Some(1);
    assert_eq!(state.admit(a.clone()).status, "admitted");
    state = terminal(state, &a);
    a.binding.revision = 3;
    a.binding.operation_id = "op-task".into();
    a.delivery_id = "delivery-3".into();
    a.predecessor_revision = Some(2);
    let before = serde_json::to_value(&state).unwrap();
    assert_eq!(state.admit(a.clone()).status, "operation_conflict");
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    a.binding.operation_id = "op-3".into();
    assert_eq!(state.admit(a).status, "admitted");
}
