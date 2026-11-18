#![cfg(feature = "host-runtime")]
use runspindle_core::{integration::*, runtime::*};
use runspindle_host::lifecycle::{Cancellation, Execution};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "core-host-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn request() -> HostRequest {
    HostRequest {
        version: runspindle_core::integration::VERSION.into(),
        admission: Admission {
            version: runspindle_core::runtime::VERSION.into(),
            delivery_namespace: "namespace".into(),
            delivery_id: "delivery".into(),
            binding: Binding {
                task_id: "task".into(),
                revision: 1,
                operation_id: "operation".into(),
                policy_revision: "policy".into(),
                host_id: "selected-host".into(),
                workspace_id: "workspace".into(),
                lease_epoch: 1,
            },
            predecessor_revision: None,
            normalized_digest: "a".repeat(64),
            grant_ref: "grant".into(),
        },
    }
}
fn script() -> NativeScript {
    NativeScript {
        request: request(),
        authorized: true,
        online: true,
        native_session_id: "actual-observed-native".into(),
        crash_after_launch: false,
        lost_reply: true,
        terminal: None,
        crash_before_effect: false,
        discovery_count: None,
        session_override: None,
        sequence_override: None,
        observed_at_override: None,
        reconcile_barrier: false,
        authority_valid_until_ms: None,
        crash_after_prepare: false,
    }
}
fn write(t: &Temp, s: &NativeScript) {
    std::fs::write(
        t.0.join("native-script.json"),
        serde_json::to_vec(s).unwrap(),
    )
    .unwrap();
}
fn call(a: HostAction) -> HostCall {
    HostCall {
        request: request(),
        action: a,
    }
}
#[test]
fn expired_or_revoked_grant_cannot_prepare_dispatch_or_restore_authority() {
    let t = Temp::new();
    let mut s = script();
    let ports = HostPorts {
        directory: Some(t.0.clone()),
    };
    s.authority_valid_until_ms = Some(100);
    assert!(s.authorized_at(99));
    assert!(!s.authorized_at(100));
    write(&t, &s);
    assert!(ports.invoke(call(HostAction::Prepare)).is_err());
    assert!(!t.0.join("native-peer.sqlite").exists());
    s.authority_valid_until_ms = None;
    write(&t, &s);
    ports.invoke(call(HostAction::Prepare)).unwrap();
    s.authority_valid_until_ms = Some(0);
    write(&t, &s);
    assert!(ports.invoke(call(HostAction::Dispatch)).is_err());
    let db = rusqlite::Connection::open(t.0.join("native-peer.sqlite")).unwrap();
    let calls: u64 = db
        .query_row(
            "SELECT count(*) FROM calls WHERE action='launch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(calls, 0);
}
#[test]
fn invalid_version_default_denial_and_complete_binding_mapping() {
    let mut r = request();
    let b = &r.admission.binding;
    let h = host_binding(b);
    assert_eq!(
        (
            &h.task_id,
            h.revision,
            &h.operation_id,
            &h.policy_revision,
            &h.host_id,
            &h.workspace_id,
            h.lease_epoch
        ),
        (
            &b.task_id,
            b.revision,
            &b.operation_id,
            &b.policy_revision,
            &b.host_id,
            &b.workspace_id,
            b.lease_epoch
        )
    );
    assert!(HostPorts { directory: None }
        .invoke(call(HostAction::Prepare))
        .is_err());
    r.version = "future".into();
    assert!(HostPorts { directory: None }
        .invoke(HostCall {
            request: r,
            action: HostAction::Prepare
        })
        .is_err());
}
#[test]
fn real_host_rejects_ambiguous_stale_rebound_observations_and_preserves_terminal() {
    let t = Temp::new();
    let mut s = script();
    write(&t, &s);
    let p = HostPorts {
        directory: Some(t.0.clone()),
    };
    assert_eq!(
        p.invoke(call(HostAction::Prepare)).unwrap().execution,
        Execution::Queued
    );
    assert_eq!(
        p.invoke(call(HostAction::Dispatch)).unwrap().execution,
        Execution::Unknown
    );
    assert_eq!(
        p.invoke(call(HostAction::Dispatch)).unwrap().execution,
        Execution::Unknown
    );
    let recovered = p.invoke(call(HostAction::Reconcile)).unwrap();
    assert_eq!(
        recovered.native_session_id.as_deref(),
        Some("actual-observed-native")
    );
    s.session_override = Some("other-session".into());
    write(&t, &s);
    assert!(p.invoke(call(HostAction::Reconcile)).is_err());
    s.session_override = None;
    s.sequence_override = Some(1);
    write(&t, &s);
    assert!(p.invoke(call(HostAction::Reconcile)).is_err());
    s.sequence_override = None;
    s.observed_at_override = Some(0);
    write(&t, &s);
    assert!(p.invoke(call(HostAction::Reconcile)).is_err());
    s.observed_at_override = None;
    s.discovery_count = Some(2);
    write(&t, &s);
    assert_eq!(
        p.invoke(call(HostAction::Reconcile)).unwrap().execution,
        Execution::Unknown
    );
    s.discovery_count = None;
    write(&t, &s);
    assert_eq!(
        p.invoke(call(HostAction::Cancel)).unwrap().cancellation,
        Cancellation::Acknowledged
    );
    s.terminal = Some(PortStatus::Completed);
    write(&t, &s);
    let terminal = p.invoke(call(HostAction::Reconcile)).unwrap();
    assert!(matches!(
        terminal.execution,
        Execution::Terminal(runspindle_host::lifecycle::Terminal::Completed)
    ));
    assert_eq!(terminal.cancellation, Cancellation::ObservedTerminal);
    let peer = rusqlite::Connection::open(t.0.join("native-peer.sqlite")).unwrap();
    let launches: u64 = peer
        .query_row(
            "SELECT count(*) FROM calls WHERE action='launch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(launches, 1);
}
#[test]
fn revoked_authority_and_changed_epoch_have_no_effects() {
    let t = Temp::new();
    let mut s = script();
    s.authorized = false;
    write(&t, &s);
    let p = HostPorts {
        directory: Some(t.0.clone()),
    };
    assert!(p.invoke(call(HostAction::Prepare)).is_err());
    assert!(!t.0.join("host.sqlite").exists());
    s.authorized = true;
    write(&t, &s);
    let mut c = call(HostAction::Prepare);
    c.request.admission.binding.lease_epoch = 2;
    assert!(p.invoke(c).is_err());
    assert!(!t.0.join("host.sqlite").exists());
}

#[test]
fn accepted_cancellation_survives_a_terminal_observation_return() {
    use runspindle_host::lifecycle as h;
    let mut snapshot = h::Snapshot {
        binding: host_binding(&request().admission.binding),
        execution: h::Execution::Terminal(h::Terminal::Completed),
        cancellation: h::Cancellation::None,
        native_session_id: Some("actual-native".into()),
        last_sequence: 2,
        observed_at_ms: Some(1),
        cancel_dispatched: false,
    };
    preserve_cancellation(&mut snapshot, true);
    assert_eq!(
        snapshot.execution,
        h::Execution::Terminal(h::Terminal::Completed)
    );
    assert_eq!(snapshot.cancellation, h::Cancellation::ObservedTerminal);
    assert!(!snapshot.cancel_dispatched);
}

#[test]
fn concurrent_stale_actor_never_inherits_current_launch_or_cancel() {
    let t = Temp::new();
    let mut script = script();
    script.request.admission.binding.lease_epoch = 2;
    write(&t, &script);
    let ports = HostPorts {
        directory: Some(t.0.clone()),
    };
    let current = script.request.clone();
    ports
        .invoke(HostCall {
            request: current.clone(),
            action: HostAction::Prepare,
        })
        .unwrap();
    for action in [HostAction::Dispatch, HostAction::Cancel] {
        let workers = [request(), current.clone()]
            .into_iter()
            .map(|request| {
                let directory = t.0.clone();
                let (start, ready) = std::sync::mpsc::channel();
                let action = action.clone();
                let worker = std::thread::spawn(move || {
                    ready
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    HostPorts {
                        directory: Some(directory),
                    }
                    .invoke(HostCall { request, action })
                });
                (start, worker)
            })
            .collect::<Vec<_>>();
        for (start, _) in &workers {
            start.send(()).unwrap();
        }
        // Join both actors before any expected mutant failure can unwind and
        // delete the shared temporary directory.
        let joined = workers
            .into_iter()
            .map(|(_, worker)| worker.join().unwrap())
            .collect::<Vec<_>>();
        let mut results = joined.into_iter();
        assert!(
            results.next().unwrap().is_err(),
            "stale actor inherited current authority"
        );
        let current_result = results.next().unwrap().unwrap();
        assert_eq!(current_result.binding.lease_epoch, 2);
        let peer = rusqlite::Connection::open(t.0.join("native-peer.sqlite")).unwrap();
        let count = |action: &str| -> u64 {
            peer.query_row(
                "SELECT count(*) FROM calls WHERE action=?1",
                [action],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(count("launch"), 1);
        assert_eq!(
            count("cancel"),
            if matches!(action, HostAction::Cancel) {
                1
            } else {
                0
            }
        );
        // Lost launch acknowledgement is reconciled, not retried. Reconnection
        // must keep the observed native identity and the current epoch.
        let snapshot = ports
            .invoke(HostCall {
                request: current.clone(),
                action: HostAction::Reconcile,
            })
            .unwrap();
        assert_eq!(
            snapshot.native_session_id.as_deref(),
            Some("actual-observed-native")
        );
        assert_eq!(snapshot.binding.lease_epoch, 2);
    }
}
