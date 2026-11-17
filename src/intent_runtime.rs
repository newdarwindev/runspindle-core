//! A distinct workflow/activity family; no legacy workflow is reinterpreted.
use crate::{
    authenticated::{AuthenticatedCall, AuthorityCheck},
    authority::{Action, LocalHelper},
    intent::{self, IntentAdmission, IntentBatch, Kind},
    runtime::*,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    WorkflowContext, WorkflowContextView, WorkflowResult,
};
pub const QUEUE: &str = "runspindle-intent-v1";
const CAPACITY: usize = intent::EXECUTABLE_CAPACITY;

pub fn workflow_id(value: &IntentAdmission) -> anyhow::Result<String> {
    let a = value.admission()?;
    Ok(format!(
        "intent-{}",
        intent::digest(&crate::envelope::canonical(&[
            &value.event.source.app_id,
            &value.event.source.account_id,
            &value.event.source.workspace_id,
            &a.binding.task_id,
        ])?)
    ))
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IntentState {
    pub history: Vec<IntentAdmission>,
    pub deliveries: BTreeMap<String, String>,
    pub pending: Option<String>,
    pub terminal_revision: Option<u64>,
    pub tombstone: bool,
    pub superseded_without_execution: Vec<u64>,
}
impl IntentState {
    pub fn install_batch(&mut self, batch: IntentBatch, active: bool) -> String {
        if batch.validate().is_err() {
            return "invalid".into();
        }
        if batch.intents.len() < self.history.len() {
            return "stale".into();
        }
        if !batch.intents.starts_with(&self.history) {
            return "history_conflict".into();
        }
        if batch.intents.len() == self.history.len() {
            return "duplicate".into();
        }
        if self.tombstone {
            return "deleted".into();
        }
        let latest = batch.intents.last().expect("validated nonempty");
        if active && latest.event.kind != Kind::Delete {
            return "predecessor_active".into();
        }
        for value in batch.intents.iter().skip(self.history.len()) {
            self.deliveries.insert(
                value.event.event_id.clone(),
                intent::digest(&value.encode().expect("validated")),
            );
            if value.event.revision != latest.event.revision && value.event.kind != Kind::Delete {
                self.superseded_without_execution.push(value.event.revision);
            }
        }
        if let Some(pending) = &self.pending {
            let pending = IntentAdmission::decode(pending.as_bytes()).expect("retained");
            if pending.event.revision != latest.event.revision {
                self.superseded_without_execution
                    .push(pending.event.revision);
            }
        }
        self.tombstone = latest.event.kind == Kind::Delete;
        self.pending = if self.tombstone {
            None
        } else {
            Some(String::from_utf8(latest.encode().expect("validated")).expect("utf8"))
        };
        self.history = batch.intents;
        "admitted".into()
    }
    /// Caller must verify current authority before entering this synchronous decision.
    pub fn admit(&mut self, value: IntentAdmission) -> String {
        let Ok(wire) = value.encode() else {
            return "invalid".into();
        };
        let hash = intent::digest(&wire);
        if let Some(old) = self.deliveries.get(&value.event.event_id) {
            return if old == &hash {
                "duplicate"
            } else {
                "delivery_conflict"
            }
            .into();
        }
        if self.tombstone {
            return "deleted".into();
        }
        // One final tombstone remains possible after executable capacity is full.
        if self.history.len() >= CAPACITY && value.event.kind != Kind::Delete {
            return "capacity".into();
        }
        if let Some(old) = self.history.last() {
            if value.event.source != old.event.source {
                return "source_conflict".into();
            }
            let Some(prior) = &value.event.previous else {
                return "revision_conflict".into();
            };
            if prior.request != old.scoped.request
                || prior.wire_sha256
                    != intent::digest(&old.encode().expect("retained validated intent"))
            {
                return "revision_conflict".into();
            }
            if value.event.kind == Kind::Edit && self.terminal_revision != Some(old.event.revision)
            {
                return "predecessor_active".into();
            }
        } else if value.event.kind != Kind::Create {
            return "missing_predecessor".into();
        }
        if self.history.iter().any(|old| {
            old.scoped.request.binding.operation_id == value.scoped.request.binding.operation_id
        }) {
            return "operation_conflict".into();
        }
        self.deliveries.insert(value.event.event_id.clone(), hash);
        if value.event.kind == Kind::Delete {
            self.tombstone = true;
            self.pending = None;
        } else {
            self.pending = Some(String::from_utf8(wire).expect("canonical utf8"));
        }
        self.history.push(value);
        "admitted".into()
    }
}
pub struct IntentPorts {
    pub helper: Option<LocalHelper>,
    pub script_directory: Option<PathBuf>,
}
impl IntentPorts {
    pub fn authorized(&self, check: &AuthorityCheck) -> bool {
        let latest = if let Ok(batch) = IntentBatch::decode(check.wire.as_bytes()) {
            let last = batch.intents.last().expect("validated");
            if last.event.kind != Kind::Delete
                || !matches!(check.action, Action::Cancel | Action::Reconcile)
            {
                return false;
            }
            last.encode().ok()
        } else {
            Some(check.wire.as_bytes().to_vec())
        };
        latest.is_some_and(|wire| {
            self.helper
                .as_ref()
                .is_some_and(|h| h.check_intent(&wire, check.action).is_ok())
        })
    }
    pub fn invoke(&self, input: &AuthenticatedCall) -> anyhow::Result<PortReply> {
        let denied = || PortReply {
            binding: input.call.admission.binding.clone(),
            status: PortStatus::Denied,
            native_session_id: None,
        };
        let batch = IntentBatch::decode(input.wire.as_bytes()).ok();
        let value = if let Some(batch) = &batch {
            batch.intents.last().expect("validated").clone()
        } else {
            let Ok(v) = IntentAdmission::decode(input.wire.as_bytes()) else {
                return Ok(denied());
            };
            v
        };
        let a = if value.event.kind == Kind::Delete {
            if !matches!(input.action, Action::Cancel | Action::Reconcile) {
                return Ok(denied());
            }
            if let Some(batch) = &batch {
                if !batch
                    .intents
                    .iter()
                    .take(batch.intents.len() - 1)
                    .any(|ancestor| {
                        ancestor.admission().ok().as_ref() == Some(&input.call.admission)
                    })
                {
                    return Ok(denied());
                }
                input.call.admission.clone()
            } else {
                let Some(prior) = value.event.previous else {
                    return Ok(denied());
                };
                let crate::envelope::Message::Admission(a) = prior.request.message()? else {
                    return Ok(denied());
                };
                a
            }
        } else {
            value.admission()?
        };
        let matches = match input.call.action {
            PortAction::Verify => matches!(
                input.action,
                Action::Dispatch | Action::Cancel | Action::Reconcile
            ),
            PortAction::ObserveHost | PortAction::Launch => input.action == Action::Dispatch,
            PortAction::Discover | PortAction::Poll => input.action == Action::Reconcile,
            PortAction::Cancel => input.action == Action::Cancel,
        };
        if a != input.call.admission
            || !matches
            || !self.authorized(&AuthorityCheck {
                wire: input.wire.clone(),
                action: input.action,
            })
        {
            return Ok(denied());
        }
        Ports {
            script_directory: self.script_directory.clone(),
        }
        .invoke(&input.call)
    }
}
#[activities]
impl IntentPorts {
    #[activity]
    pub async fn verify_intent(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        check: AuthorityCheck,
    ) -> Result<bool, ActivityError> {
        Ok(self.authorized(&check))
    }
    #[activity]
    pub async fn invoke_intent(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        input: AuthenticatedCall,
    ) -> Result<PortReply, ActivityError> {
        if input.call.action == PortAction::Launch {
            if let Some(dir) = &self.script_directory {
                wait_barrier(dir, &input.call, "launch").await?;
            }
        }
        let reply = self.invoke(&input)?;
        if input.call.action == PortAction::ObserveHost && reply.status == PortStatus::Online {
            if let Some(dir) = &self.script_directory {
                wait_barrier(dir, &input.call, "observe").await?;
            }
        }
        Ok(reply)
    }
}
async fn wait_barrier(dir: &std::path::Path, call: &PortCall, phase: &str) -> anyhow::Result<()> {
    let job = child_id(
        &call.admission.binding.task_id,
        call.admission.binding.revision,
    );
    if !dir.join(format!("intent-{phase}-hold-{job}")).exists() {
        return Ok(());
    }
    let release = dir.join(format!("intent-{phase}-release-{job}.json"));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
    while !release.exists() {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "synthetic intent barrier deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(release)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= 1_048_576
            && serde_json::from_slice::<Binding>(&bytes)? == call.admission.binding,
        "synthetic intent barrier binding"
    );
    Ok(())
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub state: IntentState,
    pub operation: Option<OperationSnapshot>,
}
#[workflow]
#[derive(Default)]
pub struct IntentWorkflowV1 {
    state: IntentState,
    operation: Option<OperationWorkflow>,
    wire: String,
}
impl OperationAccess for IntentWorkflowV1 {
    fn operation(&self) -> &OperationWorkflow {
        self.operation.as_ref().expect("active operation")
    }
    fn operation_mut(&mut self) -> &mut OperationWorkflow {
        self.operation.as_mut().expect("active operation")
    }
    fn intent_wire(&self) -> Option<String> {
        Some(self.wire.clone())
    }
}
#[workflow_methods]
impl IntentWorkflowV1 {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<()> {
        loop {
            ctx.wait_condition(|s| s.state.pending.is_some()).await?;
            ctx.state_mut(|s| {
                s.wire = s.state.pending.take().expect("pending intent");
                let a = IntentAdmission::decode(s.wire.as_bytes())
                    .and_then(|v| v.admission())
                    .expect("retained admission");
                s.operation = Some(OperationWorkflow {
                    snapshot: OperationSnapshot {
                        binding: a.binding.clone(),
                        state: OperationState::Scheduled,
                        cancellation: CancellationState::NotRequested,
                        native_session_id: None,
                    },
                    admission: a,
                });
            });
            let result = run_operation(ctx).await?;
            ctx.state_mut(|s| s.state.terminal_revision = Some(result.binding.revision));
        }
    }
    #[update]
    pub async fn admit(ctx: &mut WorkflowContext<Self>, wire: String) -> String {
        let Ok(value) = IntentAdmission::decode(wire.as_bytes()) else {
            return "invalid".into();
        };
        if workflow_id(&value).ok().as_deref() != Some(ctx.workflow_id()) {
            return "workflow_identity".into();
        }
        for action in if value.event.kind == Kind::Delete {
            vec![Action::Admit, Action::Cancel]
        } else {
            vec![Action::Admit]
        } {
            if !ctx
                .execute_activity(
                    IntentPorts::verify_intent,
                    AuthorityCheck {
                        wire: wire.clone(),
                        action,
                    },
                    single_attempt(),
                )
                .await
                .unwrap_or(false)
            {
                return "denied".into();
            }
        }
        // No await between current-state ordering checks, tombstone and cancel request.
        ctx.state_mut(|s| {
            let deleted = value.event.kind == Kind::Delete;
            let result = s.state.admit(value);
            if result == "admitted" && deleted {
                if let Some(op) = s.operation.as_mut() {
                    if !op.snapshot.state.terminal() {
                        s.wire = wire;
                        op.snapshot.cancellation = CancellationState::Requested;
                    }
                }
            }
            result
        })
    }
    #[update]
    pub async fn admit_batch(ctx: &mut WorkflowContext<Self>, wire: String) -> String {
        let Ok(batch) = IntentBatch::decode(wire.as_bytes()) else {
            return "invalid".into();
        };
        let latest = batch.intents.last().expect("validated");
        if workflow_id(latest).ok().as_deref() != Some(ctx.workflow_id()) {
            return "workflow_identity".into();
        }
        let deleted = latest.event.kind == Kind::Delete;
        let latest_wire = String::from_utf8(latest.encode().expect("validated")).expect("utf8");
        for action in if deleted {
            vec![Action::Admit, Action::Cancel]
        } else {
            vec![Action::Admit]
        } {
            if !ctx
                .execute_activity(
                    IntentPorts::verify_intent,
                    AuthorityCheck {
                        wire: latest_wire.clone(),
                        action,
                    },
                    single_attempt(),
                )
                .await
                .unwrap_or(false)
            {
                return "denied".into();
            }
        }
        ctx.state_mut(|s| {
            let active = s
                .operation
                .as_ref()
                .is_some_and(|o| !o.snapshot.state.terminal());
            let result = s.state.install_batch(batch, active);
            if result == "admitted" && deleted && active {
                s.wire = wire;
                s.operation.as_mut().expect("active").snapshot.cancellation =
                    CancellationState::Requested;
            }
            result
        })
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> Snapshot {
        Snapshot {
            state: self.state.clone(),
            operation: self.operation.as_ref().map(|o| o.snapshot.clone()),
        }
    }
}
