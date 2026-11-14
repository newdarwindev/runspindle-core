//! Versioned offline authenticated workflow entrypoints. Assertions and private
//! keys never enter workflow inputs/results; histories retain grant references.
use crate::{
    authority::{self, Action, Authority, LocalHelper},
    runtime::*,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ChildWorkflowOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
};

pub const QUEUE: &str = "runspindle-authenticated-v1";
pub struct AuthenticatedPorts {
    pub helper: Option<LocalHelper>,
    pub script_directory: Option<PathBuf>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheck {
    pub wire: String,
    pub action: Action,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedCall {
    pub wire: String,
    pub call: PortCall,
    pub action: Action,
}
impl AuthenticatedPorts {
    pub fn authorized(&self, check: &AuthorityCheck) -> bool {
        self.helper
            .as_ref()
            .is_some_and(|h| h.verify(check.wire.as_bytes(), check.action))
    }
    pub fn invoke(&self, input: &AuthenticatedCall) -> anyhow::Result<PortReply> {
        let denied = || PortReply {
            binding: input.call.admission.binding.clone(),
            status: PortStatus::Denied,
            native_session_id: None,
        };
        let Ok(admission) = authority::request(input.wire.as_bytes()) else {
            return Ok(denied());
        };
        let action_matches = match input.call.action {
            PortAction::Verify => matches!(
                input.action,
                Action::Dispatch | Action::Reconcile | Action::Cancel
            ),
            PortAction::ObserveHost | PortAction::Launch => input.action == Action::Dispatch,
            PortAction::Discover | PortAction::Poll => input.action == Action::Reconcile,
            PortAction::Cancel => input.action == Action::Cancel,
        };
        if admission != input.call.admission || !action_matches {
            return Ok(denied());
        }
        // Last guard inside the activity, immediately before crossing the adapter
        // seam. A replayed earlier verifier result cannot authorize this invocation.
        if !self.authorized(&AuthorityCheck {
            wire: input.wire.clone(),
            action: input.action,
        }) {
            return Ok(denied());
        }
        Ports {
            script_directory: self.script_directory.clone(),
        }
        .invoke(&input.call)
    }
}
#[activities]
impl AuthenticatedPorts {
    #[activity]
    pub async fn verify_authority(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        check: AuthorityCheck,
    ) -> Result<bool, ActivityError> {
        Ok(self.authorized(&check))
    }
    #[activity]
    pub async fn invoke_authenticated(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        call: AuthenticatedCall,
    ) -> Result<PortReply, ActivityError> {
        Ok(self.invoke(&call)?)
    }
}
#[workflow]
#[derive(Default)]
pub struct AuthenticatedRegistryV1 {
    state: RegistryState,
    wires: BTreeMap<String, String>,
}
#[workflow_methods]
impl AuthenticatedRegistryV1 {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<()> {
        loop {
            ctx.wait_condition(|s| !s.state.pending.is_empty()).await?;
            let (job, wire, a) = ctx.state_mut(|s| {
                let job = s.state.pending.remove(0);
                s.state.active = Some(job.clone());
                (
                    job.clone(),
                    s.wires[&job].clone(),
                    s.state.revisions[&job].clone(),
                )
            });
            let allowed = ctx
                .execute_activity(
                    AuthenticatedPorts::verify_authority,
                    AuthorityCheck {
                        wire: wire.clone(),
                        action: Action::Dispatch,
                    },
                    single_attempt(),
                )
                .await
                .unwrap_or(false);
            let snapshot = if allowed {
                ctx.start_child_workflow(
                    AuthenticatedOperationV1::run,
                    wire,
                    ChildWorkflowOptions::workflow_id(job.clone()),
                )
                .await?
                .result()
                .await?
            } else {
                OperationSnapshot {
                    binding: a.binding,
                    state: OperationState::ObservedFailed,
                    cancellation: CancellationState::NotRequested,
                    native_session_id: None,
                }
            };
            ctx.state_mut(|s| {
                s.state.terminal.insert(job, snapshot);
                s.state.active = None;
            });
        }
    }
    #[update]
    pub async fn admit(ctx: &mut WorkflowContext<Self>, wire: String) -> AdmissionReply {
        let Ok(a) = authority::request(wire.as_bytes()) else {
            return AdmissionReply::rejected("invalid");
        };
        let allowed = ctx
            .execute_activity(
                AuthenticatedPorts::verify_authority,
                AuthorityCheck {
                    wire: wire.clone(),
                    action: Action::Admit,
                },
                single_attempt(),
            )
            .await
            .unwrap_or(false);
        if !allowed {
            return AdmissionReply::rejected("denied");
        }
        // Reuse the durable registry's conflict and retention logic. Concurrent
        // verifier completions recheck identity in the same workflow activation.
        ctx.state_mut(|s| {
            let reply = s.state.admit(a);
            if reply.status == "admitted" {
                s.wires
                    .insert(reply.job_id.clone().expect("admitted job"), wire);
            }
            reply
        })
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> RegistryState {
        self.state.clone()
    }
}
#[workflow]
pub struct AuthenticatedOperationV1 {
    operation: Option<OperationWorkflow>,
    wire: String,
}
impl OperationAccess for AuthenticatedOperationV1 {
    fn operation(&self) -> &OperationWorkflow {
        self.operation.as_ref().expect("validated workflow input")
    }
    fn operation_mut(&mut self) -> &mut OperationWorkflow {
        self.operation.as_mut().expect("validated workflow input")
    }
    fn authority_wire(&self) -> Option<String> {
        Some(self.wire.clone())
    }
}
#[workflow_methods]
impl AuthenticatedOperationV1 {
    #[init]
    fn new(_ctx: &WorkflowContextView, wire: String) -> Self {
        let operation =
            authority::request(wire.as_bytes())
                .ok()
                .map(|admission| OperationWorkflow {
                    snapshot: OperationSnapshot {
                        binding: admission.binding.clone(),
                        state: OperationState::Scheduled,
                        cancellation: CancellationState::NotRequested,
                        native_session_id: None,
                    },
                    admission,
                });
        Self { operation, wire }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<OperationSnapshot> {
        if ctx.state(|s| s.operation.is_none()) {
            return Err(temporalio_sdk::ApplicationFailure::non_retryable(
                "invalid authenticated workflow input",
            )
            .into());
        }
        run_operation(ctx).await
    }
    #[update]
    pub async fn request_cancel(ctx: &mut WorkflowContext<Self>, wire: String) -> bool {
        if !ctx.state(|s| {
            s.wire == wire
                && s.operation
                    .as_ref()
                    .is_some_and(|o| !o.snapshot.state.terminal())
        }) {
            return false;
        }
        let allowed = ctx
            .execute_activity(
                AuthenticatedPorts::verify_authority,
                AuthorityCheck {
                    wire,
                    action: Action::Cancel,
                },
                single_attempt(),
            )
            .await
            .unwrap_or(false);
        if !allowed {
            return false;
        }
        ctx.state_mut(|s| {
            let snapshot = &mut s.operation_mut().snapshot;
            if snapshot.state.terminal() {
                return false;
            }
            if snapshot.cancellation == CancellationState::NotRequested {
                snapshot.cancellation = CancellationState::Requested;
            }
            true
        })
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> Option<OperationSnapshot> {
        self.operation.as_ref().map(|o| o.snapshot.clone())
    }
}
