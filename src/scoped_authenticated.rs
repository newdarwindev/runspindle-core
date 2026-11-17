//! Distinct workflow registrations; historical authenticated V1 histories stay unchanged.
use crate::{
    authenticated::{AuthenticatedPorts, AuthorityCheck},
    authority::Action,
    runtime::*,
    scoped_authority::ScopedAdmission,
};
use std::collections::BTreeMap;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ChildWorkflowOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
pub const QUEUE: &str = "runspindle-scoped-authenticated-v1";
#[workflow]
#[derive(Default)]
pub struct ScopedRegistryV1 {
    state: RegistryState,
    wires: BTreeMap<String, String>,
}
#[workflow_methods]
impl ScopedRegistryV1 {
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
                    ScopedOperationV1::run,
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
        let Ok(a) = ScopedAdmission::decode(wire.as_bytes()).and_then(|s| s.admission()) else {
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
pub struct ScopedOperationV1 {
    operation: Option<OperationWorkflow>,
    wire: String,
}
impl OperationAccess for ScopedOperationV1 {
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
impl ScopedOperationV1 {
    #[init]
    fn new(_ctx: &WorkflowContextView, wire: String) -> Self {
        let operation = ScopedAdmission::decode(wire.as_bytes())
            .and_then(|s| s.admission())
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
