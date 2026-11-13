//! Bounded Temporal development runtime. Operational activation remains disabled.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
use temporalio_common::RetryPolicy;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

pub const VERSION: &str = "runspindle.runtime.v1";
pub const QUEUE: &str = "runspindle-runtime-v1";
pub const CAPACITY: usize = 1024;

pub fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn valid_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
pub fn child_id(task: &str, revision: u64) -> String {
    format!(
        "job-{:x}",
        Sha256::digest(format!("{task}\n{revision}").as_bytes())
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub task_id: String,
    pub revision: u64,
    pub operation_id: String,
    pub policy_revision: String,
    pub host_id: String,
    pub workspace_id: String,
    pub lease_epoch: u64,
}
impl Binding {
    pub fn valid(&self) -> bool {
        self.revision > 0
            && self.lease_epoch > 0
            && [
                &self.task_id,
                &self.operation_id,
                &self.policy_revision,
                &self.host_id,
                &self.workspace_id,
            ]
            .iter()
            .all(|s| valid_id(s))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub version: String,
    pub delivery_namespace: String,
    pub delivery_id: String,
    #[serde(flatten)]
    pub binding: Binding,
    pub predecessor_revision: Option<u64>,
    pub normalized_digest: String,
    pub grant_ref: String,
}
impl Admission {
    pub fn valid(&self) -> bool {
        self.version == VERSION
            && self.binding.valid()
            && valid_id(&self.delivery_namespace)
            && valid_id(&self.delivery_id)
            && valid_id(&self.grant_ref)
            && valid_digest(&self.normalized_digest)
            && (if self.binding.revision == 1 {
                self.predecessor_revision.is_none()
            } else {
                self.predecessor_revision == self.binding.revision.checked_sub(1)
            })
    }
    fn same_intent(&self, other: &Self) -> bool {
        self.binding == other.binding
            && self.predecessor_revision == other.predecessor_revision
            && self.normalized_digest == other.normalized_digest
            && self.grant_ref == other.grant_ref
            && self.version == other.version
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Scheduled,
    Acknowledged,
    Unknown,
    ObservedCompleted,
    ObservedFailed,
    ObservedCancelled,
}
impl OperationState {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::ObservedCompleted | Self::ObservedFailed | Self::ObservedCancelled
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CancellationState {
    NotRequested,
    Requested,
    Acknowledged,
    ObservedTerminal,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationSnapshot {
    pub binding: Binding,
    pub state: OperationState,
    pub cancellation: CancellationState,
    pub native_session_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmissionReply {
    pub status: String,
    pub job_id: Option<String>,
}
impl AdmissionReply {
    fn rejected(why: &str) -> Self {
        Self {
            status: why.into(),
            job_id: None,
        }
    }
    fn accepted(status: &str, a: &Admission) -> Self {
        Self {
            status: status.into(),
            job_id: Some(child_id(&a.binding.task_id, a.binding.revision)),
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryState {
    deliveries: BTreeMap<String, Admission>,
    revisions: BTreeMap<String, Admission>,
    terminal: BTreeMap<String, OperationSnapshot>,
    pending: Vec<String>,
    active: Option<String>,
}
impl RegistryState {
    fn key(a: &Admission) -> String {
        format!("{}\n{}", a.delivery_namespace, a.delivery_id)
    }
    /// Called in a single workflow activation after verifier completion; rechecks all
    /// conflicts here because concurrent verifier activities may finish in any order.
    pub fn admit(&mut self, a: Admission) -> AdmissionReply {
        if !a.valid() {
            return AdmissionReply::rejected("invalid");
        }
        let delivery = Self::key(&a);
        if let Some(old) = self.deliveries.get(&delivery) {
            return if old == &a {
                AdmissionReply::accepted("duplicate_delivery", old)
            } else {
                AdmissionReply::rejected("delivery_conflict")
            };
        }
        if self.deliveries.len() >= CAPACITY {
            return AdmissionReply::rejected("capacity");
        }
        let job = child_id(&a.binding.task_id, a.binding.revision);
        if let Some(old) = self.revisions.get(&job) {
            if !old.same_intent(&a) {
                return AdmissionReply::rejected("intent_conflict");
            }
            let reply = AdmissionReply::accepted("existing_revision", old);
            self.deliveries.insert(delivery, a);
            return reply;
        }
        if self.revisions.len() >= CAPACITY {
            return AdmissionReply::rejected("capacity");
        }
        let latest = self
            .revisions
            .values()
            .filter(|r| r.binding.task_id == a.binding.task_id)
            .max_by_key(|r| r.binding.revision);
        if let Some(latest) = latest {
            if latest.binding.revision.checked_add(1) != Some(a.binding.revision)
                || a.predecessor_revision != Some(latest.binding.revision)
            {
                return AdmissionReply::rejected("revision_conflict");
            }
            if !self
                .terminal
                .contains_key(&child_id(&latest.binding.task_id, latest.binding.revision))
            {
                return AdmissionReply::rejected("predecessor_active");
            }
        } else if a.binding.revision != 1 {
            return AdmissionReply::rejected("revision_conflict");
        }
        if self
            .revisions
            .values()
            .any(|old| old.binding.operation_id == a.binding.operation_id)
        {
            return AdmissionReply::rejected("operation_conflict");
        }
        let reply = AdmissionReply::accepted("admitted", &a);
        // Binding and pending dispatch become one durable workflow-state decision.
        self.deliveries.insert(delivery, a.clone());
        self.revisions.insert(job.clone(), a);
        self.pending.push(job);
        reply
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PortAction {
    Verify,
    ObserveHost,
    Launch,
    Discover,
    Poll,
    Cancel,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortCall {
    pub admission: Admission,
    pub action: PortAction,
    pub native_session_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PortStatus {
    Verified,
    Denied,
    Online,
    Offline,
    Acknowledged,
    Unknown,
    Running,
    Completed,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortReply {
    pub binding: Binding,
    pub status: PortStatus,
    pub native_session_id: Option<String>,
}

/// Injected synthetic ports only. `None` is an authority verifier that always denies.
/// A real authority/host implementation is deliberately absent.
pub struct Ports {
    pub script_directory: Option<std::path::PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyntheticScript {
    pub admission: Admission,
    pub authorized: bool,
    pub online: bool,
    pub native_session_id: String,
    pub launch_lost_reply: bool,
    pub crash_after_launch: bool,
    pub terminal: Option<PortStatus>,
    pub cancel_acknowledged: bool,
    #[serde(default)]
    pub observe_delay_ms: u64,
}
fn read_bounded(path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;
    let mut data = Vec::new();
    std::fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut data)?;
    anyhow::ensure!(data.len() <= 1_048_576, "oversized input");
    Ok(data)
}
impl Ports {
    pub fn invoke(&self, call: &PortCall) -> anyhow::Result<PortReply> {
        use std::io::Write;
        let mut reply = PortReply {
            binding: call.admission.binding.clone(),
            status: PortStatus::Denied,
            native_session_id: None,
        };
        let Some(dir) = &self.script_directory else {
            return Ok(reply);
        };
        if !call.admission.valid() {
            return Ok(reply);
        }
        let data = read_bounded(&dir.join("script.json"))?;
        let scripts: Vec<SyntheticScript> = serde_json::from_slice(&data)?;
        let Some(script) = scripts.iter().find(|s| s.admission == call.admission) else {
            return Ok(reply);
        };
        let mut spy = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(dir.join("calls.jsonl"))?;
        // Activities and separate workers can overlap. One process-safe lock
        // covers the entire encoded record, including short writes and fsync.
        let mut record = serde_json::to_vec(call)?;
        record.push(b'\n');
        spy.lock()?;
        spy.write_all(&record)?;
        spy.sync_all()?;
        spy.unlock()?;
        if !script.authorized {
            return Ok(reply);
        }
        if !valid_id(&script.native_session_id) {
            return Ok(reply);
        }
        let receipt_path = dir.join(format!(
            "effect-{}.json",
            child_id(
                &call.admission.binding.task_id,
                call.admission.binding.revision
            )
        ));
        match call.action {
            PortAction::Verify => reply.status = PortStatus::Verified,
            PortAction::ObserveHost => {
                anyhow::ensure!(
                    script.observe_delay_ms <= 1000,
                    "script delay exceeds bound"
                );
                std::thread::sleep(Duration::from_millis(script.observe_delay_ms));
                reply.status = if script.online {
                    PortStatus::Online
                } else {
                    PortStatus::Offline
                }
            }
            PortAction::Launch => {
                if !script.online {
                    reply.status = PortStatus::Unknown;
                    return Ok(reply);
                }
                // Independent fake peer receipt models a durable external effect. A
                // second launch is an invariant violation, not a successful retry.
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&receipt_path)?;
                reply.status = PortStatus::Running;
                reply.native_session_id = Some(script.native_session_id.clone());
                serde_json::to_writer(&mut f, &reply)?;
                f.sync_all()?;
                std::fs::File::open(dir)?.sync_all()?;
                if script.crash_after_launch {
                    std::process::exit(93);
                }
                reply.status = if script.launch_lost_reply {
                    PortStatus::Unknown
                } else {
                    PortStatus::Acknowledged
                };
                if script.launch_lost_reply {
                    reply.native_session_id = None;
                }
            }
            PortAction::Discover | PortAction::Poll | PortAction::Cancel => {
                let Ok(bytes) = read_bounded(&receipt_path) else {
                    reply.status = PortStatus::Unknown;
                    return Ok(reply);
                };
                let receipt: PortReply = serde_json::from_slice(&bytes)?;
                if receipt.binding != call.admission.binding
                    || receipt.native_session_id.as_deref() != Some(&script.native_session_id)
                    || call
                        .native_session_id
                        .as_ref()
                        .is_some_and(|s| Some(s) != receipt.native_session_id.as_ref())
                {
                    reply.status = PortStatus::Unknown;
                    return Ok(reply);
                }
                reply.native_session_id = receipt.native_session_id;
                reply.status = if call.action == PortAction::Cancel {
                    if script.cancel_acknowledged {
                        PortStatus::Acknowledged
                    } else {
                        PortStatus::Unknown
                    }
                } else {
                    script.terminal.clone().unwrap_or(PortStatus::Running)
                };
            }
        }
        Ok(reply)
    }
}
#[activities]
impl Ports {
    #[activity]
    pub async fn invoke_port(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        call: PortCall,
    ) -> Result<PortReply, ActivityError> {
        self.invoke(&call).map_err(Into::into)
    }
}
fn single_attempt() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(2))
        .retry_policy(RetryPolicy::builder().maximum_attempts(1).build())
        .build()
}

#[workflow]
#[derive(Default)]
pub struct AdmissionRegistry {
    state: RegistryState,
}
#[workflow_methods]
impl AdmissionRegistry {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<()> {
        loop {
            ctx.wait_condition(|s| !s.state.pending.is_empty()).await?;
            let (job, admission) = ctx.state_mut(|s| {
                let job = s.state.pending.remove(0);
                s.state.active = Some(job.clone());
                (job.clone(), s.state.revisions[&job].clone())
            });
            let child = ctx
                .start_child_workflow(
                    OperationWorkflow::run,
                    admission,
                    ChildWorkflowOptions::workflow_id(job.clone()),
                )
                .await?;
            let snapshot = child.result().await?;
            ctx.state_mut(|s| {
                s.state.terminal.insert(job, snapshot);
                s.state.active = None;
            });
        }
    }
    #[update]
    pub async fn admit(ctx: &mut WorkflowContext<Self>, a: Admission) -> AdmissionReply {
        if !a.valid() {
            return AdmissionReply::rejected("invalid");
        }
        let check = ctx
            .execute_activity(
                Ports::invoke_port,
                PortCall {
                    admission: a.clone(),
                    action: PortAction::Verify,
                    native_session_id: None,
                },
                single_attempt(),
            )
            .await;
        match check {
            Ok(reply) if reply.status == PortStatus::Verified && reply.binding == a.binding => {
                ctx.state_mut(|s| s.state.admit(a))
            }
            _ => AdmissionReply::rejected("denied"),
        }
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> RegistryState {
        self.state.clone()
    }
}

#[workflow]
pub struct OperationWorkflow {
    snapshot: OperationSnapshot,
    admission: Admission,
}
#[workflow_methods]
impl OperationWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, admission: Admission) -> Self {
        Self {
            snapshot: OperationSnapshot {
                binding: admission.binding.clone(),
                state: OperationState::Scheduled,
                cancellation: CancellationState::NotRequested,
                native_session_id: None,
            },
            admission,
        }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<OperationSnapshot> {
        let a = ctx.state(|s| s.admission.clone());
        let mut launched = false;
        let mut cancel_sent = false;
        loop {
            let snapshot = ctx.state(|s| s.snapshot.clone());
            if snapshot.state.terminal() {
                return Ok(snapshot);
            }
            if snapshot.cancellation == CancellationState::Requested && !launched {
                ctx.state_mut(|s| {
                    s.snapshot.state = OperationState::ObservedCancelled;
                    s.snapshot.cancellation = CancellationState::ObservedTerminal;
                });
                continue;
            }
            // Authority is rechecked in a read-only activity before each consequential
            // command, including after worker restart; replayed grants are not fresh authority.
            let call = |action, session| PortCall {
                admission: a.clone(),
                action,
                native_session_id: session,
            };
            let verified = ctx
                .execute_activity(
                    Ports::invoke_port,
                    call(PortAction::Verify, None),
                    single_attempt(),
                )
                .await;
            if !matches!(verified, Ok(ref r) if r.status == PortStatus::Verified && r.binding == a.binding)
            {
                ctx.state_mut(|s| {
                    s.snapshot.state = if launched {
                        OperationState::Unknown
                    } else {
                        OperationState::ObservedFailed
                    }
                });
                if !launched {
                    continue;
                }
                ctx.timer(Duration::from_millis(500)).await;
                continue;
            }
            if !launched {
                let online = ctx
                    .execute_activity(
                        Ports::invoke_port,
                        call(PortAction::ObserveHost, None),
                        single_attempt(),
                    )
                    .await;
                if !matches!(online, Ok(ref r) if r.status == PortStatus::Online && r.binding == a.binding)
                {
                    ctx.timer(Duration::from_millis(500)).await;
                    continue;
                }
                // Exactly one effectful activity is ever scheduled for this workflow.
                // Updates may have run while authority/host activities were pending.
                if ctx.state(|s| s.snapshot.cancellation != CancellationState::NotRequested) {
                    continue;
                }
                launched = true;
                let launch = ctx
                    .execute_activity(
                        Ports::invoke_port,
                        call(PortAction::Launch, None),
                        single_attempt(),
                    )
                    .await;
                match launch {
                    Ok(r)
                        if r.binding == a.binding
                            && r.status == PortStatus::Acknowledged
                            && r.native_session_id.as_deref().is_some_and(valid_id) =>
                    {
                        ctx.state_mut(|s| {
                            s.snapshot.state = OperationState::Acknowledged;
                            s.snapshot.native_session_id = r.native_session_id;
                        });
                    }
                    _ => ctx.state_mut(|s| s.snapshot.state = OperationState::Unknown),
                }
            }
            let snapshot = ctx.state(|s| s.snapshot.clone());
            if snapshot.cancellation == CancellationState::Requested
                && !cancel_sent
                && snapshot.native_session_id.is_some()
            {
                cancel_sent = true;
                let result = ctx
                    .execute_activity(
                        Ports::invoke_port,
                        call(PortAction::Cancel, snapshot.native_session_id.clone()),
                        single_attempt(),
                    )
                    .await;
                if matches!(result, Ok(ref r) if r.binding == a.binding && r.native_session_id == snapshot.native_session_id && r.status == PortStatus::Acknowledged)
                {
                    ctx.state_mut(|s| s.snapshot.cancellation = CancellationState::Acknowledged);
                }
            }
            let action = if snapshot.native_session_id.is_some() {
                PortAction::Poll
            } else {
                PortAction::Discover
            };
            let observed = ctx
                .execute_activity(
                    Ports::invoke_port,
                    call(action, snapshot.native_session_id.clone()),
                    single_attempt(),
                )
                .await;
            match observed {
                Ok(r)
                    if r.binding == a.binding
                        && r.native_session_id.as_deref().is_some_and(valid_id)
                        && snapshot
                            .native_session_id
                            .as_ref()
                            .is_none_or(|id| Some(id) == r.native_session_id.as_ref()) =>
                {
                    ctx.state_mut(|s| {
                        s.snapshot.native_session_id = r.native_session_id;
                        match r.status {
                            PortStatus::Completed => {
                                s.snapshot.state = OperationState::ObservedCompleted
                            }
                            PortStatus::Failed => s.snapshot.state = OperationState::ObservedFailed,
                            PortStatus::Cancelled => {
                                s.snapshot.state = OperationState::ObservedCancelled
                            }
                            PortStatus::Running => s.snapshot.state = OperationState::Acknowledged,
                            _ => s.snapshot.state = OperationState::Unknown,
                        }
                        if s.snapshot.state.terminal()
                            && s.snapshot.cancellation != CancellationState::NotRequested
                        {
                            s.snapshot.cancellation = CancellationState::ObservedTerminal;
                        }
                    });
                }
                _ => ctx.state_mut(|s| s.snapshot.state = OperationState::Unknown),
            }
            if !ctx.state(|s| s.snapshot.state.terminal()) {
                ctx.timer(Duration::from_millis(500)).await;
            }
        }
    }
    #[update]
    pub fn request_cancel(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        binding: Binding,
    ) -> bool {
        if binding != self.snapshot.binding || self.snapshot.state.terminal() {
            return false;
        }
        if self.snapshot.cancellation == CancellationState::NotRequested {
            self.snapshot.cancellation = CancellationState::Requested;
        }
        true
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> OperationSnapshot {
        self.snapshot.clone()
    }
}
