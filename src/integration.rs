//! H02: real durable host library activities, with an explicitly scripted native peer.
use crate::runtime::{Admission, Binding, PortStatus};
use runspindle_host::{lifecycle as host, storage::Sqlite};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::RetryPolicy;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

pub const VERSION: &str = "runspindle.integration.v1";
pub fn host_binding(b: &Binding) -> host::Binding {
    host::Binding {
        task_id: b.task_id.clone(),
        revision: b.revision,
        operation_id: b.operation_id.clone(),
        policy_revision: b.policy_revision.clone(),
        host_id: b.host_id.clone(),
        workspace_id: b.workspace_id.clone(),
        lease_epoch: b.lease_epoch,
    }
}
pub fn now_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
fn read_script(path: &Path) -> anyhow::Result<NativeScript> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 1_048_576, "oversized synthetic script");
    Ok(serde_json::from_slice(&bytes)?)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRequest {
    pub version: String,
    pub admission: Admission,
}
impl HostRequest {
    pub fn valid(&self) -> bool {
        self.version == VERSION && self.admission.valid()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScript {
    pub request: HostRequest,
    pub authorized: bool,
    pub online: bool,
    pub native_session_id: String,
    pub crash_after_launch: bool,
    pub lost_reply: bool,
    pub terminal: Option<PortStatus>,
    #[serde(default)]
    pub crash_before_effect: bool,
    #[serde(default)]
    pub discovery_count: Option<usize>,
    #[serde(default)]
    pub session_override: Option<String>,
    #[serde(default)]
    pub sequence_override: Option<u64>,
    #[serde(default)]
    pub observed_at_override: Option<u64>,
    #[serde(default)]
    pub reconcile_barrier: bool,
    #[serde(default)]
    pub authority_valid_until_ms: Option<u64>,
    #[serde(default)]
    pub crash_after_prepare: bool,
}
impl NativeScript {
    pub fn authorized_at(&self, now: u64) -> bool {
        self.authorized
            && self
                .authority_valid_until_ms
                .is_none_or(|until| now < until)
    }
}
struct Grant<'a>(&'a NativeScript);
impl host::Verifier for Grant<'_> {
    fn verify(&self, b: &host::Binding, _: host::Action) -> bool {
        now_ms().is_ok_and(|now| self.0.authorized_at(now))
            && self.0.request.valid()
            && *b == host_binding(&self.0.request.admission.binding)
    }
}
struct NativePeer {
    connection: Connection,
    script: NativeScript,
    now: u64,
}
impl NativePeer {
    fn open(directory: &Path, script: NativeScript, now: u64) -> anyhow::Result<Self> {
        let connection = Connection::open(directory.join("native-peer.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY,action TEXT NOT NULL,binding TEXT NOT NULL);CREATE TABLE IF NOT EXISTS sessions(operation TEXT PRIMARY KEY,observation TEXT NOT NULL);")?;
        Ok(Self {
            connection,
            script,
            now,
        })
    }
    fn call(&self, action: &str, b: &host::Binding) -> anyhow::Result<()> {
        self.connection.execute(
            "INSERT INTO calls(action,binding) VALUES(?1,?2)",
            params![action, serde_json::to_string(b)?],
        )?;
        Ok(())
    }
    fn observed(&self, b: &host::Binding, sequence: u64) -> host::Observation {
        host::Observation {
            binding: b.clone(),
            native_session_id: self.script.native_session_id.clone(),
            sequence,
            observed_at_ms: self.now,
            terminal: None,
        }
    }
    fn launch_inner(&mut self, b: &host::Binding) -> anyhow::Result<host::LaunchReply> {
        self.call("launch", b)?;
        if self.script.crash_before_effect {
            std::process::exit(91)
        }
        let o = self.observed(b, 1);
        self.connection.execute(
            "INSERT INTO sessions VALUES(?1,?2)",
            params![b.operation_id, serde_json::to_string(&o)?],
        )?;
        if self.script.crash_after_launch {
            std::process::exit(93)
        }
        Ok(if self.script.lost_reply {
            host::LaunchReply::LostReply
        } else {
            host::LaunchReply::Accepted(o)
        })
    }
    fn discover_inner(&mut self, b: &host::Binding) -> anyhow::Result<Vec<host::Observation>> {
        self.call("discover", b)?;
        let tx = self.connection.transaction()?;
        let data: String = tx.query_row(
            "SELECT observation FROM sessions WHERE operation=?1",
            [&b.operation_id],
            |r| r.get(0),
        )?;
        let mut o: host::Observation = serde_json::from_str(&data)?;
        anyhow::ensure!(&o.binding == b, "peer binding conflict");
        o.sequence = o
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("sequence overflow"))?;
        o.observed_at_ms = self.now;
        o.terminal = match self.script.terminal {
            Some(PortStatus::Completed) => Some(host::Terminal::Completed),
            Some(PortStatus::Failed) => Some(host::Terminal::Failed),
            Some(PortStatus::Cancelled) => Some(host::Terminal::Cancelled),
            _ => None,
        };
        tx.execute(
            "UPDATE sessions SET observation=?1 WHERE operation=?2",
            params![serde_json::to_string(&o)?, b.operation_id],
        )?;
        tx.commit()?;
        if let Some(id) = &self.script.session_override {
            o.native_session_id = id.clone();
        }
        if let Some(seq) = self.script.sequence_override {
            o.sequence = seq;
        }
        if let Some(time) = self.script.observed_at_override {
            o.observed_at_ms = time;
        }
        Ok(vec![o; self.script.discovery_count.unwrap_or(1).min(3)])
    }
}
impl host::Peer for NativePeer {
    fn launch(&mut self, b: &host::Binding) -> host::LaunchReply {
        self.launch_inner(b).unwrap_or(host::LaunchReply::LostReply)
    }
    fn discover(&mut self, b: &host::Binding) -> Vec<host::Observation> {
        self.discover_inner(b).unwrap_or_default()
    }
    fn cancel(&mut self, b: &host::Binding, s: &str) -> host::CancelReply {
        if s != self.script.native_session_id || self.call("cancel", b).is_err() {
            return host::CancelReply::LostReply;
        }
        host::CancelReply::Acknowledged
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HostAction {
    Prepare,
    Dispatch,
    Reconcile,
    Cancel,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostCall {
    pub request: HostRequest,
    pub action: HostAction,
}
pub struct HostPorts {
    pub directory: Option<PathBuf>,
}
impl HostPorts {
    pub fn invoke(&self, call: HostCall) -> anyhow::Result<host::Snapshot> {
        anyhow::ensure!(call.request.valid(), "unsupported or invalid host request");
        let dir = self
            .directory
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("host authority defaults to deny"))?;
        let script = read_script(&dir.join("native-script.json"))?;
        anyhow::ensure!(
            script.request.version == call.request.version
                && script.request.admission == call.request.admission
                && script.authorized_at(now_ms()?),
            "denied host binding"
        );
        anyhow::ensure!(
            crate::runtime::valid_id(&script.native_session_id),
            "invalid peer session"
        );
        let now = now_ms()?;
        let b = host_binding(&call.request.admission.binding);
        let mut a = host::Adapter::with_storage(
            Grant(&script),
            Sqlite::open(dir.join("host.sqlite"))
                .map_err(|e| anyhow::anyhow!("host store: {e:?}"))?,
            1024,
            4096,
            30_000,
        )
        .map_err(|e| anyhow::anyhow!("host open: {e:?}"))?;
        let mut peer = NativePeer::open(dir, script.clone(), now)?;
        let presence = host::Presence {
            host_id: b.host_id.clone(),
            workspace_id: b.workspace_id.clone(),
            reachability: if script.online {
                host::Reachability::Online
            } else {
                host::Reachability::Offline
            },
            observed_at_ms: now,
            valid_until_ms: now + 30_000,
        };
        let result = match call.action {
            HostAction::Prepare => {
                let mut offline = presence;
                offline.reachability = host::Reachability::Offline;
                a.submit(
                    host::Request {
                        version: host::VERSION.into(),
                        binding: b.clone(),
                    },
                    &offline,
                    now,
                    &mut peer,
                )
            }
            HostAction::Dispatch => a.dispatch(&b, &presence, now, &mut peer),
            HostAction::Reconcile => a.reconcile(&b, now, &mut peer),
            HostAction::Cancel => a.cancel(&b, &mut peer),
        };
        // The queue remains bounded; consumed metadata is durable state, not discarded
        // native observations. Draining is explicit and separately transactional.
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(host::Error::Unknown | host::Error::Ambiguous) => a
                .snapshot(&b)
                .map_err(|e| anyhow::anyhow!("host snapshot: {e:?}"))?,
            Err(error) => return Err(anyhow::anyhow!("host action: {error:?}")),
        };
        Ok(snapshot)
    }
}
#[activities]
impl HostPorts {
    #[activity]
    pub async fn verify(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        request: HostRequest,
    ) -> Result<bool, ActivityError> {
        let Some(dir) = &self.directory else {
            return Ok(false);
        };
        let script = read_script(&dir.join("native-script.json"))?;
        Ok(request.valid()
            && script.authorized_at(now_ms()?)
            && script.request.admission == request.admission
            && script.request.version == request.version)
    }
    #[activity]
    pub async fn coordinate(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        call: HostCall,
    ) -> Result<host::Snapshot, ActivityError> {
        let snapshot = self.invoke(call.clone())?;
        if matches!(call.action, HostAction::Prepare) {
            if let Some(dir) = &self.directory {
                if read_script(&dir.join("native-script.json"))?.crash_after_prepare {
                    std::process::exit(94);
                }
            }
        }
        if matches!(call.action, HostAction::Reconcile)
            && matches!(snapshot.execution, host::Execution::Terminal(_))
        {
            if let Some(dir) = &self.directory {
                let script = read_script(&dir.join("native-script.json"))?;
                if script.reconcile_barrier {
                    let operation = &call.request.admission.binding.operation_id;
                    let write_receipt = |name: &str| -> anyhow::Result<()> {
                        use std::io::Write;
                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(dir.join(format!("{name}-{operation}.json")))?;
                        file.write_all(&serde_json::to_vec(&snapshot)?)?;
                        file.sync_all()?;
                        Ok(())
                    };
                    write_receipt("reconcile-ready")?;
                    let release = dir.join(format!("reconcile-release-{operation}.json"));
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                    while !release.exists() {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(
                                anyhow::anyhow!("synthetic reconcile barrier deadline").into()
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let release_binding: Binding =
                        serde_json::from_slice(&std::fs::read(release)?)?;
                    if release_binding != call.request.admission.binding {
                        return Err(anyhow::anyhow!("barrier binding mismatch").into());
                    }
                    write_receipt("reconcile-returned")?;
                }
            }
        }
        Ok(snapshot)
    }
}
pub fn once() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(3))
        .retry_policy(RetryPolicy::builder().maximum_attempts(1).build())
        .build()
}
pub fn preserve_cancellation(snapshot: &mut host::Snapshot, requested: bool) {
    if requested && snapshot.cancellation == host::Cancellation::None {
        snapshot.cancellation = if matches!(snapshot.execution, host::Execution::Terminal(_)) {
            host::Cancellation::ObservedTerminal
        } else {
            host::Cancellation::Requested
        };
    }
}
pub fn terminal(s: &host::Snapshot) -> bool {
    matches!(
        s.execution,
        host::Execution::Terminal(_)
            | host::Execution::Rejected
            | host::Execution::CancelledBeforeDispatch
    )
}
#[workflow]
pub struct NativeOperationWorkflow {
    request: HostRequest,
    snapshot: Option<host::Snapshot>,
    cancel_requested: bool,
}
#[workflow_methods]
impl NativeOperationWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, request: HostRequest) -> Self {
        Self {
            request,
            snapshot: None,
            cancel_requested: false,
        }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<host::Snapshot> {
        loop {
            let (request, snapshot, cancel) =
                ctx.state(|s| (s.request.clone(), s.snapshot.clone(), s.cancel_requested));
            if let Some(ref s) = snapshot {
                if terminal(s) {
                    return Ok(s.clone());
                }
            }
            let action = match snapshot {
                None => HostAction::Prepare,
                Some(ref s)
                    if cancel
                        && (s.cancellation == host::Cancellation::None
                            || s.execution == host::Execution::Queued) =>
                {
                    HostAction::Cancel
                }
                Some(ref s) if cancel && !s.cancel_dispatched && s.native_session_id.is_some() => {
                    HostAction::Cancel
                }
                Some(ref s) if s.execution == host::Execution::Queued => HostAction::Dispatch,
                _ => HostAction::Reconcile,
            };
            let options = if matches!(action, HostAction::Reconcile) {
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(25))
                    .retry_policy(RetryPolicy::builder().maximum_attempts(1).build())
                    .build()
            } else {
                once()
            };
            let result = ctx
                .execute_activity(HostPorts::coordinate, HostCall { request, action }, options)
                .await;
            if let Ok(snapshot) = result {
                ctx.state_mut(|s| {
                    let mut snapshot = snapshot;
                    if terminal(&snapshot) {
                        preserve_cancellation(&mut snapshot, s.cancel_requested);
                    }
                    s.snapshot = Some(snapshot);
                });
            }
            ctx.timer(Duration::from_millis(250)).await;
        }
    }
    #[update]
    pub fn request_cancel(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        binding: Binding,
    ) -> bool {
        if binding != self.request.admission.binding || self.snapshot.as_ref().is_some_and(terminal)
        {
            return false;
        }
        self.cancel_requested = true;
        true
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> Option<host::Snapshot> {
        self.snapshot.clone().map(|mut snapshot| {
            preserve_cancellation(&mut snapshot, self.cancel_requested);
            snapshot
        })
    }
}

#[workflow]
#[derive(Default)]
pub struct HostAdmissionRegistry {
    state: crate::runtime::RegistryState,
    outcomes: std::collections::BTreeMap<String, host::Snapshot>,
}
#[workflow_methods]
impl HostAdmissionRegistry {
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
                    NativeOperationWorkflow::run,
                    HostRequest {
                        version: VERSION.into(),
                        admission,
                    },
                    temporalio_sdk::ChildWorkflowOptions::workflow_id(job.clone()),
                )
                .await?;
            let snapshot = child.result().await?;
            ctx.state_mut(|s| {
                // Only actual native terminal observations satisfy Runtime v1's
                // predecessor gate. A local pre-dispatch cancel stays distinct.
                if let host::Execution::Terminal(t) = snapshot.execution {
                    let state = match t {
                        host::Terminal::Completed => {
                            crate::runtime::OperationState::ObservedCompleted
                        }
                        host::Terminal::Failed => crate::runtime::OperationState::ObservedFailed,
                        host::Terminal::Cancelled => {
                            crate::runtime::OperationState::ObservedCancelled
                        }
                    };
                    let cancellation = match snapshot.cancellation {
                        host::Cancellation::None => crate::runtime::CancellationState::NotRequested,
                        host::Cancellation::Requested => {
                            crate::runtime::CancellationState::Requested
                        }
                        host::Cancellation::Acknowledged => {
                            crate::runtime::CancellationState::Acknowledged
                        }
                        host::Cancellation::ObservedTerminal => {
                            crate::runtime::CancellationState::ObservedTerminal
                        }
                    };
                    s.state.terminal.insert(
                        job.clone(),
                        crate::runtime::OperationSnapshot {
                            binding: s.state.revisions[&job].binding.clone(),
                            state,
                            cancellation,
                            native_session_id: snapshot.native_session_id.clone(),
                        },
                    );
                }
                s.outcomes.insert(job, snapshot);
                s.state.active = None;
            });
        }
    }
    #[update]
    pub async fn admit(
        ctx: &mut WorkflowContext<Self>,
        request: HostRequest,
    ) -> crate::runtime::AdmissionReply {
        let verified = if request.valid() {
            ctx.execute_activity(HostPorts::verify, request.clone(), once())
                .await
                .unwrap_or(false)
        } else {
            false
        };
        if verified {
            ctx.state_mut(|s| s.state.admit(request.admission))
        } else {
            crate::runtime::AdmissionReply {
                status: "denied".into(),
                job_id: None,
            }
        }
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> crate::runtime::RegistryState {
        self.state.clone()
    }
}
