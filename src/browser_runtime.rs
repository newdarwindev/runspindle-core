//! Explicit synthetic Temporal Browser lane over the real durable registry.
//! No site adapter, credentials, production issuer or deployment is supplied.
use crate::{
    browser_effect_registry::BrowserRegistry,
    browser_effect_wire::{
        Action, Authority, Outcome, Peer, Target, EVIDENCE_CLASS, MAX_PAYLOAD, VERSION,
    },
    durable::digest,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::RetryPolicy;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

pub const QUEUE: &str = "runspindle-browser-v1";
pub const MAX_RECONCILES: u32 = 16;
pub const MAX_IDLE_TICKS: u32 = 120;
pub fn workflow_id(target: &Target) -> anyhow::Result<String> {
    anyhow::ensure!(target.valid(), "invalid browser target");
    Ok(format!(
        "browser-{}",
        digest(&serde_json::to_vec(&(
            &target.binding.principal,
            &target.binding.intent,
            target.binding.revision
        ))?)
    ))
}
fn read(path: &Path, limit: usize) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= limit, "synthetic browser input capacity");
    Ok(bytes)
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserScript {
    pub target: Target,
    pub expected_sha256: String,
    pub dispatch_authorized: bool,
    pub reconcile_authorized: bool,
    pub not_after_unix_ms: u64,
    pub crash_before_effect: bool,
    pub crash_after_effect: bool,
    pub lost_reply: bool,
}
fn script(directory: &Path) -> anyhow::Result<BrowserScript> {
    let s: BrowserScript =
        serde_json::from_slice(&read(&directory.join("browser-script.json"), 16384)?)?;
    anyhow::ensure!(
        s.target.valid()
            && s.expected_sha256.len() == 64
            && s.expected_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid synthetic browser script"
    );
    Ok(s)
}
struct Grant<'a>(&'a Path);
impl Authority for Grant<'_> {
    fn verify(&self, target: &Target, hash: &str, action: Action) -> bool {
        let Ok(s) = script(self.0) else { return false };
        let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return false;
        };
        s.target == *target
            && s.expected_sha256 == hash
            && now.as_millis() < u128::from(s.not_after_unix_ms)
            && match action {
                Action::Dispatch => s.dispatch_authorized,
                Action::Reconcile => s.reconcile_authorized,
            }
    }
}
struct SyntheticPeer<'a>(&'a Path);
impl Peer for SyntheticPeer<'_> {
    fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
        let request: serde_json::Value = serde_json::from_slice(raw)?;
        let target: Target = serde_json::from_value(request["target"].clone())?;
        let hash = request["payload_digest"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing digest"))?;
        let action = match request["action"].as_str() {
            Some("dispatch") => Action::Dispatch,
            Some("reconcile") => Action::Reconcile,
            _ => anyhow::bail!("invalid action"),
        };
        anyhow::ensure!(
            request["version"] == VERSION && Grant(self.0).verify(&target, hash, action),
            "synthetic browser denied"
        );
        let configuration = script(self.0)?;
        let payload = read(&self.0.join("browser-payload.bin"), MAX_PAYLOAD)?;
        anyhow::ensure!(
            !payload.is_empty() && digest(&payload) == hash,
            "synthetic browser payload mismatch"
        );
        if action == Action::Dispatch {
            let hex: String = payload.iter().map(|b| format!("{b:02x}")).collect();
            anyhow::ensure!(
                request["payload_hex"] == hex,
                "synthetic browser bytes mismatch"
            );
        } else {
            anyhow::ensure!(
                request["payload_hex"].is_null(),
                "synthetic reconciliation payload"
            );
        }
        let mut db = Connection::open(self.0.join("browser-site.sqlite"))?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(action TEXT NOT NULL,request TEXT NOT NULL);CREATE TABLE IF NOT EXISTS effects(operation TEXT PRIMARY KEY,target TEXT NOT NULL,payload BLOB NOT NULL,digest TEXT NOT NULL);")?;
        anyhow::ensure!(
            Grant(self.0).verify(&target, hash, action),
            "synthetic browser authority changed"
        );
        let label = if action == Action::Dispatch {
            "dispatch"
        } else {
            "reconcile"
        };
        db.execute(
            "INSERT INTO calls VALUES(?1,?2)",
            params![label, String::from_utf8(raw.to_vec())?],
        )?;
        let outcome = if action == Action::Dispatch {
            if configuration.crash_before_effect {
                std::process::exit(94);
            }
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            anyhow::ensure!(
                Grant(self.0).verify(&target, hash, action),
                "synthetic browser authority changed"
            );
            tx.execute(
                "INSERT INTO effects VALUES(?1,?2,?3,?4)",
                params![
                    target.binding.operation,
                    serde_json::to_string(&target)?,
                    payload,
                    hash
                ],
            )?;
            tx.commit()?;
            if configuration.crash_after_effect {
                std::process::exit(95);
            }
            if configuration.lost_reply {
                anyhow::bail!("synthetic lost reply");
            }
            "unknown" // Submission acknowledgement is never readback evidence.
        } else {
            let stored: Option<(String, Vec<u8>, String)> = db
                .query_row(
                    "SELECT target,payload,digest FROM effects WHERE operation=?1",
                    [&target.binding.operation],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            match stored {
                None => "unknown",
                Some((stored, bytes, digest)) => {
                    anyhow::ensure!(
                        serde_json::from_str::<Target>(&stored)? == target
                            && bytes == payload
                            && digest == hash,
                        "synthetic site binding mismatch"
                    );
                    "confirmed"
                }
            }
        };
        Ok(serde_json::to_vec(
            &serde_json::json!({"version":VERSION,"request_sha256":digest(raw),"evidence_class":EVIDENCE_CLASS,"outcome":outcome}),
        )?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserCall {
    pub target: Target,
    pub reconcile: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserObservation {
    pub outcome: String,
}
pub struct BrowserPorts {
    pub directory: Option<PathBuf>,
    /// Explicit development-only mode; never accepted from workflow input.
    pub pinned_fixture: bool,
    pub chromium_fixture: bool,
}
impl BrowserPorts {
    pub fn invoke(&self, call: BrowserCall) -> anyhow::Result<BrowserObservation> {
        self.invoke_inner(call)
            .map_err(|_| anyhow::anyhow!("browser activity denied"))
    }
    fn invoke_inner(&self, call: BrowserCall) -> anyhow::Result<BrowserObservation> {
        anyhow::ensure!(call.target.valid(), "invalid browser target");
        let directory = self
            .directory
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("browser defaults to deny"))?;
        anyhow::ensure!(
            !(self.pinned_fixture || self.chromium_fixture) || directory == Path::new("/effects"),
            "fixed pinned fixture root required"
        );
        let configuration = script(directory)?;
        let action = if call.reconcile {
            Action::Reconcile
        } else {
            Action::Dispatch
        };
        anyhow::ensure!(
            Grant(directory).verify(&call.target, &configuration.expected_sha256, action),
            "browser denied"
        );
        let mut registry = BrowserRegistry::open(directory.join("browser-registry.sqlite"))?;
        let payload = if call.reconcile {
            None
        } else {
            Some(read(&directory.join("browser-payload.bin"), MAX_PAYLOAD)?)
        };
        let result = if self.pinned_fixture || self.chromium_fixture {
            anyhow::ensure!(
                directory == Path::new("/effects"),
                "fixed pinned fixture root required"
            );
            let mut peer = PinnedPeer {
                chromium: self.chromium_fixture,
                directory,
                target: &call.target,
                hash: &configuration.expected_sha256,
                action,
            };
            if call.reconcile {
                registry.reconcile(&call.target, &Grant(directory), &mut peer)?
            } else {
                registry.dispatch(
                    call.target.clone(),
                    payload.as_deref().expect("dispatch payload"),
                    &Grant(directory),
                    &mut peer,
                )?
            }
        } else {
            let mut peer = SyntheticPeer(directory);
            if call.reconcile {
                registry.reconcile(&call.target, &Grant(directory), &mut peer)?
            } else {
                registry.dispatch(
                    call.target,
                    payload.as_deref().expect("dispatch payload"),
                    &Grant(directory),
                    &mut peer,
                )?
            }
        };
        Ok(BrowserObservation {
            outcome: match result {
                Outcome::Unknown => "unknown",
                Outcome::Confirmed => "confirmed",
                Outcome::Rejected => "rejected",
            }
            .into(),
        })
    }
}
struct PinnedPeer<'a> {
    chromium: bool,
    directory: &'a Path,
    target: &'a Target,
    hash: &'a str,
    action: Action,
}
impl Peer for PinnedPeer<'_> {
    fn exchange(&mut self, raw: &[u8]) -> anyhow::Result<Vec<u8>> {
        let current = || Grant(self.directory).verify(self.target, self.hash, self.action);
        if self.chromium {
            crate::browser_fixture_peer::exchange_chromium(raw, self.action, current)
        } else {
            crate::browser_fixture_peer::exchange(raw, self.action, current)
        }
    }
}
#[activities]
impl BrowserPorts {
    #[activity]
    pub async fn coordinate(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        call: BrowserCall,
    ) -> Result<BrowserObservation, ActivityError> {
        Ok(self.invoke(call)?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserState {
    pub target: Target,
    /// Historical completed-activity result, never fresh authorization.
    pub historical: Option<BrowserObservation>,
    pub attempts: u32,
    pub dispatch_finished: bool,
    pub completed_reconciles: u32,
    pub pending: bool,
    pub finished: bool,
    pub idle_ticks: u32,
}
impl BrowserState {
    pub fn new(target: Target) -> Self {
        Self {
            target,
            historical: None,
            attempts: 0,
            dispatch_finished: false,
            completed_reconciles: 0,
            pending: false,
            finished: false,
            idle_ticks: 0,
        }
    }
    pub fn request_reconcile(&mut self, target: &Target) -> bool {
        if !target.valid()
            || *target != self.target
            || self.finished
            || self.pending
            || self.completed_reconciles >= MAX_RECONCILES
        {
            return false;
        }
        self.pending = true;
        true
    }
    fn terminal(&self) -> bool {
        self.historical
            .as_ref()
            .is_some_and(|s| s.outcome == "confirmed" || s.outcome == "rejected")
    }
}
fn activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(3))
        .retry_policy(RetryPolicy::builder().maximum_attempts(3).build())
        .build()
}
#[workflow]
pub struct BrowserWorkflow {
    state: BrowserState,
}
#[workflow_methods]
impl BrowserWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, target: Target) -> Self {
        Self {
            state: BrowserState::new(target),
        }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<BrowserState> {
        let target = ctx.state(|s| s.state.target.clone());
        if !target.valid() {
            ctx.state_mut(|s| s.state.finished = true);
            return Ok(ctx.state(|s| s.state.clone()));
        }
        ctx.state_mut(|s| s.state.attempts += 1);
        let initial = ctx
            .execute_activity(
                BrowserPorts::coordinate,
                BrowserCall {
                    target: target.clone(),
                    reconcile: false,
                },
                activity_options(),
            )
            .await;
        ctx.state_mut(|s| {
            s.state.historical = initial.ok();
            s.state.dispatch_finished = true;
        });
        loop {
            if ctx.state(|s| {
                s.state.terminal()
                    || s.state.idle_ticks >= MAX_IDLE_TICKS
                    || s.state.completed_reconciles >= MAX_RECONCILES
            }) {
                ctx.state_mut(|s| s.state.finished = true);
                return Ok(ctx.state(|s| s.state.clone()));
            }
            if ctx.state(|s| s.state.pending) {
                ctx.state_mut(|s| {
                    s.state.attempts += 1;
                });
                let result = ctx
                    .execute_activity(
                        BrowserPorts::coordinate,
                        BrowserCall {
                            target: target.clone(),
                            reconcile: true,
                        },
                        activity_options(),
                    )
                    .await;
                ctx.state_mut(|s| {
                    s.state.pending = false;
                    s.state.historical = result.ok();
                    s.state.completed_reconciles += 1;
                });
            } else {
                ctx.timer(Duration::from_secs(1)).await;
                ctx.state_mut(|s| s.state.idle_ticks += 1);
            }
        }
    }
    #[update]
    pub fn request_reconcile(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        target: Target,
    ) -> bool {
        self.state.request_reconcile(&target)
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> BrowserState {
        self.state.clone()
    }
}

// Separate workflow type fixes activity deadlines in history. Never select
// workflow command timeouts from worker-local peer configuration.
pub const ENGINE_QUEUE: &str = "runspindle-browser-engine-v1";
pub fn engine_workflow_id(target: &Target) -> anyhow::Result<String> {
    Ok(format!("browser-engine-v1-{}", workflow_id(target)?))
}
fn engine_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
        .retry_policy(RetryPolicy::builder().maximum_attempts(3).build())
        .build()
}
#[workflow]
pub struct BrowserEngineWorkflowV1 {
    state: BrowserState,
}
#[workflow_methods]
impl BrowserEngineWorkflowV1 {
    #[init]
    fn new(_ctx: &WorkflowContextView, target: Target) -> Self {
        Self {
            state: BrowserState::new(target),
        }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<BrowserState> {
        let target = ctx.state(|s| s.state.target.clone());
        if !target.valid() {
            ctx.state_mut(|s| s.state.finished = true);
            return Ok(ctx.state(|s| s.state.clone()));
        }
        ctx.state_mut(|s| s.state.attempts += 1);
        let initial = ctx
            .execute_activity(
                BrowserPorts::coordinate,
                BrowserCall {
                    target: target.clone(),
                    reconcile: false,
                },
                engine_activity_options(),
            )
            .await;
        ctx.state_mut(|s| {
            s.state.historical = initial.ok();
            s.state.dispatch_finished = true;
        });
        loop {
            if ctx.state(|s| {
                s.state.terminal()
                    || s.state.idle_ticks >= MAX_IDLE_TICKS
                    || s.state.completed_reconciles >= MAX_RECONCILES
            }) {
                ctx.state_mut(|s| s.state.finished = true);
                return Ok(ctx.state(|s| s.state.clone()));
            }
            if ctx.state(|s| s.state.pending) {
                ctx.state_mut(|s| {
                    s.state.attempts += 1;
                });
                let result = ctx
                    .execute_activity(
                        BrowserPorts::coordinate,
                        BrowserCall {
                            target: target.clone(),
                            reconcile: true,
                        },
                        engine_activity_options(),
                    )
                    .await;
                ctx.state_mut(|s| {
                    s.state.pending = false;
                    s.state.historical = result.ok();
                    s.state.completed_reconciles += 1;
                });
            } else {
                ctx.timer(Duration::from_secs(1)).await;
                ctx.state_mut(|s| s.state.idle_ticks += 1);
            }
        }
    }
    #[update]
    pub fn request_reconcile(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        target: Target,
    ) -> bool {
        self.state.request_reconcile(&target)
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> BrowserState {
        self.state.clone()
    }
}
