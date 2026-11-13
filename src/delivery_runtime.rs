//! H07 synthetic transport and real Temporal outbox coordination.
use crate::{durable::*, integration::once, outbox::*, runtime::Binding};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    WorkflowContext, WorkflowContextView, WorkflowResult,
};
fn read(path: &Path) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= MAX_BYTES, "synthetic input capacity");
    Ok(bytes)
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactScript {
    pub version: String,
    pub binding: Binding,
    pub artifact_id: String,
    pub artifact_revision: u64,
    pub expected_sha256: String,
    pub validator_revision: String,
    pub authorized: bool,
    pub crash_after_stage: bool,
    pub crash_after_accept: bool,
}
struct ScriptValidator<'a>(&'a ArtifactScript);
impl Validator for ScriptValidator<'_> {
    fn revision(&self) -> &str {
        &self.0.validator_revision
    }
    fn validate(&self, a: &ArtifactRef, bytes: &[u8], policy: &str) -> bool {
        a.binding == self.0.binding
            && a.artifact_id == self.0.artifact_id
            && a.artifact_revision == self.0.artifact_revision
            && digest(bytes) == self.0.expected_sha256
            && a.sha256 == self.0.expected_sha256
            && policy == self.0.binding.policy_revision
    }
}
struct AcceptGrant<'a>(&'a ArtifactScript);
impl AcceptanceAuthority for AcceptGrant<'_> {
    fn verify(&self, v: &Validation) -> bool {
        self.0.authorized
            && self.0.version == VERSION
            && v.artifact.binding == self.0.binding
            && v.artifact.artifact_id == self.0.artifact_id
            && v.artifact.artifact_revision == self.0.artifact_revision
            && v.artifact.sha256 == self.0.expected_sha256
            && v.validator_revision == self.0.validator_revision
            && v.policy_revision == self.0.binding.policy_revision
            && v.passed
    }
}
pub fn artifact_command(directory: &Path, action: &str) -> anyhow::Result<serde_json::Value> {
    let script: ArtifactScript =
        serde_json::from_slice(&read(&directory.join("artifact-script.json"))?)?;
    anyhow::ensure!(
        script.version == VERSION && script.binding.valid(),
        "unsupported artifact request"
    );
    let mut store = Store::open(directory.join("core.sqlite"))?;
    if action == "stage" {
        let bytes = read(&directory.join("artifact.bin"))?;
        let artifact = store.put(
            VERSION,
            script.binding.clone(),
            script.artifact_id.clone(),
            script.artifact_revision,
            &bytes,
        )?;
        let validation = store.validate(
            VERSION,
            &artifact,
            &script.binding.policy_revision,
            &ScriptValidator(&script),
        )?;
        if script.crash_after_stage {
            std::process::exit(96)
        }
        return Ok(serde_json::to_value(validation)?);
    }
    let validation: Validation =
        serde_json::from_slice(&read(&directory.join("validation.json"))?)?;
    if action == "accept" {
        let accepted = store.accept(VERSION, &validation, &AcceptGrant(&script))?;
        if script.crash_after_accept {
            std::process::exit(95)
        }
        return Ok(serde_json::to_value(accepted)?);
    }
    anyhow::ensure!(action == "inspect", "unsupported artifact action");
    let accepted = Accepted { validation };
    let bytes = store.accepted(&accepted)?;
    Ok(serde_json::json!({"accepted":accepted,"sha256":digest(&bytes),"bytes":bytes.len()}))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryScript {
    pub request: DeliveryRequest,
    pub authorized: bool,
    pub crash_before_send: bool,
    pub crash_after_send: bool,
    pub lost_reply: bool,
    #[serde(default)]
    pub ambiguous: bool,
    #[serde(default)]
    pub receipt_override: Option<String>,
    #[serde(default)]
    pub destination_override: Option<String>,
}
struct DeliveryGrant<'a>(&'a DeliveryScript);
impl DeliveryAuthority for DeliveryGrant<'_> {
    fn verify(&self, r: &DeliveryRequest) -> bool {
        self.0.authorized && r.valid() && self.0.request == *r
    }
}
struct Destination {
    connection: Connection,
    script: DeliveryScript,
}
impl Destination {
    fn open(directory: &Path, script: DeliveryScript) -> anyhow::Result<Self> {
        let connection = Connection::open(directory.join("destination.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY,action TEXT NOT NULL,request TEXT NOT NULL);CREATE TABLE IF NOT EXISTS receipts(operation TEXT PRIMARY KEY,receipt TEXT NOT NULL,bytes BLOB NOT NULL);")?;
        Ok(Self { connection, script })
    }
    fn call(&self, action: &str, request: &DeliveryRequest) -> anyhow::Result<()> {
        self.connection.execute(
            "INSERT INTO calls(action,request) VALUES(?1,?2)",
            params![action, serde_json::to_string(request)?],
        )?;
        Ok(())
    }
}
impl Transport for Destination {
    fn send(
        &mut self,
        request: &DeliveryRequest,
        bytes: &[u8],
    ) -> anyhow::Result<Option<DeliveryReceipt>> {
        if self.script.crash_before_send {
            std::process::exit(92)
        }
        anyhow::ensure!(
            request == &self.script.request
                && self.script.authorized
                && digest(bytes) == request.accepted.artifact().sha256,
            "scripted destination binding mismatch"
        );
        self.call("send", request)?;
        let receipt = DeliveryReceipt {
            request: request.clone(),
            receipt_id: format!(
                "receipt-{}",
                digest(request.delivery_operation_id.as_bytes())
            ),
        };
        self.connection.execute(
            "INSERT INTO receipts VALUES(?1,?2,?3)",
            params![
                request.delivery_operation_id,
                serde_json::to_string(&receipt)?,
                bytes
            ],
        )?;
        if self.script.crash_after_send {
            std::process::exit(94)
        }
        Ok(if self.script.lost_reply {
            None
        } else {
            Some(receipt)
        })
    }
    fn lookup(&mut self, request: &DeliveryRequest) -> anyhow::Result<Vec<DeliveryReceipt>> {
        self.call("lookup", request)?;
        let data: Option<String> = self
            .connection
            .query_row(
                "SELECT receipt FROM receipts WHERE operation=?1",
                [&request.delivery_operation_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(data) = data else { return Ok(vec![]) };
        let mut receipt: DeliveryReceipt = serde_json::from_str(&data)?;
        if let Some(id) = &self.script.receipt_override {
            receipt.receipt_id = id.clone();
        }
        if let Some(destination) = &self.script.destination_override {
            receipt.request.destination_id = destination.clone();
        }
        Ok(vec![receipt; if self.script.ambiguous { 2 } else { 1 }])
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryCall {
    pub request: DeliveryRequest,
    pub reconcile: bool,
}
pub struct DeliveryPorts {
    pub directory: Option<PathBuf>,
}
impl DeliveryPorts {
    pub fn invoke(&self, call: DeliveryCall) -> anyhow::Result<DeliverySnapshot> {
        anyhow::ensure!(call.request.valid(), "unsupported delivery request");
        let dir = self
            .directory
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("delivery defaults to deny"))?;
        let script: DeliveryScript =
            serde_json::from_slice(&read(&dir.join("delivery-script.json"))?)?;
        anyhow::ensure!(
            script.authorized && script.request == call.request,
            "delivery denied"
        );
        let mut outbox = Outbox::open(dir.join("core.sqlite"))?;
        let mut destination = Destination::open(dir, script.clone())?;
        let result = if call.reconcile {
            outbox.reconcile(&call.request, &DeliveryGrant(&script), &mut destination)
        } else {
            outbox.deliver(&call.request, &DeliveryGrant(&script), &mut destination)
        };
        match result {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => match outbox.snapshot(&call.request)? {
                Some(snapshot) if snapshot.state != DeliveryState::Delivered => Ok(snapshot),
                _ => Err(error),
            },
        }
    }
}
#[activities]
impl DeliveryPorts {
    #[activity]
    pub async fn coordinate(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
        call: DeliveryCall,
    ) -> Result<DeliverySnapshot, ActivityError> {
        Ok(self.invoke(call)?)
    }
}
#[workflow]
pub struct DeliveryWorkflow {
    request: DeliveryRequest,
    snapshot: Option<DeliverySnapshot>,
}
#[workflow_methods]
impl DeliveryWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, request: DeliveryRequest) -> Self {
        Self {
            request,
            snapshot: None,
        }
    }
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<DeliverySnapshot> {
        let mut attempted = false;
        loop {
            if let Some(done) = ctx
                .state(|s| s.snapshot.clone())
                .filter(|s| s.state == DeliveryState::Delivered && !s.conflict)
            {
                return Ok(done);
            }
            let request = ctx.state(|s| s.request.clone());
            let call = DeliveryCall {
                request,
                reconcile: attempted,
            };
            attempted = true;
            if let Ok(snapshot) = ctx
                .execute_activity(DeliveryPorts::coordinate, call, once())
                .await
            {
                ctx.state_mut(|s| s.snapshot = Some(snapshot));
            }
            ctx.timer(Duration::from_millis(250)).await;
        }
    }
    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> Option<DeliverySnapshot> {
        self.snapshot.clone()
    }
}
