//! Explicit synthetic SDK/client checkpoints, outside deterministic workflows.
//! Never enabled by environment or by an admission request.
use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Mutex};
use temporalio_common::protos::coresdk::{
    workflow_activation::{workflow_activation_job::Variant as Activation, WorkflowActivation},
    workflow_commands::{update_response::Response, workflow_command::Variant as Command},
    workflow_completion::{workflow_activation_completion::Status, WorkflowActivationCompletion},
};
use temporalio_sdk::interceptors::WorkerInterceptor;

pub struct Checkpoint {
    directory: PathBuf,
    point: String,
    registries: Mutex<BTreeMap<String, bool>>,
}
impl Checkpoint {
    pub fn new(directory: PathBuf, point: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            directory == std::path::Path::new("/effects"),
            "synthetic /effects required"
        );
        anyhow::ensure!(
            matches!(
                point.as_str(),
                "admission_commit" | "before_child_creation" | "after_child_creation"
            ),
            "unsupported admission checkpoint"
        );
        Ok(Self {
            directory,
            point,
            registries: Mutex::new(BTreeMap::new()),
        })
    }
    fn stop(&self, run_id: &str) {
        let path = self.directory.join("admission-checkpoint.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("new synthetic checkpoint receipt");
        file.write_all(
            &serde_json::to_vec(&serde_json::json!({"point":self.point,"run_id":run_id}))
                .expect("checkpoint JSON"),
        )
        .expect("checkpoint receipt");
        file.sync_all().expect("durable checkpoint receipt");
        std::process::exit(95);
    }
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for Checkpoint {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        let mut registries = self.registries.lock().expect("checkpoint registry map");
        for job in &activation.jobs {
            if let Some(Activation::InitializeWorkflow(init)) = &job.variant {
                registries.insert(
                    activation.run_id.clone(),
                    init.workflow_type == "HostAdmissionRegistry",
                );
            }
        }
        if activation.is_replaying || !registries.get(&activation.run_id).copied().unwrap_or(false)
        {
            return Ok(());
        }
        if self.point == "after_child_creation" && activation.jobs.iter().any(|job| matches!(&job.variant, Some(Activation::ResolveChildWorkflowExecutionStart(start)) if matches!(&start.status, Some(temporalio_common::protos::coresdk::workflow_activation::resolve_child_workflow_execution_start::Status::Succeeded(_))))) {
            self.stop(&activation.run_id);
        }
        Ok(())
    }
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if !self
            .registries
            .lock()
            .expect("checkpoint registry map")
            .get(&completion.run_id)
            .copied()
            .unwrap_or(false)
        {
            return;
        }
        let Some(Status::Successful(success)) = &completion.status else {
            return;
        };
        let admission = success.commands.iter().any(|command| matches!(&command.variant,
            Some(Command::UpdateResponse(update)) if matches!(&update.response, Some(Response::Completed(payload)) if serde_json::from_slice::<serde_json::Value>(&payload.data).ok().and_then(|v| v["status"].as_str().map(str::to_owned)).as_deref() == Some("admitted"))));
        let child = success.commands.iter().any(|command| {
            matches!(
                &command.variant,
                Some(Command::StartChildWorkflowExecution(_))
            )
        });
        // These are pre-server-commit command boundaries. Workflow state and child
        // intent may share a workflow-task transaction; do not invent a durable gap.
        if (self.point == "admission_commit" && admission)
            || (self.point == "before_child_creation" && child)
        {
            self.stop(&completion.run_id);
        }
    }
}
