//! Explicit synthetic SDK/client checkpoints, outside deterministic workflows.
//! Never enabled by environment or by an admission request.
use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Mutex};
use temporalio_common::protos::coresdk::{
    workflow_activation::{workflow_activation_job::Variant as Activation, WorkflowActivation},
    workflow_commands::{update_response::Response, workflow_command::Variant as Command},
    workflow_completion::{workflow_activation_completion::Status, WorkflowActivationCompletion},
};
use temporalio_sdk::interceptors::WorkerInterceptor;

fn intent_launch(activity_type: &str, bytes: &[u8]) -> bool {
    activity_type == "IntentPorts::invoke_intent"
        && serde_json::from_slice::<serde_json::Value>(bytes)
            .ok()
            .is_some_and(|v| v["call"]["action"].as_str() == Some("launch"))
}

pub struct Checkpoint {
    directory: PathBuf,
    point: String,
    registries: Mutex<BTreeMap<String, bool>>,
    replaying: Mutex<BTreeMap<String, bool>>,
    workflow_type: String,
    workflow_id: Option<String>,
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
            replaying: Mutex::new(BTreeMap::new()),
            workflow_type: "HostAdmissionRegistry".into(),
            workflow_id: None,
        })
    }
    #[cfg(any(feature = "verified-authority", test))]
    pub fn exact(
        directory: PathBuf,
        point: String,
        workflow_type: &str,
        workflow_id: String,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            directory == std::path::Path::new("/effects"),
            "synthetic /effects required"
        );
        anyhow::ensure!(
            !workflow_id.is_empty() && workflow_id.len() <= 256,
            "exact workflow ID required"
        );
        let valid = match workflow_type {
            "AuthenticatedRegistryV1" | "ScopedRegistryV1" => matches!(
                point.as_str(),
                "admission_commit" | "before_child_creation" | "after_child_creation"
            ),
            "IntentWorkflowV1" => matches!(
                point.as_str(),
                "admission_commit" | "before_dispatch_scheduling"
            ),
            _ => false,
        };
        anyhow::ensure!(valid, "unsupported exact workflow/checkpoint combination");
        Ok(Self {
            directory,
            point,
            registries: Mutex::new(BTreeMap::new()),
            replaying: Mutex::new(BTreeMap::new()),
            workflow_type: workflow_type.into(),
            workflow_id: Some(workflow_id),
        })
    }
    fn selected(&self, workflow_type: &str, workflow_id: &str) -> bool {
        workflow_type == self.workflow_type
            && self.workflow_id.as_ref().is_none_or(|id| id == workflow_id)
    }
    fn live_completion(&self, run_id: &str) -> bool {
        self.replaying
            .lock()
            .expect("checkpoint replay map")
            .get(run_id)
            .copied()
            == Some(false)
    }
    fn stop(&self, run_id: &str) {
        let path = self.directory.join("admission-checkpoint.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("new synthetic checkpoint receipt");
        file.write_all(
            &serde_json::to_vec(&serde_json::json!({"point":self.point,"run_id":run_id,"workflow_type":self.workflow_type,"workflow_id":self.workflow_id}))
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
        self.replaying
            .lock()
            .expect("checkpoint replay map")
            .insert(activation.run_id.clone(), activation.is_replaying);
        let mut registries = self.registries.lock().expect("checkpoint registry map");
        for job in &activation.jobs {
            if let Some(Activation::InitializeWorkflow(init)) = &job.variant {
                registries.insert(
                    activation.run_id.clone(),
                    self.selected(&init.workflow_type, &init.workflow_id),
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
        if !self.live_completion(&completion.run_id) {
            return;
        }
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
            Some(Command::UpdateResponse(update)) if matches!(&update.response, Some(Response::Completed(payload)) if serde_json::from_slice::<serde_json::Value>(&payload.data).ok().is_some_and(|v| v["status"].as_str() == Some("admitted") || (self.workflow_type == "IntentWorkflowV1" && v.as_str() == Some("admitted"))))));
        let child = success.commands.iter().any(|command| {
            matches!(
                &command.variant,
                Some(Command::StartChildWorkflowExecution(_))
            )
        });
        let launch = success
            .commands
            .iter()
            .any(|command| match &command.variant {
                Some(Command::ScheduleActivity(activity)) => activity
                    .arguments
                    .iter()
                    .any(|payload| intent_launch(&activity.activity_type, &payload.data)),
                _ => false,
            });
        // These are pre-server-commit command boundaries. Workflow state and child
        // intent may share a workflow-task transaction; do not invent a durable gap.
        if (self.point == "admission_commit" && admission)
            || (self.point == "before_child_creation" && child)
            || (self.point == "before_dispatch_scheduling" && launch)
        {
            self.stop(&completion.run_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn launch_selector_requires_actual_namespaced_sdk_activity_and_action() {
        let launch = br#"{"call":{"action":"launch"}}"#;
        assert!(intent_launch("IntentPorts::invoke_intent", launch));
        assert!(!intent_launch("invoke_intent", launch));
        assert!(!intent_launch(
            "AuthenticatedPorts::invoke_authenticated",
            launch
        ));
        assert!(!intent_launch(
            "IntentPorts::invoke_intent",
            br#"{"call":{"action":"poll"}}"#
        ));
        assert!(!intent_launch("IntentPorts::invoke_intent", b"invalid"));
    }
    #[test]
    fn exact_scope_does_not_stop_other_workflows() {
        let cp = Checkpoint::exact(
            "/effects".into(),
            "admission_commit".into(),
            "ScopedRegistryV1",
            "one".into(),
        )
        .unwrap();
        assert!(cp.selected("ScopedRegistryV1", "one"));
        assert!(!cp.selected("ScopedRegistryV1", "two"));
        assert!(!cp.selected("AuthenticatedRegistryV1", "one"));
    }
    #[test]
    fn replay_or_unknown_completion_cannot_trigger_checkpoint() {
        let cp = Checkpoint::exact(
            "/effects".into(),
            "admission_commit".into(),
            "ScopedRegistryV1",
            "one".into(),
        )
        .unwrap();
        assert!(!cp.live_completion("run"));
        cp.replaying.lock().unwrap().insert("run".into(), true);
        assert!(!cp.live_completion("run"));
        cp.replaying.lock().unwrap().insert("run".into(), false);
        assert!(cp.live_completion("run"));
    }
    #[test]
    fn unsupported_checkpoint_combinations_fail_closed() {
        for (kind, point) in [
            ("Unknown", "admission_commit"),
            ("IntentWorkflowV1", "after_child_creation"),
            ("ScopedRegistryV1", "before_dispatch_scheduling"),
        ] {
            assert!(
                Checkpoint::exact("/effects".into(), point.into(), kind, "one".into()).is_err()
            );
        }
        assert!(Checkpoint::exact(
            "/tmp".into(),
            "admission_commit".into(),
            "IntentWorkflowV1",
            "one".into()
        )
        .is_err());
        assert!(Checkpoint::exact(
            "/effects".into(),
            "admission_commit".into(),
            "IntentWorkflowV1",
            "".into()
        )
        .is_err());
    }
}
