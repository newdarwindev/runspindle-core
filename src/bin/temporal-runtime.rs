//! Explicit synthetic development CLI; never resolves credentials or production configuration.
use runspindle_core::runtime::*;
use serde::Deserialize;
use std::{io::Read, path::PathBuf, time::Duration};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowStartOptions,
};
use temporalio_sdk::{
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
    Runtime, Worker, WorkerOptions,
};

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Start {
        registry_id: String,
    },
    Admit {
        registry_id: String,
        admission: Admission,
    },
    Registry {
        registry_id: String,
    },
    Operation {
        job_id: String,
    },
    Cancel {
        job_id: String,
        binding: Binding,
    },
    History {
        workflow_id: String,
    },
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("replay") {
        let path = args
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("history path required"))?;
        let history = temporalio_client::WorkflowHistory::from_json(&std::fs::read(path)?)?;
        let replayer = WorkflowReplayer::new(
            WorkflowReplayerOptions::new()
                .register_workflow::<AdmissionRegistry>()?
                .register_workflow::<OperationWorkflow>()?
                .build(),
        )?;
        replayer.replay_workflow(history).await?;
        println!("{{\"status\":\"passed\",\"evidence_class\":\"isolated_integration\",\"check\":\"history_replay\"}}");
        return Ok(());
    }
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    anyhow::ensure!(
        matches!(mode, "worker" | "client"),
        "expected worker, client or replay"
    );
    // Deliberately no environment profile/API key/credential discovery.
    let target = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("http://temporal:7233");
    anyhow::ensure!(
        target == "http://temporal:7233",
        "development lane target must be explicit internal temporal:7233"
    );
    let conn = Connection::connect(
        ConnectionOptions::new(target.parse::<url::Url>()?)
            .connect_timeout(Duration::from_secs(5))
            .build(),
    )
    .await?;
    let client = Client::new(conn, ClientOptions::new("default").build())?;
    if mode == "worker" {
        let runtime = Runtime::from_current_tokio(Default::default())?;
        let script_directory = args.get(3).map(PathBuf::from);
        let options = WorkerOptions::new(QUEUE)
            .register_workflow::<AdmissionRegistry>()?
            .register_workflow::<OperationWorkflow>()?
            .register_activities(Ports { script_directory })
            .build();
        Worker::new(&runtime, client, options)?
            .run_until(tokio::signal::ctrl_c())
            .await?;
        return Ok(());
    }
    let mut input = String::new();
    if let Some(path) = args.get(3) {
        std::fs::File::open(path)?
            .take(1_048_577)
            .read_to_string(&mut input)?;
    } else {
        std::io::stdin()
            .take(1_048_577)
            .read_to_string(&mut input)?;
    }
    anyhow::ensure!(input.len() <= 1_048_576, "oversized input");
    let command: Command = serde_json::from_str(&input)?;
    let result = match command {
        Command::Start { registry_id } => {
            anyhow::ensure!(valid_id(&registry_id), "invalid registry id");
            let h = client
                .start_workflow(
                    AdmissionRegistry::run,
                    (),
                    WorkflowStartOptions::new(QUEUE, registry_id).build(),
                )
                .await?;
            serde_json::json!({"status":"started", "run_id":h.run_id()})
        }
        Command::Admit {
            registry_id,
            admission,
        } => {
            anyhow::ensure!(valid_id(&registry_id), "invalid registry id");
            serde_json::to_value(
                client
                    .get_workflow_handle::<AdmissionRegistry>(registry_id)
                    .execute_update(AdmissionRegistry::admit, admission, Default::default())
                    .await?,
            )?
        }
        Command::Registry { registry_id } => serde_json::to_value(
            client
                .get_workflow_handle::<AdmissionRegistry>(registry_id)
                .query(AdmissionRegistry::snapshot, (), Default::default())
                .await?,
        )?,
        Command::Operation { job_id } => serde_json::to_value(
            client
                .get_workflow_handle::<OperationWorkflow>(job_id)
                .query(OperationWorkflow::snapshot, (), Default::default())
                .await?,
        )?,
        Command::Cancel { job_id, binding } => {
            serde_json::json!({"accepted":client.get_workflow_handle::<OperationWorkflow>(job_id)
            .execute_update(OperationWorkflow::request_cancel, binding, Default::default()).await?})
        }
        Command::History { workflow_id } => {
            let bytes = client
                .get_workflow_handle::<temporalio_client::UntypedWorkflow>(workflow_id)
                .fetch_history(Default::default())
                .to_json()
                .await?;
            serde_json::from_slice(&bytes)?
        }
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
