#[path = "checkpoints/mod.rs"]
mod admission_checkpoints;
#[cfg(feature = "artifact-store")]
#[path = "smoke/mod.rs"]
mod smoke;
// Explicit synthetic development CLI; never resolves credentials or production configuration.
#[cfg(feature = "host-runtime")]
use runspindle_core::integration::*;
use runspindle_core::runtime::*;
#[cfg(feature = "delivery-runtime")]
use runspindle_core::{
    delivery_runtime::{artifact_command, DeliveryPorts, DeliveryWorkflow},
    outbox::DeliveryRequest,
};
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
    #[cfg(feature = "delivery-runtime")]
    Deliver {
        workflow_id: String,
        request: DeliveryRequest,
    },
    #[cfg(feature = "delivery-runtime")]
    Delivery {
        workflow_id: String,
    },
    #[cfg(feature = "host-runtime")]
    StartHost {
        registry_id: String,
    },
    #[cfg(feature = "host-runtime")]
    AdmitHost {
        registry_id: String,
        request: HostRequest,
    },
    #[cfg(feature = "host-runtime")]
    HostRegistry {
        registry_id: String,
    },
    #[cfg(feature = "host-runtime")]
    Native {
        job_id: String,
    },
    #[cfg(feature = "host-runtime")]
    CancelNative {
        job_id: String,
        binding: Binding,
    },
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
    #[cfg(feature = "artifact-store")]
    if args.get(1).map(String::as_str) == Some("offline-smoke") {
        anyhow::ensure!(
            args.len() == 2,
            "offline smoke accepts no target or configuration"
        );
        println!("{}", smoke::run()?);
        return Ok(());
    }
    if args.get(1).map(String::as_str) == Some("replay") {
        let path = args
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("history path required"))?;
        let history = temporalio_client::WorkflowHistory::from_json(&std::fs::read(path)?)?;
        let replay_options = WorkflowReplayerOptions::new()
            .register_workflow::<AdmissionRegistry>()?
            .register_workflow::<OperationWorkflow>()?;
        #[cfg(feature = "host-runtime")]
        let replay_options = replay_options
            .register_workflow::<NativeOperationWorkflow>()?
            .register_workflow::<HostAdmissionRegistry>()?;
        #[cfg(feature = "delivery-runtime")]
        let replay_options = replay_options.register_workflow::<DeliveryWorkflow>()?;
        let replayer = WorkflowReplayer::new(replay_options.build())?;
        replayer.replay_workflow(history).await?;
        println!("{{\"status\":\"passed\",\"evidence_class\":\"isolated_integration\",\"check\":\"history_replay\"}}");
        return Ok(());
    }
    #[cfg(feature = "delivery-runtime")]
    if args.get(1).map(String::as_str) == Some("artifact") {
        anyhow::ensure!(
            args.get(2).map(String::as_str) == Some("/effects"),
            "explicit synthetic /effects directory required"
        );
        let action = args
            .get(3)
            .ok_or_else(|| anyhow::anyhow!("artifact action required"))?;
        println!(
            "{}",
            artifact_command(std::path::Path::new("/effects"), action)?
        );
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
            .register_activities(Ports {
                script_directory: script_directory.clone(),
            });
        #[cfg(feature = "host-runtime")]
        let options = options
            .register_workflow::<NativeOperationWorkflow>()?
            .register_workflow::<HostAdmissionRegistry>()?
            .register_activities(HostPorts {
                directory: script_directory,
            });
        #[cfg(feature = "delivery-runtime")]
        let options = options
            .register_workflow::<DeliveryWorkflow>()?
            .register_activities(DeliveryPorts {
                directory: args.get(3).map(PathBuf::from),
            });
        let mut options = options.build();
        if let Some(point) = args.get(4) {
            options.worker_interceptor(admission_checkpoints::Checkpoint::new(
                args.get(3)
                    .map(PathBuf::from)
                    .ok_or_else(|| anyhow::anyhow!("synthetic directory required"))?,
                point.clone(),
            )?);
        }
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
        #[cfg(feature = "delivery-runtime")]
        Command::Deliver {
            workflow_id,
            request,
        } => {
            anyhow::ensure!(
                valid_id(&workflow_id) && request.valid(),
                "invalid delivery workflow request"
            );
            let h = client
                .start_workflow(
                    DeliveryWorkflow::run,
                    request,
                    WorkflowStartOptions::new(QUEUE, workflow_id.clone()).build(),
                )
                .await?;
            serde_json::json!({"workflow_id":workflow_id,"run_id":h.run_id()})
        }
        #[cfg(feature = "delivery-runtime")]
        Command::Delivery { workflow_id } => serde_json::to_value(
            client
                .get_workflow_handle::<DeliveryWorkflow>(workflow_id)
                .query(DeliveryWorkflow::snapshot, (), Default::default())
                .await?,
        )?,
        #[cfg(feature = "host-runtime")]
        Command::StartHost { registry_id } => {
            anyhow::ensure!(valid_id(&registry_id), "invalid registry");
            let h = client
                .start_workflow(
                    HostAdmissionRegistry::run,
                    (),
                    WorkflowStartOptions::new(QUEUE, registry_id).build(),
                )
                .await?;
            serde_json::json!({"status":"started","run_id":h.run_id()})
        }
        #[cfg(feature = "host-runtime")]
        Command::AdmitHost {
            registry_id,
            request,
        } => serde_json::to_value(
            client
                .get_workflow_handle::<HostAdmissionRegistry>(registry_id)
                .execute_update(HostAdmissionRegistry::admit, request, Default::default())
                .await?,
        )?,
        #[cfg(feature = "host-runtime")]
        Command::HostRegistry { registry_id } => serde_json::to_value(
            client
                .get_workflow_handle::<HostAdmissionRegistry>(registry_id)
                .query(HostAdmissionRegistry::snapshot, (), Default::default())
                .await?,
        )?,
        #[cfg(feature = "host-runtime")]
        Command::Native { job_id } => serde_json::to_value(
            client
                .get_workflow_handle::<NativeOperationWorkflow>(job_id)
                .query(NativeOperationWorkflow::snapshot, (), Default::default())
                .await?,
        )?,
        #[cfg(feature = "host-runtime")]
        Command::CancelNative { job_id, binding } => {
            serde_json::json!({"accepted":client.get_workflow_handle::<NativeOperationWorkflow>(job_id).execute_update(NativeOperationWorkflow::request_cancel,binding,Default::default()).await?})
        }
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
    if let Some(point) = args.get(4) {
        anyhow::ensure!(
            point == "before_reply" && args.get(3).is_some(),
            "unsupported client checkpoint"
        );
        anyhow::ensure!(
            result.get("status").and_then(|v| v.as_str()) == Some("admitted"),
            "admitted reply required"
        );
        std::process::exit(96);
    }
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
