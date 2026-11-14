//! Disposable offline executable check of real byte storage and default denial.
#[cfg(feature = "artifact-store")]
pub fn run() -> anyhow::Result<serde_json::Value> {
    use runspindle_core::{durable::*, runtime::Binding};
    let root =
        std::env::temp_dir().join(format!("runspindle-offline-smoke-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let cleanup = Cleanup(root);
    let path = cleanup.0.join("core.sqlite");
    let binding = Binding {
        task_id: "synthetic-task".into(),
        revision: 1,
        operation_id: "synthetic-operation".into(),
        policy_revision: "synthetic-policy".into(),
        host_id: "synthetic-host".into(),
        workspace_id: "synthetic-workspace".into(),
        lease_epoch: 1,
    };
    let bytes = b"offline synthetic development artifact\x00\xff";
    let mut store = Store::open(&path)?;
    anyhow::ensure!(
        store
            .put("future", binding.clone(), "artifact".into(), 1, bytes)
            .is_err(),
        "unsupported store version accepted"
    );
    let reference = store.put(VERSION, binding.clone(), "artifact".into(), 1, bytes)?;
    anyhow::ensure!(
        store
            .put(VERSION, binding, "artifact".into(), 1, b"changed")
            .is_err(),
        "changed artifact accepted"
    );
    let validation = Validation {
        artifact: reference.clone(),
        validator_revision: "synthetic-validator".into(),
        policy_revision: "synthetic-policy".into(),
        passed: true,
    };
    anyhow::ensure!(
        store.accept(VERSION, &validation, &Deny).is_err(),
        "unverified acceptance authorized"
    );
    drop(store);
    let recovered = Store::open(&path)?;
    anyhow::ensure!(
        recovered.bytes(&reference)? == bytes,
        "reopened bytes mismatch"
    );
    Ok(
        serde_json::json!({"status":"passed","scope":"offline_development_subset","artifact_sha256":reference.sha256,
        "reopened_bytes":true,"changed_bytes":"denied","unverified_acceptance":"denied","production_activation":"unsupported"}),
    )
}
