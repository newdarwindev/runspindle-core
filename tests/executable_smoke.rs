#![cfg(feature = "artifact-store")]
#[test]
fn installed_entrypoint_checks_storage_and_rejects_configuration() {
    let binary = env!("CARGO_BIN_EXE_temporal-runtime");
    let result = std::process::Command::new(binary)
        .arg("offline-smoke")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(record["status"], "passed");
    assert_eq!(record["unverified_acceptance"], "denied");
    let denied = std::process::Command::new(binary)
        .args(["offline-smoke", "http://external.invalid"])
        .output()
        .unwrap();
    assert!(!denied.status.success());
}
