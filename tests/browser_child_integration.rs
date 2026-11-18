#![cfg(all(feature = "browser-child-peer", target_os = "linux"))]
use runspindle_core::{
    browser_child_peer::OwnedChildPeer, browser_effect_registry::BrowserRegistry,
    browser_effect_wire::*,
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};
const PIN: &str = "379f020c650f3571867a17c6e84f28ff953d6770";
const PAYLOAD: &[u8] = b"synthetic-pipe-action";
struct TestAuthority;
impl Authority for TestAuthority {
    fn verify(&self, target: &Target, hash: &str, _: Action) -> bool {
        target == &fixture_target() && hash == runspindle_core::durable::digest(PAYLOAD)
    }
}
fn fixture_target() -> Target {
    Target {
        binding: Binding {
            principal: "fixture".into(),
            account: "fixture-account".into(),
            browser: "fixture-browser".into(),
            tab: "fixture-tab".into(),
            intent: "fixture-intent".into(),
            revision: 1,
            operation: "fixture-operation".into(),
            epoch: 1,
            origin: "https://fixture.invalid".into(),
        },
        native_session: "fixture-session".into(),
        frame: "fixture-frame".into(),
        frame_origin: "https://fixture.invalid".into(),
    }
}
fn child(root: &Path, state: &Path, action: &str, lose: bool) -> OwnedChildPeer {
    // Explicit test application opts in after env_clear; no production issuer.
    let mut args = vec!["-I".into(), "-B".into(), "-c".into(), "import os,runpy,sys;os.environ['RUNSPINDLE_SYNTHETIC_PIPE_APP']='1';p=sys.argv.pop(1);sys.argv[0]=p;runpy.run_path(p,run_name='__main__')".into(), root.join("tests/fixtures/effect_pipe_app.py").to_str().unwrap().into(), "--site-state".into(), state.to_str().unwrap().into(), "--action".into(), action.into()];
    if lose {
        args.push("--lose-response".into());
    }
    OwnedChildPeer::new(
        PathBuf::from("/usr/bin/python3"),
        args,
        Duration::from_secs(5),
    )
    .unwrap()
}
#[test]
#[ignore = "requires explicit exact pinned Browser fixture checkout; no fallback"]
fn actual_receiver_lost_reply_reopen_reconciles_without_resubmit() {
    let root = PathBuf::from(
        std::env::var("RUNSPINDLE_BROWSER_PIPE_SOURCE").expect("exact Browser source required"),
    );
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(head.status.success());
    assert_eq!(String::from_utf8(head.stdout).unwrap().trim(), PIN);
    assert!(source_clean(&root));
    let dir = std::env::temp_dir().join(format!(
        "core-browser-pipe-integration-{}",
        std::process::id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let db = dir.join("core.sqlite");
    let site = dir.join("target-owned.txt");
    let target = fixture_target();
    let mut registry = BrowserRegistry::open(&db).unwrap();
    assert_eq!(
        registry
            .dispatch(
                target.clone(),
                PAYLOAD,
                &TestAuthority,
                &mut child(&root, &site, "dispatch", true)
            )
            .unwrap(),
        Outcome::Unknown
    );
    assert_eq!(std::fs::read(&site).unwrap(), b"accepted\n");
    drop(registry);
    let mut registry = BrowserRegistry::open(&db).unwrap();
    assert_eq!(
        registry
            .dispatch(
                target.clone(),
                PAYLOAD,
                &TestAuthority,
                &mut child(&root, &site, "dispatch", false)
            )
            .unwrap(),
        Outcome::Unknown
    );
    assert_eq!(
        registry
            .reconcile(
                &target,
                &TestAuthority,
                &mut child(&root, &site, "reconcile", false)
            )
            .unwrap(),
        Outcome::Confirmed
    );
    assert_eq!(std::fs::read(&site).unwrap(), b"accepted\n");
    assert!(registry
        .reconcile(&target, &Deny, &mut child(&root, &site, "reconcile", false))
        .is_err());
    drop(registry);
    std::fs::remove_dir_all(dir).unwrap();
}

fn source_clean(root: &Path) -> bool {
    let status = Command::new("git")
        .args([
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--ignored=matching",
        ])
        .current_dir(root)
        .output()
        .unwrap();
    status.status.success() && status.stdout.is_empty()
}
#[test]
fn ignored_bytecode_is_not_an_exact_source_checkout() {
    let root =
        std::env::temp_dir().join(format!("core-browser-source-gate-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let git = |args: &[&str]| {
        let result = Command::new("git")
            .args(args)
            .current_dir(&root)
            .output()
            .unwrap();
        assert!(result.status.success());
    };
    git(&["init", "--quiet"]);
    std::fs::write(root.join(".gitignore"), "__pycache__/\n").unwrap();
    git(&["add", ".gitignore"]);
    git(&[
        "-c",
        "user.name=Synthetic Test",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Synthetic gate fixture",
    ]);
    assert!(source_clean(&root));
    std::fs::create_dir(root.join("__pycache__")).unwrap();
    std::fs::write(
        root.join("__pycache__/ignored.pyc"),
        b"synthetic bytecode canary",
    )
    .unwrap();
    assert!(!source_clean(&root));
    std::fs::remove_dir_all(root).unwrap();
}
