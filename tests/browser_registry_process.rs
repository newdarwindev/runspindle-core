#![cfg(feature = "artifact-store")]
//! Real process exit/concurrent writers; an independent file models a site effect.
use runspindle_core::{browser_effect_registry::BrowserRegistry, browser_effect_wire::*};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "core-browser-registry-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn target() -> Target {
    Target {
        binding: Binding {
            principal: "principal".into(),
            account: "account".into(),
            browser: "browser".into(),
            tab: "tab".into(),
            intent: "intent".into(),
            revision: 1,
            operation: "operation".into(),
            epoch: 1,
            origin: "https://example.invalid".into(),
        },
        native_session: "session".into(),
        frame: "frame".into(),
        frame_origin: "https://example.invalid".into(),
    }
}
struct Allow;
impl Authority for Allow {
    fn verify(&self, t: &Target, d: &str, _: Action) -> bool {
        t == &target() && d == runspindle_core::durable::digest(b"x")
    }
}
fn response(request: &[u8], state: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"version":VERSION,"request_sha256":runspindle_core::durable::digest(request),"evidence_class":EVIDENCE_CLASS,"outcome":state})).unwrap()
}
struct Site {
    root: PathBuf,
    mode: String,
    calls: usize,
}
impl Peer for Site {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.calls += 1;
        let value: Value = serde_json::from_slice(request)?;
        if value["action"] == "dispatch" {
            if self.mode == "before" {
                std::process::exit(94);
            }
            let mut effect = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.root.join("effect.json"))?;
            effect.write_all(request)?;
            effect.sync_all()?;
            if self.mode == "after" {
                std::process::exit(95);
            }
            Ok(response(request, "unknown"))
        } else {
            let state = if self.root.join("effect.json").exists() {
                let accepted: Value =
                    serde_json::from_slice(&std::fs::read(self.root.join("effect.json"))?)?;
                anyhow::ensure!(
                    accepted["payload_hex"] == "78"
                        && accepted["target"] == value["target"]
                        && accepted["payload_digest"] == value["payload_digest"],
                    "independent effect mismatch"
                );
                "confirmed"
            } else {
                "unknown"
            };
            Ok(response(request, state))
        }
    }
}
#[test]
fn registry_child() {
    let Ok(root) = std::env::var("CORE_BROWSER_REGISTRY_CHILD") else {
        return;
    };
    let root = PathBuf::from(root);
    let mode = std::env::var("CORE_BROWSER_REGISTRY_MODE").unwrap();
    let registry = BrowserRegistry::open(root.join("core.sqlite"));
    if mode == "damaged" {
        std::fs::write(
            root.join("open-damaged"),
            if registry.is_err() {
                "blocked"
            } else {
                "opened"
            },
        )
        .unwrap();
        let mut peer = Site {
            root: root.clone(),
            mode,
            calls: 0,
        };
        let outcome =
            registry.and_then(|mut registry| registry.dispatch(target(), b"x", &Allow, &mut peer));
        std::fs::write(root.join("calls-damaged"), peer.calls.to_string()).unwrap();
        assert!(outcome.is_err());
        return;
    }
    let mut registry = registry.unwrap();
    if mode == "concurrent" {
        let id = std::env::var("CORE_BROWSER_REGISTRY_ID").unwrap();
        std::fs::write(root.join(format!("ready-{id}")), b"").unwrap();
        let end = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.join("release").exists() {
            assert!(std::time::Instant::now() < end, "release timeout");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    let mut peer = Site {
        root: root.clone(),
        mode,
        calls: 0,
    };
    assert_eq!(
        registry
            .dispatch(target(), b"x", &Allow, &mut peer)
            .unwrap(),
        Outcome::Unknown
    );
    let id = std::env::var("CORE_BROWSER_REGISTRY_ID").unwrap();
    std::fs::write(root.join(format!("calls-{id}")), peer.calls.to_string()).unwrap();
}
fn spawn(root: &Path, mode: &str, id: &str) -> Child {
    Child(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "registry_child", "--nocapture"])
            .env("CORE_BROWSER_REGISTRY_CHILD", root)
            .env("CORE_BROWSER_REGISTRY_MODE", mode)
            .env("CORE_BROWSER_REGISTRY_ID", id)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(root.join(format!("stderr-{id}"))).unwrap())
            .spawn()
            .unwrap(),
    )
}
fn wait(child: &mut Child, root: &Path, id: &str) -> i32 {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            return status.code().unwrap_or(-1);
        }
        assert!(
            std::time::Instant::now() < end,
            "child timeout: {}",
            std::fs::read_to_string(root.join(format!("stderr-{id}"))).unwrap()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
#[test]
fn fresh_process_missing_browser_reservation_blocks_duplicate_dispatch() {
    let t = Temp::new();
    let mut initial = spawn(&t.0, "after", "initial");
    assert_eq!(wait(&mut initial, &t.0, "initial"), 95);
    let effect = std::fs::read(t.0.join("effect.json")).unwrap();

    let db = rusqlite::Connection::open(t.0.join("core.sqlite")).unwrap();
    let trigger: String = db
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='browser_operations_retained'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    db.execute_batch(&format!(
        "DROP TRIGGER browser_operations_retained;DELETE FROM browser_operations;{trigger};"
    ))
    .unwrap();

    let mut recovered = spawn(&t.0, "damaged", "damaged");
    let exit = wait(&mut recovered, &t.0, "damaged");
    assert_eq!(
        std::fs::read_to_string(t.0.join("calls-damaged")).unwrap(),
        "0"
    );
    assert_eq!(
        std::fs::read_to_string(t.0.join("open-damaged")).unwrap(),
        "blocked"
    );
    assert_eq!(exit, 0);
    assert_eq!(std::fs::read(t.0.join("effect.json")).unwrap(), effect);
}
#[test]
fn fresh_process_crash_before_or_after_site_effect_never_restores_dispatch_right() {
    for (mode, exit, expected) in [
        ("before", 94, Outcome::Unknown),
        ("after", 95, Outcome::Confirmed),
    ] {
        let temp = Temp::new();
        let mut child = spawn(&temp.0, mode, "crash");
        assert_eq!(wait(&mut child, &temp.0, "crash"), exit);
        assert_eq!(temp.0.join("effect.json").exists(), mode == "after");
        let mut registry = BrowserRegistry::open(temp.0.join("core.sqlite")).unwrap();
        let mut peer = Site {
            root: temp.0.clone(),
            mode: "recover".into(),
            calls: 0,
        };
        assert!(registry.dispatch(target(), b"x", &Deny, &mut peer).is_err());
        assert!(registry.reconcile(&target(), &Deny, &mut peer).is_err());
        assert_eq!(peer.calls, 0);
        assert_eq!(
            registry
                .dispatch(target(), b"x", &Allow, &mut peer)
                .unwrap(),
            Outcome::Unknown
        );
        assert_eq!(peer.calls, 0);
        assert_eq!(
            registry.reconcile(&target(), &Allow, &mut peer).unwrap(),
            expected
        );
        assert_eq!(peer.calls, 1);
        drop(registry);
        let mut reopened = BrowserRegistry::open(temp.0.join("core.sqlite")).unwrap();
        assert_eq!(
            reopened
                .dispatch(target(), b"x", &Allow, &mut peer)
                .unwrap(),
            Outcome::Unknown
        );
        assert_eq!(peer.calls, 1);
        assert_eq!(
            reopened
                .historical_snapshot(&target())
                .unwrap()
                .unwrap()
                .observation,
            expected
        );
    }
}
#[test]
fn competing_fresh_processes_reserve_one_dispatch() {
    let temp = Temp::new();
    drop(BrowserRegistry::open(temp.0.join("core.sqlite")).unwrap());
    let mut first = spawn(&temp.0, "concurrent", "1");
    let mut second = spawn(&temp.0, "concurrent", "2");
    let end = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !(temp.0.join("ready-1").exists() && temp.0.join("ready-2").exists()) {
        assert!(std::time::Instant::now() < end, "ready timeout");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    std::fs::write(temp.0.join("release"), b"").unwrap();
    let statuses = [
        wait(&mut first, &temp.0, "1"),
        wait(&mut second, &temp.0, "2"),
    ];
    assert_eq!(statuses, [0, 0]);
    let calls = |id: &str| {
        std::fs::read_to_string(temp.0.join(format!("calls-{id}")))
            .unwrap()
            .parse::<usize>()
            .unwrap()
    };
    assert_eq!(calls("1") + calls("2"), 1);
    let effect: Value =
        serde_json::from_slice(&std::fs::read(temp.0.join("effect.json")).unwrap()).unwrap();
    assert_eq!(effect["payload_hex"], "78");
}
