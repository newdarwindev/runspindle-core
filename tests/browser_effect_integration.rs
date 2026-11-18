#![cfg(feature = "artifact-store")]
//! Explicit pinned Browser package; trusted fixture has no network/browser adapter.
use runspindle_core::browser_effect_wire::*;
use std::{
    io::{Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "core-browser-wire-{}-{}",
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
struct Allow;
impl Authority for Allow {
    fn verify(&self, _: &Target, _: &str, _: Action) -> bool {
        true
    }
}
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct PythonPeer {
    source: PathBuf,
    root: PathBuf,
    action: &'static str,
    discard: bool,
}
impl Peer for PythonPeer {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(request.len() < 4096, "fixture request bound");
        let mut child = Child(
            Command::new("python3")
                .arg(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/fixtures/browser_effect_peer.py"),
                )
                .arg("--source")
                .arg(&self.source)
                .arg("--root")
                .arg(&self.root)
                .arg("--action")
                .arg(self.action)
                .env("PYTHONDONTWRITEBYTECODE", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?,
        );
        child.0.stdin.take().unwrap().write_all(request)?;
        let stdout = child.0.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout.take(4097).read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.0.try_wait()? {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.0.kill()?;
                child.0.wait()?;
                let _ = reader.join();
                anyhow::bail!("fixture deadline");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let bytes = reader
            .join()
            .map_err(|_| anyhow::anyhow!("fixture reader"))??;
        anyhow::ensure!(
            status.success() && bytes.len() <= 4096 && !self.discard,
            "fixture exchange unavailable"
        );
        Ok(bytes)
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
fn setup(temp: &Temp) -> PythonPeer {
    // Literal expectation is independent of Client encoding.
    std::fs::write(temp.0.join("target.json"),r#"{"binding":{"principal":"principal","account":"account","browser":"browser","tab":"tab","intent":"intent","revision":1,"operation":"operation","epoch":1,"origin":"https://example.invalid"},"native_session":"session","frame":"frame","frame_origin":"https://example.invalid"}"#).unwrap();
    std::fs::write(temp.0.join("payload.bin"), b"x").unwrap();
    PythonPeer {
        source: std::env::var_os("RUNSPINDLE_BROWSER_WIRE_SOURCE")
            .expect("explicit pinned Browser source required")
            .into(),
        root: temp.0.clone(),
        action: "dispatch",
        discard: false,
    }
}
fn counts(temp: &Temp) -> (u64, u64) {
    let db = rusqlite::Connection::open(temp.0.join("site.sqlite")).unwrap();
    (
        db.query_row(
            "SELECT count(*) FROM calls WHERE action='submit'",
            [],
            |r| r.get(0),
        )
        .unwrap(),
        db.query_row(
            "SELECT count(*) FROM calls WHERE action='readback'",
            [],
            |r| r.get(0),
        )
        .unwrap(),
    )
}
#[test]
#[ignore = "requires explicit pinned Browser package"]
fn actual_browser_lost_response_reconstruction_and_readback_never_resubmit() {
    let temp = Temp::new();
    let mut peer = setup(&temp);
    let client = Client::new(target(), b"x").unwrap();
    peer.discard = true;
    assert_eq!(
        client.dispatch(&Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(counts(&temp), (1, 0));
    drop(client);
    drop(peer);
    // Both Core client and Python receiver are reconstructed. Browser SQLite
    // retains the original reservation, not a process-local fake object.
    let client = Client::new(target(), b"x").unwrap();
    let mut peer = setup(&temp);
    assert_eq!(
        client.dispatch(&Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(counts(&temp), (1, 0));
    peer.action = "reconcile";
    std::fs::write(temp.0.join("unresolved"), b"").unwrap();
    assert_eq!(
        client.reconcile(&Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(counts(&temp), (1, 1));
    std::fs::remove_file(temp.0.join("unresolved")).unwrap();
    assert_eq!(
        client.reconcile(&Allow, &mut peer).unwrap(),
        Outcome::Confirmed
    );
    assert_eq!(counts(&temp), (1, 2));
    std::fs::write(temp.0.join("revoked"), b"").unwrap();
    assert_eq!(
        client.reconcile(&Allow, &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(counts(&temp), (1, 2));
    let db = rusqlite::Connection::open(temp.0.join("site.sqlite")).unwrap();
    let bytes: Vec<u8> = db
        .query_row("SELECT payload FROM effects", [], |r| r.get(0))
        .unwrap();
    assert_eq!(bytes, b"x");
}
#[test]
#[ignore = "requires explicit pinned Browser package"]
fn actual_browser_wrong_target_digest_or_action_has_no_site_effect() {
    for variant in 0..4 {
        let temp = Temp::new();
        let mut peer = setup(&temp);
        let mut t = target();
        if variant == 0 {
            t.binding.account = "other".into();
        }
        if variant == 1 {
            t.binding.epoch = 2;
        }
        let client = Client::new(t, if variant == 2 { b"y" } else { b"x" }).unwrap();
        let outcome = if variant == 3 {
            client.reconcile(&Allow, &mut peer)
        } else {
            client.dispatch(&Allow, &mut peer)
        };
        assert_eq!(outcome.unwrap(), Outcome::Unknown);
        assert_eq!(counts(&temp), (0, 0));
    }
}
