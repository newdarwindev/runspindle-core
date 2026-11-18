//! Explicit development-only pinned Python Browser fixture transport.
//! Fixed executable, mounts and embedded helper bytes; never workflow-selected.
use crate::browser_effect_wire::{Action, MAX_REQUEST, MAX_RESPONSE};
use std::{
    io::{Read, Write},
    path::Path,
    process::{Child, Command, Stdio},
    thread::JoinHandle,
    time::{Duration, Instant},
};
const HELPER: &[u8] = include_bytes!("../tests/fixtures/browser_effect_peer.py");
const PIN: &[u8] = include_bytes!("../tests/fixtures/browser_source_pin.json");
fn exact_file(path: &Path, expected: &[u8]) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() == expected.len() as u64,
        "fixture bytes unavailable"
    );
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(expected.len() as u64 + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes == expected, "fixture bytes mismatch");
    Ok(())
}
struct Process {
    child: Child,
    writer: Option<JoinHandle<std::io::Result<()>>>,
    reader: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
fn run(raw: &[u8], action: Action, current: impl Fn() -> bool) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(raw.len() <= MAX_REQUEST, "fixture request capacity");
    let mount = Path::new("/browser-fixture");
    anyhow::ensure!(
        std::fs::symlink_metadata(mount)?.is_dir(),
        "fixture mount unavailable"
    );
    exact_file(&mount.join("browser_effect_peer.py"), HELPER)?;
    exact_file(&mount.join("browser_source_pin.json"), PIN)?;
    anyhow::ensure!(current(), "fixture authority denied");
    let child = Command::new("/usr/bin/python3")
        .args([
            "-I",
            "-B",
            "/browser-fixture/browser_effect_peer.py",
            "--source",
            "/browser-source",
            "--root",
            "/effects/browser-peer",
            "--action",
            if action == Action::Dispatch {
                "dispatch"
            } else {
                "reconcile"
            },
        ])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut process = Process {
        child,
        writer: None,
        reader: None,
    };
    let mut input = process
        .child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("fixture input"))?;
    let output = process
        .child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("fixture output"))?;
    let bytes = raw.to_vec();
    process.writer = Some(
        std::thread::Builder::new()
            .name("browser-fixture-input".into())
            .spawn(move || input.write_all(&bytes))?,
    );
    process.reader = Some(
        std::thread::Builder::new()
            .name("browser-fixture-output".into())
            .spawn(move || {
                let mut bytes = Vec::new();
                output
                    .take(MAX_RESPONSE as u64 + 1)
                    .read_to_end(&mut bytes)?;
                Ok(bytes)
            })?,
    );
    let end = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = process.child.try_wait()? {
            break status;
        }
        anyhow::ensure!(Instant::now() < end, "fixture deadline");
        std::thread::sleep(Duration::from_millis(5));
    };
    let written = process.writer.take().expect("owned writer").join();
    let output = process.reader.take().expect("owned reader").join();
    // All child I/O and process resources are reaped before the deliberate
    // Core-worker crash. This trusted fixture marker is never workflow input.
    drop(process);
    if action == Action::Dispatch
        && Path::new("/effects/browser-peer/crash-core-after-exchange").is_file()
    {
        std::process::exit(95);
    }
    written.map_err(|_| anyhow::anyhow!("fixture writer"))??;
    let output = output.map_err(|_| anyhow::anyhow!("fixture reader"))??;
    anyhow::ensure!(
        status.success() && output.len() <= MAX_RESPONSE,
        "fixture exchange unavailable"
    );
    Ok(output)
}
pub(crate) fn exchange(
    raw: &[u8],
    action: Action,
    current: impl Fn() -> bool,
) -> anyhow::Result<Vec<u8>> {
    run(raw, action, current).map_err(|_| anyhow::anyhow!("pinned browser fixture unavailable"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mounted_helper_must_match_embedded_bytes_and_not_be_symlink() {
        let path = std::env::temp_dir().join(format!("browser-helper-pin-{}", std::process::id()));
        std::fs::write(&path, HELPER).unwrap();
        assert!(exact_file(&path, HELPER).is_ok());
        let mut changed = HELPER.to_vec();
        changed[0] ^= 1;
        std::fs::write(&path, &changed).unwrap();
        assert!(exact_file(&path, HELPER).is_err());
        std::fs::write(&path, vec![0u8; HELPER.len() + 1]).unwrap();
        assert!(exact_file(&path, HELPER).is_err());
        std::fs::remove_file(&path).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("/etc/hosts", &path).unwrap();
            assert!(exact_file(&path, HELPER).is_err());
            std::fs::remove_file(&path).unwrap();
        }
    }
    #[test]
    fn oversized_exchange_fails_before_any_helper_launch() {
        let error = exchange(&vec![b'x'; MAX_REQUEST + 1], Action::Dispatch, || {
            panic!("must not invoke authority")
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "pinned browser fixture unavailable");
        assert_eq!(error.chain().count(), 1);
    }
}
