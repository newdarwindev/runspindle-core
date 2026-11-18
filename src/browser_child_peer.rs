//! One exchange over inherited pipes to an explicitly composed trusted endpoint.
//! Linux only, no issuer/default executable/service/network. This transport is
//! not a sandbox or peer authenticator; application composition verifies source.
use crate::browser_effect_wire::{Peer, MAX_REQUEST, MAX_RESPONSE};
use std::{
    io::{Read, Write},
    os::{fd::AsFd, unix::process::CommandExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub struct OwnedChildPeer {
    program: PathBuf,
    args: Vec<String>,
    timeout: Duration,
}
impl OwnedChildPeer {
    pub fn new(program: PathBuf, args: Vec<String>, timeout: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(
            program.is_absolute()
                && args.len() <= 32
                && args.iter().all(|s| s.len() <= 4096 && !s.contains('\0'))
                && timeout >= Duration::from_millis(10)
                && timeout <= Duration::from_secs(30),
            "invalid browser child configuration"
        );
        Ok(Self {
            program,
            args,
            timeout,
        })
    }
    fn run(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            !request.is_empty() && request.len() <= MAX_REQUEST,
            "invalid browser frame"
        );
        let start = Instant::now();
        let child = Command::new(&self.program)
            .args(&self.args)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;
        let mut owned = Owned {
            child,
            stopped: false,
        };
        let mut input = owned.child.stdin.take();
        let mut output = owned
            .child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing pipe"))?;
        nonblocking(
            input
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing pipe"))?,
        )?;
        nonblocking(&output)?;
        let mut frame = (request.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(request);
        let mut sent = 0;
        let mut response = Vec::new();
        let mut eof = false;
        loop {
            anyhow::ensure!(start.elapsed() < self.timeout, "browser child deadline");
            if let Some(writer) = input.as_mut() {
                match writer.write(&frame[sent..]) {
                    Ok(0) => anyhow::bail!("closed browser request pipe"),
                    Ok(n) => sent += n,
                    Err(e) if transient(&e) => (),
                    Err(e) => return Err(e.into()),
                }
                if sent == frame.len() {
                    input.take();
                } // EOF is part of framing.
            }
            if !eof {
                let mut chunk = [0; 4096];
                match output.read(&mut chunk) {
                    Ok(0) => eof = true,
                    Ok(n) => {
                        anyhow::ensure!(
                            response.len() + n <= MAX_RESPONSE + 4,
                            "oversized browser response"
                        );
                        response.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) if transient(&e) => (),
                    Err(e) => return Err(e.into()),
                }
            }
            if eof && finished_unreaped(owned.child.id())? {
                anyhow::ensure!(sent == frame.len(), "incomplete browser request");
                let status = owned.stop()?;
                anyhow::ensure!(status.success(), "browser child failed");
                return decode(response);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }
}
impl Peer for OwnedChildPeer {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        // Never expose OS errors, child argv, stderr or payload via public errors.
        self.run(request)
            .map_err(|_| anyhow::anyhow!("browser child exchange unavailable"))
    }
}
fn decode(frame: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(frame.len() >= 4, "partial browser response");
    let size = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    anyhow::ensure!(
        size > 0 && size <= MAX_RESPONSE && frame.len() == size + 4,
        "invalid browser response frame"
    );
    Ok(frame[4..].to_vec())
}
fn transient(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
    )
}
fn nonblocking(fd: impl AsFd) -> std::io::Result<()> {
    let flags = rustix::fs::fcntl_getfl(&fd)?;
    rustix::fs::fcntl_setfl(fd, flags | rustix::fs::OFlags::NONBLOCK)?;
    Ok(())
}
fn finished_unreaped(pid: u32) -> std::io::Result<bool> {
    use rustix::process::{waitid, Pid, WaitId, WaitIdOptions};
    let pid =
        Pid::from_raw(pid as i32).ok_or_else(|| std::io::Error::other("invalid child pid"))?;
    Ok(waitid(
        WaitId::Pid(pid),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
    )?
    .is_some())
}
struct Owned {
    child: Child,
    stopped: bool,
}
impl Owned {
    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.stopped = true;
        // The direct child is unreaped here; signal only its private process group.
        if let Some(pid) = rustix::process::Pid::from_raw(self.child.id() as i32) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
        self.child.wait()
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        // No status-consuming wait occurs before stop. Never signal after reap.
        if self.stopped {
            return;
        }
        let _ = self.stop();
    }
}
