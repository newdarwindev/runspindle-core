#![cfg(all(feature = "browser-child-peer", target_os = "linux"))]
use runspindle_core::browser_child_peer::OwnedChildPeer;
use runspindle_core::browser_effect_wire::{Peer, MAX_RESPONSE};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
fn peer(code: &str, milliseconds: u64) -> OwnedChildPeer {
    OwnedChildPeer::new(
        PathBuf::from("/usr/bin/python3"),
        vec!["-I".into(), "-B".into(), "-c".into(), code.into()],
        Duration::from_millis(milliseconds),
    )
    .unwrap()
}
const READ: &str = "import sys,struct\nh=sys.stdin.buffer.read(4)\nn=struct.unpack('>I',h)[0]\nb=sys.stdin.buffer.read(n)\nassert len(b)==n and sys.stdin.buffer.read(1)==b''\n";
#[test]
fn one_frame_eof_and_environment_clear() {
    let code=format!("{READ}import os\nassert not os.environ.get('HOME')\nsys.stdout.buffer.write(struct.pack('>I',len(b))+b)\n");
    assert_eq!(peer(&code, 1000).exchange(b"fixture").unwrap(), b"fixture");
}
#[test]
fn partial_extra_oversized_and_failed_process_are_unknown_errors() {
    for tail in [
        "sys.stdout.buffer.write(b'\\0\\0')",
        "sys.stdout.buffer.write(struct.pack('>I',1)+b'ab')",
        "sys.stdout.buffer.write(struct.pack('>I',5000)+b'x'*5000)",
        "sys.stdout.buffer.write(struct.pack('>I',1)+b'a');sys.exit(1)",
    ] {
        let error = peer(&format!("{READ}{tail}\n"), 1000)
            .exchange(b"fixture")
            .unwrap_err();
        assert_eq!(error.to_string(), "browser child exchange unavailable");
    }
}
#[test]
fn exact_response_bound() {
    let code = format!(
        "{READ}sys.stdout.buffer.write(struct.pack('>I',{MAX_RESPONSE})+b'x'*{MAX_RESPONSE})\n"
    );
    assert_eq!(
        peer(&code, 1000).exchange(b"fixture").unwrap().len(),
        MAX_RESPONSE
    );
}
#[test]
fn blocked_request_and_response_obey_total_deadline() {
    for (code, request) in [
        (
            "import time;time.sleep(10)".to_owned(),
            vec![b'x'; 1_000_000],
        ),
        (
            format!("{READ}import time;time.sleep(10)"),
            b"fixture".to_vec(),
        ),
    ] {
        let now = Instant::now();
        assert!(peer(&code, 100).exchange(&request).is_err());
        assert!(now.elapsed() < Duration::from_secs(2));
    }
}
#[test]
fn descendant_holding_output_cannot_extend_deadline() {
    let code =
        format!("{READ}import os,time\nif os.fork()==0: time.sleep(10)\nelse: sys.exit(0)\n");
    let now = Instant::now();
    assert!(peer(&code, 100).exchange(b"fixture").is_err());
    assert!(now.elapsed() < Duration::from_secs(2));
}
#[test]
fn empty_request_and_invalid_configuration_never_spawn() {
    assert!(peer("raise AssertionError('not entered')", 100)
        .exchange(b"")
        .is_err());
    assert!(
        OwnedChildPeer::new(PathBuf::from("relative"), vec![], Duration::from_secs(1)).is_err()
    );
    assert!(OwnedChildPeer::new(
        PathBuf::from("/usr/bin/python3"),
        vec![],
        Duration::from_secs(31)
    )
    .is_err());
}
