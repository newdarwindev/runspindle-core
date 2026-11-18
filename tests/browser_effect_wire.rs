#![cfg(feature = "artifact-store")]
use runspindle_core::browser_effect_wire::*;
use runspindle_core::durable::digest;
use serde_json::{json, Value};
use std::cell::Cell;

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
struct Grant {
    allowed: Cell<bool>,
}
impl Authority for Grant {
    fn verify(&self, actual: &Target, hash: &str, _: Action) -> bool {
        self.allowed.get() && actual == &target() && hash == digest(b"x")
    }
}
fn grant() -> Grant {
    Grant {
        allowed: Cell::new(true),
    }
}
fn response(raw: &[u8], outcome: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"version":VERSION,"request_sha256":digest(raw),
        "evidence_class":EVIDENCE_CLASS,"outcome":outcome}))
    .unwrap()
}
struct Spy<F> {
    calls: Vec<Vec<u8>>,
    callback: F,
}
impl<F: FnMut(&[u8]) -> anyhow::Result<Vec<u8>>> Peer for Spy<F> {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.calls.push(request.to_vec());
        (self.callback)(request)
    }
}
fn spy<F: FnMut(&[u8]) -> anyhow::Result<Vec<u8>>>(callback: F) -> Spy<F> {
    Spy {
        calls: Vec::new(),
        callback,
    }
}
#[test]
fn literal_browser_request_and_response_vector_is_exact() {
    let client = Client::new(target(), b"x").unwrap();
    let raw = br#"{"version":"runspindle.browser-effect.v1","action":"dispatch","target":{"binding":{"principal":"principal","account":"account","browser":"browser","tab":"tab","intent":"intent","revision":1,"operation":"operation","epoch":1,"origin":"https://example.invalid"},"native_session":"session","frame":"frame","frame_origin":"https://example.invalid"},"payload_digest":"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881","payload_hex":"78"}"#;
    let reply = br#"{"evidence_class":"browser_guarded_effect_observation","outcome":"unknown","request_sha256":"676116b8037e2e3c4d32b5e5bfc6d61e3830a448819c83a4a8209723c24b7a89","version":"runspindle.browser-effect.v1"}"#;
    let mut peer = spy(|request| {
        assert_eq!(request, raw);
        Ok(reply.to_vec())
    });
    assert_eq!(
        client.dispatch(&grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn separately_authorized_reconcile_has_null_payload_and_terminal_outcomes() {
    struct ReconcileOnly;
    impl Authority for ReconcileOnly {
        fn verify(&self, actual: &Target, hash: &str, action: Action) -> bool {
            actual == &target() && hash == digest(b"x") && action == Action::Reconcile
        }
    }
    let client = Client::new(target(), b"x").unwrap();
    for (name, expected) in [
        ("confirmed", Outcome::Confirmed),
        ("rejected", Outcome::Rejected),
        ("unknown", Outcome::Unknown),
    ] {
        let mut peer = spy(|raw| {
            let request: Value = serde_json::from_slice(raw).unwrap();
            assert_eq!(request["action"], "reconcile");
            assert!(request["payload_hex"].is_null());
            assert_eq!(request["payload_digest"], digest(b"x"));
            Ok(response(raw, name))
        });
        assert!(client.dispatch(&ReconcileOnly, &mut peer).is_err());
        assert!(peer.calls.is_empty());
        assert_eq!(
            client.reconcile(&ReconcileOnly, &mut peer).unwrap(),
            expected
        );
        assert_eq!(peer.calls.len(), 1);
    }
}
#[test]
fn malformed_unbound_future_duplicate_and_oversized_observations_stay_unknown() {
    let client = Client::new(target(), b"x").unwrap();
    for mode in 0..11 {
        let mut peer = spy(|raw| {
            let mut value: Value = serde_json::from_slice(&response(raw, "confirmed")).unwrap();
            match mode {
                0 => return Ok(b"SYNTHETIC-PRIVATE-not-json".to_vec()),
                1 => value["version"] = json!("future"),
                2 => value["request_sha256"] = json!("a".repeat(64)),
                3 => value["evidence_class"] = json!("transport_ack"),
                4 => value["outcome"] = json!("accepted"),
                5 => value["outcome"] = json!(true),
                6 => value["private-extra"] = json!("SYNTHETIC-PRIVATE"),
                7 => {
                    value.as_object_mut().unwrap().remove("request_sha256");
                }
                8 => return Ok(vec![b'x'; MAX_RESPONSE + 1]),
                9 => {
                    let wire = String::from_utf8(serde_json::to_vec(&value).unwrap()).unwrap();
                    return Ok(wire
                        .replace(
                            "\"outcome\":\"confirmed\"",
                            "\"outcome\":\"confirmed\",\"outcome\":\"confirmed\"",
                        )
                        .into_bytes());
                }
                _ => return Ok(vec![0xff]),
            }
            Ok(serde_json::to_vec(&value).unwrap())
        });
        assert_eq!(
            client.dispatch(&grant(), &mut peer).unwrap(),
            Outcome::Unknown,
            "mode {mode}"
        );
        assert_eq!(peer.calls.len(), 1);
    }
}
#[test]
fn missing_or_stale_authority_has_no_peer_effect() {
    let client = Client::new(target(), b"x").unwrap();
    let mut peer = spy(|raw| Ok(response(raw, "confirmed")));
    assert!(client.dispatch(&Deny, &mut peer).is_err());
    let revoked = Grant {
        allowed: Cell::new(false),
    };
    assert!(client.reconcile(&revoked, &mut peer).is_err());
    assert!(peer.calls.is_empty());
}
#[test]
fn post_callback_authority_loss_never_promotes_even_confirmed_response() {
    let client = Client::new(target(), b"x").unwrap();
    let authority = grant();
    let mut peer = spy(|raw| {
        authority.allowed.set(false);
        Ok(response(raw, "confirmed"))
    });
    let error = client.dispatch(&authority, &mut peer).unwrap_err();
    assert_eq!(error.to_string(), "browser effect authority changed");
    assert_eq!(error.chain().count(), 1);
    assert_eq!(peer.calls.len(), 1);
    assert!(client.dispatch(&authority, &mut peer).is_err());
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn peer_error_is_unknown_without_error_disclosure_or_retry() {
    let client = Client::new(target(), b"x").unwrap();
    let mut peer = spy(|_| Err(anyhow::anyhow!("SYNTHETIC-PRIVATE").context("secret-context")));
    assert_eq!(
        client.dispatch(&grant(), &mut peer).unwrap(),
        Outcome::Unknown
    );
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn construction_copies_payload_and_target_without_mutable_identity_access() {
    let mut original_target = target();
    let mut bytes = b"x".to_vec();
    let client = Client::new(original_target.clone(), &bytes).unwrap();
    original_target.binding.account = "other".into();
    bytes[0] = b'y';
    let mut peer = spy(|raw| {
        let value: Value = serde_json::from_slice(raw).unwrap();
        assert_eq!(value["target"]["binding"]["account"], "account");
        assert_eq!(value["payload_hex"], "78");
        Ok(response(raw, "unknown"))
    });
    client.dispatch(&grant(), &mut peer).unwrap();
    assert_eq!(client.target(), &target());
    assert_eq!(client.payload_digest(), digest(b"x"));
}
#[test]
fn invalid_identity_origin_epoch_or_payload_denies_before_transport_exists() {
    for mode in 0..12 {
        let mut t = target();
        match mode {
            0 => t.binding.epoch = 0,
            1 => t.binding.revision = u64::MAX,
            2 => t.binding.account = ".bad".into(),
            3 => t.frame = "x".repeat(129),
            4 => t.binding.origin = "https://example.invalid/".into(),
            5 => t.frame_origin = "https://secret@example.invalid".into(),
            6 => t.binding.origin = "http://example.invalid".into(),
            7 => t.binding.origin = "https://example.invalid?x=1".into(),
            8 => t.native_session.clear(),
            9 => t.binding.origin = "https://example.invalid\0".into(),
            10 => t.frame_origin = "https://example.invalid\n".into(),
            _ => t.binding.origin = "https://example.invalid\u{a0}".into(),
        }
        assert!(Client::new(t, b"x").is_err(), "mode {mode}");
    }
    assert!(Client::new(target(), b"").is_err());
    assert!(Client::new(target(), &vec![b'x'; MAX_PAYLOAD + 1]).is_err());
    assert!(Client::new(target(), &vec![b'x'; MAX_PAYLOAD]).is_ok());
}
