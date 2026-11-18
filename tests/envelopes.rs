#![cfg(feature = "runtime-envelope")]
use runspindle_core::envelope::{Envelope, Expected, Message};
use serde_json::{json, Value};

#[test]
fn vectors_map_existing_component_types_and_roundtrip() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("../conformance/envelope-v1/vectors.json")).unwrap();
    for v in vectors {
        let bytes = serde_json::to_vec(&v["envelope"]).unwrap();
        let e = Envelope::decode(&bytes).unwrap();
        let (component, back) = match e.message().unwrap() {
            Message::Admission(a) => (
                serde_json::to_value(&a).unwrap(),
                Envelope::admission(e.message_id.clone(), &a).unwrap(),
            ),
            Message::Observation(o) => (
                serde_json::to_value(&o).unwrap(),
                Envelope::observation(e.message_id.clone(), &o).unwrap(),
            ),
            Message::Delivery(r) => (
                serde_json::to_value(&r).unwrap(),
                Envelope::delivery(e.message_id.clone(), &r).unwrap(),
            ),
            Message::Receipt(r) => (
                serde_json::to_value(&r).unwrap(),
                Envelope::receipt(e.message_id.clone(), &r).unwrap(),
            ),
        };
        assert_eq!(component, v["component"]);
        assert_eq!(e.encode().unwrap(), back.encode().unwrap());
    }
}
#[test]
fn malformed_and_mismatched_context_never_reaches_adapter() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("../conformance/envelope-v1/vectors.json")).unwrap();
    let e = &vectors[0]["envelope"];
    let mut invalid = Vec::new();
    for (key, value) in [
        ("version", json!("future")),
        ("message_type", json!("unknown")),
        ("message_id", json!("-bad")),
    ] {
        let mut e = e.clone();
        e[key] = value;
        invalid.push(serde_json::to_vec(&e).unwrap());
    }
    for key in [
        "version",
        "message_type",
        "message_id",
        "binding",
        "payload",
    ] {
        let mut e = e.clone();
        e.as_object_mut().unwrap().remove(key);
        invalid.push(serde_json::to_vec(&e).unwrap());
    }
    for value in [json!(0), json!(-1), json!(1.0), json!(true), json!("1")] {
        let mut e = e.clone();
        e["binding"]["lease_epoch"] = value;
        invalid.push(serde_json::to_vec(&e).unwrap());
    }
    let mut missing = e.clone();
    missing["payload"]
        .as_object_mut()
        .unwrap()
        .remove("predecessor_revision");
    invalid.push(serde_json::to_vec(&missing).unwrap());
    let mut repeated = e.clone();
    repeated["payload"]["binding"] = e["binding"].clone();
    invalid.push(serde_json::to_vec(&repeated).unwrap());
    let raw = serde_json::to_string(e).unwrap();
    invalid.push(
        raw.replace("\"revision\":1", "\"revision\":1,\"revision\":2")
            .into_bytes(),
    );
    invalid.push(
        raw.replace("\"lease_epoch\":1", "\"lease_epoch\":18446744073709551616")
            .into_bytes(),
    );
    let mut calls = Vec::new();
    for bytes in invalid {
        if let Ok(e) = Envelope::decode(&bytes) {
            calls.push(e);
        }
    }
    assert_eq!(calls.len(), 0);
    let mut adapters = Vec::new();
    for v in &vectors {
        let e = Envelope::decode(&serde_json::to_vec(&v["envelope"]).unwrap()).unwrap();
        let mut binding = e.binding.clone();
        binding.lease_epoch = 2;
        if let Ok(m) = e.checked_message(&Expected {
            binding: &binding,
            session: Some("wrong"),
            last_sequence: u64::MAX,
            destination: Some(("account-1", "wrong")),
        }) {
            adapters.push(m);
        }
        if e.message_type != "admission_request" {
            if let Ok(m) = e.checked_message(&Expected {
                binding: &e.binding,
                session: Some("wrong"),
                last_sequence: u64::MAX,
                destination: Some(("account-1", "wrong")),
            }) {
                adapters.push(m);
            }
        }
    }
    assert_eq!(adapters.len(), 0);
}

#[test]
fn wire_diagnostics_never_retain_untrusted_fields_or_values() {
    const SECRET: &str = "private-diagnostic-canary";
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("../conformance/envelope-v1/vectors.json")).unwrap();
    for vector in vectors {
        let original = vector["envelope"].clone();
        for path in ["", "/binding", "/payload"] {
            let mut changed = original.clone();
            changed
                .pointer_mut(path)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(SECRET.into(), json!(SECRET));
            let error = Envelope::decode(&serde_json::to_vec(&changed).unwrap()).unwrap_err();
            assert_eq!(error.to_string(), "invalid wire structure");
            assert_eq!(error.chain().count(), 1);
            assert!(!format!("{error:?}").contains(SECRET));
        }
        let mut changed = original.clone();
        changed["binding"]["revision"] = json!(SECRET);
        let error = Envelope::decode(&serde_json::to_vec(&changed).unwrap()).unwrap_err();
        assert_eq!(error.to_string(), "invalid wire structure");
        assert_eq!(error.chain().count(), 1);
        assert!(!format!("{error:?}").contains(SECRET));
        // Direct message() is public too: it must not expose malformed payloads.
        let mut envelope = Envelope::decode(&serde_json::to_vec(&original).unwrap()).unwrap();
        envelope
            .payload
            .as_object_mut()
            .unwrap()
            .insert(SECRET.into(), json!(SECRET));
        let error = envelope.message().unwrap_err();
        assert_eq!(error.to_string(), "invalid wire structure");
        assert_eq!(error.chain().count(), 1);
    }
    let error = runspindle_core::envelope::strict_json(b"{broken").unwrap_err();
    assert_eq!(error.to_string(), "invalid wire JSON");
    assert_eq!(error.chain().count(), 1);
}

#[cfg(feature = "verified-authority")]
#[test]
fn scoped_and_intent_ingress_diagnostics_use_the_same_safe_boundary() {
    use runspindle_core::{
        intent::{IntentAdmission, IntentBatch},
        scoped_authority::ScopedAdmission,
    };
    let bytes = br#"{"private-diagnostic-canary":"private-diagnostic-canary"}"#;
    let errors = [
        ScopedAdmission::decode(bytes).unwrap_err(),
        IntentAdmission::decode(bytes).unwrap_err(),
        IntentBatch::decode(bytes).unwrap_err(),
    ];
    for error in errors {
        assert_eq!(error.to_string(), "invalid wire structure");
        assert_eq!(error.chain().count(), 1);
        assert!(!format!("{error:?}").contains("private-diagnostic-canary"));
    }
}
