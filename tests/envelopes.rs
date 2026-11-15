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
