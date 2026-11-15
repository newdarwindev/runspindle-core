//! Disposable synthetic issuer used only by explicit offline conformance tests.
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use runspindle_core::{
    authority::{self, Action, Assertion, LocalHelper},
    envelope,
};
use rusqlite::{params, Connection};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::Path,
};
#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Init,
    Issue {
        wire: String,
        #[serde(default)]
        changes: serde_json::Map<String, serde_json::Value>,
        #[serde(default)]
        bad_signature: bool,
    },
    Revoke {
        grant_ref: String,
    },
    Stale,
}
pub fn run(directory: &Path, input: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        directory == Path::new("/effects"),
        "synthetic issuer requires disposable /effects mount"
    );
    let mut bytes = Vec::new();
    std::fs::File::open(input)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    let command: Command = serde_json::from_value(envelope::strict_json(&bytes)?)?;
    let db = directory.join("authority.sqlite");
    match command {
        Command::Init => {
            let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .map_err(|_| anyhow::anyhow!("ephemeral key generation"))?;
            let pair = Ed25519KeyPair::from_pkcs8(key.as_ref())
                .map_err(|_| anyhow::anyhow!("ephemeral key parse"))?;
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(directory.join("ephemeral.pk8"))?;
            f.write_all(key.as_ref())?;
            f.sync_all()?;
            let c = Connection::open(&db)?;
            authority::initialize_synthetic_store(&c)?;
            let now = authority::now_ms()?;
            c.execute(
                "INSERT INTO authority_state VALUES(1,?1,?2)",
                params![now, now + 600_000],
            )?;
            c.execute(
                "INSERT INTO trust_roots VALUES('synthetic-issuer','test-key',?1,?2,?3,0)",
                params![pair.public_key().as_ref(), now, now + 600_000],
            )?;
            let helper = LocalHelper {
                database: db,
                issuer: "synthetic-issuer".into(),
                actor: "synthetic-actor".into(),
                audience: "offline-core".into(),
                allowed_actions: vec![
                    Action::Admit,
                    Action::Dispatch,
                    Action::Reconcile,
                    Action::Cancel,
                ],
            };
            std::fs::write(
                directory.join("authority-helper.json"),
                serde_json::to_vec(&helper)?,
            )?;
        }
        Command::Issue {
            wire,
            changes,
            bad_signature,
        } => {
            let admission = authority::request(wire.as_bytes())?;
            let now = authority::now_ms()?;
            let a = Assertion {
                version: authority::VERSION.into(),
                issuer: "synthetic-issuer".into(),
                key_id: "test-key".into(),
                actor: "synthetic-actor".into(),
                audience: "offline-core".into(),
                grant_id: admission.grant_ref.clone(),
                request_digest: format!("{:x}", Sha256::digest(wire.as_bytes())),
                actions: vec![
                    Action::Admit,
                    Action::Dispatch,
                    Action::Reconcile,
                    Action::Cancel,
                ],
                policy_revision: admission.binding.policy_revision.clone(),
                target: admission.binding,
                not_before_ms: now,
                expires_at_ms: now + 300_000,
            };
            let mut value = serde_json::to_value(a)?;
            for (k, v) in changes {
                value[&k] = v;
            }
            let a: Assertion = serde_json::from_value(value)?;
            let key = std::fs::read(directory.join("ephemeral.pk8"))?;
            let pair = Ed25519KeyPair::from_pkcs8(&key)
                .map_err(|_| anyhow::anyhow!("ephemeral key parse"))?;
            let mut signature = pair.sign(&a.signing_bytes()?).as_ref().to_vec();
            if bad_signature {
                signature[0] ^= 1;
            }
            let c = Connection::open(db)?;
            c.execute(
                "INSERT INTO grants VALUES(?1,?2,?3)",
                params![admission.grant_ref, serde_json::to_string(&a)?, signature],
            )?;
        }
        Command::Revoke { grant_ref } => {
            Connection::open(db)?.execute("INSERT INTO revocations VALUES(?1)", [grant_ref])?;
        }
        Command::Stale => {
            Connection::open(db)?.execute("UPDATE authority_state SET valid_until_ms=0", [])?;
        }
    }
    println!("{{\"status\":\"prepared_synthetic_only\"}}");
    Ok(())
}
