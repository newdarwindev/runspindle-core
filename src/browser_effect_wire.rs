//! Bounded trusted-peer client for authenticated Browser effect execution.
//! Legacy peers may retain a Browser journal; the external-owner composition
//! reserves effects in Core BrowserRegistry and uses a stateless Browser peer.
//! No launcher, network, retry, durable Core scheduling or issuer is supplied.
use serde::{Deserialize, Serialize};

pub const VERSION: &str = "runspindle.browser-effect.v1";
pub const EVIDENCE_CLASS: &str = "browser_guarded_effect_observation";
pub const MAX_PAYLOAD: usize = 1_048_576;
pub const MAX_REQUEST: usize = 2 * MAX_PAYLOAD + 8192;
pub const MAX_RESPONSE: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub principal: String,
    pub account: String,
    pub browser: String,
    pub tab: String,
    pub intent: String,
    pub revision: u64,
    pub operation: String,
    pub epoch: u64,
    pub origin: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub binding: Binding,
    pub native_session: String,
    pub frame: String,
    pub frame_origin: String,
}
fn identity(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn origin(value: &str) -> bool {
    // Conservative HTTPS-origin subset of Browser's contract, never normalize
    // or silently retarget what the caller supplied. No path (even slash).
    let Some(authority) = value.strip_prefix("https://") else {
        return false;
    };
    !authority.is_empty()
        && !authority
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || "/?#@\\".contains(c))
        && url::Url::parse(value).is_ok_and(|u| {
            u.host_str().is_some() && u.username().is_empty() && u.password().is_none()
        })
}
impl Target {
    pub fn valid(&self) -> bool {
        let b = &self.binding;
        [
            &b.principal,
            &b.account,
            &b.browser,
            &b.tab,
            &b.intent,
            &b.operation,
            &self.native_session,
            &self.frame,
        ]
        .into_iter()
        .all(|v| identity(v))
            && (1..=i64::MAX as u64).contains(&b.revision)
            && (1..=i64::MAX as u64).contains(&b.epoch)
            && origin(&b.origin)
            && origin(&self.frame_origin)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Dispatch,
    Reconcile,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Unknown,
    Confirmed,
    Rejected,
}

/// Trusted current Core authority. Browser must separately authenticate its own
/// exact grant. Neither serialized fields nor request hashes supply authority.
pub trait Authority {
    fn verify(&self, target: &Target, payload_digest: &str, action: Action) -> bool;
}
pub struct Deny;
impl Authority for Deny {
    fn verify(&self, _: &Target, _: &str, _: Action) -> bool {
        false
    }
}
/// Trusted, bounded local exchange. Implementations own framing, timeouts,
/// authenticated peer selection and out-of-band Browser admission tickets.
pub trait Peer {
    fn exchange(&mut self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
}
#[derive(Serialize)]
struct Request<'a> {
    version: &'static str,
    action: Action,
    target: &'a Target,
    payload_digest: &'a str,
    payload_hex: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: String,
    request_sha256: String,
    evidence_class: String,
    outcome: Outcome,
}
/// Immutable request identity and exact payload, but NOT a durable registry.
/// The caller must retain/reconstruct the same operation across restart and may
/// not create another ID to retry an unknown result. This client itself reserves
/// nothing: use Core BrowserRegistry for the external-owner path. Legacy fixture
/// peers also retain Browser journals; those are not migrated by this client.
pub struct Client {
    target: Target,
    payload: Vec<u8>,
    digest: String,
}
impl Client {
    pub fn new(target: Target, payload: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            target.valid() && !payload.is_empty() && payload.len() <= MAX_PAYLOAD,
            "invalid browser effect request"
        );
        let client = Self {
            target,
            payload: payload.to_vec(),
            digest: crate::durable::digest(payload),
        };
        client.request(Action::Dispatch)?;
        Ok(client)
    }
    pub fn target(&self) -> &Target {
        &self.target
    }
    pub fn payload_digest(&self) -> &str {
        &self.digest
    }
    fn request(&self, action: Action) -> anyhow::Result<Vec<u8>> {
        let payload_hex = if action == Action::Dispatch {
            let mut hex = String::with_capacity(self.payload.len() * 2);
            const DIGITS: &[u8] = b"0123456789abcdef";
            for byte in &self.payload {
                hex.push(DIGITS[(byte >> 4) as usize] as char);
                hex.push(DIGITS[(byte & 15) as usize] as char);
            }
            Some(hex)
        } else {
            None
        };
        let raw = serde_json::to_vec(&Request {
            version: VERSION,
            action,
            target: &self.target,
            payload_digest: &self.digest,
            payload_hex,
        })
        .map_err(|_| anyhow::anyhow!("invalid browser effect request"))?;
        anyhow::ensure!(raw.len() <= MAX_REQUEST, "oversized browser effect request");
        Ok(raw)
    }
    fn exchange<A: Authority, P: Peer>(
        &self,
        action: Action,
        authority: &A,
        peer: &mut P,
        ready: impl FnOnce() -> anyhow::Result<bool>,
    ) -> anyhow::Result<Option<Outcome>> {
        anyhow::ensure!(
            authority.verify(&self.target, &self.digest, action),
            "browser effect denied"
        );
        let request = self.request(action)?;
        if !ready()? {
            return Ok(None);
        }
        let response = peer.exchange(&request);
        // Even a cached terminal observation cannot outlive current authority.
        // A denial after dispatch does not undo its possible external effect.
        anyhow::ensure!(
            authority.verify(&self.target, &self.digest, action),
            "browser effect authority changed"
        );
        let Ok(bytes) = response else {
            return Ok(Some(Outcome::Unknown));
        };
        if bytes.len() > MAX_RESPONSE {
            return Ok(Some(Outcome::Unknown));
        }
        let Ok(response) = serde_json::from_slice::<Response>(&bytes) else {
            return Ok(Some(Outcome::Unknown));
        };
        if response.version != VERSION
            || response.evidence_class != EVIDENCE_CLASS
            || response.request_sha256 != crate::durable::digest(&request)
        {
            return Ok(Some(Outcome::Unknown));
        }
        Ok(Some(response.outcome))
    }
    pub(crate) fn dispatch_reserved<A: Authority, P: Peer>(
        &self,
        authority: &A,
        peer: &mut P,
        ready: impl FnOnce() -> anyhow::Result<bool>,
    ) -> anyhow::Result<Option<Outcome>> {
        self.exchange(Action::Dispatch, authority, peer, ready)
    }
    pub fn dispatch<A: Authority, P: Peer>(
        &self,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        self.exchange(Action::Dispatch, authority, peer, || Ok(true))
            .map(|outcome| outcome.unwrap_or(Outcome::Unknown))
    }
    pub fn reconcile<A: Authority, P: Peer>(
        &self,
        authority: &A,
        peer: &mut P,
    ) -> anyhow::Result<Outcome> {
        self.exchange(Action::Reconcile, authority, peer, || Ok(true))
            .map(|outcome| outcome.unwrap_or(Outcome::Unknown))
    }
}
