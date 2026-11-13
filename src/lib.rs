//! Process-local contract models, plus an opt-in bounded Temporal development runtime.
//!
//! Caller-supplied observations and digests are compared, not authenticated.
//! See `specs/contracts.md` for the trust boundary and remaining gaps.

#![forbid(unsafe_code)]

pub mod artifact;
pub mod identity;

#[cfg(feature = "temporal-runtime")]
pub mod runtime;

#[cfg(feature = "host-runtime")]
pub mod integration;

#[derive(Debug, PartialEq, Eq)]
pub enum ReadinessError {
    NotImplemented,
}

/// Production/native activation and external operational effects remain unavailable.
pub fn activate() -> Result<(), ReadinessError> {
    Err(ReadinessError::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unimplemented_activation_fails_closed() {
        assert_eq!(activate(), Err(ReadinessError::NotImplemented));
    }
}
