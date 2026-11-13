//! Dependency-free, in-memory contracts. No I/O or runtime activation.
//!
//! Caller-supplied observations and digests are compared, not authenticated.
//! See `specs/contracts.md` for the trust boundary and remaining gaps.

#![forbid(unsafe_code)]

pub mod artifact;
pub mod identity;

#[derive(Debug, PartialEq, Eq)]
pub enum ReadinessError {
    NotImplemented,
}

/// Native activation, Temporal integration and external effects are unimplemented.
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
