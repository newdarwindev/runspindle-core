//! Public contract stub. It performs no external operations.
#[derive(Debug, PartialEq)]
pub enum ReadinessError { NotImplemented }

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
