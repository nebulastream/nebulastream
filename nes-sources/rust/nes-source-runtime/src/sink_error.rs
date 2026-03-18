// Error types for the sink runtime framework.
//
// SinkError provides structured error reporting for sink failures.
// Mirrors the source framework's SourceError pattern for consistency.

use std::fmt;

/// Error type for sink execution failures.
#[derive(Debug)]
pub struct SinkError {
    message: String,
}

impl SinkError {
    /// Create a new SinkError with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SinkError: {}", self.message)
    }
}

impl std::error::Error for SinkError {}

impl From<String> for SinkError {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for SinkError {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_error_from_string() {
        let err = SinkError::new("test error");
        assert_eq!(format!("{}", err), "SinkError: test error");
    }

    #[test]
    fn sink_error_from_str_impl() {
        let err: SinkError = "test error".into();
        assert_eq!(format!("{}", err), "SinkError: test error");
    }

    #[test]
    fn sink_error_from_string_impl() {
        let err: SinkError = String::from("test error").into();
        assert_eq!(format!("{}", err), "SinkError: test error");
    }

    #[test]
    fn sink_error_is_std_error() {
        fn _assert_error<T: std::error::Error>() {}
        _assert_error::<SinkError>();
    }
}
