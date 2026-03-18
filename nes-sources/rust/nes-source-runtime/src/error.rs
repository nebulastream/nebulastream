// Error types for the source runtime framework.
//
// SourceResult represents the outcome of a source's `run()` method.
// SourceError provides structured error reporting for source failures.

use std::fmt;

/// Result of a source's `run()` method.
#[derive(Debug)]
pub enum SourceResult {
    /// Source has no more data to produce.
    EndOfStream,
    /// Source encountered an error during execution.
    Error(SourceError),
}

/// Error type for source execution failures.
#[derive(Debug)]
pub struct SourceError {
    message: String,
}

impl SourceError {
    /// Create a new SourceError with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SourceError: {}", self.message)
    }
}

impl std::error::Error for SourceError {}

impl From<String> for SourceError {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for SourceError {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_error_from_string() {
        let err = SourceError::new("test error");
        assert_eq!(format!("{}", err), "SourceError: test error");
    }

    #[test]
    fn source_error_from_str_impl() {
        let err: SourceError = "test error".into();
        assert_eq!(format!("{}", err), "SourceError: test error");
    }

    #[test]
    fn source_error_from_string_impl() {
        let err: SourceError = String::from("test error").into();
        assert_eq!(format!("{}", err), "SourceError: test error");
    }

    #[test]
    fn source_error_is_std_error() {
        fn _assert_error<T: std::error::Error>() {}
        _assert_error::<SourceError>();
    }

    #[test]
    fn source_result_variants() {
        let _eos = SourceResult::EndOfStream;
        let _err = SourceResult::Error(SourceError::new("fail"));
    }
}
