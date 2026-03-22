use sea_orm::{DbErr, RuntimeErr, SqlxError};

// To be implemented by retryable error types.
// The implementation should categorize on the source error type, e.g., by pattern matching self.
pub trait Retryable {
    fn retryable(&self) -> bool;
}

impl Retryable for DbErr {
    // IO and connection-related DB errors are usually retryable
    fn retryable(&self) -> bool {
        match self {
            DbErr::ConnectionAcquire(_) => true,
            DbErr::Conn(RuntimeErr::SqlxError(err))
            | DbErr::Exec(RuntimeErr::SqlxError(err))
            | DbErr::Query(RuntimeErr::SqlxError(err)) => {
                matches!(err, SqlxError::PoolTimedOut | SqlxError::Io(_))
            }
            _ => false,
        }
    }
}
