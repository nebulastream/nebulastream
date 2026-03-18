use sea_orm::{DbErr, RuntimeErr, SqlxError};

pub trait Retryable {
    fn retryable(&self) -> bool;
}

impl Retryable for DbErr {
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
