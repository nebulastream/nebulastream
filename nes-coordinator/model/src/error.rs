/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! The system's failure type and error codes.

use sea_orm::{DbErr, RuntimeErr};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

include!(concat!(env!("OUT_DIR"), "/error_code.rs"));

/// A failure with an error code for compatibility with `ExceptionDefinitions.inc` on the C++ side.
/// Attached as a typed cause of an `anyhow::Error`.
///
/// A failure without a code is reported as `UnknownException`,
/// so raising this distinguishes classified vs. unclassified failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{msg}")]
pub struct CodedError {
    #[serde(with = "code_number")]
    pub code: ErrorCode,
    pub msg: String,
    /// A C++ failure's stacktrace, empty for an error from rust.
    pub trace: String,
}

impl CodedError {
    /// A failure raised in Rust, without stacktrace.
    pub fn new(code: ErrorCode, msg: impl Display) -> Self {
        Self {
            code,
            msg: msg.to_string(),
            trace: String::new(),
        }
    }

    /// A failure raised in C++, relayed with the code and stacktrace.
    #[must_use]
    pub const fn relayed(code: ErrorCode, msg: String, trace: String) -> Self {
        Self { code, msg, trace }
    }
}

mod code_number {
    use super::ErrorCode;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(code: &ErrorCode, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(*code as u16)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ErrorCode, D::Error> {
        u16::deserialize(deserializer).map(ErrorCode::from_code)
    }
}

/// Why the database/catalog refused a write.
enum Violation {
    /// A row with that key is already there.
    Duplicate,
    /// A row that the write refers to is not there.
    MissingReference,
}

/// Returns a broken constraint from a rejected write, if any.
fn violation(err: &DbErr) -> Option<Violation> {
    let runtime = match err {
        DbErr::Exec(runtime) | DbErr::Query(runtime) | DbErr::Conn(runtime) => runtime,
        _ => return None,
    };
    let RuntimeErr::SqlxError(sqlx_err) = runtime else {
        return None;
    };
    let reported = sqlx_err.as_database_error()?;
    if reported.is_unique_violation() {
        return Some(Violation::Duplicate);
    }
    if reported.is_foreign_key_violation() {
        return Some(Violation::MissingReference);
    }
    None
}

/// Why a catalog write failed, which decides whether writing again can help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFailure {
    /// The row no longer exists.
    RowGone,
    /// A trigger, a constraint or a model hook refused the row, so writing it again fails the same way.
    Rejected,
    /// The write may succeed later, for example when the database is busy or a connection is lost.
    Transient,
}

/// SQLite's extended result code for `RAISE(ABORT, ...)` in a trigger, which sqlx has no error kind for.
const SQLITE_CONSTRAINT_TRIGGER: &str = "1811";

#[must_use]
pub fn classify_write(err: &DbErr) -> WriteFailure {
    let runtime = match err {
        DbErr::RecordNotUpdated => return WriteFailure::RowGone,
        DbErr::Custom(_) => return WriteFailure::Rejected,
        DbErr::Exec(runtime) | DbErr::Query(runtime) | DbErr::Conn(runtime) => runtime,
        _ => return WriteFailure::Transient,
    };
    let RuntimeErr::SqlxError(sqlx_err) = runtime else {
        return WriteFailure::Transient;
    };
    let Some(reported) = sqlx_err.as_database_error() else {
        return WriteFailure::Transient;
    };
    if reported.kind() != sqlx::error::ErrorKind::Other
        || reported.code().as_deref() == Some(SQLITE_CONSTRAINT_TRIGGER)
    {
        return WriteFailure::Rejected;
    }
    WriteFailure::Transient
}

/// Chooses the error code for a rejected catalog write based on the constraint violation.
pub fn catalog_write(
    duplicate: ErrorCode,
    missing_reference: ErrorCode,
) -> impl Fn(DbErr) -> anyhow::Error {
    move |err| {
        let code = match violation(&err) {
            Some(Violation::Duplicate) => duplicate,
            Some(Violation::MissingReference) => missing_reference,
            None => ErrorCode::CatalogWriteRejected,
        };
        anyhow::Error::new(CodedError::new(code, err))
    }
}

/// Raises a `CodedError` as an `anyhow::Error`, so a call site reads like `anyhow::bail!`.
#[macro_export]
macro_rules! coded_bail {
    ($code:expr, $($arg:tt)*) => {
        return Err(::anyhow::Error::new($crate::error::CodedError::new(
            $code,
            format!($($arg)*),
        )))
    };
}

#[cfg(test)]
pub(crate) fn code_of<T>(result: anyhow::Result<T>) -> Option<ErrorCode> {
    let error = result.err()?;
    error.downcast_ref::<CodedError>().map(|coded| coded.code)
}

#[cfg(test)]
mod tests {
    use super::{CodedError, ErrorCode, WriteFailure, classify_write};
    use sea_orm::{DbErr, RuntimeErr};

    /// The numbers are the contract with C++, so a few are pinned.
    /// A change here means the shared list changed and the C++ side changed with it.
    #[test]
    fn a_failed_write_is_classified_by_whether_retrying_can_help() {
        assert_eq!(
            classify_write(&DbErr::RecordNotUpdated),
            WriteFailure::RowGone
        );
        assert_eq!(
            classify_write(&DbErr::Custom("hook".to_string())),
            WriteFailure::Rejected
        );
        assert_eq!(
            classify_write(&DbErr::Conn(RuntimeErr::Internal("down".to_string()))),
            WriteFailure::Transient
        );
    }

    #[test]
    fn codes_match_shared_list() {
        assert_eq!(ErrorCode::PlacementFailure as u16, 2300);
        assert_eq!(ErrorCode::CatalogUnavailable as u16, 6000);
        assert_eq!(ErrorCode::CatalogWriteRejected as u16, 6001);
        assert_eq!(ErrorCode::UnknownException as u16, 9999);
    }

    #[test]
    fn code_round_trip() {
        assert_eq!(
            ErrorCode::from_code(ErrorCode::PlacementFailure as u16),
            ErrorCode::PlacementFailure
        );
    }

    #[test]
    fn undefined_number_is_unknown_exception() {
        assert_eq!(ErrorCode::from_code(4242), ErrorCode::UnknownException);
    }

    /// The boundary recovers the code by downcasting, so wrapping has to preserve the cause.
    #[test]
    fn a_wrapped_failure_still_names_its_code() {
        let error = anyhow::Error::new(CodedError::new(
            ErrorCode::CatalogWriteRejected,
            "a duplicate source",
        ))
        .context("while applying the statement");

        let found = error.downcast_ref::<CodedError>().expect("cause is kept");
        assert_eq!(found.code, ErrorCode::CatalogWriteRejected);
        assert_eq!(found.msg, "a duplicate source");
    }
}
