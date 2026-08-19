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

//! The error codes the system defines, and the failure type that names one.

use sea_orm::{DbErr, RuntimeErr};
use std::fmt::Display;

include!(concat!(env!("OUT_DIR"), "/error_code.rs"));

/// A failure that names the code it reports, so a caller can act on the kind rather than read the
/// message. Attached as a typed cause of an `anyhow::Error`, which the FFI boundary looks for.
///
/// A failure that does not name one is reported as `UnknownException`, so raising this is what
/// distinguishes a classified failure from one nobody has got to yet.
#[derive(Debug, thiserror::Error)]
#[error("{msg}")]
pub struct CodedError {
    pub code: ErrorCode,
    pub msg: String,
    /// The stacktrace a C++ failure came with, empty for one raised here.
    pub trace: String,
}

impl CodedError {
    /// A failure raised in Rust, which has no stacktrace to report.
    pub fn new(code: ErrorCode, msg: impl Display) -> Self {
        Self {
            code,
            msg: msg.to_string(),
            trace: String::new(),
        }
    }

    /// A failure C++ raised, relayed with the code and stacktrace it reported.
    #[must_use]
    pub const fn relayed(code: ErrorCode, msg: String, trace: String) -> Self {
        Self { code, msg, trace }
    }
}

/// What the database refused a write for, when it says.
enum Violation {
    /// A row with that key is already there.
    Duplicate,
    /// A row the write refers to is not there.
    MissingReference,
}

/// Reads the constraint a rejected write broke, if the database reported one.
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

/// Names the code a rejected catalog write reports, given what the two constraints on this entity
/// mean. A taken name and a reference to something absent are the caller's errors and are reported
/// as themselves. Anything else the database refused is the write failing.
///
/// An entity with no foreign key passes `CatalogWriteRejected` as the reference code, because a
/// violation it cannot have should not be given a misleading name.
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
mod tests {
    use super::{CodedError, ErrorCode};

    /// The numbers are the contract with C++, so a few are pinned rather than trusted to the
    /// generator. A change here means the shared list changed and the C++ side moved with it.
    #[test]
    fn codes_match_the_shared_list() {
        assert_eq!(ErrorCode::PlacementFailure as u16, 2300);
        assert_eq!(ErrorCode::CatalogUnavailable as u16, 6000);
        assert_eq!(ErrorCode::CatalogWriteRejected as u16, 6001);
        assert_eq!(ErrorCode::UnknownException as u16, 9999);
    }

    #[test]
    fn a_code_survives_the_round_trip_through_a_number() {
        assert_eq!(
            ErrorCode::from_code(ErrorCode::PlacementFailure as u16),
            ErrorCode::PlacementFailure
        );
    }

    /// A number the list does not define is what the boundary reports for an unclassified failure,
    /// so reading one back must not panic or invent a code.
    #[test]
    fn an_undefined_number_reads_as_unknown() {
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
