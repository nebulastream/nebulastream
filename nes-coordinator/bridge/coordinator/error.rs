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

//! How a failure reaches C++, in the two shapes the boundary allows.
//! A function that may throw returns `FfiError`, which cxx renders into an exception message.
//! A function that reports its failure as data fills in a `BridgeError`, which keeps the error code and the stacktrace.

use std::fmt;

use model::error::{CodedError, ErrorCode};
use model::query::query_fragment::{Model as QueryFragment, QueryFragmentError};

use crate::ffi;

/// Reported for a failure that names no error code of its own.
/// Taken from the shared list rather than written out, so a caller mapping codes back to exceptions needs no special case.
const UNKNOWN_ERROR_CODE: u16 = ErrorCode::UnknownException as u16;

/// The error every function C++ calls returns.
/// cxx renders a returned `Err` with `Display`, and anyhow's `Display` prints only the outermost context, so the cause chain
/// would be dropped at the boundary.
/// Flattening on construction keeps the root cause, which for a catalog read is the part that says why the database refused.
pub struct FfiError(String);

impl fmt::Display for FfiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<anyhow::Error> for FfiError {
    fn from(error: anyhow::Error) -> Self {
        Self(format!("{error:#}"))
    }
}

impl From<serde_json::Error> for FfiError {
    fn from(error: serde_json::Error) -> Self {
        anyhow::Error::new(error).into()
    }
}

impl From<sea_orm::DbErr> for FfiError {
    fn from(error: sea_orm::DbErr) -> Self {
        anyhow::Error::new(error).into()
    }
}

/// The absence of a failure, which the bridge encodes as a zero code.
pub(crate) fn no_error() -> ffi::BridgeError {
    ffi::BridgeError {
        code: 0,
        msg: String::new(),
        trace: String::new(),
    }
}

impl From<&anyhow::Error> for ffi::BridgeError {
    fn from(error: &anyhow::Error) -> Self {
        match error.downcast_ref::<CodedError>() {
            // The code comes from the classified cause, the message from the whole chain, because
            // the context a caller added above it says which operation the failure happened in.
            Some(failure) => Self {
                code: failure.code as u16,
                msg: format!("{error:#}"),
                trace: failure.trace.clone(),
            },
            // A failure no layer classified.
            // Reporting zero would read as success, so it reports the unknown one, and a caller seeing it has found a path
            // still to be given a code.
            None => Self {
                code: UNKNOWN_ERROR_CODE,
                msg: format!("{error:#}"),
                trace: String::new(),
            },
        }
    }
}

/// The structured error of the first failed fragment.
/// An internal worker error has the code the caller matches on and is preferred, where a transport error has only a message.
/// A failed query is expected to have at least one such fragment, so the code-less fallback should not normally be reached.
pub(crate) fn fragment_error(fragments: &[QueryFragment]) -> ffi::BridgeError {
    for fragment in fragments {
        if let Some(QueryFragmentError::Internal { code, msg, trace }) = &fragment.error {
            return ffi::BridgeError {
                code: *code,
                msg: msg.clone(),
                trace: trace.clone(),
            };
        }
    }
    // A transport error means the worker could not be reached, which is a failure of its own
    // rather than one the worker reported, so it has no code of the worker's to relay.
    for fragment in fragments {
        if let Some(QueryFragmentError::Transport { msg }) = &fragment.error {
            return ffi::BridgeError {
                code: ErrorCode::WorkerUnreachable as u16,
                msg: msg.clone(),
                trace: String::new(),
            };
        }
    }
    ffi::BridgeError {
        code: UNKNOWN_ERROR_CODE,
        msg: "query failed without a fragment error".to_string(),
        trace: String::new(),
    }
}
