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

//! How a failure crosses the boundary, and the one place its shape is converted.
//! A call that answers a question reports its failure in the struct that it returns,
//! so the code and the trace survive.
//! A constructor may throw instead: its failure has no code to keep, and cxx renders the message.

use std::fmt;

use model::error::{CodedError, ErrorCode};
use model::query::Model as Query;
use model::query::query_fragment::{QueryError, QueryFragmentError};

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    /// An error crossing the FFI boundary, or none.
    /// cxx shared structs cannot hold an `Option`, so a zero code means that nothing failed.
    /// The default value is that absence.
    #[derive(Default)]
    struct BridgeError {
        code: u16,
        msg: String,
        trace: String,
    }
}

/// The error of a throwing constructor that C++ calls.
/// cxx renders a returned error with `Display`, which for anyhow prints only the outermost context.
/// The chain is flattened on construction so the root cause survives the boundary.
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

impl ffi::BridgeError {
    pub(crate) fn is_none(&self) -> bool {
        self.code == 0
    }

    /// The reported failure, with the code and stacktrace that the other side gave it.
    pub(crate) fn into_error(self) -> Option<CodedError> {
        (!self.is_none())
            .then(|| CodedError::relayed(ErrorCode::from_code(self.code), self.msg, self.trace))
    }
}

impl From<&CodedError> for ffi::BridgeError {
    fn from(failure: &CodedError) -> Self {
        Self {
            code: failure.code as u16,
            msg: failure.msg.clone(),
            trace: failure.trace.clone(),
        }
    }
}

impl From<&anyhow::Error> for ffi::BridgeError {
    fn from(error: &anyhow::Error) -> Self {
        match error.downcast_ref::<CodedError>() {
            // The code comes from the classified cause and the message from the whole chain,
            // because the context that a caller added above it says where the failure happened.
            Some(failure) => Self {
                code: failure.code as u16,
                msg: format!("{error:#}"),
                trace: failure.trace.clone(),
            },
            // A failure that no layer classified.
            // Zero would read as success, so the unknown code is reported instead.
            // A caller that sees it has found a path still to be given a code.
            None => Self {
                code: ErrorCode::UnknownException as u16,
                msg: format!("{error:#}"),
                trace: String::new(),
            },
        }
    }
}

impl From<&QueryError> for ffi::BridgeError {
    fn from(failure: &QueryError) -> Self {
        match &failure.error {
            QueryFragmentError::Internal(coded) => coded.into(),
            // A transport error means the worker was not reached, not that it reported a failure.
            // There is no code of the worker's to relay, so the unreachable code is used instead.
            QueryFragmentError::Transport { msg } => Self {
                code: ErrorCode::WorkerUnreachable as u16,
                msg: msg.clone(),
                trace: String::new(),
            },
        }
    }
}

/// The failure that ended a query.
/// A failed query should have one recorded, so the code-less fallback is rarely reached.
pub(crate) fn query_failure(query: &Query) -> ffi::BridgeError {
    match &query.error {
        Some(failure) => failure.into(),
        None => ffi::BridgeError {
            code: ErrorCode::UnknownException as u16,
            msg: "query failed without a fragment error".to_string(),
            trace: String::new(),
        },
    }
}
