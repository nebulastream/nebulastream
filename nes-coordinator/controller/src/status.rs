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

//! What a worker reports about its fragments, decoded from its status response.

use crate::worker_client::remote::worker_rpc_service::nes::SerializableQueryId;
use crate::worker_client::remote::worker_rpc_service::{Error, WorkerStatusResponse};
use chrono::{DateTime, Utc};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use std::collections::BTreeMap;

/// The fragments that a worker runs, and those that terminated since the requested time.
/// A fragment that the worker does not know is absent.
#[derive(Debug)]
pub(crate) struct WorkerStatus {
    pub fragments: BTreeMap<QueryFragmentId, WorkerFragmentState>,
    /// The worker's clock when it took the status, in milliseconds since the Unix epoch.
    pub until: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// A fragment's state as its worker reports it.
pub(crate) enum WorkerFragmentState {
    /// Accepted and still compiling or starting.
    Started,
    Running {
        start: DateTime<Utc>,
    },
    /// Without an error, the fragment finished or was stopped; the worker lists both the same way.
    Terminated {
        start: Option<DateTime<Utc>>,
        stop: DateTime<Utc>,
        error: Option<QueryFragmentError>,
    },
}

impl From<WorkerStatusResponse> for WorkerStatus {
    fn from(response: WorkerStatusResponse) -> Self {
        // The controller did not start a query with an id that is not a fragment id, so it is left out and never stopped as an orphan.
        let get_fragment_id =
            |id: Option<SerializableQueryId>| id?.get_fragment_id().map(QueryFragmentId::new);
        let active = response.active_queries.into_iter().filter_map(|query| {
            let fragment = query
                .started_unix_timestamp_in_milli_seconds
                .map(to_datetime)
                .map_or(WorkerFragmentState::Started, |start| {
                    WorkerFragmentState::Running { start }
                });
            Some((get_fragment_id(query.query_id)?, fragment))
        });
        let terminated = response.terminated_queries.into_iter().filter_map(|query| {
            let fragment = WorkerFragmentState::Terminated {
                start: query
                    .started_unix_timestamp_in_milli_seconds
                    .map(to_datetime),
                stop: to_datetime(query.terminated_unix_timestamp_in_milli_seconds),
                error: query.error.as_ref().map(QueryFragmentError::from),
            };
            Some((get_fragment_id(query.query_id)?, fragment))
        });
        Self {
            fragments: active.chain(terminated).collect(),
            until: response.until_unix_timestamp_in_milli_seconds,
        }
    }
}

impl From<&Error> for QueryFragmentError {
    fn from(err: &Error) -> Self {
        let code =
            u16::try_from(err.code).map_or(ErrorCode::UnknownException, ErrorCode::from_code);
        Self::Internal(CodedError::relayed(
            code,
            err.message.clone(),
            err.stack_trace.clone(),
        ))
    }
}

/// A time that `DateTime` cannot hold becomes the Unix epoch.
pub(crate) fn to_datetime(ms: u64) -> DateTime<Utc> {
    i64::try_from(ms)
        .ok()
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or_default()
}

