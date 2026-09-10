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

//! The coordinator a C++ frontend owns and operates by SQL string.
//! There are two ways to submit: one renders the result for printing and returns as soon as the catalog write is done,
//! the other blocks until the statement is terminal and reports a typed outcome for a caller that acts on it.

use anyhow::Result;
use coordinator::EarlyTermination;
use model::database::StateBackend;
use model::query::query_state::QueryState;
use model::query::{GetQuery, Model as Query};
use model::request::Payload;
use model::statement::{Statement, StatementResult};
use std::time::Duration;

use tokio::sync::watch;

use crate::error::{FfiError, fragment_error, no_error};
use crate::ffi;
use crate::handle::{CoordinatorHandle, build_coordinator, default_host_for};

/// Owns a `CoordinatorHandle` on behalf of a C++ frontend that operates the coordinator by SQL string rather than by typed request.
/// Statements go in as strings and come back rendered as strings, so no catalog model types cross the FFI boundary.
pub struct EmbeddedCoordinator {
    handle: CoordinatorHandle,
    /// Set to true to release a shutdown wait early.
    /// Held here rather than passed in per call so that another thread can reach it while the waiting thread is parked inside the FFI.
    cancel: watch::Sender<bool>,
    /// The address this deployment places a statement on when it omits its HOST clause.
    default_host: String,
}

/// Start a coordinator for a C++ frontend.
/// An empty `db_path` selects the ephemeral in-memory catalog, and a non-empty path a persistent sqlite catalog there.
pub(crate) fn start_embedded_coordinator(
    db_path: &str,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> Result<Box<EmbeddedCoordinator>, FfiError> {
    let state_backend = if db_path.is_empty() {
        StateBackend::Memory
    } else {
        StateBackend::sqlite(db_path)
    };
    Ok(Box::new(EmbeddedCoordinator {
        handle: build_coordinator(state_backend, mode, optimizer_config)?,
        cancel: watch::channel(false).0,
        default_host: default_host_for(mode).to_string(),
    }))
}

impl EmbeddedCoordinator {
    /// The address a statement that omits its HOST clause is placed on, empty when this deployment requires an explicit one.
    /// A frontend reads it to register a worker there, rather than restating the address and risking the two drifting apart.
    pub(crate) fn default_host(&self) -> String {
        self.default_host.clone()
    }

    /// Submit one SQL statement and render its result.
    /// No statement waits for a query to reach a runtime state, so a result reports the state at the time of the catalog
    /// write and a query started or stopped by the statement will still be moving.
    /// The catalog write itself has already happened when this returns.
    /// Waiting is confined to shut down, in `await_termination`.
    pub(crate) fn submit_sql(&self, sql: &str, as_json: bool) -> Result<String, FfiError> {
        let payload = Payload::sql(sql.to_string());
        Ok(render(self.handle.send(payload)?, as_json)?)
    }

    /// Blocks until every currently installed query has reached a terminal state, then returns their final states.
    /// Used by a frontend to wait for outstanding queries on shutdown.
    /// Returns an empty string if `cancel_await_termination` released the wait before the queries finished.
    pub(crate) fn await_termination(&self, as_json: bool) -> Result<String, FfiError> {
        let payload = Payload::parsed(Statement::GetQuery(GetQuery::all())).until_terminated(None);
        match self
            .handle
            .send_until(payload, &mut self.cancel.subscribe())?
        {
            Some(result) => Ok(render(result, as_json)?),
            None => Ok(String::new()),
        }
    }

    /// Releases a shutdown wait without waiting for the queries.
    /// Meant for a frontend that has been asked to terminate and wants to stop waiting on queries that may still run for a long time.
    /// Safe to call when nothing is waiting, and the next wait then returns immediately.
    pub(crate) fn cancel_await_termination(&self) {
        let _ = self.cancel.send(true);
    }

    /// Submit one SQL statement and block until it is terminal, then report its final state.
    /// The wait happens on the submission itself: a create parks until its query completes, so no second read is needed.
    /// A statement that starts no query is terminal the moment it returns.
    /// A deadline of zero waits indefinitely, and any other value answers `QueryWaitTimeout` once it passes.
    /// The failure code is preserved: a planning failure reports the code the planner determined, and a runtime failure
    /// reports the code of the fragment that failed rather than the query's code-less error summary.
    pub(crate) fn submit(&self, sql: &str, timeout_ms: u64) -> ffi::StatementOutcome {
        let deadline = (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms));
        match self
            .handle
            .send(Payload::sql(sql.to_string()).until_completed(deadline))
        {
            // The create parked until Completed, so reaching here means it succeeded.
            Ok(StatementResult::CreatedQuery(query, _)) => outcome(&query, no_error()),
            // An EXPLAIN answers with the plan it computed, which the caller reads rather than a result file.
            Ok(StatementResult::ExplainedQuery(explanation)) => applied(explanation),
            // Every other statement is a catalog write, terminal the moment it returns.
            Ok(_) => applied(String::new()),
            // A query that terminates without completing comes back as an early termination with its
            // fragments, whose structured error still has the code the caller matches on.
            Err(error) => match error.downcast_ref::<EarlyTermination>() {
                Some(EarlyTermination(query, fragments)) => {
                    outcome(query, fragment_error(fragments))
                }
                None => failed(&error),
            },
        }
    }
}

/// A statement that created no query, terminal the moment it returns, answering with the given text.
fn applied(result: String) -> ffi::StatementOutcome {
    ffi::StatementOutcome {
        error: no_error(),
        query_id: 0,
        state: ffi::QueryState::Completed,
        start_ms: 0,
        stop_ms: 0,
        result,
    }
}

/// A statement that could not be applied, reported with its error code.
fn failed(error: &anyhow::Error) -> ffi::StatementOutcome {
    ffi::StatementOutcome {
        error: ffi::BridgeError::from(error),
        query_id: 0,
        state: ffi::QueryState::Failed,
        start_ms: 0,
        stop_ms: 0,
        result: String::new(),
    }
}

/// The outcome of a terminal query: its final state and timestamps, with the given error.
fn outcome(query: &Query, error: ffi::BridgeError) -> ffi::StatementOutcome {
    ffi::StatementOutcome {
        error,
        query_id: *query.id,
        state: query.state.into(),
        start_ms: query
            .start_timestamp
            .map_or(0, |t| t.timestamp_millis() as u64),
        stop_ms: query
            .stop_timestamp
            .map_or(0, |t| t.timestamp_millis() as u64),
        result: String::new(),
    }
}

impl From<QueryState> for ffi::QueryState {
    fn from(state: QueryState) -> Self {
        match state {
            QueryState::Pending => Self::Pending,
            QueryState::Started => Self::Started,
            QueryState::Running => Self::Running,
            QueryState::Completed => Self::Completed,
            QueryState::Stopped => Self::Stopped,
            QueryState::Failed => Self::Failed,
        }
    }
}

fn render(result: StatementResult, as_json: bool) -> Result<String> {
    if as_json {
        Ok(serde_json::to_string_pretty(&result)?)
    } else {
        Ok(format!("{result}"))
    }
}
