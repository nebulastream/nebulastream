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

//! Starting a coordinator and operating it.
//! A Rust caller keeps it and sends parsed or SQL statements over it.
//! A C++ frontend owns one through the bridge, submits SQL, and reads a typed outcome back.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use controller::in_process::WorkerFactory;
use coordinator::start_with_runtime;
use model::database::StateBackend;
use model::error::{CodedError, ErrorCode};
use model::query::GetQuery;
use model::query::Model as Query;
use model::query::query_state::QueryState;
use model::request::{Statement, Until, Wait};
use model::statement::StatementResult;
use request_handler::{Client, EarlyTermination, SqlPlanner, WaitTimeout};
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

use crate::error::{FfiError, query_failure, query_failures};
use crate::planner::FfiSqlPlanner;
use crate::worker::BridgeWorkerFactory;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    enum WorkerMode {
        Embedded,
        Remote,
    }

    /// Where the coordinator keeps its catalog.
    enum StateBackendKind {
        /// In memory. The catalog is lost when the process exits.
        Memory,
        /// In a sqlite file.
        Sqlite,
    }

    /// When `submit` returns.
    enum WaitMode {
        /// As soon as the statement is applied to the catalog.
        None,
        /// For a statement that creates a query: when the query has completed or has terminated in another way.
        /// Any other statement returns as soon as it is applied.
        UntilCompleted,
    }

    /// The lifecycle state of a query, as the catalog records it.
    /// A copy of the catalog's enum, because cxx can only share an enum that is declared here.
    enum QueryState {
        Pending,
        Started,
        Running,
        Completed,
        Stopped,
        Failed,
    }

    /// The result of one submitted statement.
    /// A cxx struct cannot hold an optional, so a value that is not set is 0.
    struct StatementOutcome {
        /// Set when the statement failed: the submission or planning error, or the error of the first failed fragment.
        error: BridgeError,
        /// Every failure: one entry per failed fragment, or the one error above.
        errors: Vec<BridgeError>,
        /// 0 for a statement that creates no query.
        query_id: i64,
        /// The state of the query when `submit` returned.
        state: QueryState,
        /// Milliseconds since the epoch, or 0 when not set.
        start_ms: u64,
        /// Milliseconds since the epoch, or 0 when not set.
        stop_ms: u64,
        /// The rendered result, as plain text or JSON.
        /// Empty when the statement failed.
        result: String,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        type BridgeError = crate::error::ffi::BridgeError;
    }

    impl Vec<BridgeError> {}

    extern "Rust" {
        /// A coordinator owned by a C++ frontend; dropping the box shuts it down.
        type Coordinator;

        /// `db_path` is only used for the sqlite backend.
        fn start_coordinator(
            backend: StateBackendKind,
            db_path: &str,
            worker_mode: WorkerMode,
            optimizer_config: &str,
        ) -> Result<Box<Coordinator>>;

        /// The host that is used for a statement without a HOST clause.
        /// Empty when a HOST clause is required.
        fn default_host(self: &Coordinator) -> String;

        /// Runs one statement and returns when `wait` says.
        /// `timeout_ms` bounds the wait and fails it with `QueryWaitTimeout`. 0 means no timeout.
        /// A timeout does not stop the query. The outcome has its id.
        /// Does not throw. A failure is returned in the error field, so its code is kept.
        /// Safe to call from several threads at once.
        fn submit(
            self: &Coordinator,
            sql: &str,
            wait: WaitMode,
            timeout_ms: u64,
            as_json: bool,
        ) -> StatementOutcome;

        /// Makes every waiting `submit` and `await_queries` return at once, without waiting for the queries.
        /// For a frontend that has to terminate while queries are still running.
        /// Later waits return immediately as well.
        fn cancel_waits(self: &Coordinator);

        /// Blocks until every query has terminated, or until the waits are cancelled.
        fn await_queries(self: &Coordinator);
    }
}

/// Owns the coordinator's runtime and request channel, so dropping it shuts the coordinator down.
pub struct Coordinator {
    client: Client,
    runtime: Option<Runtime>,
    /// Cancelled to release waiting calls.
    /// Another thread does that while the waiting thread is blocked inside the FFI.
    cancel: CancellationToken,
    default_host: String,
}

/// The address at which a frontend registers the in-process worker of an embedded deployment.
const EMBEDDED_WORKER_HOST: &str = "localhost:8080";

/// How long dropping the coordinator waits for its runtime.
/// Calls into C++ run as blocking tasks that cannot be interrupted, and without a bound the drop would wait for all of them.
const SHUTDOWN_BOUND: Duration = Duration::from_millis(500);

/// Embedded deployments run a single in-process worker, so defaulting to it is unambiguous.
/// Remote deployments have several, so there is nothing to default to.
fn default_host_for(mode: ffi::WorkerMode) -> &'static str {
    match mode {
        ffi::WorkerMode::Embedded => EMBEDDED_WORKER_HOST,
        _ => "",
    }
}

impl Coordinator {
    pub fn start(
        state_backend: StateBackend,
        mode: ffi::WorkerMode,
        optimizer_config: &str,
    ) -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .context("failed to create coordinator runtime")?;

        let default_host = default_host_for(mode).to_string();
        let planner: Arc<dyn SqlPlanner> = Arc::new(FfiSqlPlanner {
            rt_handle: runtime.handle().clone(),
            optimizer_config: optimizer_config.to_string(),
            default_host: default_host.clone(),
        });
        let factory: Option<Arc<dyn WorkerFactory>> = match mode {
            ffi::WorkerMode::Embedded => Some(Arc::new(BridgeWorkerFactory)),
            _ => None,
        };

        let sender = start_with_runtime(&runtime, state_backend, planner, factory)?;
        Ok(Self {
            client: Client::new(sender),
            runtime: Some(runtime),
            cancel: CancellationToken::new(),
            default_host,
        })
    }

    /// For a caller outside the runtime, such as a nebucli thread or a C++ thread inside the FFI.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("runtime present until drop")
            .handle()
            .block_on(fut)
    }

    pub const fn client(&self) -> &Client {
        &self.client
    }

    pub fn send(&self, statement: Statement) -> Result<StatementResult> {
        self.block_on(self.client.send(statement))
    }

    /// Returns `None` as soon as the waits are cancelled.
    /// A request that was already sent is not withdrawn: the coordinator still processes it, and its reply is dropped.
    fn block_until_cancelled<T>(
        &self,
        request: impl Future<Output = Result<T>>,
    ) -> Result<Option<T>> {
        if self.cancel.is_cancelled() {
            return Ok(None);
        }
        self.block_on(async {
            tokio::select! {
                reply = request => reply.map(Some),
                () = self.cancel.cancelled() => Ok(None),
            }
        })
    }

    /// A frontend registers its in-process worker at this address, so the address is defined in one place.
    pub(crate) fn default_host(&self) -> String {
        self.default_host.clone()
    }

    pub(crate) fn cancel_waits(&self) {
        self.cancel.cancel();
    }

    pub(crate) fn await_queries(&self) {
        let every_query = Wait {
            until: Until::Terminated,
            timeout: None,
        };
        let _ = self.block_until_cancelled(self.client.watch(GetQuery::all(), every_query));
    }

    pub(crate) fn submit(
        &self,
        sql: &str,
        wait: ffi::WaitMode,
        timeout_ms: u64,
        as_json: bool,
    ) -> ffi::StatementOutcome {
        let timeout = (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms));
        let statement = Statement::Sql(sql.to_string());
        let result = match self.block_until_cancelled(self.client.send(statement)) {
            Ok(None) => return wait_cancelled(),
            Ok(Some(result)) => result,
            Err(error) => return ffi::StatementOutcome::failed(&error),
        };
        // The create has replied with a pending query, and the wait is a second request.
        let result = match (wait, result) {
            (ffi::WaitMode::UntilCompleted, StatementResult::CreatedQuery(created)) => {
                let completed = self.client.await_completed(vec![created.query.id], timeout);
                match self.block_until_cancelled(completed) {
                    Ok(None) => return wait_cancelled(),
                    Ok(Some(mut rows)) => StatementResult::CreatedQuery(rows.remove(0)),
                    Err(error) => return failed_wait(&error),
                }
            }
            (_, result) => result,
        };
        let rendered = match render(&result, as_json) {
            Ok(rendered) => rendered,
            Err(error) => return ffi::StatementOutcome::failed(&error),
        };
        match result {
            // A create reports the query's identity and the state that the wait released it in.
            StatementResult::CreatedQuery(created) => ffi::StatementOutcome::of_query(
                &created.query,
                ffi::BridgeError::default(),
                Vec::new(),
                rendered,
            ),
            // Every other statement is terminal the moment it returns.
            _ => ffi::StatementOutcome::applied(rendered),
        }
    }
}

/// The outcome of a wait for completion that did not complete.
fn failed_wait(error: &anyhow::Error) -> ffi::StatementOutcome {
    // A query that terminates without completing is reported as failed, and its row records the failure that ended it.
    if let Some(EarlyTermination { row, .. }) = error.downcast_ref() {
        return ffi::StatementOutcome::of_query(
            &row.query,
            query_failure(&row.query),
            query_failures(row),
            String::new(),
        );
    }
    // The query of a timed out wait is still running, so the outcome keeps its id for the caller to stop it.
    if let Some(WaitTimeout { rows, .. }) = error.downcast_ref()
        && let [row, ..] = rows.as_slice()
    {
        return ffi::StatementOutcome::of_query(
            &row.query,
            ffi::BridgeError::from(error),
            vec![ffi::BridgeError::from(error)],
            String::new(),
        );
    }
    ffi::StatementOutcome::failed(error)
}

/// The wait was cancelled before the reply arrived, or the statement was not sent because the waits were already cancelled.
fn wait_cancelled() -> ffi::StatementOutcome {
    ffi::StatementOutcome::failed(
        &CodedError::new(
            ErrorCode::QueryWaitCancelled,
            "the wait was cancelled before the statement returned",
        )
        .into(),
    )
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(SHUTDOWN_BOUND);
        }
    }
}

pub(crate) fn start_coordinator(
    backend: ffi::StateBackendKind,
    db_path: &str,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> Result<Box<Coordinator>, FfiError> {
    let state_backend = match backend {
        ffi::StateBackendKind::Sqlite => {
            if db_path.is_empty() {
                return Err(anyhow!("the sqlite backend needs a database path").into());
            }
            StateBackend::sqlite(db_path)
        }
        _ => StateBackend::Memory,
    };
    Ok(Box::new(Coordinator::start(
        state_backend,
        mode,
        optimizer_config,
    )?))
}

impl ffi::StatementOutcome {
    /// A statement that created no query. It is finished as soon as it returns.
    fn applied(result: String) -> Self {
        Self {
            error: ffi::BridgeError::default(),
            errors: Vec::new(),
            query_id: 0,
            state: ffi::QueryState::Completed,
            start_ms: 0,
            stop_ms: 0,
            result,
        }
    }

    /// A statement that could not be applied.
    fn failed(error: &anyhow::Error) -> Self {
        Self {
            error: ffi::BridgeError::from(error),
            errors: vec![ffi::BridgeError::from(error)],
            query_id: 0,
            state: ffi::QueryState::Failed,
            start_ms: 0,
            stop_ms: 0,
            result: String::new(),
        }
    }

    /// A statement that created a query, with the state and timestamps of the query.
    fn of_query(
        query: &Query,
        error: ffi::BridgeError,
        errors: Vec<ffi::BridgeError>,
        result: String,
    ) -> Self {
        Self {
            error,
            errors,
            query_id: *query.id,
            state: query.state.into(),
            start_ms: query
                .start_timestamp
                .map_or(0, |t| t.timestamp_millis() as u64),
            stop_ms: query
                .stop_timestamp
                .map_or(0, |t| t.timestamp_millis() as u64),
            result,
        }
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

fn render(result: &StatementResult, as_json: bool) -> Result<String> {
    if as_json {
        Ok(serde_json::to_string_pretty(result)?)
    } else {
        Ok(result.to_string())
    }
}
