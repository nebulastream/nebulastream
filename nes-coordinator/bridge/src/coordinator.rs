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

//! Starting a coordinator, or connecting to one that another process runs, and operating it.
//! A Rust caller keeps it and sends parsed or SQL statements over it.
//! A C++ frontend owns one through the bridge, submits SQL, and reads a typed outcome back,
//! the same way for both backends.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use client::{Client, ClientError, WaitKind};
use coordinator::{EarlyTermination, start_with_runtime};
use model::database::StateBackend;
use model::query::Model as Query;
use model::query::query_state::QueryState;
use model::request::{Request, StatementInput, Wait};
use model::statement::StatementResult;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;

use crate::error::{FfiError, query_failure};
use crate::planner::sql_planner;
use crate::worker::embedded_worker_factory;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    enum WorkerMode {
        Embedded,
        Remote,
    }

    /// How long a submit holds its answer back.
    /// Mirrors the request's own wait, reduced to what a C++ frontend asks for.
    enum WaitMode {
        /// Answer once the statement is applied to the catalog.
        None,
        /// Hold a create until its query completes, or terminates some other way.
        UntilCompleted,
        /// Hold a read until every query that it lists has terminated.
        UntilTerminated,
    }

    /// A query's lifecycle state, as the catalog records it.
    /// Mirrors the catalog's own enum so a caller matches on a state rather than on an integer.
    enum QueryState {
        Pending,
        Started,
        Running,
        Completed,
        Stopped,
        Failed,
    }

    /// The outcome of one submitted statement.
    /// A cxx shared struct cannot hold an optional, so an absent value is encoded as zero.
    struct StatementOutcome {
        /// A submission or planning failure, or the error of the first fragment that failed.
        error: BridgeError,
        /// Zero for a statement that creates no query.
        query_id: i64,
        /// The query's state when the wait released it.
        state: QueryState,
        start_ms: u64,
        stop_ms: u64,
        /// Plain text or JSON as the frontend asked; empty when the statement failed.
        result: String,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        type BridgeError = crate::error::ffi::BridgeError;
    }

    extern "Rust" {
        /// A coordinator owned by a C++ frontend; dropping the box shuts it down.
        type Coordinator;

        /// An empty `db_path` selects the in-memory catalog, otherwise a sqlite one at that path.
        fn start_coordinator(
            db_path: &str,
            worker_mode: WorkerMode,
            optimizer_config: &str,
        ) -> Result<Box<Coordinator>>;

        fn connect_coordinator(url: &str) -> Result<Box<Coordinator>>;

        /// The host for a statement that omits its HOST clause, or empty when one is required.
        fn default_host(self: &Coordinator) -> String;

        /// Runs one statement and holds the answer back as `wait` says.
        /// Fails after `timeout_ms` with `QueryWaitTimeout`; zero waits without a deadline.
        /// Never throws: a failure is reported in the error field so its code is kept.
        /// Safe to call from several threads at once.
        fn submit(
            self: &Coordinator,
            sql: &str,
            wait: WaitMode,
            timeout_ms: u64,
            as_json: bool,
        ) -> StatementOutcome;

        /// Releases every parked wait without waiting for the queries.
        /// For a frontend that was told to terminate while queries may still run for a long time.
        /// Safe to call when nothing is waiting; the next wait then returns immediately.
        fn cancel_waits(self: &Coordinator);
    }
}

pub struct Coordinator {
    backend: Backend,
    default_host: String,
}

enum Backend {
    Embedded {
        sender: async_channel::Sender<Request>,
        runtime: Option<Runtime>,
        cancel: watch::Sender<bool>,
    },
    Remote(Client),
}

const POLL_SLICE: Duration = Duration::from_secs(1);

/// The host a statement without a HOST clause is placed on, or empty when there is none to default to.
/// Embedded deployments run a single in-process worker, so defaulting to it is unambiguous.
/// Remote deployments have several, so there is nothing to default to.
#[must_use]
pub fn default_host(mode: ffi::WorkerMode) -> &'static str {
    match mode {
        ffi::WorkerMode::Embedded => "localhost:8080",
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

        let default_host = default_host(mode).to_string();
        let planner = sql_planner(runtime.handle().clone(), optimizer_config, &default_host);
        let factory = match mode {
            ffi::WorkerMode::Embedded => Some(embedded_worker_factory()),
            _ => None,
        };

        let sender = start_with_runtime(&runtime, state_backend, planner, factory)?;
        Ok(Self {
            backend: Backend::Embedded {
                sender,
                runtime: Some(runtime),
                cancel: watch::channel(false).0,
            },
            default_host,
        })
    }

    pub fn connect(url: &str) -> Result<Self> {
        let url = url
            .parse()
            .with_context(|| format!("invalid coordinator URL '{url}'"))?;
        let client = Client::new(url)?.with_poll_slice(POLL_SLICE);
        Ok(Self {
            backend: Backend::Remote(client),
            default_host: String::new(),
        })
    }

    pub fn send(&self, input: StatementInput, wait: Wait) -> Result<StatementResult> {
        match &self.backend {
            Backend::Embedded {
                sender, runtime, ..
            } => {
                let (rx, req) = Request::new(input, wait);
                sender
                    .send_blocking(req)
                    .map_err(|_| anyhow!("coordinator shut down"))?;
                block_on(runtime, rx)?
            }
            Backend::Remote(client) => remote_send(client, input, wait),
        }
    }

    /// Answers `None` as soon as the waits are cancelled.
    /// The request is abandoned rather than withdrawn.
    /// The coordinator still holds it and later answers into a receiver that nobody reads,
    /// or, over the network, finishes the request in flight for a client that no longer asks again.
    fn send_until_cancelled(
        &self,
        input: StatementInput,
        wait: Wait,
    ) -> Result<Option<StatementResult>> {
        match &self.backend {
            Backend::Embedded {
                sender,
                runtime,
                cancel,
            } => {
                let mut cancel = cancel.subscribe();
                if *cancel.borrow_and_update() {
                    return Ok(None);
                }
                let (rx, req) = Request::new(input, wait);
                sender
                    .send_blocking(req)
                    .map_err(|_| anyhow!("coordinator shut down"))?;
                block_on(runtime, async {
                    tokio::select! {
                        reply = rx => reply.map_err(anyhow::Error::from).and_then(|r| r).map(Some),
                        _ = cancel.changed() => Ok(None),
                    }
                })
            }
            Backend::Remote(client) => match remote_send(client, input, wait) {
                Err(error) if is_cancelled(&error) => Ok(None),
                result => result.map(Some),
            },
        }
    }

    /// A frontend reads it to register its worker there.
    /// The address is then written once and cannot drift.
    pub(crate) fn default_host(&self) -> String {
        self.default_host.clone()
    }

    pub(crate) fn cancel_waits(&self) {
        match &self.backend {
            Backend::Embedded { cancel, .. } => {
                let _ = cancel.send(true);
            }
            Backend::Remote(client) => client.cancel(),
        }
    }

    pub(crate) fn submit(
        &self,
        sql: &str,
        wait: ffi::WaitMode,
        timeout_ms: u64,
        as_json: bool,
    ) -> ffi::StatementOutcome {
        let timeout = (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms));
        let wait = match wait {
            ffi::WaitMode::UntilCompleted => Wait::UntilState {
                state: QueryState::Completed,
                timeout,
            },
            ffi::WaitMode::UntilTerminated => Wait::UntilTerminated { timeout },
            _ => Wait::None,
        };
        let result = match self.send_until_cancelled(StatementInput::Sql(sql.to_string()), wait) {
            // The wait was released before the answer arrived, so there is nothing to report.
            Ok(None) => return applied(String::new()),
            Ok(Some(result)) => result,
            // A query that terminates without completing comes back as an early termination,
            // and its row records the failure that ended it.
            Err(error) => {
                return match error.downcast_ref::<EarlyTermination>() {
                    Some(EarlyTermination(created)) => {
                        outcome(&created.query, query_failure(&created.query), String::new())
                    }
                    None => failed(&error),
                };
            }
        };
        let rendered = match render(&result, as_json) {
            Ok(rendered) => rendered,
            Err(error) => return failed(&error),
        };
        match result {
            // A create reports the query's identity and the state that the wait released it in.
            StatementResult::CreatedQuery(created) => {
                outcome(&created.query, ffi::BridgeError::default(), rendered)
            }
            // Every other statement is terminal the moment it returns.
            _ => applied(rendered),
        }
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        if let Backend::Embedded { runtime, .. } = &mut self.backend
            && let Some(rt) = runtime.take()
        {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

fn block_on<F: Future>(runtime: &Option<Runtime>, fut: F) -> F::Output {
    runtime
        .as_ref()
        .expect("runtime present until drop")
        .handle()
        .block_on(fut)
}

fn remote_send(client: &Client, input: StatementInput, wait: Wait) -> Result<StatementResult> {
    let (kind, deadline, target) = match wait {
        Wait::UntilState { state, timeout } => (wait_kind(state), deadline(timeout), Some(state)),
        Wait::UntilTerminated { timeout } => (Some(WaitKind::Terminated), deadline(timeout), None),
        Wait::None => (None, None, None),
    };
    let result = match (input, kind) {
        (StatementInput::Sql(sql), None) => client.sql(&sql)?,
        (StatementInput::Sql(sql), Some(kind)) => client.sql_wait(&sql, kind, deadline)?,
        (StatementInput::Parsed(statement), None) => client.statement(&statement)?,
        (StatementInput::Parsed(statement), Some(kind)) => {
            client.statement_wait(&statement, kind, deadline)?
        }
    };
    match (result, target) {
        (StatementResult::CreatedQuery(created), Some(target))
            if matches!(
                created.query.state,
                QueryState::Stopped | QueryState::Failed
            ) && created.query.state != target =>
        {
            Err(anyhow::Error::new(EarlyTermination(created)))
        }
        (result, _) => Ok(result),
    }
}

const fn wait_kind(state: QueryState) -> Option<WaitKind> {
    match state {
        QueryState::Pending => None,
        QueryState::Started => Some(WaitKind::Started),
        QueryState::Running => Some(WaitKind::Running),
        QueryState::Completed => Some(WaitKind::Completed),
        QueryState::Stopped | QueryState::Failed => Some(WaitKind::Terminated),
    }
}

fn deadline(timeout: Option<Duration>) -> Option<Instant> {
    timeout.map(|timeout| Instant::now() + timeout)
}

fn is_cancelled(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<ClientError>(),
        Some(ClientError::Cancelled)
    )
}

pub(crate) fn start_coordinator(
    db_path: &str,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> Result<Box<Coordinator>, FfiError> {
    let state_backend = if db_path.is_empty() {
        StateBackend::Memory
    } else {
        StateBackend::sqlite(db_path)
    };
    Ok(Box::new(Coordinator::start(
        state_backend,
        mode,
        optimizer_config,
    )?))
}

pub(crate) fn connect_coordinator(url: &str) -> Result<Box<Coordinator>, FfiError> {
    Ok(Box::new(Coordinator::connect(url)?))
}

/// A statement that created no query is terminal at once; its answer, if any, is the given text.
fn applied(result: String) -> ffi::StatementOutcome {
    ffi::StatementOutcome {
        error: ffi::BridgeError::default(),
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

/// The outcome of a created query: its state and timestamps, with the given error and answer.
fn outcome(query: &Query, error: ffi::BridgeError, result: String) -> ffi::StatementOutcome {
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
        result,
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
        Ok(format!("{result}"))
    }
}
