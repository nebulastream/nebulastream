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

//! The FFI surface between the C++ frontend and the Rust coordinator.
//! The bridge block below declares everything that crosses, and each group of implementations has its own module.
//!
//! `catalog` holds the reads and writes the C++ optimizer makes while it plans a statement.
//! `handle` starts a coordinator and keeps it open, and `embedded` is the one a C++ frontend owns and submits SQL to.
//! `error` holds how a failure reaches C++, and `test_context` a catalog a C++ test seeds without a running coordinator.
//! `planner_bridge` and `worker_bridge` go the other way and call into C++, to plan a statement and to run a fragment.

mod catalog;
mod embedded;
mod error;
mod handle;
mod planner_bridge;
mod test_context;
mod worker_bridge;

// cxx resolves each item the bridge names in the module the bridge is declared in, so every one of them is imported here.
use catalog::{
    create_anonymous_sink, create_anonymous_source, get_logical_source, get_ml_model,
    get_sink_descriptor, get_source_descriptors, get_topology, get_worker,
};
use embedded::{EmbeddedCoordinator, start_embedded_coordinator};
use test_context::{TestTransactionContext, create_test_planner_context};

pub use catalog::TransactionContext;
pub use error::FfiError;
pub use handle::{CoordinatorHandle, start_coordinator};

#[cxx::bridge]
pub mod ffi {
    enum WorkerMode {
        Embedded,
        Remote,
    }

    struct LogicalSource {
        name: String,
        schema_json: String,
        /// Zero when the lookup found one, and otherwise the code the caller raises.
        /// A miss means the statement asked for something that is not there, so the lookup reports the code the shared list gives that.
        /// The lookup puts it in the struct rather than returning an `Err`, because cxx renders a returned error with `Display` and the
        /// code would not survive the crossing.
        error: BridgeError,
    }

    struct SourceDescriptor {
        id: i64,
        logical_source_name: String,
        host_addr: String,
        source_type: String,
        source_config_json: String,
        parser_config_json: String,
        is_anonymous: bool,
    }

    struct SinkDescriptor {
        id: i64,
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
        /// Zero when the lookup found one, and otherwise the code the caller raises.
        /// See `LogicalSource` for why the lookup reports it here.
        error: BridgeError,
    }

    struct MlModel {
        name: String,
        path: String,
        input_schema_json: String,
        output_schema_json: String,
        imported_json: String,
        /// Zero when the lookup found one, and otherwise the code the caller raises.
        /// See `LogicalSource` for why the lookup reports it here.
        error: BridgeError,
    }

    struct Worker {
        host_addr: String,
        data_addr: String,
        max_operators: i32,
        /// Zero when the lookup found one, and otherwise the code the caller raises.
        /// See `LogicalSource` for why the lookup reports it here.
        error: BridgeError,
    }

    struct NetworkLink {
        src_addr: String,
        dst_addr: String,
    }

    struct Topology {
        nodes: Vec<String>,
        links: Vec<NetworkLink>,
    }

    /// An error crossing the FFI boundary, or the absence of one: `code` is zero when nothing failed.
    /// cxx shared structs cannot hold an `Option`, so the zero code stands in for it.
    #[namespace = "NES"]
    struct BridgeError {
        code: u16,
        msg: String,
        trace: String,
    }

    #[namespace = "NES"]
    struct PlannedQueryFragment {
        host_addr: String,
        plan: Vec<u8>,
        num_operators: i32,
        has_source: bool,
    }

    /// `error` reports whether planning succeeded, and the remaining fields are only meaningful when it did.
    /// Planning failures travel in the struct rather than as a thrown exception so the NES error code and stacktrace
    /// survive, which a cxx exception (message only) would drop.
    #[namespace = "NES"]
    struct PlannedStatement {
        error: BridgeError,
        json: String,
        fragments: Vec<PlannedQueryFragment>,
        source_ids: Vec<i64>,
        sink_ids: Vec<i64>,
    }

    #[namespace = "NES"]
    unsafe extern "C++" {
        include!("PlannerBridge.hpp");

        fn plan_sql(
            ctx: &TransactionContext,
            sql: &str,
            optimizer_config: &str,
            default_host: &str,
        ) -> Result<PlannedStatement>;
    }

    /// `error` reports whether the status read itself worked, and the remaining fields are only meaningful when it did.
    /// `query_error` is the failure the query recorded on the worker, which a successful read reports as data.
    #[namespace = "NES"]
    struct BridgeQueryStatus {
        error: BridgeError,
        state: i32,
        start_ms: u64,
        stop_ms: u64,
        query_error: BridgeError,
    }

    #[namespace = "NES"]
    unsafe extern "C++" {
        include!("WorkerBridge.hpp");

        type WorkerBridge;
        fn start_worker(config_json: &str) -> Result<UniquePtr<WorkerBridge>>;
        fn start_query(bridge: Pin<&mut WorkerBridge>, serialized_fragment: &[u8]) -> BridgeError;
        fn stop_query(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeError;
        fn query_status(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeQueryStatus;
        fn worker_version() -> String;
        fn call_enable_memcom();
    }

    #[namespace = "NES"]
    extern "Rust" {
        type TransactionContext;

        /// Test-support: a `TransactionContext` backed by a throwaway in-memory catalog, so C++ tests can run the
        /// optimizer without a running coordinator.
        /// Seed it with `execute_seed_statement`, then hand `context()` to a `QueryOptimizer`.
        type TestTransactionContext;
        fn create_test_planner_context() -> Result<Box<TestTransactionContext>>;
        fn context(self: &TestTransactionContext) -> &TransactionContext;
        fn execute_seed_statement(
            self: &TestTransactionContext,
            statement_json: &str,
        ) -> Result<()>;

        fn get_logical_source(ctx: &TransactionContext, name: &str) -> LogicalSource;
        fn get_source_descriptors(
            ctx: &TransactionContext,
            logical_source_name: &str,
        ) -> Result<Vec<SourceDescriptor>>;
        fn get_sink_descriptor(ctx: &TransactionContext, name: &str) -> SinkDescriptor;
        fn get_worker(ctx: &TransactionContext, host_addr: &str) -> Worker;
        fn get_topology(ctx: &TransactionContext) -> Result<Topology>;
        fn get_ml_model(ctx: &TransactionContext, name: &str) -> MlModel;
        fn create_anonymous_source(
            ctx: &TransactionContext,
            internal: bool,
            source_type: &str,
            schema_json: &str,
            source_config_json: &str,
            parser_config_json: &str,
            host_addr: &str,
        ) -> Result<i64>;
        fn create_anonymous_sink(
            ctx: &TransactionContext,
            internal: bool,
            sink_type: &str,
            schema_json: &str,
            config_json: &str,
            host_addr: &str,
        ) -> Result<i64>;
    }

    /// The lifecycle state a query is in, as the catalog records it.
    /// Mirrors the catalog's own enum so a caller matches on a state rather than on an integer.
    #[namespace = "NES"]
    enum QueryState {
        Pending,
        Started,
        Running,
        Completed,
        Stopped,
        Failed,
    }

    /// The final outcome of one submitted statement, once it has reached a terminal state.
    /// A statement that creates no query is terminal the moment it returns and reports `query_id` zero.
    /// A query blocks until it stops, and its terminal `state` with the aggregated `error` reports how it ended.
    /// `error` also reports a submission or planning failure with its code.
    /// An unset timestamp reads as zero, since a cxx shared struct cannot hold an optional.
    #[namespace = "NES"]
    struct StatementOutcome {
        error: BridgeError,
        query_id: i64,
        state: QueryState,
        start_ms: u64,
        stop_ms: u64,
        /// What the statement answered with, which only a statement answering in text has.
        /// An EXPLAIN answers with the plan it computed, and every other statement leaves this empty.
        result: String,
    }

    #[namespace = "NES"]
    extern "Rust" {
        /// An embedded coordinator owned by a C++ frontend.
        /// It holds the long-lived request channel and its runtime, and dropping the box shuts the coordinator down.
        type EmbeddedCoordinator;
        fn start_embedded_coordinator(
            db_path: &str,
            worker_mode: WorkerMode,
            optimizer_config: &str,
        ) -> Result<Box<EmbeddedCoordinator>>;
        fn submit_sql(self: &EmbeddedCoordinator, sql: &str, as_json: bool) -> Result<String>;
        fn await_termination(self: &EmbeddedCoordinator, as_json: bool) -> Result<String>;
        fn cancel_await_termination(self: &EmbeddedCoordinator);

        /// The address a statement that omits its HOST clause is placed on, empty when this deployment requires an explicit one.
        fn default_host(self: &EmbeddedCoordinator) -> String;

        /// The typed counterpart to the rendering submit above, for a caller that acts on the result rather than printing it.
        /// Blocks until the statement is terminal: immediately for a statement that starts no query, and once the query stops otherwise.
        /// Gives up after `timeout_ms` and answers with `QueryWaitTimeout`, so a query that never terminates fails its
        /// caller rather than parking it forever. Zero waits without a deadline.
        /// Never throws: a failure travels in the returned error field, so a planning error code survives instead of being
        /// flattened into an exception message.
        /// Safe to call from several threads at once.
        fn submit(self: &EmbeddedCoordinator, sql: &str, timeout_ms: u64) -> StatementOutcome;
    }
}
