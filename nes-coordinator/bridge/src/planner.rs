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

//! Planning a statement in the C++ planner.
//! The planner reads the catalog only from the snapshot that it is given, so no transaction crosses the bridge.

use anyhow::{Context, Result, anyhow};
use cxx::UniquePtr;
use model::ConnectorKind;
use model::error::{CodedError, ErrorCode};
use model::identifier::{SinkId, SourceId};
use model::query::query_fragment::CreateQueryFragment;
use model::sink;
use model::source::physical;
use model::statement::ParsedStatement;
use model::worker::endpoint::NetworkAddr;
use request_handler::{CatalogSnapshot, SqlPlanner};
use tracing::debug;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    struct LogicalSource {
        name: String,
        schema_json: String,
    }

    /// A shared source of the snapshot, or a source that the planner created for a query.
    struct PhysicalSource {
        id: i64,
        /// Empty for a created source.
        logical_source: String,
        host_addr: String,
        source_type: String,
        source_config_json: String,
        parser_config_json: String,
        /// Whether the planner created it as a network channel between two workers.
        internal: bool,
    }

    /// A shared sink of the snapshot, or a sink that the planner created for a query.
    struct Sink {
        id: i64,
        /// Empty for a created sink.
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
        /// Whether the planner created it as a network channel between two workers.
        internal: bool,
    }

    struct MlModel {
        name: String,
        /// One JSON object with the keys `name`, `path`, `imported`, `inputs` and `outputs`.
        model_json: String,
    }

    struct Worker {
        host_addr: String,
        data_addr: String,
        /// -1 when the worker has no limit.
        max_operators: i32,
    }

    struct NetworkLink {
        src_addr: String,
        dst_addr: String,
    }

    /// The catalog as the planner sees it.
    /// The planner assigns the next ids to the sources and sinks that it creates.
    struct CatalogSnapshot {
        logical_sources: Vec<LogicalSource>,
        physical_sources: Vec<PhysicalSource>,
        sinks: Vec<Sink>,
        models: Vec<MlModel>,
        workers: Vec<Worker>,
        links: Vec<NetworkLink>,
        next_source_id: i64,
        next_sink_id: i64,
    }

    struct PlannedQueryFragment {
        host_addr: String,
        plan: Vec<u8>,
        num_operators: i32,
        has_source: bool,
    }

    /// The result of planning one statement.
    /// The other fields are only meaningful when `error` is empty.
    struct PlannedStatement {
        error: BridgeError,
        /// The statement for the coordinator to execute, as JSON.
        statement_json: String,
        /// The optimized plan of a query before placement, as protobuf bytes.
        /// Empty for every other statement.
        global_plan: Vec<u8>,
        /// The per-worker plans of a query, each as protobuf bytes.
        /// Empty for every other statement.
        fragments: Vec<PlannedQueryFragment>,
        /// The sources and sinks that the optimizer created for a query or for an EXPLAIN.
        created_sources: Vec<PhysicalSource>,
        created_sinks: Vec<Sink>,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        include!("PlannerBridge.hpp");

        type BridgeError = crate::error::ffi::BridgeError;

        /// The settings that every statement of one coordinator is planned with.
        type Planner;

        /// Throws when an optimizer option is invalid.
        /// An empty default host means that a statement without a HOST clause is rejected.
        fn make_planner(optimizer_config: &str, default_host: &str) -> Result<UniquePtr<Planner>>;

        fn plan_sql(planner: &Planner, snapshot: &CatalogSnapshot, sql: &str) -> PlannedStatement;
    }
}

/// Plans SQL in the C++ planner.
pub(crate) struct FfiSqlPlanner {
    planner: UniquePtr<ffi::Planner>,
}

// Safety: the C++ planner is not changed after it is made, and planning only reads it.
unsafe impl Send for FfiSqlPlanner {}
unsafe impl Sync for FfiSqlPlanner {}

impl FfiSqlPlanner {
    pub(crate) fn new(optimizer_config: &str, default_host: &str) -> Result<Self> {
        let planner = ffi::make_planner(optimizer_config, default_host)
            .context("invalid optimizer configuration")?;
        Ok(Self { planner })
    }
}

impl SqlPlanner for FfiSqlPlanner {
    fn plan(&self, sql: &str, snapshot: CatalogSnapshot) -> Result<ParsedStatement> {
        let planned = ffi::plan_sql(&self.planner, &snapshot.try_into()?, sql);
        if let Some(failure) = planned.error.into_error() {
            // The caller is usually the person who wrote the SQL and gets the message and the code.
            // The C++ stacktrace would be noise to them and goes to the log instead.
            debug!(code = ?failure.code, trace = %failure.trace, "planning failed");
            return Err(failure.into());
        }
        let mut statement: ParsedStatement = serde_json::from_str(&planned.statement_json)?;

        // Only a created query keeps the sources and sinks of the optimizer, so an EXPLAIN writes nothing.
        if let ParsedStatement::CreateQuery(ref mut q) = statement {
            q.global_plan = planned.global_plan;
            q.fragments = planned
                .fragments
                .into_iter()
                .map(CreateQueryFragment::try_from)
                .collect::<Result<_>>()?;
            q.created_sources = planned
                .created_sources
                .into_iter()
                .map(physical::Model::try_from)
                .collect::<Result<_>>()?;
            q.created_sinks = planned
                .created_sinks
                .into_iter()
                .map(sink::Model::try_from)
                .collect::<Result<_>>()?;
        }
        Ok(statement)
    }

    fn validate_worker_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.is_null() {
            return Ok(());
        }
        crate::worker::validate_worker_config(&config.to_string())
    }
}

impl TryFrom<CatalogSnapshot> for ffi::CatalogSnapshot {
    type Error = anyhow::Error;

    fn try_from(snapshot: CatalogSnapshot) -> Result<Self> {
        let logical_sources = snapshot
            .logical_sources
            .into_iter()
            .map(|s| {
                Ok(ffi::LogicalSource {
                    name: s.name,
                    schema_json: serde_json::to_string(&s.schema)?,
                })
            })
            .collect::<Result<_>>()?;
        let physical_sources = snapshot
            .physical_sources
            .into_iter()
            .map(|s| {
                Ok(ffi::PhysicalSource {
                    id: *s.id,
                    logical_source: s.logical_source.unwrap_or_default(),
                    host_addr: s.host_addr.to_string(),
                    source_type: s.source_type,
                    source_config_json: serde_json::to_string(&s.source_config)?,
                    parser_config_json: serde_json::to_string(&s.parser_config)?,
                    internal: false,
                })
            })
            .collect::<Result<_>>()?;
        let sinks = snapshot
            .sinks
            .into_iter()
            .map(|s| {
                Ok(ffi::Sink {
                    id: *s.id,
                    name: s.name.unwrap_or_default(),
                    host_addr: s.host_addr.to_string(),
                    sink_type: s.sink_type,
                    schema_json: serde_json::to_string(&s.schema)?,
                    config_json: serde_json::to_string(&s.config)?,
                    internal: false,
                })
            })
            .collect::<Result<_>>()?;
        let models = snapshot
            .models
            .into_iter()
            .map(|m| ffi::MlModel {
                model_json: serde_json::json!({
                    "name": m.name,
                    "path": m.path,
                    "imported": m.imported,
                    "inputs": m.input_schema,
                    "outputs": m.output_schema,
                })
                .to_string(),
                name: m.name,
            })
            .collect();
        let workers = snapshot
            .workers
            .into_iter()
            .map(|w| ffi::Worker {
                host_addr: w.host_addr.to_string(),
                data_addr: w.data_addr.to_string(),
                max_operators: w.max_operators.unwrap_or(-1),
            })
            .collect();
        let links = snapshot
            .links
            .into_iter()
            .map(|l| ffi::NetworkLink {
                src_addr: l.source_host_addr.to_string(),
                dst_addr: l.target_host_addr.to_string(),
            })
            .collect();
        Ok(Self {
            logical_sources,
            physical_sources,
            sinks,
            models,
            workers,
            links,
            next_source_id: snapshot.next_source_id,
            next_sink_id: snapshot.next_sink_id,
        })
    }
}

impl TryFrom<ffi::PlannedQueryFragment> for CreateQueryFragment {
    type Error = anyhow::Error;

    fn try_from(f: ffi::PlannedQueryFragment) -> Result<Self> {
        let host_addr = f.host_addr.parse().map_err(|err: String| {
            anyhow!(
                "the planner placed a fragment on '{}', which is not a worker address: {err}",
                f.host_addr
            )
        })?;
        Ok(Self {
            host_addr,
            plan: f.plan,
            num_operators: f.num_operators,
            has_source: f.has_source,
        })
    }
}

/// The host of a created source or sink is one of its options, so a malformed one is an invalid option.
fn parse_host(host_addr: &str) -> Result<NetworkAddr> {
    host_addr
        .parse()
        .map_err(|err: String| CodedError::new(ErrorCode::InvalidConfigParameter, err).into())
}

const fn created_kind(internal: bool) -> ConnectorKind {
    if internal {
        ConnectorKind::Internal
    } else {
        ConnectorKind::Anonymous
    }
}

impl TryFrom<ffi::PhysicalSource> for physical::Model {
    type Error = anyhow::Error;

    fn try_from(s: ffi::PhysicalSource) -> Result<Self> {
        Ok(Self {
            id: SourceId::new(s.id),
            logical_source: None,
            host_addr: parse_host(&s.host_addr)?,
            source_type: s.source_type,
            source_config: serde_json::from_str(&s.source_config_json)?,
            parser_config: serde_json::from_str(&s.parser_config_json)?,
            kind: created_kind(s.internal),
            query_id: None,
        })
    }
}

impl TryFrom<ffi::Sink> for sink::Model {
    type Error = anyhow::Error;

    fn try_from(s: ffi::Sink) -> Result<Self> {
        Ok(Self {
            id: SinkId::new(s.id),
            name: None,
            host_addr: parse_host(&s.host_addr)?,
            sink_type: s.sink_type,
            schema: serde_json::from_str(&s.schema_json)?,
            config: serde_json::from_str(&s.config_json)?,
            kind: created_kind(s.internal),
            query_id: None,
        })
    }
}
