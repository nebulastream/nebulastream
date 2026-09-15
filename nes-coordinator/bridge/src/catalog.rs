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

//! The catalog lookups and writes the C++ optimizer makes while planning a statement.
//! They all run on the transaction of the request that is being planned,
//! so the optimizer sees the writes of that request and a rollback undoes them.
//! The optimizer thread has no async runtime, so each call blocks on the runtime of the request.
//! Every name in these calls is in SQL form, as described in `nes-coordinator/model/src/name.rs`.

use model::ConnectorKind;
use model::Execute;
use model::error::{CodedError, ErrorCode};
use model::ml_model::GetMlModel;
use model::name::Identifier;
use model::query;
use model::sink::{CreateAnonymousSink, GetSink};
use model::source::logical::GetLogicalSource;
use model::source::physical::{CreateAnonymousSource, GetPhysicalSource};
use model::worker::endpoint::NetworkAddr;
use model::worker::network_link::Entity as NetworkLinkEntity;
use model::worker::{DesiredWorkerState, GetWorker};
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
use std::collections::HashSet;
use tokio::runtime::Handle;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    #[derive(Default)]
    struct LogicalSource {
        name: String,
        schema_json: String,
        error: BridgeError,
    }

    #[derive(Default)]
    struct SourceDescriptor {
        id: i64,
        host_addr: String,
        source_type: String,
        source_config_json: String,
        parser_config_json: String,
        is_anonymous: bool,
    }

    #[derive(Default)]
    struct SinkDescriptor {
        id: i64,
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
        error: BridgeError,
    }

    #[derive(Default)]
    struct MlModel {
        /// One JSON object with the keys `name`, `path`, `imported`, `inputs` and `outputs`.
        model_json: String,
        error: BridgeError,
    }

    #[derive(Default)]
    struct Worker {
        host_addr: String,
        data_addr: String,
        /// The operators that the worker can still host, -1 when the worker has no limit.
        max_operators: i32,
        error: BridgeError,
    }

    #[derive(Default)]
    struct NetworkLink {
        src_addr: String,
        dst_addr: String,
    }

    #[derive(Default)]
    struct Topology {
        error: BridgeError,
        nodes: Vec<String>,
        links: Vec<NetworkLink>,
    }

    #[derive(Default)]
    struct SourceDescriptors {
        error: BridgeError,
        items: Vec<SourceDescriptor>,
    }

    #[derive(Default)]
    struct CreatedId {
        error: BridgeError,
        id: i64,
    }

    #[derive(Default)]
    struct QueryExists {
        error: BridgeError,
        exists: bool,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        type BridgeError = crate::error::ffi::BridgeError;
    }

    extern "Rust" {
        #[derive(ExternType)]
        type PlanningTransaction;

        fn get_logical_source(ctx: &PlanningTransaction, name: &str) -> LogicalSource;
        fn get_source_descriptors(
            ctx: &PlanningTransaction,
            logical_source_name: &str,
        ) -> SourceDescriptors;
        fn get_sink_descriptor(ctx: &PlanningTransaction, name: &str) -> SinkDescriptor;
        fn get_worker(ctx: &PlanningTransaction, host_addr: &str) -> Worker;
        fn get_topology(ctx: &PlanningTransaction) -> Topology;
        fn get_ml_model(ctx: &PlanningTransaction, name: &str) -> MlModel;
        fn contains_query(ctx: &PlanningTransaction, name: &str) -> QueryExists;
        fn create_anonymous_source(
            ctx: &PlanningTransaction,
            internal: bool,
            source_type: &str,
            source_config_json: &str,
            parser_config_json: &str,
            host_addr: &str,
        ) -> CreatedId;
        fn create_anonymous_sink(
            ctx: &PlanningTransaction,
            internal: bool,
            sink_type: &str,
            schema_json: &str,
            config_json: &str,
            host_addr: &str,
        ) -> CreatedId;
    }
}

/// One request's transaction and the runtime that completes its futures.
/// The optimizer borrows both for one planning call, so the request keeps ownership of the transaction.
pub struct PlanningTransaction {
    txn: DatabaseTransaction,
    handle: Handle,
}

impl PlanningTransaction {
    pub(crate) fn new(txn: DatabaseTransaction, handle: Handle) -> Self {
        Self { txn, handle }
    }

    pub(crate) fn into_txn(self) -> DatabaseTransaction {
        self.txn
    }

    /// For callers that run their own statement on the transaction.
    pub(crate) fn txn(&self) -> &DatabaseTransaction {
        &self.txn
    }

    pub(crate) fn block_on<F: Future>(&self, work: F) -> F::Output {
        self.handle.block_on(work)
    }

    /// A database failure gets the same error code for every lookup, so the code is assigned here.
    fn execute_blocking<E: Execute>(&self, req: E) -> anyhow::Result<E::Response> {
        self.handle
            .block_on(req.execute(&self.txn))
            .map_err(catalog_unavailable)
    }
}

fn catalog_unavailable(error: anyhow::Error) -> anyhow::Error {
    if error.downcast_ref::<CodedError>().is_none()
        && error.downcast_ref::<sea_orm::DbErr>().is_some()
    {
        return CodedError::new(ErrorCode::CatalogUnavailable, format!("{error:#}")).into();
    }
    error
}

/// An FFI result struct with an `error` field.
trait WithError: Default {
    fn from_error(error: ffi::BridgeError) -> Self;
}

macro_rules! impl_with_error {
    ($($result:ident),* $(,)?) => {$(
        impl WithError for ffi::$result {
            fn from_error(error: ffi::BridgeError) -> Self {
                Self { error, ..Self::default() }
            }
        }
    )*};
}
impl_with_error!(
    LogicalSource,
    SourceDescriptors,
    SinkDescriptor,
    MlModel,
    Worker,
    Topology,
    CreatedId,
    QueryExists
);

/// Puts a failure into the `error` field of the result struct.
fn or_error<T: WithError>(result: anyhow::Result<T>) -> T {
    result.unwrap_or_else(|err| T::from_error((&err).into()))
}

pub(crate) fn get_logical_source(ctx: &PlanningTransaction, name: &str) -> ffi::LogicalSource {
    or_error(logical_source(ctx, name))
}

fn logical_source(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::LogicalSource> {
    let found =
        ctx.execute_blocking(GetLogicalSource::all().with_name(Identifier::parse(name)?))?;
    let source = found.into_iter().next().ok_or_else(|| {
        CodedError::new(
            ErrorCode::UnknownSourceName,
            format!("logical source '{name}' not found"),
        )
    })?;
    Ok(ffi::LogicalSource {
        name: source.name.to_sql(),
        schema_json: serde_json::to_string(&source.schema)?,
        ..Default::default()
    })
}

pub(crate) fn get_source_descriptors(
    ctx: &PlanningTransaction,
    logical_source_name: &str,
) -> ffi::SourceDescriptors {
    or_error(source_descriptors(ctx, logical_source_name))
}

fn source_descriptors(
    ctx: &PlanningTransaction,
    logical_source_name: &str,
) -> anyhow::Result<ffi::SourceDescriptors> {
    let items = ctx
        .execute_blocking(
            GetPhysicalSource::all().with_logical_source(Identifier::parse(logical_source_name)?),
        )?
        .into_iter()
        .map(|s| {
            Ok(ffi::SourceDescriptor {
                id: *s.id,
                host_addr: s.host_addr.to_string(),
                source_type: s.source_type.to_sql(),
                source_config_json: serde_json::to_string(&s.source_config)?,
                parser_config_json: serde_json::to_string(&s.parser_config)?,
                is_anonymous: s.kind != ConnectorKind::Shared,
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(ffi::SourceDescriptors {
        items,
        ..Default::default()
    })
}

pub(crate) fn get_sink_descriptor(ctx: &PlanningTransaction, name: &str) -> ffi::SinkDescriptor {
    or_error(sink_descriptor(ctx, name))
}

fn sink_descriptor(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::SinkDescriptor> {
    let found = ctx.execute_blocking(GetSink::all().with_name(Identifier::parse(name)?))?;
    let sink = found.into_iter().next().ok_or_else(|| {
        CodedError::new(
            ErrorCode::UnknownSinkName,
            format!("sink '{name}' not found"),
        )
    })?;
    Ok(ffi::SinkDescriptor {
        id: *sink.id,
        name: sink
            .name
            .as_ref()
            .map(Identifier::to_sql)
            .unwrap_or_default(),
        host_addr: sink.host_addr.to_string(),
        sink_type: sink.sink_type.to_sql(),
        schema_json: serde_json::to_string(&sink.schema)?,
        config_json: serde_json::to_string(&sink.config)?,
        ..Default::default()
    })
}

pub(crate) fn contains_query(ctx: &PlanningTransaction, name: &str) -> ffi::QueryExists {
    or_error(query_exists(ctx, name))
}

fn query_exists(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::QueryExists> {
    let holder = query::Entity::find()
        .filter(query::Column::Name.eq(Identifier::parse(name)?))
        .filter(query::Column::Dropped.eq(false));
    let found = ctx
        .block_on(holder.one(&ctx.txn))
        .map_err(|error| catalog_unavailable(error.into()))?;
    Ok(ffi::QueryExists {
        exists: found.is_some(),
        ..Default::default()
    })
}

pub(crate) fn get_ml_model(ctx: &PlanningTransaction, name: &str) -> ffi::MlModel {
    or_error(ml_model(ctx, name))
}

fn ml_model(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::MlModel> {
    let found = ctx.execute_blocking(GetMlModel::all().with_name(name.to_string()))?;
    let model = found.into_iter().next().ok_or_else(|| {
        CodedError::new(
            ErrorCode::UnknownModelName,
            format!("ml model '{name}' not found"),
        )
    })?;
    Ok(ffi::MlModel {
        model_json: serde_json::json!({
            "name": model.name,
            "path": model.path,
            "imported": model.imported,
            "inputs": model.input_schema,
            "outputs": model.output_schema,
        })
        .to_string(),
        ..Default::default()
    })
}

pub(crate) fn get_worker(ctx: &PlanningTransaction, host_addr: &str) -> ffi::Worker {
    or_error(worker(ctx, host_addr))
}

fn worker(ctx: &PlanningTransaction, host_addr: &str) -> anyhow::Result<ffi::Worker> {
    let addr = host_addr
        .parse::<NetworkAddr>()
        .map_err(|err| CodedError::new(ErrorCode::InvalidTopology, err))?;
    let found = ctx.execute_blocking(GetWorker::all().with_host_addr(addr))?;
    let worker = found.into_iter().next().ok_or_else(|| {
        CodedError::new(
            ErrorCode::UnknownWorker,
            format!("worker '{host_addr}' not found"),
        )
    })?;
    let free = ctx
        .block_on(worker.free_capacity(ctx.txn()))
        .map_err(|err| catalog_unavailable(err.into()))?;
    Ok(ffi::Worker {
        host_addr: worker.host_addr.to_string(),
        data_addr: worker.data_addr.to_string(),
        // A negative value means unbounded, so an overfull worker reports no room at all.
        max_operators: free.map_or(-1, |free| i32::try_from(free.max(0)).unwrap_or(i32::MAX)),
        ..Default::default()
    })
}

pub(crate) fn get_topology(ctx: &PlanningTransaction) -> ffi::Topology {
    or_error(topology(ctx))
}

fn topology(ctx: &PlanningTransaction) -> anyhow::Result<ffi::Topology> {
    let workers =
        ctx.execute_blocking(GetWorker::all().with_desired_state(DesiredWorkerState::Active))?;
    let links = ctx
        .block_on(NetworkLinkEntity::find().all(ctx.txn()))
        .map_err(|err| catalog_unavailable(err.into()))?;
    let nodes: HashSet<NetworkAddr> = workers.into_iter().map(|w| w.host_addr).collect();
    Ok(ffi::Topology {
        links: links
            .into_iter()
            .filter(|l| nodes.contains(&l.source_host_addr) && nodes.contains(&l.target_host_addr))
            .map(|l| ffi::NetworkLink {
                src_addr: l.source_host_addr.to_string(),
                dst_addr: l.target_host_addr.to_string(),
            })
            .collect(),
        nodes: nodes.iter().map(ToString::to_string).collect(),
        ..Default::default()
    })
}

/// The host of an anonymous source or sink is one of its options, so a malformed one is an invalid option.
fn parse_host(host_addr: &str) -> anyhow::Result<NetworkAddr> {
    host_addr
        .parse()
        .map_err(|err: String| CodedError::new(ErrorCode::InvalidConfigParameter, err).into())
}

pub(crate) fn create_anonymous_source(
    ctx: &PlanningTransaction,
    internal: bool,
    source_type: &str,
    source_config_json: &str,
    parser_config_json: &str,
    host_addr: &str,
) -> ffi::CreatedId {
    or_error((|| {
        let source = ctx.execute_blocking(CreateAnonymousSource {
            source_type: source_type.parse()?,
            source_config: serde_json::from_str(source_config_json)?,
            parser_config: serde_json::from_str(parser_config_json)?,
            host_addr: parse_host(host_addr)?,
            internal,
        })?;
        Ok(ffi::CreatedId {
            id: *source.id,
            ..Default::default()
        })
    })())
}

pub(crate) fn create_anonymous_sink(
    ctx: &PlanningTransaction,
    internal: bool,
    sink_type: &str,
    schema_json: &str,
    config_json: &str,
    host_addr: &str,
) -> ffi::CreatedId {
    or_error((|| {
        let sink = ctx.execute_blocking(CreateAnonymousSink {
            sink_type: sink_type.parse()?,
            schema: serde_json::from_str(schema_json)?,
            config: serde_json::from_str(config_json)?,
            host_addr: parse_host(host_addr)?,
            internal,
        })?;
        Ok(ffi::CreatedId {
            id: *sink.id,
            ..Default::default()
        })
    })())
}
