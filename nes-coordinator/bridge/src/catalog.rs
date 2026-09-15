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

//! The C++ optimizer's catalog reads and writes while it plans a statement.
//! All of them run on the planned request's own transaction.
//! The optimizer thus sees that request's writes, and none of them outlive a rollback.
//! The optimizer's thread has no runtime of its own, so each call blocks on the request's runtime.
//! Each answer reports failure in its own error field, with a zero code for success.
//! A cxx exception would keep only the message, and the C++ side raises the error by its code.

use anyhow::anyhow;
use model::ConnectorKind;
use model::Execute;
use model::coded_bail;
use model::error::{CodedError, ErrorCode};
use model::ml_model::GetMlModel;
use model::sink::{CreateAnonymousSink, GetSink};
use model::source::logical::GetLogicalSource;
use model::source::physical::{CreateAnonymousSource, GetPhysicalSource};
use model::worker::GetWorker;
use model::worker::endpoint::NetworkAddr;
use model::worker::network_link::Entity as NetworkLinkEntity;
use sea_orm::{DatabaseTransaction, EntityTrait};
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
        name: String,
        path: String,
        input_schema_json: String,
        output_schema_json: String,
        imported_json: String,
        error: BridgeError,
    }

    #[derive(Default)]
    struct Worker {
        host_addr: String,
        data_addr: String,
        /// -1 when the worker has no limit.
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

/// One request's transaction and the runtime that runs its futures.
/// The optimizer borrows it for one planning call, so the request keeps ownership.
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

    /// For callers that run a statement of their own rather than one of the lookups below.
    pub(crate) fn txn(&self) -> &DatabaseTransaction {
        &self.txn
    }

    pub(crate) fn block_on<F: Future>(&self, work: F) -> F::Output {
        self.handle.block_on(work)
    }

    /// A database that cannot answer fails every lookup alike, so it is classified here once.
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

/// A lookup's answer with a field for its own failure, so the error code crosses the boundary.
trait Answer: Default {
    fn failed(error: ffi::BridgeError) -> Self;
}

macro_rules! answers {
    ($($answer:ident),* $(,)?) => {$(
        impl Answer for ffi::$answer {
            fn failed(error: ffi::BridgeError) -> Self {
                Self { error, ..Self::default() }
            }
        }
    )*};
}
answers!(
    LogicalSource,
    SourceDescriptors,
    SinkDescriptor,
    MlModel,
    Worker,
    Topology,
    CreatedId
);

fn answered<T: Answer>(result: anyhow::Result<T>) -> T {
    result.unwrap_or_else(|err| T::failed((&err).into()))
}

pub(crate) fn get_logical_source(ctx: &PlanningTransaction, name: &str) -> ffi::LogicalSource {
    answered(logical_source(ctx, name))
}

fn logical_source(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::LogicalSource> {
    let found = ctx.execute_blocking(GetLogicalSource::all().with_name(name.to_string()))?;
    let Some(source) = found.into_iter().next() else {
        coded_bail!(
            ErrorCode::UnknownSourceName,
            "logical source '{name}' not found"
        );
    };
    Ok(ffi::LogicalSource {
        name: source.name,
        schema_json: serde_json::to_string(&source.schema)?,
        ..Default::default()
    })
}

pub(crate) fn get_source_descriptors(
    ctx: &PlanningTransaction,
    logical_source_name: &str,
) -> ffi::SourceDescriptors {
    answered(source_descriptors(ctx, logical_source_name))
}

fn source_descriptors(
    ctx: &PlanningTransaction,
    logical_source_name: &str,
) -> anyhow::Result<ffi::SourceDescriptors> {
    let items = ctx
        .execute_blocking(
            GetPhysicalSource::all().with_logical_source(logical_source_name.to_string()),
        )?
        .into_iter()
        .map(|s| {
            Ok(ffi::SourceDescriptor {
                id: *s.id,
                host_addr: s.host_addr.to_string(),
                source_type: s.source_type.to_string(),
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
    answered(sink_descriptor(ctx, name))
}

fn sink_descriptor(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::SinkDescriptor> {
    let found = ctx.execute_blocking(GetSink::all().with_name(name.to_string()))?;
    let Some(sink) = found.into_iter().next() else {
        coded_bail!(ErrorCode::UnknownSinkName, "sink '{name}' not found");
    };
    Ok(ffi::SinkDescriptor {
        id: *sink.id,
        name: sink.name.unwrap_or_default(),
        host_addr: sink.host_addr.to_string(),
        sink_type: sink.sink_type.to_string(),
        schema_json: serde_json::to_string(&sink.schema)?,
        config_json: serde_json::to_string(&sink.config)?,
        ..Default::default()
    })
}

pub(crate) fn get_ml_model(ctx: &PlanningTransaction, name: &str) -> ffi::MlModel {
    answered(ml_model(ctx, name))
}

fn ml_model(ctx: &PlanningTransaction, name: &str) -> anyhow::Result<ffi::MlModel> {
    let found = ctx.execute_blocking(GetMlModel::all().with_name(name.to_string()))?;
    let Some(model) = found.into_iter().next() else {
        coded_bail!(ErrorCode::UnknownModelName, "ml model '{name}' not found");
    };
    Ok(ffi::MlModel {
        name: model.name,
        path: model.path,
        input_schema_json: serde_json::to_string(&model.input_schema)?,
        output_schema_json: serde_json::to_string(&model.output_schema)?,
        imported_json: serde_json::to_string(&model.imported)?,
        ..Default::default()
    })
}

pub(crate) fn get_worker(ctx: &PlanningTransaction, host_addr: &str) -> ffi::Worker {
    answered(worker(ctx, host_addr))
}

fn worker(ctx: &PlanningTransaction, host_addr: &str) -> anyhow::Result<ffi::Worker> {
    let addr = host_addr
        .parse::<NetworkAddr>()
        .map_err(|err| CodedError::new(ErrorCode::InvalidTopology, err))?;
    let found = ctx.execute_blocking(GetWorker::all().with_host_addr(addr))?;
    let Some(worker) = found.into_iter().next() else {
        coded_bail!(ErrorCode::UnknownWorker, "worker '{host_addr}' not found");
    };
    Ok(ffi::Worker {
        host_addr: worker.host_addr.to_string(),
        data_addr: worker.data_addr.to_string(),
        max_operators: worker.max_operators.unwrap_or(-1),
        ..Default::default()
    })
}

pub(crate) fn get_topology(ctx: &PlanningTransaction) -> ffi::Topology {
    answered(topology(ctx))
}

fn topology(ctx: &PlanningTransaction) -> anyhow::Result<ffi::Topology> {
    let workers = ctx.execute_blocking(GetWorker::all())?;
    let links = ctx
        .block_on(NetworkLinkEntity::find().all(ctx.txn()))
        .map_err(|err| catalog_unavailable(err.into()))?;
    Ok(ffi::Topology {
        nodes: workers
            .into_iter()
            .map(|w| w.host_addr.to_string())
            .collect(),
        links: links
            .into_iter()
            .map(|l| ffi::NetworkLink {
                src_addr: l.source_host_addr.to_string(),
                dst_addr: l.target_host_addr.to_string(),
            })
            .collect(),
        ..Default::default()
    })
}

pub(crate) fn create_anonymous_source(
    ctx: &PlanningTransaction,
    internal: bool,
    source_type: &str,
    source_config_json: &str,
    parser_config_json: &str,
    host_addr: &str,
) -> ffi::CreatedId {
    answered((|| {
        let source = ctx.execute_blocking(CreateAnonymousSource {
            source_type: source_type.to_string(),
            source_config: serde_json::from_str(source_config_json)?,
            parser_config: serde_json::from_str(parser_config_json)?,
            host_addr: host_addr.parse().map_err(|e: String| anyhow!("{e}"))?,
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
    answered((|| {
        let sink = ctx.execute_blocking(CreateAnonymousSink {
            sink_type: sink_type.to_string(),
            schema: serde_json::from_str(schema_json)?,
            config: serde_json::from_str(config_json)?,
            host_addr: host_addr.parse().map_err(|e: String| anyhow!("{e}"))?,
            internal,
        })?;
        Ok(ffi::CreatedId {
            id: *sink.id,
            ..Default::default()
        })
    })())
}
