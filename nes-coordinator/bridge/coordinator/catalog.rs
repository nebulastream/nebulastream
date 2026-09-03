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

//! The catalog reads and writes the C++ optimizer makes while it plans a statement.
//! All of them run on the transaction of the request being planned, so the optimizer sees the writes of its own request
//! and neither the reads nor the writes outlive a rolled back one.
//! The optimizer calls in from a thread with no runtime of its own, so each one blocks on the runtime handle the request
//! was started with.

use anyhow::anyhow;
use model::ConnectorKind;
use model::Execute;
use model::error::ErrorCode;
use model::ml_model::GetMlModel;
use model::sink::{CreateAnonymousSink, GetSink};
use model::source::logical::GetLogicalSource;
use model::source::physical::{CreateAnonymousSource, GetPhysicalSource};
use model::worker::GetWorker;
use model::worker::endpoint::NetworkAddr;
use model::worker::network_link::Entity as NetworkLinkEntity;
use sea_orm::{DatabaseTransaction, EntityTrait};
use tokio::runtime::Handle;

use crate::error::FfiError;
use crate::error::no_error;
use crate::ffi;

/// The transaction of one request, together with the runtime it has to be driven on.
/// Handed to the optimizer for the length of a planning call and taken back afterwards, so the request keeps ownership of it.
pub struct TransactionContext {
    txn: DatabaseTransaction,
    handle: Handle,
}

impl TransactionContext {
    pub(crate) fn new(txn: DatabaseTransaction, handle: Handle) -> Self {
        Self { txn, handle }
    }

    pub(crate) fn into_txn(self) -> DatabaseTransaction {
        self.txn
    }

    /// The transaction to run a statement on, for a caller that needs one this module does not already offer.
    pub(crate) fn txn(&self) -> &DatabaseTransaction {
        &self.txn
    }

    /// Drives a catalog future to completion on the runtime the request was started with.
    pub(crate) fn block_on<F: Future>(&self, work: F) -> F::Output {
        self.handle.block_on(work)
    }

    fn execute_blocking<E: Execute>(&self, req: E) -> anyhow::Result<E::Response> {
        self.handle.block_on(req.execute(&self.txn))
    }
}

/// The failure a lookup reports in its own struct.
/// A lookup answers with a payload rather than an `Err`, because cxx renders a returned error with `Display` and the code would not
/// survive the crossing.
/// The caller raises the exception this code names.
fn failed(code: ErrorCode, msg: String) -> ffi::BridgeError {
    ffi::BridgeError {
        code: code as u16,
        msg,
        trace: String::new(),
    }
}

pub(crate) fn get_logical_source(ctx: &TransactionContext, name: &str) -> ffi::LogicalSource {
    let missing = |code: ErrorCode, msg: String| ffi::LogicalSource {
        name: String::new(),
        schema_json: String::new(),
        error: failed(code, msg),
    };

    let found = match ctx.execute_blocking(GetLogicalSource::all().with_name(name.to_string())) {
        Ok(rows) => rows.into_iter().next(),
        Err(err) => return missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    };
    let Some(logical_source) = found else {
        return missing(
            ErrorCode::UnknownSourceName,
            format!("logical source '{name}' not found"),
        );
    };
    match serde_json::to_string(&logical_source.schema) {
        Ok(schema_json) => ffi::LogicalSource {
            name: logical_source.name,
            schema_json,
            error: no_error(),
        },
        Err(err) => missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    }
}

pub(crate) fn get_source_descriptors(
    ctx: &TransactionContext,
    logical_source_name: &str,
) -> Result<Vec<ffi::SourceDescriptor>, FfiError> {
    ctx.execute_blocking(
        GetPhysicalSource::all().with_logical_source(logical_source_name.to_string()),
    )?
    .into_iter()
    .map(|s| {
        Ok(ffi::SourceDescriptor {
            id: *s.id,
            logical_source_name: s.logical_source.unwrap_or_default(),
            host_addr: s.host_addr.to_string(),
            source_type: s.source_type.to_string(),
            source_config_json: serde_json::to_string(&s.source_config)?,
            parser_config_json: serde_json::to_string(&s.parser_config)?,
            is_anonymous: if s.kind == ConnectorKind::Shared {
                false
            } else {
                true
            },
        })
    })
    .collect()
}

pub(crate) fn get_sink_descriptor(ctx: &TransactionContext, name: &str) -> ffi::SinkDescriptor {
    let missing = |code: ErrorCode, msg: String| ffi::SinkDescriptor {
        id: 0,
        name: String::new(),
        host_addr: String::new(),
        sink_type: String::new(),
        schema_json: String::new(),
        config_json: String::new(),
        error: failed(code, msg),
    };

    let found = match ctx.execute_blocking(GetSink::all().with_name(name.to_string())) {
        Ok(rows) => rows.into_iter().next(),
        Err(err) => return missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    };
    let Some(sink) = found else {
        return missing(
            ErrorCode::UnknownSinkName,
            format!("sink '{name}' not found"),
        );
    };
    match (
        serde_json::to_string(&sink.schema),
        serde_json::to_string(&sink.config),
    ) {
        (Ok(schema_json), Ok(config_json)) => ffi::SinkDescriptor {
            id: *sink.id,
            name: sink.name.unwrap_or_default(),
            host_addr: sink.host_addr.to_string(),
            sink_type: sink.sink_type.to_string(),
            schema_json,
            config_json,
            error: no_error(),
        },
        (Err(err), _) | (_, Err(err)) => missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    }
}

pub(crate) fn get_ml_model(ctx: &TransactionContext, name: &str) -> ffi::MlModel {
    let missing = |code: ErrorCode, msg: String| ffi::MlModel {
        name: String::new(),
        path: String::new(),
        input_schema_json: String::new(),
        output_schema_json: String::new(),
        imported_json: String::new(),
        error: failed(code, msg),
    };

    let found = match ctx.execute_blocking(GetMlModel::all().with_name(name.to_string())) {
        Ok(rows) => rows.into_iter().next(),
        Err(err) => return missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    };
    let Some(model) = found else {
        return missing(
            ErrorCode::UnknownModelName,
            format!("ml model '{name}' not found"),
        );
    };
    match (
        serde_json::to_string(&model.input_schema),
        serde_json::to_string(&model.output_schema),
        serde_json::to_string(&model.imported),
    ) {
        (Ok(input_schema_json), Ok(output_schema_json), Ok(imported_json)) => ffi::MlModel {
            name: model.name,
            path: model.path,
            input_schema_json,
            output_schema_json,
            imported_json,
            error: no_error(),
        },
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
            missing(ErrorCode::CatalogUnavailable, format!("{err:#}"))
        }
    }
}

pub(crate) fn get_worker(ctx: &TransactionContext, host_addr: &str) -> ffi::Worker {
    let missing = |code: ErrorCode, msg: String| ffi::Worker {
        host_addr: String::new(),
        data_addr: String::new(),
        max_operators: 0,
        error: failed(code, msg),
    };

    let addr: NetworkAddr = match host_addr.parse::<NetworkAddr>() {
        Ok(addr) => addr,
        Err(err) => return missing(ErrorCode::InvalidTopology, err),
    };
    let found = match ctx.execute_blocking(GetWorker::all().with_host_addr(addr)) {
        Ok(rows) => rows.into_iter().next(),
        Err(err) => return missing(ErrorCode::CatalogUnavailable, format!("{err:#}")),
    };
    let Some(worker) = found else {
        return missing(
            ErrorCode::UnknownWorker,
            format!("worker '{host_addr}' not found"),
        );
    };
    ffi::Worker {
        host_addr: worker.host_addr.to_string(),
        data_addr: worker.data_addr.to_string(),
        max_operators: worker.max_operators.unwrap_or(-1),
        error: no_error(),
    }
}

pub(crate) fn get_topology(ctx: &TransactionContext) -> Result<ffi::Topology, FfiError> {
    let workers = ctx.execute_blocking(GetWorker::all())?;
    let links = ctx.block_on(NetworkLinkEntity::find().all(ctx.txn()))?;
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
    })
}

pub(crate) fn create_anonymous_source(
    ctx: &TransactionContext,
    internal: bool,
    source_type: &str,
    _schema_json: &str,
    source_config_json: &str,
    parser_config_json: &str,
    host_addr: &str,
) -> Result<i64, FfiError> {
    let source = ctx.execute_blocking(CreateAnonymousSource {
        source_type: source_type.to_string(),
        source_config: serde_json::from_str(source_config_json)?,
        parser_config: serde_json::from_str(parser_config_json)?,
        host_addr: host_addr.parse().map_err(|e: String| anyhow!("{e}"))?,
        internal,
    })?;
    Ok(*source.id)
}

pub(crate) fn create_anonymous_sink(
    ctx: &TransactionContext,
    internal: bool,
    sink_type: &str,
    schema_json: &str,
    config_json: &str,
    host_addr: &str,
) -> Result<i64, FfiError> {
    let sink = ctx.execute_blocking(CreateAnonymousSink {
        sink_type: sink_type.to_string(),
        schema: serde_json::from_str(schema_json)?,
        config: serde_json::from_str(config_json)?,
        host_addr: host_addr.parse().map_err(|e: String| anyhow!("{e}"))?,
        internal,
    })?;
    Ok(*sink.id)
}
