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
mod planner_bridge;
mod worker_bridge;

use anyhow::{Result, anyhow};
use controller::embedded::WorkerFactory;
use model::database::StateBackend;
use model::ml_model::GetMlModel;
use model::request::{Payload, Request};
use model::sink::GetSink;
use model::source::logical::GetLogicalSource;
use model::source::physical::GetPhysicalSource;
use model::statement::StatementResult;
use model::worker::GetWorker;
use model::worker::endpoint::NetworkAddr;
use planner_bridge::FfiSqlPlanner;
use sea_orm::{DatabaseTransaction, EntityTrait};
use std::sync::Arc;
use std::time::Duration;
use worker_bridge::BridgeWorkerFactory;

#[cxx::bridge]
pub mod ffi {
    enum WorkerMode {
        Embedded,
        Remote,
    }

    struct LogicalSource {
        name: String,
        schema_json: String,
    }

    struct SourceDescriptor {
        id: i64,
        logical_source_name: String,
        host_addr: String,
        source_type: String,
        source_config_json: String,
        parser_config_json: String,
    }

    struct SinkDescriptor {
        id: i64,
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
    }

    struct MlModel {
        name: String,
        path: String,
        input_schema_json: String,
        output_schema_json: String,
        imported_json: String,
    }

    struct Worker {
        host_addr: String,
        data_addr: String,
        max_operators: i32,
    }

    struct NetworkLink {
        src_addr: String,
        dst_addr: String,
    }

    struct Topology {
        nodes: Vec<String>,
        links: Vec<NetworkLink>,
    }

    #[namespace = "NES"]
    struct PlannedFragment {
        host_addr: String,
        plan: Vec<u8>,
        num_operators: i32,
        has_source: bool,
    }

    #[namespace = "NES"]
    struct PlannedStatement {
        json: String,
        fragments: Vec<PlannedFragment>,
        source_ids: Vec<i64>,
        sink_ids: Vec<i64>,
    }

    #[namespace = "NES"]
    unsafe extern "C++" {
        include!("SqlPlannerBridge.hpp");

        fn plan_sql(
            ctx: &PlannerContext,
            sql: &str,
            optimizer_config: &str,
        ) -> Result<PlannedStatement>;
    }

    #[namespace = "NES"]
    struct BridgeResult {
        code: u16,
        msg: String,
        trace: String,
    }

    #[namespace = "NES"]
    struct BridgeQueryStatus {
        result: BridgeResult,
        state: i32,
        start_ms: u64,
        stop_ms: u64,
        query_error: BridgeResult,
    }

    #[namespace = "NES"]
    unsafe extern "C++" {
        include!("WorkerBridge.hpp");

        type WorkerBridge;
        fn start_worker(config_json: &str) -> Result<UniquePtr<WorkerBridge>>;
        fn register_query(
            bridge: Pin<&mut WorkerBridge>,
            serialized_fragment: &[u8],
        ) -> BridgeResult;
        fn start_query(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeResult;
        fn stop_query(bridge: Pin<&mut WorkerBridge>, id: i64, mode: u8) -> BridgeResult;
        fn query_status(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeQueryStatus;
        fn call_enable_memcom();
    }

    #[namespace = "NES"]
    extern "Rust" {
        type PlannerContext;
        fn get_logical_source(ctx: &PlannerContext, name: &str) -> Result<LogicalSource>;
        fn get_source_descriptors(
            ctx: &PlannerContext,
            logical_source_name: &str,
        ) -> Result<Vec<SourceDescriptor>>;
        fn get_sink_descriptor(ctx: &PlannerContext, name: &str) -> Result<SinkDescriptor>;
        fn get_worker(ctx: &PlannerContext, host_addr: &str) -> Result<Worker>;
        fn get_topology(ctx: &PlannerContext) -> Result<Topology>;
        fn get_ml_model(ctx: &PlannerContext, name: &str) -> Result<MlModel>;
        fn create_inline_source(
            ctx: &PlannerContext,
            internal: bool,
            source_type: &str,
            schema_json: &str,
            source_config_json: &str,
            parser_config_json: &str,
            host_addr: &str,
        ) -> Result<i64>;
        fn create_inline_sink(
            ctx: &PlannerContext,
            internal: bool,
            sink_type: &str,
            schema_json: &str,
            config_json: &str,
            host_addr: &str,
        ) -> Result<i64>;
    }
}

pub struct PlannerContext {
    txn: DatabaseTransaction,
    handle: tokio::runtime::Handle,
}

impl PlannerContext {
    pub(crate) fn new(txn: DatabaseTransaction, handle: tokio::runtime::Handle) -> Self {
        Self { txn, handle }
    }

    pub(crate) fn into_txn(self) -> DatabaseTransaction {
        self.txn
    }

    fn execute_blocking<E: model::Execute>(&self, req: E) -> Result<E::Response> {
        self.handle.block_on(req.execute(&self.txn))
    }
}

fn get_logical_source(ctx: &PlannerContext, name: &str) -> Result<ffi::LogicalSource> {
    let logical_source = ctx
        .execute_blocking(GetLogicalSource::all().with_name(name.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("logical source '{name}' not found"))?;
    Ok(ffi::LogicalSource {
        name: logical_source.name,
        schema_json: serde_json::to_string(&logical_source.schema)?,
    })
}

fn get_source_descriptors(
    ctx: &PlannerContext,
    logical_source_name: &str,
) -> Result<Vec<ffi::SourceDescriptor>> {
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
        })
    })
    .collect()
}

fn get_sink_descriptor(ctx: &PlannerContext, name: &str) -> Result<ffi::SinkDescriptor> {
    let sinks = ctx.execute_blocking(GetSink::all().with_name(name.to_string()))?;
    let sink = sinks
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("sink '{name}' not found"))?;
    Ok(ffi::SinkDescriptor {
        id: *sink.id,
        name: sink.name.unwrap_or_default(),
        host_addr: sink.host_addr.to_string(),
        sink_type: sink.sink_type.to_string(),
        schema_json: serde_json::to_string(&sink.schema)?,
        config_json: serde_json::to_string(&sink.config)?,
    })
}

fn get_ml_model(ctx: &PlannerContext, name: &str) -> Result<ffi::MlModel> {
    let model = ctx
        .execute_blocking(GetMlModel::all().with_name(name.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("ml model '{name}' not found"))?;
    Ok(ffi::MlModel {
        name: model.name,
        path: model.path,
        input_schema_json: serde_json::to_string(&model.input_schema)?,
        output_schema_json: serde_json::to_string(&model.output_schema)?,
        imported_json: serde_json::to_string(&model.imported)?,
    })
}

fn get_worker(ctx: &PlannerContext, host_addr: &str) -> Result<ffi::Worker> {
    let addr: NetworkAddr = host_addr.parse().map_err(|e: String| anyhow!("{e}"))?;
    let workers = ctx.execute_blocking(GetWorker::all().with_host_addr(addr))?;
    let worker = workers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("worker '{host_addr}' not found"))?;
    Ok(ffi::Worker {
        host_addr: worker.host_addr.to_string(),
        data_addr: worker.data_addr.to_string(),
        max_operators: worker.max_operators.unwrap_or(-1),
    })
}

fn get_topology(ctx: &PlannerContext) -> Result<ffi::Topology> {
    let workers = ctx.execute_blocking(GetWorker::all())?;
    let links = ctx
        .handle
        .block_on(model::worker::network_link::Entity::find().all(&ctx.txn))?;
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

fn create_inline_source(
    ctx: &PlannerContext,
    internal: bool,
    source_type: &str,
    _schema_json: &str,
    source_config_json: &str,
    parser_config_json: &str,
    host_addr: &str,
) -> Result<i64> {
    use model::source::physical::CreateInlineSource;
    let source = ctx.execute_blocking(CreateInlineSource {
        source_type: serde_json::from_value(serde_json::Value::String(source_type.to_string()))
            .map_err(|_| anyhow!("unknown source type '{source_type}'"))?,
        source_config: serde_json::from_str(source_config_json)?,
        parser_config: serde_json::from_str(parser_config_json)?,
        host_addr: serde_json::from_str(host_addr)?,
        internal,
    })?;
    Ok(*source.id)
}

fn create_inline_sink(
    ctx: &PlannerContext,
    internal: bool,
    sink_type: &str,
    schema_json: &str,
    config_json: &str,
    host_addr: &str,
) -> Result<i64> {
    use model::sink::CreateInlineSink;
    let sink = ctx.execute_blocking(CreateInlineSink {
        sink_type: serde_json::from_value(serde_json::Value::String(sink_type.to_string()))
            .map_err(|_| anyhow!("unknown sink type '{sink_type}'"))?,
        schema: serde_json::from_str(schema_json)?,
        config: serde_json::from_str(config_json)?,
        host_addr: serde_json::from_str(host_addr)?,
        internal,
    })?;
    Ok(*sink.id)
}

pub struct CoordinatorHandle {
    pub sender: flume::Sender<Request>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl CoordinatorHandle {
    pub fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("runtime present until drop")
            .handle()
            .block_on(fut)
    }

    pub fn send(&self, input: Payload) -> Result<StatementResult> {
        let (rx, req) = Request::from(input);
        self.sender
            .send(req)
            .map_err(|_| anyhow!("coordinator shut down"))?;
        self.block_on(rx)?
    }
}

impl Drop for CoordinatorHandle {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

pub fn start_coordinator(
    db_path: &str,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> CoordinatorHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("failed to create runtime");

    let planner: Arc<dyn coordinator::SqlPlanner> = Arc::new(FfiSqlPlanner {
        rt_handle: runtime.handle().clone(),
        optimizer_config: optimizer_config.to_string(),
    });

    let factory: Option<Arc<dyn WorkerFactory>> = match mode {
        ffi::WorkerMode::Embedded => {
            ffi::call_enable_memcom();
            Some(Arc::new(BridgeWorkerFactory))
        }
        _ => None,
    };

    let sender = coordinator::start_with_runtime(
        &runtime,
        Some(StateBackend::sqlite(db_path)),
        Some(planner),
        factory,
        None,
    );
    CoordinatorHandle {
        sender,
        runtime: Some(runtime),
    }
}
