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
use anyhow::anyhow;
use anyhow::Result;
use catalog::Catalog;
use catalog::database::StateBackend;
use model::request::{Request, Statement};
use model::source::logical_source::GetLogicalSource;
use model::source::physical_source::GetPhysicalSource;
use model::sink::GetSink;
use model::worker::GetWorker;
use std::sync::Arc;

#[cxx::bridge]
pub mod ffi {
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
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
    }

    struct Worker {
        host_addr: String,
        data_addr: String,
        capacity: i32,
    }

    struct NetworkLink {
        upstream: String,
        downstream: String,
    }

    struct Topology {
        nodes: Vec<String>,
        links: Vec<NetworkLink>,
    }

    struct PlannedFragment {
        host_addr: String,
        plan: Vec<u8>,
        used_capacity: i32,
        has_source: bool,
    }

    struct PlannedStatement {
        /// JSON-encoded Statement variant (for all statement types).
        json: String,
        /// Non-empty only for query plans — carries the serialized protobuf plans.
        fragments: Vec<PlannedFragment>,
    }

    extern "Rust" {
        type CoordinatorHandle;
        fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle>;
        fn execute_statement(handle: &CoordinatorHandle, sql: &str, format: &str) -> Result<String>;
    }

    #[namespace = "NES"]
    unsafe extern "C++" {
        include!("SqlPlannerBridge.hpp");

        fn plan_sql(ctx: &CatalogRef, sql: &str) -> Result<PlannedStatement>;
    }

    #[namespace = "NES"]
    extern "Rust" {
        type CatalogRef;
        fn get_logical_source(ctx: &CatalogRef, name: &str) -> Result<LogicalSource>;
        fn get_source_descriptors(
            ctx: &CatalogRef,
            logical_source_name: &str,
        ) -> Result<Vec<SourceDescriptor>>;
        fn get_sink_descriptor(ctx: &CatalogRef, name: &str) -> Result<SinkDescriptor>;
        fn get_worker(ctx: &CatalogRef, host_addr: &str) -> Result<Worker>;
        fn get_topology(ctx: &CatalogRef) -> Result<Topology>;
    }
}

pub struct CoordinatorHandle {
    sender: flume::Sender<Request>,
    catalog: Arc<Catalog>,
    runtime: tokio::runtime::Runtime,
}

fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle> {
    let (sender, catalog, runtime) = coordinator::start(Some(StateBackend::sqlite(db_path)), None);
    Box::new(CoordinatorHandle { sender, catalog, runtime })
}

fn execute_statement(handle: &CoordinatorHandle, sql: &str, format: &str) -> Result<String> {
    use model::query::fragment::CreateFragment;

    let catalog_ref = CatalogRef::new(
        handle.catalog.clone(),
        handle.runtime.handle().clone(),
    );
    let planned = ffi::plan_sql(&catalog_ref, sql).map_err(|e| anyhow!("{}", e))?;
    let mut statement: Statement = serde_json::from_str(&planned.json)?;

    // For query plans, attach the fragments from the cxx bridge (protobuf bytes)
    if let Statement::CreateQuery(ref mut q) = statement {
        q.fragments = planned
            .fragments
            .into_iter()
            .map(|f| CreateFragment {
                host_addr: f.host_addr.parse().expect("invalid host_addr from planner"),
                plan: f.plan,
                used_capacity: f.used_capacity,
                has_source: f.has_source,
            })
            .collect();
    }

    // Send structured statement to coordinator
    let (rx, req) = Request::new(statement);
    handle
        .sender
        .send(req)
        .map_err(|_| anyhow!("coordinator channel closed"))?;
    let response = rx
        .blocking_recv()
        .map_err(|_| anyhow!("coordinator dropped the request"))??;
    match format {
        "json" => Ok(serde_json::to_string(&response)?),
        "text" => Ok(response.to_string()),
        _ => Err(anyhow!("unknown format '{}', expected 'json' or 'text'", format)),
    }
}

pub struct CatalogRef {
    catalog: Arc<Catalog>,
    handle: tokio::runtime::Handle,
}

impl CatalogRef {
    pub fn new(catalog: Arc<Catalog>, handle: tokio::runtime::Handle) -> Self {
        Self { catalog, handle }
    }
}

fn get_logical_source(ctx: &CatalogRef, name: &str) -> Result<ffi::LogicalSource> {
    let req = GetLogicalSource::all().with_name(name.to_string());
    let sources = ctx.handle.block_on(ctx.catalog.source.get_logical_source(req))?;
    let source = sources
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("logical source '{}' not found", name))?;
    Ok(ffi::LogicalSource {
        name: source.name,
        schema_json: serde_json::to_string(&source.schema)?,
    })
}

fn get_source_descriptors(
    ctx: &CatalogRef,
    logical_source_name: &str,
) -> Result<Vec<ffi::SourceDescriptor>> {
    let req = GetPhysicalSource::all().with_logical_source(logical_source_name.to_string());
    let sources = ctx
        .handle
        .block_on(ctx.catalog.source.get_physical_source(req))?;
    sources
        .into_iter()
        .map(|s| {
            Ok(ffi::SourceDescriptor {
                id: s.id,
                logical_source_name: s.logical_source,
                host_addr: s.host_addr.to_string(),
                source_type: s.source_type.to_string(),
                source_config_json: serde_json::to_string(&s.source_config)?,
                parser_config_json: serde_json::to_string(&s.parser_config)?,
            })
        })
        .collect()
}

fn get_sink_descriptor(ctx: &CatalogRef, name: &str) -> Result<ffi::SinkDescriptor> {
    let req = GetSink::all().with_name(name.to_string());
    let sinks = ctx.handle.block_on(ctx.catalog.sink.get_sink(req))?;
    let sink = sinks
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("sink '{}' not found", name))?;
    Ok(ffi::SinkDescriptor {
        name: sink.name,
        host_addr: sink.host_addr.to_string(),
        sink_type: sink.sink_type.to_string(),
        schema_json: serde_json::to_string(&sink.schema)?,
        config_json: serde_json::to_string(&sink.config)?,
    })
}

fn get_worker(ctx: &CatalogRef, host_addr: &str) -> Result<ffi::Worker> {
    let addr = host_addr
        .parse()
        .map_err(|e: String| anyhow!("invalid host_addr '{}': {}", host_addr, e))?;
    let req = GetWorker::all().with_host_addr(addr);
    let workers = ctx.handle.block_on(ctx.catalog.worker.get_worker(req))?;
    let worker = workers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("worker '{}' not found", host_addr))?;
    Ok(ffi::Worker {
        host_addr: worker.host_addr.to_string(),
        data_addr: worker.data_addr.to_string(),
        capacity: worker.capacity.unwrap_or(-1),
    })
}

fn get_topology(ctx: &CatalogRef) -> Result<ffi::Topology> {
    let (workers, edges) = ctx.handle.block_on(ctx.catalog.worker.get_topology())?;
    Ok(ffi::Topology {
        nodes: workers.into_iter().map(|w| w.host_addr.to_string()).collect(),
        links: edges
            .into_iter()
            .map(|(upstream, downstream)| ffi::NetworkLink {
                upstream: upstream.to_string(),
                downstream: downstream.to_string(),
            })
            .collect(),
    })
}
