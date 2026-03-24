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
    struct FfiLogicalSource {
        name: String,
        schema_json: String,
    }

    struct FfiSourceDescriptor {
        id: i64,
        logical_source_name: String,
        host_addr: String,
        source_type: String,
        source_config_json: String,
        parser_config_json: String,
    }

    struct FfiSinkDescriptor {
        name: String,
        host_addr: String,
        sink_type: String,
        schema_json: String,
        config_json: String,
    }

    struct FfiWorker {
        host_addr: String,
        grpc_addr: String,
        capacity: i32,
    }

    struct FfiTopologyEdge {
        upstream: String,
        downstream: String,
    }

    extern "Rust" {
        type CoordinatorHandle;
        fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle>;
        fn execute_statement(handle: &CoordinatorHandle, sql: &str) -> Result<String>;

        type CatalogRef;
        fn get_logical_source(ctx: &CatalogRef, name: &str) -> Result<FfiLogicalSource>;
        fn get_source_descriptors(
            ctx: &CatalogRef,
            logical_source_name: &str,
        ) -> Result<Vec<FfiSourceDescriptor>>;
        fn get_sink_descriptor(ctx: &CatalogRef, name: &str) -> Result<FfiSinkDescriptor>;
        fn get_worker(ctx: &CatalogRef, host_addr: &str) -> Result<FfiWorker>;
    }
}

pub struct CoordinatorHandle {
    sender: flume::Sender<Request>,
}

fn start_coordinator(db_path: &str) -> Box<CoordinatorHandle> {
    let sender = coordinator::start(Some(StateBackend::sqlite(db_path)), None);
    Box::new(CoordinatorHandle { sender })
}

fn execute_statement(handle: &CoordinatorHandle, sql: &str) -> Result<String> {
    let statement = Statement::Sql(sql.to_string());
    let (rx, req) = Request::new(statement);
    handle
        .sender
        .send(req)
        .map_err(|_| anyhow!("coordinator channel closed"))?;
    let response = rx
        .blocking_recv()
        .map_err(|_| anyhow!("coordinator dropped the request"))??;
    Ok(serde_json::to_string(&response)?)
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

fn get_logical_source(ctx: &CatalogRef, name: &str) -> Result<ffi::FfiLogicalSource> {
    let req = GetLogicalSource::all().with_name(name.to_string());
    let sources = ctx.handle.block_on(ctx.catalog.source.get_logical_source(req))?;
    let source = sources
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("logical source '{}' not found", name))?;
    Ok(ffi::FfiLogicalSource {
        name: source.name,
        schema_json: serde_json::to_string(&source.schema)?,
    })
}

fn get_source_descriptors(
    ctx: &CatalogRef,
    logical_source_name: &str,
) -> Result<Vec<ffi::FfiSourceDescriptor>> {
    let req = GetPhysicalSource::all().with_logical_source(logical_source_name.to_string());
    let sources = ctx
        .handle
        .block_on(ctx.catalog.source.get_physical_source(req))?;
    sources
        .into_iter()
        .map(|s| {
            Ok(ffi::FfiSourceDescriptor {
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

fn get_sink_descriptor(ctx: &CatalogRef, name: &str) -> Result<ffi::FfiSinkDescriptor> {
    let req = GetSink::all().with_name(name.to_string());
    let sinks = ctx.handle.block_on(ctx.catalog.sink.get_sink(req))?;
    let sink = sinks
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("sink '{}' not found", name))?;
    Ok(ffi::FfiSinkDescriptor {
        name: sink.name,
        host_addr: sink.host_addr.to_string(),
        sink_type: sink.sink_type.to_string(),
        schema_json: serde_json::to_string(&sink.schema)?,
        config_json: serde_json::to_string(&sink.config)?,
    })
}

fn get_worker(ctx: &CatalogRef, host_addr: &str) -> Result<ffi::FfiWorker> {
    let addr = host_addr
        .parse()
        .map_err(|e: String| anyhow!("invalid host_addr '{}': {}", host_addr, e))?;
    let req = GetWorker::all().with_host_addr(addr);
    let workers = ctx.handle.block_on(ctx.catalog.worker.get_worker(req))?;
    let worker = workers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("worker '{}' not found", host_addr))?;
    Ok(ffi::FfiWorker {
        host_addr: worker.host_addr.to_string(),
        grpc_addr: worker.grpc_addr.to_string(),
        capacity: worker.capacity.unwrap_or(-1),
    })
}
