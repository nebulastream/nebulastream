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

//! Runs the fragments of a worker in the coordinator's own process.

mod client;

use crate::query_fragment::RawStatus;
use crate::worker_task::{ActiveFragments, Backend};
use client::InProcessTransport;
use model::query::query_fragment::QueryFragmentError;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;

/// A worker in the coordinator's own process.
/// Its calls block, so the transport runs them off the async runtime.
pub trait InProcessWorker: Send + Sync + 'static {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError>;
    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError>;
    fn get_query_fragment_status(&self, id: i64) -> Result<RawStatus, QueryFragmentError>;
}

/// Constructs an in-process worker from the worker row's config JSON.
/// Letting the host supply the factory
/// keeps this crate decoupled from any concrete worker implementation.
pub trait WorkerFactory: Send + Sync + 'static {
    fn create(&self, config_json: &str) -> anyhow::Result<Arc<dyn InProcessWorker>>;

    /// The build that a worker from this factory would report, returned without creating one.
    fn version(&self) -> String;
}

pub(crate) struct InProcessBackend {
    factory: Arc<dyn WorkerFactory>,
    worker: worker::Model,
}

impl InProcessBackend {
    pub(crate) fn new(factory: Arc<dyn WorkerFactory>, worker: worker::Model) -> Self {
        Self { factory, worker }
    }
}

impl Backend for InProcessBackend {
    type Transport = InProcessTransport;

    const HEARTBEAT: Option<Duration> = None;

    async fn connect(&self, _: &NetworkAddr) -> anyhow::Result<InProcessTransport> {
        let config = embedded_worker_config(&self.worker)?;
        let factory = self.factory.clone();
        let worker = run_blocking(move || factory.create(&config)).await??;
        Ok(InProcessTransport::new(worker))
    }

    async fn healthy(&self, _: &InProcessTransport) -> bool {
        true
    }

    /// An in-process worker has no call that lists its fragments, so nothing is compared for it.
    async fn active_fragments(
        &self,
        _: &InProcessTransport,
        after: u64,
    ) -> anyhow::Result<ActiveFragments> {
        Ok(ActiveFragments {
            fragments: Vec::new(),
            until: after,
        })
    }
}

/// The worker process option for its data-plane address, which is spelled differently from the catalog column.
const WORKER_DATA_ADDRESS_OPTION: &str = "data_address";

/// The worker's config with its data address added as an option, unless the config already sets one.
/// The catalog keeps the address in a column because the coordinator routes by it and keeps it unique,
/// but a worker reads it only from its options and without it starts no data receiver.
fn embedded_worker_config(worker: &worker::Model) -> anyhow::Result<String> {
    let mut options = match &worker.config {
        Value::Null => Map::new(),
        Value::Object(options) => options.clone(),
        other => anyhow::bail!(
            "the config of worker {} is not an object: {other}",
            worker.host_addr
        ),
    };
    options
        .entry(WORKER_DATA_ADDRESS_OPTION)
        .or_insert_with(|| Value::String(worker.data_addr.to_string()));
    Ok(Value::Object(options).to_string())
}

/// Runs a blocking worker call off the async runtime and raises its panic again in the caller.
#[cfg(not(madsim))]
async fn run_blocking<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> Result<R, JoinError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|join_err| match join_err.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(join_err) => join_err,
        })
}

/// Under madsim no in-process worker runs and blocking tasks are unavailable, so the call runs inline.
#[cfg(madsim)]
async fn run_blocking<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> Result<R, JoinError> {
    Ok(f())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::worker::{DesiredWorkerState, WorkerState};

    fn worker_with(config: Value) -> worker::Model {
        worker::Model {
            host_addr: NetworkAddr::new("worker", 8080).unwrap(),
            data_addr: NetworkAddr::new("worker", 9090).unwrap(),
            max_operators: None,
            config,
            current_state: WorkerState::Pending,
            desired_state: DesiredWorkerState::Active,
        }
    }

    fn option(config: &str) -> String {
        let parsed: Value = serde_json::from_str(config).unwrap();
        parsed[WORKER_DATA_ADDRESS_OPTION]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn a_worker_without_a_config_gets_its_data_address() {
        let config = embedded_worker_config(&worker_with(Value::Null)).unwrap();
        assert_eq!(option(&config), "worker:9090");
    }

    #[test]
    fn an_explicit_data_address_wins() {
        let worker = worker_with(serde_json::json!({ WORKER_DATA_ADDRESS_OPTION: "other:1" }));
        assert_eq!(option(&embedded_worker_config(&worker).unwrap()), "other:1");
    }

    #[test]
    fn a_config_that_is_not_an_object_is_rejected() {
        assert!(embedded_worker_config(&worker_with(serde_json::json!([1]))).is_err());
    }
}
