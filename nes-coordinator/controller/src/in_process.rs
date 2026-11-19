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

//! Reaches a worker that runs in the coordinator's own process.

use crate::calls::{CallError, Transport};
use crate::remote::worker_rpc_service::WorkerStatusResponse;
use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use model::worker;
use prost::Message;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::task::JoinError;

/// A worker in the coordinator's own process.
/// Its calls block, so the transport runs each on a thread of its own, and a long start does not delay a stop or a status request.
pub trait InProcessWorker: Send + Sync + 'static {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError>;
    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError>;
    /// The serialized `WorkerStatusResponse` that the worker's gRPC service would return for `after_ms`.
    fn get_worker_status(&self, after_ms: u64) -> anyhow::Result<Vec<u8>>;
}

/// Launches an in-process worker from the worker row's config JSON.
/// The host supplies the launcher, so this crate does not depend on a concrete worker.
pub trait EmbeddedWorkerLauncher: Send + Sync + 'static {
    fn launch(&self, config_json: &str) -> anyhow::Result<Arc<dyn InProcessWorker>>;

    /// The build that a worker from this launcher would run, returned without launching one.
    fn version(&self) -> String;
}

/// Launches the worker, so a worker loop calls it until it succeeds once.
pub(crate) async fn connect(
    launcher: Arc<dyn EmbeddedWorkerLauncher>,
    worker: worker::Model,
) -> anyhow::Result<InProcessTransport> {
    let config = build_config(&worker)?;
    let worker = run_blocking(move || launcher.launch(&config)).await??;
    Ok(InProcessTransport { worker })
}

/// The worker process option for its data-plane address, which is spelled differently from the catalog column.
const DATA_ADDRESS_OPTION: &str = "data_address";

/// The worker's config with its data address added as an option, unless the config already sets one.
/// The catalog keeps the address in a column because the coordinator routes by it and keeps it unique,
/// but a worker reads it only from its options,
/// and without it the worker starts no data receiver.
fn build_config(worker: &worker::Model) -> anyhow::Result<String> {
    let mut options = match &worker.config {
        Value::Null => Map::new(),
        Value::Object(options) => options.clone(),
        other => anyhow::bail!(
            "the config of worker {} is not an object: {other}",
            worker.host_addr
        ),
    };
    options
        .entry(DATA_ADDRESS_OPTION)
        .or_insert_with(|| Value::String(worker.data_addr.to_string()));
    Ok(Value::Object(options).to_string())
}

#[derive(Clone)]
pub(crate) struct InProcessTransport {
    worker: Arc<dyn InProcessWorker>,
}

impl Transport for InProcessTransport {
    async fn start(&self, plan: SerializableQueryPlan) -> Result<(), CallError> {
        let prepared = plan.encode_to_vec();
        let worker = self.worker.clone();
        call(move || worker.start_query_fragment(prepared)).await
    }

    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        let worker = self.worker.clone();
        let id = *id;
        call(move || worker.stop_query_fragment(id)).await
    }

    async fn get_status(&self, after: u64) -> anyhow::Result<WorkerStatusResponse> {
        let worker = self.worker.clone();
        let bytes = run_blocking(move || worker.get_worker_status(after)).await??;
        Ok(WorkerStatusResponse::decode(bytes.as_slice())?)
    }
}

/// A call to a worker in this process never fails transiently.
fn classify(error: QueryFragmentError) -> CallError {
    match &error {
        QueryFragmentError::Internal(coded) if coded.code == ErrorCode::QueryAlreadyRegistered => {
            CallError::AlreadyExists
        }
        _ => CallError::Permanent(error),
    }
}

async fn call<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
) -> Result<T, CallError> {
    run_blocking(f)
        .await
        .unwrap_or_else(|join_err| {
            Err(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::UnknownException,
                join_err,
            )))
        })
        .map_err(classify)
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
    use model::worker::endpoint::NetworkAddr;
    use model::worker::{DesiredWorkerState, WorkerState};

    fn build_worker(config: Value) -> worker::Model {
        worker::Model {
            host_addr: NetworkAddr::new("worker", 8080).unwrap(),
            data_addr: NetworkAddr::new("worker", 9090).unwrap(),
            max_operators: None,
            config,
            current_state: WorkerState::Pending,
            desired_state: DesiredWorkerState::Active,
        }
    }

    #[test]
    fn data_address_added() {
        let config = build_config(&build_worker(Value::Null)).unwrap();
        assert_eq!(config, r#"{"data_address":"worker:9090"}"#);
    }

    #[test]
    fn explicit_data_address_kept() {
        let worker = build_worker(serde_json::json!({ DATA_ADDRESS_OPTION: "other:1" }));
        let config = build_config(&worker).unwrap();
        assert_eq!(config, r#"{"data_address":"other:1"}"#);
    }

    #[test]
    fn non_object_config_rejected() {
        assert!(build_config(&build_worker(serde_json::json!([1]))).is_err());
    }
}
