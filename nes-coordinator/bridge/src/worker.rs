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

use controller::worker_client::in_process::{EmbeddedWorkerLauncher, InProcessWorker};
use model::query::query_fragment::QueryFragmentError;
use std::sync::Arc;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        include!("WorkerBridge.hpp");

        type BridgeError = crate::error::ffi::BridgeError;

        type WorkerBridge;
        fn start_worker(config_json: &str) -> Result<UniquePtr<WorkerBridge>>;
        fn validate_worker_config(config_json: &str) -> BridgeError;
        fn start_query_fragment(bridge: &WorkerBridge, serialized_fragment: &[u8]) -> BridgeError;
        fn stop_query_fragment(bridge: &WorkerBridge, id: i64) -> BridgeError;
        fn get_worker_status(bridge: &WorkerBridge, after_ms: u64) -> Result<Vec<u8>>;
        fn worker_version() -> String;
    }
}

pub(crate) fn validate_worker_config(config_json: &str) -> anyhow::Result<()> {
    match ffi::validate_worker_config(config_json).into_error() {
        Some(failure) => Err(failure.into()),
        None => Ok(()),
    }
}

/// Launches in-process C++ workers for the embedded deployment.
pub(crate) struct BridgeWorkerLauncher;

impl EmbeddedWorkerLauncher for BridgeWorkerLauncher {
    fn launch(&self, config_json: &str) -> anyhow::Result<Arc<dyn InProcessWorker>> {
        Ok(Arc::new(BridgeWorker::new(config_json)?))
    }

    fn version(&self) -> String {
        ffi::worker_version()
    }
}

/// An in-process worker, backed by the C++ single-node worker through cxx.
pub struct BridgeWorker {
    bridge: cxx::UniquePtr<ffi::WorkerBridge>,
}

// Safety: the C++ worker takes calls from several threads at once, as it does from its gRPC service.
unsafe impl Send for BridgeWorker {}
unsafe impl Sync for BridgeWorker {}

impl BridgeWorker {
    pub fn new(config_json: &str) -> anyhow::Result<Self> {
        Ok(Self {
            bridge: ffi::start_worker(config_json)?,
        })
    }
}

fn into_result(error: ffi::BridgeError) -> Result<(), QueryFragmentError> {
    error
        .into_error()
        .map_or(Ok(()), |failure| Err(QueryFragmentError::Internal(failure)))
}

impl InProcessWorker for BridgeWorker {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError> {
        into_result(ffi::start_query_fragment(&self.bridge, &plan))
    }

    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError> {
        into_result(ffi::stop_query_fragment(&self.bridge, id))
    }

    fn get_worker_status(&self, after_ms: u64) -> anyhow::Result<Vec<u8>> {
        Ok(ffi::get_worker_status(&self.bridge, after_ms)?)
    }
}
