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

use controller::fragment::RawStatus;
use controller::in_process::{InProcessWorker, WorkerFactory};
use model::query::query_fragment::QueryFragmentError;
use std::sync::{Arc, Mutex};

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    /// One status read's answer.
    /// The read's own failure is a field rather than an exception,
    /// because a cxx exception keeps only a message and the caller acts on the code.
    /// The remaining fields are only meaningful when that field reports no failure.
    /// The query's own failure on the worker is reported as data, separately from the read's.
    struct BridgeQueryStatus {
        error: BridgeError,
        state: i32,
        start_ms: u64,
        stop_ms: u64,
        query_error: BridgeError,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        include!("WorkerBridge.hpp");

        type BridgeError = crate::error::ffi::BridgeError;

        type WorkerBridge;
        fn start_worker(config_json: &str) -> Result<UniquePtr<WorkerBridge>>;
        fn start_query(bridge: Pin<&mut WorkerBridge>, serialized_fragment: &[u8]) -> BridgeError;
        fn stop_query(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeError;
        fn query_status(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeQueryStatus;
        fn worker_version() -> String;
    }
}

/// Starts in-process C++ workers for the embedded deployment.
pub(crate) struct BridgeWorkerFactory;

impl WorkerFactory for BridgeWorkerFactory {
    fn create(&self, cfg: &str) -> anyhow::Result<Arc<dyn InProcessWorker>> {
        Ok(Arc::new(BridgeWorker::new(cfg)?))
    }

    fn version(&self) -> String {
        ffi::worker_version()
    }
}

/// An in-process worker, backed by the C++ single-node worker through cxx.
pub struct BridgeWorker {
    bridge: Mutex<cxx::UniquePtr<ffi::WorkerBridge>>,
}

// Safety: every call into the worker takes the pointer mutably and goes through the mutex,
// so the C++ worker is reached by one thread at a time.
unsafe impl Send for BridgeWorker {}
unsafe impl Sync for BridgeWorker {}

impl BridgeWorker {
    pub fn new(config_json: &str) -> anyhow::Result<Self> {
        let bridge = ffi::start_worker(config_json)?;
        Ok(Self {
            bridge: Mutex::new(bridge),
        })
    }
}

fn check(error: ffi::BridgeError) -> Result<(), QueryFragmentError> {
    error
        .into_error()
        .map_or(Ok(()), |failure| Err(QueryFragmentError::Internal(failure)))
}

impl InProcessWorker for BridgeWorker {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::start_query(bridge.pin_mut(), &plan))
    }

    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::stop_query(bridge.pin_mut(), id))
    }

    fn get_query_fragment_status(&self, id: i64) -> Result<RawStatus, QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        let status = ffi::query_status(bridge.pin_mut(), id);
        check(status.error)?;
        Ok(RawStatus {
            state: status.state,
            start_ms: (status.start_ms != 0).then_some(status.start_ms),
            stop_ms: (status.stop_ms != 0).then_some(status.stop_ms),
            error: status
                .query_error
                .into_error()
                .map(QueryFragmentError::Internal),
        })
    }
}
