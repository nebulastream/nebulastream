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

use controller::in_process::{InProcessWorker, WorkerFactory};
use controller::query_fragment::RawStatus;
use model::error::{CodedError, ErrorCode};
use model::query::query_fragment::QueryFragmentError;
use std::sync::{Arc, Mutex};

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    /// The result of one status read.
    /// `error` is set when the read itself failed, and the other fields are then meaningless.
    /// `found` is false when the worker does not know the query.
    /// `query_error` is the failure of the query on the worker, if it failed.
    struct QueryFragmentStatus {
        error: BridgeError,
        found: bool,
        state: i32,
        /// Milliseconds since the epoch, or 0 when not set.
        start_ms: u64,
        /// Milliseconds since the epoch, or 0 when not set.
        stop_ms: u64,
        query_error: BridgeError,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        include!("WorkerBridge.hpp");

        type BridgeError = crate::error::ffi::BridgeError;

        type WorkerBridge;
        fn start_worker(config_json: &str) -> Result<UniquePtr<WorkerBridge>>;
        fn validate_worker_config(config_json: &str) -> BridgeError;
        fn start_query_fragment(
            bridge: Pin<&mut WorkerBridge>,
            serialized_fragment: &[u8],
        ) -> BridgeError;
        fn stop_query_fragment(bridge: Pin<&mut WorkerBridge>, id: i64) -> BridgeError;
        fn get_query_fragment_status(
            bridge: Pin<&mut WorkerBridge>,
            id: i64,
        ) -> QueryFragmentStatus;
        fn worker_version() -> String;
    }
}

pub(crate) fn validate_worker_config(config_json: &str) -> anyhow::Result<()> {
    match ffi::validate_worker_config(config_json).into_error() {
        Some(failure) => Err(failure.into()),
        None => Ok(()),
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

fn into_result(error: ffi::BridgeError) -> Result<(), QueryFragmentError> {
    error
        .into_error()
        .map_or(Ok(()), |failure| Err(QueryFragmentError::Internal(failure)))
}

impl InProcessWorker for BridgeWorker {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        into_result(ffi::start_query_fragment(bridge.pin_mut(), &plan))
    }

    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        into_result(ffi::stop_query_fragment(bridge.pin_mut(), id))
    }

    fn get_query_fragment_status(&self, id: i64) -> Result<RawStatus, QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        let status = ffi::get_query_fragment_status(bridge.pin_mut(), id);
        into_result(status.error)?;
        if !status.found {
            return Err(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::QueryNotFound,
                format!("query {id} is not known to the worker"),
            )));
        }
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
