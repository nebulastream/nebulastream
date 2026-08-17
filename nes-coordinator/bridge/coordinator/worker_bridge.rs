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

use crate::ffi;
use anyhow::anyhow;
use controller::embedded::{Worker, WorkerFactory};
use controller::fragment::QueryFragmentStatus;
use model::query::query_fragment::{QueryFragmentError, QueryFragmentState};
use std::sync::{Arc, Mutex};

pub(crate) struct BridgeWorkerFactory;

impl WorkerFactory for BridgeWorkerFactory {
    fn create(&self, cfg: &str) -> anyhow::Result<Arc<dyn Worker>> {
        Ok(Arc::new(EmbeddedWorkerRef::new(cfg)?))
    }

    fn version(&self) -> String {
        ffi::worker_version()
    }
}

/// In-process worker backed by a C++ `SingleNodeWorker` via cxx FFI.
pub struct EmbeddedWorkerRef {
    bridge: Mutex<cxx::UniquePtr<ffi::WorkerBridge>>,
}

// Safety: SingleNodeWorker is internally synchronized.
// The Mutex serializes access to the UniquePtr from the Rust side.
unsafe impl Send for EmbeddedWorkerRef {}
unsafe impl Sync for EmbeddedWorkerRef {}

impl EmbeddedWorkerRef {
    pub fn new(config_json: &str) -> anyhow::Result<Self> {
        let bridge = ffi::start_worker(config_json).map_err(|e| anyhow!("{}", e))?;
        Ok(Self {
            bridge: Mutex::new(bridge),
        })
    }
}

impl From<ffi::BridgeError> for Option<QueryFragmentError> {
    fn from(error: ffi::BridgeError) -> Self {
        (error.code != 0).then_some(QueryFragmentError::Internal {
            code: error.code,
            msg: error.msg,
            trace: error.trace,
        })
    }
}

fn check(error: ffi::BridgeError) -> Result<(), QueryFragmentError> {
    Option::from(error).map_or(Ok(()), Err)
}

impl Worker for EmbeddedWorkerRef {
    fn start_query_fragment(&self, plan: Vec<u8>) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::start_query(bridge.pin_mut(), &plan))
    }

    fn stop_query_fragment(&self, id: i64) -> Result<(), QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::stop_query(bridge.pin_mut(), id))
    }

    fn get_query_fragment_status(
        &self,
        id: i64,
    ) -> Result<QueryFragmentStatus, QueryFragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        let status = ffi::query_status(bridge.pin_mut(), id);
        check(status.error)?;
        let state = QueryFragmentState::try_from(status.state).map_err(|unknown| {
            QueryFragmentError::Transport {
                msg: format!("worker reported unknown fragment state: {unknown}"),
            }
        })?;
        Ok(QueryFragmentStatus {
            state,
            start_timestamp: (status.start_ms != 0).then_some(status.start_ms),
            stop_timestamp: (status.stop_ms != 0).then_some(status.stop_ms),
            error: status.query_error.into(),
        })
    }
}
