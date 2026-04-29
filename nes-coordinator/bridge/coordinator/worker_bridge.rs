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
use controller::fragment::Status;
use model::query::fragment::{FragmentError, StopMode};
use std::sync::{Arc, Mutex};

pub(crate) struct BridgeWorkerFactory;

impl WorkerFactory for BridgeWorkerFactory {
    fn create(&self, config_json: &str) -> Arc<dyn Worker> {
        Arc::new(EmbeddedWorkerRef::new(config_json).expect("failed to start embedded worker"))
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

fn check(result: ffi::BridgeResult) -> Result<(), FragmentError> {
    if result.code == 0 {
        Ok(())
    } else {
        Err(FragmentError::Internal {
            code: result.code,
            msg: result.msg,
            trace: result.trace,
        })
    }
}

fn to_error(r: ffi::BridgeResult) -> Option<FragmentError> {
    (r.code != 0).then(|| FragmentError::Internal {
        code: r.code,
        msg: r.msg,
        trace: r.trace,
    })
}

impl Worker for EmbeddedWorkerRef {
    fn register_fragment(&self, plan: Vec<u8>) -> Result<(), FragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::register_query(bridge.pin_mut(), &plan))
    }

    fn start_fragment(&self, id: i64) -> Result<(), FragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::start_query(bridge.pin_mut(), id))
    }

    fn stop_fragment(&self, id: i64, mode: StopMode) -> Result<(), FragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        check(ffi::stop_query(bridge.pin_mut(), id, mode as u8))
    }

    fn get_fragment_status(&self, id: i64) -> Result<Status, FragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        let status = ffi::query_status(bridge.pin_mut(), id);
        check(status.result)?;
        Ok(Status {
            state: status.state.into(),
            start_timestamp: (status.start_ms != 0).then_some(status.start_ms),
            stop_timestamp: (status.stop_ms != 0).then_some(status.stop_ms),
            error: to_error(status.query_error),
        })
    }
}
