use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use controller::worker::embedded::{EmbeddedWorker, EmbeddedWorkerError, EmbeddedWorkerFactory};
use model::query::fragment::FragmentState;
use model::query::StopMode;
use crate::ffi;

pub(crate) struct BridgeWorkerFactory;

impl EmbeddedWorkerFactory for BridgeWorkerFactory {
    fn create(&self, config_json: &str) -> Arc<dyn EmbeddedWorker> {
        Arc::new(EmbeddedWorkerRef::new(config_json).expect("failed to start embedded worker"))
    }
}


/// In-process worker backed by a C++ `SingleNodeWorker` via cxx FFI.
pub struct EmbeddedWorkerRef {
    bridge: Mutex<cxx::UniquePtr<ffi::WorkerBridge>>,
}

// Safety: SingleNodeWorker is internally synchronized (used from gRPC thread pool).
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

fn cxx_err(e: cxx::Exception) -> EmbeddedWorkerError {
    EmbeddedWorkerError {
        code: 0,
        msg: e.to_string(),
        trace: String::new(),
    }
}

impl EmbeddedWorker for EmbeddedWorkerRef {
    fn register_fragment(&self, plan: Vec<u8>) -> anyhow::Result<(), EmbeddedWorkerError> {
        let mut bridge = self.bridge.lock().unwrap();
        ffi::register_query(bridge.pin_mut(), &plan).map_err(cxx_err)
    }

    fn start_fragment(&self, id: i64) -> anyhow::Result<(), EmbeddedWorkerError> {
        let mut bridge = self.bridge.lock().unwrap();
        ffi::start_query(bridge.pin_mut(), id).map_err(cxx_err)
    }

    fn stop_fragment(&self, id: i64, mode: StopMode) -> anyhow::Result<(), EmbeddedWorkerError> {
        let mut bridge = self.bridge.lock().unwrap();
        ffi::stop_query(bridge.pin_mut(), id, mode as u8).map_err(cxx_err)
    }

    fn get_fragment_status(&self, id: i64) -> anyhow::Result<FragmentState, EmbeddedWorkerError> {
        let mut bridge = self.bridge.lock().unwrap();
        let state = ffi::query_status(bridge.pin_mut(), id).map_err(cxx_err)?;
        Ok(state.into())
    }
}
