use crate::ffi;
use anyhow::anyhow;
use controller::embedded::{EmbeddedWorker, EmbeddedWorkerFactory};
use controller::fragment::FragmentStatus;
use model::query::fragment::{FragmentError, StopMode};
use std::sync::{Arc, Mutex};

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

impl EmbeddedWorker for EmbeddedWorkerRef {
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

    fn get_fragment_status(&self, id: i64) -> Result<FragmentStatus, FragmentError> {
        let mut bridge = self.bridge.lock().unwrap();
        let status = ffi::query_status(bridge.pin_mut(), id);
        check(status.result)?;
        Ok(FragmentStatus {
            state: status.state.into(),
            start_timestamp: (status.start_ms != 0).then_some(status.start_ms),
            stop_timestamp: (status.stop_ms != 0).then_some(status.stop_ms),
            error: to_error(status.query_error),
        })
    }
}
