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
use crate::worker_task::Backend;
use client::InProcessTransport;
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use std::sync::Arc;
use std::time::Duration;

/// A worker in the coordinator's own process.
/// Its calls block; an internal adapter runs them off the async runtime for the lifecycle driver.
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

    /// The build that a worker from this factory would report.
    /// Answered without creating one,
    /// because every in-process worker is the build that this binary was linked with.
    fn version(&self) -> String;
}

pub(crate) struct InProcessBackend {
    worker: Arc<dyn InProcessWorker>,
}

impl InProcessBackend {
    pub(crate) fn new(worker: Arc<dyn InProcessWorker>) -> Self {
        Self { worker }
    }
}

impl Backend for InProcessBackend {
    type Link = ();
    type Transport = InProcessTransport;

    async fn connect(&self, _: &NetworkAddr) -> anyhow::Result<()> {
        Ok(())
    }

    fn transport(&self, (): &(), _: &NetworkAddr) -> InProcessTransport {
        InProcessTransport::new(self.worker.clone())
    }

    fn heartbeat(&self) -> Option<Duration> {
        None
    }

    async fn healthy(&self, (): &()) -> bool {
        true
    }
}
