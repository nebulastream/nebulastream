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

//! Starting a coordinator and keeping it open.
//! A handle owns the runtime the coordinator runs on and the channel its requests go through, so dropping the handle
//! shuts the coordinator down.
//! Both the C++ frontend and a Rust caller start one this way, and they differ only in where the catalog is kept.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use controller::embedded::WorkerFactory;
use coordinator::{SqlPlanner, start_with_runtime};
use model::database::StateBackend;
use model::request::{Payload, Request};
use model::statement::StatementResult;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;

use crate::ffi;
use crate::planner_bridge::FfiSqlPlanner;
use crate::worker_bridge::BridgeWorkerFactory;

pub struct CoordinatorHandle {
    pub sender: async_channel::Sender<Request>,
    runtime: Option<Runtime>,
}

impl CoordinatorHandle {
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("runtime present until drop")
            .handle()
            .block_on(fut)
    }

    pub fn send(&self, input: Payload) -> Result<StatementResult> {
        let (rx, req) = Request::from(input);
        self.sender
            .send_blocking(req)
            .map_err(|_| anyhow!("coordinator shut down"))?;
        self.block_on(rx)?
    }

    /// Like `send`, but gives up as soon as `cancel` is set, answering `None`.
    /// The reply is abandoned rather than withdrawn: the coordinator still holds the parked request and answers it later
    /// into a receiver nobody reads.
    pub fn send_until(
        &self,
        input: Payload,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<Option<StatementResult>> {
        if *cancel.borrow_and_update() {
            return Ok(None);
        }
        let (rx, req) = Request::from(input);
        self.sender
            .send_blocking(req)
            .map_err(|_| anyhow!("coordinator shut down"))?;
        self.block_on(async {
            tokio::select! {
                reply = rx => reply.map_err(anyhow::Error::from).and_then(|r| r).map(Some),
                _ = cancel.changed() => Ok(None),
            }
        })
    }
}

impl Drop for CoordinatorHandle {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

/// The address a statement that omits its HOST clause is placed on, or empty when the deployment requires an explicit one.
/// Embedded deployments run a single in-process worker, so defaulting to that worker is unambiguous.
/// Remote deployments have several, so there is nothing to default to.
pub(crate) fn default_host_for(mode: ffi::WorkerMode) -> &'static str {
    match mode {
        ffi::WorkerMode::Embedded => "localhost:8080",
        _ => "",
    }
}

pub(crate) fn build_coordinator(
    state_backend: StateBackend,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> Result<CoordinatorHandle> {
    let runtime = Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .context("failed to create coordinator runtime")?;

    let planner: Arc<dyn SqlPlanner> = Arc::new(FfiSqlPlanner {
        rt_handle: runtime.handle().clone(),
        optimizer_config: optimizer_config.to_string(),
        default_host: default_host_for(mode).to_string(),
    });

    let factory: Option<Arc<dyn WorkerFactory>> = match mode {
        ffi::WorkerMode::Embedded => {
            ffi::call_enable_memcom();
            Some(Arc::new(BridgeWorkerFactory))
        }
        _ => None,
    };

    let sender = start_with_runtime(&runtime, Some(state_backend), Some(planner), factory, None)?;
    Ok(CoordinatorHandle {
        sender,
        runtime: Some(runtime),
    })
}

pub fn start_coordinator(
    db_path: &str,
    mode: ffi::WorkerMode,
    optimizer_config: &str,
) -> Result<CoordinatorHandle> {
    build_coordinator(StateBackend::sqlite(db_path), mode, optimizer_config)
}
