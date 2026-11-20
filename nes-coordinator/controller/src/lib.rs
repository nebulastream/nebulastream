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

//! Reconciliation: one loop per worker that moves the worker and its fragments toward the state that the catalog wants.
//! The catalog is the only authority, so a restart resumes from the state that the previous process left behind.
//!
//! The loops are level-triggered: a periodic tick re-reads the catalog regardless of notifications.
//! Watch channels coalesce change signals into wake-ups, which lower latency.
//! A missed wake-up only delays the reaction until the next poll.

use crate::config::{EMBEDDED_STATUS_INTERVAL, POLL_INTERVAL, REMOTE_STATUS_INTERVAL};
use crate::worker_client::in_process::{self, EmbeddedWorkerLauncher};
use crate::worker_client::remote;
use crate::worker_loop::WorkerLoop;
use model::database::Database;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{Instrument, info, info_span, warn};

mod buggify;
mod config;
mod fragment;
mod join_set;
mod status;
mod version;
pub mod worker_client;
mod worker_loop;

pub use version::get_worker_versions;

/// Moves every worker in the catalog, and the fragments assigned to it, toward its desired state.
///
/// With a worker launcher, every worker runs in this process, and without one, every worker is reached over gRPC.
pub struct Controller {
    db: Database,
    intent_rx: watch::Receiver<()>,
    state_tx: watch::Sender<()>,
    /// A worker gets a new loop only once its old one finished, so no worker has two loops.
    workers: HashMap<NetworkAddr, JoinHandle<()>>,
    launcher: Option<Arc<dyn EmbeddedWorkerLauncher>>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

impl Controller {
    /// `intent_rx` signals that the catalog may have changed.
    /// `state_tx` is signaled whenever observed state advances, so a watcher need not wait for its next poll.
    #[must_use]
    pub fn new(
        db: Database,
        intent_rx: watch::Receiver<()>,
        state_tx: watch::Sender<()>,
        launcher: Option<Arc<dyn EmbeddedWorkerLauncher>>,
    ) -> Self {
        Self {
            db,
            intent_rx,
            state_tx,
            workers: HashMap::new(),
            launcher,
        }
    }

    /// Returns once the sender of the catalog-change signal is dropped, and the worker loops end with it.
    /// Outside madsim, a panic in a worker loop is raised again here.
    pub async fn run(mut self) {
        info!("starting");
        let mut poll = tokio::time::interval(POLL_INTERVAL);
        loop {
            tokio::select! {
                change = self.intent_rx.changed() => if change.is_err() { return; },
                _ = poll.tick() => {}
            }
            for (addr, task) in self.workers.extract_if(|_, task| task.is_finished()) {
                if let Err(err) = task.await {
                    // A madsim join error has no panic payload.
                    #[cfg(not(madsim))]
                    if err.is_panic() {
                        std::panic::resume_unwind(err.into_panic());
                    }
                    warn!(%addr, error = %err, "worker loop failed");
                }
            }
            let workers = worker::Entity::get_managed(&self.db)
                .await
                .inspect_err(|err| warn!(error = %err, "failed to fetch workers"))
                .unwrap_or_default();
            for worker in workers {
                if !self.workers.contains_key(&worker.host_addr) {
                    let addr = worker.host_addr.clone();
                    let task = self.spawn_loop(worker);
                    self.workers.insert(addr, task);
                }
            }
        }
    }

    fn spawn_loop(&self, worker: worker::Model) -> JoinHandle<()> {
        let addr = worker.host_addr.clone();
        let (db, state_tx) = (self.db.clone(), self.state_tx.clone());
        if let Some(launcher) = self.launcher.clone() {
            let span = info_span!("embedded_worker", addr = %addr);
            let connect = move || in_process::connect(launcher.clone(), worker.clone());
            let task = WorkerLoop::new(addr, connect, EMBEDDED_STATUS_INTERVAL, db, state_tx);
            tokio::spawn(task.run(self.intent_rx.clone()).instrument(span))
        } else {
            let span = info_span!("remote_worker", addr = %addr);
            let connect = move || remote::connect(worker.host_addr.clone());
            let task = WorkerLoop::new(addr, connect, REMOTE_STATUS_INTERVAL, db, state_tx);
            tokio::spawn(task.run(self.intent_rx.clone()).instrument(span))
        }
    }
}
