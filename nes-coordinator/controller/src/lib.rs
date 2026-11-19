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

use crate::config::{EMBEDDED_REPORT_INTERVAL, POLL_INTERVAL, REMOTE_REPORT_INTERVAL};
use crate::in_process::WorkerFactory;
use crate::worker_task::WorkerTask;
use model::database::Database;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{Instrument, info, info_span, warn};

mod config;
pub mod in_process;
pub mod remote;
mod report;
mod version;
mod worker_task;

pub use version::worker_versions;
pub(crate) mod util;

/// Moves every worker in the catalog, and the fragments assigned to it, toward its desired state.
/// Runs each worker in this process when a worker factory is given, and over gRPC otherwise.
pub struct Controller {
    db: Database,
    intent_rx: watch::Receiver<()>,
    state_tx: watch::Sender<()>,
    /// A loop ends once its worker is recorded as removed, and a worker that becomes actionable again gets a new one.
    workers: HashMap<NetworkAddr, JoinHandle<()>>,
    factory: Option<Arc<dyn WorkerFactory>>,
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
        factory: Option<Arc<dyn WorkerFactory>>,
    ) -> Self {
        Self {
            db,
            intent_rx,
            state_tx,
            workers: HashMap::new(),
            factory,
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
            self.join_finished().await;
            match worker::Entity::actionable(&self.db).await {
                Ok(workers) => {
                    for worker in workers {
                        if !self.workers.contains_key(&worker.host_addr) {
                            let task = self.spawn_task(worker.clone());
                            self.workers.insert(worker.host_addr, task);
                        }
                    }
                }
                Err(e) => warn!(error = ?e, "failed to fetch workers"),
            }
        }
    }

    async fn join_finished(&mut self) {
        let finished: Vec<NetworkAddr> = self
            .workers
            .iter()
            .filter(|(_, task)| task.is_finished())
            .map(|(addr, _)| addr.clone())
            .collect();
        for addr in finished {
            if let Some(task) = self.workers.remove(&addr)
                && let Err(err) = task.await
            {
                // A madsim join error has no panic payload.
                #[cfg(not(madsim))]
                if err.is_panic() {
                    std::panic::resume_unwind(err.into_panic());
                }
                warn!(%addr, error = %err, "worker task failed");
            }
        }
    }

    fn spawn_task(&self, worker: worker::Model) -> JoinHandle<()> {
        let addr = worker.host_addr.clone();
        let (db, state_tx) = (self.db.clone(), self.state_tx.clone());
        let intent_rx = self.intent_rx.clone();
        if let Some(factory) = self.factory.clone() {
            let span = info_span!("embedded_worker", addr = %addr);
            let connect = move || in_process::connect(factory.clone(), worker.clone());
            let task = WorkerTask::new(addr, connect, EMBEDDED_REPORT_INTERVAL, db, state_tx);
            tokio::spawn(task.run(intent_rx).instrument(span))
        } else {
            let span = info_span!("remote_worker", addr = %addr);
            let target = addr.clone();
            let connect = move || remote::connect(target.clone());
            let task = WorkerTask::new(addr, connect, REMOTE_REPORT_INTERVAL, db, state_tx);
            tokio::spawn(task.run(intent_rx).instrument(span))
        }
    }
}
