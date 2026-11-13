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

//! Reconciliation hierarchy: one task per catalog row whose observed state has to move toward its desired state.
//! The controller runs one task per active worker,
//! and each worker task runs one task per fragment that is assigned to its worker.
//! A worker task is aborted when its row is marked for removal.
//! The catalog is the only authority, so a restart resumes from the state that the previous process left behind.
//!
//! The loop is level-triggered: a periodic tick re-reads the catalog regardless of notifications.
//! Watch channels coalesce change signals into wake-ups, which lower latency.
//! A missed wake-up only delays the reaction until the next poll.

use crate::config::POLL_INTERVAL;
use crate::in_process::{InProcessBackend, WorkerFactory};
use crate::remote::RemoteBackend;
use crate::util::buggify::buggify;
use crate::util::reconcile::{Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use crate::worker_task::{WorkerTask, stop_removed_fragments};
use model::database::Database;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use model::worker::{DesiredWorkerState, WorkerTransition};
use sea_orm::ActiveModelTrait;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, info, info_span, warn};

mod config;
pub mod in_process;
pub mod query_fragment;
pub mod remote;
mod version;
mod worker_task;

pub use version::worker_versions;
pub(crate) mod util;

/// Runs each worker in this process when a worker factory is given, and over gRPC otherwise.
pub struct Controller {
    db: Database,
    /// Signals that the catalog may have changed.
    intent_rx: watch::Receiver<()>,
    /// Signals that observed state advanced, so a watcher need not wait for its next poll.
    state_tx: watch::Sender<()>,
    workers: TaskMap<NetworkAddr>,
    factory: Option<Arc<dyn WorkerFactory>>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

impl Controller {
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
            workers: TaskMap::new(),
            factory,
        }
    }

    pub async fn run(mut self) {
        info!("starting");
        let mut intent_rx = self.intent_rx.clone();
        reconcile_loop(&mut self, &mut intent_rx, POLL_INTERVAL, None).await;
    }

    fn spawn_task(&mut self, worker_model: worker::Model) {
        let addr = worker_model.host_addr.clone();
        let intent_rx = self.intent_rx.clone();
        let state_tx = self.state_tx.clone();
        let db = self.db.clone();
        // The reconcile pass calls this every tick for every active worker,
        // so the task is built only when the address is not tracked yet.
        if let Some(factory) = self.factory.clone() {
            let span = info_span!("embedded_worker", addr = %addr);
            self.workers.spawn_if_untracked(addr, move || {
                let backend = InProcessBackend::new(factory, worker_model.clone());
                WorkerTask::new(worker_model, backend, db, intent_rx, state_tx)
                    .run()
                    .instrument(span)
            });
        } else {
            let span = info_span!("remote_worker", addr = %addr);
            self.workers.spawn_if_untracked(addr, move || {
                WorkerTask::new(worker_model, RemoteBackend, db, intent_rx, state_tx)
                    .run()
                    .instrument(span)
            });
        }
    }
}

impl Reconciler for Controller {
    type Key = NetworkAddr;

    fn tasks(&mut self) -> &mut TaskMap<NetworkAddr> {
        &mut self.workers
    }

    async fn reconcile(&mut self) {
        let workers = match worker::Entity::actionable(&self.db).await {
            Ok(workers) => workers,
            Err(e) => {
                warn!(error = ?e, "failed to fetch workers");
                return;
            }
        };

        for worker in workers {
            match worker.desired_state {
                DesiredWorkerState::Active => {
                    self.spawn_task(worker);
                }
                DesiredWorkerState::Removed => {
                    info!(addr = %worker.host_addr, "aborting task");
                    self.workers.abort(&worker.host_addr);
                    if self.factory.is_none() {
                        let (db, addr) = (self.db.clone(), worker.host_addr.clone());
                        tokio::spawn(async move {
                            stop_removed_fragments(&RemoteBackend, &db, &addr).await;
                        });
                    }
                    if buggify() {
                        continue;
                    }
                    let mut update: worker::ActiveModel = worker.into();
                    update.apply_transition(WorkerTransition::Removed);
                    if let Err(e) = update.save(&self.db).await {
                        // The worker stays actionable, so the next tick retries it.
                        warn!(error = %e, "failed to update worker");
                    }
                }
            }
        }
    }
}
