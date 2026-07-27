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

//! Reconciliation hierarchy: one task per DB row whose state has to be moved toward its target.
//! The top-level controller spawns one worker task per active worker row,
//! and each worker task spawns one query_fragment task per query_fragment that is assigned to it.
//! A worker task's lifetime mirrors its row:
//! it exists while the row's desired state asks for it,
//! and is aborted when the row is marked for removal.
//! Authority is in the DB, never in task memory,
//! so a restart resumes from exactly the state that the previous process left behind.
//!
//! The loop is level-triggered: a periodic tick re-reads the DB regardless of notifications.
//! Watch channels coalesce intent signals down to "go look again" wake-ups,
//! which makes latency good without making correctness depend on any individual signal arriving.
//! A missed wake-up only delays the next reaction to the next poll interval.

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
mod error;
pub mod in_process;
pub mod query_fragment;
pub mod remote;
mod version;
mod worker_task;

pub use version::worker_versions;
pub(crate) mod util;

/// Top of the reconciliation hierarchy: one task per active worker row.
/// A worker may run in-process, backed by an injected factory, or out-of-process over gRPC.
pub struct Controller {
    db: Database,
    /// Wake-up signal:
    /// a change here means the database likely changed and the worker table should be re-read.
    intent_rx: watch::Receiver<()>,
    /// Outbound signal that this level advanced observed state,
    /// so a watcher can react without waiting for its next poll.
    state_tx: watch::Sender<()>,
    workers: TaskMap<NetworkAddr>,
    factory: Option<Arc<dyn WorkerFactory>>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

/// The option that a worker reads its data-plane address from,
/// declared by `SingleNodeWorkerConfiguration` on the C++ side.
///
/// Deliberately not the `Worker::DataAddr` identifier of the migration crate.
/// That one is the `data_addr` column that the coordinator routes by;
/// this one is an option of the worker process, and the two spellings differ.
/// Passing the column name here leaves the option unset.
const WORKER_DATA_ADDRESS_OPTION: &str = "data_address";

/// The configuration that an embedded worker is built from:
/// its own config with the address that it serves data on added as an option.
///
/// The data address is a column of the worker row rather than one of its options,
/// because the coordinator routes and enforces uniqueness by it.
/// A worker reads it from its options instead, and without it starts no receiver,
/// so anything sent to it reports that the receiver was never initialized.
/// Passing the column through as an option is what lets two embedded workers exchange data.
/// A config that already has the option keeps it, so an explicit value still wins.
fn embedded_worker_config(worker: &worker::Model) -> anyhow::Result<String> {
    let mut config = match &worker.config {
        serde_json::Value::Null => serde_json::Value::Object(serde_json::Map::new()),
        config @ serde_json::Value::Object(_) => config.clone(),
        other => anyhow::bail!(
            "the config of worker {} is not an object: {other}",
            worker.host_addr
        ),
    };
    if let Some(options) = config.as_object_mut() {
        options
            .entry(WORKER_DATA_ADDRESS_OPTION)
            .or_insert_with(|| serde_json::Value::String(worker.data_addr.to_string()));
    }
    Ok(config.to_string())
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
        // Build the task lazily,
        // so it only runs when the address is not already tracked.
        // The reconcile pass calls this every tick for every active worker.
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

    /// One pass over the worker table:
    /// start a task for each active worker that has none yet,
    /// and abort and mark removed the ones whose desired state asks for it.
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
                        // Log and move on to the other workers;
                        // this one is still actionable and retried next tick.
                        warn!(error = %e, "failed to update worker");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::worker::{DesiredWorkerState, WorkerState, endpoint::NetworkAddr};

    fn worker_with(config: serde_json::Value) -> worker::Model {
        worker::Model {
            host_addr: NetworkAddr::new("worker", 8080).unwrap(),
            data_addr: NetworkAddr::new("worker", 9090).unwrap(),
            max_operators: None,
            config,
            current_state: WorkerState::Pending,
            desired_state: DesiredWorkerState::Active,
        }
    }

    fn option(config: &str) -> String {
        let parsed: serde_json::Value = serde_json::from_str(config).unwrap();
        parsed[WORKER_DATA_ADDRESS_OPTION]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn a_worker_without_a_config_gets_its_data_address() {
        let config = embedded_worker_config(&worker_with(serde_json::Value::Null)).unwrap();
        assert_eq!(option(&config), "worker:9090");
    }

    #[test]
    fn an_explicit_data_address_wins() {
        let worker = worker_with(serde_json::json!({ WORKER_DATA_ADDRESS_OPTION: "other:1" }));
        assert_eq!(option(&embedded_worker_config(&worker).unwrap()), "other:1");
    }

    #[test]
    fn a_config_that_is_not_an_object_is_rejected() {
        assert!(embedded_worker_config(&worker_with(serde_json::json!([1]))).is_err());
    }
}
