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

#![warn(clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::redundant_pub_crate,
    // The lint suggests Duration::from_mins/from_hours, which are not stable in
    // the pinned dev-container rustc, so the constants stay in from_secs.
    clippy::duration_suboptimal_units
)]

//! Reconciliation hierarchy: one task per DB row whose state has to be moved
//! toward its target. The top-level controller spawns one worker task per active
//! worker row, and each worker task spawns one fragment task per fragment assigned
//! to it. A task's lifetime mirrors its row: it exists while the row's desired state
//! asks for it, and is aborted when the row is marked for removal. Authority
//! lives in the DB, never in task memory, so a restart resumes from exactly
//! the state the previous process left behind.
//!
//! The loop is level-triggered: a periodic tick re-reads the DB regardless of
//! notifications. Watch channels coalesce intent signals down to "go look
//! again" wake-ups, which makes latency good without making correctness depend
//! on any individual signal arriving. A missed wake-up only delays the next
//! reaction to the next poll interval.

use crate::config::POLL_INTERVAL;
use crate::embedded::WorkerFactory;
use crate::util::buggify::buggify;
use crate::util::reconcile::{Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use model::worker::{DesiredWorkerState, WorkerTransition};
use sea_orm::ActiveModelTrait;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, info, info_span, warn};

mod config;
pub mod embedded;
mod error;
pub mod fragment;
pub mod remote;
mod version;

pub use version::worker_versions;
pub(crate) mod util;

/// Top of the reconciliation hierarchy.
///
/// Watches the worker table and keeps one task running per active worker row.
/// When a row's desired state is set to removed, it tears the task down. A
/// worker may run in-process, backed by an injected factory, or out-of-process
/// over gRPC.
pub struct Controller {
    db: DatabaseConnection,
    /// Wake-up signal: a change here means the database likely changed and the
    /// worker table should be re-read.
    intent_rx: watch::Receiver<()>,
    /// Outbound signal that this level advanced observed state, so a watcher
    /// can react without waiting for its next poll.
    state_tx: Arc<watch::Sender<()>>,
    workers: TaskMap<NetworkAddr>,
    factory: Option<Arc<dyn WorkerFactory>>,
}

/// The option a worker reads its data-plane address from, declared by `SingleNodeWorkerConfiguration` on the C++ side.
///
/// Deliberately not the `Worker::DataAddr` identifier of the migration crate. That one names the `data_addr` **column**
/// the coordinator routes by; this one names an **option of the worker process**, and the two spellings differ. Passing
/// the column name here leaves the option unset, and the worker starts no receiver.
const WORKER_DATA_ADDRESS_OPTION: &str = "data_address";

/// The configuration an embedded worker is built from: its own config, plus the address it serves data on.
///
/// The data address is a column of the worker row rather than one of its options, because the coordinator routes and
/// enforces uniqueness by it. A worker reads it from its options instead, and one that finds no value there starts no
/// receiver, so anything sent to it reports that the receiver was never initialized. Passing the column through as an
/// option is what lets two embedded workers exchange data.
///
/// A config that already names one keeps it, so an explicit option still wins.
fn embedded_worker_config(worker: &worker::Model) -> String {
    let mut config = worker.config.clone();
    if !config.is_object() {
        config = serde_json::Value::Object(serde_json::Map::new());
    }
    if let Some(options) = config.as_object_mut() {
        options
            .entry(WORKER_DATA_ADDRESS_OPTION)
            .or_insert_with(|| serde_json::Value::String(worker.data_addr.to_string()));
    }
    config.to_string()
}

impl Controller {
    #[must_use]
    pub fn new(
        db: DatabaseConnection,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
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
        reconcile_loop(&mut self, &mut intent_rx, POLL_INTERVAL).await;
    }

    fn spawn_task(&mut self, worker_model: worker::Model) {
        let addr = worker_model.host_addr.clone();
        let intent_rx = self.intent_rx.clone();
        let state_tx = self.state_tx.clone();
        let db = self.db.clone();
        // Build the task (and, for embedded, the underlying worker) lazily, so
        // it only runs when the address is not already tracked. reconcile()
        // calls this every tick for every active worker.
        if let Some(factory) = self.factory.clone() {
            let span = info_span!("embedded_worker", addr = %addr);
            self.workers.spawn_if_untracked(addr, move || {
                // Constructing the worker happens inside the task so that a bad
                // config fails just this one, instead of unwinding the caller.
                async move {
                    let embedded_worker = factory.create(&embedded_worker_config(&worker_model))?;
                    embedded::WorkerTask::new(
                        worker_model,
                        embedded_worker,
                        db,
                        intent_rx,
                        state_tx,
                    )
                    .run()
                    .await
                }
                .instrument(span)
            });
        } else {
            let span = info_span!("remote_worker", addr = %addr);
            self.workers.spawn_if_untracked(addr, move || {
                remote::WorkerTask::new(worker_model, db, intent_rx, state_tx)
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

    /// One pass over the worker table: start a task for each active worker not
    /// already running, and abort then mark removed the ones whose desired
    /// state asks for it.
    async fn reconcile(&mut self) {
        let workers = match worker::Entity::actionable(&self.db).await {
            Ok(workers) => workers,
            Err(e) => {
                warn!("failed to fetch workers: {e:?}");
                return;
            }
        };

        for worker in workers {
            match worker.desired_state {
                DesiredWorkerState::Active => {
                    self.spawn_task(worker);
                }
                DesiredWorkerState::Removed => {
                    info!("aborting task for {}", worker.host_addr);
                    self.workers.abort(&worker.host_addr);
                    if buggify!() {
                        continue;
                    }
                    let mut update: worker::ActiveModel = worker.into();
                    update.apply_transition(WorkerTransition::Removed);
                    if let Err(e) = update.save(&self.db).await {
                        // Log and move on to the other workers; this one is
                        // still actionable and retried next tick.
                        warn!("failed to update worker: {e}");
                    }
                }
            }
        }
    }
}
