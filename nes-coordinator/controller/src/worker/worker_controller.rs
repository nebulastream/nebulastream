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

use crate::worker::embedded::{EmbeddedWorkerFactory, EmbeddedWorkerTask};
use crate::util::task_map::TaskMap;
use crate::worker::worker_registry::WorkerRegistry;
use crate::worker::worker_task::GrpcWorkerTask;
use catalog::Reconcilable;
use catalog::WorkerStateManager;
use model::worker::{DesiredWorkerState, GetWorker, WorkerState};
use model::worker::endpoint::NetworkAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tracing::{error, info, warn};

const WORKER_SERVICE_POLL_INTERVAL: Duration = Duration::from_secs(5);

// Manages one long-running task per worker. Each task handles its own
// connect -> health check -> reconnect loop (see worker_task). The controller
// only decides *which* workers should exist by comparing DB intent with
// currently tracked tasks, then spawns or aborts accordingly.
pub struct WorkerController {
    worker_catalog: Arc<WorkerStateManager>,
    registry: WorkerRegistry,
    tasks: TaskMap<NetworkAddr>,
    factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
}

impl WorkerController {
    pub fn new(
        worker_catalog: Arc<WorkerStateManager>,
        registry: WorkerRegistry,
        factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
    ) -> Self {
        WorkerController {
            worker_catalog,
            registry,
            tasks: TaskMap::new(),
            factory,
        }
    }

    pub async fn run(mut self) {
        let mut intent_rx = self.worker_catalog.subscribe_intent();
        info!("starting");
        self.ensure_tracked().await;
        self.reconcile().await;

        loop {
            select! {
                // A client changed desired worker state (create or drop)
                change = intent_rx.changed() => {
                    if change.is_err() {
                        // Catalog dropped -- coordinator is shutting down
                        info!("worker catalog notification channel closed, shutting down");
                        return;
                    }
                }
                // A worker_task completed (worker was removed or connection permanently lost)
                Some(task_result) = self.tasks.join_next() => {
                    match task_result {
                        Ok(addr) => {
                            info!(%addr, "worker task exited");
                        }
                        // We aborted this task ourselves via reconcile -> abort(); already cleaned up
                        Err(e) if e.is_cancelled() => {}
                        // Unreachable: TaskMap catches panics via catch_unwind
                        Err(e) => error!("worker task failed: {e:?}"),
                    }
                }
                // Periodic fallback in case a notification was missed
                _ = tokio::time::sleep(WORKER_SERVICE_POLL_INTERVAL) => {}
            }
            // After any event, re-check DB for workers whose desired != actual state
            self.reconcile().await;
        }
    }

    fn spawn_worker_task(&mut self, worker: model::worker::Model) {
        let addr = worker.host_addr.clone();
        if let Some(factory) = &self.factory {
            let config_json = worker.config.to_string();
            let handle = factory.create(&config_json);
            let task = EmbeddedWorkerTask::new(
                worker,
                handle,
                self.worker_catalog.clone(),
                self.registry.clone(),
            )
            .run();
            self.tasks.spawn_if_untracked(addr, (), task);
        } else {
            let task =
                GrpcWorkerTask::new(worker, self.worker_catalog.clone(), self.registry.clone())
                    .run();
            self.tasks.spawn_if_untracked(addr, (), task);
        }
    }

    // Ensures that for each worker that should be active, we have one corresponding task running
    async fn ensure_tracked(&mut self) {
        let workers = match self
            .worker_catalog
            .get_worker(GetWorker::all().with_desired_state(DesiredWorkerState::Active))
            .await
        {
            Ok(w) => w,
            Err(e) => {
                warn!("failed to fetch desired-active workers: {e:?}");
                return;
            }
        };

        for worker in workers {
            self.spawn_worker_task(worker);
        }
    }

    async fn reconcile(&mut self) {
        // Returns workers where desired_state != actual_state
        let mismatched_workers = match self.worker_catalog.get_mismatch().await {
            Ok(workers) => workers,
            Err(e) => {
                warn!("failed to fetch workers: {e:?}");
                return;
            }
        };

        for mismatched_worker in mismatched_workers {
            match mismatched_worker.desired_state {
                DesiredWorkerState::Active => {
                    self.spawn_worker_task(mismatched_worker);
                }
                DesiredWorkerState::Removed => {
                    // Abort drops the RegistryGuard inside worker_task -> auto-unregisters
                    self.tasks.abort(&mismatched_worker.host_addr);
                    self.worker_catalog
                        .mark_worker(mismatched_worker.into(), WorkerState::Removed)
                        .await
                        .expect("failed to mark worker as removed");
                }
            }
        }
    }
}
