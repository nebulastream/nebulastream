use crate::worker::poly_join_set::TaskMap;
use crate::worker::worker_task::WorkerTask;
use crate::worker::worker_registry::WorkerRegistry;
use catalog::Reconcilable;
use catalog::worker_catalog::WorkerCatalog;
use model::worker::{DesiredWorkerState, GetWorker};
use model::worker::endpoint::GrpcAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tracing::{error, info, warn};

const WORKER_SERVICE_POLL_INTERVAL: Duration = Duration::from_secs(5);

// Manages one long-running task per worker. Each task handles its own
// connect → health check → reconnect loop (see worker_task). The controller
// only decides *which* workers should exist by comparing DB intent with
// currently tracked tasks, then spawns or aborts accordingly.
pub struct WorkerController {
    worker_catalog: Arc<WorkerCatalog>,
    // Holds the RPC send-side for each active worker; worker_task registers
    // itself here once connected so that QueryController can dispatch RPCs.
    registry: WorkerRegistry,
    // One entry per worker: key = GrpcAddr, value = abort handle.
    // JoinSet aborts all tasks on drop.
    tasks: TaskMap<GrpcAddr>,
}

impl WorkerController {
    pub fn new(worker_catalog: Arc<WorkerCatalog>, registry: WorkerRegistry) -> Self {
        WorkerController {
            worker_catalog,
            registry,
            tasks: TaskMap::new(),
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
                        // Catalog dropped — coordinator is shutting down
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
                        // We aborted this task ourselves via reconcile → abort(); already cleaned up
                        Err(e) if e.is_cancelled() => {}
                        // Unreachable: TaskMap catches panics via catch_unwind
                        Err(e) => error!("worker task failed: {e:?}"),
                    }
                }
                // Periodic fallback in case a notification was missed
                _ = tokio::time::sleep(WORKER_SERVICE_POLL_INTERVAL) => {}
            }
            // After any event, re-check DB for workers whose desired ≠ actual state
            self.reconcile().await;
        }
    }

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
            let addr = worker.grpc_addr.clone();
            if !self.tasks.contains_key(&addr) {
                let task = WorkerTask::new(
                    worker,
                    self.worker_catalog.clone(),
                    self.registry.clone(),
                );
                self.tasks.spawn(addr, (), task.run());
            }
        }
    }

    async fn reconcile(&mut self) {
        // Returns workers where desired_state ≠ actual_state
        let mismatched_workers = match self.worker_catalog.get_mismatch().await {
            Ok(workers) => workers,
            Err(e) => {
                warn!("failed to fetch workers: {e:?}");
                return;
            }
        };

        for mismatch in mismatched_workers {
            let addr = mismatch.grpc_addr.clone();
            match mismatch.desired_state {
                DesiredWorkerState::Active => {
                    // Only spawn if we don't already have a task for this address
                    if !self.tasks.contains_key(&addr) {
                        let task = WorkerTask::new(
                            mismatch,
                            self.worker_catalog.clone(),
                            self.registry.clone(),
                        );
                        self.tasks.spawn(addr, (), task.run());
                    }
                }
                DesiredWorkerState::Removed => {
                    // Abort drops the RegistryGuard inside worker_task → auto-unregisters
                    self.tasks.abort(&addr);
                    self.worker_catalog
                        .remove_worker(mismatch.into())
                        .await
                        .expect("failed to mark worker as removed");
                }
            }
        }
    }
}
