use crate::query::QueryId;
use crate::query::context::QueryContext;
use crate::query::query_task;
use crate::worker::poly_join_set::TaskMap;
use crate::worker::worker_registry::WorkerRegistryHandle;
use catalog::Catalog;
use catalog::Reconcilable;
use model::query::*;

use model::query;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const QUERY_CONTROLLER_POLL_INTERVAL: Duration = Duration::from_secs(5);
pub struct QueryController {
    catalog: Arc<Catalog>,
    worker_registry: WorkerRegistryHandle,
    reconcilers: TaskMap<QueryId, flume::Sender<StopMode>>,
}

impl QueryController {
    pub fn new(catalog: Arc<Catalog>, worker_registry: WorkerRegistryHandle) -> Self {
        QueryController {
            catalog,
            worker_registry,
            reconcilers: TaskMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut intent = self.catalog.query.subscribe_intent();
        info!("starting");
        self.reconcile().await;

        loop {
            tokio::select! {
                change = intent.changed() => {
                    if change.is_err() {
                        info!("query catalog notification channel closed, shutting down");
                        return;
                    }
                }
                Some(task_result) = self.reconcilers.join_next() => {
                    if let Err(e) = task_result && !e.is_cancelled() {
                        error!("reconciler task failed: {e:?}");
                    }
                }
                _ = tokio::time::sleep(QUERY_CONTROLLER_POLL_INTERVAL) => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        let mismatches = match self.catalog.query.get_mismatch().await {
            Ok(m) => m,
            Err(e) => {
                warn!("failed to fetch query mismatches: {e}");
                return;
            }
        };

        for mismatch in mismatches {
            match self.reconcilers.get(&mismatch.id) {
                Some(stop_channel) => match mismatch.desired_state {
                    query_state::DesiredQueryState::Completed => {
                        debug!(
                            query_id = mismatch.id,
                            "reconciliation already running, skipping"
                        );
                    }
                    query_state::DesiredQueryState::Stopped => {
                        self.send_stop_signal(&mismatch, stop_channel);
                    }
                },
                None => {
                    self.spawn_task(mismatch);
                }
            }
        }
    }

    fn spawn_task(&mut self, mismatch: query::Model) {
        let (stop_tx, stop_rx) = flume::bounded(1);
        let query_id = mismatch.id;

        let ctx = QueryContext {
            query: mismatch,
            fragments: Vec::new(),
            catalog: self.catalog.clone(),
            worker_registry: self.worker_registry.clone(),
        };

        self.reconcilers.spawn(query_id, stop_tx, async move {
            query_task::run(ctx, stop_rx).await;
        });
    }

    fn send_stop_signal(
        &self,
        query_to_stop: &query::Model,
        stop_channel: &flume::Sender<StopMode>,
    ) {
        let mode = query_to_stop
            .stop_mode
            .expect("if desired_state == 'Stopped', the stop mode must be set");
        // try_send instead of send_async: reconcile is called from a synchronous
        // loop over all mismatches and must not yield. If the channel is full, a
        // stop signal is already queued and the reconciler will see it.
        match stop_channel.try_send(mode) {
            Ok(_) => debug!(query_id = query_to_stop.id, ?mode, "sent stop signal"),
            Err(flume::TrySendError::Full(_)) => {}
            Err(flume::TrySendError::Disconnected(_)) => {
                debug!(
                    query_id = query_to_stop.id,
                    "reconciliation task already finished"
                )
            }
        }
    }
}
