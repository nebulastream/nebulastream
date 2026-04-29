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

mod client;

use crate::fragment::{Status, Task};
use crate::util::reconcile::{Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use client::FragmentClient;
use model::identifier::FragmentId;
use model::query::fragment;
use model::query::fragment::{FragmentError, StopMode};
use model::worker;
use model::worker::WorkerTransition;
use sea_orm::{ActiveModelTrait, DatabaseConnection};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Instrument, error, info, info_span, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// In-process worker interface. Implementors run fragment lifecycle commands
/// synchronously inside the coordinator process; an internal adapter exposes
/// them to the lifecycle driver as an async client.
pub trait Worker: Send + Sync + 'static {
    fn register_fragment(&self, plan: Vec<u8>) -> Result<(), FragmentError>;
    fn start_fragment(&self, id: i64) -> Result<(), FragmentError>;
    fn stop_fragment(&self, id: i64, mode: StopMode) -> Result<(), FragmentError>;
    fn get_fragment_status(&self, id: i64) -> Result<Status, FragmentError>;
}

/// Constructs an in-process worker from the worker row's config JSON.
/// Letting the host supply the factory keeps this crate decoupled from any
/// concrete worker implementation.
pub trait WorkerFactory: Send + Sync + 'static {
    fn create(&self, config_json: &str) -> Arc<dyn Worker>;
}

/// Per-worker reconciliation task for the in-process backend. Marks the
/// worker active in the DB on start, then loops: load actionable fragments
/// assigned to this worker and spawn a lifecycle driver for each one.
pub(super) struct WorkerTask {
    worker: worker::Model,
    handle: Arc<dyn Worker>,
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    fragments: TaskMap<FragmentId>,
}

impl WorkerTask {
    pub(super) fn new(
        worker: worker::Model,
        handle: Arc<dyn Worker>,
        db: DatabaseConnection,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
    ) -> Self {
        Self {
            worker,
            handle,
            db,
            intent_rx,
            state_tx,
            fragments: TaskMap::new(),
        }
    }

    pub(super) async fn run(mut self) -> anyhow::Result<()> {
        info!("starting");
        #[cfg(madsim)]
        if tokio::madsim::buggify::buggify() {
            return Err(anyhow::anyhow!("buggify: worker active update failed"));
        }
        let mut active: worker::ActiveModel = self.worker.clone().into();
        active.apply_transition(WorkerTransition::Active);
        active
            .update(&self.db)
            .await
            .inspect_err(|e| error!("failed to update worker {e:?}"))?;

        let mut intent_rx = self.intent_rx.clone();
        reconcile_loop(&mut self, &mut intent_rx, POLL_INTERVAL).await;
        Ok(())
    }
}

impl Reconciler for WorkerTask {
    type Key = FragmentId;

    fn tasks(&mut self) -> &mut TaskMap<FragmentId> {
        &mut self.fragments
    }

    async fn reconcile(&mut self) {
        #[cfg(madsim)]
        if tokio::madsim::buggify::buggify() {
            return;
        }
        let fragments = match fragment::Entity::actionable(&self.db, &self.worker.host_addr).await {
            Ok(fragments) => fragments,
            Err(e) => {
                warn!("failed to load fragments: {e}");
                return;
            }
        };

        for fragment in fragments {
            let id = fragment.id;
            let query_id = fragment.query_id;
            let client = FragmentClient::new(self.handle.clone());
            let task = Task::new(fragment, self.db.clone(), client, self.state_tx.clone())
                .run()
                .instrument(info_span!("fragment", id = %id, query_id = %query_id));
            self.fragments.spawn_if_untracked(id, task);
        }
    }
}
