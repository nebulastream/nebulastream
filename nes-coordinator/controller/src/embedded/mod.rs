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

use crate::fragment::{FragmentStatus, FragmentTask};
use crate::util::reconcile::{Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use client::EmbeddedFragmentClient;
use model::identifier::FragmentId;
use model::query::fragment;
use model::query::fragment::{FragmentError, FragmentState, StopMode};
use model::worker;
use model::worker::WorkerState;
use sea_orm::ActiveValue::Set;
use sea_orm::prelude::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Instrument, error, info, info_span, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub trait EmbeddedWorker: Send + Sync + 'static {
    fn register_fragment(&self, plan: Vec<u8>) -> Result<(), FragmentError>;
    fn start_fragment(&self, id: i64) -> Result<(), FragmentError>;
    fn stop_fragment(&self, id: i64, mode: StopMode) -> Result<(), FragmentError>;
    fn get_fragment_status(&self, id: i64) -> Result<FragmentStatus, FragmentError>;
}

pub trait EmbeddedWorkerFactory: Send + Sync + 'static {
    fn create(&self, config_json: &str) -> Arc<dyn EmbeddedWorker>;
}

pub(super) struct EmbeddedWorkerTask {
    worker: worker::Model,
    handle: Arc<dyn EmbeddedWorker>,
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    fragments: TaskMap<FragmentId>,
}

impl EmbeddedWorkerTask {
    pub(super) fn new(
        worker: worker::Model,
        handle: Arc<dyn EmbeddedWorker>,
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
        active.current_state = Set(WorkerState::Active);
        active
            .update(&self.db)
            .await
            .inspect_err(|e| error!("failed to update worker {e:?}"))?;

        let mut intent_rx = self.intent_rx.clone();
        reconcile_loop(&mut self, &mut intent_rx, POLL_INTERVAL).await;
        Ok(())
    }
}

impl Reconciler for EmbeddedWorkerTask {
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
            let client = EmbeddedFragmentClient::new(self.handle.clone());
            let task = FragmentTask::new(fragment, self.db.clone(), client, self.state_tx.clone())
                .run()
                .instrument(info_span!("fragment", id = %id, query_id = %query_id));
            self.fragments.spawn_if_untracked(id, task);
        }
    }
}
