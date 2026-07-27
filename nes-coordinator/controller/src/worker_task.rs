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

use crate::config::{HEALTH_CHECK_INTERVAL, POLL_INTERVAL, RECONNECT_INTERVAL};
use crate::query_fragment::{QueryFragmentTasks, Transport};
use crate::util::buggify::buggify_return;
use crate::util::reconcile::{Exit, Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use model::identifier::QueryFragmentId;
use model::worker;
use model::worker::WorkerTransition;
use model::worker::endpoint::NetworkAddr;
use sea_orm::{ActiveModelTrait, DatabaseConnection};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

pub(crate) trait Backend: Send + Sync + 'static {
    type Link: Send + Sync;
    type Transport: Transport + 'static;

    fn connect(
        &self,
        addr: &NetworkAddr,
    ) -> impl Future<Output = anyhow::Result<Self::Link>> + Send;
    fn transport(&self, link: &Self::Link, addr: &NetworkAddr) -> Self::Transport;
    fn heartbeat(&self) -> Option<Duration>;
    fn healthy(&self, link: &Self::Link) -> impl Future<Output = bool> + Send;
}

pub(crate) struct WorkerTask<B: Backend> {
    worker: worker::Model,
    backend: B,
    link: Option<B::Link>,
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    fragments: QueryFragmentTasks,
}

impl<B: Backend> WorkerTask<B> {
    pub(crate) fn new(
        worker: worker::Model,
        backend: B,
        db: DatabaseConnection,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
    ) -> Self {
        Self {
            worker,
            backend,
            link: None,
            db,
            intent_rx,
            state_tx,
            fragments: QueryFragmentTasks::new(),
        }
    }

    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        loop {
            match self.backend.connect(&self.worker.host_addr).await {
                Ok(link) => self.link = Some(link),
                Err(err) => {
                    error!("{err:#}");
                    self.mark(WorkerTransition::Unreachable).await?;
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                    continue;
                }
            }
            self.mark(WorkerTransition::Active).await?;
            info!("active");
            let heartbeat = self.backend.heartbeat();
            let mut intent_rx = self.intent_rx.clone();
            let exit = reconcile_loop(&mut self, &mut intent_rx, POLL_INTERVAL, heartbeat).await;
            self.link = None;
            self.fragments = QueryFragmentTasks::new();
            match exit {
                Exit::Shutdown => return Ok(()),
                Exit::LinkLost => {
                    warn!("unreachable");
                    self.mark(WorkerTransition::Unreachable).await?;
                    tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
                }
            }
        }
    }

    async fn mark(&self, transition: WorkerTransition) -> anyhow::Result<()> {
        buggify_return!(Err(anyhow::anyhow!("buggify: worker update failed")));
        let mut update: worker::ActiveModel = self.worker.clone().into();
        update.apply_transition(transition);
        update
            .update(&self.db)
            .await
            .inspect_err(|e| warn!("failed to update worker: {e}"))?;
        Ok(())
    }
}

impl<B: Backend> Reconciler for WorkerTask<B> {
    type Key = QueryFragmentId;

    fn tasks(&mut self) -> &mut TaskMap<QueryFragmentId> {
        self.fragments.tasks()
    }

    async fn reconcile(&mut self) {
        buggify_return!();
        let Some(link) = &self.link else {
            return;
        };
        let (backend, addr) = (&self.backend, &self.worker.host_addr);
        self.fragments
            .reconcile_from_catalog(&self.db, addr, &self.state_tx, || {
                backend.transport(link, addr)
            })
            .await;
    }

    async fn alive(&mut self) -> bool {
        match &self.link {
            Some(link) => self.backend.healthy(link).await,
            None => false,
        }
    }
}
