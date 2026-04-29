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

use crate::embedded::{EmbeddedWorkerFactory, EmbeddedWorkerTask};
use crate::remote::RemoteWorkerTask;
use crate::util::reconcile::{Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use model::worker::{DesiredWorkerState, WorkerTransition};
use sea_orm::ActiveModelTrait;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{Instrument, info, info_span, warn};

pub mod embedded;
mod error;
pub mod fragment;
pub mod remote;
pub(crate) mod util;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct Controller {
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    workers: TaskMap<NetworkAddr>,
    factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
}

impl Controller {
    pub fn new(
        db: DatabaseConnection,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
        factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
    ) -> Self {
        Controller {
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
        if let Some(factory) = &self.factory {
            let embedded_worker = factory.create(&worker_model.config.to_string());
            let task = EmbeddedWorkerTask::new(
                worker_model,
                embedded_worker,
                self.db.clone(),
                intent_rx,
                state_tx,
            )
            .run()
            .instrument(info_span!("embedded_worker", addr = %addr.clone()));
            self.workers.spawn_if_untracked(addr, task);
        } else {
            let task = RemoteWorkerTask::new(worker_model, self.db.clone(), intent_rx, state_tx)
                .run()
                .instrument(info_span!("remote_worker", addr = %addr.clone()));
            self.workers.spawn_if_untracked(addr, task);
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
                warn!("failed to fetch workers: {e:?}");
                return;
            }
        };

        for mismatch in workers {
            match mismatch.desired_state {
                DesiredWorkerState::Active => {
                    self.spawn_task(mismatch);
                }
                DesiredWorkerState::Removed => {
                    info!("aborting task for {}", mismatch.host_addr);
                    self.workers.abort(&mismatch.host_addr);
                    #[cfg(madsim)]
                    if tokio::madsim::buggify::buggify() {
                        continue;
                    }
                    let mut update: worker::ActiveModel = mismatch.into();
                    update.apply_transition(WorkerTransition::Removed);
                    if let Err(e) = update.save(&self.db).await {
                        warn!("failed to update worker: {e}");
                        return;
                    }
                }
            }
        }
    }
}
