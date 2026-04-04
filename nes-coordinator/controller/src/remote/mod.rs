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

use crate::error::WorkerTaskError;
use crate::fragment::FragmentTask;
use crate::util::reconcile::Reconciler;
use crate::util::task_map::TaskMap;
use client::RemoteFragmentClient;
use model::identifier::FragmentId;
use model::query::fragment;
use model::query::fragment::FragmentState;
use model::worker;
use model::worker::WorkerState;
use model::worker::endpoint::NetworkAddr;
use sea_orm::ActiveValue::Set;
use sea_orm::prelude::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::{Channel, Endpoint};
use tracing::{Instrument, debug, error, info, info_span, warn};

pub mod worker_rpc_service {
    pub mod nes {
        tonic::include_proto!("nes");
    }
    tonic::include_proto!("_");
}

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_client::HealthClient;
pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_MAX_RETRIES: usize = 10;
const CONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);
const ENDPOINT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);
const ENDPOINT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(60);

fn connect_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(50)
        .max_delay(CONNECT_MAX_DELAY)
        .map(jitter)
        .take(CONNECT_MAX_RETRIES)
}

#[tracing::instrument(level = "info")]
async fn connect(addr: &NetworkAddr) -> Result<Channel, WorkerTaskError> {
    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
        .map_err(|e| WorkerTaskError::Connection {
            addr: addr.clone(),
            err: e,
        })?
        .http2_keep_alive_interval(ENDPOINT_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(ENDPOINT_KEEP_ALIVE_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT);

    let addr = addr.clone();
    Retry::spawn(connect_retry_strategy(), || async {
        endpoint.connect().await.map_err(|e| {
            warn!("retrying connection");
            WorkerTaskError::Connection {
                addr: addr.clone(),
                err: e,
            }
        })
    })
    .await
}

async fn health_check(client: &HealthClient<Channel>) -> bool {
    let mut client = client.clone();
    let req = tonic::Request::new(health_proto::HealthCheckRequest::default());
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, client.check(req)).await,
        Ok(Ok(_))
    )
}

pub(super) struct RemoteWorkerTask {
    worker: worker::Model,
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    fragments: TaskMap<FragmentId>,
    worker_client: Option<WorkerRpcServiceClient<Channel>>,
}

impl RemoteWorkerTask {
    pub(super) fn new(
        worker: worker::Model,
        db: DatabaseConnection,
        intent_rx: watch::Receiver<()>,
        state_tx: Arc<watch::Sender<()>>,
    ) -> Self {
        Self {
            worker,
            db,
            intent_rx,
            state_tx,
            fragments: TaskMap::new(),
            worker_client: None,
        }
    }

    pub(super) async fn run(mut self) -> anyhow::Result<()> {
        loop {
            match connect(&self.worker.host_addr).await {
                Ok(channel) => {
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
                    info!("active");

                    self.serve(channel).await;
                    self.fragments = TaskMap::new();

                    warn!("unreachable");
                    #[cfg(madsim)]
                    if tokio::madsim::buggify::buggify() {
                        return Err(anyhow::anyhow!("buggify: worker unreachable update failed"));
                    }
                    let mut unreachable: worker::ActiveModel = self.worker.clone().into();
                    unreachable.current_state = Set(WorkerState::Unreachable);
                    unreachable
                        .update(&self.db)
                        .await
                        .inspect_err(|e| warn!("failed to mark worker unreachable: {e}"))?;
                }
                Err(connect_err) => {
                    error!("failed to connect after {CONNECT_MAX_RETRIES} attempts: {connect_err}");
                    #[cfg(madsim)]
                    if tokio::madsim::buggify::buggify() {
                        return Err(anyhow::anyhow!("buggify: worker unreachable update failed"));
                    }
                    let mut unreachable: worker::ActiveModel = self.worker.clone().into();
                    unreachable.current_state = Set(WorkerState::Unreachable);
                    unreachable
                        .update(&self.db)
                        .await
                        .inspect_err(|e| warn!("failed to mark worker unreachable: {e}"))?;
                }
            }
            tokio::time::sleep(RECONNECT_INTERVAL).await;
        }
    }

    /// Reconcile fragments and run health checks until the connection drops.
    /// Uses the same Reconciler pattern as Controller and EmbeddedWorkerTask,
    /// but adds a heartbeat branch that breaks on health check failure.
    async fn serve(&mut self, channel: Channel) {
        let health_client = HealthClient::new(channel.clone());
        self.worker_client = Some(WorkerRpcServiceClient::new(channel));
        let mut heartbeat = tokio::time::interval(HEALTH_CHECK_INTERVAL);
        let mut poll = tokio::time::interval(POLL_INTERVAL);
        self.reconcile().await;
        loop {
            {
                let tasks = &mut self.fragments;
                select! {
                    Some(result) = tasks.join_next() => {
                        match result {
                            Ok(id) => debug!(fragment_id = %id, "fragment task completed"),
                            Err(e) if e.is_cancelled() => {}
                            Err(e) => warn!("fragment task failed: {e:?}"),
                        }
                    }
                    Ok(()) = self.intent_rx.changed() => {}
                    _ = poll.tick() => {}
                    _ = heartbeat.tick() => {
                        if !health_check(&health_client).await {
                            warn!("health check failed");
                            self.worker_client = None;
                            return;
                        }
                        continue;
                    }
                }
            }
            self.reconcile().await;
        }
    }
}

impl Reconciler for RemoteWorkerTask {
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

        let Some(ref client) = self.worker_client else {
            return;
        };

        for fragment in fragments {
            let id = fragment.id;
            let query_id = fragment.query_id;
            let db = self.db.clone();
            let fragment_client =
                RemoteFragmentClient::new(client.clone(), self.worker.host_addr.clone());
            let task = FragmentTask::new(fragment, db, fragment_client, self.state_tx.clone())
                .run()
                .instrument(info_span!("fragment", id = %id, query_id = %query_id));
            self.fragments.spawn_if_untracked(id, task);
        }
    }
}
