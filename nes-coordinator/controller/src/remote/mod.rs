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

//! Runs the fragments of a worker in another process over gRPC: connects with
//! retries, records whether the worker is reachable, and reconciles its fragments
//! while connected.

mod client;

use crate::config::{
    CONNECT_MAX_DELAY, CONNECT_MAX_RETRIES, CONNECT_TIMEOUT, ENDPOINT_KEEP_ALIVE_INTERVAL,
    ENDPOINT_KEEP_ALIVE_TIMEOUT, HEALTH_CHECK_INTERVAL, POLL_INTERVAL, PROBE_TIMEOUT,
    RECONNECT_INTERVAL,
};
use crate::error::WorkerTaskError;
use crate::fragment::FragmentTask;
use crate::util::buggify::buggify_return;
use crate::util::reconcile::Reconciler;
use crate::util::task_map::TaskMap;
use client::QueryFragmentClient;
use model::identifier::QueryFragmentId;
use model::query::query_fragment;
use model::worker;
use model::worker::WorkerTransition;
use model::worker::endpoint::NetworkAddr;
use sea_orm::ActiveModelTrait;
use sea_orm::DatabaseConnection;
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

        impl SerializableQueryId {
            /// The wire format carries two string ids while the coordinator
            /// identifies fragments by a single integer. Carry the integer in
            /// the local id, rendered as a decimal string, until the wire
            /// format itself moves to an integer id.
            pub fn from_fragment_id(id: i64) -> Self {
                Self {
                    local_query_id: id.to_string(),
                    distributed_query_id: String::new(),
                }
            }
        }
    }
    tonic::include_proto!("_");
}

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_client::HealthClient;
pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

fn connect_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(50)
        .max_delay(CONNECT_MAX_DELAY)
        .map(jitter)
        .take(CONNECT_MAX_RETRIES)
}

#[tracing::instrument(level = "info")]
async fn connect(addr: &NetworkAddr) -> Result<Channel, WorkerTaskError> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
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
            warn!("worker {addr} unreachable: {e}");
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

/// Per-worker reconciliation task for the out-of-process backend.
/// Reconnects to the worker over gRPC with backoff, reflects connection
/// state back into the DB, and while connected runs the same reconciliation
/// loop as the in-process variant with an added heartbeat probe that tears
/// down on failure. Reconnection is the task's own responsibility. The
/// parent only decides whether the task should exist at all (via the
/// worker row's desired state); transient unreachability is invisible to it.
pub(super) struct WorkerTask {
    worker: worker::Model,
    db: DatabaseConnection,
    intent_rx: watch::Receiver<()>,
    state_tx: Arc<watch::Sender<()>>,
    fragments: TaskMap<QueryFragmentId>,
    worker_client: Option<WorkerRpcServiceClient<Channel>>,
}

impl WorkerTask {
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
                    buggify_return!(Err(anyhow::anyhow!("buggify: worker active update failed")));
                    let mut active: worker::ActiveModel = self.worker.clone().into();
                    active.apply_transition(WorkerTransition::Active);
                    active
                        .update(&self.db)
                        .await
                        .inspect_err(|e| error!("failed to update worker {e:?}"))?;
                    info!("active");

                    self.serve(channel).await;
                    self.fragments = TaskMap::new();

                    warn!("unreachable");
                    buggify_return!(Err(anyhow::anyhow!(
                        "buggify: worker unreachable update failed"
                    )));
                    let mut unreachable: worker::ActiveModel = self.worker.clone().into();
                    unreachable.apply_transition(WorkerTransition::Unreachable);
                    unreachable
                        .update(&self.db)
                        .await
                        .inspect_err(|e| warn!("failed to mark worker unreachable: {e}"))?;
                    // Back off before reconnecting so a reachable but failing
                    // worker does not get retried in a tight loop. This is
                    // shorter than the connect-failure backoff because the
                    // worker was reachable and should recover promptly.
                    tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
                }
                Err(connect_err) => {
                    error!("failed to connect after {CONNECT_MAX_RETRIES} attempts: {connect_err}");
                    buggify_return!(Err(anyhow::anyhow!(
                        "buggify: worker unreachable update failed"
                    )));
                    let mut unreachable: worker::ActiveModel = self.worker.clone().into();
                    unreachable.apply_transition(WorkerTransition::Unreachable);
                    unreachable
                        .update(&self.db)
                        .await
                        .inspect_err(|e| warn!("failed to mark worker unreachable: {e}"))?;
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        }
    }

    /// Reconcile fragments and run health checks until the connection drops.
    /// Same reconciliation loop as the in-process variant, with an extra
    /// heartbeat branch that breaks the loop on a health-check failure so
    /// the outer reconnect path can take over.
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

impl Reconciler for WorkerTask {
    type Key = QueryFragmentId;

    fn tasks(&mut self) -> &mut TaskMap<QueryFragmentId> {
        &mut self.fragments
    }

    async fn reconcile(&mut self) {
        buggify_return!();
        let fragments =
            match query_fragment::Entity::actionable(&self.db, &self.worker.host_addr).await {
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
            let rpc_client = client.clone();
            let host_addr = self.worker.host_addr.clone();
            let state_tx = self.state_tx.clone();
            let span = info_span!("fragment", id = %id, query_id = %query_id);
            self.fragments.spawn_if_untracked(id, move || {
                FragmentTask::new(
                    fragment,
                    db,
                    QueryFragmentClient::new(rpc_client, host_addr),
                    state_tx,
                )
                .run()
                .instrument(span)
            });
        }
    }
}
