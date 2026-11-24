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

use crate::buggify::buggify;
use crate::config::MAX_FAILED_STATUS_REQUESTS;
use crate::fragment::FragmentReconciler;
use crate::fragment::calls::{Call, Cancelled};
use crate::status::{WorkerFragmentState, WorkerStatus};
use crate::worker_client::WorkerClient;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use model::database::Database;
use model::worker::endpoint::NetworkAddr;
use model::worker::{self, DesiredWorkerState, WorkerState, WorkerTransition};
use sea_orm::{ActiveModelTrait, EntityTrait};
use std::ops::ControlFlow::{self, Break, Continue};
use std::time::Duration;
use tokio::sync::{OnceCell, watch};
use tokio::time::interval;
use tokio_retry2::strategy::FixedInterval;
use tokio_retry2::{Retry, RetryError};
use tracing::{info, warn};

type Connect<T> = Box<dyn Fn() -> BoxFuture<'static, anyhow::Result<T>> + Send + Sync>;

/// The client from a successful connect is kept for the life of the loop, so an in-process worker is launched once.
struct Connection<T> {
    connect: Connect<T>,
    client: OnceCell<T>,
}

impl<T: WorkerClient> Connection<T> {
    /// Repeats a failed request, including a failed connect, after `retry_delay`,
    /// and returns `None` once several requests in a row failed.
    async fn get_status(&self, after: u64, retry_delay: Duration) -> Option<(T, WorkerStatus)> {
        let delays = FixedInterval::new(retry_delay).take(MAX_FAILED_STATUS_REQUESTS - 1);
        Retry::spawn(delays, || self.try_get_status(after))
            .await
            .ok()
    }

    async fn try_get_status(
        &self,
        after: u64,
    ) -> Result<(T, WorkerStatus), RetryError<anyhow::Error>> {
        let status = async {
            let client = self
                .client
                .get_or_try_init(|| (self.connect)())
                .await
                .context("connect failed")?;
            let response = client.get_status(after).await?;
            anyhow::Ok((client.clone(), response.into()))
        };
        status.await.map_err(|err| {
            warn!(error = %format_args!("{err:#}"), "status request failed");
            RetryError::transient(err)
        })
    }
}

/// Keeps one worker and its fragments in line with the catalog.
pub(crate) struct WorkerLoop<T> {
    addr: NetworkAddr,
    connection: Connection<T>,
    poll_interval: Duration,
    db: Database,
    state_tx: watch::Sender<()>,
    fragments: FragmentReconciler,
}

impl<T: WorkerClient> WorkerLoop<T> {
    /// `poll_interval` also spaces the repeats of a failed status request and of a failed start.
    pub(crate) fn new<C, F>(
        addr: NetworkAddr,
        connect: C,
        poll_interval: Duration,
        db: Database,
        state_tx: watch::Sender<()>,
    ) -> Self
    where
        C: Fn() -> F + Send + Sync + 'static,
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        Self {
            fragments: FragmentReconciler::new(
                addr.clone(),
                db.clone(),
                state_tx.clone(),
                poll_interval,
            ),
            addr,
            connection: Connection {
                connect: Box::new(move || connect().boxed()),
                client: OnceCell::new(),
            },
            poll_interval,
            db,
            state_tx,
        }
    }

    /// Returns once the catalog-change sender is dropped, or once the worker's row is gone or records the worker as removed.
    pub(crate) async fn run(mut self, mut intent_rx: watch::Receiver<()>) {
        let mut poll = interval(self.poll_interval);
        loop {
            tokio::select! {
                changed = intent_rx.changed() => if changed.is_err() { return; },
                _ = poll.tick() => {}
                Some(joined) = self.fragments.join_next() => match joined {
                    Ok((Call::Start(id), result)) => self.fragments.finalize_start(id, result).await,
                    // The next round that still finds the fragment listed repeats a failed stop.
                    // A round after every stop would repeat it without pause while the worker winds the fragment down.
                    Ok((Call::Stop(_), _)) => continue,
                    // Only a runtime that shuts down cancels a call, so the calls of a further round would end the same way.
                    Err(Cancelled) => return,
                },
            }
            poll.reset();
            if self.run_round().await.is_break() {
                return;
            }
        }
    }

    async fn run_round(&mut self) -> ControlFlow<()> {
        if buggify() {
            return Continue(());
        }
        let worker = match worker::Entity::find_by_id(self.addr.clone())
            .one(&self.db)
            .await
        {
            Ok(Some(worker))
                if worker.desired_state == DesiredWorkerState::Removed
                    && worker.current_state == WorkerState::Removed =>
            {
                info!("removed");
                return Break(());
            }
            Ok(Some(worker)) => worker,
            Ok(None) => return Break(()),
            Err(err) => {
                warn!(error = %err, "could not read the worker");
                return Continue(());
            }
        };
        // NOTE: Read the status before the managed rows.
        // Then an active fragment without a managed row has a terminal row or none, and neither changes again.
        // In the other order, a fragment that was created and started between the two reads would be stopped.
        let status = self
            .connection
            .get_status(self.fragments.get_after(), self.poll_interval)
            .await;
        if let Some(worker_transition) =
            worker_transition(&worker, status.as_ref().map(|(_, status)| status))
        {
            self.update_worker_state(&worker, worker_transition).await;
        }
        if let Some((client, status)) = status {
            self.fragments.reconcile(&client, &status).await;
        }
        Continue(())
    }

    /// A failed write is logged, and a later round derives the transition again.
    async fn update_worker_state(&self, worker: &worker::Model, transition: WorkerTransition) {
        let mut updated: worker::ActiveModel = worker.clone().into();
        updated.apply_transition(transition);
        if buggify() {
            warn!("buggify: worker update failed");
            return;
        }
        info!(?transition, "worker");
        if let Err(err) = updated.update(&self.db).await {
            warn!(error = %err, "failed to update worker");
            return;
        }
        // A wait on a fragment of this worker can depend on the worker's state.
        let _ = self.state_tx.send(());
    }
}

/// Returns `None` while the worker's state stays.
///
/// ```text
///             answers
/// Pending ───────────────▶ Active
///    │                     ▲    │
///    │             answers │    │ no answer
///    │                     │    ▼
///    └─── no answer ─────▶ Unreachable
/// ```
///
/// Once the worker is to be removed, any state moves to Removed when the worker lists no live fragment or does not answer.
fn worker_transition(
    worker: &worker::Model,
    status: Option<&WorkerStatus>,
) -> Option<WorkerTransition> {
    let transition = match (worker.desired_state, status) {
        (DesiredWorkerState::Active, None) => Some(WorkerTransition::Unreachable),
        (DesiredWorkerState::Active, Some(_)) => Some(WorkerTransition::Active),
        // A removed worker that does not answer is given up, and its fragments may keep running.
        (DesiredWorkerState::Removed, None) => Some(WorkerTransition::Removed),
        // The removal fails the worker's fragments in the catalog,
        // so each fragment that the worker still lists is stopped as unmanaged, and the removal waits for those stops.
        (DesiredWorkerState::Removed, Some(status)) => status
            .fragments
            .values()
            .all(|fragment| matches!(fragment, WorkerFragmentState::Terminated { .. }))
            .then_some(WorkerTransition::Removed),
    };
    transition.filter(|transition| transition.get_state() != worker.current_state)
}
