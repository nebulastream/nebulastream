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
use crate::calls::{Call, CallError, InFlightCalls, Transport};
use crate::config::{MAX_FAILED_STATUS_REQUESTS, OBSERVATION_WRITE_INTERVAL};
use crate::reconcile::{Correction, FragmentStatus, WorkerStatus, reconcile};
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use model::database::Database;
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{self, ManagedFragment, QueryFragmentTransition};
use model::worker::endpoint::NetworkAddr;
use model::worker::{self, DesiredWorkerState, WorkerState, WorkerTransition};
use sea_orm::{ActiveModelTrait, DbErr, EntityTrait, Unchanged};
use std::mem;
use std::ops::ControlFlow::{self, Break, Continue};
use std::time::Duration;
use tokio::sync::{OnceCell, watch};
use tokio::time::{Instant, interval};
use tokio_retry2::strategy::FixedInterval;
use tokio_retry2::{Retry, RetryError};
use tracing::{debug, error, info, warn};

type Connect<T> = Box<dyn Fn() -> BoxFuture<'static, anyhow::Result<T>> + Send + Sync>;

/// The transport from a successful connect is kept for the life of the loop, so an in-process worker is launched once.
struct Connection<T> {
    connect: Connect<T>,
    transport: OnceCell<T>,
}

impl<T: Transport> Connection<T> {
    /// Repeats a failed request after `retry_delay`, and returns `None` once several requests in a row failed.
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
            let transport = self
                .transport
                .get_or_try_init(|| (self.connect)())
                .await
                .context("connect failed")?;
            let response = transport.get_status(after).await?;
            anyhow::Ok((transport.clone(), response.into()))
        };
        status.await.map_err(|err| {
            warn!(error = %format_args!("{err:#}"), "status request failed");
            RetryError::transient(err)
        })
    }
}

/// Keeps one worker in line with the catalog.
/// Each round reads the worker's status and its managed rows, and applies the corrections that the policy derives from them.
/// Only what the status and the catalog cannot tell is kept between rounds.
pub(crate) struct WorkerLoop<T> {
    addr: NetworkAddr,
    connection: Connection<T>,
    poll_interval: Duration,
    db: Database,
    state_tx: watch::Sender<()>,
    /// The next status lists the fragments that terminated at or after this worker time.
    after: u64,
    observed_at: Option<Instant>,
}

impl<T: Transport> WorkerLoop<T> {
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
            addr,
            connection: Connection {
                connect: Box::new(move || connect().boxed()),
                transport: OnceCell::new(),
            },
            poll_interval,
            db,
            state_tx,
            after: 0,
            observed_at: None,
        }
    }

    /// Returns once the catalog-change sender is dropped, or once the worker is recorded as removed.
    pub(crate) async fn run(mut self, mut intent_rx: watch::Receiver<()>) {
        let mut poll = interval(self.poll_interval);
        let mut in_flight = InFlightCalls::new(self.db.clone(), self.poll_interval);
        loop {
            tokio::select! {
                changed = intent_rx.changed() => if changed.is_err() { return; },
                _ = poll.tick() => {}
                Some((call, result)) = in_flight.join_next() => match call {
                    Call::Start(id) => {
                        match result {
                            Ok(()) => {}
                            // An earlier start reached the worker, but its reply was lost.
                            // The status did not list the fragment, so it may have terminated before the time from which the status listed terminations.
                            // From zero, the next status lists every termination again.
                            Err(CallError::AlreadyExists) => self.after = 0,
                            Err(CallError::Transient(err) | CallError::Permanent(err)) => {
                                error!(fragment = %id, error = %err, "start failed for good");
                                let _ = self
                                    .record(id, QueryFragmentTransition::failed_now(err))
                                    .await;
                            }
                        }
                    }
                    // The next round that still finds the fragment listed repeats a failed stop.
                    // A round after every stop would repeat it without pause while the worker winds the fragment down.
                    Call::Stop(_) => continue,
                },
            }
            poll.reset();
            if self.run_round(&mut in_flight).await.is_break() {
                return;
            }
        }
    }

    async fn run_round(&mut self, calls: &mut InFlightCalls) -> ControlFlow<()> {
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
        // Read the status before the managed rows.
        // Then an active fragment without a managed row has a terminal row or none, and neither changes again.
        // In the other order, a fragment that was created and started between the two reads would be stopped.
        let status = self
            .connection
            .get_status(self.after, self.poll_interval)
            .await;
        if let Some(transition) = transition(&worker, status.as_ref().map(|(_, status)| status)) {
            self.record_worker(&worker, transition).await;
        }
        if let Some((transport, status)) = status {
            self.reconcile_fragments(&transport, &status, calls).await;
        }
        Continue(())
    }

    async fn reconcile_fragments(
        &mut self,
        transport: &T,
        status: &WorkerStatus,
        calls: &mut InFlightCalls,
    ) {
        // The removal of a worker fails all its fragments in the catalog, so a removed worker has none managed.
        let Ok(managed) = query_fragment::Entity::get_managed(&self.db, &self.addr)
            .await
            .inspect_err(|err| warn!(error = %err, "could not load the managed fragments"))
        else {
            return;
        };
        let previous = mem::replace(&mut self.after, status.until);
        for correction in reconcile(status, &managed) {
            match correction {
                Correction::Start(id) => calls.spawn(transport, Call::Start(id)),
                Correction::Stop(id) => calls.spawn(transport, Call::Stop(id)),
                // A termination that was not recorded must stay in the next status.
                Correction::Record(id, transition) => {
                    if self.record(id, transition).await.is_err() {
                        self.after = previous;
                    }
                }
            }
        }
        self.mark_observed(status, &managed).await;
    }

    /// A failed write is logged and not repeated here,
    /// because a later round derives the write again from its own status and the row's state at that time.
    async fn record(
        &self,
        id: QueryFragmentId,
        transition: QueryFragmentTransition,
    ) -> Result<(), DbErr> {
        debug!(fragment = %id, ?transition, "record");
        let mut update = query_fragment::ActiveModel {
            id: Unchanged(id),
            ..Default::default()
        };
        update.apply_transition(transition);
        let written = if buggify() {
            Err(DbErr::Custom("buggify: write failed".to_string()))
        } else {
            update.update(&self.db).await
        };
        if let Err(err) = written {
            warn!(fragment = %id, error = %err, "could not record the fragment state");
            return Err(err);
        }
        // The simulation drops some wake-ups, so it checks that the periodic poll alone advances every watcher.
        if !buggify() {
            let _ = self.state_tx.send(());
        }
        Ok(())
    }

    /// Refreshes the time at which the worker last listed each running fragment.
    /// A request that waits for fresh state compares this time with its submission.
    async fn mark_observed(&mut self, status: &WorkerStatus, managed: &[ManagedFragment]) {
        if self
            .observed_at
            .is_some_and(|at| at.elapsed() < OBSERVATION_WRITE_INTERVAL)
        {
            return;
        }
        let running: Vec<QueryFragmentId> = managed
            .iter()
            .map(|row| row.id)
            .filter(|id| {
                matches!(
                    status.fragments.get(id),
                    Some(FragmentStatus::Running { .. })
                )
            })
            .collect();
        if running.is_empty() {
            return;
        }
        self.observed_at = Some(Instant::now());
        if let Err(err) = query_fragment::Entity::mark_observed(&self.db, running).await {
            warn!(error = %err, "could not mark the running fragments as observed");
            return;
        }
        let _ = self.state_tx.send(());
    }

    /// A failed write is logged, and a later round derives the transition again.
    async fn record_worker(&self, worker: &worker::Model, transition: WorkerTransition) {
        if worker.current_state == transition.get_state() {
            return;
        }
        let mut update: worker::ActiveModel = worker.clone().into();
        update.apply_transition(transition);
        if buggify() {
            warn!("buggify: worker update failed");
            return;
        }
        info!(?transition, "worker");
        if let Err(err) = update.update(&self.db).await {
            warn!(error = %err, "failed to update worker");
            return;
        }
        // A wait on a fragment of this worker can depend on the worker's state.
        let _ = self.state_tx.send(());
    }
}

fn transition(worker: &worker::Model, status: Option<&WorkerStatus>) -> Option<WorkerTransition> {
    match (worker.desired_state, status) {
        (DesiredWorkerState::Active, None) => Some(WorkerTransition::Unreachable),
        (DesiredWorkerState::Active, Some(_)) => Some(WorkerTransition::Active),
        // A removed worker that does not answer is given up, and its fragments may keep running.
        (DesiredWorkerState::Removed, None) => Some(WorkerTransition::Removed),
        (DesiredWorkerState::Removed, Some(status)) => status
            .fragments
            .values()
            .all(|fragment| matches!(fragment, FragmentStatus::Terminated { .. }))
            .then_some(WorkerTransition::Removed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::worker_rpc_service::WorkerStatusResponse;
    use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
    use crate::remote::worker_rpc_service::worker_status_response::ActiveQuery;
    use futures::future::ready;
    use model::worker::WorkerState;
    use sea_orm::ConnectionTrait;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Fake {
        active: Arc<Mutex<Vec<i64>>>,
        stops: Arc<Mutex<Vec<QueryFragmentId>>>,
    }

    impl Transport for Fake {
        async fn start(&self, _: SerializableQueryPlan) -> Result<(), CallError> {
            Ok(())
        }

        async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
            self.stops.lock().unwrap().push(id);
            Ok(())
        }

        async fn get_status(&self, _: u64) -> anyhow::Result<WorkerStatusResponse> {
            let active = self.active.lock().unwrap().clone();
            let active_queries = active.into_iter().map(|id| ActiveQuery {
                query_id: Some(SerializableQueryId::from_fragment_id(id)),
                started_unix_timestamp_in_milli_seconds: Some(1_000),
            });
            Ok(WorkerStatusResponse {
                active_queries: active_queries.collect(),
                ..Default::default()
            })
        }
    }

    fn addr() -> NetworkAddr {
        NetworkAddr::new("worker", 8080).unwrap()
    }

    async fn build_loop(
        desired_state: &str,
        fake: Option<Fake>,
    ) -> (WorkerLoop<Fake>, InFlightCalls, watch::Receiver<()>) {
        let db = Database::for_test().await;
        db.execute_unprepared(&format!(
            "INSERT INTO worker (host_addr, data_addr, desired_state) \
             VALUES ('worker:8080', 'worker:9090', '{desired_state}')"
        ))
        .await
        .unwrap();
        let connect = move || ready(fake.clone().ok_or_else(|| anyhow::anyhow!("down")));
        let (state_tx, state_rx) = watch::channel(());
        let poll_interval = Duration::from_millis(1);
        let calls = InFlightCalls::new(db.clone(), poll_interval);
        let worker_loop = WorkerLoop::new(addr(), connect, poll_interval, db, state_tx);
        (worker_loop, calls, state_rx)
    }

    async fn get_state(worker_loop: &WorkerLoop<Fake>) -> WorkerState {
        worker::Entity::find_by_id(addr())
            .one(&worker_loop.db)
            .await
            .unwrap()
            .unwrap()
            .current_state
    }

    #[tokio::test]
    async fn active_once() {
        let (mut worker_loop, mut calls, mut state_rx) =
            build_loop("Active", Some(Fake::default())).await;
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert_eq!(get_state(&worker_loop).await, WorkerState::Active);
        assert!(state_rx.has_changed().unwrap());
        state_rx.mark_unchanged();
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert!(!state_rx.has_changed().unwrap());
    }

    #[tokio::test]
    async fn observed_notifies() {
        let fake = Fake::default();
        let (mut worker_loop, mut calls, mut state_rx) =
            build_loop("Active", Some(fake.clone())).await;
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        state_rx.mark_unchanged();
        worker_loop
            .db
            .execute_unprepared(
                "INSERT INTO query (sql, global_plan) VALUES ('', x''); \
                 INSERT INTO query_fragment (query_id, host_addr, plan, num_operators, has_source, current_state) \
                 VALUES (1, 'worker:8080', x'', 0, false, 'Running')",
            )
            .await
            .unwrap();
        fake.active.lock().unwrap().push(1);
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert!(state_rx.has_changed().unwrap());
    }

    #[tokio::test]
    async fn unreachable() {
        let (mut worker_loop, mut calls, _state_rx) = build_loop("Active", None).await;
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert_eq!(get_state(&worker_loop).await, WorkerState::Unreachable);
    }

    #[tokio::test]
    async fn removed_once_idle() {
        let fake = Fake::default();
        fake.active.lock().unwrap().push(1);
        let (mut worker_loop, mut calls, _state_rx) =
            build_loop("Removed", Some(fake.clone())).await;
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert!(calls.join_next().await.is_some());
        assert_eq!(*fake.stops.lock().unwrap(), vec![QueryFragmentId::new(1)]);
        assert_eq!(get_state(&worker_loop).await, WorkerState::Pending);
        fake.active.lock().unwrap().clear();
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert_eq!(get_state(&worker_loop).await, WorkerState::Removed);
        assert!(worker_loop.run_round(&mut calls).await.is_break());
    }

    #[tokio::test]
    async fn removed_unreachable() {
        let (mut worker_loop, mut calls, _state_rx) = build_loop("Removed", None).await;
        assert!(worker_loop.run_round(&mut calls).await.is_continue());
        assert_eq!(get_state(&worker_loop).await, WorkerState::Removed);
        assert!(worker_loop.run_round(&mut calls).await.is_break());
    }
}
