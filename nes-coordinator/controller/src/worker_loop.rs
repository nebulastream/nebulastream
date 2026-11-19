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

use crate::config::{
    HEALTH_CHECK_MAX_MISSES, MAX_CONSECUTIVE_FAILURES, OBSERVATION_WRITE_INTERVAL,
};
use crate::remote::worker_rpc_service::WorkerStatusResponse;
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use crate::report::{Action, Reported, WorkerReport, decide};
use crate::util::buggify::buggify;
use chrono::Utc;
use futures::StreamExt;
use futures::future::{BoxFuture, join_all};
use futures::stream::FuturesUnordered;
use model::database::Database;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    self, ActionableFragment, QueryFragmentError, QueryFragmentTransition,
};
use model::worker::endpoint::NetworkAddr;
use model::worker::{self, DesiredWorkerState, WorkerTransition};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, DbErr, EntityTrait, QueryFilter, Unchanged};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

/// A failed worker call, classified by what the loop does next.
pub(crate) enum CallError {
    /// The worker already accepted a start for the fragment's id.
    AlreadyExists,
    /// Repeating the call may succeed.
    Transient(QueryFragmentError),
    /// Repeating the call cannot succeed, so a failed start fails the fragment.
    Failed(QueryFragmentError),
}

/// The calls that a worker loop makes to its worker.
pub(crate) trait Transport: Clone + Send + Sync + 'static {
    fn start(
        &self,
        plan: SerializableQueryPlan,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Result<(), CallError>> + Send;

    /// Lists every active fragment, and the fragments that terminated at or after `after`, in the worker's milliseconds.
    fn report(
        &self,
        after: u64,
    ) -> impl Future<Output = anyhow::Result<WorkerStatusResponse>> + Send;
}

type StartResult = (QueryFragmentId, Result<(), CallError>);

/// Keeps one worker in line with the catalog.
/// Each pass reads the worker's report and its wanted rows, and applies the actions that the policy derives from them.
/// Only what the report and the catalog cannot tell is kept between passes.
pub(crate) struct WorkerTask<T, C> {
    addr: NetworkAddr,
    connect: C,
    transport: Option<T>,
    interval: Duration,
    db: Database,
    state_tx: watch::Sender<()>,
    /// The next report lists the fragments that terminated at or after this worker time.
    after: u64,
    /// Failed reports in a row, including failed connects.
    missed: u32,
    /// A fragment is never started again while an earlier start for it is in flight.
    starting: HashSet<QueryFragmentId>,
    /// The starts that the last pass decided on, which the loop then spawns.
    new_starts: Vec<QueryFragmentId>,
    failed_starts: HashMap<QueryFragmentId, u32>,
    observed_at: Option<Instant>,
}

impl<T, C, F> WorkerTask<T, C>
where
    T: Transport,
    C: Fn() -> F + Send + Sync,
    F: Future<Output = anyhow::Result<T>> + Send,
{
    /// The transport from a successful connect is kept for the life of the task,
    /// so an in-process worker is created once.
    pub(crate) fn new(
        addr: NetworkAddr,
        connect: C,
        interval: Duration,
        db: Database,
        state_tx: watch::Sender<()>,
    ) -> Self {
        Self {
            addr,
            connect,
            transport: None,
            interval,
            db,
            state_tx,
            after: 0,
            missed: 0,
            starting: HashSet::new(),
            new_starts: Vec::new(),
            failed_starts: HashMap::new(),
            observed_at: None,
        }
    }

    /// Returns once the catalog-change sender is dropped, or once the worker is recorded as removed.
    pub(crate) async fn run(mut self, mut intent_rx: watch::Receiver<()>) {
        let mut poll = tokio::time::interval(self.interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Starts run as their own tasks, so a long compile delays neither the reports nor the stops.
        let mut starts = FuturesUnordered::new();
        loop {
            poll.reset();
            if self.pass().await.is_break() {
                return;
            }
            for id in std::mem::take(&mut self.new_starts) {
                starts.push(self.spawn_start(id));
            }
            loop {
                tokio::select! {
                    change = intent_rx.changed() => {
                        if change.is_err() { return; }
                        // An unreachable worker is asked again only on the poll tick,
                        // so a burst of catalog changes does not use up its missed reports at once.
                        if self.missed == 0 { break; }
                    }
                    _ = poll.tick() => break,
                    Some((id, result)) = starts.next() => {
                        if self.finish_start(id, result).await { break; }
                    }
                }
            }
        }
    }

    async fn pass(&mut self) -> ControlFlow<()> {
        if buggify() {
            return ControlFlow::Continue(());
        }
        let worker = match worker::Entity::find_by_id(self.addr.clone())
            .one(&self.db)
            .await
        {
            Ok(Some(worker)) => worker,
            Ok(None) => return ControlFlow::Break(()),
            Err(err) => {
                warn!(error = %err, "could not read the worker");
                return ControlFlow::Continue(());
            }
        };
        let removed = worker.desired_state == DesiredWorkerState::Removed;
        // Read the report before the wanted rows.
        // Then an active fragment without a wanted row has a terminal row or none, and neither changes again.
        // In the other order, a fragment that was created and started between the two reads would be stopped.
        let Some(report) = self.report().await else {
            self.missed += 1;
            if self.missed < HEALTH_CHECK_MAX_MISSES {
                return ControlFlow::Continue(());
            }
            // A removed worker that does not report is given up, and its fragments may keep running.
            if removed {
                return self.mark_removed(&worker).await;
            }
            self.mark(&worker, WorkerTransition::Unreachable).await;
            return ControlFlow::Continue(());
        };
        self.missed = 0;
        let live = |reported: &Reported| !matches!(reported, Reported::Terminated { .. });
        if removed && !report.fragments.values().any(live) {
            return self.mark_removed(&worker).await;
        }
        if !removed {
            self.mark(&worker, WorkerTransition::Active).await;
        }
        // The removal of a worker fails all its fragments in the catalog, so a removed worker has none wanted.
        let wanted = match query_fragment::Entity::actionable(&self.db, &self.addr).await {
            Ok(wanted) => wanted,
            Err(err) => {
                warn!(error = %err, "could not load the wanted fragments");
                return ControlFlow::Continue(());
            }
        };
        self.failed_starts
            .retain(|id, _| wanted.iter().any(|row| row.id == *id));
        let mut stops = Vec::new();
        let mut recorded_all = true;
        for action in decide(&report, &wanted) {
            match action {
                Action::Start(id) => {
                    if self.starting.insert(id) {
                        self.new_starts.push(id);
                    }
                }
                Action::Stop(id) => stops.push(id),
                Action::Record(id, transition) => recorded_all &= self.record(id, transition).await,
            }
        }
        if let Some(transport) = &self.transport {
            // The transport logs a failed stop, and the next report shows whether it took effect.
            join_all(stops.into_iter().map(|id| transport.stop(id))).await;
        }
        // A termination that was not recorded must stay in the next report.
        if recorded_all {
            self.after = report.until;
        }
        self.mark_observed(&report, &wanted).await;
        ControlFlow::Continue(())
    }

    async fn report(&mut self) -> Option<WorkerReport> {
        if self.transport.is_none() {
            match (self.connect)().await {
                Ok(transport) => self.transport = Some(transport),
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "connect failed");
                    return None;
                }
            }
        }
        match self.transport.as_ref()?.report(self.after).await {
            Ok(response) => Some(response.into()),
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), "report failed");
                None
            }
        }
    }

    fn spawn_start(&self, id: QueryFragmentId) -> BoxFuture<'static, StartResult> {
        let task = tokio::spawn(start(self.transport.clone(), self.db.clone(), id));
        Box::pin(async move {
            let result = task.await.unwrap_or_else(|err| {
                Err(CallError::Failed(QueryFragmentError::Internal(
                    CodedError::new(ErrorCode::UnknownException, err),
                )))
            });
            (id, result)
        })
    }

    /// Returns whether the next pass should run now.
    /// After a retryable failure it waits for the poll tick, so a failing start is not repeated in a tight loop.
    async fn finish_start(&mut self, id: QueryFragmentId, result: Result<(), CallError>) -> bool {
        self.starting.remove(&id);
        let error = match result {
            Ok(()) => None,
            // An earlier start whose reply was lost reached the worker.
            // The report should have listed the fragment, so it may have missed its termination;
            // the next report lists every termination again.
            Err(CallError::AlreadyExists) => {
                self.after = 0;
                None
            }
            Err(CallError::Transient(err)) => {
                warn!(fragment = %id, error = %err, "start failed");
                let failures = self.failed_starts.entry(id).or_default();
                *failures += 1;
                if *failures < MAX_CONSECUTIVE_FAILURES {
                    return false;
                }
                Some(QueryFragmentError::Transport {
                    msg: format!("retry budget exhausted, last error: {err}"),
                })
            }
            Err(CallError::Failed(err)) => Some(err),
        };
        self.failed_starts.remove(&id);
        if let Some(err) = error {
            error!(fragment = %id, error = %err, "start failed for good");
            self.record(id, QueryFragmentTransition::failed_now(err))
                .await;
        }
        true
    }

    /// Returns whether the write succeeded.
    /// A failed write is logged and repeated by a later pass,
    /// because the row's state, or another writer's change to it, decides again.
    async fn record(&self, id: QueryFragmentId, transition: QueryFragmentTransition) -> bool {
        debug!(fragment = %id, ?transition, "record");
        let mut update = query_fragment::ActiveModel {
            id: Unchanged(id),
            ..Default::default()
        };
        update.apply_transition(transition);
        let written = if buggify() {
            Err(DbErr::Custom("buggify: write failed".to_string()))
        } else {
            update.update(&self.db).await.map(|_| ())
        };
        if let Err(err) = written {
            warn!(fragment = %id, error = %err, "could not record the fragment state");
            return false;
        }
        // The simulation drops some wake-ups, so it checks that the periodic poll alone advances every watcher.
        if !buggify() {
            let _ = self.state_tx.send(());
        }
        true
    }

    /// Refreshes the time at which the worker last reported each running fragment.
    /// A request that waits for fresh state compares this time with its submission.
    async fn mark_observed(&mut self, report: &WorkerReport, wanted: &[ActionableFragment]) {
        if self
            .observed_at
            .is_some_and(|at| at.elapsed() < OBSERVATION_WRITE_INTERVAL)
        {
            return;
        }
        let running = wanted
            .iter()
            .map(|row| row.id)
            .filter(|id| matches!(report.fragments.get(id), Some(Reported::Running { .. })));
        let running: Vec<QueryFragmentId> = running.collect();
        if running.is_empty() {
            return;
        }
        self.observed_at = Some(Instant::now());
        let update = query_fragment::Entity::update_many()
            .col_expr(
                query_fragment::Column::LastObservedAt,
                Expr::value(Utc::now()),
            )
            .filter(query_fragment::Column::Id.is_in(running))
            .exec(&self.db)
            .await;
        if let Err(err) = update {
            warn!(error = %err, "could not mark the running fragments as observed");
        }
    }

    async fn mark_removed(&self, worker: &worker::Model) -> ControlFlow<()> {
        if self.mark(worker, WorkerTransition::Removed).await {
            info!("removed");
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Returns whether the worker's row has the given state afterwards.
    async fn mark(&self, worker: &worker::Model, transition: WorkerTransition) -> bool {
        if worker.current_state == transition.state() {
            return true;
        }
        let mut update: worker::ActiveModel = worker.clone().into();
        update.apply_transition(transition);
        if buggify() {
            warn!("buggify: worker update failed");
            return false;
        }
        info!(?transition, "worker");
        update
            .update(&self.db)
            .await
            .inspect_err(|err| warn!(error = %err, "failed to update worker"))
            .is_ok()
    }
}

/// A deleted row has nothing to start.
async fn start<T: Transport>(
    transport: Option<T>,
    db: Database,
    id: QueryFragmentId,
) -> Result<(), CallError> {
    let transient = |msg: String| CallError::Transient(QueryFragmentError::Transport { msg });
    let transport = transport.ok_or_else(|| transient("not connected".to_string()))?;
    let row = query_fragment::Entity::find_by_id(id)
        .one(&db)
        .await
        .map_err(|err| transient(err.to_string()))?;
    let Some(row) = row else {
        return Ok(());
    };
    transport.start(plan_for(id, &row.plan)?).await
}

// The stored plan has no id, and the worker identifies the plan by the fragment's id.
fn plan_for(id: QueryFragmentId, plan: &[u8]) -> Result<SerializableQueryPlan, CallError> {
    let mut query_plan: SerializableQueryPlan = prost::Message::decode(plan).map_err(|e| {
        CallError::Failed(QueryFragmentError::Internal(CodedError::new(
            ErrorCode::UnknownException,
            format!("failed to decode stored query plan: {e}"),
        )))
    })?;
    query_plan.query_id = Some(SerializableQueryId::from_fragment_id(*id));
    Ok(query_plan)
}
