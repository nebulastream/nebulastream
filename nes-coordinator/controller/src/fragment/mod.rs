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
use crate::config::OBSERVATION_WRITE_INTERVAL;
use crate::status::{WorkerFragmentState, WorkerStatus};
use crate::worker_client::{CallError, WorkerClient};
use calls::{Call, Cancelled, InFlightCalls};
use model::database::Database;
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{self, ManagedFragment, QueryFragmentTransition};
use model::worker::endpoint::NetworkAddr;
use sea_orm::{ActiveModelTrait, DbErr, Unchanged};
use std::mem;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, error, warn};
use transition::{Correction, reconcile};

pub(crate) mod calls;
mod transition;

/// Keeps the fragments of one worker in line with the catalog.
pub(crate) struct FragmentReconciler {
    addr: NetworkAddr,
    db: Database,
    state_tx: watch::Sender<()>,
    /// The `until` of the last status with every transition recorded, e.g.:
    ///     after 0,   status until 100, every record succeeds        -> after 100
    ///     after 100, status until 205, a record fails               -> after 100
    ///     a start finds the fragment already accepted by the worker -> after 0
    after: u64,
    observed_at: Option<Instant>,
    in_flight: InFlightCalls,
}

impl FragmentReconciler {
    pub(crate) fn new(
        addr: NetworkAddr,
        db: Database,
        state_tx: watch::Sender<()>,
        retry_delay: Duration,
    ) -> Self {
        Self {
            addr,
            in_flight: InFlightCalls::new(db.clone(), retry_delay),
            db,
            state_tx,
            after: 0,
            observed_at: None,
        }
    }

    pub(crate) const fn get_after(&self) -> u64 {
        self.after
    }

    /// Applies the corrections that the policy derives from the status and the managed rows.
    /// Reads the managed rows itself, so the status must be read before the call.
    /// Starts and stops run in the background, and their results come from `join_next`.
    pub(crate) async fn reconcile<T: WorkerClient>(&mut self, client: &T, status: &WorkerStatus) {
        let Ok(managed) = query_fragment::Entity::get_managed(&self.db, &self.addr)
            .await
            .inspect_err(|err| warn!(error = %err, "could not load the managed fragments"))
        else {
            return;
        };
        let previous = mem::replace(&mut self.after, status.until);
        for correction in reconcile(status, &managed) {
            match correction {
                Correction::Start(id) => self.in_flight.spawn(client, Call::Start(id)),
                Correction::Stop(id) => self.in_flight.spawn(client, Call::Stop(id)),
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

    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<(Call, Result<(), CallError>), Cancelled>> {
        self.in_flight.join_next().await
    }

    /// Fails the fragment in the catalog, unless the start succeeded or an earlier start already reached the worker.
    pub(crate) async fn finalize_start(
        &mut self,
        id: QueryFragmentId,
        result: Result<(), CallError>,
    ) {
        match result {
            Ok(()) => {}
            // An earlier start reached the worker, but its reply was lost.
            // The status did not list the fragment, so it may have terminated before the time from which the status listed terminations.
            // From zero, the next status lists every termination again.
            Err(CallError::AlreadyExists) => self.after = 0,
            Err(CallError::Transient(err) | CallError::Permanent(err)) => {
                error!(fragment = %id, error = %err, "start failed for good");
                // A status never lists a failed start, so a lost write leads to a new start in the next round.
                let _ = self
                    .record(id, QueryFragmentTransition::failed_now(err))
                    .await;
            }
        }
    }

    /// A failed write is logged and not repeated here, because each caller has its own way to repeat it.
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
        written.inspect_err(|err| {
            warn!(fragment = %id, error = %err, "could not record the fragment state");
        })?;
        // The simulation drops some wake-ups, so it checks that the periodic poll alone advances every watcher.
        if !buggify() {
            let _ = self.state_tx.send(());
        }
        Ok(())
    }

    /// Records the current time as the last observation of each running fragment,
    /// at most once per observation write interval.
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
                    Some(WorkerFragmentState::Running { .. })
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
}
