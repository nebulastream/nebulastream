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
    HEALTH_CHECK_MAX_MISSES, ORPHAN_CHECK_INTERVAL, POLL_INTERVAL, RECONNECT_INTERVAL,
    REMOVED_WORKER_STOP_DEADLINE,
};
use crate::query_fragment::{QueryFragmentTasks, Transport};
use crate::util::buggify::buggify;
use crate::util::reconcile::{Exit, Reconciler, reconcile_loop};
use crate::util::task_map::TaskMap;
use model::database::Database;
use model::identifier::QueryFragmentId;
use model::query::query_fragment;
use model::worker;
use model::worker::WorkerTransition;
use model::worker::endpoint::NetworkAddr;
use sea_orm::ActiveModelTrait;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
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
    /// The fragments that the worker runs and that became active at or after `after`.
    fn active_fragments(
        &self,
        link: &Self::Link,
        after: u64,
    ) -> impl Future<Output = anyhow::Result<ActiveFragments>> + Send;
}

/// A worker's list of active fragments. Times are the worker's clock in milliseconds since the Unix epoch.
pub(crate) struct ActiveFragments {
    /// Each fragment with the time since which the worker lists it, if the worker reports one.
    pub fragments: Vec<(QueryFragmentId, Option<u64>)>,
    /// When the worker took the list.
    pub until: u64,
}

/// The fragments that the worker runs although the catalog does not want them there.
fn orphans(active: &ActiveFragments, wanted: &HashSet<QueryFragmentId>) -> Vec<QueryFragmentId> {
    active
        .fragments
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| !wanted.contains(id))
        .collect()
}

/// The `after` of the next listing.
/// It must not pass a fragment that is still active, because the worker would stop listing that fragment,
/// and a fragment that the catalog stops wanting later would then run unseen.
fn next_after(active: &ActiveFragments, after: u64) -> u64 {
    if active.fragments.is_empty() {
        return active.until;
    }
    active
        .fragments
        .iter()
        .map(|(_, since)| *since)
        .min()
        .flatten()
        .unwrap_or(after)
}

/// Stops every fragment that the worker runs although the catalog does not want it there,
/// and returns the `after` of the next call.
/// A per-fragment task ends once its fragment is recorded as terminal,
/// so a start that takes effect after that point is only visible in the worker's own list.
/// Nothing is remembered between calls: a stop that fails is repeated because the fragment is listed again.
pub(crate) async fn stop_orphans<B: Backend>(
    backend: &B,
    link: &B::Link,
    db: &Database,
    addr: &NetworkAddr,
    after: u64,
) -> u64 {
    let active = match backend.active_fragments(link, after).await {
        Ok(active) => active,
        Err(err) => {
            warn!(error = %format_args!("{err:#}"), "could not list the active fragments");
            return after;
        }
    };
    // The worker is read before the catalog.
    // A fragment that was active then and is not wanted now has a terminal row or none, which is final.
    // In the other order, a fragment created and started between the two reads would be stopped.
    let wanted: HashSet<QueryFragmentId> = match query_fragment::Entity::actionable(db, addr).await
    {
        Ok(rows) => rows.iter().map(|row| row.id).collect(),
        Err(err) => {
            warn!(error = %err, "could not load the wanted fragments");
            return after;
        }
    };
    let orphans = orphans(&active, &wanted);
    if !orphans.is_empty() {
        for id in &orphans {
            warn!(fragment = %id, "stopping a fragment that the catalog does not want on this worker");
        }
        let transport = backend.transport(link, addr);
        futures::future::join_all(orphans.iter().map(|id| transport.stop(*id))).await;
    }
    next_after(&active, after)
}

pub(crate) async fn stop_removed_fragments<B: Backend>(
    backend: &B,
    db: &Database,
    addr: &NetworkAddr,
) {
    let ids = match query_fragment::Entity::failed_by_worker_removal(db, addr).await {
        Ok(ids) => ids,
        Err(err) => {
            warn!(%addr, error = %err, "could not list the fragments of removed worker");
            return;
        }
    };
    if ids.is_empty() {
        return;
    }
    let stop_all = async {
        let link = backend.connect(addr).await?;
        let transport = backend.transport(&link, addr);
        futures::future::join_all(ids.iter().map(|id| transport.stop(*id))).await;
        anyhow::Ok(())
    };
    match tokio::time::timeout(REMOVED_WORKER_STOP_DEADLINE, stop_all).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(%addr, error = %format_args!("{err:#}"), "could not reach removed worker")
        }
        Err(_) => warn!(%addr, "gave up stopping the fragments of removed worker"),
    }
}

pub(crate) struct WorkerTask<B: Backend> {
    worker: worker::Model,
    backend: B,
    link: Option<B::Link>,
    missed_probes: u32,
    db: Database,
    intent_rx: watch::Receiver<()>,
    state_tx: watch::Sender<()>,
    fragments: QueryFragmentTasks,
    last_orphan_check: Option<Instant>,
    listed_after: u64,
}

impl<B: Backend> WorkerTask<B> {
    pub(crate) fn new(
        worker: worker::Model,
        backend: B,
        db: Database,
        intent_rx: watch::Receiver<()>,
        state_tx: watch::Sender<()>,
    ) -> Self {
        Self {
            worker,
            backend,
            link: None,
            missed_probes: 0,
            db,
            intent_rx,
            state_tx,
            fragments: QueryFragmentTasks::new(),
            last_orphan_check: None,
            listed_after: 0,
        }
    }

    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        let mut unreachable = false;
        loop {
            match self.backend.connect(&self.worker.host_addr).await {
                Ok(link) => {
                    self.link = Some(link);
                    self.missed_probes = 0;
                    self.last_orphan_check = None;
                    self.listed_after = 0;
                }
                Err(err) => {
                    if !unreachable {
                        error!(error = %format_args!("{err:#}"), "connect failed");
                        self.mark(WorkerTransition::Unreachable).await?;
                        unreachable = true;
                    }
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
                    unreachable = true;
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        }
    }

    async fn mark(&self, transition: WorkerTransition) -> anyhow::Result<()> {
        if buggify() {
            return Err(anyhow::anyhow!("buggify: worker update failed"));
        }
        let mut update: worker::ActiveModel = self.worker.clone().into();
        update.apply_transition(transition);
        update
            .update(&self.db)
            .await
            .inspect_err(|e| warn!(error = %e, "failed to update worker"))?;
        Ok(())
    }
}

impl<B: Backend> Reconciler for WorkerTask<B> {
    type Key = QueryFragmentId;

    fn tasks(&mut self) -> &mut TaskMap<QueryFragmentId> {
        self.fragments.tasks()
    }

    async fn reconcile(&mut self) {
        if buggify() {
            return;
        }
        let Some(link) = &self.link else {
            return;
        };
        let (backend, addr) = (&self.backend, &self.worker.host_addr);
        self.fragments
            .reconcile_from_catalog(&self.db, addr, &self.state_tx, || {
                backend.transport(link, addr)
            })
            .await;
        if self
            .last_orphan_check
            .is_none_or(|at| at.elapsed() >= ORPHAN_CHECK_INTERVAL)
        {
            self.last_orphan_check = Some(Instant::now());
            self.listed_after =
                stop_orphans(backend, link, &self.db, addr, self.listed_after).await;
        }
    }

    async fn alive(&mut self) -> bool {
        let Some(link) = &self.link else {
            return false;
        };
        if self.backend.healthy(link).await {
            self.missed_probes = 0;
            return true;
        }
        self.missed_probes += 1;
        self.missed_probes < HEALTH_CHECK_MAX_MISSES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active(fragments: &[(i64, Option<u64>)], until: u64) -> ActiveFragments {
        ActiveFragments {
            fragments: fragments
                .iter()
                .map(|(id, since)| (QueryFragmentId::new(*id), *since))
                .collect(),
            until,
        }
    }

    #[test]
    fn only_fragments_the_catalog_does_not_want_are_orphans() {
        let wanted = HashSet::from([QueryFragmentId::new(1)]);
        let listed = active(&[(1, Some(5)), (2, Some(6)), (3, None)], 10);
        assert_eq!(
            orphans(&listed, &wanted),
            vec![QueryFragmentId::new(2), QueryFragmentId::new(3)]
        );
        assert!(orphans(&active(&[], 10), &wanted).is_empty());
    }

    #[test]
    fn the_next_listing_starts_at_the_worker_time_when_nothing_is_active() {
        assert_eq!(next_after(&active(&[], 10), 3), 10);
    }

    #[test]
    fn the_next_listing_does_not_pass_the_oldest_active_fragment() {
        assert_eq!(next_after(&active(&[(1, Some(7)), (2, Some(5))], 10), 3), 5);
    }

    #[test]
    fn the_next_listing_does_not_move_while_an_active_fragment_has_no_time() {
        assert_eq!(next_after(&active(&[(1, Some(7)), (2, None)], 10), 3), 3);
    }
}
