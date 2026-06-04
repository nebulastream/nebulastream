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

//! Property assertions a workload runs once the simulation has quiesced.
//!
//! An invariant compares a piece of model state against what the
//! coordinator and workers actually report, and panics on mismatch.
//! Keeping the contract this small lets each workload assemble its own
//! list of relevant invariants from short, reusable pieces.
//!
//! This module is the single home for the system invariants the simulation
//! checks. Each is a condition that must hold after the run has quiesced:
//!
//!  - Catalog fragments of active queries match the model and are Running.
//!  - Catalog fragments of dropped queries match the model and are Stopped.
//!  - Workers report the same active fragments as the model.
//!  - Catalog workers marked active match the model's active workers.
//!  - Catalog workers marked removed match the model's dropped workers.

#![cfg(madsim)]

use crate::harness::TestHarness;
use crate::model_state::ModelState;
use async_trait::async_trait;
use model::identifier::{QueryFragmentId, QueryId};
use model::query::GetQuery;
use model::query::query_fragment::{self, QueryFragmentState};
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{GetWorker, WorkerState};
use std::collections::HashSet;
use tracing::info;

#[async_trait(?Send)]
pub trait Invariant<Ctx> {
    fn name(&self) -> &str;
    async fn check(&self, harness: &TestHarness, ctx: &Ctx);
}

pub async fn check_invariants<Ctx>(
    invariants: &[&dyn Invariant<Ctx>],
    harness: &TestHarness,
    ctx: &Ctx,
) {
    for inv in invariants {
        inv.check(harness, ctx).await;
        info!("invariant {}: ok", inv.name());
    }
}

async fn catalog_fragments_of(
    harness: &TestHarness,
    qids: Vec<QueryId>,
) -> Vec<query_fragment::Model> {
    let StatementResult::Queries(queries) = harness
        .send(Statement::GetQuery(
            GetQuery::all().with_fragments().with_ids(qids),
        ))
        .await
        .unwrap()
    else {
        unreachable!();
    };
    queries.into_iter().flat_map(|(_, f)| f).collect()
}

async fn catalog_workers_in(state: WorkerState, harness: &TestHarness) -> HashSet<NetworkAddr> {
    let StatementResult::Workers(workers) = harness
        .send(Statement::GetWorker(
            GetWorker::all().with_current_state(state),
        ))
        .await
        .unwrap()
    else {
        unreachable!();
    };
    workers.into_iter().map(|w| w.host_addr).collect()
}

/// Catalog fragments of active queries match the model and are all live.
pub struct CatalogQueryFragmentsActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for CatalogQueryFragmentsActive {
    fn name(&self) -> &str {
        "catalog_fragments_active"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let fragments = catalog_fragments_of(harness, model.active_queries()).await;
        let ids: HashSet<QueryFragmentId> = fragments.iter().map(|f| f.id).collect();
        assert_eq!(
            ids,
            model.active_fragments(),
            "{}: ID mismatch",
            self.name()
        );
        // Started counts as live: starting a fragment is a single call that only marks it accepted,
        // and the next poll is what promotes it, so an active fragment sits in Started for up to one
        // poll interval. Both are non-terminal, which is what this invariant is really asserting.
        for f in &fragments {
            assert!(
                matches!(
                    f.current_state,
                    QueryFragmentState::Started | QueryFragmentState::Running
                ),
                "{}: fragment {} is {:?}, expect Started or Running",
                self.name(),
                f.id,
                f.current_state,
            );
        }
    }
}

/// Catalog fragments of dropped queries match the model and are all Stopped.
pub struct CatalogQueryFragmentsDropped;

#[async_trait(?Send)]
impl Invariant<ModelState> for CatalogQueryFragmentsDropped {
    fn name(&self) -> &str {
        "catalog_fragments_dropped"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let fragments = catalog_fragments_of(harness, model.dropped_queries()).await;
        let ids: HashSet<QueryFragmentId> = fragments.iter().map(|f| f.id).collect();
        assert_eq!(
            ids,
            model.dropped_fragments(),
            "{}: ID mismatch",
            self.name(),
        );
        for f in &fragments {
            assert_eq!(
                f.current_state,
                QueryFragmentState::Stopped,
                "{}: fragment {} is {:?}, expect Stopped",
                self.name(),
                f.id,
                f.current_state,
            );
        }
    }
}

/// Workers report the same set of active fragments as the model.
pub struct WorkerQueryFragmentsActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for WorkerQueryFragmentsActive {
    fn name(&self) -> &str {
        "worker_fragments_active"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let (active, _) = harness.worker_status(&model.active_workers()).await;
        let actual: HashSet<_> = active.into_iter().collect();
        let expected = model.active_fragments();
        let leaked: Vec<_> = actual.difference(&expected).collect();
        let missing: Vec<_> = expected.difference(&actual).collect();
        assert!(
            leaked.is_empty() && missing.is_empty(),
            "{}: {} leaked (active on worker but query dropped): {:?}, {} missing (expected active but not on worker): {:?}",
            self.name(),
            leaked.len(),
            leaked,
            missing.len(),
            missing,
        );
    }
}

/// Catalog workers marked Active match the model's active workers.
pub struct CatalogWorkersActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for CatalogWorkersActive {
    fn name(&self) -> &str {
        "catalog_workers_active"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let active = catalog_workers_in(WorkerState::Active, harness).await;
        assert_eq!(
            active,
            model.active_workers().into_iter().collect::<HashSet<_>>(),
            "{}: set mismatch",
            self.name(),
        );
    }
}

/// Catalog workers marked Removed match the model's dropped workers.
pub struct CatalogWorkersDropped;

#[async_trait(?Send)]
impl Invariant<ModelState> for CatalogWorkersDropped {
    fn name(&self) -> &str {
        "catalog_workers_dropped"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let removed = catalog_workers_in(WorkerState::Removed, harness).await;
        assert_eq!(
            &removed,
            model.dropped_workers(),
            "{}: set mismatch",
            self.name(),
        );
    }
}
