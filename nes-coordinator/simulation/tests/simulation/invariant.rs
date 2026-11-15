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

//! Assertions that a trial runs once the simulation has quiesced.
//!
//! An invariant compares one piece of model state against what the coordinator or the workers report,
//! and panics on mismatch.

use crate::harness::Harness;
use crate::model_state::ModelState;
use model::identifier::{QueryFragmentId, QueryId};
use model::query::query_fragment::{self, QueryFragmentState};
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryWithFragments};
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{GetWorker, WorkerState};
use std::collections::HashSet;
use tracing::info;

pub async fn check_all(harness: &Harness, model: &ModelState, min_failed_queries: usize) {
    check_active_catalog_fragments(harness, model).await;
    check_dropped_catalog_queries(harness, model).await;
    check_failed_catalog_queries(harness, model).await;
    check_active_worker_fragments(harness, model).await;
    check_active_catalog_workers(harness, model).await;
    check_dropped_catalog_workers(harness, model).await;
    check_failed_query_count(model, min_failed_queries);
    info!("all invariants hold");
}

async fn get_catalog_queries(harness: &Harness, qids: Vec<QueryId>) -> Vec<QueryWithFragments> {
    let StatementResult::Queries(queries) = harness
        .send(ParsedStatement::GetQuery(
            GetQuery::all().with_fragments().with_ids(qids),
        ))
        .await
        .unwrap()
    else {
        unreachable!("a query read returned another kind of result");
    };
    queries
}

async fn get_catalog_fragments(
    harness: &Harness,
    qids: Vec<QueryId>,
) -> Vec<query_fragment::Model> {
    let queries = get_catalog_queries(harness, qids).await;
    queries.into_iter().flat_map(|row| row.fragments).collect()
}

/// A fragment ends as Stopped unless its worker was removed first, which fails it.
/// The model does not know which of the two happened first.
pub fn is_stopped_or_failed_by_drop(fragment: &query_fragment::Model, model: &ModelState) -> bool {
    match fragment.current_state {
        QueryFragmentState::Stopped => true,
        QueryFragmentState::Failed => model.dropped_workers().contains(&fragment.host_addr),
        _ => false,
    }
}

fn assert_stopped_or_failed_by_drop(
    name: &str,
    fragment: &query_fragment::Model,
    model: &ModelState,
) {
    assert!(
        is_stopped_or_failed_by_drop(fragment, model),
        "{name}: fragment {} on {} is {:?}, expect Stopped, or Failed on a dropped worker",
        fragment.id,
        fragment.host_addr,
        fragment.current_state,
    );
}

async fn get_catalog_workers(harness: &Harness, state: WorkerState) -> HashSet<NetworkAddr> {
    let StatementResult::Workers(workers) = harness
        .send(ParsedStatement::GetWorker(
            GetWorker::all().with_current_state(state),
        ))
        .await
        .unwrap()
    else {
        unreachable!("a worker read returned another kind of result");
    };
    workers.into_iter().map(|w| w.host_addr).collect()
}

/// Catalog fragments of active queries match the model and are all Running.
async fn check_active_catalog_fragments(harness: &Harness, model: &ModelState) {
    const NAME: &str = "active_catalog_fragments";
    let fragments = get_catalog_fragments(harness, model.active_queries()).await;
    let ids: HashSet<QueryFragmentId> = fragments.iter().map(|f| f.id).collect();
    assert_eq!(ids, model.active_fragments(), "{NAME}: ID mismatch");
    for f in &fragments {
        assert_eq!(
            f.current_state,
            QueryFragmentState::Running,
            "{NAME}: fragment {} is {:?}, expect Running",
            f.id,
            f.current_state,
        );
    }
}

/// Every dropped query has terminated and is deleted.
/// The worker check below shows that its fragments were stopped on the workers.
async fn check_dropped_catalog_queries(harness: &Harness, model: &ModelState) {
    const NAME: &str = "dropped_catalog_queries";
    let left: Vec<_> = get_catalog_queries(harness, model.dropped_queries())
        .await
        .into_iter()
        .map(|row| (row.query.id, row.query.state))
        .collect();
    assert!(
        left.is_empty(),
        "{NAME}: dropped queries left in the catalog: {left:?}"
    );
}

/// A query that ran on a dropped worker is Failed, and each of its fragments is Stopped or Failed on a dropped worker.
async fn check_failed_catalog_queries(harness: &Harness, model: &ModelState) {
    const NAME: &str = "failed_catalog_queries";
    let queries = get_catalog_queries(harness, model.failed_queries()).await;
    let ids: HashSet<QueryFragmentId> = queries
        .iter()
        .flat_map(|row| row.fragments.iter().map(|f| f.id))
        .collect();
    assert_eq!(ids, model.failed_fragments(), "{NAME}: ID mismatch");
    for row in &queries {
        assert_eq!(
            row.query.state,
            QueryState::Failed,
            "{NAME}: query {} is {:?}, expect Failed",
            row.query.id,
            row.query.state,
        );
        for f in &row.fragments {
            assert_stopped_or_failed_by_drop(NAME, f, model);
        }
    }
}

/// Workers report the same set of active fragments as the model.
async fn check_active_worker_fragments(harness: &Harness, model: &ModelState) {
    const NAME: &str = "active_worker_fragments";
    let actual = harness.get_worker_fragments(model.active_workers()).await;
    let expected = model.active_fragments();
    let leaked: Vec<_> = actual.difference(&expected).collect();
    let missing: Vec<_> = expected.difference(&actual).collect();
    assert!(
        leaked.is_empty() && missing.is_empty(),
        "{NAME}: {} leaked (active on worker but query dropped): {:?}, {} missing (expected active but not on worker): {:?}",
        leaked.len(),
        leaked,
        missing.len(),
        missing,
    );
}

/// Catalog workers marked Active match the model's active workers.
async fn check_active_catalog_workers(harness: &Harness, model: &ModelState) {
    const NAME: &str = "active_catalog_workers";
    let active = get_catalog_workers(harness, WorkerState::Active).await;
    assert_eq!(&active, model.active_workers(), "{NAME}: set mismatch");
}

/// Catalog workers marked Removed match the model's dropped workers.
async fn check_dropped_catalog_workers(harness: &Harness, model: &ModelState) {
    const NAME: &str = "dropped_catalog_workers";
    let removed = get_catalog_workers(harness, WorkerState::Removed).await;
    assert_eq!(&removed, model.dropped_workers(), "{NAME}: set mismatch");
}

fn check_failed_query_count(model: &ModelState, min: usize) {
    let failed = model.failed_query_count();
    assert!(
        failed >= min,
        "worker drops failed {failed} queries, expect at least {min}"
    );
    info!("worker drops failed {failed} queries");
}
