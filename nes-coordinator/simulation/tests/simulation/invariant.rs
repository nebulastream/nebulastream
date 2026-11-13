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

use crate::harness::TestHarness;
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

pub async fn check_all(harness: &TestHarness, model: &ModelState) {
    get_active_catalog_fragments(harness, model).await;
    get_dropped_catalog_fragments(harness, model).await;
    get_failed_catalog_queries(harness, model).await;
    get_active_worker_fragments(harness, model).await;
    get_active_catalog_workers(harness, model).await;
    get_dropped_catalog_workers(harness, model).await;
    info!("all invariants hold");
}

async fn get_catalog_queries(harness: &TestHarness, qids: Vec<QueryId>) -> Vec<QueryWithFragments> {
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
    harness: &TestHarness,
    qids: Vec<QueryId>,
) -> Vec<query_fragment::Model> {
    let queries = get_catalog_queries(harness, qids).await;
    queries.into_iter().flat_map(|row| row.fragments).collect()
}

/// A fragment ends as Stopped unless its worker was removed first, which fails it.
/// The model does not know which of the two happened first.
fn stopped_or_failed_by_removal(fragment: &query_fragment::Model, model: &ModelState) -> bool {
    match fragment.current_state {
        QueryFragmentState::Stopped => true,
        QueryFragmentState::Failed => model.dropped_workers().contains(&fragment.host_addr),
        _ => false,
    }
}

async fn get_catalog_workers(state: WorkerState, harness: &TestHarness) -> HashSet<NetworkAddr> {
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
async fn get_active_catalog_fragments(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "catalog_fragments_active";
    let fragments = get_catalog_fragments(harness, model.active_queries()).await;
    let ids: HashSet<QueryFragmentId> = fragments.iter().map(|f| f.id).collect();
    assert_eq!(ids, model.active_fragments(), "{}: ID mismatch", NAME);
    for f in &fragments {
        assert_eq!(
            f.current_state,
            QueryFragmentState::Running,
            "{}: fragment {} is {:?}, expect Running",
            NAME,
            f.id,
            f.current_state,
        );
    }
}

/// Catalog fragments of dropped queries match the model and are all Stopped, or Failed on a dropped worker.
async fn get_dropped_catalog_fragments(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "catalog_fragments_dropped";
    let fragments = get_catalog_fragments(harness, model.dropped_queries()).await;
    let ids: HashSet<QueryFragmentId> = fragments.iter().map(|f| f.id).collect();
    assert_eq!(ids, model.dropped_fragments(), "{}: ID mismatch", NAME,);
    for f in &fragments {
        assert!(
            stopped_or_failed_by_removal(f, model),
            "{}: fragment {} on {} is {:?}, expect Stopped, or Failed on a dropped worker",
            NAME,
            f.id,
            f.host_addr,
            f.current_state,
        );
    }
}

/// A query that ran on a dropped worker is Failed: one fragment is Failed on a dropped worker, and none is left running.
async fn get_failed_catalog_queries(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "catalog_queries_failed";
    let queries = get_catalog_queries(harness, model.failed_queries()).await;
    let ids: HashSet<QueryFragmentId> = queries
        .iter()
        .flat_map(|row| row.fragments.iter().map(|f| f.id))
        .collect();
    assert_eq!(ids, model.failed_fragments(), "{}: ID mismatch", NAME);
    for row in &queries {
        assert_eq!(
            row.query.state,
            QueryState::Failed,
            "{}: query {} is {:?}, expect Failed",
            NAME,
            row.query.id,
            row.query.state,
        );
        assert!(
            row.fragments
                .iter()
                .any(|f| f.current_state == QueryFragmentState::Failed),
            "{}: query {} has no Failed fragment",
            NAME,
            row.query.id,
        );
        for f in &row.fragments {
            assert!(
                stopped_or_failed_by_removal(f, model),
                "{}: fragment {} on {} is {:?}, expect Stopped, or Failed on a dropped worker",
                NAME,
                f.id,
                f.host_addr,
                f.current_state,
            );
        }
    }
}

/// Workers report the same set of active fragments as the model.
async fn get_active_worker_fragments(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "worker_fragments_active";
    let active = harness.get_active_fragments(&model.active_workers()).await;
    let actual: HashSet<_> = active.into_iter().collect();
    let expected = model.active_fragments();
    let leaked: Vec<_> = actual.difference(&expected).collect();
    let missing: Vec<_> = expected.difference(&actual).collect();
    assert!(
        leaked.is_empty() && missing.is_empty(),
        "{}: {} leaked (active on worker but query dropped): {:?}, {} missing (expected active but not on worker): {:?}",
        NAME,
        leaked.len(),
        leaked,
        missing.len(),
        missing,
    );
}

/// Catalog workers marked Active match the model's active workers.
async fn get_active_catalog_workers(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "catalog_workers_active";
    let active = get_catalog_workers(WorkerState::Active, harness).await;
    assert_eq!(
        active,
        model.active_workers().into_iter().collect::<HashSet<_>>(),
        "{}: set mismatch",
        NAME,
    );
}

/// Catalog workers marked Removed match the model's dropped workers.
async fn get_dropped_catalog_workers(harness: &TestHarness, model: &ModelState) {
    const NAME: &str = "catalog_workers_dropped";
    let removed = get_catalog_workers(WorkerState::Removed, harness).await;
    assert_eq!(&removed, model.dropped_workers(), "{}: set mismatch", NAME,);
}
