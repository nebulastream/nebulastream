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

//! Predicates over the end state of a trial, which run once the simulation has quiesced.
//!
//! Each predicate compares the ledger with the catalog or with what the workers report,
//! and lists every query or fragment that violates it.

use crate::harness::Harness;
use crate::ledger::{Ledger, LedgerQuery};
use futures::future::join_all;
use model::identifier::{QueryFragmentId, QueryId};
use model::query::query_fragment::QueryFragmentState;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryWithFragments};
use model::statement::{ParsedStatement, StatementResult};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use tracing::info;

type Catalog = HashMap<QueryId, QueryWithFragments>;

/// Run only after the workloads have finished and the system has had time to recover.
pub async fn check_all(harness: &Harness, ledger: &Ledger, min_failed_queries: usize) {
    let catalog = get_catalog_queries(harness).await;

    let mut violations = Vec::new();
    for (&id, query) in ledger.queries() {
        if query.is_dropped() {
            check_dropped_query(id, query, ledger, &catalog, &mut violations);
        } else if query
            .fragments()
            .values()
            .any(|&host| ledger.is_dropped(host))
        {
            check_failed_query(id, query, ledger, &catalog, &mut violations);
        } else {
            check_running_query(id, query, ledger, &catalog, &mut violations);
        }
    }
    check_worker_fragments(harness, ledger, &catalog, &mut violations).await;

    if !violations.is_empty() {
        let mut report = format!("{} violations:", violations.len());
        for violation in &violations {
            let _ = write!(report, "\n  {violation}");
        }
        panic!("{report}");
    }
    check_failed_query_count(ledger, min_failed_queries);
    info!("all invariants hold");
}

async fn get_catalog_queries(harness: &Harness) -> Catalog {
    let StatementResult::Queries(queries) = harness
        .send(ParsedStatement::GetQuery(GetQuery::all().with_fragments()))
        .await
        .unwrap()
    else {
        unreachable!("a query read returned another kind of result");
    };
    queries.into_iter().map(|row| (row.query.id, row)).collect()
}

fn describe_hosts(query: &LedgerQuery, ledger: &Ledger) -> String {
    let hosts: BTreeSet<String> = query
        .fragments()
        .values()
        .map(|&host| ledger.get_addr(host).to_string())
        .collect();
    format!("{hosts:?}")
}

/// Every query that the client did not drop and whose hosts all remain registered is Running,
/// with the fragments that its create reply listed.
fn check_running_query(
    id: QueryId,
    query: &LedgerQuery,
    ledger: &Ledger,
    catalog: &Catalog,
    violations: &mut Vec<String>,
) {
    const NAME: &str = "running_queries";
    let Some(row) = catalog.get(&id) else {
        violations.push(format!(
            "{NAME}: query {id} on {} is missing from the catalog",
            describe_hosts(query, ledger)
        ));
        return;
    };
    if row.query.state != QueryState::Running {
        violations.push(format!(
            "{NAME}: query {id} on {} is {:?}, expect Running",
            describe_hosts(query, ledger),
            row.query.state
        ));
    }
    let created: BTreeSet<QueryFragmentId> = query.fragments().keys().copied().collect();
    let stored: BTreeSet<QueryFragmentId> = row.fragments.iter().map(|f| f.id).collect();
    if created != stored {
        violations.push(format!(
            "{NAME}: query {id} has fragments {stored:?} in the catalog, its create reply listed {created:?}"
        ));
    }
    for fragment in &row.fragments {
        if fragment.current_state != QueryFragmentState::Running {
            violations.push(format!(
                "{NAME}: fragment {} of query {id} on {} is {:?}, expect Running",
                fragment.id, fragment.host_addr, fragment.current_state
            ));
        }
    }
}

/// Every query with a fragment on a removed host is Failed, unless the client dropped it.
/// Its fragments are Stopped, or Failed on a removed host.
fn check_failed_query(
    id: QueryId,
    query: &LedgerQuery,
    ledger: &Ledger,
    catalog: &Catalog,
    violations: &mut Vec<String>,
) {
    const NAME: &str = "failed_queries";
    let removed: Vec<String> = query
        .fragments()
        .iter()
        .filter(|&(_, &host)| ledger.is_dropped(host))
        .map(|(fragment, &host)| format!("fragment {fragment} on {}", ledger.get_addr(host)))
        .collect();
    let Some(row) = catalog.get(&id) else {
        violations.push(format!(
            "{NAME}: query {id} with {removed:?} on a removed host is missing from the catalog"
        ));
        return;
    };
    if row.query.state != QueryState::Failed {
        violations.push(format!(
            "{NAME}: query {id} with {removed:?} on a removed host is {:?}, expect Failed",
            row.query.state
        ));
    }
    for fragment in &row.fragments {
        let on_removed_host = query
            .fragments()
            .get(&fragment.id)
            .is_some_and(|&host| ledger.is_dropped(host));
        let accepted = match fragment.current_state {
            QueryFragmentState::Stopped => true,
            QueryFragmentState::Failed => on_removed_host,
            _ => false,
        };
        if !accepted {
            violations.push(format!(
                "{NAME}: fragment {} of failed query {id} on {} is {:?}, expect Stopped, or Failed on a removed host",
                fragment.id, fragment.host_addr, fragment.current_state
            ));
        }
    }
}

/// Every query that the client dropped is deleted from the catalog.
fn check_dropped_query(
    id: QueryId,
    query: &LedgerQuery,
    ledger: &Ledger,
    catalog: &Catalog,
    violations: &mut Vec<String>,
) {
    const NAME: &str = "dropped_queries";
    if let Some(row) = catalog.get(&id) {
        violations.push(format!(
            "{NAME}: dropped query {id} on {} is still in the catalog as {:?}",
            describe_hosts(query, ledger),
            row.query.state
        ));
    }
}

/// On every registered worker, the fragments that the catalog lists as Running are the ones that the worker reports as active.
async fn check_worker_fragments(
    harness: &Harness,
    ledger: &Ledger,
    catalog: &Catalog,
    violations: &mut Vec<String>,
) {
    const NAME: &str = "worker_fragments";
    let addrs = ledger.get_registered_addrs();
    let reports = join_all(
        addrs
            .iter()
            .map(|addr| harness.get_worker_fragments(addr.clone())),
    )
    .await;
    for (addr, reported) in addrs.iter().zip(reports) {
        let running: HashSet<QueryFragmentId> = catalog
            .values()
            .flat_map(|row| &row.fragments)
            .filter(|f| &f.host_addr == addr && f.current_state == QueryFragmentState::Running)
            .map(|f| f.id)
            .collect();
        let mut missing: Vec<_> = running.difference(&reported).collect();
        let mut leaked: Vec<_> = reported.difference(&running).collect();
        missing.sort_unstable();
        leaked.sort_unstable();
        if !missing.is_empty() {
            violations.push(format!(
                "{NAME}: fragments {missing:?} on {addr} are Running in the catalog but not active on the worker"
            ));
        }
        if !leaked.is_empty() {
            violations.push(format!(
                "{NAME}: fragments {leaked:?} on {addr} are active on the worker but not Running in the catalog"
            ));
        }
    }
}

/// Fails a trial that never checked a failed query, so it cannot pass without having dropped a worker that ran a fragment.
fn check_failed_query_count(ledger: &Ledger, min: usize) {
    let failed = ledger
        .queries()
        .values()
        .filter(|query| {
            !query.is_dropped() && query.fragments().values().any(|&h| ledger.is_dropped(h))
        })
        .count();
    assert!(
        failed >= min,
        "worker drops failed {failed} queries, expect at least {min}"
    );
    info!("worker drops failed {failed} queries");
}
