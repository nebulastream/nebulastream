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

use super::query_fragment::{
    self, CreateQueryFragment, DesiredQueryFragmentState, QueryFragmentState,
};
use super::query_state::QueryState;
use super::*;
use crate::ConnectorKind;
use crate::identifier::{QueryFragmentId, QueryId};
use crate::source::logical::CreateLogicalSource;
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use chrono::{DateTime, Utc};
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;
use proptest::sample::{Index, Select, select};
use proptest::strategy::BoxedStrategy;
use sea_orm::Iterable;

// The link mask of the topology generator has 32 bits.
const MAX_WORKERS: usize = 32;
const MAX_NUM_FRAGMENTS: usize = 16;
const MAX_OPERATORS_PER_FRAGMENT: i32 = 12;
const NAME: &str = "[a-z][a-z0-9_]{2,29}";

/// Test fixture that bundles a `CreateQuery` with every catalog row that it depends on.
/// The test setup inserts them in dependency order.
#[derive(Debug, Clone)]
pub struct CreateQueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub logical_source: CreateLogicalSource,
    pub kind: ConnectorKind,
    pub connector_type: String,
    pub sink_name: String,
    pub sink_host: NetworkAddr,
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let placement = (
            any::<Index>(),
            any::<Index>(),
            any::<bool>(),
            vec(any::<u8>(), 0..8),
        );
        (
            CreateWorker::topology_dag(1..=MAX_WORKERS),
            vec(placement, 1..=MAX_NUM_FRAGMENTS),
            any::<CreateLogicalSource>(),
            select(ConnectorKind::iter().collect::<Vec<_>>()),
            "[A-Za-z]{1,10}",
            NAME,
            any::<Index>(),
            option::of(any::<Identifier>()),
            vec(any::<u8>(), 0..8),
        )
            .prop_map(
                |(
                    workers,
                    placements,
                    logical_source,
                    kind,
                    connector_type,
                    sink_name,
                    sink_fragment,
                    name,
                    global_plan,
                )| {
                    // Track each worker's remaining `max_operators` so generated fixtures stay within it;
                    // otherwise the insert is rejected.
                    // The per-fragment cap keeps test cases small.
                    let mut remaining: Vec<i32> = workers
                        .iter()
                        .map(|worker| worker.max_operators.unwrap_or(i32::MAX))
                        .collect();
                    let mut fragments: Vec<CreateQueryFragment> = placements
                        .into_iter()
                        .map(|(worker, operators, has_source, plan)| {
                            let idx = worker.index(workers.len());
                            let available = remaining[idx].min(MAX_OPERATORS_PER_FRAGMENT);
                            let num_operators = operators.index(available as usize + 1) as i32;
                            remaining[idx] -= num_operators;
                            CreateQueryFragment {
                                host_addr: workers[idx].host_addr.clone(),
                                plan,
                                num_operators,
                                has_source,
                            }
                        })
                        .collect();
                    // A query needs a source, so the first fragment always hosts one.
                    fragments[0].has_source = true;

                    let sink_host = sink_fragment.get(&fragments).host_addr.clone();
                    let sql = format!("SELECT * FROM {} INTO {};", logical_source.name, sink_name);
                    let query = CreateQuery {
                        name,
                        global_plan,
                        ..CreateQuery::new(sql).with_fragments(fragments)
                    };
                    CreateQueryWithRefs {
                        workers,
                        logical_source,
                        kind,
                        connector_type,
                        sink_name,
                        sink_host,
                        query,
                    }
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for QueryState {
    type Parameters = ();
    type Strategy = Select<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        select(Self::iter().collect::<Vec<_>>())
    }
}

impl Arbitrary for QueryFragmentState {
    type Parameters = ();
    type Strategy = Select<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        select(Self::iter().collect::<Vec<_>>())
    }
}

impl Arbitrary for DesiredQueryFragmentState {
    type Parameters = ();
    type Strategy = Select<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        select(Self::iter().collect::<Vec<_>>())
    }
}

/// No time, or a whole second between 1970 and 2100.
fn timestamp() -> impl Strategy<Value = Option<DateTime<Utc>>> {
    option::of((0..4_102_444_800i64).prop_map(|secs| {
        DateTime::from_timestamp(secs, 0).expect("the range is within chrono's bounds")
    }))
}

/// A query row without an error, whose state need not match its timestamps.
impl Arbitrary for Model {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        (
            any::<QueryId>(),
            any::<Identifier>(),
            any::<String>(),
            vec(any::<u8>(), 0..8),
            any::<QueryState>(),
            timestamp(),
            timestamp(),
            any::<bool>(),
        )
            .prop_map(
                |(id, name, sql, global_plan, state, start_timestamp, stop_timestamp, dropped)| {
                    Self {
                        id,
                        name,
                        sql,
                        global_plan,
                        state,
                        start_timestamp,
                        stop_timestamp,
                        error: None,
                        dropped,
                    }
                },
            )
            .boxed()
    }
}

/// A fragment row without an error, whose state need not match its timestamps.
impl Arbitrary for query_fragment::Model {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        (
            any::<QueryFragmentId>(),
            any::<QueryId>(),
            any::<NetworkAddr>(),
            vec(any::<u8>(), 0..8),
            1..=MAX_OPERATORS_PER_FRAGMENT,
            any::<bool>(),
            any::<QueryFragmentState>(),
            any::<DesiredQueryFragmentState>(),
            timestamp(),
            timestamp(),
            timestamp(),
        )
            .prop_map(
                |(
                    id,
                    query_id,
                    host_addr,
                    plan,
                    num_operators,
                    has_source,
                    current_state,
                    desired_state,
                    start_timestamp,
                    stop_timestamp,
                    last_observed_at,
                )| Self {
                    id,
                    query_id,
                    host_addr,
                    plan,
                    num_operators,
                    has_source,
                    current_state,
                    desired_state,
                    start_timestamp,
                    stop_timestamp,
                    error: None,
                    last_observed_at,
                },
            )
            .boxed()
    }
}

/// The fragments belong to the query, and a query can have none.
impl Arbitrary for QueryWithFragments {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;
    fn arbitrary_with((): ()) -> Self::Strategy {
        (
            any::<Model>(),
            vec(any::<query_fragment::Model>(), 0..=MAX_NUM_FRAGMENTS),
        )
            .prop_map(|(query, fragments)| {
                let fragments = fragments
                    .into_iter()
                    .map(|fragment| query_fragment::Model {
                        query_id: query.id,
                        ..fragment
                    })
                    .collect();
                Self { query, fragments }
            })
            .boxed()
    }
}
