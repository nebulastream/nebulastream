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

use super::query_fragment::CreateQueryFragment;
use super::*;
use crate::ConnectorKind;
use crate::source::logical::CreateLogicalSource;
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::{Index, select};
use proptest::strategy::BoxedStrategy;
use sea_orm::Iterable;

/// Test fixture that bundles a `CreateQuery` with every catalog row that it depends on
/// (workers, logical source, physical sources, sink).
/// The `setup` helpers insert them in dependency order.
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
        const MAX_WORKERS: usize = 32;
        const MAX_NUM_FRAGMENTS: usize = 16;
        const MAX_OPERATORS_PER_FRAGMENT: i32 = 12;
        const NAME: &str = "[a-z][a-z0-9_]{2,29}";
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
            proptest::option::of(NAME),
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
                )| {
                    // Track each worker's remaining `max_operators` so generated fixtures stay within it;
                    // otherwise the insert is rejected.
                    // The per-query_fragment cap keeps test cases small.
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
                    fragments[0].has_source = true;

                    let sink_host = sink_fragment.get(&fragments).host_addr.clone();
                    let sql = format!("SELECT * FROM {} INTO {};", logical_source.name, sink_name);
                    let query = CreateQuery {
                        name,
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
