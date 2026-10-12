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

use super::*;
use crate::worker::CreateWorker;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

#[derive(Debug, Clone)]
pub(crate) struct QueryFragmentsWithRefs {
    pub(crate) workers: Vec<CreateWorker>,
    pub(crate) fragments: Vec<CreateQueryFragment>,
}

impl Arbitrary for QueryFragmentsWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        const MAX_WORKERS: u8 = 32;
        const MAX_NUM_FRAGMENTS: usize = 16;
        const MAX_OPERATORS_PER_FRAGMENT: i32 = 12;
        (
            CreateWorker::topology_dag(1, MAX_WORKERS),
            1..=MAX_NUM_FRAGMENTS,
        )
            .prop_flat_map(|(workers, num)| {
                let n = workers.len();
                (Just(workers), vec((0..n, any::<bool>()), num))
            })
            .prop_map(|(workers, placements)| {
                // Track each worker's remaining `max_operators` so generated fixtures stay within it;
                // otherwise the insert is rejected.
                // The per-query_fragment cap keeps test cases small.
                let mut remaining: Vec<i32> = workers
                    .iter()
                    .map(|worker| worker.max_operators.unwrap_or(i32::MAX))
                    .collect();

                let mut fragments: Vec<CreateQueryFragment> = placements
                    .into_iter()
                    .map(|(idx, has_source)| {
                        // Each query_fragment takes half of what is left
                        // so several fragments can share one worker.
                        let remaining_capacity = remaining[idx].min(MAX_OPERATORS_PER_FRAGMENT);
                        let num_operators = remaining_capacity / 2;
                        remaining[idx] -= num_operators;
                        CreateQueryFragment {
                            host_addr: workers[idx].host_addr.clone(),
                            plan: vec![],
                            num_operators,
                            has_source,
                        }
                    })
                    .collect();
                fragments[0].has_source = true;

                QueryFragmentsWithRefs { workers, fragments }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
