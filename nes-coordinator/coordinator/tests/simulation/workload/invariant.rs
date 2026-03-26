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

#![cfg(madsim)]

use crate::harness::TestHarness;
use async_trait::async_trait;
use model::query::fragment;
use model::query::GetQuery;
use model::worker::endpoint::NetworkAddr;
use model::{query};
use std::collections::{HashMap, HashSet};
use tracing::info;

pub struct InvariantContext {
    live_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
    pub queries: Vec<(query::Model, Vec<fragment::Model>)>,
    pub active_by_worker: HashMap<NetworkAddr, HashSet<u64>>,
}

impl InvariantContext {
    pub async fn build(
        harness: &TestHarness,
        created_query_ids: Vec<i64>,
        dropped_query_ids: Vec<i64>,
    ) -> Self {
        let dropped_set: HashSet<i64> = dropped_query_ids.iter().copied().collect();
        let live_query_ids: Vec<i64> = created_query_ids
            .into_iter()
            .filter(|id| !dropped_set.contains(id))
            .collect();

        let all_ids: Vec<i64> = live_query_ids
            .iter()
            .chain(dropped_query_ids.iter())
            .copied()
            .collect();

        let queries = harness
            .get_queries(GetQuery::all().with_ids(all_ids).with_fragments())
            .await
            .unwrap();

        let active_by_worker = harness.active_fragments_by_worker().await;

        Self {
            live_query_ids,
            dropped_query_ids,
            queries,
            active_by_worker,
        }
    }

    pub fn live_query_ids(&self) -> &[i64] {
        &self.live_query_ids
    }

    pub fn dropped_query_ids(&self) -> &[i64] {
        &self.dropped_query_ids
    }

    pub fn empty() -> Self {
        Self {
            live_query_ids: Vec::new(),
            dropped_query_ids: Vec::new(),
            queries: Vec::new(),
            active_by_worker: HashMap::new(),
        }
    }
}

#[async_trait(?Send)]
pub trait Invariant {
    fn name(&self) -> &str;
    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext);
}

pub async fn check_invariants(
    invariants: &[&dyn Invariant],
    harness: &TestHarness,
    ctx: &InvariantContext,
) {
    for inv in invariants {
        inv.check(harness, ctx).await;
        info!("invariant {}: ok", inv.name());
    }
}
