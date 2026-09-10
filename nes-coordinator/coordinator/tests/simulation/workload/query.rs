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

//! Sends create/drop query traffic against the simulated cluster and
//! asserts that the catalog and the workers stay in agreement.
//!
//! Each operation is one create-query or drop-query action. The workload
//! picks `num_ops` of them, chooses create or drop by `create_weight` and
//! `drop_weight`, spreads them across `[begin, end)` with random delays,
//! and lets the runner replay each through the model state so every
//! operation runs against a well-formed catalog. The final checks
//! cover three angles: fragments of active queries are `Running` in the
//! catalog, fragments of dropped queries are `Stopped` in the catalog,
//! and worker-side active fragments match what the model expects.

#![cfg(madsim)]

use crate::harness::TestHarness;
use crate::invariant::{
    CatalogQueryFragmentsActive, CatalogQueryFragmentsDropped, WorkerQueryFragmentsActive,
};
use crate::model_state::{ModelState, Operation};
use crate::workload::{
    Workload, WorkloadFactory, check_invariants, execute, gen_delays, parse_options, pick_weighted,
};
use async_trait::async_trait;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info};

const DEFAULT_NUM_OPS: usize = 100;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryConfig {
    begin: u64,
    end: u64,
    num_ops: usize,
    create_weight: u32,
    drop_weight: u32,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            begin: 0,
            end: 300,
            create_weight: 3,
            drop_weight: 1,
        }
    }
}

pub struct QueryWorkload {
    begin: Duration,
    end: Duration,
    num_ops: usize,
    create_weight: u32,
    drop_weight: u32,
    model: Rc<RefCell<ModelState>>,
}

impl QueryWorkload {
    pub const NAME: &str = "Query";

    pub fn from_options(
        options: &HashMap<String, toml::Value>,
        model: Rc<RefCell<ModelState>>,
    ) -> Self {
        let cfg: QueryConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(cfg.begin),
            end: Duration::from_secs(cfg.end),
            num_ops: cfg.num_ops,
            create_weight: cfg.create_weight,
            drop_weight: cfg.drop_weight,
            model,
        }
    }
}

#[async_trait(?Send)]
impl Workload for QueryWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&mut self, harness: &TestHarness) {
        info!(
            "{}: running {} ops between ({}s..{}s)",
            self.name(),
            self.num_ops,
            self.begin.as_secs(),
            self.end.as_secs(),
        );

        tokio::time::sleep(self.begin).await;

        let delays = gen_delays(self.num_ops, self.begin.as_secs(), self.end.as_secs());
        debug!("{delays:?}");

        for delay in delays {
            let op = pick_weighted(&[
                (Operation::CreateQuery, self.create_weight),
                (Operation::DropQuery, self.drop_weight),
            ]);
            execute(op, &self.model, harness).await;

            tokio::time::sleep(delay).await;
        }

        info!("{}: completed {} ops", self.name(), self.num_ops);
    }

    // The model state is read-only across this await chain: it is only
    // ever mutated by `run` between ticks, and `check` is the final
    // call of each tick. Madsim is single-threaded so no other task can
    // borrow it concurrently.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn check(&self, harness: &TestHarness) {
        let model = self.model.borrow();
        check_invariants(
            &[
                &CatalogQueryFragmentsActive,
                &CatalogQueryFragmentsDropped,
                &WorkerQueryFragmentsActive,
            ],
            harness,
            &*model,
        )
        .await;
    }
}

inventory::submit! {
    WorkloadFactory {
        name: QueryWorkload::NAME,
        create: |opts, model| Box::new(QueryWorkload::from_options(opts, model)),
    }
}
