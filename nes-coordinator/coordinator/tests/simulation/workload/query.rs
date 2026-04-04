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
use crate::model_state::{ModelState, Operation};
use crate::workload::{
    Invariant, Workload, WorkloadFactory, check_invariants, execute, gen_delays, parse_options,
    pick_weighted,
};
use async_trait::async_trait;
use model::identifier::{FragmentId, QueryId};
use model::query::GetQuery;
use model::query::fragment::{self, FragmentState};
use model::statement::{Statement, StatementResult};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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

    async fn check(&self, harness: &TestHarness) {
        check_invariants(
            &[
                &DbFragmentsActive,
                &DbFragmentsDropped,
                &WorkerFragmentsActive,
            ],
            harness,
            &*self.model.borrow(),
        )
        .await;
    }
}

async fn db_fragments_of(harness: &TestHarness, qids: Vec<QueryId>) -> Vec<fragment::Model> {
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

/// DB fragments of active queries match the model and are all `Running`.
struct DbFragmentsActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for DbFragmentsActive {
    fn name(&self) -> &str {
        "db_fragments_active"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let fragments = db_fragments_of(harness, model.active_queries()).await;
        let ids: HashSet<FragmentId> = fragments.iter().map(|f| f.id).collect();
        assert_eq!(
            ids,
            model.active_fragments(),
            "{}: ID mismatch",
            self.name()
        );
        for f in &fragments {
            assert_eq!(
                f.current_state,
                FragmentState::Running,
                "{}: fragment {} is {:?}, expect Running",
                self.name(),
                f.id,
                f.current_state,
            );
        }
    }
}

/// DB fragments of dropped queries match the model and are all `Stopped`.
struct DbFragmentsDropped;

#[async_trait(?Send)]
impl Invariant<ModelState> for DbFragmentsDropped {
    fn name(&self) -> &str {
        "db_fragments_dropped"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let fragments = db_fragments_of(harness, model.dropped_queries()).await;
        let ids: HashSet<FragmentId> = fragments.iter().map(|f| f.id).collect();
        assert_eq!(
            ids,
            model.dropped_fragments(),
            "{}: ID mismatch",
            self.name(),
        );
        for f in &fragments {
            assert_eq!(
                f.current_state,
                FragmentState::Stopped,
                "{}: fragment {} is {:?}, expect Stopped",
                self.name(),
                f.id,
                f.current_state,
            );
        }
    }
}

/// Workers report the same set of active fragments as the model.
struct WorkerFragmentsActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for WorkerFragmentsActive {
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
            leaked.len(), leaked,
            missing.len(), missing,
        );
    }
}


inventory::submit! {
    WorkloadFactory {
        name: QueryWorkload::NAME,
        create: |opts, model| Box::new(QueryWorkload::from_options(opts, model)),
    }
}
