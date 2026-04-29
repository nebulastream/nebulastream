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

//! Drives worker join/leave traffic during a trial.
//!
//! On `setup` the model is primed with whichever workers already exist
//! in the catalog so that asserts later in the run are not fooled by
//! pre-existing state. `start` then runs random create/drop operations
//! over the configured window. The final checks compare the set of
//! `Active` and `Removed` workers in the catalog against the model.

#![cfg(madsim)]

use crate::harness::TestHarness;
use crate::model_state::{ModelState, Operation};
use crate::workload::{
    check_invariants, execute, gen_delays, parse_options, pick_weighted, Invariant, Workload,
    WorkloadFactory,
};
use async_trait::async_trait;
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{GetWorker, WorkerState};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info};

const DEFAULT_NUM_OPS: usize = 32;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WorkerConfig {
    begin: u64,
    end: u64,
    num_ops: usize,
    create_weight: u32,
    drop_weight: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: 120,
            num_ops: DEFAULT_NUM_OPS,
            create_weight: 2,
            drop_weight: 1,
        }
    }
}

pub struct WorkerWorkload {
    begin: Duration,
    end: Duration,
    num_ops: usize,
    create_weight: u32,
    drop_weight: u32,
    model: Rc<RefCell<ModelState>>,
}

impl WorkerWorkload {
    pub const NAME: &str = "Worker";

    pub fn from_options(
        options: &HashMap<String, toml::Value>,
        model: Rc<RefCell<ModelState>>,
    ) -> Self {
        let cfg: WorkerConfig = parse_options(options);
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
impl Workload for WorkerWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn setup(&mut self, harness: &TestHarness) {
        let StatementResult::Workers(workers) = harness
            .send(Statement::GetWorker(GetWorker::all()))
            .await
            .unwrap()
        else {
            unreachable!();
        };
        for worker in workers {
            self.model
                .borrow_mut()
                .observe(StatementResult::CreatedWorker(worker));
        }
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
            tokio::time::sleep(delay).await;

            let op = pick_weighted(&[
                (Operation::CreateWorker, self.create_weight),
                (Operation::DropWorker, self.drop_weight),
            ]);
            execute(op, &self.model, harness).await;
        }

        info!("{}: completed {} ops", self.name(), self.num_ops);
    }

    // See the matching comment in workload/query.rs::check.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn check(&self, harness: &TestHarness) {
        let model = self.model.borrow();
        check_invariants(&[&DbWorkersActive, &DbWorkersDropped], harness, &*model).await;
    }
}

async fn db_workers_in(state: WorkerState, harness: &TestHarness) -> HashSet<NetworkAddr> {
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

/// DB workers with `current_state == Active` match the model's active workers.
struct DbWorkersActive;

#[async_trait(?Send)]
impl Invariant<ModelState> for DbWorkersActive {
    fn name(&self) -> &str {
        "db_workers_active"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let active = db_workers_in(WorkerState::Active, harness).await;
        assert_eq!(
            active,
            model.active_workers().into_iter().collect::<HashSet<_>>(),
            "{}: set mismatch",
            self.name(),
        );
    }
}

/// DB workers with `current_state == Removed` match the model's dropped workers.
struct DbWorkersDropped;

#[async_trait(?Send)]
impl Invariant<ModelState> for DbWorkersDropped {
    fn name(&self) -> &str {
        "db_workers_dropped"
    }

    async fn check(&self, harness: &TestHarness, model: &ModelState) {
        let removed = db_workers_in(WorkerState::Removed, harness).await;
        assert_eq!(
            &removed,
            model.dropped_workers(),
            "{}: set mismatch",
            self.name(),
        );
    }
}

inventory::submit! {
    WorkloadFactory {
        name: WorkerWorkload::NAME,
        create: |opts, model| Box::new(WorkerWorkload::from_options(opts, model)),
    }
}
