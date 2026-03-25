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
use crate::workload::{
    Invariant, InvariantContext, Workload, WorkloadFactory, check_invariants, generate_weighted,
    parse_options, precompute_delays,
};
use async_trait::async_trait;
use model::worker;
use model::worker::{DesiredWorkerState, WorkerState};
use proptest::prelude::*;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

const DEFAULT_NUM_OPS: usize = 15;
const DEFAULT_END_SECS: u64 = 30;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClusterConfig {
    num_ops: usize,
    begin: u64,
    end: u64,
    create_weight: u32,
    drop_weight: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            begin: 0,
            end: DEFAULT_END_SECS,
            create_weight: 3,
            drop_weight: 1,
        }
    }
}

pub struct ClusterWorkload {
    num_ops: usize,
    begin: Duration,
    end: Duration,
    create_weight: u32,
    drop_weight: u32,
    state: RefCell<ClusterState>,
}

#[derive(Default)]
struct ClusterState {
    registered_worker_indices: Vec<usize>,
}

#[derive(Clone, Debug)]
enum ClusterOp {
    CreateWorker(usize),
    DropWorker(usize),
}

struct GeneratorState {
    registered: Vec<bool>,
}

impl GeneratorState {
    fn new(num_workers: usize) -> Self {
        Self {
            registered: vec![true; num_workers],
        }
    }

    fn valid_ops(
        &self,
        create_weight: u32,
        drop_weight: u32,
    ) -> Vec<(u32, BoxedStrategy<ClusterOp>)> {
        let mut strategies: Vec<(u32, BoxedStrategy<ClusterOp>)> = Vec::new();

        let unregistered: Vec<usize> = self
            .registered
            .iter()
            .enumerate()
            .filter(|(_, r)| !**r)
            .map(|(i, _)| i)
            .collect();
        if !unregistered.is_empty() {
            strategies.push((
                create_weight,
                prop::sample::select(unregistered)
                    .prop_map(ClusterOp::CreateWorker)
                    .boxed(),
            ));
        }

        let registered: Vec<usize> = self
            .registered
            .iter()
            .enumerate()
            .filter(|(_, r)| **r)
            .map(|(i, _)| i)
            .collect();
        if !registered.is_empty() {
            strategies.push((
                drop_weight,
                prop::sample::select(registered)
                    .prop_map(ClusterOp::DropWorker)
                    .boxed(),
            ));
        }

        strategies
    }

    fn apply(&mut self, op: &ClusterOp) {
        match op {
            ClusterOp::CreateWorker(i) => self.registered[*i] = true,
            ClusterOp::DropWorker(_) => {}
        }
    }
}

impl ClusterWorkload {
    pub const NAME: &str = "Cluster";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: ClusterConfig = parse_options(options);
        Self {
            num_ops: c.num_ops,
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            create_weight: c.create_weight,
            drop_weight: c.drop_weight,
            state: RefCell::new(ClusterState::default()),
        }
    }

    fn generate_op(&self, gen_state: &mut GeneratorState) -> ClusterOp {
        let strategies = gen_state.valid_ops(self.create_weight, self.drop_weight);
        let op = generate_weighted(strategies);
        gen_state.apply(&op);
        op
    }

    async fn execute_op(
        &self,
        harness: &TestHarness,
        i: usize,
        op: ClusterOp,
        result: &mut ClusterState,
    ) {
        match op {
            ClusterOp::CreateWorker(idx) => {
                info!("cluster[{i}]: CreateWorker({idx})");
                harness.restart_worker(idx);
                let worker = harness.worker_config(idx);

                let workers = harness.get_workers().await.unwrap();
                let exists = workers.iter().any(|w| w.host_addr == worker.host_addr);

                if !exists {
                    let resp = harness.create_worker(&worker).await;
                    match resp {
                        Ok(_) => {
                            info!("cluster[{i}]: CreateWorker({idx}) succeeded");
                            result.registered_worker_indices.push(idx);
                        }
                        Err(e) => info!("cluster[{i}]: CreateWorker({idx}) failed: {e}"),
                    }
                } else {
                    info!("cluster[{i}]: CreateWorker({idx}) already exists, skipping");
                    result.registered_worker_indices.push(idx);
                }
            }
            ClusterOp::DropWorker(idx) => {
                info!("cluster[{i}]: DropWorker({idx})");
                let host = harness.worker_host(idx);
                let resp = harness.drop_worker(host).await;
                match resp {
                    Ok(_) => info!("cluster[{i}]: DropWorker({idx}) succeeded"),
                    Err(e) => info!("cluster[{i}]: DropWorker({idx}) rejected: {e}"),
                }
            }
        }
    }

}

#[async_trait(?Send)]
impl Workload for ClusterWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: running {} ops ({:?}..{:?})",
            self.name(), self.num_ops, self.begin, self.end,
        );

        tokio::time::sleep(self.begin).await;

        let delays = precompute_delays(self.num_ops, self.end - self.begin);
        let mut gen_state = GeneratorState::new(harness.num_workers());
        let mut result = ClusterState::default();

        for i in 0..self.num_ops {
            let op = self.generate_op(&mut gen_state);
            self.execute_op(harness, i, op, &mut result).await;
            tokio::time::sleep(delays[i]).await;
        }
        info!("{}: completed {} ops", self.name(), self.num_ops);

        *self.state.borrow_mut() = result;
    }

    async fn check(&self, harness: &TestHarness) {
        let ctx = InvariantContext::empty();
        check_invariants(&[&WorkerConvergence], harness, &ctx).await;
    }
}

pub struct WorkerConvergence;

#[async_trait(?Send)]
impl Invariant for WorkerConvergence {
    fn name(&self) -> &str {
        "worker_convergence"
    }

    async fn check(&self, harness: &TestHarness, _ctx: &InvariantContext) {
        let workers = harness.get_workers().await.unwrap();
        for w in &workers {
            match w.desired_state {
                DesiredWorkerState::Active => {
                    assert_eq!(
                        w.current_state,
                        WorkerState::Active,
                        "worker_convergence: worker {} desired=Active current={:?}",
                        w.host_addr,
                        w.current_state
                    );
                }
                DesiredWorkerState::Removed => {
                    assert!(
                        w.current_state == WorkerState::Removed
                            || w.current_state == WorkerState::Unreachable,
                        "worker_convergence: worker {} desired=Removed current={:?}",
                        w.host_addr,
                        w.current_state
                    );
                }
            }
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: ClusterWorkload::NAME,
        create: |opts| Box::new(ClusterWorkload::from_options(opts)),
    }
}
