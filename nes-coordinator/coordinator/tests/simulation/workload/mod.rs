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

//! Workloads and the factories that build them.
//!
//! A workload is the unit of behavior a trial composes from. There are
//! two flavors:
//!
//!  - Driver workloads (`query`, `worker`) submit catalog operations and
//!    later assert that the catalog and the workers agree with the
//!    expected end state.
//!  - Failure-injection workloads (`attrition`, `partition`, `stall`,
//!    `swizzle`, `degradation`) perturb the network or node lifecycle
//!    around them.
//!
//! Both kinds register themselves with `inventory`, so trials reference
//! them by name in TOML without a central registry.

#![cfg(madsim)]

pub mod attrition;
pub mod degradation;
pub mod partition;
pub mod query;
pub mod stall;
pub mod swizzle;
pub mod worker;

pub use crate::invariant::{check_invariants, Invariant};

use crate::harness::TestHarness;
use crate::model_state::{ModelState, Operation, Step};
use async_trait::async_trait;
use madsim::rand::seq::SliceRandom;
use madsim::rand::{thread_rng, Rng};
use model::statement::{Statement, StatementResult};
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info, warn};

pub fn gen_delays(num_ops: usize, lo: u64, hi: u64) -> Vec<Duration> {
    debug_assert!(num_ops > 0 && hi >= lo);
    let mut rng = thread_rng();
    let lo_ms = lo * 1000;
    let hi_ms = hi * 1000;
    let mut timestamps: Vec<u64> = (0..num_ops).map(|_| rng.gen_range(lo_ms..=hi_ms)).collect();
    timestamps.sort();
    let mut prev = lo_ms;
    let delays: Vec<Duration> = timestamps
        .iter()
        .map(|&t| {
            let delay = Duration::from_millis(t - prev);
            prev = t;
            delay
        })
        .collect();

    debug!(delays = %delays.iter().map(|d| format!("{}ms", d.as_millis())).collect::<Vec<_>>().join(", "));
    delays
}

pub fn pick_weighted<T: Copy>(choices: &[(T, u32)]) -> T {
    choices
        .choose_weighted(&mut thread_rng(), |(_, w)| *w)
        .unwrap()
        .0
}

/// Lifecycle a workload moves through during a trial. `setup` runs in
/// sequence before any workload starts, so a workload can seed the
/// model from the catalog or stage shared state without racing the
/// others. `start` is where the actual driving or fault injection
/// happens and is run concurrently across all workloads. `check` runs
/// at the end and is allowed to panic to fail the trial.
#[async_trait(?Send)]
pub trait Workload {
    fn name(&self) -> &str;

    async fn setup(&mut self, _harness: &TestHarness) {}

    async fn start(&mut self, harness: &TestHarness);

    async fn check(&self, _harness: &TestHarness) {}
}

type WorkloadCtor = fn(&HashMap<String, toml::Value>, Rc<RefCell<ModelState>>) -> Box<dyn Workload>;

pub struct WorkloadFactory {
    pub name: &'static str,
    pub create: WorkloadCtor,
}

pub fn parse_options<T: DeserializeOwned + Default>(options: &HashMap<String, toml::Value>) -> T {
    if options.is_empty() {
        return T::default();
    }
    let table: toml::map::Map<String, toml::Value> = options
        .iter()
        .filter(|(k, _)| k.as_str() != "name")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if table.is_empty() {
        return T::default();
    }
    toml::Value::Table(table)
        .try_into()
        .expect("failed to parse workload options")
}

inventory::collect!(WorkloadFactory);

pub struct FailureInjectorFactory {
    pub should_inject: fn(already_added: usize) -> bool,
    pub create: fn() -> Box<dyn Workload>,
}

inventory::collect!(FailureInjectorFactory);

const MAX_FAILURE_INJECTIONS_PER_TYPE: usize = 3;
const BASE_INJECTION_PROBABILITY: f64 = 0.1;

pub fn create_workload(
    name: &str,
    options: &HashMap<String, toml::Value>,
    model: Rc<RefCell<ModelState>>,
) -> Box<dyn Workload> {
    for factory in inventory::iter::<WorkloadFactory> {
        if factory.name == name {
            return (factory.create)(options, model);
        }
    }
    panic!("unknown workload: {name}");
}

/// Drive the resolve/observe loop for an operation, including all prerequisites.
pub async fn execute(op: Operation, model: &Rc<RefCell<ModelState>>, harness: &TestHarness) {
    loop {
        let step = model.borrow().resolve(op);
        let (stmt, is_ready) = match step {
            Ok(Step::Prerequisite(s)) => (s, false),
            Ok(Step::Ready(s)) => (s, true),
            Err(e) => {
                warn!("operation not possible: {e:?}");
                return;
            }
        };
        let rsp = send_stmt(harness, stmt).await;
        model.borrow_mut().observe(rsp);
        if is_ready {
            return;
        }
    }
}

async fn send_stmt(harness: &TestHarness, stmt: Statement) -> StatementResult {
    match stmt {
        Statement::CreateWorker(req) => harness.create_worker(req).await.unwrap(),
        Statement::DropWorker(req) => harness.drop_worker(req).await.unwrap(),
        other => harness.send(other).await.unwrap(),
    }
}

pub fn inject_failure_workloads(workloads: &mut Vec<Box<dyn Workload>>) {
    let mut rng = madsim::rand::thread_rng();
    for factory in inventory::iter::<FailureInjectorFactory> {
        let mut count = 0;
        while count < MAX_FAILURE_INJECTIONS_PER_TYPE
            && (factory.should_inject)(count)
            && rng.gen_bool((BASE_INJECTION_PROBABILITY / (1.0 + count as f64)).clamp(0.0, 1.0))
        {
            let w = (factory.create)();
            info!("auto-injecting failure workload: {}", w.name());
            workloads.push(w);
            count += 1;
        }
    }
}
