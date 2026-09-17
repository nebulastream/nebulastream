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

//! Workloads/Faultloads.
//!
//! A workload is the unit of behavior that composes a trial.
//! There are two kinds:
//!
//!  - Driver workloads (`ops`) submit catalog operations for queries or workers.
//!  - Failure-injection workloads (`attrition`, `clog`, `stall`, `degradation`)
//!    degrade/stop the network or nodes for a defined duration.
//!
//! A trial selects its workloads by name in TOML.

pub mod attrition;
pub mod clog;
pub mod degradation;
pub mod ops;
pub mod stall;

use crate::config::WorkloadSpec;
use crate::harness::TestHarness;
use crate::model_state::{ModelState, Operation, Step};
use madsim::rand::seq::SliceRandom;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use model::statement::{ParsedStatement, StatementResult};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, warn};

pub fn gen_delays(num_ops: usize, lo: u64, hi: u64) -> Vec<Duration> {
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

/// Picks the nodes that a fault hits: the share `rate` of the candidates, and at least one.
/// With no victim the trial would pass without having injected the fault.
pub fn pick_victims(candidates: &[NodeId], rate: f64) -> Vec<NodeId> {
    assert!(
        !candidates.is_empty(),
        "a fault workload started before any node it could hit existed"
    );
    let count = ((candidates.len() as f64 * rate).round() as usize).clamp(1, candidates.len());
    candidates
        .choose_multiple(&mut thread_rng(), count)
        .copied()
        .collect()
}

impl WorkloadSpec {
    /// Runs concurrently with the other workloads of the trial.
    pub async fn run(&self, model: &Rc<RefCell<ModelState>>, harness: &TestHarness) {
        use Operation::{CreateQuery, CreateWorker, DropQuery, DropWorker};
        match self {
            Self::Worker(cfg) => {
                ops::run("Worker", cfg, CreateWorker, DropWorker, model, harness).await;
            }
            Self::Query(cfg) => {
                ops::run("Query", cfg, CreateQuery, DropQuery, model, harness).await;
            }
            Self::Attrition(attrition) => attrition.run(harness).await,
            Self::Clog(clog) => clog.run(harness).await,
            Self::Stall(stall) => stall.run(harness).await,
            Self::Degradation(degradation) => degradation.run(harness).await,
        }
    }
}

/// Sends the statements for an operation, prerequisites first, and folds each response into the model.
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

async fn send_stmt(harness: &TestHarness, stmt: ParsedStatement) -> StatementResult {
    match stmt {
        ParsedStatement::CreateWorker(req) => harness.create_worker(req).await.unwrap(),
        ParsedStatement::DropWorker(req) => harness.drop_worker(req).await.unwrap(),
        other => harness.send(other).await.unwrap(),
    }
}
