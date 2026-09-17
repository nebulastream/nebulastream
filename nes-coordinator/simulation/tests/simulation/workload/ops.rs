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

//! Driver workloads: create and drop traffic for queries or for workers.
//!
//! A driver runs `num_ops` operations at random offsets inside `[begin, end]`.
//! Each operation is a create or a drop, picked by weight:
//! `create_weight = 3` and `drop_weight = 1` make each operation a create with probability 3/4.

use crate::harness::Harness;
use crate::ledger::{Ledger, Operation};
use crate::workload::pick_offsets;
use madsim::rand::seq::SliceRandom;
use madsim::rand::thread_rng;
use serde::Deserialize;
use std::cell::RefCell;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

/// An operation without a target, such as a drop with nothing to drop, is skipped with a warning.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsWorkload {
    #[serde(default)]
    pub begin: u64,
    pub end: u64,
    pub num_ops: usize,
    pub create_weight: u32,
    pub drop_weight: u32,
}

impl OpsWorkload {
    pub async fn run(
        &self,
        name: &str,
        create: Operation,
        drop: Operation,
        ledger: &RefCell<Ledger>,
        harness: &Harness,
    ) {
        info!(
            "{name}: running {} ops between ({}s..{}s)",
            self.num_ops, self.begin, self.end
        );

        tokio::time::sleep(Duration::from_secs(self.begin)).await;

        let window_start = Instant::now();
        for offset in pick_offsets(self.num_ops, self.end - self.begin) {
            tokio::time::sleep_until(window_start + offset).await;

            let &(op, _) = [(create, self.create_weight), (drop, self.drop_weight)]
                .choose_weighted(&mut thread_rng(), |&(_, weight)| weight)
                .expect("validated weights include a non-zero one");
            let planned = ledger.borrow_mut().plan(op);
            let Some((statement, sent)) = planned else {
                warn!("{name}: {op:?} has no target");
                continue;
            };
            let reply = harness.send(statement).await;
            ledger.borrow_mut().record(sent, reply);
        }

        info!("{name}: completed {} ops", self.num_ops);
    }
}
