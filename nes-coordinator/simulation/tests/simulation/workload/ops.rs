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
//! A driver runs `num_ops` operations, each create or drop by `create_weight` and `drop_weight`,
//! spread across `[begin, end]` with random delays.
//! Every operation is resolved through the model state so it runs against a well-formed catalog.

use crate::harness::TestHarness;
use crate::model_state::{ModelState, Operation};
use crate::workload::{execute, gen_delays, pick_weighted};
use serde::Deserialize;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpsConfig {
    #[serde(default)]
    pub begin: u64,
    pub end: u64,
    pub num_ops: usize,
    pub create_weight: u32,
    pub drop_weight: u32,
}

pub async fn run(
    name: &str,
    cfg: &OpsConfig,
    create: Operation,
    drop: Operation,
    model: &Rc<RefCell<ModelState>>,
    harness: &TestHarness,
) {
    info!(
        "{name}: running {} ops between ({}s..{}s)",
        cfg.num_ops, cfg.begin, cfg.end
    );

    tokio::time::sleep(Duration::from_secs(cfg.begin)).await;

    for delay in gen_delays(cfg.num_ops, cfg.begin, cfg.end) {
        tokio::time::sleep(delay).await;

        let op = pick_weighted(&[(create, cfg.create_weight), (drop, cfg.drop_weight)]);
        execute(op, model, harness).await;
    }

    info!("{name}: completed {} ops", cfg.num_ops);
}
