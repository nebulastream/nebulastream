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

//! Runs one trial: builds the harness, runs the workloads concurrently,
//! and runs the checks once the simulation window has elapsed.
//!
//! `buggify` is a fault-injection switch.
//! The controller and the fake worker have buggify points.
//! While buggify is on, a point fires at random but seed-reproducible moments and runs its fault, such as an early error or a slow reply.
//! This reaches failure paths that are otherwise rare.
//! A trial enables it at the start and disables it partway through, so the system can recover before the final checks.

use crate::config::TrialConfig;
use crate::harness::Harness;
use crate::invariant;
use crate::model_state::ModelState;
use futures::future::join_all;
use madsim::buggify;
use madsim::runtime::Handle;
use std::cell::RefCell;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::uptime;

#[expect(
    clippy::await_holding_refcell_ref,
    reason = "the model is only mutated while workloads run, the checks run after all of them finished, and madsim is single-threaded"
)]
pub async fn run(name: &str, cfg: &TrialConfig) {
    init_tracing();

    let seed = Handle::current().seed();
    let timeout = Duration::from_secs(cfg.timeout);

    info!("=== simulation test: {name} (seed={seed}, timeout={timeout:?}) ===");

    let model = RefCell::new(ModelState::default());

    let harness = Harness::start();

    let start = Instant::now();
    let deadline = start + timeout;

    schedule_buggify(cfg.buggify_until, start);

    join_all(
        cfg.workloads
            .iter()
            .map(|workload| workload.run(&model, &harness)),
    )
    .await;
    info!("workloads finished");

    tokio::time::sleep_until(deadline).await;
    info!("simulation window elapsed, running checks");

    invariant::check_all(&harness, &model.borrow(), cfg.min_failed_queries).await;

    info!("=== simulation test PASSED (seed={seed}) ===");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("off")),
        )
        .with_timer(uptime())
        .with_target(false)
        .try_init();
}

fn schedule_buggify(until: Option<u64>, start: Instant) {
    let Some(until) = until else {
        return;
    };
    buggify::enable();
    info!("buggify enabled");
    tokio::spawn(async move {
        tokio::time::sleep_until(start + Duration::from_secs(until)).await;
        buggify::disable();
        info!("buggify disabled");
    });
}
