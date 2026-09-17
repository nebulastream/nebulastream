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
//! The controller and the fake worker have buggify points; when buggify is on,
//! a point fires at random (but seed-reproducible) moments and runs its fault there,
//! for example an early error return or a slow reply.
//! This hits failure paths that are otherwise rare.
//! It is enabled at the start of a trial and disabled partway through,
//! so the system can recover before the final checks.

use crate::config::TestConfig;
use crate::harness::TestHarness;
use crate::invariant;
use crate::model_state::ModelState;
use futures::future::join_all;
use madsim::runtime::Handle;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tracing::info;

#[expect(
    clippy::await_holding_refcell_ref,
    reason = "the model is only mutated while workloads run, the checks run after all of them finished, and madsim is single-threaded"
)]
pub async fn run_test(name: &str, cfg: TestConfig) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
        )
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .try_init();

    let seed = Handle::current().seed();
    let timeout = Duration::from_secs(cfg.timeout);

    info!("=== simulation test: {name} (seed={seed}, timeout={timeout:?}) ===");

    if cfg.buggify_until.is_some() {
        madsim::buggify::enable();
        info!("buggify enabled");
    }

    let buggify_end = cfg.buggify_until.map(Duration::from_secs);

    // Using Rc is safe because madsim always runs within a single thread.
    let model = Rc::new(RefCell::new(ModelState::default()));

    let harness = TestHarness::start().await;

    let start = tokio::time::Instant::now();
    let deadline = start + timeout;

    if let Some(buggify_end) = buggify_end {
        let buggify_deadline = start + buggify_end;
        tokio::spawn(async move {
            tokio::time::sleep_until(buggify_deadline).await;
            madsim::buggify::disable();
            info!("buggify disabled");
        });
    }

    join_all(
        cfg.workload
            .iter()
            .map(|workload| workload.run(&model, &harness)),
    )
    .await;
    info!("workloads finished");

    tokio::time::sleep_until(deadline).await;
    info!("simulation window elapsed, running checks");

    invariant::check_all(&harness, &model.borrow()).await;

    let failed = model.borrow().failed_queries().len();
    assert!(
        failed >= cfg.min_queries_failed_by_worker_drop,
        "worker drops failed {failed} queries, expect at least {}",
        cfg.min_queries_failed_by_worker_drop,
    );
    info!("worker drops failed {failed} queries");

    info!("=== simulation test PASSED (seed={seed}) ===");
}
