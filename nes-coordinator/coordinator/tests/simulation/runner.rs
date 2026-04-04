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
use crate::config::TestConfig;
use crate::harness::TestHarness;
use crate::model_state::ModelState;
use crate::workload::{Workload, create_workload, inject_failure_workloads};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use model::query::GetQuery;
use model::sink::GetSink;
use model::source::physical::GetPhysicalSource;
use model::statement::Statement;
use model::worker::GetWorker;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tracing::info;

pub async fn run_test(cfg: TestConfig) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .try_init();

    let seed = Handle::current().seed();
    let title = cfg.title.as_deref().unwrap_or("unnamed");
    let timeout = cfg.timeout();

    info!("=== simulation test: {title} (seed={seed}, timeout={timeout:?}) ===");

    let should_buggify = cfg
        .buggify
        .unwrap_or_else(|| thread_rng().gen_bool(TestConfig::buggify_probability()));
    if should_buggify {
        madsim::buggify::enable();
        info!("buggify enabled");
    }

    let buggify_end = cfg.buggify_end.map(Duration::from_secs);

    let model = Rc::new(RefCell::new(ModelState::default()));

    let mut workloads: Vec<Box<dyn Workload>> = match cfg.workload {
        Some(ref entries) => entries
            .iter()
            .map(|entry| create_workload(&entry.name, &entry.options, model.clone()))
            .collect(),
        None => Vec::new(),
    };

    if cfg.run_failure_workloads() {
        inject_failure_workloads(&mut workloads);
    }

    let harness = TestHarness::start(cfg.network).await;

    info!(
        "workloads: [{}]",
        workloads
            .iter()
            .map(|workload| workload.name())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for workload in &mut workloads {
        workload.setup(&harness).await;
    }

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
        workloads
            .iter_mut()
            .map(|workload| workload.start(&harness)),
    )
    .await;
    info!("workloads finished");

    tokio::time::sleep_until(deadline).await;
    info!("simulation window elapsed, running checks");

    for workload in &workloads {
        workload.check(&harness).await;
        info!("check {}: ok", workload.name());
    }

    // println!(
    //     "{:#}",
    //     harness
    //         .send(Statement::GetWorker(GetWorker::all()))
    //         .await
    //         .unwrap()
    // );
    // println!(
    //     "{:#}",
    //     harness
    //         .send(Statement::GetQuery(GetQuery::all().with_fragments()))
    //         .await
    //         .unwrap()
    // );
    //
    // println!(
    //     "{:#}",
    //     harness
    //         .send(Statement::GetPhysicalSource(GetPhysicalSource::all()))
    //         .await
    //         .unwrap()
    // );
    // println!(
    //     "{:#}",
    //     harness
    //         .send(Statement::GetSink(GetSink::all()))
    //         .await
    //         .unwrap()
    // );

    info!("=== simulation test PASSED (seed={seed}) ===");
}
