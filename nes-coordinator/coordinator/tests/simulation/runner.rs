#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::spec::TestSpec;
use crate::workload::{Workload, create_workload, inject_failure_workloads};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use model::worker::CreateWorker;
use tracing::info;

pub async fn run_test(spec: TestSpec) {
    let seed = Handle::current().seed();
    let title = spec.title.as_deref().unwrap_or("unnamed");
    let timeout = spec.timeout();

    info!("=== simulation test: {title} (seed={seed}, timeout={timeout:?}) ===");

    let should_buggify = spec
        .buggify
        .unwrap_or_else(|| thread_rng().gen_bool(TestSpec::buggify_probability()));
    if should_buggify {
        madsim::buggify::enable();
        info!("buggify enabled");
    }

    let mut workloads: Vec<Box<dyn Workload>> = match spec.workload {
        Some(ref entries) => entries
            .iter()
            .map(|entry| create_workload(&entry.name, &entry.options))
            .collect(),
        None => Vec::new(),
    };

    if spec.run_failure_workloads() {
        inject_failure_workloads(&mut workloads);
    }

    let min_workers = workloads.iter().map(|w| w.min_workers()).max().unwrap_or(1);
    let topology = arb(CreateWorker::topology(min_workers));

    let network = spec.network.clone();
    let harness = TestHarness::start(&topology, network).await.unwrap();

    if workloads.is_empty() {
        info!("no workloads configured, skipping");
        return;
    }

    info!(
        "workloads: [{}]",
        workloads
            .iter()
            .map(|w| w.name())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for w in &mut workloads {
        w.setup(&harness).await;
    }

    let deadline = tokio::time::Instant::now() + timeout;

    join_all(workloads.iter().map(|w| w.start(&harness))).await;
    info!("workloads finished");

    tokio::time::sleep_until(deadline).await;
    info!("simulation window elapsed, running checks");

    for w in &workloads {
        w.check(&harness).await;
        info!("check {}: ok", w.name());
    }

    info!("=== simulation test PASSED (seed={seed}) ===");
}
