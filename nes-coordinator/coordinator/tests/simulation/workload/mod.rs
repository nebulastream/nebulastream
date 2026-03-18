#![cfg(madsim)]

pub mod attrition;
pub mod cluster;
pub mod degradation;
pub mod invariant;
pub mod partition;
pub mod query;

pub use invariant::{Invariant, InvariantContext, check_invariants};

use crate::harness::arb;
use async_trait::async_trait;
use crate::harness::TestHarness;
use madsim::rand::Rng;
use proptest::prelude::*;
use proptest::strategy::Union;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use tracing::info;

const JITTER_FRACTION: u64 = 2;

pub fn precompute_delays(num_ops: usize, duration: Duration) -> Vec<Duration> {
    if num_ops == 0 {
        return Vec::new();
    }
    let base_ms = duration.as_millis() as u64 / num_ops as u64;
    let half = base_ms / JITTER_FRACTION;
    let lo = base_ms.saturating_sub(half);
    let hi = base_ms + half;
    let mut rng = madsim::rand::thread_rng();
    (0..num_ops)
        .map(|_| Duration::from_millis(rng.gen_range(lo..=hi)))
        .collect()
}

pub fn precompute_times(count: usize, start: Duration, span: Duration) -> Vec<Duration> {
    if count == 0 {
        return Vec::new();
    }
    let start_ms = start.as_millis() as u64;
    let slot_ms = span.as_millis() as u64 / count as u64;
    let mut rng = madsim::rand::thread_rng();
    (0..count)
        .map(|i| {
            let slot_start = start_ms + (i as u64) * slot_ms;
            Duration::from_millis(rng.gen_range(slot_start..=slot_start + slot_ms))
        })
        .collect()
}


pub fn generate_weighted<Op: Clone + Debug>(strategies: Vec<(u32, BoxedStrategy<Op>)>) -> Op {
    let strategy = Union::new_weighted(strategies);
    arb(strategy)
}

#[async_trait(?Send)]
pub trait Workload {
    fn name(&self) -> &str;

    fn min_workers(&self) -> u8 {
        1
    }

    async fn setup(&mut self, _harness: &TestHarness) {}

    async fn start(&self, harness: &TestHarness);

    async fn check(&self, _harness: &TestHarness) {}
}

pub struct WorkloadFactory {
    pub name: &'static str,
    pub create: fn(&HashMap<String, toml::Value>) -> Box<dyn Workload>,
}

pub fn parse_options<T: DeserializeOwned + Default>(
    options: &HashMap<String, toml::Value>,
) -> T {
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
) -> Box<dyn Workload> {
    for factory in inventory::iter::<WorkloadFactory> {
        if factory.name == name {
            return (factory.create)(options);
        }
    }
    panic!("unknown workload: {name}");
}

pub fn inject_failure_workloads(workloads: &mut Vec<Box<dyn Workload>>) {
    let mut rng = madsim::rand::thread_rng();
    for factory in inventory::iter::<FailureInjectorFactory> {
        let mut count = 0;
        while count < MAX_FAILURE_INJECTIONS_PER_TYPE
            && (factory.should_inject)(count)
            && rng.gen_bool(
                (BASE_INJECTION_PROBABILITY / (1.0 + count as f64)).clamp(0.0, 1.0),
            )
        {
            let w = (factory.create)();
            info!("auto-injecting failure workload: {}", w.name());
            workloads.push(w);
            count += 1;
        }
    }
}
