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

//! Simulation test entry point.
//!
//! Each `.toml` file under `tests/simulation/configs` defines one or more
//! deterministic trials. A trial picks a seed, builds a fresh madsim
//! runtime, runs the configured workloads against an in-process
//! coordinator and a swarm of fake workers, and asserts invariants at
//! the end.
//!
//! Trials run on a worker thread under a wall-clock timeout so a stuck
//! simulation cannot hang the whole test binary. Seeds are surfaced on
//! every failure so a flake can be replayed with `MADSIM_TEST_SEED=...`.
//!
//! The crate has to compile without madsim (clippy, normal builds,
//! tooling) so most of the test code is gated on `cfg(madsim)`; the
//! non-madsim build produces an empty trial list.

#[cfg(madsim)]
mod config;
#[cfg(madsim)]
mod harness;
#[cfg(madsim)]
mod invariant;
mod model_state;
#[cfg(madsim)]
mod runner;
#[cfg(madsim)]
mod worker;
#[cfg(madsim)]
mod workload;

use libtest_mimic::{Arguments, Trial};

#[cfg(madsim)]
use {
    madsim::runtime::Runtime,
    std::env,
    std::fs,
    std::path::Path,
    std::sync::mpsc,
    std::thread,
    std::time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(madsim)]
const DEFAULT_WALL_TIMEOUT_SECS: u64 = 90;

fn main() {
    let args = Arguments::from_args();

    #[cfg(madsim)]
    let trials = discover_trials();

    #[cfg(not(madsim))]
    let trials: Vec<Trial> = vec![];

    libtest_mimic::run(&args, trials).exit();
}

#[cfg(madsim)]
fn discover_trials() -> Vec<Trial> {
    use config::TestFile;

    let config_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/simulation/configs");

    let mut trials = Vec::new();

    let mut entries: Vec<_> = fs::read_dir(&config_dir)
        .unwrap_or_else(|e| panic!("failed to read config dir {}: {e}", config_dir.display()))
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "toml"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    let env_seed: Option<u64> = env::var("MADSIM_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok());

    let mut trial_index: u64 = 0;

    for entry in entries {
        let path = entry.path();
        let stem = path.file_stem().unwrap().to_str().unwrap().to_string();

        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let file: TestFile = toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

        let num_tests = file.test.len();

        for (i, spec) in file.test.into_iter().enumerate() {
            let name = match &spec.title {
                Some(t) => format!("{stem}::{t}"),
                None if num_tests == 1 => stem.clone(),
                None => format!("{stem}::test_{i}"),
            };

            let seed = env_seed.unwrap_or_else(|| {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                nanos ^ trial_index.wrapping_mul(0x9E3779B97F4A7C15)
            });
            trial_index += 1;

            let trial_name = name.clone();
            trials.push(Trial::test(name, move || {
                run_trial(&trial_name, spec, seed);
                Ok(())
            }));
        }
    }

    trials
}

#[cfg(madsim)]
fn run_trial(name: &str, spec: config::TestConfig, seed: u64) {
    let wall_timeout = env::var("SIM_WALL_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_WALL_TIMEOUT_SECS));

    let (tx, rx) = mpsc::channel();
    let _ = thread::Builder::new()
        .name(format!("sim-{name}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
                rt.block_on(async {
                    runner::run_test(spec).await;
                });
            }));
            let _ = tx.send(result);
        })
        .expect("failed to spawn trial worker thread");

    match rx.recv_timeout(wall_timeout) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => {
            print_repro(name, seed, "FAILED");
            std::panic::resume_unwind(payload);
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            print_repro(
                name,
                seed,
                &format!("TIMED OUT after {}s", wall_timeout.as_secs()),
            );
            panic!(
                "trial {name} exceeded wall-clock timeout of {}s",
                wall_timeout.as_secs()
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            print_repro(name, seed, "ABORTED (worker disconnected)");
            panic!("trial {name} worker thread disconnected without sending a result");
        }
    }
}

#[cfg(madsim)]
fn print_repro(name: &str, seed: u64, status: &str) {
    eprintln!();
    eprintln!("=== {name} {status} (seed={seed}) ===");
    eprintln!("  reproduce with:");
    eprintln!("    MADSIM_TEST_SEED={seed} cargo test --test simulation -- --exact '{name}'");
    eprintln!();
}
