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
//! Each `.toml` file under `tests/simulation/configs` defines one or more deterministic trials.
//! A trial picks a seed, builds a fresh madsim runtime,
//! runs the configured workloads against an in-process coordinator and a set of fake workers,
//! and asserts invariants at the end.
//!
//! Trials run on a worker thread under a wall-clock timeout so a stuck simulation cannot hang the whole test binary.
//! The seed is printed on every failure so a flake can be replayed with `MADSIM_TEST_SEED=...`.
//!
//! The crate has to compile without madsim (clippy, normal builds, tooling),
//! so most of the test code is behind `cfg(madsim)` and the non-madsim build produces an empty trial list.

#[cfg(madsim)]
mod config;
#[cfg(madsim)]
mod harness;
#[cfg(madsim)]
mod invariant;
#[cfg(madsim)]
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

#[cfg(all(madsim, not(tokio_unstable)))]
compile_error!("build the simulation with RUSTFLAGS=\"--cfg madsim --cfg tokio_unstable\"");

#[cfg(madsim)]
const DEFAULT_WALL_TIMEOUT_SECS: u64 = 90;

fn main() {
    let args = Arguments::from_args();

    #[cfg(madsim)]
    init_process_wide_hash_keys();

    #[cfg(madsim)]
    let trials = discover_trials();

    #[cfg(not(madsim))]
    let trials: Vec<Trial> = vec![];

    libtest_mimic::run(&args, trials).exit();
}

/// Sets the process-wide hash keys of `ahash` from a fixed seed, before any trial runs.
/// `ahash` reads them once per process from the system's entropy source,
/// which the simulator replaces with the seeded random stream of the running trial.
/// Without this call the first trial to hash a value would spend part of its stream on the keys,
/// and every later trial would inherit keys that depend on that trial's seed.
/// A trial would then behave differently depending on which trials ran before it in the process.
#[cfg(madsim)]
fn init_process_wide_hash_keys() {
    Runtime::with_seed_and_config(0, madsim::Config::default()).block_on(async {
        let _ = ahash::AHasher::default();
    });
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

    let env_seed: Option<u64> = env::var("MADSIM_TEST_SEED").ok().map(|s| {
        s.parse()
            .unwrap_or_else(|e| panic!("MADSIM_TEST_SEED={s} is not a seed: {e}"))
    });

    let mut trial_index: u64 = 0;

    for entry in entries {
        let path = entry.path();
        let stem = path.file_stem().unwrap().to_str().unwrap().to_string();

        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let file: TestFile = toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

        for spec in file.test {
            spec.validate();
            let name = format!("{stem}::{}", spec.title);

            // No seed pinned: derive one per trial from the wall clock.
            // Trials can start in the same nanosecond, so xor in the trial index times a large odd constant to keep seeds distinct.
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

    let trial = name.to_string();
    let (tx, rx) = mpsc::channel();
    let _ = thread::Builder::new()
        .name(format!("sim-{name}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
                seeded_tokio_context(seed).block_on(tokio_real::task::unconstrained(async {
                    rt.block_on(runner::run_test(&trial, spec))
                }));
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

/// A tokio runtime that only gives the trial's thread a seeded tokio random generator; nothing is spawned on it.
/// The simulator replaces tokio's runtime, but `select!` and watch channels are still tokio's own,
/// and they pick branches and waiter slots from a per-thread generator.
/// Tokio seeds that generator from a process-wide counter, so without this a trial would behave differently
/// depending on how many trials started before it in the process.
/// The only public way to seed it is a runtime's seed, which needs `--cfg tokio_unstable`.
/// The trial has to run without tokio's cooperative budget inside this context,
/// because nothing resets the budget between the simulator's task polls.
#[cfg(madsim)]
fn seeded_tokio_context(seed: u64) -> tokio_real::runtime::Runtime {
    tokio_real::runtime::Builder::new_current_thread()
        .rng_seed(tokio_real::runtime::RngSeed::from_bytes(
            &seed.to_le_bytes(),
        ))
        .build()
        .expect("failed to build the tokio runtime")
}

#[cfg(madsim)]
fn print_repro(name: &str, seed: u64, status: &str) {
    eprintln!();
    eprintln!("=== {name} {status} (seed={seed}) ===");
    eprintln!("  reproduce with:");
    eprintln!(
        "    RUSTFLAGS=\"--cfg madsim --cfg tokio_unstable\" MADSIM_TEST_SEED={seed} cargo test -p simulation --test simulation -- --exact '{name}'"
    );
    eprintln!();
}
