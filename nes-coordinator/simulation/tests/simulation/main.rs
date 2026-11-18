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
//! Each trial runs on its own thread under a limit of simulated time, so a stuck simulation fails at the same point for the same seed.
//! A wall-clock watchdog aborts the whole process if a trial stops returning to the scheduler, which the time limit cannot see.
//! Every failure prints its seed, which `MADSIM_TEST_SEED=...` replays.
//! The binary also runs unit trials of the harness itself.
//!
//! The crate has to compile without madsim (clippy, normal builds, tooling),
//! so most of the test code is behind `cfg(madsim)` and the non-madsim build produces an empty trial list.

#![cfg_attr(
    madsim,
    expect(
        clippy::future_not_send,
        reason = "madsim runs every trial on one thread, and workloads share the model through a RefCell"
    )
)]

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
mod unit;
#[cfg(madsim)]
mod worker;
#[cfg(madsim)]
mod workload;

use libtest_mimic::{Arguments, Trial};

#[cfg(madsim)]
use {
    ahash::AHasher,
    config::{TrialConfig, TrialFile},
    madsim::Config,
    madsim::runtime::Runtime,
    std::env,
    std::ffi::OsStr,
    std::fs::{self, DirEntry},
    std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    std::path::Path,
    std::process,
    std::sync::mpsc::{self, RecvTimeoutError},
    std::thread,
    std::time::{Duration, SystemTime, UNIX_EPOCH},
    tokio_real::runtime::{Builder as TokioRuntimeBuilder, RngSeed, Runtime as TokioRuntime},
    tokio_real::task::unconstrained,
};

#[cfg(all(madsim, not(tokio_unstable)))]
compile_error!("build the simulation with RUSTFLAGS=\"--cfg madsim --cfg tokio_unstable\"");

/// Simulated time that the checks get after the trial's timeout.
/// It is longer than the harness's reply timeout, so a stuck statement fails with its own message first.
#[cfg(madsim)]
const CHECK_TIME_BUDGET: Duration = Duration::from_secs(60);

#[cfg(madsim)]
const DEFAULT_WALL_TIMEOUT_SECS: u64 = 90;

fn main() {
    let args = Arguments::from_args();

    #[cfg(madsim)]
    init_process_wide_hash_keys();

    #[cfg(madsim)]
    let trials: Vec<Trial> = discover_trials()
        .into_iter()
        .chain(unit::build_trials())
        .collect();

    #[cfg(not(madsim))]
    let trials: Vec<Trial> = vec![];

    libtest_mimic::run(&args, trials).exit();
}

/// Sets the process-wide hash keys of `ahash` from a fixed seed, before any trial runs.
/// `ahash` reads them once per process from the system's entropy source,
/// which the simulator replaces with the seeded random stream of the running trial.
/// Otherwise the first trial to hash a value would spend part of its stream on the keys,
/// and every later trial would inherit keys that depend on that trial's seed.
#[cfg(madsim)]
fn init_process_wide_hash_keys() {
    Runtime::with_seed_and_config(0, Config::default()).block_on(async {
        let _ = AHasher::default();
    });
}

#[cfg(madsim)]
fn discover_trials() -> Vec<Trial> {
    let config_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/simulation/configs");

    let mut trials = Vec::new();

    let mut entries: Vec<_> = fs::read_dir(&config_dir)
        .unwrap_or_else(|e| panic!("failed to read config dir {}: {e}", config_dir.display()))
        .map(|e| {
            e.unwrap_or_else(|e| panic!("failed to read an entry of {}: {e}", config_dir.display()))
        })
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "toml"))
        .collect();
    entries.sort_by_key(DirEntry::file_name);

    let env_seed: Option<u64> = env::var("MADSIM_TEST_SEED").ok().map(|s| {
        s.parse()
            .unwrap_or_else(|e| panic!("MADSIM_TEST_SEED={s} is not a seed: {e}"))
    });

    let wall_timeout = env::var("SIM_WALL_TIMEOUT_SECS").map_or(
        Duration::from_secs(DEFAULT_WALL_TIMEOUT_SECS),
        |s| {
            Duration::from_secs(s.parse().unwrap_or_else(|e| {
                panic!("SIM_WALL_TIMEOUT_SECS={s} is not a number of seconds: {e}")
            }))
        },
    );

    for entry in entries {
        let path = entry.path();
        let stem = path
            .file_stem()
            .and_then(OsStr::to_str)
            .unwrap_or_else(|| panic!("{} has no UTF-8 file name", path.display()));

        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let file: TrialFile = toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

        for cfg in file.trials {
            cfg.validate()
                .unwrap_or_else(|e| panic!("invalid trial in {}: {e}", path.display()));
            let name = format!("{stem}::{}", cfg.title);

            // No seed pinned: derive one per trial from the wall clock.
            // Trials can start in the same nanosecond, so xor in the trial index times a large odd constant to keep seeds distinct.
            let seed = env_seed.unwrap_or_else(|| {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                nanos ^ (trials.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            });

            trials.push(Trial::test(name.clone(), move || {
                run_trial(&name, cfg, seed, wall_timeout);
                Ok(())
            }));
        }
    }

    trials
}

/// The trial runs on a fresh thread, see `run_seeded`.
/// The runtime panics once simulated time passes the trial's timeout plus the check budget.
/// That limit is checked between scheduler steps, so it misses tasks that wake each other without letting time pass.
/// The wall-clock watchdog catches those and aborts the process,
/// because the stuck thread cannot be stopped and would keep a CPU busy for the remaining trials.
#[cfg(madsim)]
fn run_trial(name: &str, cfg: TrialConfig, seed: u64, wall_timeout: Duration) {
    let trial_name = name.to_string();
    let (tx, rx) = mpsc::channel();
    let _ = thread::Builder::new()
        .name(format!("sim-{name}"))
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_seeded(
                    seed,
                    Some(Duration::from_secs(cfg.timeout) + CHECK_TIME_BUDGET),
                    runner::run(&trial_name, &cfg),
                );
            }));
            let _ = tx.send(result);
        })
        .expect("failed to spawn trial thread");

    match rx.recv_timeout(wall_timeout) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => {
            print_repro(name, seed, "FAILED");
            resume_unwind(payload);
        }
        Err(RecvTimeoutError::Timeout) => {
            print_repro(
                name,
                seed,
                &format!("TIMED OUT after {wall_timeout:?} of wall-clock time"),
            );
            eprintln!("aborting: the stuck trial thread cannot be stopped");
            process::abort();
        }
        Err(RecvTimeoutError::Disconnected) => {
            print_repro(name, seed, "ABORTED (trial thread disconnected)");
            panic!("trial {name} thread disconnected without sending a result");
        }
    }
}

/// Runs `body` in a madsim runtime with the given seed, inside a tokio runtime with the same seed.
/// Call it only on a thread that has not run a simulation or hashed a value before:
/// madsim seeds std's hash keys and tokio seeds its random generator once per thread,
/// so on a reused thread the result for a seed depends on what ran before on that thread.
#[cfg(madsim)]
fn run_seeded<F: Future>(seed: u64, time_limit: Option<Duration>, body: F) -> F::Output {
    let mut rt = Runtime::with_seed_and_config(seed, Config::default());
    if let Some(limit) = time_limit {
        rt.set_time_limit(limit);
    }
    build_seeded_tokio_runtime(seed).block_on(unconstrained(async { rt.block_on(body) }))
}

/// A tokio runtime that only seeds the trial thread's tokio random generator.
/// Nothing is spawned on it.
/// The simulator replaces tokio's runtime, but `select!` and watch channels are still tokio's own,
/// and they pick branches and waiter slots from a per-thread generator.
/// Tokio seeds that generator from a process-wide counter, so without this a trial would behave differently
/// depending on how many trials started before it in the process.
/// The only public way to seed it is a runtime's seed, which needs `--cfg tokio_unstable`.
/// The trial has to run without tokio's cooperative budget inside this context,
/// because nothing resets the budget between the simulator's task polls.
#[cfg(madsim)]
fn build_seeded_tokio_runtime(seed: u64) -> TokioRuntime {
    TokioRuntimeBuilder::new_current_thread()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
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
