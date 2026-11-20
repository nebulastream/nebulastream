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
//! Each `.toml` file under `tests/simulation/configs` defines one or more trials.
//! Every trial gets its own seed and a fresh madsim runtime,
//! and runs on a separate thread under a wall-clock timeout, so a stuck simulation cannot hang the whole test binary.
//! Every failed config trial prints its seed, which `MADSIM_TEST_SEED=...` replays.
//! The binary also runs the tests of the fake worker.
//!
//! The crate has to compile without madsim (clippy, normal builds, tooling),
//! so most of the test code is behind `cfg(madsim)` and the non-madsim build produces an empty trial list.

#![cfg_attr(
    madsim,
    expect(
        clippy::future_not_send,
        reason = "madsim runs every trial on one thread, and workloads share the ledger through a RefCell"
    )
)]

#[cfg(madsim)]
mod config;
#[cfg(madsim)]
mod harness;
#[cfg(madsim)]
mod invariant;
#[cfg(madsim)]
mod ledger;
#[cfg(madsim)]
mod runner;
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
    std::sync::mpsc,
    std::thread,
    std::time::{Duration, SystemTime, UNIX_EPOCH},
    tokio_real::runtime::{Builder as TokioRuntimeBuilder, RngSeed, Runtime as TokioRuntime},
    tokio_real::task::unconstrained,
};

#[cfg(all(madsim, not(tokio_unstable)))]
compile_error!("build the simulation with RUSTFLAGS=\"--cfg madsim --cfg tokio_unstable\"");

/// `SIM_WALL_TIMEOUT_SECS` overrides it.
#[cfg(madsim)]
const DEFAULT_WALL_TIMEOUT_SECS: u64 = 90;

fn main() {
    let args = Arguments::from_args();

    #[cfg(madsim)]
    init_process_wide_hash_keys();

    #[cfg(madsim)]
    let trials: Vec<Trial> = discover_trials()
        .into_iter()
        .chain(worker::tests::build_trials())
        .collect();

    #[cfg(not(madsim))]
    let trials: Vec<Trial> = vec![];

    libtest_mimic::run(&args, trials).exit();
}

/// Sets the process-wide hash keys of `ahash` from a fixed seed, before any trial runs.
/// `ahash` reads them once per process from the system's entropy source,
/// which the simulator replaces with the seeded random stream of the running trial.
/// Otherwise the first trial to hash a value would use part of its stream for the keys,
/// and every later trial would get keys that depend on that trial's seed.
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

            // Two seeds can be derived in the same nanosecond, so xor in the trial index times a large odd constant to keep seeds distinct.
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

#[cfg(madsim)]
fn run_trial(name: &str, cfg: TrialConfig, seed: u64, wall_timeout: Duration) {
    let trial_name = name.to_string();
    let (tx, rx) = mpsc::channel();
    let _ = thread::Builder::new()
        .name(format!("sim-{name}"))
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let rt = Runtime::with_seed_and_config(seed, Config::default());
                build_seeded_tokio_runtime(seed).block_on(unconstrained(async {
                    rt.block_on(runner::run(&trial_name, &cfg));
                }));
            }));
            let _ = tx.send(result);
        })
        .expect("failed to spawn trial worker thread");

    let Ok(result) = rx.recv_timeout(wall_timeout) else {
        print_repro(name, seed, &format!("TIMED OUT after {wall_timeout:?}"));
        panic!("trial {name} exceeded wall-clock timeout of {wall_timeout:?}");
    };
    if let Err(payload) = result {
        print_repro(name, seed, "FAILED");
        resume_unwind(payload);
    }
}

/// A tokio runtime that only seeds the trial thread's tokio random generator.
/// Nothing is spawned on it.
/// The simulator replaces tokio's runtime, but `select!` and watch channels are still tokio's own,
/// and they pick branches and waiter slots from a per-thread generator.
/// Tokio seeds that generator from a process-wide counter,
/// so without this a trial would behave differently depending on how many trials started before it in the process.
/// The only public way to seed it is a runtime's seed, which needs `--cfg tokio_unstable`.
/// NOTE: The trial has to run without tokio's cooperative budget inside this context,
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
