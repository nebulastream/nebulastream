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

#[cfg(madsim)]
mod worker;
#[cfg(madsim)]
mod harness;
#[cfg(madsim)]
mod spec;
#[cfg(madsim)]
mod runner;
#[cfg(madsim)]
mod workload;

use libtest_mimic::{Arguments, Trial};

#[cfg(madsim)]
use madsim::runtime::Runtime;
#[cfg(madsim)]
use std::env;
#[cfg(madsim)]
use std::fs;
#[cfg(madsim)]
use std::path::Path;
#[cfg(madsim)]
use std::time::{SystemTime, UNIX_EPOCH};

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
    use spec::TestFile;

    let config_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/simulation/configs");

    let mut trials = Vec::new();

    let mut entries: Vec<_> = fs::read_dir(&config_dir)
        .unwrap_or_else(|e| panic!("failed to read config dir {}: {e}", config_dir.display()))
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == "toml")
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let stem = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

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

            trials.push(Trial::test(name, move || {
                run_trial(spec);
                Ok(())
            }));
        }
    }

    trials
}

#[cfg(madsim)]
fn run_trial(spec: spec::TestSpec) {
    let seed: u64 = env::var("MADSIM_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });

    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            runner::run_test(spec).await;
        });
    }));

    if let Err(e) = result {
        eprintln!("MADSIM_TEST_SEED={seed}");
        std::panic::resume_unwind(e);
    }
}
