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

use crate::Config;
use clap::{Parser, ValueEnum};
use model::database::StateBackend;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Parser)]
#[command(name = "nes-server")]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub listen: SocketAddr,

    #[arg(long)]
    pub db: Option<String>,

    #[arg(long, value_enum, default_value_t = WorkerMode::Remote)]
    pub worker_mode: WorkerMode,

    #[arg(long, default_value = "")]
    pub optimizer_config: String,

    #[arg(long, default_value_t = 30)]
    pub wait_cap_secs: u64,

    #[arg(long, default_value_t = coordinator::DEFAULT_REQUEST_QUEUE_CAPACITY)]
    pub request_queue: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WorkerMode {
    Embedded,
    Remote,
}

impl Args {
    #[must_use]
    pub const fn config(&self) -> Config {
        Config {
            wait_cap: Duration::from_secs(self.wait_cap_secs),
            queue_capacity: self.request_queue,
        }
    }

    #[must_use]
    pub fn state_backend(&self) -> StateBackend {
        match self.db.as_deref() {
            Some(path) if !path.is_empty() => StateBackend::sqlite(path),
            _ => StateBackend::Memory,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_run_an_in_memory_remote_coordinator() {
        let args = Args::try_parse_from(["nes-server"]).unwrap();
        assert_eq!(args.listen, "127.0.0.1:8081".parse().unwrap());
        assert_eq!(args.worker_mode, WorkerMode::Remote);
        assert!(matches!(args.state_backend(), StateBackend::Memory));
        let config = args.config();
        assert_eq!(config.wait_cap, Duration::from_secs(30));
        assert_eq!(
            config.queue_capacity,
            coordinator::DEFAULT_REQUEST_QUEUE_CAPACITY
        );
    }

    #[test]
    fn an_empty_db_path_keeps_the_catalog_in_memory() {
        let args = Args::try_parse_from(["nes-server", "--db", ""]).unwrap();
        assert!(matches!(args.state_backend(), StateBackend::Memory));
        let args = Args::try_parse_from(["nes-server", "--db", "/tmp/catalog.db"]).unwrap();
        assert!(matches!(args.state_backend(), StateBackend::Sqlite { .. }));
    }

    #[test]
    fn the_worker_mode_and_limits_are_configurable() {
        let args = Args::try_parse_from([
            "nes-server",
            "--worker-mode",
            "embedded",
            "--wait-cap-secs",
            "5",
            "--request-queue",
            "8",
        ])
        .unwrap();
        assert_eq!(args.worker_mode, WorkerMode::Embedded);
        assert_eq!(args.config().wait_cap, Duration::from_secs(5));
        assert_eq!(args.config().queue_capacity, 8);
    }
}
