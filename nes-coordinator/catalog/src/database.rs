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

use crate::error::Retryable;
use anyhow::Result;
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection, DbErr};
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::warn;

const SQLITE_MAX_CONNECTIONS: u32 = 8;
const BASE_RETRY_DURATION_MS: u64 = 10;
const RETRY_BACKOFF_FACTOR: u64 = 2;
const MAX_RETRIES: usize = 5;

#[derive(Default)]
pub enum StateBackend {
    #[default]
    Memory,
    Sqlite {
        endpoint: String,
        opts: ConnectOptions,
    },
}

impl StateBackend {
    pub fn sqlite(path: &str) -> Self {
        let endpoint = format!("sqlite:{path}?mode=rwc");
        let opts = ConnectOptions::new(&endpoint);
        Self::Sqlite { endpoint, opts }
    }
}

#[derive(Clone)]
pub struct Database {
    pub(crate) conn: DatabaseConnection,
}

impl Database {
    pub async fn with(backend: StateBackend) -> Result<Self> {
        const MAX_DURATION: Duration = Duration::new(u64::MAX / 4, 0);

        match backend {
            StateBackend::Memory => {
                const IN_MEMORY_DB: &str = "sqlite::memory:";

                let mut opts = ConnectOptions::new(IN_MEMORY_DB);
                opts.min_connections(1)
                    .max_connections(1)
                    .acquire_timeout(MAX_DURATION)
                    .connect_timeout(MAX_DURATION)
                    .idle_timeout(MAX_DURATION)
                    .max_lifetime(MAX_DURATION)
                    .test_before_acquire(false)
                    .sqlx_logging(false);
                let conn = sea_orm::Database::connect(opts).await?;
                Ok(Self { conn })
            }
            StateBackend::Sqlite { endpoint: _, opts } => {
                let mut opts = opts;
                opts.min_connections(1)
                    .max_connections(SQLITE_MAX_CONNECTIONS)
                    .acquire_timeout(MAX_DURATION)
                    .connect_timeout(MAX_DURATION)
                    .idle_timeout(MAX_DURATION)
                    .max_lifetime(MAX_DURATION)
                    .sqlx_logging(false);
                let conn = sea_orm::Database::connect(opts).await?;
                Ok(Self { conn })
            }
        }
    }

    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory)
            .await
            .expect("could not connect to the database");
        Migrator::up(&this.conn, None)
            .await
            .expect("could not apply migrations");
        this
    }

    pub async fn migrate(&self) -> Result<()> {
        Migrator::up(&self.conn, None).await?;
        Ok(())
    }

    pub async fn with_retry<F, Fut, T>(&self, op: F) -> Result<T, DbErr>
    where
        F: Fn(DatabaseConnection) -> Fut,
        Fut: Future<Output = Result<T, DbErr>>,
    {
        RetryIf::spawn(
            ExponentialBackoff::from_millis(BASE_RETRY_DURATION_MS)
                .factor(RETRY_BACKOFF_FACTOR)
                .map(jitter)
                .take(MAX_RETRIES),
            || op(self.conn.clone()),
            |err: &DbErr| {
                if err.retryable() {
                    warn!("retrying: {err}");
                    return true;
                }
                false
            },
        )
        .await
    }
}
