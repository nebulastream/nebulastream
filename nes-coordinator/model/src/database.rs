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

use anyhow::Result;
use migration::Migrator;
pub use sea_orm::DatabaseTransaction;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr, SqlxSqliteConnector, TransactionTrait};
use sea_orm_migration::MigratorTrait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::str::FromStr;
use std::time::Duration;
use sqlx::ConnectOptions;

const SQLITE_MIN_CONNECTIONS: u32 = 1;
const SQLITE_MAX_CONNECTIONS: u32 = 8;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
pub enum StateBackend {
    #[default]
    Memory,
    Sqlite {
        path: String,
    },
}

impl StateBackend {
    pub fn sqlite(path: &str) -> Self {
        Self::Sqlite {
            path: path.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Database {
    pub conn: DatabaseConnection,
}

impl Database {
    pub async fn with(backend: StateBackend) -> Result<Self> {
        match backend {
            StateBackend::Memory => {
                let sqlite_opts = SqliteConnectOptions::from_str("sqlite::memory:")?
                    .foreign_keys(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .busy_timeout(SQLITE_BUSY_TIMEOUT);

                let pool = SqlitePoolOptions::new()
                    .min_connections(SQLITE_MIN_CONNECTIONS)
                    .max_connections(1)
                    .test_before_acquire(false)
                    .connect_with(sqlite_opts)
                    .await?;

                let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
                Ok(Self { conn })
            }
            StateBackend::Sqlite { path } => {
                let sqlite_opts = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))?
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .disable_statement_logging()
                    .busy_timeout(SQLITE_BUSY_TIMEOUT);

                let pool = SqlitePoolOptions::new()
                    .min_connections(SQLITE_MIN_CONNECTIONS)
                    .max_connections(SQLITE_MAX_CONNECTIONS)
                    .test_before_acquire(false)
                    .connect_with(sqlite_opts)
                    .await?;

                let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
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

    /// Begin an immediate transaction. Sea-orm's default `begin()` issues a
    /// deferred `BEGIN` which only acquires the write lock on the first write.
    /// This causes `SQLITE_BUSY_SNAPSHOT` (517) when a concurrent reader holds
    /// a stale WAL snapshot. `BEGIN IMMEDIATE` acquires the write lock upfront,
    /// letting `busy_timeout` handle contention instead of failing immediately.
    pub async fn begin(&self) -> Result<DatabaseTransaction, DbErr> {
        // Sea-orm's begin() issues `BEGIN` (deferred). We close that empty
        // transaction with `END` (alias for COMMIT) and re-open it as
        // `BEGIN IMMEDIATE` to acquire the write lock upfront.
        let txn = self.conn.begin().await?;
        txn.execute_unprepared("END; BEGIN IMMEDIATE").await?;
        Ok(txn)
    }
}
