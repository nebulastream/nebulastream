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
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, ExecResult, QueryResult,
    SqlxSqliteConnector, Statement, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;
use sqlx::ConnectOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::str::FromStr;
use std::time::Duration;

const SQLITE_MIN_CONNECTIONS: u32 = 1;
const SQLITE_MAX_CONNECTIONS: u32 = 8;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Selects the SQLite backing store: an ephemeral in-memory database
/// (for tests and short-lived coordinators) or a file at `path`.
pub enum StateBackend {
    Memory,
    Sqlite { path: String },
}

impl StateBackend {
    pub fn sqlite(path: &str) -> Self {
        Self::Sqlite {
            path: path.to_string(),
        }
    }
}

/// Pooled handle to the coordinator catalog. Implements the connection
/// trait so requests can run against it directly, and exposes `begin`
/// for explicit transactions.
#[derive(Clone)]
pub struct Database {
    conn: DatabaseConnection,
}

#[async_trait::async_trait]
impl ConnectionTrait for Database {
    fn get_database_backend(&self) -> DbBackend {
        self.conn.get_database_backend()
    }

    async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        self.conn.execute(stmt).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.conn.execute_unprepared(sql).await
    }

    async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.conn.query_one(stmt).await
    }

    async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.conn.query_all(stmt).await
    }

    fn support_returning(&self) -> bool {
        self.conn.support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.conn.is_mock_connection()
    }
}

impl Database {
    pub fn connection(&self) -> &DatabaseConnection {
        &self.conn
    }
}

impl Database {
    pub async fn with(backend: StateBackend) -> Result<Self> {
        match backend {
            StateBackend::Memory => {
                let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
                    .foreign_keys(true)
                    .pragma("recursive_triggers", "ON")
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .busy_timeout(SQLITE_BUSY_TIMEOUT);

                // In-memory SQLite is per-connection: a second connection would
                // see an empty, separate database. Hard-cap the pool at one.
                let pool = SqlitePoolOptions::new()
                    .min_connections(SQLITE_MIN_CONNECTIONS)
                    .max_connections(SQLITE_MIN_CONNECTIONS)
                    .test_before_acquire(false)
                    .connect_with(opts)
                    .await?;

                let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
                Ok(Self { conn })
            }
            StateBackend::Sqlite { path } => {
                // Build options programmatically (not via `from_str`) so URI
                // fragments in `path` cannot silently override the pragmas
                // below (foreign_keys, recursive_triggers, WAL, ...).
                let opts = SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .pragma("recursive_triggers", "ON")
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .disable_statement_logging()
                    .busy_timeout(SQLITE_BUSY_TIMEOUT);

                let pool = SqlitePoolOptions::new()
                    .min_connections(SQLITE_MIN_CONNECTIONS)
                    .max_connections(SQLITE_MAX_CONNECTIONS)
                    .test_before_acquire(false)
                    .connect_with(opts)
                    .await?;

                let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
                Ok(Self { conn })
            }
        }
    }

    /// Build an in-memory database with all migrations applied. Panics on
    /// failure; for tests only.
    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory)
            .await
            .expect("failed to connect to the database");
        Migrator::up(&this.conn, None)
            .await
            .expect("failed to apply migrations");
        this
    }

    pub async fn migrate(&self) -> Result<()> {
        Ok(Migrator::up(&self.conn, None).await?)
    }

    /// Begin an immediate transaction. Sea-orm default `begin()` issues a
    /// deferred `BEGIN` which only acquires the write lock on the first write.
    /// This causes `SQLITE_BUSY_SNAPSHOT` (517) when a concurrent reader holds
    /// a stale WAL snapshot. `BEGIN IMMEDIATE` acquires the write lock upfront,
    /// letting `busy_timeout` handle contention instead of failing immediately.
    pub async fn begin(&self) -> Result<DatabaseTransaction, DbErr> {
        // Sea-orm begin() issues `BEGIN` (deferred). We close that empty
        // transaction with `END` (alias for COMMIT) and re-open it as
        // `BEGIN IMMEDIATE` to acquire the write lock upfront.
        let txn = self.conn.begin().await?;
        txn.execute_unprepared("END; BEGIN IMMEDIATE").await?;
        Ok(txn)
    }
}
