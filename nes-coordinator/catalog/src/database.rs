use anyhow::Result;
use crate::error::Retryable;
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection, DbErr};
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::warn;

const SQLITE_MAX_CONNECTIONS: u32 = 8;
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
                    // When enabled (the default), sqlx pings the connection on every acquire.
                    // If a tokio::select! cancels the caller mid-ping, the connection is dropped,
                    // which destroys the in-memory database. Disabling the ping makes acquire
                    // cancellation-safe. See: https://docs.rs/sqlx/latest/sqlx/struct.Pool.html
                    .test_before_acquire(false)
                    .sqlx_logging(false);
                let conn = sea_orm::Database::connect(opts).await?;
                Ok(Self { conn })
            }
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
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
            ExponentialBackoff::from_millis(50).factor(RETRY_BACKOFF_FACTOR).map(jitter).take(MAX_RETRIES),
            || op(self.conn.clone()),
            |err: &DbErr| {
                if err.retryable() {
                    warn!("transient database error, retrying: {err}");
                    return true;
                }
                false
            },
        )
        .await
    }
}
