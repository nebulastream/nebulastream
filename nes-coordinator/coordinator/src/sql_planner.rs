use anyhow::Result;
use catalog::database::DatabaseTransaction;
use model::request::Statement;

pub trait SqlPlanner: Send + Sync {
    fn plan(&self, txn: DatabaseTransaction, sql: &str) -> Result<(Statement, DatabaseTransaction)>;
}
