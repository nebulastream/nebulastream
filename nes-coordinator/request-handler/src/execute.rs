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

use crate::RequestHandler;
use anyhow::Context;
use model::Execute;
use model::error::{CodedError, ErrorCode};
use model::statement::{ParsedStatement, StatementResult};
use sea_orm::DatabaseTransaction;
use serde_json::{Value, json};

/// Translates raw SQL into the typed statement that the coordinator executes.
///
/// The implementation is outside this crate so the coordinator has no SQL parser or optimizer dependency.
pub trait SqlPlanner: Send + Sync {
    /// The planner can read the catalog and stage inline rows (anonymous sources and sinks) in `txn`.
    /// It returns `txn`, so the caller commits or rolls back those rows together with the statement.
    /// It may block, so the caller runs it on a blocking thread.
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> anyhow::Result<(ParsedStatement, DatabaseTransaction)>;

    fn validate_worker_config(&self, config: &Value) -> anyhow::Result<()>;
}

impl RequestHandler {
    // The simulator marks blocking calls as deprecated.
    // The simulation harness passes no planner, so this function returns before the blocking call.
    #[allow(deprecated)]
    pub(super) async fn execute_sql(&self, sql: String) -> anyhow::Result<StatementResult> {
        let planner = self.planner.clone().context("no SQL planner configured")?;
        let txn = self.begin().await?;
        let (statement, txn) =
            tokio::task::spawn_blocking(move || planner.plan(txn, &sql)).await??;
        self.apply(statement, txn).await
    }

    pub(super) async fn execute(
        &self,
        statement: ParsedStatement,
    ) -> anyhow::Result<StatementResult> {
        let txn = self.begin().await?;
        self.apply(statement, txn).await
    }

    async fn begin(&self) -> anyhow::Result<DatabaseTransaction> {
        self.db
            .begin()
            .await
            .map_err(|e| CodedError::new(ErrorCode::CatalogUnavailable, e).into())
    }

    async fn apply(
        &self,
        statement: ParsedStatement,
        txn: DatabaseTransaction,
    ) -> anyhow::Result<StatementResult> {
        if let ParsedStatement::CreateWorker(worker) = &statement {
            if self.launcher.is_none() && !worker.config.is_null() && worker.config != json!({}) {
                model::coded_bail!(
                    ErrorCode::InvalidConfigParameter,
                    "the remote worker {} takes its options when it is started, not from CREATE WORKER",
                    worker.host_addr
                );
            }
            // Only the planner can validate worker options, so without one they are stored unchecked.
            if let Some(planner) = &self.planner {
                planner.validate_worker_config(&worker.config)?;
            }
        }
        match statement {
            // A worker's version is not stored in the catalog, so the catalog only selects the workers.
            // The transaction holds the write lock, so it ends before the workers are called.
            // No other request is served until every worker has answered or timed out.
            ParsedStatement::GetWorkerVersion(req) => {
                let workers = req.execute(&txn).await?;
                commit(txn).await?;
                let versions =
                    controller::get_worker_versions(workers, self.launcher.clone()).await;
                Ok(StatementResult::WorkerVersions(versions))
            }
            // Planning an EXPLAIN creates anonymous sources and sinks that no query owns,
            // so its transaction is rolled back.
            ParsedStatement::ExplainQuery { explanation } => {
                txn.rollback()
                    .await
                    .map_err(|e| CodedError::new(ErrorCode::CatalogUnavailable, e))?;
                Ok(StatementResult::ExplainedQuery(explanation))
            }
            statement => {
                let result = statement.execute_on(&txn).await?;
                commit(txn).await?;
                Ok(result)
            }
        }
    }
}

async fn commit(txn: DatabaseTransaction) -> anyhow::Result<()> {
    txn.commit()
        .await
        .map_err(|e| CodedError::new(ErrorCode::CatalogWriteRejected, e).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::database::Database;
    use model::query::CreateQueryWithRefs;
    use model::sink;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use sea_orm::EntityTrait;
    use tokio::sync::watch;

    #[tokio::test]
    async fn explain_rolls_back() {
        let db = Database::for_test().await;
        let mut req = any::<CreateQueryWithRefs>()
            .new_tree(&mut TestRunner::deterministic())
            .unwrap()
            .current();
        // Planning an EXPLAIN of a query with inline connectors creates them in the transaction.
        let txn = db.begin().await.unwrap();
        model::query::setup_refs(&txn, &mut req).await;

        let (_, receiver) = async_channel::bounded(1);
        let (intent_tx, _) = watch::channel(());
        let (_, state_rx) = watch::channel(());
        let handler = RequestHandler::new(receiver, db.clone(), intent_tx, state_rx, None, None);
        let explain = ParsedStatement::ExplainQuery {
            explanation: String::new(),
        };
        let explained = handler.apply(explain, txn).await.unwrap();

        assert!(matches!(explained, StatementResult::ExplainedQuery(_)));
        assert!(sink::Entity::find().all(&db).await.unwrap().is_empty());
    }
}
