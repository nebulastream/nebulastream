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

use crate::{CatalogSnapshot, RequestHandler, WorkerMode, WorkerVersions};
use anyhow::Context;
use model::Execute;
use model::error::{CodedError, ErrorCode};
use model::statement::{ParsedStatement, StatementResult};
use sea_orm::DatabaseTransaction;
use serde_json::Value;

impl<V: WorkerVersions> RequestHandler<V> {
    // The simulator marks blocking calls as deprecated.
    // The simulation harness passes no planner, so this function returns before the blocking call.
    #[allow(deprecated)]
    pub(super) async fn execute_sql(&self, sql: String) -> anyhow::Result<StatementResult> {
        let planner = self.planner.clone().context("no SQL planner configured")?;
        let txn = self.begin().await?;
        let snapshot = CatalogSnapshot::read(&txn).await?;
        let statement = tokio::task::spawn_blocking(move || planner.plan(&sql, snapshot)).await??;
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
        Ok(self
            .db
            .begin()
            .await
            .map_err(|e| CodedError::new(ErrorCode::CatalogUnavailable, e))?)
    }

    async fn apply(
        &self,
        statement: ParsedStatement,
        txn: DatabaseTransaction,
    ) -> anyhow::Result<StatementResult> {
        if let ParsedStatement::CreateWorker(worker) = &statement {
            // `null` and `{}` count as no options, and any other value, such as `{"a": 1}` or `"a"`, counts as options.
            let has_options = match &worker.config {
                Value::Null => false,
                Value::Object(options) => !options.is_empty(),
                _ => true,
            };
            if self.mode == WorkerMode::Remote && has_options {
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
        // A worker's version is not stored in the catalog, so the catalog only selects the workers.
        let result = if let ParsedStatement::GetWorkerVersion(req) = statement {
            let workers = req.execute(&txn).await?;
            StatementResult::WorkerVersions(self.versions.versions(workers).await)
        } else {
            statement.execute_on(&txn).await?
        };
        txn.commit()
            .await
            .map_err(|e| CodedError::new(ErrorCode::CatalogWriteRejected, e))?;
        Ok(result)
    }
}
