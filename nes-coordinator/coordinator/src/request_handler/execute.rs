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

use super::RequestHandler;
use model::Execute;
use model::error::{CodedError, ErrorCode};
use model::request::StatementInput;
use model::statement::{Statement, StatementResult};

impl RequestHandler {
    /// Plans and runs one statement inside one catalog transaction,
    /// so whatever the planner staged is committed or rolled back together with the statement's own writes.
    #[allow(deprecated)]
    pub(super) async fn execute(&self, input: StatementInput) -> anyhow::Result<StatementResult> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| CodedError::new(ErrorCode::CatalogUnavailable, e))?;
        let (statement, txn) = match input {
            StatementInput::Parsed(statement) => (statement, txn),
            StatementInput::Sql(sql) => {
                let planner = self
                    .planner
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("no SQL planner configured"))?;
                // The simulator deprecates blocking calls but never configures a planner;
                // the real planner is synchronous, so it has to run on the blocking pool.
                tokio::task::spawn_blocking(move || planner.plan(txn, &sql)).await??
            }
        };
        // The worker version request is the exception: a worker's version is not stored anywhere,
        // so the catalog only selects the workers and each of them is then asked for its version.
        let result = if let Statement::GetWorkerVersion(req) = statement {
            let workers = req.execute(&txn).await?;
            StatementResult::WorkerVersions(
                controller::worker_versions(workers, self.factory.clone()).await,
            )
        } else {
            statement.execute_on(&txn).await?
        };
        txn.commit()
            .await
            .map_err(|e| CodedError::new(ErrorCode::CatalogWriteRejected, e))?;
        Ok(result)
    }
}
