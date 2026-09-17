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
use model::statement::ParsedStatement;
use sea_orm::DatabaseTransaction;

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
    ) -> Result<(ParsedStatement, DatabaseTransaction)>;

    fn validate_worker_config(&self, config: &serde_json::Value) -> Result<()>;
}
