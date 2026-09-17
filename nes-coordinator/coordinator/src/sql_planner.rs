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
use model::database::DatabaseTransaction;
use model::statement::Statement;

/// Translates raw SQL into the typed statement that the coordinator executes.
///
/// The implementation is outside this crate so the coordinator has no SQL parser or optimizer dependency.
///
/// The open transaction is passed in and returned so the planner can read catalog state
/// and stage inline rows (anonymous sources and sinks, plan blobs) in the same transaction that executes the result,
/// and the caller commits or rolls back everything as one unit.
///
/// `plan` is synchronous because the only current implementation calls into C++.
/// Callers must invoke it from a context that tolerates a blocking call.
pub trait SqlPlanner: Send + Sync {
    fn plan(&self, txn: DatabaseTransaction, sql: &str)
    -> Result<(Statement, DatabaseTransaction)>;
}
