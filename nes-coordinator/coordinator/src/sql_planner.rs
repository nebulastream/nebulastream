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

/// Translates raw SQL into the typed statement the coordinator executes.
///
/// The implementation lives outside this crate so the coordinator stays
/// free of any SQL parser or optimizer dependency: it just hands off the
/// open transaction together with the source text, and expects the same
/// transaction back along with the translated statement.
///
/// The transaction is passed through so the planner can read catalog
/// state and stage inline rows (anonymous sources/sinks, plan blobs)
/// inside the same atomic unit that will execute the result. Returning
/// it lets the caller commit or roll the whole thing back as one unit.
///
/// `plan` is synchronous because the only current implementation calls
/// into C++. Callers must invoke it from a context that tolerates a
/// blocking call.
pub trait SqlPlanner: Send + Sync {
    fn plan(&self, txn: DatabaseTransaction, sql: &str)
    -> Result<(Statement, DatabaseTransaction)>;
}
