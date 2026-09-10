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

//! Test support, so a C++ optimizer test can run without a coordinator.
//! It seeds a throwaway in-memory catalog and reads it back through the same FFI the coordinator uses in production.

use std::sync::OnceLock;

use anyhow::anyhow;
use model::database::Database;
use model::statement::Statement;
use tokio::runtime::{Builder, Runtime};

use crate::catalog::TransactionContext;
use crate::error::FfiError;

/// A `TransactionContext` over a throwaway in-memory catalog, kept alive together with the `Database` whose single pooled
/// connection the transaction borrows.
pub struct TestTransactionContext {
    // Held in `Option` so `Drop` can drop them, in order, inside an entered runtime context (see below).
    ctx: Option<TransactionContext>,
    db: Option<Database>,
}

impl TestTransactionContext {
    pub(crate) fn context(&self) -> &TransactionContext {
        self.ctx
            .as_ref()
            .expect("test planner context used after drop")
    }

    /// Run one already-planned `Statement` (the JSON produced by `plan_sql`) against the seeding transaction.
    /// The write stays in the same uncommitted transaction the optimizer later reads, matching how a real request sees its own writes.
    pub(crate) fn execute_seed_statement(&self, statement_json: &str) -> Result<(), FfiError> {
        let statement: Statement = serde_json::from_str(statement_json)?;
        let ctx = self.context();
        ctx.block_on(statement.execute_on(ctx.txn()))
            .map_err(|e| anyhow!("failed to execute seed statement: {e:#}"))?;
        Ok(())
    }
}

impl Drop for TestTransactionContext {
    fn drop(&mut self) {
        // The transaction and its pooled sqlx connection must be dropped inside a Tokio context or sqlx panics.
        // This `drop` runs on the C++ test thread, which has no runtime, so enter the static one and drop them here,
        // transaction first and then pool, while the guard is live.
        // The emptied `Option` fields then drop later.
        let _guard = test_runtime().enter();
        drop(self.ctx.take());
        drop(self.db.take());
    }
}

/// A runtime for the test contexts.
/// It outlives every `TestTransactionContext` (and thus the `Handle` each stores), so the optimizer's blocking catalog reads
/// always have a runtime.
fn test_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build test runtime")
    })
}

pub(crate) fn create_test_planner_context() -> Result<Box<TestTransactionContext>, FfiError> {
    let runtime = test_runtime();
    let db = runtime.block_on(Database::for_test());
    let txn = runtime.block_on(db.begin())?;
    let ctx = TransactionContext::new(txn, runtime.handle().clone());
    Ok(Box::new(TestTransactionContext {
        ctx: Some(ctx),
        db: Some(db),
    }))
}
