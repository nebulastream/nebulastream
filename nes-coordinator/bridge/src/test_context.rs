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

//! Test support for C++ optimizer tests, which run without a coordinator.
//! The seeded catalog is read back through the production FFI, so a test exercises the same path.

use std::sync::OnceLock;

use anyhow::anyhow;
use model::database::Database;
use model::statement::Statement;
use tokio::runtime::{Builder, Runtime};

use crate::catalog::PlanningTransaction;
use crate::error::FfiError;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    unsafe extern "C++" {
        include!("nes-coordinator-bridge/catalog.h");
        type PlanningTransaction = crate::catalog::PlanningTransaction;
    }

    extern "Rust" {
        /// A transaction on a throwaway in-memory catalog.
        type TestPlanningTransaction;
        fn create_test_planner_context() -> Result<Box<TestPlanningTransaction>>;
        fn context(self: &TestPlanningTransaction) -> &PlanningTransaction;
        fn execute_seed_statement(
            self: &TestPlanningTransaction,
            statement_json: &str,
        ) -> Result<()>;
    }
}

/// A transaction on a throwaway catalog and the database whose one pooled connection it borrows.
pub struct TestPlanningTransaction {
    // Optional so they can be dropped in order inside an entered runtime context (see below).
    ctx: Option<PlanningTransaction>,
    db: Option<Database>,
}

impl TestPlanningTransaction {
    pub(crate) fn context(&self) -> &PlanningTransaction {
        self.ctx
            .as_ref()
            .expect("test planner context used after drop")
    }

    /// Runs one planned statement, given as the planner's JSON, on the seeding transaction.
    /// The write stays uncommitted in the transaction that the optimizer later reads.
    /// A real request sees its own writes the same way.
    pub(crate) fn execute_seed_statement(&self, statement_json: &str) -> Result<(), FfiError> {
        let statement: Statement =
            serde_json::from_str(statement_json).map_err(anyhow::Error::new)?;
        let ctx = self.context();
        ctx.block_on(statement.execute_on(ctx.txn()))
            .map_err(|e| anyhow!("failed to execute seed statement: {e:#}"))?;
        Ok(())
    }
}

impl Drop for TestPlanningTransaction {
    fn drop(&mut self) {
        // sqlx panics if the transaction or its pooled connection drops outside a Tokio context.
        // This runs on the C++ test thread, which has no runtime, so the static one is entered.
        // The transaction drops first and then the pool, while the guard is held.
        // The emptied `Option` fields then drop later.
        let _guard = test_runtime().enter();
        drop(self.ctx.take());
        drop(self.db.take());
    }
}

/// A runtime for the test contexts.
/// It outlives every context, so the optimizer's blocking catalog reads always have a runtime.
fn test_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build test runtime")
    })
}

pub(crate) fn create_test_planner_context() -> Result<Box<TestPlanningTransaction>, FfiError> {
    let runtime = test_runtime();
    let db = runtime.block_on(Database::for_test());
    let txn = runtime.block_on(db.begin()).map_err(anyhow::Error::new)?;
    let ctx = PlanningTransaction::new(txn, runtime.handle().clone());
    Ok(Box::new(TestPlanningTransaction {
        ctx: Some(ctx),
        db: Some(db),
    }))
}
