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

use anyhow::{Result, anyhow};
use model::query::query_fragment::CreateQueryFragment;
use model::statement::ParsedStatement;
use request_handler::SqlPlanner;
use sea_orm::DatabaseTransaction;
use tracing::debug;

use crate::catalog::PlanningTransaction;

#[cxx::bridge(namespace = "NES::Bridge")]
pub(crate) mod ffi {
    struct PlannedQueryFragment {
        host_addr: String,
        plan: Vec<u8>,
        num_operators: i32,
        has_source: bool,
    }

    /// The result of planning one statement.
    /// The other fields are only meaningful when `error` is empty.
    struct PlannedStatement {
        error: BridgeError,
        /// The statement for the coordinator to execute, as JSON.
        statement_json: String,
        /// The per-worker plans of a query, each as protobuf bytes.
        /// Empty for every other statement.
        fragments: Vec<PlannedQueryFragment>,
    }

    unsafe extern "C++" {
        include!("nes-coordinator-bridge/error.h");
        include!("nes-coordinator-bridge/catalog.h");
        include!("PlannerBridge.hpp");

        type BridgeError = crate::error::ffi::BridgeError;
        type PlanningTransaction = crate::catalog::PlanningTransaction;

        fn plan_sql(
            ctx: &PlanningTransaction,
            sql: &str,
            optimizer_config: &str,
            default_host: &str,
        ) -> PlannedStatement;
    }
}

impl TryFrom<ffi::PlannedQueryFragment> for CreateQueryFragment {
    type Error = anyhow::Error;

    fn try_from(f: ffi::PlannedQueryFragment) -> Result<Self> {
        let host_addr = f.host_addr.parse().map_err(|err: String| {
            anyhow!(
                "the planner placed a fragment on '{}', which is not a worker address: {err}",
                f.host_addr
            )
        })?;
        Ok(Self {
            host_addr,
            plan: f.plan,
            num_operators: f.num_operators,
            has_source: f.has_source,
        })
    }
}

/// Plans SQL in the C++ optimizer, with the request's transaction lent to it as the catalog.
pub(crate) struct FfiSqlPlanner {
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) optimizer_config: String,
    /// Empty means a statement that omits its HOST clause is rejected.
    pub(crate) default_host: String,
}

impl SqlPlanner for FfiSqlPlanner {
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> Result<(ParsedStatement, DatabaseTransaction)> {
        let ctx = PlanningTransaction::new(txn, self.rt_handle.clone());

        let planned = ffi::plan_sql(&ctx, sql, &self.optimizer_config, &self.default_host);
        if let Some(failure) = planned.error.into_error() {
            // The caller is usually the person who wrote the SQL and gets the message and the code.
            // The C++ stacktrace would be noise to them and goes to the log instead.
            debug!(code = ?failure.code, trace = %failure.trace, "planning failed");
            return Err(failure.into());
        }
        let mut statement: ParsedStatement = serde_json::from_str(&planned.statement_json)?;

        if let ParsedStatement::CreateQuery(ref mut q) = statement {
            q.fragments = planned
                .fragments
                .into_iter()
                .map(CreateQueryFragment::try_from)
                .collect::<Result<_>>()?;
        }

        Ok((statement, ctx.into_txn()))
    }

    fn validate_worker_config(&self, config: &serde_json::Value) -> Result<()> {
        if config.is_null() {
            return Ok(());
        }
        crate::worker::validate_worker_config(&config.to_string())
    }
}
