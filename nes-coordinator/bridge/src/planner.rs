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
use coordinator::SqlPlanner;
use model::query::query_fragment::CreateQueryFragment;
use model::statement::Statement;
use sea_orm::DatabaseTransaction;
use std::sync::Arc;
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

    /// The remaining fields are only meaningful when no error is reported.
    struct PlannedStatement {
        error: BridgeError,
        json: String,
        fragments: Vec<PlannedQueryFragment>,
        source_ids: Vec<i64>,
        sink_ids: Vec<i64>,
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

impl From<ffi::PlannedQueryFragment> for CreateQueryFragment {
    fn from(f: ffi::PlannedQueryFragment) -> Self {
        Self {
            host_addr: f.host_addr.parse().expect("invalid host_addr from planner"),
            plan: f.plan,
            num_operators: f.num_operators,
            has_source: f.has_source,
        }
    }
}

/// Plans SQL in the C++ optimizer, with the request's transaction lent to it as the catalog.
pub(crate) struct FfiSqlPlanner {
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) optimizer_config: String,
    /// Empty means a statement that omits its HOST clause is rejected.
    pub(crate) default_host: String,
}

#[must_use]
pub fn sql_planner(
    rt_handle: tokio::runtime::Handle,
    optimizer_config: &str,
    default_host: &str,
) -> Arc<dyn SqlPlanner> {
    Arc::new(FfiSqlPlanner {
        rt_handle,
        optimizer_config: optimizer_config.to_string(),
        default_host: default_host.to_string(),
    })
}

impl SqlPlanner for FfiSqlPlanner {
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> Result<(Statement, DatabaseTransaction)> {
        let ctx = PlanningTransaction::new(txn, self.rt_handle.clone());

        let planned = ffi::plan_sql(&ctx, sql, &self.optimizer_config, &self.default_host);
        if let Some(failure) = planned.error.into_error() {
            // The caller is usually the person who wrote the SQL and gets the message and the code.
            // The C++ stacktrace would be noise to them and goes to the log instead.
            debug!(code = ?failure.code, trace = %failure.trace, "planning failed");
            return Err(failure.into());
        }
        let mut statement: Statement = serde_json::from_str(&planned.json)?;

        if let Statement::CreateQuery(ref mut q) = statement {
            q.fragments = planned
                .fragments
                .into_iter()
                .map(CreateQueryFragment::from)
                .collect();
            q.source_ids = planned.source_ids.into_iter().map(Into::into).collect();
            q.sink_ids = planned.sink_ids.into_iter().map(Into::into).collect();
        }

        Ok((statement, ctx.into_txn()))
    }
}
