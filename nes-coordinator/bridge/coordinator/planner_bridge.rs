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
use model::error::{CodedError, ErrorCode};
use model::query::query_fragment::CreateQueryFragment;
use model::statement::Statement;
use sea_orm::DatabaseTransaction;
use tracing::debug;

use crate::TransactionContext;
use crate::ffi;

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

pub(crate) struct FfiSqlPlanner {
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) optimizer_config: String,
    /// Host substituted for statements that omit their HOST clause.
    /// Empty means such statements are rejected, and a non-empty address is used for embedded deployments.
    pub(crate) default_host: String,
}

impl coordinator::SqlPlanner for FfiSqlPlanner {
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> Result<(Statement, DatabaseTransaction)> {
        let ctx = TransactionContext::new(txn, self.rt_handle.clone());

        let planned = ffi::plan_sql(&ctx, sql, &self.optimizer_config, &self.default_host)
            .map_err(|e| anyhow!("{}", e))?;
        if planned.error.code != 0 {
            // The caller is usually a person who wrote the SQL, so they get the message and the code.
            // The C++ stacktrace would be noise to them and goes to the log instead.
            debug!(
                code = planned.error.code,
                trace = %planned.error.trace,
                "planning failed"
            );
            return Err(anyhow::Error::new(CodedError::relayed(
                ErrorCode::from_code(planned.error.code),
                planned.error.msg,
                planned.error.trace,
            )));
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
