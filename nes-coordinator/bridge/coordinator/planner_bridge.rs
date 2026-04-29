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
use model::query::fragment::CreateFragment;
use model::statement::Statement;
use sea_orm::DatabaseTransaction;

use crate::PlannerContext;
use crate::ffi;

impl From<ffi::PlannedFragment> for CreateFragment {
    fn from(f: ffi::PlannedFragment) -> Self {
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
}

impl coordinator::SqlPlanner for FfiSqlPlanner {
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> Result<(Statement, DatabaseTransaction)> {
        let ctx = PlannerContext::new(txn, self.rt_handle.clone());

        let planned =
            ffi::plan_sql(&ctx, sql, &self.optimizer_config).map_err(|e| anyhow!("{}", e))?;
        let mut statement: Statement = serde_json::from_str(&planned.json)?;

        if let Statement::CreateQuery(ref mut q) = statement {
            q.fragments = planned
                .fragments
                .into_iter()
                .map(CreateFragment::from)
                .collect();
            q.source_ids = planned.source_ids.into_iter().map(Into::into).collect();
            q.sink_ids = planned.sink_ids.into_iter().map(Into::into).collect();
        }

        Ok((statement, ctx.into_txn()))
    }
}
