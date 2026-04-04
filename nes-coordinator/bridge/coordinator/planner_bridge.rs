use anyhow::{Result, anyhow};
use model::query::fragment::CreateFragment;
use model::request::Statement;
use sea_orm::DatabaseTransaction;

use crate::PlannerContext;
use crate::ffi;

impl From<ffi::PlannedQueryFragment> for CreateFragment {
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
}

impl coordinator::SqlPlanner for FfiSqlPlanner {
    fn plan(
        &self,
        txn: DatabaseTransaction,
        sql: &str,
    ) -> Result<(Statement, DatabaseTransaction)> {
        let ctx = PlannerContext::new(txn, self.rt_handle.clone());

        let planned = ffi::plan_sql(&ctx, sql).map_err(|e| anyhow!("{}", e))?;
        let mut statement: Statement = serde_json::from_str(&planned.json)?;

        if let Statement::CreateQuery(ref mut q) = statement {
            q.fragments = planned
                .fragments
                .into_iter()
                .map(CreateFragment::from)
                .collect();
            q.source_ids = planned.source_ids;
            q.sink_ids = planned.sink_ids;
        }

        Ok((statement, ctx.into_txn()))
    }
}
