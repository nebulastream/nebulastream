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

use std::collections::HashSet;

use anyhow::Result;
use model::ConnectorKind;
use model::error::{CodedError, ErrorCode};
use model::ml_model;
use model::sink;
use model::source::{logical, physical};
use model::statement::ParsedStatement;
use model::worker::{self, DesiredWorkerState, network_link};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DbBackend, DbErr, EntityTrait, QueryFilter,
    Statement,
};

/// Translates raw SQL into the typed statement that the coordinator executes.
///
/// The implementation is outside this crate so the coordinator has no SQL parser or optimizer dependency.
pub trait SqlPlanner: Send + Sync {
    /// A query statement also holds the anonymous and internal sources and sinks that the planner created,
    /// with identifiers counted on from the snapshot.
    /// It may block, so the caller runs it on a blocking thread.
    fn plan(&self, sql: &str, snapshot: CatalogSnapshot) -> Result<ParsedStatement>;

    fn validate_worker_config(&self, config: &serde_json::Value) -> Result<()>;
}

/// The part of the catalog that planning reads.
///
/// The caller reads it in the transaction that later applies the statement,
/// so the identifiers that the planner assigns are still free when its rows are inserted.
#[derive(Debug)]
pub struct CatalogSnapshot {
    pub logical_sources: Vec<logical::Model>,
    /// Only shared sources, because a query selects sources by their logical source.
    pub physical_sources: Vec<physical::Model>,
    /// Only shared sinks, because a query selects sinks by name.
    pub sinks: Vec<sink::Model>,
    pub models: Vec<ml_model::Model>,
    /// Only active workers, because placement uses no other.
    pub workers: Vec<worker::Model>,
    /// Only links between active workers.
    pub links: Vec<network_link::Model>,
    pub next_source_id: i64,
    pub next_sink_id: i64,
}

impl CatalogSnapshot {
    pub async fn read(txn: &DatabaseTransaction) -> Result<Self> {
        Self::try_read(txn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogUnavailable, err).into())
    }

    async fn try_read(txn: &DatabaseTransaction) -> Result<Self, DbErr> {
        let workers = worker::Entity::find()
            .filter(worker::Column::DesiredState.eq(DesiredWorkerState::Active))
            .all(txn)
            .await?;
        let active: HashSet<_> = workers.iter().map(|w| w.host_addr.clone()).collect();
        let links = network_link::Entity::find()
            .all(txn)
            .await?
            .into_iter()
            .filter(|l| {
                active.contains(&l.source_host_addr) && active.contains(&l.target_host_addr)
            })
            .collect();
        Ok(Self {
            logical_sources: logical::Entity::find().all(txn).await?,
            physical_sources: physical::Entity::find()
                .filter(physical::Column::Kind.eq(ConnectorKind::Shared))
                .all(txn)
                .await?,
            sinks: sink::Entity::find()
                .filter(sink::Column::Kind.eq(ConnectorKind::Shared))
                .all(txn)
                .await?,
            models: ml_model::Entity::find().all(txn).await?,
            workers,
            links,
            next_source_id: next_id(txn, "physical_source").await?,
            next_sink_id: next_id(txn, "sink").await?,
        })
    }
}

/// AUTOINCREMENT never reuses an identifier, also not the one of a deleted row.
/// The sequence table holds the last identifier, which can be larger than any identifier in use.
async fn next_id(txn: &DatabaseTransaction, table: &str) -> Result<i64, DbErr> {
    let last = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT seq FROM sqlite_sequence WHERE name = ?",
            [table.into()],
        ))
        .await?
        .map(|row| row.try_get_by_index::<i64>(0))
        .transpose()?;
    Ok(last.map_or(1, |seq| seq + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::database::Database;
    use serde_json::json;

    async fn run(db: &Database, statement: serde_json::Value) {
        let statement: ParsedStatement = serde_json::from_value(statement).unwrap();
        statement.execute_with(db).await.unwrap();
    }

    #[tokio::test]
    async fn the_next_source_id_skips_the_id_of_a_dropped_source() {
        let db = Database::for_test().await;
        run(&db, json!({"tag": "CreateWorker", "host_addr": "w:8080", "data_addr": "w:9090", "max_operators": null, "peers": [], "config": {}})).await;
        run(
            &db,
            json!({"tag": "CreateLogicalSource", "name": "S", "schema": []}),
        )
        .await;
        run(&db, json!({"tag": "CreatePhysicalSource", "logical_source": "S", "host_addr": "w:8080", "source_type": "FILE"})).await;
        run(
            &db,
            json!({"tag": "DropPhysicalSource", "id": null, "logical_source": null}),
        )
        .await;

        let txn = db.begin().await.unwrap();
        let snapshot = CatalogSnapshot::read(&txn).await.unwrap();
        assert!(snapshot.physical_sources.is_empty());
        assert_eq!((snapshot.next_source_id, snapshot.next_sink_id), (2, 1));
    }
}
