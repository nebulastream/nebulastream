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

//! The initial catalog migration: creates every table, index, and trigger in the data model.
//! For the reasoning behind the schema design, see the model crate.

use crate::triggers::m20260204_185834_init as triggers;
use crate::{assert_not_has_tables, drop_tables};
use sea_orm::DbBackend;
use sea_orm_migration::prelude::{Index as MigrationIndex, Table as MigrationTable, *};

/// See the crate README for what a migration is and when to add one.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(
            manager,
            LogicalSource,
            PhysicalSource,
            Sink,
            Worker,
            NetworkLink,
            QueryFragment,
            Query,
            QuerySource,
            QuerySink,
            MlModel
        );

        match manager.get_database_backend() {
            DbBackend::MySql | DbBackend::Postgres => {
                return Err(DbErr::Custom(
                    "only SQLite is currently supported".to_string(),
                ));
            }
            DbBackend::Sqlite => {
                // Per-connection pragmas (foreign_keys, journal_mode, synchronous,
                // busy_timeout) are set via SqliteConnectOptions in `database.rs`,
                // which applies them to every connection in the pool.
            }
        }

        // Create tables in FK dependency order: parents first (LogicalSource, Worker),
        // then tables that reference them, then pure join tables.
        // `down` drops in reverse.
        for table in [
            logical_source_table(),
            worker_table(),
            physical_source_table(),
            sink_table(),
            network_link_table(),
            query_table(),
            query_fragment_table(),
            query_source_table(),
            query_sink_table(),
            ml_model_table(),
        ] {
            manager.create_table(table).await?;
        }
        for index in indexes() {
            manager.create_index(index).await?;
        }

        if let Some(sql) = triggers::up(manager.get_database_backend()) {
            manager.get_connection().execute_unprepared(sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        if let Some(sql) = triggers::down(backend) {
            manager.get_connection().execute_unprepared(sql).await?;
        }

        // Drop children before parents (reverse topological order).
        // SQLite ignores DROP TABLE ... CASCADE and enforces foreign keys,
        // so dropping a parent while a child still references it fails on a non-empty catalog.
        drop_tables!(
            manager,
            QuerySource,
            QuerySink,
            QueryFragment,
            PhysicalSource,
            Sink,
            NetworkLink,
            MlModel,
            LogicalSource,
            Worker,
            Query
        );
        Ok(())
    }
}

// Table definitions are formatted by hand, one line per column, for readability.
// rustfmt would spread every column over four lines and hide the structure behind the builder syntax.

#[rustfmt::skip]
fn logical_source_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(LogicalSource::Table)
        .col(ColumnDef::new(LogicalSource::Name).string().not_null().primary_key())
        .col(ColumnDef::new(LogicalSource::Schema).json_binary().not_null())
        .to_owned()
}

#[rustfmt::skip]
fn worker_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(Worker::Table)
        // TODO(yschroeder97): the host addr (IP, port) is not suitable as a stable PK for a worker
        .col(ColumnDef::new(Worker::HostAddr).string().not_null().primary_key())
        .col(ColumnDef::new(Worker::DataAddr).string().not_null().unique_key())
        .col(ColumnDef::new(Worker::MaxOperators).integer().null()
            .check(Expr::col(Worker::MaxOperators).is_null().or(Expr::col(Worker::MaxOperators).gte(0))))
        .col(ColumnDef::new(Worker::Config).json_binary().not_null().default("{}"))
        // keep in sync with model::worker::WorkerState
        .col(ColumnDef::new(Worker::CurrentState).string().not_null().default("Pending")
            .check(Expr::col(Worker::CurrentState).is_in(["Pending", "Active", "Unreachable", "Removed"])))
        // keep in sync with model::worker::DesiredWorkerState
        .col(ColumnDef::new(Worker::DesiredState).string().not_null().default("Active")
            .check(Expr::col(Worker::DesiredState).is_in(["Active", "Removed"])))
        .check(Expr::col(Worker::HostAddr).ne(Expr::col(Worker::DataAddr)))
        .to_owned()
}

#[rustfmt::skip]
fn physical_source_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(PhysicalSource::Table)
        .col(ColumnDef::new(PhysicalSource::Id).big_integer().not_null().auto_increment().primary_key())
        // NULL for query-owned (anonymous/internal) sources, which have no logical binding.
        // Shared sources (CREATE PHYSICAL SOURCE FOR <logical>) always reference one.
        .col(ColumnDef::new(PhysicalSource::LogicalSource).string().null())
        .col(ColumnDef::new(PhysicalSource::HostAddr).string().not_null())
        // Free-form connector type, owned and validated by the C++ side.
        .col(ColumnDef::new(PhysicalSource::SourceType).string().not_null())
        .col(ColumnDef::new(PhysicalSource::SourceConfig).json_binary().not_null())
        .col(ColumnDef::new(PhysicalSource::ParserConfig).json_binary().not_null())
        // keep in sync with model::ConnectorKind
        .col(ColumnDef::new(PhysicalSource::Kind).string().not_null().default("Shared")
            .check(Expr::col(PhysicalSource::Kind).is_in(["Shared", "Anonymous", "Internal"])))
        .foreign_key(&mut foreign_key(
            (PhysicalSource::Table, PhysicalSource::LogicalSource),
            (LogicalSource::Table, LogicalSource::Name),
            ForeignKeyAction::Restrict,
        ))
        .foreign_key(&mut foreign_key(
            (PhysicalSource::Table, PhysicalSource::HostAddr),
            (Worker::Table, Worker::HostAddr),
            ForeignKeyAction::Restrict,
        ))
        .to_owned()
}

#[rustfmt::skip]
fn sink_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(Sink::Table)
        .col(ColumnDef::new(Sink::Id).big_integer().not_null().auto_increment().primary_key())
        .col(ColumnDef::new(Sink::Name).string().null().unique_key())
        .col(ColumnDef::new(Sink::HostAddr).string().not_null())
        // Free-form connector type, owned and validated by the C++ side.
        .col(ColumnDef::new(Sink::SinkType).string().not_null())
        .col(ColumnDef::new(Sink::Schema).json_binary().not_null())
        .col(ColumnDef::new(Sink::Config).json_binary().not_null())
        // keep in sync with model::ConnectorKind
        .col(ColumnDef::new(Sink::Kind).string().not_null().default("Shared")
            .check(Expr::col(Sink::Kind).is_in(["Shared", "Anonymous", "Internal"])))
        .foreign_key(&mut foreign_key(
            (Sink::Table, Sink::HostAddr),
            (Worker::Table, Worker::HostAddr),
            ForeignKeyAction::Restrict,
        ))
        .to_owned()
}

#[rustfmt::skip]
fn network_link_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(NetworkLink::Table)
        .col(ColumnDef::new(NetworkLink::SourceHostAddr).string().not_null())
        .col(ColumnDef::new(NetworkLink::TargetHostAddr).string().not_null()
            .check(Expr::col(NetworkLink::TargetHostAddr).ne(Expr::col(NetworkLink::SourceHostAddr))))
        .primary_key(MigrationIndex::create().col(NetworkLink::SourceHostAddr).col(NetworkLink::TargetHostAddr))
        .to_owned()
}

#[rustfmt::skip]
fn query_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(Query::Table)
        .col(ColumnDef::new(Query::Id).big_integer().not_null().auto_increment().primary_key())
        .col(ColumnDef::new(Query::Name).string().null())
        .col(ColumnDef::new(Query::Sql).string().not_null())
        // keep in sync with model::query::query_state::QueryState
        .col(ColumnDef::new(Query::State).string().not_null().default("Pending")
            .check(Expr::col(Query::State).is_in(["Pending", "Started", "Running", "Completed", "Stopped", "Failed"])))
        .col(ColumnDef::new(Query::StartTimestamp).date_time().null())
        .col(ColumnDef::new(Query::StopTimestamp).date_time().null())
        .col(ColumnDef::new(Query::Error).json_binary().null())
        .to_owned()
}

#[rustfmt::skip]
fn query_fragment_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(QueryFragment::Table)
        .col(ColumnDef::new(QueryFragment::Id).big_integer().not_null().auto_increment().primary_key())
        .col(ColumnDef::new(QueryFragment::QueryId).big_integer().not_null())
        .col(ColumnDef::new(QueryFragment::HostAddr).string().not_null())
        .col(ColumnDef::new(QueryFragment::Plan).binary().not_null())
        .col(ColumnDef::new(QueryFragment::NumOperators).integer().not_null()
            .check(Expr::col(QueryFragment::NumOperators).gte(0)))
        .col(ColumnDef::new(QueryFragment::HasSource).boolean().not_null())
        // keep in sync with model::query::query_fragment::QueryFragmentState
        .col(ColumnDef::new(QueryFragment::CurrentState).string().not_null().default("Pending")
            .check(Expr::col(QueryFragment::CurrentState).is_in(["Pending", "Started", "Running", "Completed", "Stopped", "Failed"])))
        // keep in sync with model::query::query_fragment::DesiredQueryFragmentState
        .col(ColumnDef::new(QueryFragment::DesiredState).string().not_null().default("Completed")
            .check(Expr::col(QueryFragment::DesiredState).is_in(["Completed", "Stopped"])))
        .col(ColumnDef::new(QueryFragment::StartTimestamp).date_time().null())
        .col(ColumnDef::new(QueryFragment::StopTimestamp).date_time().null())
        .col(ColumnDef::new(QueryFragment::Error).json_binary().null())
        .col(ColumnDef::new(QueryFragment::LastObservedAt).date_time().null())
        .foreign_key(&mut foreign_key(
            (QueryFragment::Table, QueryFragment::QueryId),
            (Query::Table, Query::Id),
            ForeignKeyAction::Cascade,
        ))
        .foreign_key(&mut foreign_key(
            (QueryFragment::Table, QueryFragment::HostAddr),
            (Worker::Table, Worker::HostAddr),
            ForeignKeyAction::Restrict,
        ))
        .to_owned()
}

#[rustfmt::skip]
fn query_source_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(QuerySource::Table)
        .col(ColumnDef::new(QuerySource::QueryId).big_integer().not_null())
        .col(ColumnDef::new(QuerySource::SourceId).big_integer().not_null())
        .primary_key(MigrationIndex::create().col(QuerySource::QueryId).col(QuerySource::SourceId))
        .foreign_key(&mut foreign_key(
            (QuerySource::Table, QuerySource::QueryId),
            (Query::Table, Query::Id),
            ForeignKeyAction::Cascade,
        ))
        .foreign_key(&mut foreign_key(
            (QuerySource::Table, QuerySource::SourceId),
            (PhysicalSource::Table, PhysicalSource::Id),
            ForeignKeyAction::Cascade,
        ))
        .to_owned()
}

#[rustfmt::skip]
fn query_sink_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(QuerySink::Table)
        .col(ColumnDef::new(QuerySink::QueryId).big_integer().not_null())
        .col(ColumnDef::new(QuerySink::SinkId).big_integer().not_null())
        .primary_key(MigrationIndex::create().col(QuerySink::QueryId).col(QuerySink::SinkId))
        .foreign_key(&mut foreign_key(
            (QuerySink::Table, QuerySink::QueryId),
            (Query::Table, Query::Id),
            ForeignKeyAction::Cascade,
        ))
        .foreign_key(&mut foreign_key(
            (QuerySink::Table, QuerySink::SinkId),
            (Sink::Table, Sink::Id),
            ForeignKeyAction::Cascade,
        ))
        .to_owned()
}

#[rustfmt::skip]
fn ml_model_table() -> TableCreateStatement {
    MigrationTable::create()
        .table(MlModel::Table)
        .col(ColumnDef::new(MlModel::Name).string().not_null().primary_key())
        .col(ColumnDef::new(MlModel::Path).string().not_null())
        .col(ColumnDef::new(MlModel::InputSchema).json_binary().not_null())
        .col(ColumnDef::new(MlModel::OutputSchema).json_binary().not_null())
        .col(ColumnDef::new(MlModel::Imported).json_binary().not_null())
        .to_owned()
}

// Indexes for the hot lookup paths.
// The join tables' composite primary keys already cover lookups from the query side,
// so their indexes serve the reverse direction.
#[rustfmt::skip]
fn indexes() -> Vec<IndexCreateStatement> {
    vec![
        // Finding a query's fragments
        MigrationIndex::create().table(QueryFragment::Table).name("idx_query_fragment_query_id")
            .col(QueryFragment::QueryId).to_owned(),
        // The per-worker reconciler scans fragments by host and state on every tick
        MigrationIndex::create().table(QueryFragment::Table).name("idx_query_fragment_host_addr_current_state")
            .col(QueryFragment::HostAddr).col(QueryFragment::CurrentState).to_owned(),
        // Partial index: only Shared sources hold a logical_source,
        // so exclude the NULL (anonymous/internal) rows from the index.
        MigrationIndex::create().table(PhysicalSource::Table).name("idx_physical_source_logical_source")
            .col(PhysicalSource::LogicalSource)
            .and_where(Expr::col(PhysicalSource::LogicalSource).is_not_null()).to_owned(),
        // Finding the queries that use a source (orphan cleanup, drop guards)
        MigrationIndex::create().table(QuerySource::Table).name("idx_query_source_source_id")
            .col(QuerySource::SourceId).to_owned(),
        // Finding the queries that use a sink (orphan cleanup, drop guards)
        MigrationIndex::create().table(QuerySink::Table).name("idx_query_sink_sink_id")
            .col(QuerySink::SinkId).to_owned(),
        // Finding a worker's incoming links; the primary key covers the outgoing ones
        MigrationIndex::create().table(NetworkLink::Table).name("idx_network_link_target_host_addr")
            .col(NetworkLink::TargetHostAddr).to_owned(),
    ]
}

/// Builds a foreign key from one `(table, column)` pair to another.
/// Updates to a referenced key are always rejected; only the delete action varies.
fn foreign_key<F: IntoIden + 'static, T: IntoIden + 'static>(
    from: (F, F),
    to: (T, T),
    on_delete: ForeignKeyAction,
) -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .from(from.0, from.1)
        .to(to.0, to.1)
        .on_delete(on_delete)
        .on_update(ForeignKeyAction::Restrict)
        .to_owned()
}

#[derive(DeriveIden)]
enum LogicalSource {
    Table,
    Name,
    Schema,
}

#[derive(DeriveIden)]
enum PhysicalSource {
    Table,
    Id,
    LogicalSource,
    HostAddr,
    SourceType,
    SourceConfig,
    ParserConfig,
    Kind,
}

#[allow(clippy::enum_variant_names)]
#[derive(DeriveIden)]
enum Sink {
    Table,
    Id,
    Name,
    HostAddr,
    SinkType,
    Schema,
    Config,
    Kind,
}

#[derive(DeriveIden)]
enum Worker {
    Table,
    HostAddr,
    DataAddr,
    MaxOperators,
    Config,
    CurrentState,
    DesiredState,
}

#[derive(DeriveIden)]
enum NetworkLink {
    Table,
    SourceHostAddr,
    TargetHostAddr,
}

#[derive(DeriveIden)]
enum QueryFragment {
    Table,
    Id,
    QueryId,
    HostAddr,
    Plan,
    NumOperators,
    HasSource,
    CurrentState,
    DesiredState,
    StartTimestamp,
    StopTimestamp,
    Error,
    LastObservedAt,
}

#[derive(DeriveIden)]
enum Query {
    Table,
    Id,
    Name,
    Sql,
    State,
    StartTimestamp,
    StopTimestamp,
    Error,
}

#[derive(DeriveIden)]
enum QuerySource {
    Table,
    QueryId,
    SourceId,
}

#[derive(DeriveIden)]
enum QuerySink {
    Table,
    QueryId,
    SinkId,
}

#[derive(DeriveIden)]
enum MlModel {
    Table,
    Name,
    Path,
    InputSchema,
    OutputSchema,
    Imported,
}
