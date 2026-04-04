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

use crate::triggers::m20260204_185834_init as triggers;
use crate::{assert_not_has_tables, drop_tables};
use model::ConnectorKind;
use model::query::StopMode;
use model::query::fragment::FragmentState;
use model::query::query_state::{DesiredQueryState, QueryState};
use model::worker::{DesiredWorkerState, WorkerState};
use sea_orm::DbBackend;
use sea_orm_migration::prelude::{Index as MigrationIndex, Table as MigrationTable, *};
use strum::IntoEnumIterator;

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
            Fragment,
            Query,
            QuerySource,
            QuerySink
        );

        match manager.get_database_backend() {
            DbBackend::MySql | DbBackend::Postgres => {
                return Err(DbErr::Custom(
                    "only SQLite is currently supported".to_string(),
                ));
            }
            DbBackend::Sqlite => {
                manager
                    .get_connection()
                    .execute_unprepared("PRAGMA foreign_keys = ON")
                    .await
                    .expect("failed to set foreign key enforcement");

                manager
                    .get_connection()
                    .execute_unprepared("PRAGMA journal_mode = WAL")
                    .await
                    .expect("failed to set journal mode");
            }
        }

        manager
            .create_table(
                MigrationTable::create()
                    .table(LogicalSource::Table)
                    .col(
                        ColumnDef::new(LogicalSource::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(LogicalSource::Schema).json().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Worker::Table)
                    .col(
                        ColumnDef::new(Worker::HostAddr)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Worker::DataAddr)
                            .string()
                            .not_null()
                            .unique_key(),
                    )
                    .col(
                        ColumnDef::new(Worker::MaxOperators)
                            .integer()
                            .null()
                            .check(Expr::col(Worker::MaxOperators).is_null().or(Expr::col(Worker::MaxOperators).gte(0))),
                    )
                    .col(
                        ColumnDef::new(Worker::Config)
                            .json_binary()
                            .not_null()
                            .default("{}"),
                    )
                    .col(
                        ColumnDef::new(Worker::CurrentState)
                            .string()
                            .not_null()
                            .default(WorkerState::default().to_string()),
                    )
                    .col(
                        ColumnDef::new(Worker::DesiredState)
                            .string()
                            .not_null()
                            .default(DesiredWorkerState::default().to_string())
                            .check(
                                Expr::col(Worker::DesiredState).is_in(
                                    DesiredWorkerState::iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .check(Expr::col(Worker::HostAddr).ne(Expr::col(Worker::DataAddr)))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(PhysicalSource::Table)
                    .col(
                        ColumnDef::new(PhysicalSource::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::LogicalSource)
                            .string()
                            .null(),
                    )
                    .col(ColumnDef::new(PhysicalSource::HostAddr).string().null())
                    .col(
                        ColumnDef::new(PhysicalSource::SourceType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::SourceConfig)
                            .json()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::ParserConfig)
                            .json()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PhysicalSource::Kind)
                            .string()
                            .not_null()
                            .default(ConnectorKind::Shared.to_string())
                            .check(
                                Expr::col(PhysicalSource::Kind).is_in(
                                    ConnectorKind::iter()
                                        .map(|k| k.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(PhysicalSource::Table, PhysicalSource::LogicalSource)
                            .to(LogicalSource::Table, LogicalSource::Name)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(PhysicalSource::Table, PhysicalSource::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Sink::Table)
                    .col(
                        ColumnDef::new(Sink::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Sink::Name).string().null().unique_key())
                    .col(ColumnDef::new(Sink::HostAddr).string().null())
                    .col(ColumnDef::new(Sink::SinkType).string().not_null())
                    .col(ColumnDef::new(Sink::Schema).json().not_null())
                    .col(ColumnDef::new(Sink::Config).json().not_null())
                    .col(
                        ColumnDef::new(Sink::Kind)
                            .string()
                            .not_null()
                            .default(ConnectorKind::Shared.to_string())
                            .check(
                                Expr::col(Sink::Kind).is_in(
                                    ConnectorKind::iter()
                                        .map(|k| k.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Sink::Table, Sink::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(NetworkLink::Table)
                    .col(
                        ColumnDef::new(NetworkLink::SourceHostAddr)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(NetworkLink::TargetHostAddr)
                            .string()
                            .not_null()
                            .check(
                                Expr::col(NetworkLink::TargetHostAddr)
                                    .ne(Expr::col(NetworkLink::SourceHostAddr)),
                            ),
                    )
                    .primary_key(
                        MigrationIndex::create()
                            .col(NetworkLink::SourceHostAddr)
                            .col(NetworkLink::TargetHostAddr),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Query::Table)
                    .col(
                        ColumnDef::new(Query::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Query::Name).string().null())
                    .col(ColumnDef::new(Query::Statement).string().not_null())
                    .col(
                        ColumnDef::new(Query::CurrentState)
                            .string()
                            .not_null()
                            .default(QueryState::default().to_string()),
                    )
                    .col(
                        ColumnDef::new(Query::DesiredState)
                            .string()
                            .not_null()
                            .default(DesiredQueryState::default().to_string())
                            .check(
                                Expr::col(Query::DesiredState).is_in(
                                    DesiredQueryState::iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>(),
                                ),
                            ),
                    )
                    .col(ColumnDef::new(Query::StartTimestamp).date_time().null())
                    .col(ColumnDef::new(Query::StopTimestamp).date_time().null())
                    .col(
                        ColumnDef::new(Query::StopMode).string().null().check(
                            Expr::col(Query::StopMode)
                                .is_null()
                                .or(Expr::col(Query::StopMode).is_in(
                                    StopMode::iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                                )),
                        ),
                    )
                    .col(ColumnDef::new(Query::Error).json().null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(Fragment::Table)
                    .col(
                        ColumnDef::new(Fragment::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Fragment::QueryId).big_integer().not_null())
                    .col(ColumnDef::new(Fragment::HostAddr).string().not_null())
                    .col(ColumnDef::new(Fragment::Plan).binary().not_null())
                    .col(
                        ColumnDef::new(Fragment::NumOperators)
                            .integer()
                            .not_null()
                            .check(Expr::col(Fragment::NumOperators).gte(0)),
                    )
                    .col(ColumnDef::new(Fragment::HasSource).boolean().not_null())
                    .col(
                        ColumnDef::new(Fragment::CurrentState)
                            .string()
                            .not_null()
                            .default(FragmentState::default().to_string()),
                    )
                    .col(ColumnDef::new(Fragment::StartTimestamp).date_time().null())
                    .col(ColumnDef::new(Fragment::StopTimestamp).date_time().null())
                    .col(ColumnDef::new(Fragment::Error).json().null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(Fragment::Table, Fragment::QueryId)
                            .to(Query::Table, Query::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Fragment::Table, Fragment::HostAddr)
                            .to(Worker::Table, Worker::HostAddr)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                MigrationIndex::create()
                    .table(Fragment::Table)
                    .name("idx_fragment_query_id")
                    .col(Fragment::QueryId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(QuerySource::Table)
                    .col(
                        ColumnDef::new(QuerySource::QueryId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QuerySource::SourceId)
                            .big_integer()
                            .not_null(),
                    )
                    .primary_key(
                        MigrationIndex::create()
                            .col(QuerySource::QueryId)
                            .col(QuerySource::SourceId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(QuerySource::Table, QuerySource::QueryId)
                            .to(Query::Table, Query::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(QuerySource::Table, QuerySource::SourceId)
                            .to(PhysicalSource::Table, PhysicalSource::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                MigrationTable::create()
                    .table(QuerySink::Table)
                    .col(
                        ColumnDef::new(QuerySink::QueryId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QuerySink::SinkId)
                            .big_integer()
                            .not_null(),
                    )
                    .primary_key(
                        MigrationIndex::create()
                            .col(QuerySink::QueryId)
                            .col(QuerySink::SinkId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(QuerySink::Table, QuerySink::QueryId)
                            .to(Query::Table, Query::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(QuerySink::Table, QuerySink::SinkId)
                            .to(Sink::Table, Sink::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

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

        drop_tables!(
            manager,
            QuerySource,
            QuerySink,
            LogicalSource,
            PhysicalSource,
            Sink,
            Worker,
            NetworkLink,
            Fragment,
            Query
        );
        Ok(())
    }
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

#[derive(DeriveIden)]
enum Sink {
    Table,
    Id,
    Name,
    HostAddr,
    #[allow(clippy::enum_variant_names)]
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
enum Fragment {
    Table,
    Id,
    QueryId,
    HostAddr,
    Plan,
    NumOperators,
    HasSource,
    CurrentState,
    StartTimestamp,
    StopTimestamp,
    Error,
}

#[derive(DeriveIden)]
enum Query {
    Table,
    Id,
    Name,
    Statement,
    CurrentState,
    DesiredState,
    StartTimestamp,
    StopTimestamp,
    StopMode,
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
