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

use super::{Column, Entity, Model};
use crate::error::{CodedError, ErrorCode, catalog_write};
use crate::identifier::{QueryId, SourceId};
use crate::name::Identifier;
use crate::query::query_source;
use crate::query::query_state::QueryState;
use crate::{ConnectorKind, Execute, IntoCondition, coded_bail, query};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};
use serde::Deserialize;

/// Deletes the shared physical sources that match every set field and returns the deleted rows.
/// Fails with `CatalogWriteRejected` and deletes nothing while a non-terminal query uses a matched source.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DropPhysicalSource {
    pub id: Option<SourceId>,
    pub logical_source: Option<Identifier>,
}

impl DropPhysicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: SourceId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_logical_source(mut self, logical_source: Identifier) -> Self {
        self.logical_source = Some(logical_source);
        self
    }
}

impl IntoCondition for DropPhysicalSource {
    fn to_condition(&self) -> Condition {
        // Query-owned sources are deleted with their query, so a user drop never matches them.
        Condition::all()
            .add(Column::Kind.eq(ConnectorKind::Shared))
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(
                self.logical_source
                    .clone()
                    .map(|v| Column::LogicalSource.eq(v)),
            )
    }
}

impl Execute for DropPhysicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let in_use: Option<(SourceId, QueryId)> = query_source::Entity::find()
            .select_only()
            .column(query_source::Column::SourceId)
            .column(query_source::Column::QueryId)
            .inner_join(Entity)
            .inner_join(query::Entity)
            .filter(self.to_condition())
            .filter(query::Column::State.is_not_in(QueryState::TERMINAL))
            .into_tuple()
            .one(conn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogUnavailable, err))
            .context("failed to find the queries that read the physical source(s)")?;
        if let Some((source, query)) = in_use {
            coded_bail!(
                ErrorCode::CatalogWriteRejected,
                "cannot drop physical source {source}: query {query} still reads from it"
            );
        }
        crate::drop_all::<Entity>(self, conn)
            .await
            .map_err(catalog_write(
                ErrorCode::CatalogWriteRejected,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to drop physical source(s)")
    }
}
