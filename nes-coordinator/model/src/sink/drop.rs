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

use super::{Column, Model};
use crate::error::{CodedError, ErrorCode, catalog_write};
use crate::identifier::{QueryId, SinkId};
use crate::query::query_sink;
use crate::query::query_state::QueryState;
use crate::{ConnectorKind, Execute, IntoCondition, coded_bail, query, sink};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};
use serde::Deserialize;

/// Deletes the shared sinks that match every set field and returns the deleted rows.
/// Fails with `CatalogWriteRejected` and deletes nothing while a non-terminal query uses a matched sink.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropSink {
    pub name: Option<String>,
    pub id: Option<SinkId>,
}

impl DropSink {
    pub fn all() -> Self {
        Self::default()
    }
}

impl IntoCondition for DropSink {
    fn to_condition(&self) -> Condition {
        // Query-owned sinks are deleted with their query, so a user drop never matches them.
        Condition::all()
            .add(Column::Kind.eq(ConnectorKind::Shared))
            .add_option(self.name.clone().map(|v| Column::Name.eq(v)))
            .add_option(self.id.map(|v| Column::Id.eq(v)))
    }
}

impl Execute for DropSink {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let in_use: Option<(SinkId, Option<String>, QueryId)> = query_sink::Entity::find()
            .select_only()
            .column(query_sink::Column::SinkId)
            .column(Column::Name)
            .column(query_sink::Column::QueryId)
            .inner_join(sink::Entity)
            .inner_join(query::Entity)
            .filter(self.to_condition())
            .filter(query::Column::State.is_not_in(QueryState::TERMINAL))
            .into_tuple()
            .one(conn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogUnavailable, err))
            .context("failed to find the queries that write into the sink(s)")?;
        if let Some((id, name, query)) = in_use {
            let sink = name.unwrap_or_else(|| id.to_string());
            coded_bail!(
                ErrorCode::CatalogWriteRejected,
                "cannot drop sink {sink}: query {query} still writes into it"
            );
        }
        crate::drop_all::<sink::Entity>(self, conn)
            .await
            .map_err(catalog_write(
                ErrorCode::CatalogWriteRejected,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to drop sink(s)")
    }
}
