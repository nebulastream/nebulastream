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

use crate::error::{CodedError, ErrorCode, catalog_write};
use crate::identifier::{SinkId, SourceId};
use crate::query::query_fragment::{self, CreateQueryFragment};
use crate::query::{ActiveModel, QueryWithFragments, query_sink, query_source};
use crate::sink;
use crate::source::physical;
use crate::{ConnectorKind, Execute};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;
use std::collections::BTreeSet;

/// Registers a query with its fragments, and links it to the sources and sinks that it uses.
/// Fails with `QueryInvalid` when the query has no fragment, no source or no sink.
/// The anonymous and internal sources and sinks among the ids become owned by the query.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateQuery {
    #[serde(default)]
    pub name: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub global_plan: Vec<u8>,
    #[serde(default)]
    pub fragments: Vec<CreateQueryFragment>,
    /// Each source that the query reads, listed once.
    /// A query can read one source more than once (a union of a stream with itself, for example),
    /// but the link table records which sources a query uses, not how often it reads each one.
    #[serde(default)]
    pub source_ids: BTreeSet<SourceId>,
    /// Each sink that the query writes into, listed once.
    #[serde(default)]
    pub sink_ids: BTreeSet<SinkId>,
}

impl CreateQuery {
    pub fn new(sql: String) -> Self {
        Self {
            name: None,
            sql,
            global_plan: Vec::new(),
            fragments: Vec::new(),
            source_ids: BTreeSet::new(),
            sink_ids: BTreeSet::new(),
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_fragments(mut self, fragments: Vec<CreateQueryFragment>) -> Self {
        self.fragments = fragments;
        self
    }
}

impl From<CreateQuery> for ActiveModel {
    fn from(req: CreateQuery) -> Self {
        Self {
            id: NotSet,
            name: Set(req.name),
            sql: Set(req.sql),
            global_plan: Set(req.global_plan),
            state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            error: NotSet,
            dropped: NotSet,
        }
    }
}

impl Execute for CreateQuery {
    type Response = QueryWithFragments;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<QueryWithFragments> {
        if self.fragments.is_empty() {
            crate::coded_bail!(
                ErrorCode::QueryInvalid,
                "a query must have at least one query_fragment"
            );
        }
        if self.source_ids.is_empty() {
            crate::coded_bail!(
                ErrorCode::QueryInvalid,
                "a query must reference at least one source"
            );
        }
        if self.sink_ids.is_empty() {
            crate::coded_bail!(
                ErrorCode::QueryInvalid,
                "a query must reference at least one sink"
            );
        }
        let fragments = self.fragments.clone();
        let source_ids = self.source_ids.clone();
        let sink_ids = self.sink_ids.clone();
        let query = ActiveModel::from(self.clone())
            .insert(conn)
            .await
            .map_err(catalog_write(
                ErrorCode::QueryAlreadyRegistered,
                ErrorCode::CatalogWriteRejected,
            ))
            .context("failed to insert query")?;
        query_fragment::Entity::insert_many(
            fragments
                .into_iter()
                .map(|fragment| fragment.into_active_model(query.id)),
        )
        .exec(conn)
        .await
        .map_err(catalog_write(
            ErrorCode::CatalogWriteRejected,
            ErrorCode::UnknownWorker,
        ))
        .context("failed to insert query fragments")?;
        query_source::Entity::insert_many(source_ids.into_iter().map(|sid| {
            query_source::ActiveModel {
                query_id: Set(query.id),
                source_id: Set(sid),
            }
        }))
        .exec(conn)
        .await
        .map_err(catalog_write(
            ErrorCode::CatalogWriteRejected,
            ErrorCode::UnknownSourceName,
        ))
        .context("failed to link query sources")?;
        query_sink::Entity::insert_many(sink_ids.into_iter().map(|sid| query_sink::ActiveModel {
            query_id: Set(query.id),
            sink_id: Set(sid),
        }))
        .exec(conn)
        .await
        .map_err(catalog_write(
            ErrorCode::CatalogWriteRejected,
            ErrorCode::UnknownSinkName,
        ))
        .context("failed to link query sinks")?;
        // The planner creates the query's own sources and sinks before the query row exists,
        // so they are bound to the query only now.
        physical::Entity::update_many()
            .col_expr(physical::Column::QueryId, Expr::value(query.id))
            .filter(physical::Column::Id.is_in(self.source_ids.iter().copied()))
            .filter(physical::Column::Kind.ne(ConnectorKind::Shared))
            .exec(conn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogWriteRejected, err))
            .context("failed to bind the query's own sources")?;
        sink::Entity::update_many()
            .col_expr(sink::Column::QueryId, Expr::value(query.id))
            .filter(sink::Column::Id.is_in(self.sink_ids.iter().copied()))
            .filter(sink::Column::Kind.ne(ConnectorKind::Shared))
            .exec(conn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogWriteRejected, err))
            .context("failed to bind the query's own sinks")?;
        let fragments = query_fragment::Entity::find()
            .filter(query_fragment::Column::QueryId.eq(query.id))
            .all(conn)
            .await
            .map_err(|err| CodedError::new(ErrorCode::CatalogUnavailable, err))
            .context("failed to load created query fragments")?;
        Ok(QueryWithFragments { query, fragments })
    }
}
