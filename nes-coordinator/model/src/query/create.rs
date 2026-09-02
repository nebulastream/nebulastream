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

use crate::Execute;
use crate::error::{ErrorCode, catalog_write};
use crate::identifier::{SinkId, SourceId};
use crate::query::query_fragment::{self, CreateQueryFragment};
use crate::query::{self, ActiveModel, query_sink, query_source};
use anyhow::{Context, Result};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Deserialize)]
pub struct CreateQuery {
    #[serde(default)]
    pub name: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub fragments: Vec<CreateQueryFragment>,
    /// The sources this query reads, and the sinks it writes into, each named once.
    /// A query may read one source more than once, a union of a stream with itself for example, and the plan
    /// records every one of those reads. The link table answers which sources a query uses rather than how
    /// often it reads each one, so a set is what is stored here.
    #[serde(default)]
    pub source_ids: BTreeSet<SourceId>,
    #[serde(default)]
    pub sink_ids: BTreeSet<SinkId>,
}

impl CreateQuery {
    pub fn new(sql: String) -> Self {
        Self {
            name: None,
            sql,
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
            state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            error: NotSet,
        }
    }
}

impl Execute for CreateQuery {
    type Response = (query::Model, Vec<query_fragment::Model>);
    async fn execute(
        &self,
        conn: &impl ConnectionTrait,
    ) -> Result<(query::Model, Vec<query_fragment::Model>)> {
        anyhow::ensure!(
            !self.fragments.is_empty(),
            "a query must have at least one fragment"
        );
        anyhow::ensure!(
            !self.source_ids.is_empty(),
            "a query must reference at least one source"
        );
        anyhow::ensure!(
            !self.sink_ids.is_empty(),
            "a query must reference at least one sink"
        );
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
        .context("failed to insert query fragments")?;
        query_source::Entity::insert_many(source_ids.into_iter().map(|sid| {
            query_source::ActiveModel {
                query_id: Set(query.id),
                source_id: Set(sid),
            }
        }))
        .exec(conn)
        .await
        .context("failed to link query sources")?;
        query_sink::Entity::insert_many(sink_ids.into_iter().map(|sid| query_sink::ActiveModel {
            query_id: Set(query.id),
            sink_id: Set(sid),
        }))
        .exec(conn)
        .await
        .context("failed to link query sinks")?;
        let fragments = query_fragment::Entity::find()
            .filter(query_fragment::Column::QueryId.eq(query.id))
            .all(conn)
            .await
            .context("failed to load created query fragments")?;
        Ok((query, fragments))
    }
}
