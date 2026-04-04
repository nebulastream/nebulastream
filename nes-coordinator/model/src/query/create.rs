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
use crate::identifier::{SinkId, SourceId};
use crate::query::fragment::{self, CreateFragment};
use crate::query::{self, query_sink, query_source, ActiveModel};
use anyhow::Result;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CreateQuery {
    #[serde(default)]
    pub name: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub fragments: Vec<CreateFragment>,
    #[serde(default)]
    pub source_ids: Vec<SourceId>,
    #[serde(default)]
    pub sink_ids: Vec<SinkId>,
}

impl CreateQuery {
    pub fn new(sql: String) -> Self {
        Self {
            name: None,
            sql,
            fragments: Vec::new(),
            source_ids: Vec::new(),
            sink_ids: Vec::new(),
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_fragments(mut self, fragments: Vec<CreateFragment>) -> Self {
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
    type Response = (query::Model, Vec<fragment::Model>);
    async fn execute(
        self,
        conn: &impl ConnectionTrait,
    ) -> Result<(query::Model, Vec<fragment::Model>)> {
        anyhow::ensure!(!self.fragments.is_empty(), "a query must have at least one fragment");
        anyhow::ensure!(!self.source_ids.is_empty(), "a query must reference at least one source");
        anyhow::ensure!(!self.sink_ids.is_empty(), "a query must reference at least one sink");
        let fragments = self.fragments.clone();
        let source_ids = self.source_ids.clone();
        let sink_ids = self.sink_ids.clone();
        let query = ActiveModel::from(self).insert(conn).await?;
        fragment::Entity::insert_many(fragments.into_iter().map(|f| f.into_active_model(query.id)))
            .exec(conn)
            .await?;
        query_source::Entity::insert_many(source_ids.into_iter().map(|sid| {
            query_source::ActiveModel {
                query_id: Set(query.id),
                source_id: Set(sid),
            }
        }))
        .exec(conn)
        .await?;
        query_sink::Entity::insert_many(sink_ids.into_iter().map(|sid| {
            query_sink::ActiveModel {
                query_id: Set(query.id),
                sink_id: Set(sid),
            }
        }))
        .exec(conn)
        .await?;
        let fragments = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(query.id))
            .all(conn)
            .await?;
        Ok((query, fragments))
    }
}
