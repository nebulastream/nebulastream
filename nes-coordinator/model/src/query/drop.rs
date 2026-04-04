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
use crate::query::fragment;
use crate::query::fragment::{DesiredFragmentState, StopMode};
use crate::query::{self, Entity};
use anyhow::Result;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

use super::get::GetQuery;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DropQuery {
    pub stop_mode: StopMode,
    pub filters: GetQuery,
    #[serde(default)]
    pub should_block: bool,
}

impl DropQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_filters(mut self, filters: GetQuery) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_stop_mode(mut self, stop_mode: StopMode) -> Self {
        self.stop_mode = stop_mode;
        self
    }

    pub fn should_block(mut self) -> Self {
        self.should_block = true;
        self
    }
}

impl Execute for DropQuery {
    type Response = Vec<query::Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<query::Model>> {
        use crate::IntoCondition;

        let queries = Entity::find()
            .filter(self.filters.into_condition())
            .all(conn)
            .await?;
        let query_ids: Vec<_> = queries.iter().map(|q| q.id).collect();
        // Set stop_mode on all fragments of the query
        fragment::Entity::update_many()
            .col_expr(fragment::Column::StopMode, Expr::value(self.stop_mode))
            .filter(fragment::Column::QueryId.is_in(query_ids.clone()))
            .exec(conn)
            .await?;
        // Set desired_state to Stopped (graceful: only source fragments)
        let mut update = fragment::Entity::update_many()
            .col_expr(
                fragment::Column::DesiredState,
                Expr::value(DesiredFragmentState::Stopped),
            )
            .filter(fragment::Column::QueryId.is_in(query_ids));
        if self.stop_mode == StopMode::Graceful {
            update = update.filter(fragment::Column::HasSource.eq(true));
        }
        update.exec(conn).await?;
        Ok(queries)
    }
}
