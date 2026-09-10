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
use crate::query::{self, Column, Entity, StopQuery};
use anyhow::Result;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

use super::get::GetQuery;

/// Stops the matched queries and marks them dropped.
/// The catalog deletes a dropped query, its fragments, and the sources and sinks it owns
/// once every fragment has terminated, in this transaction if that is already the case.
///
/// The returned queries reflect their state at drop time.
/// A caller that waits for the drop to finish finds the queries gone.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropQuery {
    #[serde(default)]
    pub filters: GetQuery,
}

impl DropQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_filters(mut self, filters: GetQuery) -> Self {
        self.filters = filters;
        self
    }
}

impl Execute for DropQuery {
    type Response = Vec<query::Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<query::Model>> {
        let stopped = StopQuery {
            filters: self.filters.clone(),
        }
        .execute(conn)
        .await?;
        let query_ids: Vec<_> = stopped.iter().map(|query| query.id).collect();
        Entity::update_many()
            .col_expr(Column::Dropped, Expr::value(true))
            .filter(Column::Id.is_in(query_ids))
            .exec(conn)
            .await?;
        Ok(stopped
            .into_iter()
            .map(|query| query::Model {
                dropped: true,
                ..query
            })
            .collect())
    }
}
