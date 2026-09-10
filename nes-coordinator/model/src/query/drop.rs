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
use crate::query::query_fragment;
use crate::query::query_fragment::DesiredQueryFragmentState;
use crate::query::{self, Entity};
use anyhow::Result;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

use super::get::GetQuery;

/// Records the intent to stop the matched queries by setting every one of their
/// fragments' desired state to Stopped, which stops the pipeline at its current
/// position rather than letting downstream fragments finish the data already in
/// flight. The actual stop happens asynchronously as the controller reconciles.
///
/// The returned queries reflect their state at drop time, so they are usually
/// still running. A caller that waits for the drop to finish is handed the
/// queries in their final, reconciled state instead.
#[derive(Clone, Debug, Default, Deserialize)]
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
        use crate::IntoCondition;

        let queries = Entity::find()
            .filter(self.filters.to_condition())
            .all(conn)
            .await?;
        let query_ids: Vec<_> = queries.iter().map(|query| query.id).collect();
        query_fragment::Entity::update_many()
            .col_expr(
                query_fragment::Column::DesiredState,
                Expr::value(DesiredQueryFragmentState::Stopped),
            )
            .filter(query_fragment::Column::QueryId.is_in(query_ids))
            .exec(conn)
            .await?;
        Ok(queries)
    }
}
