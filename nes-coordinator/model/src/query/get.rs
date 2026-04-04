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

use crate::identifier::QueryId;
use crate::query::fragment;
use crate::query::query_state::QueryState;
use crate::query::{self, Column, Entity};
use crate::{Execute, IntoCondition};
use anyhow::Result;
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetQuery {
    pub ids: Option<Vec<QueryId>>,
    pub name: Option<String>,
    pub state: Option<QueryState>,
    pub with_fragments: bool,
    pub wait_for_poll: bool,
}

impl GetQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: QueryId) -> Self {
        self.ids = Some(vec![id]);
        self
    }

    pub fn with_ids(mut self, ids: Vec<QueryId>) -> Self {
        self.ids = Some(ids);
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_state(mut self, state: QueryState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn with_fragments(mut self) -> Self {
        self.with_fragments = true;
        self
    }

    pub fn wait_for_poll(mut self) -> Self {
        self.wait_for_poll = true;
        self
    }
}

impl IntoCondition for GetQuery {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.ids.map(|ids| Column::Id.is_in(ids)))
            .add_option(self.name.map(|v| Column::Name.eq(v)))
            .add_option(self.state.map(|state| Column::State.eq(state)))
    }
}

impl Execute for GetQuery {
    type Response = Vec<(query::Model, Vec<fragment::Model>)>;
    async fn execute(
        self,
        conn: &impl ConnectionTrait,
    ) -> Result<Vec<(query::Model, Vec<fragment::Model>)>> {
        if self.with_fragments {
            Ok(Entity::find()
                .filter(self.into_condition())
                .find_with_related(fragment::Entity)
                .all(conn)
                .await?)
        } else {
            let queries = Entity::find()
                .filter(self.into_condition())
                .all(conn)
                .await?;
            Ok(queries.into_iter().map(|q| (q, vec![])).collect())
        }
    }
}
