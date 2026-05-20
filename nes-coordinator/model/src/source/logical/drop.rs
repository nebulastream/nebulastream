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

use crate::source::logical::{Column, Entity, Model};
use crate::{Execute, IntoCondition};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DropLogicalSource {
    pub name: Option<String>,
}

impl DropLogicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl IntoCondition for DropLogicalSource {
    fn to_condition(&self) -> Condition {
        Condition::all().add_option(self.name.clone().map(|n| Column::Name.eq(n)))
    }
}

impl Execute for DropLogicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let condition = self.to_condition();
        let sources = Entity::find()
            .filter(condition.clone())
            .all(conn)
            .await
            .context("failed to fetch logical source(s)")?;
        Entity::delete_many()
            .filter(condition)
            .exec(conn)
            .await
            .context("failed to delete logical source(s)")?;
        Ok(sources)
    }
}
