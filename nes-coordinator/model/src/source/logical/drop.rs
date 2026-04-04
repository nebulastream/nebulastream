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

use crate::{Execute, IntoCondition};
use crate::source::logical::{Column, Entity, Model};
use anyhow::Result;
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct DropLogicalSource {
    pub with_name: Option<String>,
}

impl IntoCondition for DropLogicalSource {
    fn into_condition(self) -> Condition {
        Condition::all().add_option(self.with_name.map(|name| Column::Name.eq(name)))
    }
}

impl Execute for DropLogicalSource {
    type Response = Vec<Model>;
    async fn execute(self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let condition = self.into_condition();
        let sources = Entity::find()
            .filter(condition.clone())
            .all(conn)
            .await?;
        Entity::delete_many()
            .filter(condition)
            .exec(conn)
            .await?;
        Ok(sources)
    }
}
