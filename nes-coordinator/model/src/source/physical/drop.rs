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

use super::{Column, Entity, Model};
use crate::identifier::SourceId;
use crate::{ConnectorKind, Execute, IntoCondition};
use anyhow::Result;
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DropPhysicalSource {
    pub id: Option<SourceId>,
    pub logical_source: Option<String>,
}

impl DropPhysicalSource {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: SourceId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_logical_source(mut self, logical_source: String) -> Self {
        self.logical_source = Some(logical_source);
        self
    }
}

impl IntoCondition for DropPhysicalSource {
    fn to_condition(&self) -> Condition {
        // Only Shared rows are user-droppable. Query-owned (inline /
        // internal) sources are deleted automatically once their owning
        // query goes away, so we never touch them here.
        Condition::all()
            .add(Column::Kind.eq(ConnectorKind::Shared))
            .add_option(self.id.map(|v| Column::Id.eq(v)))
            .add_option(
                self.logical_source
                    .clone()
                    .map(|v| Column::LogicalSource.eq(v)),
            )
    }
}

impl Execute for DropPhysicalSource {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let condition = self.to_condition();
        let sources = Entity::find().filter(condition.clone()).all(conn).await?;
        Entity::delete_many().filter(condition).exec(conn).await?;
        Ok(sources)
    }
}
