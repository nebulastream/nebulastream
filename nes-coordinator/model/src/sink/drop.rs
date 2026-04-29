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

use super::{Column, Model};
use crate::identifier::SinkId;
use crate::{ConnectorKind, Execute, IntoCondition, sink};
use anyhow::{Context, Result};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DropSink {
    pub name: Option<String>,
    pub id: Option<SinkId>,
}

impl DropSink {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: SinkId) -> Self {
        self.id = Some(id);
        self
    }
}

impl IntoCondition for DropSink {
    fn to_condition(&self) -> Condition {
        Condition::all()
            .add(Column::Kind.eq(ConnectorKind::Shared))
            .add_option(self.name.clone().map(|v| Column::Name.eq(v)))
            .add_option(self.id.map(|v| Column::Id.eq(v)))
    }
}

impl Execute for DropSink {
    type Response = Vec<Model>;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<Vec<Model>> {
        let condition = self.to_condition();
        let sinks = sink::Entity::find()
            .filter(condition.clone())
            .all(conn)
            .await
            .context("failed to find sink")?;
        sink::Entity::delete_many()
            .filter(condition)
            .exec(conn)
            .await
            .context("failed to delete sink(s)")?;
        Ok(sinks)
    }
}
